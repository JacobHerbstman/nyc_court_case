# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_zap_source_integrity/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(jsonlite)
  library(lubridate)
  library(purrr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

period_lookup <- tibble(
  period = c("pre_1976", "1976-1979", "1980-1984", "1985-1989", "1990-1999", "2000-2009", "2010-2019", "2020-2025", "2026_plus", "missing_year"),
  period_order = seq_len(10)
)

assign_period <- function(year_value) {
  case_when(
    is.na(year_value) ~ "missing_year",
    year_value < 1976 ~ "pre_1976",
    year_value <= 1979 ~ "1976-1979",
    year_value <= 1984 ~ "1980-1984",
    year_value <= 1989 ~ "1985-1989",
    year_value <= 1999 ~ "1990-1999",
    year_value <= 2009 ~ "2000-2009",
    year_value <= 2019 ~ "2010-2019",
    year_value <= 2025 ~ "2020-2025",
    TRUE ~ "2026_plus"
  )
}

nonmissing_value <- function(x) {
  !is.na(x) & str_squish(as.character(x)) != ""
}

has_action_code <- function(x, code) {
  raw_value <- str_to_upper(coalesce(as.character(x), ""))
  str_detect(raw_value, paste0("(^|[^A-Z0-9])", code, "([^A-Z0-9]|$)"))
}

has_ulurp_code <- function(x, code) {
  raw_value <- str_to_upper(coalesce(as.character(x), ""))
  str_detect(raw_value, paste0("[0-9]{6,7}A?", code, "[A-Z]"))
}

flag_usability <- function(usable_flag, limited_flag) {
  case_when(
    usable_flag ~ "usable",
    limited_flag ~ "limited",
    TRUE ~ "not_recommended"
  )
}

standard_cd <- read_csv("../input/cd_homeownership_1990_measure.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = suppressWarnings(as.integer(borocd)),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name,
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro))
  ) |>
  arrange(borocd)

if (nrow(standard_cd) != n_distinct(standard_cd$borocd)) {
  stop("Homeownership measure is not unique by borocd.")
}

mappluto_cd <- read_parquet("../input/dcp_mappluto_current_25v4.parquet", col_select = c("bbl", "cd")) |>
  transmute(
    bbl_standardized = as.character(bbl),
    mappluto_borocd = suppressWarnings(as.integer(cd))
  ) |>
  filter(!is.na(bbl_standardized), bbl_standardized != "") |>
  distinct(bbl_standardized, .keep_all = TRUE)

zap_bbl <- read_parquet("../input/zap_project_bbl.parquet", col_select = c("project_id", "bbl_standardized")) |>
  transmute(
    project_id = as.character(project_id),
    bbl_standardized = as.character(bbl_standardized)
  ) |>
  filter(!is.na(project_id), project_id != "", !is.na(bbl_standardized), bbl_standardized != "") |>
  distinct(project_id, bbl_standardized)

if (nrow(zap_bbl) != nrow(distinct(zap_bbl, project_id, bbl_standardized))) {
  stop("Staged ZAP BBL links are not unique by project_id and bbl_standardized.")
}

if (nrow(mappluto_cd) != n_distinct(mappluto_cd$bbl_standardized)) {
  stop("Current MapPLUTO BBL-CD crosswalk is not unique by BBL.")
}

project_df <- read_parquet("../input/zap_project_data.parquet") |>
  mutate(
    project_id = as.character(project_id),
    cert_year = year(certified_referred_date_parsed),
    completed_year = year(completed_date_parsed),
    approval_year = year(approval_date_parsed),
    app_filed_year = year(app_filed_date_parsed),
    period = assign_period(cert_year),
    ulurp_flag = str_to_upper(str_squish(coalesce(as.character(ulurp_non), ""))) == "ULURP",
    borocd = suppressWarnings(as.integer(community_district_standardized)),
    standard_cd_flag = borocd %in% standard_cd$borocd,
    applicant_type_clean = str_to_lower(str_squish(coalesce(as.character(applicant_type), ""))),
    private_applicant_flag = str_detect(applicant_type_clean, "private"),
    public_applicant_flag = str_detect(applicant_type_clean, "public|city|agency|government"),
    all_text = str_to_upper(str_squish(paste(
      coalesce(as.character(project_name), ""),
      coalesce(as.character(project_brief), ""),
      coalesce(as.character(primary_applicant), ""),
      coalesce(as.character(ceqr_leadagency), "")
    ))),
    mih_flag_bool = str_to_lower(str_squish(coalesce(as.character(mih_flag), ""))) %in% c("true", "t", "yes", "y", "1")
  ) |>
  left_join(period_lookup, by = "period", relationship = "many-to-one")

if (nrow(project_df) != n_distinct(project_df$project_id)) {
  stop("Staged ZAP project data are not unique by project_id.")
}

action_codes <- tibble(
  code_family = c(
    rep("rezoning_special", 3),
    rep("public_land_disposition", 4),
    rep("hpd_public_housing_broad", 9)
  ),
  code = c("ZM", "ZR", "ZS", "HA", "PP", "PQ", "MM", "HA", "HD", "HO", "HU", "HP", "HG", "HC", "HL", "HM")
) |>
  distinct(code_family, code)

unique_action_codes <- sort(unique(action_codes$code))

for (code_value in unique_action_codes) {
  project_df[[paste0("actions_has_", str_to_lower(code_value))]] <- has_action_code(project_df$actions, code_value)
  project_df[[paste0("ulurp_has_", str_to_lower(code_value))]] <- has_ulurp_code(project_df$ulurp_numbers, code_value)
  project_df[[paste0("either_has_", str_to_lower(code_value))]] <-
    project_df[[paste0("actions_has_", str_to_lower(code_value))]] |
    project_df[[paste0("ulurp_has_", str_to_lower(code_value))]]
}

rezoning_codes <- c("ZM", "ZR", "ZS")
public_land_codes <- c("HA", "PP", "PQ", "MM")
hpd_housing_codes <- c("HA", "HD", "HO", "HU", "HP", "HG", "HC", "HL", "HM")

project_df <- project_df |>
  mutate(
    rezoning_special_action_flag = if_any(all_of(paste0("either_has_", str_to_lower(rezoning_codes))), identity),
    public_land_disposition_action_flag = if_any(all_of(paste0("either_has_", str_to_lower(public_land_codes))), identity),
    hpd_public_housing_action_flag = if_any(all_of(paste0("either_has_", str_to_lower(hpd_housing_codes))), identity),
    housing_strict_text_flag = str_detect(
      all_text,
      "\\b(RESIDENTIAL|RESIDENCE|RESIDENCES|HOUSING|DWELLING|DWELLINGS|APARTMENT|APARTMENTS|AFFORDABLE HOUSING|INCLUSIONARY HOUSING|SUPPORTIVE HOUSING|SENIOR HOUSING|HOMELESS SHELTER)\\b"
    ),
    housing_broad_text_flag = housing_strict_text_flag |
      str_detect(all_text, "\\b(MIXED[ -]?USE|AFFORD|RESIDENT\\b|MIH\\b|UDAAP|URBAN DEVELOPMENT ACTION AREA|DORMITORY|SENIOR|SUPPORTIVE)\\b"),
    hpd_text_flag = str_detect(all_text, "\\b(HPD|HOUSING PRESERVATION|DEPARTMENT OF HOUSING)\\b"),
    housing_action_code_flag = hpd_public_housing_action_flag | hpd_text_flag,
    housing_any_candidate_flag = housing_broad_text_flag | mih_flag_bool | housing_action_code_flag
  )

coverage_fields <- c(
  "project_id",
  "project_name",
  "project_brief",
  "project_status",
  "public_status",
  "ulurp_non",
  "actions",
  "ulurp_numbers",
  "ceqr_leadagency",
  "primary_applicant",
  "applicant_type",
  "borough",
  "community_district",
  "community_district_standardized",
  "current_milestone",
  "current_milestone_date_parsed",
  "app_filed_date_parsed",
  "noticed_date_parsed",
  "certified_referred_date_parsed",
  "approval_date_parsed",
  "completed_date_parsed",
  "mih_flag"
)

source_catalog <- read_csv("../input/source_catalog.csv", show_col_types = FALSE, na = c("", "NA"))
zap_project_source <- source_catalog |>
  filter(source_id == "dcp_zap_project_data")

if (nrow(zap_project_source) != 1) {
  stop("Source catalog must contain exactly one dcp_zap_project_data row.")
}

zap_project_dataset_id <- str_match(zap_project_source$official_url[[1]], "([a-z0-9]{4}-[a-z0-9]{4})")[, 2]
if (is.na(zap_project_dataset_id)) {
  stop("Could not parse Socrata dataset id for dcp_zap_project_data.")
}

zap_project_metadata_filename <- paste0(zap_project_dataset_id, "_metadata.json")
zap_project_pull_date <- resolve_raw_pull_date(list(dcp_zap_project_data = c(
  zap_project_source$expected_filename[[1]],
  zap_project_metadata_filename
)))
metadata_path <- file.path(raw_source_dir("dcp_zap_project_data"), zap_project_pull_date, zap_project_metadata_filename)

metadata_df <- if (length(metadata_path) == 1 && file.exists(metadata_path)) {
  metadata_json <- fromJSON(metadata_path, simplifyVector = FALSE)
  bind_rows(lapply(metadata_json$columns, function(column_row) {
    tibble(
      field = as.character(column_row$fieldName),
      metadata_cached_non_null = suppressWarnings(as.numeric(column_row$cachedContents$non_null)),
      metadata_cached_null = suppressWarnings(as.numeric(column_row$cachedContents$null)),
      metadata_cached_cardinality = suppressWarnings(as.numeric(column_row$cachedContents$cardinality))
    )
  }))
} else {
  tibble(
    field = character(),
    metadata_cached_non_null = numeric(),
    metadata_cached_null = numeric(),
    metadata_cached_cardinality = numeric()
  )
}

make_field_coverage <- function(df, scope_label) {
  map_dfr(coverage_fields, function(field_value) {
    df |>
      group_by(period, period_order) |>
      summarise(
        scope = scope_label,
        field = field_value,
        project_count = n(),
        nonmissing_count = sum(nonmissing_value(.data[[field_value]])),
        nonmissing_share = if_else(project_count > 0, nonmissing_count / project_count, NA_real_),
        distinct_value_count = n_distinct(.data[[field_value]][nonmissing_value(.data[[field_value]])]),
        min_date = if (inherits(.data[[field_value]], "Date")) as.character(suppressWarnings(min(.data[[field_value]], na.rm = TRUE))) else NA_character_,
        max_date = if (inherits(.data[[field_value]], "Date")) as.character(suppressWarnings(max(.data[[field_value]], na.rm = TRUE))) else NA_character_,
        .groups = "drop"
      )
  }) |>
    mutate(
      min_date = if_else(min_date %in% c("Inf", "NA"), NA_character_, min_date),
      max_date = if_else(max_date %in% c("-Inf", "NA"), NA_character_, max_date)
    )
}

source_catalog_note <- source_catalog |>
  filter(source_id == "dcp_zap_project_data") |>
  transmute(source_id, source_url = official_url, source_type = access_mode, coverage_start_date = start_date, source_note = notes)

source_field_coverage <- bind_rows(
  make_field_coverage(project_df, "all_project_rows"),
  make_field_coverage(filter(project_df, ulurp_flag), "ulurp_rows"),
  make_field_coverage(filter(project_df, ulurp_flag, cert_year >= 1976, cert_year <= 2025), "ulurp_1976_2025")
) |>
  left_join(metadata_df, by = "field", relationship = "many-to-one") |>
  mutate(
    source_id = "dcp_zap_project_data",
    source_url = source_catalog_note$source_url[1],
    coverage_start_date = source_catalog_note$coverage_start_date[1]
  ) |>
  arrange(scope, period_order, field)

action_scope_df <- project_df |>
  filter(ulurp_flag, cert_year >= 1976, cert_year <= 2025) |>
  select(project_id, period, period_order, actions, ulurp_numbers, housing_any_candidate_flag)

action_code_recovery <- pmap_dfr(action_codes, function(code_family, code) {
  action_scope_df |>
    mutate(
      code_family = code_family,
      code = code,
      actions_hit = has_action_code(actions, code),
      ulurp_numbers_hit = has_ulurp_code(ulurp_numbers, code),
      either_hit = actions_hit | ulurp_numbers_hit,
      actions_only_hit = actions_hit & !ulurp_numbers_hit,
      ulurp_numbers_only_hit = !actions_hit & ulurp_numbers_hit,
      both_hit = actions_hit & ulurp_numbers_hit
    ) |>
    group_by(period, period_order, code_family, code) |>
    summarise(
      project_count = n(),
      actions_nonmissing_count = sum(nonmissing_value(actions)),
      ulurp_numbers_nonmissing_count = sum(nonmissing_value(ulurp_numbers)),
      actions_hit_count = sum(actions_hit),
      ulurp_numbers_hit_count = sum(ulurp_numbers_hit),
      either_hit_count = sum(either_hit),
      actions_only_hit_count = sum(actions_only_hit),
      ulurp_numbers_only_hit_count = sum(ulurp_numbers_only_hit),
      both_hit_count = sum(both_hit),
      housing_candidate_either_hit_count = sum(either_hit & housing_any_candidate_flag),
      .groups = "drop"
    )
}) |>
  mutate(
    source_support = case_when(
      actions_hit_count > 0 & ulurp_numbers_hit_count > 0 ~ "actions_and_ulurp_numbers",
      actions_hit_count > 0 ~ "actions_only",
      ulurp_numbers_hit_count > 0 ~ "ulurp_numbers_only",
      TRUE ~ "no_hits"
    )
  ) |>
  arrange(period_order, code_family, code)

housing_scope <- project_df |>
  filter(ulurp_flag, cert_year >= 1976, cert_year <= 2025) |>
  mutate(
    strict_only = housing_strict_text_flag & !mih_flag_bool & !housing_action_code_flag,
    broad_only = housing_broad_text_flag & !housing_strict_text_flag & !mih_flag_bool & !housing_action_code_flag,
    action_only = housing_action_code_flag & !housing_broad_text_flag & !mih_flag_bool,
    mih_any = mih_flag_bool,
    no_housing_candidate = !housing_any_candidate_flag
  )

housing_flag_counts <- housing_scope |>
  group_by(period, period_order) |>
  summarise(
    section = "flag_counts",
    project_count = n(),
    housing_strict_text_flag = sum(housing_strict_text_flag),
    housing_broad_text_flag = sum(housing_broad_text_flag),
    mih_flag_bool = sum(mih_flag_bool),
    housing_action_code_flag = sum(housing_action_code_flag),
    hpd_text_flag = sum(hpd_text_flag),
    hpd_public_housing_action_flag = sum(hpd_public_housing_action_flag),
    housing_any_candidate_flag = sum(housing_any_candidate_flag),
    broad_only = sum(broad_only),
    action_only = sum(action_only),
    no_housing_candidate = sum(no_housing_candidate),
    .groups = "drop"
  ) |>
  pivot_longer(
    cols = -c(period, period_order, section, project_count),
    names_to = "flag_or_combination",
    values_to = "count"
  ) |>
  mutate(share = if_else(project_count > 0, count / project_count, NA_real_))

housing_overlap_counts <- housing_scope |>
  count(
    period,
    period_order,
    housing_strict_text_flag,
    housing_broad_text_flag,
    mih_flag_bool,
    housing_action_code_flag,
    housing_any_candidate_flag,
    name = "count"
  ) |>
  group_by(period, period_order) |>
  mutate(project_count = sum(count), share = count / project_count) |>
  ungroup() |>
  mutate(
    section = "flag_overlap",
    flag_or_combination = str_c(
      "strict=", housing_strict_text_flag,
      ";broad=", housing_broad_text_flag,
      ";mih=", mih_flag_bool,
      ";action=", housing_action_code_flag,
      ";any=", housing_any_candidate_flag
    )
  ) |>
  select(period, period_order, section, project_count, flag_or_combination, count, share)

housing_flag_overlap <- bind_rows(housing_flag_counts, housing_overlap_counts) |>
  arrange(period_order, section, flag_or_combination)

project_bbl_summary <- zap_bbl |>
  left_join(mappluto_cd, by = "bbl_standardized", relationship = "many-to-one") |>
  mutate(mappluto_standard_cd_flag = mappluto_borocd %in% standard_cd$borocd) |>
  group_by(project_id) |>
  summarise(
    bbl_count = n_distinct(bbl_standardized),
    matched_bbl_count = n_distinct(bbl_standardized[!is.na(mappluto_borocd)]),
    standard_cd_matched_bbl_count = n_distinct(bbl_standardized[mappluto_standard_cd_flag]),
    matched_cd_count = n_distinct(mappluto_borocd[mappluto_standard_cd_flag]),
    matched_cd_list = str_c(sort(unique(mappluto_borocd[mappluto_standard_cd_flag])), collapse = ";"),
    .groups = "drop"
  ) |>
  mutate(matched_cd_list = if_else(matched_cd_list == "", NA_character_, matched_cd_list))

geography_project_df <- project_df |>
  filter(ulurp_flag, cert_year >= 1976, cert_year <= 2025) |>
  select(project_id, period, period_order, borocd, standard_cd_flag, community_district_multi_flag, housing_any_candidate_flag) |>
  left_join(project_bbl_summary, by = "project_id", relationship = "one-to-one") |>
  mutate(
    bbl_count = coalesce(bbl_count, 0L),
    matched_bbl_count = coalesce(matched_bbl_count, 0L),
    standard_cd_matched_bbl_count = coalesce(standard_cd_matched_bbl_count, 0L),
    matched_cd_count = coalesce(matched_cd_count, 0L),
    bbl_any_flag = bbl_count > 0,
    bbl_matched_flag = matched_bbl_count > 0,
    bbl_standard_cd_matched_flag = standard_cd_matched_bbl_count > 0,
    bbl_multi_cd_flag = matched_cd_count > 1,
    single_bbl_cd = if_else(matched_cd_count == 1, suppressWarnings(as.integer(matched_cd_list)), NA_integer_),
    primary_bbl_conflict_flag = standard_cd_flag & !is.na(single_bbl_cd) & borocd != single_bbl_cd
  )

geography_assignment_audit <- geography_project_df |>
  group_by(period, period_order) |>
  summarise(
    project_count = n(),
    primary_cd_nonmissing_count = sum(!is.na(borocd)),
    primary_cd_nonmissing_share = primary_cd_nonmissing_count / project_count,
    primary_standard_cd_count = sum(standard_cd_flag),
    primary_standard_cd_share = primary_standard_cd_count / project_count,
    source_multi_cd_project_count = sum(str_to_lower(coalesce(as.character(community_district_multi_flag), "")) %in% c("true", "t", "1", "yes")),
    source_multi_cd_project_share = source_multi_cd_project_count / project_count,
    bbl_any_project_count = sum(bbl_any_flag),
    bbl_any_project_share = bbl_any_project_count / project_count,
    bbl_matched_project_count = sum(bbl_matched_flag),
    bbl_matched_project_share = bbl_matched_project_count / project_count,
    bbl_standard_cd_project_count = sum(bbl_standard_cd_matched_flag),
    bbl_standard_cd_project_share = bbl_standard_cd_project_count / project_count,
    bbl_multi_cd_project_count = sum(bbl_multi_cd_flag),
    bbl_multi_cd_project_share = bbl_multi_cd_project_count / project_count,
    primary_bbl_single_cd_conflict_count = sum(primary_bbl_conflict_flag, na.rm = TRUE),
    primary_bbl_single_cd_conflict_share = if_else(sum(!is.na(single_bbl_cd) & standard_cd_flag) > 0, primary_bbl_single_cd_conflict_count / sum(!is.na(single_bbl_cd) & standard_cd_flag), NA_real_),
    mean_bbl_count = mean(bbl_count),
    mean_matched_bbl_count = mean(matched_bbl_count),
    housing_candidate_project_count = sum(housing_any_candidate_flag),
    housing_candidate_bbl_standard_cd_project_count = sum(housing_any_candidate_flag & bbl_standard_cd_matched_flag),
    .groups = "drop"
  ) |>
  arrange(period_order)

period_support <- geography_assignment_audit |>
  left_join(
    project_df |>
      filter(ulurp_flag, cert_year >= 1976, cert_year <= 2025) |>
      group_by(period, period_order) |>
      summarise(
        cert_date_nonmissing_share = mean(!is.na(certified_referred_date_parsed)),
        actions_nonmissing_share = mean(nonmissing_value(actions)),
        ulurp_numbers_nonmissing_share = mean(nonmissing_value(ulurp_numbers)),
        status_nonmissing_share = mean(nonmissing_value(project_status)),
        approval_date_nonmissing_share = mean(!is.na(approval_date_parsed)),
        completed_date_nonmissing_share = mean(!is.na(completed_date_parsed)),
        completed_status_count = sum(project_status == "Complete", na.rm = TRUE),
        completed_with_completion_date_share = if_else(completed_status_count > 0, sum(project_status == "Complete" & !is.na(completed_date_parsed), na.rm = TRUE) / completed_status_count, NA_real_),
        .groups = "drop"
      ),
    by = c("period", "period_order"),
    relationship = "one-to-one"
  ) |>
  left_join(
    action_code_recovery |>
      group_by(period, period_order) |>
      summarise(
        recovered_action_code_count = sum(either_hit_count),
        recovered_rezoning_special_count = sum(either_hit_count[code_family == "rezoning_special"]),
        recovered_public_land_count = sum(either_hit_count[code_family == "public_land_disposition"]),
        .groups = "drop"
      ),
    by = c("period", "period_order"),
    relationship = "one-to-one"
  )

outcome_usability <- period_support |>
  transmute(
    period,
    period_order,
    project_count,
    application_count_usability = flag_usability(
      project_count >= 50 & cert_date_nonmissing_share >= 0.95 & primary_standard_cd_share >= 0.90,
      project_count >= 20 & cert_date_nonmissing_share >= 0.80 & primary_standard_cd_share >= 0.75
    ),
    action_category_split_usability = flag_usability(
      project_count >= 50 & ulurp_numbers_nonmissing_share >= 0.80 & recovered_action_code_count > 0,
      project_count >= 20 & ulurp_numbers_nonmissing_share >= 0.60 & recovered_action_code_count > 0
    ),
    status_outcome_usability = flag_usability(
      project_count >= 50 & status_nonmissing_share >= 0.95 & !period %in% c("2020-2025"),
      project_count >= 20 & status_nonmissing_share >= 0.80
    ),
    approval_timing_usability = flag_usability(
      project_count >= 50 & approval_date_nonmissing_share >= 0.80,
      project_count >= 20 & approval_date_nonmissing_share >= 0.50
    ),
    completion_timing_usability = flag_usability(
      completed_status_count >= 20 & completed_with_completion_date_share >= 0.80,
      completed_status_count >= 10 & completed_with_completion_date_share >= 0.50
    ),
    bbl_fractional_geography_usability = flag_usability(
      bbl_standard_cd_project_share >= 0.80,
      bbl_standard_cd_project_share >= 0.50
    ),
    support_note = str_c(
      "cert_share=", round(cert_date_nonmissing_share, 3),
      "; primary_cd_share=", round(primary_standard_cd_share, 3),
      "; actions_share=", round(actions_nonmissing_share, 3),
      "; ulurp_numbers_share=", round(ulurp_numbers_nonmissing_share, 3),
      "; approval_date_share=", round(approval_date_nonmissing_share, 3),
      "; bbl_standard_cd_share=", round(bbl_standard_cd_project_share, 3)
    )
  ) |>
  pivot_longer(
    cols = ends_with("_usability"),
    names_to = "outcome_type",
    values_to = "usability"
  ) |>
  mutate(outcome_type = str_remove(outcome_type, "_usability$")) |>
  arrange(period_order, outcome_type)

manual_review_sample <- bind_rows(
  housing_scope |>
    filter(housing_broad_text_flag, !housing_strict_text_flag, !mih_flag_bool, !housing_action_code_flag) |>
    mutate(sample_reason = "broad_text_only"),
  housing_scope |>
    filter(housing_action_code_flag, !housing_broad_text_flag, !mih_flag_bool) |>
    mutate(sample_reason = "action_code_only"),
  housing_scope |>
    filter(housing_strict_text_flag) |>
    mutate(sample_reason = "strict_text_candidate"),
  housing_scope |>
    filter(mih_flag_bool) |>
    mutate(sample_reason = "mih_candidate"),
  geography_project_df |>
    filter(bbl_count == 0, housing_any_candidate_flag) |>
    inner_join(housing_scope, by = c("project_id", "period", "period_order"), relationship = "one-to-one", suffix = c("_geo", "")) |>
    mutate(sample_reason = "housing_candidate_no_bbl"),
  geography_project_df |>
    filter(primary_bbl_conflict_flag) |>
    inner_join(housing_scope, by = c("project_id", "period", "period_order"), relationship = "one-to-one", suffix = c("_geo", "")) |>
    mutate(sample_reason = "primary_cd_bbl_cd_conflict")
) |>
  group_by(sample_reason, period, period_order) |>
  arrange(project_id, .by_group = TRUE) |>
  slice_head(n = 5) |>
  ungroup() |>
  transmute(
    sample_reason,
    period,
    period_order,
    cert_year,
    project_id,
    project_name,
    project_status,
    public_status,
    primary_applicant,
    applicant_type,
    borocd = coalesce(borocd, borocd_geo),
    actions,
    ulurp_numbers,
    housing_strict_text_flag,
    housing_broad_text_flag,
    mih_flag_bool,
    housing_action_code_flag,
    housing_any_candidate_flag,
    project_brief
  ) |>
  arrange(sample_reason, period_order, project_id)

pre_2016_ulurp <- project_df |>
  filter(ulurp_flag, cert_year >= 1976, cert_year <= 2015)

qc_df <- bind_rows(
  tibble(
    metric = "standard_cd_count",
    value = as.character(n_distinct(standard_cd$borocd)),
    status = if_else(n_distinct(standard_cd$borocd) == 59, "pass", "fail"),
    note = "Standard community districts in the treatment denominator file."
  ),
  tibble(
    metric = "project_duplicate_id_count",
    value = as.character(nrow(project_df) - n_distinct(project_df$project_id)),
    status = if_else(nrow(project_df) == n_distinct(project_df$project_id), "pass", "fail"),
    note = "Staged ZAP projects should be unique by project_id."
  ),
  tibble(
    metric = "project_bbl_duplicate_key_count",
    value = as.character(nrow(zap_bbl) - nrow(distinct(zap_bbl, project_id, bbl_standardized))),
    status = if_else(nrow(zap_bbl) == nrow(distinct(zap_bbl, project_id, bbl_standardized)), "pass", "fail"),
    note = "Staged ZAP project-BBL links should be unique."
  ),
  tibble(
    metric = "mappluto_bbl_duplicate_count",
    value = as.character(nrow(mappluto_cd) - n_distinct(mappluto_cd$bbl_standardized)),
    status = if_else(nrow(mappluto_cd) == n_distinct(mappluto_cd$bbl_standardized), "pass", "fail"),
    note = "Current MapPLUTO BBL-CD crosswalk should be unique by BBL."
  ),
  tibble(
    metric = "ulurp_1976_2025_project_count",
    value = as.character(nrow(filter(project_df, ulurp_flag, cert_year >= 1976, cert_year <= 2025))),
    status = if_else(nrow(filter(project_df, ulurp_flag, cert_year >= 1976, cert_year <= 2025)) > 0, "pass", "fail"),
    note = "ULURP projects in the construction window."
  ),
  tibble(
    metric = "pre_2016_actions_nonmissing_share",
    value = as.character(mean(nonmissing_value(pre_2016_ulurp$actions))),
    status = "info",
    note = "Historical actions are expected to be sparse; action proxies must not rely on this field alone."
  ),
  tibble(
    metric = "pre_2016_ulurp_numbers_nonmissing_share",
    value = as.character(mean(nonmissing_value(pre_2016_ulurp$ulurp_numbers))),
    status = if_else(mean(nonmissing_value(pre_2016_ulurp$ulurp_numbers)) >= 0.75, "pass", "fail"),
    note = "Historical action recovery should rely on ULURP numbers if this support is high."
  ),
  tibble(
    metric = "pre_2016_rezoning_special_recovered_count",
    value = as.character(sum(pre_2016_ulurp$rezoning_special_action_flag, na.rm = TRUE)),
    status = if_else(sum(pre_2016_ulurp$rezoning_special_action_flag, na.rm = TRUE) > 0, "pass", "fail"),
    note = "Guard against all-zero historical rezoning/special-permit proxies caused by blank actions."
  ),
  tibble(
    metric = "pre_2016_public_land_recovered_count",
    value = as.character(sum(pre_2016_ulurp$public_land_disposition_action_flag, na.rm = TRUE)),
    status = if_else(sum(pre_2016_ulurp$public_land_disposition_action_flag, na.rm = TRUE) > 0, "pass", "fail"),
    note = "Guard against all-zero historical public-land proxies caused by blank actions."
  ),
  tibble(
    metric = "approval_timing_not_recommended_period_count",
    value = as.character(sum(outcome_usability$outcome_type == "approval_timing" & outcome_usability$usability == "not_recommended")),
    status = "info",
    note = "Approval timing is diagnostic only; sparse approval dates should not block the source audit."
  ),
  tibble(
    metric = "source_field_coverage_row_count",
    value = as.character(nrow(source_field_coverage)),
    status = if_else(nrow(source_field_coverage) > 0, "pass", "fail"),
    note = "Audit field coverage output rows."
  ),
  tibble(
    metric = "outcome_usability_row_count",
    value = as.character(nrow(outcome_usability)),
    status = if_else(nrow(outcome_usability) > 0, "pass", "fail"),
    note = "Outcome usability output rows."
  )
)

write_csv(source_field_coverage, "../output/zap_source_field_coverage_by_era.csv", na = "")
write_csv(action_code_recovery, "../output/zap_action_code_recovery_by_era.csv", na = "")
write_csv(housing_flag_overlap, "../output/zap_housing_flag_overlap.csv", na = "")
write_csv(manual_review_sample, "../output/zap_project_manual_review_sample.csv", na = "")
write_csv(geography_assignment_audit, "../output/zap_geography_assignment_audit.csv", na = "")
write_csv(outcome_usability, "../output/zap_outcome_usability_by_period.csv", na = "")
write_csv(qc_df, "../output/zap_source_integrity_qc.csv", na = "")

if (any(qc_df$status == "fail")) {
  stop("ZAP source integrity audit failed; inspect ../output/zap_source_integrity_qc.csv")
}
