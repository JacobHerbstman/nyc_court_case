# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_zap_housing_pipeline_from_raw/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(lubridate)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

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

has_action_code <- function(x, code) {
  raw_value <- str_to_upper(coalesce(as.character(x), ""))
  str_detect(raw_value, paste0("(^|[^A-Z0-9])", code, "([^A-Z0-9]|$)"))
}

has_ulurp_code <- function(x, code) {
  raw_value <- str_to_upper(coalesce(as.character(x), ""))
  str_detect(raw_value, paste0("[0-9]{6,7}A?", code, "[A-Z]"))
}

source_label <- function(actions_flag, ulurp_numbers_flag) {
  case_when(
    actions_flag & ulurp_numbers_flag ~ "actions_and_ulurp_numbers",
    actions_flag ~ "actions_only",
    ulurp_numbers_flag ~ "ulurp_numbers_only",
    TRUE ~ "none"
  )
}

simple_status <- function(project_status) {
  case_when(
    project_status == "Complete" ~ "completed",
    project_status %in% c("Withdrawn-Other", "Terminated", "Terminated-Applicant Unresponsive") ~ "withdrawn_terminated",
    project_status %in% c("Active", "On-Hold") ~ "unresolved_or_in_process",
    project_status == "Record Closed" ~ "other_closed",
    TRUE ~ "missing_status"
  )
}

zap_audit_qc <- read_csv("../input/zap_source_integrity_qc.csv", show_col_types = FALSE, na = c("", "NA"))

audit_fail_count <- sum(zap_audit_qc$status == "fail", na.rm = TRUE)
if (audit_fail_count > 0) {
  stop("Source integrity audit has failing hard checks; inspect ../input/zap_source_integrity_qc.csv")
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

redevelopment_denoms <- read_csv("../input/cd_redevelopment_potential_baseline.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = suppressWarnings(as.integer(borocd)),
    residential_acres = suppressWarnings(as.numeric(residential_acres))
  ) |>
  arrange(borocd)

if (nrow(redevelopment_denoms) != n_distinct(redevelopment_denoms$borocd)) {
  stop("Redevelopment potential baseline is not unique by borocd.")
}

cd_denoms <- standard_cd |>
  left_join(redevelopment_denoms, by = "borocd", relationship = "one-to-one")

if (any(is.na(cd_denoms$occupied_units_1990)) || any(is.na(cd_denoms$residential_acres))) {
  stop("Missing occupied-unit or residential-acre denominators.")
}

outcome_usability <- read_csv("../input/zap_outcome_usability_by_period.csv", show_col_types = FALSE, na = c("", "NA")) |>
  select(period, outcome_type, usability) |>
  pivot_wider(names_from = outcome_type, values_from = usability, names_prefix = "usability_")

required_usability_cols <- c(
  "period",
  "usability_application_count",
  "usability_action_category_split",
  "usability_status_outcome",
  "usability_bbl_fractional_geography"
)

if (!all(required_usability_cols %in% names(outcome_usability))) {
  stop("Outcome usability file is missing required support columns.")
}

mappluto_cd <- read_parquet("../input/dcp_mappluto_current_25v4.parquet", col_select = c("bbl", "cd")) |>
  transmute(
    bbl_standardized = as.character(bbl),
    borocd = suppressWarnings(as.integer(cd))
  ) |>
  filter(!is.na(bbl_standardized), bbl_standardized != "") |>
  distinct(bbl_standardized, .keep_all = TRUE)

if (nrow(mappluto_cd) != n_distinct(mappluto_cd$bbl_standardized)) {
  stop("Current MapPLUTO BBL-CD crosswalk is not unique by BBL.")
}

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

project_df <- read_parquet("../input/zap_project_data.parquet") |>
  mutate(
    project_id = as.character(project_id),
    cert_year = year(certified_referred_date_parsed),
    period = assign_period(cert_year),
    ulurp_flag = str_to_upper(str_squish(coalesce(as.character(ulurp_non), ""))) == "ULURP",
    borocd_primary = suppressWarnings(as.integer(community_district_standardized)),
    primary_standard_cd_flag = borocd_primary %in% standard_cd$borocd,
    applicant_type_clean = str_to_lower(str_squish(coalesce(as.character(applicant_type), ""))),
    private_applicant_flag = str_detect(applicant_type_clean, "private"),
    public_applicant_flag = str_detect(applicant_type_clean, "public|city|agency|government"),
    all_text = str_to_upper(str_squish(paste(
      coalesce(as.character(project_name), ""),
      coalesce(as.character(project_brief), ""),
      coalesce(as.character(primary_applicant), ""),
      coalesce(as.character(ceqr_leadagency), "")
    ))),
    mih_flag_bool = str_to_lower(str_squish(coalesce(as.character(mih_flag), ""))) %in% c("true", "t", "yes", "y", "1"),
    status_simple = simple_status(project_status)
  )

if (nrow(project_df) != n_distinct(project_df$project_id)) {
  stop("Staged ZAP project data are not unique by project_id.")
}

rezoning_codes <- c("ZM", "ZR", "ZS")
public_land_codes <- c("HA", "PP", "PQ", "MM")
hpd_housing_codes <- c("HA", "HD", "HO", "HU", "HP", "HG", "HC", "HL", "HM")
unique_action_codes <- sort(unique(c(rezoning_codes, public_land_codes, hpd_housing_codes)))

for (code_value in unique_action_codes) {
  project_df[[paste0("actions_has_", str_to_lower(code_value))]] <- has_action_code(project_df$actions, code_value)
  project_df[[paste0("ulurp_has_", str_to_lower(code_value))]] <- has_ulurp_code(project_df$ulurp_numbers, code_value)
  project_df[[paste0("either_has_", str_to_lower(code_value))]] <-
    project_df[[paste0("actions_has_", str_to_lower(code_value))]] |
    project_df[[paste0("ulurp_has_", str_to_lower(code_value))]]
}

project_base <- project_df |>
  filter(ulurp_flag, cert_year >= 1976, cert_year <= 2025) |>
  mutate(
    rezoning_special_actions_flag = if_any(all_of(paste0("actions_has_", str_to_lower(rezoning_codes))), identity),
    rezoning_special_ulurp_numbers_flag = if_any(all_of(paste0("ulurp_has_", str_to_lower(rezoning_codes))), identity),
    rezoning_special_action_flag = rezoning_special_actions_flag | rezoning_special_ulurp_numbers_flag,
    rezoning_special_source = source_label(rezoning_special_actions_flag, rezoning_special_ulurp_numbers_flag),
    public_land_disposition_actions_flag = if_any(all_of(paste0("actions_has_", str_to_lower(public_land_codes))), identity),
    public_land_disposition_ulurp_numbers_flag = if_any(all_of(paste0("ulurp_has_", str_to_lower(public_land_codes))), identity),
    public_land_disposition_action_flag = public_land_disposition_actions_flag | public_land_disposition_ulurp_numbers_flag,
    public_land_disposition_source = source_label(public_land_disposition_actions_flag, public_land_disposition_ulurp_numbers_flag),
    hpd_public_housing_actions_flag = if_any(all_of(paste0("actions_has_", str_to_lower(hpd_housing_codes))), identity),
    hpd_public_housing_ulurp_numbers_flag = if_any(all_of(paste0("ulurp_has_", str_to_lower(hpd_housing_codes))), identity),
    hpd_public_housing_action_flag = hpd_public_housing_actions_flag | hpd_public_housing_ulurp_numbers_flag,
    hpd_public_housing_source = source_label(hpd_public_housing_actions_flag, hpd_public_housing_ulurp_numbers_flag),
    housing_strict_text_flag = str_detect(
      all_text,
      "\\b(RESIDENTIAL|RESIDENCE|RESIDENCES|HOUSING|DWELLING|DWELLINGS|APARTMENT|APARTMENTS|AFFORDABLE HOUSING|INCLUSIONARY HOUSING|SUPPORTIVE HOUSING|SENIOR HOUSING|HOMELESS SHELTER)\\b"
    ),
    housing_broad_text_flag = housing_strict_text_flag |
      str_detect(all_text, "\\b(MIXED[ -]?USE|AFFORD|RESIDENT\\b|MIH\\b|UDAAP|URBAN DEVELOPMENT ACTION AREA|DORMITORY|SENIOR|SUPPORTIVE)\\b"),
    hpd_text_flag = str_detect(all_text, "\\b(HPD|HOUSING PRESERVATION|DEPARTMENT OF HOUSING)\\b"),
    housing_action_code_flag = hpd_public_housing_action_flag | hpd_text_flag,
    housing_any_candidate_flag = housing_broad_text_flag | mih_flag_bool | housing_action_code_flag,
    all_ulurp_apps = TRUE,
    housing_any_candidate_apps = housing_any_candidate_flag,
    housing_strict_text_apps = housing_strict_text_flag,
    housing_broad_text_apps = housing_broad_text_flag,
    housing_mih_apps = mih_flag_bool,
    housing_action_code_apps = housing_action_code_flag,
    housing_any_private_apps = housing_any_candidate_flag & private_applicant_flag,
    housing_any_public_apps = housing_any_candidate_flag & public_applicant_flag,
    housing_any_rezoning_special_apps = housing_any_candidate_flag & rezoning_special_action_flag,
    housing_any_public_land_disposition_apps = housing_any_candidate_flag & public_land_disposition_action_flag,
    housing_any_hpd_public_housing_apps = housing_any_candidate_flag & hpd_public_housing_action_flag
  ) |>
  left_join(
    standard_cd |>
      select(borocd_primary = borocd, borough_code_primary = borough_code, borough_name_primary = borough_name, treat_z_boro_primary = treat_z_boro),
    by = "borocd_primary",
    relationship = "many-to-one"
  ) |>
  select(
    project_id,
    source_vintage,
    source_raw_path,
    project_name,
    project_brief,
    project_status,
    public_status,
    status_simple,
    ulurp_non,
    actions,
    ulurp_numbers,
    ceqr_type,
    ceqr_number,
    ceqr_leadagency,
    primary_applicant,
    applicant_type,
    private_applicant_flag,
    public_applicant_flag,
    borough,
    borough_code,
    borough_name_standardized,
    community_district,
    borocd_primary,
    primary_standard_cd_flag,
    borough_code_primary,
    borough_name_primary,
    treat_z_boro_primary,
    community_district_multi_flag,
    council_district_multi_flag,
    app_filed_date_parsed,
    noticed_date_parsed,
    certified_referred_date_parsed,
    approval_date_parsed,
    completed_date_parsed,
    cert_year,
    period,
    current_milestone,
    current_milestone_date_parsed,
    mih_flag,
    mih_flag_bool,
    housing_strict_text_flag,
    housing_broad_text_flag,
    hpd_text_flag,
    housing_action_code_flag,
    housing_any_candidate_flag,
    rezoning_special_action_flag,
    rezoning_special_actions_flag,
    rezoning_special_ulurp_numbers_flag,
    rezoning_special_source,
    public_land_disposition_action_flag,
    public_land_disposition_actions_flag,
    public_land_disposition_ulurp_numbers_flag,
    public_land_disposition_source,
    hpd_public_housing_action_flag,
    hpd_public_housing_actions_flag,
    hpd_public_housing_ulurp_numbers_flag,
    hpd_public_housing_source,
    all_of(paste0("actions_has_", str_to_lower(unique_action_codes))),
    all_of(paste0("ulurp_has_", str_to_lower(unique_action_codes))),
    all_of(paste0("either_has_", str_to_lower(unique_action_codes))),
    all_ulurp_apps,
    housing_any_candidate_apps,
    housing_strict_text_apps,
    housing_broad_text_apps,
    housing_mih_apps,
    housing_action_code_apps,
    housing_any_private_apps,
    housing_any_public_apps,
    housing_any_rezoning_special_apps,
    housing_any_public_land_disposition_apps,
    housing_any_hpd_public_housing_apps
  ) |>
  arrange(cert_year, borocd_primary, project_id)

outcome_dictionary <- tribble(
  ~outcome_name, ~outcome_label, ~requires_action_split,
  "all_ulurp_apps", "All ULURP applications", FALSE,
  "housing_any_candidate_apps", "Housing-oriented ULURP applications", FALSE,
  "housing_strict_text_apps", "Strict-text housing applications", FALSE,
  "housing_broad_text_apps", "Broad-text housing applications", FALSE,
  "housing_mih_apps", "MIH-flagged housing applications", FALSE,
  "housing_action_code_apps", "Housing-action proxy applications", TRUE,
  "housing_any_private_apps", "Private housing-oriented applications", FALSE,
  "housing_any_public_apps", "Public housing-oriented applications", FALSE,
  "housing_any_rezoning_special_apps", "Housing-oriented rezoning/special-permit applications", TRUE,
  "housing_any_public_land_disposition_apps", "Housing-oriented public-land/disposition applications", TRUE,
  "housing_any_hpd_public_housing_apps", "Housing-oriented HPD/public-housing proxy applications", TRUE
)

make_project_outcome_rows <- function(df, assignment_type) {
  df |>
    pivot_longer(
      cols = all_of(outcome_dictionary$outcome_name),
      names_to = "outcome_name",
      values_to = "outcome_included"
    ) |>
    filter(outcome_included) |>
    left_join(outcome_dictionary, by = "outcome_name", relationship = "many-to-one") |>
    mutate(assignment_type = assignment_type) |>
    select(
      assignment_type,
      project_id,
      outcome_name,
      outcome_label,
      requires_action_split,
      borocd,
      assignment_weight,
      cert_year,
      period,
      project_status,
      status_simple,
      primary_applicant,
      applicant_type,
      private_applicant_flag,
      public_applicant_flag,
      project_name,
      actions,
      ulurp_numbers
    ) |>
    arrange(outcome_name, cert_year, borocd, project_id)
}

primary_project_cd <- project_base |>
  filter(primary_standard_cd_flag) |>
  mutate(
    borocd = borocd_primary,
    assignment_weight = 1
  ) |>
  make_project_outcome_rows("primary_zap_cd")

bbl_cd_weights <- zap_bbl |>
  left_join(mappluto_cd, by = "bbl_standardized", relationship = "many-to-one") |>
  filter(borocd %in% standard_cd$borocd) |>
  count(project_id, borocd, name = "matched_bbl_count_in_cd") |>
  group_by(project_id) |>
  mutate(
    matched_bbl_count_total = sum(matched_bbl_count_in_cd),
    assignment_weight = matched_bbl_count_in_cd / matched_bbl_count_total
  ) |>
  ungroup()

if (nrow(bbl_cd_weights) != nrow(distinct(bbl_cd_weights, project_id, borocd))) {
  stop("BBL-CD weights are not unique by project_id and borocd.")
}

bbl_project_cd <- bbl_cd_weights |>
  left_join(project_base, by = "project_id", relationship = "many-to-one") |>
  filter(!is.na(cert_year)) |>
  make_project_outcome_rows("bbl_fractional_current_mappluto")

make_cd_year_panel <- function(project_cd_df, assignment_type_value) {
  observed_counts <- project_cd_df |>
    group_by(borocd, year = cert_year, outcome_name) |>
    summarise(
      project_count_observed = sum(assignment_weight),
      distinct_project_count_observed = n_distinct(project_id),
      .groups = "drop"
    )

  expand_grid(
    cd_denoms,
    year = 1976:2025,
    outcome_dictionary
  ) |>
    mutate(
      period = assign_period(year),
      assignment_type = assignment_type_value
    ) |>
    left_join(outcome_usability, by = "period", relationship = "many-to-one") |>
    left_join(observed_counts, by = c("borocd", "year", "outcome_name"), relationship = "one-to-one") |>
    mutate(
      project_count_observed = coalesce(project_count_observed, 0),
      distinct_project_count_observed = coalesce(distinct_project_count_observed, 0L),
      support_problem = case_when(
        usability_application_count == "not_recommended" ~ "not_recommended_application_count",
        requires_action_split & usability_action_category_split == "not_recommended" ~ "not_recommended_action_category_split",
        assignment_type == "bbl_fractional_current_mappluto" & usability_bbl_fractional_geography == "not_recommended" ~ "not_recommended_bbl_fractional_geography",
        TRUE ~ NA_character_
      ),
      analysis_usability = case_when(
        !is.na(support_problem) ~ "not_recommended",
        usability_application_count == "limited" ~ "limited",
        requires_action_split & usability_action_category_split == "limited" ~ "limited",
        assignment_type == "bbl_fractional_current_mappluto" & usability_bbl_fractional_geography == "limited" ~ "limited",
        TRUE ~ "usable"
      ),
      project_count = if_else(analysis_usability == "not_recommended", NA_real_, project_count_observed),
      rate_per_10000_occupied_units_1990 = 10000 * project_count / occupied_units_1990,
      rate_per_residential_acre = project_count / residential_acres
    ) |>
    arrange(outcome_name, year, borocd)
}

primary_cd_year_panel <- make_cd_year_panel(primary_project_cd, "primary_zap_cd")
bbl_cd_year_panel <- make_cd_year_panel(bbl_project_cd, "bbl_fractional_current_mappluto")

make_mature_status_panel <- function(project_cd_df, assignment_type_value) {
  observed_status <- project_cd_df |>
    filter(cert_year <= 2015) |>
    group_by(borocd, year = cert_year, outcome_name, status_simple) |>
    summarise(
      status_project_count_observed = sum(assignment_weight),
      distinct_status_project_count_observed = n_distinct(project_id),
      .groups = "drop"
    )

  expand_grid(
    cd_denoms,
    year = 1976:2015,
    outcome_dictionary,
    status_simple = c("completed", "withdrawn_terminated", "unresolved_or_in_process", "other_closed", "missing_status")
  ) |>
    mutate(
      period = assign_period(year),
      assignment_type = assignment_type_value
    ) |>
    left_join(outcome_usability, by = "period", relationship = "many-to-one") |>
    left_join(observed_status, by = c("borocd", "year", "outcome_name", "status_simple"), relationship = "one-to-one") |>
    mutate(
      status_project_count_observed = coalesce(status_project_count_observed, 0),
      distinct_status_project_count_observed = coalesce(distinct_status_project_count_observed, 0L),
      support_problem = case_when(
        usability_application_count == "not_recommended" ~ "not_recommended_application_count",
        usability_status_outcome == "not_recommended" ~ "not_recommended_status_outcome",
        requires_action_split & usability_action_category_split == "not_recommended" ~ "not_recommended_action_category_split",
        assignment_type == "bbl_fractional_current_mappluto" & usability_bbl_fractional_geography == "not_recommended" ~ "not_recommended_bbl_fractional_geography",
        TRUE ~ NA_character_
      ),
      analysis_usability = case_when(
        !is.na(support_problem) ~ "not_recommended",
        usability_application_count == "limited" | usability_status_outcome == "limited" ~ "limited",
        requires_action_split & usability_action_category_split == "limited" ~ "limited",
        assignment_type == "bbl_fractional_current_mappluto" & usability_bbl_fractional_geography == "limited" ~ "limited",
        TRUE ~ "usable"
      ),
      status_project_count = if_else(analysis_usability == "not_recommended", NA_real_, status_project_count_observed),
      status_rate_per_10000_occupied_units_1990 = 10000 * status_project_count / occupied_units_1990,
      status_rate_per_residential_acre = status_project_count / residential_acres
    ) |>
    group_by(assignment_type, borocd, year, outcome_name) |>
    mutate(
      total_project_count = sum(status_project_count, na.rm = TRUE),
      status_share = if_else(total_project_count > 0, status_project_count / total_project_count, NA_real_)
    ) |>
    ungroup() |>
    arrange(outcome_name, year, borocd, status_simple)
}

primary_mature_status_panel <- make_mature_status_panel(primary_project_cd, "primary_zap_cd")
bbl_mature_status_panel <- make_mature_status_panel(bbl_project_cd, "bbl_fractional_current_mappluto")

pre_2016_base <- project_base |>
  filter(cert_year <= 2015)

qc_df <- bind_rows(
  tibble(
    metric = "source_audit_fail_count",
    value = as.character(audit_fail_count),
    status = if_else(audit_fail_count == 0, "pass", "fail"),
    note = "The conservative source audit must pass before this rebuild runs."
  ),
  tibble(
    metric = "project_base_duplicate_id_count",
    value = as.character(nrow(project_base) - n_distinct(project_base$project_id)),
    status = if_else(nrow(project_base) == n_distinct(project_base$project_id), "pass", "fail"),
    note = "Audited project base should be unique by project_id."
  ),
  tibble(
    metric = "project_base_row_count",
    value = as.character(nrow(project_base)),
    status = if_else(nrow(project_base) > 0, "pass", "fail"),
    note = "ULURP project rows in 1976-2025 with raw fields and construction flags."
  ),
  tibble(
    metric = "primary_project_cd_duplicate_key_count",
    value = as.character(nrow(primary_project_cd) - nrow(distinct(primary_project_cd, project_id, outcome_name, borocd))),
    status = if_else(nrow(primary_project_cd) == nrow(distinct(primary_project_cd, project_id, outcome_name, borocd)), "pass", "fail"),
    note = "Primary project-CD rows should be unique by project, outcome, and CD."
  ),
  tibble(
    metric = "bbl_project_cd_duplicate_key_count",
    value = as.character(nrow(bbl_project_cd) - nrow(distinct(bbl_project_cd, project_id, outcome_name, borocd))),
    status = if_else(nrow(bbl_project_cd) == nrow(distinct(bbl_project_cd, project_id, outcome_name, borocd)), "pass", "fail"),
    note = "BBL-fractional project-CD rows should be unique by project, outcome, and CD."
  ),
  tibble(
    metric = "bbl_weight_sum_bad_project_count",
    value = as.character(
      bbl_cd_weights |>
        group_by(project_id) |>
        summarise(weight_sum = sum(assignment_weight), .groups = "drop") |>
        filter(abs(weight_sum - 1) > 1e-8) |>
        nrow()
    ),
    status = if_else(
      bbl_cd_weights |>
        group_by(project_id) |>
        summarise(weight_sum = sum(assignment_weight), .groups = "drop") |>
        filter(abs(weight_sum - 1) > 1e-8) |>
        nrow() == 0,
      "pass",
      "fail"
    ),
    note = "Fractional BBL weights should sum to one among matched standard-CD projects."
  ),
  tibble(
    metric = "pre_2016_rezoning_special_ulurp_recovered_count",
    value = as.character(sum(pre_2016_base$rezoning_special_ulurp_numbers_flag, na.rm = TRUE)),
    status = if_else(sum(pre_2016_base$rezoning_special_ulurp_numbers_flag, na.rm = TRUE) > 0, "pass", "fail"),
    note = "Historical rezoning/special-permit recovery must come from ULURP numbers rather than blank actions."
  ),
  tibble(
    metric = "pre_2016_public_land_ulurp_recovered_count",
    value = as.character(sum(pre_2016_base$public_land_disposition_ulurp_numbers_flag, na.rm = TRUE)),
    status = if_else(sum(pre_2016_base$public_land_disposition_ulurp_numbers_flag, na.rm = TRUE) > 0, "pass", "fail"),
    note = "Historical public-land recovery must come from ULURP numbers rather than blank actions."
  ),
  tibble(
    metric = "primary_panel_cd_count",
    value = as.character(n_distinct(primary_cd_year_panel$borocd)),
    status = if_else(n_distinct(primary_cd_year_panel$borocd) == 59, "pass", "fail"),
    note = "Primary CD-year panel should cover the 59 standard CDs."
  ),
  tibble(
    metric = "primary_panel_year_range",
    value = str_c(min(primary_cd_year_panel$year), "-", max(primary_cd_year_panel$year)),
    status = if_else(min(primary_cd_year_panel$year) == 1976 & max(primary_cd_year_panel$year) == 2025, "pass", "fail"),
    note = "Primary CD-year panel support window."
  ),
  tibble(
    metric = "bbl_panel_not_recommended_unmasked_count",
    value = as.character(sum(bbl_cd_year_panel$analysis_usability == "not_recommended" & !is.na(bbl_cd_year_panel$project_count))),
    status = if_else(sum(bbl_cd_year_panel$analysis_usability == "not_recommended" & !is.na(bbl_cd_year_panel$project_count)) == 0, "pass", "fail"),
    note = "Unsupported BBL-fractional period/outcome cells must be masked, not filled as zeros."
  ),
  tibble(
    metric = "primary_panel_not_recommended_unmasked_count",
    value = as.character(sum(primary_cd_year_panel$analysis_usability == "not_recommended" & !is.na(primary_cd_year_panel$project_count))),
    status = if_else(sum(primary_cd_year_panel$analysis_usability == "not_recommended" & !is.na(primary_cd_year_panel$project_count)) == 0, "pass", "fail"),
    note = "Unsupported primary period/outcome cells must be masked if any exist."
  ),
  tibble(
    metric = "negative_count_or_rate_cell_count",
    value = as.character(
      sum(primary_cd_year_panel$project_count_observed < 0, na.rm = TRUE) +
        sum(bbl_cd_year_panel$project_count_observed < 0, na.rm = TRUE) +
        sum(primary_cd_year_panel$rate_per_10000_occupied_units_1990 < 0, na.rm = TRUE) +
        sum(bbl_cd_year_panel$rate_per_10000_occupied_units_1990 < 0, na.rm = TRUE)
    ),
    status = if_else(
      sum(primary_cd_year_panel$project_count_observed < 0, na.rm = TRUE) +
        sum(bbl_cd_year_panel$project_count_observed < 0, na.rm = TRUE) +
        sum(primary_cd_year_panel$rate_per_10000_occupied_units_1990 < 0, na.rm = TRUE) +
        sum(bbl_cd_year_panel$rate_per_10000_occupied_units_1990 < 0, na.rm = TRUE) == 0,
      "pass",
      "fail"
    ),
    note = "Counts and rates should be nonnegative."
  ),
  tibble(
    metric = "approval_timing_output_created",
    value = "0",
    status = "pass",
    note = "Approval-delay outputs are intentionally not constructed because audit marks historical approval timing as unsupported."
  )
)

write_csv(project_base, "../output/zap_housing_project_base_audited.csv", na = "")
write_csv(primary_project_cd, "../output/zap_housing_project_cd_primary.csv", na = "")
write_csv(bbl_project_cd, "../output/zap_housing_project_cd_bbl_fractional.csv", na = "")
write_csv(primary_cd_year_panel, "../output/zap_housing_cd_year_panel_primary.csv", na = "")
write_csv(bbl_cd_year_panel, "../output/zap_housing_cd_year_panel_bbl_fractional.csv", na = "")
write_csv(primary_mature_status_panel, "../output/zap_housing_mature_status_panel_primary.csv", na = "")
write_csv(bbl_mature_status_panel, "../output/zap_housing_mature_status_panel_bbl_fractional.csv", na = "")

if (any(qc_df$status == "fail")) {
  stop("ZAP housing pipeline construction checks failed.")
}
