# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_zap_housing_pipeline_from_raw/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(lubridate)
  library(readr)
  library(stringr)
})

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

if (sum(zap_audit_qc$status == "fail", na.rm = TRUE) > 0) {
  stop("Source integrity audit has failing hard checks; inspect ../input/zap_source_integrity_qc.csv")
}

standard_cd <- read_csv("../input/cd_homeownership_1990_measure.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = suppressWarnings(as.integer(borocd)),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name,
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro))
  ) |>
  arrange(borocd)

if (nrow(standard_cd) != n_distinct(standard_cd$borocd)) {
  stop("Homeownership measure is not unique by borocd.")
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

if (nrow(project_base) == 0) {
  stop("Audited ZAP housing project base has no rows.")
}

if (nrow(project_base) != n_distinct(project_base$project_id)) {
  stop("Audited ZAP housing project base is not unique by project_id.")
}

write_csv(project_base, "../output/zap_housing_project_base_audited.csv", na = "")
