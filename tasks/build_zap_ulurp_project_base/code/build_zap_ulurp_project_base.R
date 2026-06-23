# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_zap_ulurp_project_base/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

has_action_code <- function(actions_string, action_code) {
  str_detect(str_to_upper(coalesce(actions_string, "")), paste0("\\b", action_code, "\\b"))
}

assert_unique_keys <- function(df, key_cols, df_name) {
  duplicate_keys <- df |>
    count(across(all_of(key_cols)), name = "source_row_count") |>
    filter(source_row_count > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(df_name, " is not unique by ", paste(key_cols, collapse = ", "), ".")
  }
}

cohort_base_df <- read_csv("../input/zap_housing_cohort_base.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    project_id = as.character(project_id),
    borocd = suppressWarnings(as.integer(borocd)),
    cert_year = suppressWarnings(as.integer(cert_year)),
    bbl_count = suppressWarnings(as.integer(bbl_count)),
    has_bbl = as.logical(has_bbl),
    is_complete = as.logical(is_complete),
    is_fail = as.logical(is_fail),
    is_unresolved = as.logical(is_unresolved)
  )

if ("actions" %in% names(cohort_base_df)) {
  cohort_base_df <- cohort_base_df |>
    rename(cohort_actions = actions)
} else {
  cohort_base_df <- cohort_base_df |>
    mutate(cohort_actions = NA_character_)
}

assert_unique_keys(cohort_base_df, "project_id", "ZAP housing cohort base input")

zap_raw_df <- read_parquet(
  "../input/zap_project_data.parquet",
  col_select = c(
    "project_id", "applicant_type", "primary_applicant", "ceqr_leadagency",
    "council_district_first", "current_milestone", "current_milestone_date_parsed",
    "approval_date_parsed", "completed_date_parsed", "ulurp_numbers", "actions"
  )
) |>
  as.data.frame() |>
  as_tibble() |>
  transmute(
    project_id = as.character(project_id),
    applicant_type = as.character(applicant_type),
    primary_applicant = as.character(primary_applicant),
    ceqr_leadagency = as.character(ceqr_leadagency),
    council_district_first = suppressWarnings(as.integer(council_district_first)),
    current_milestone = as.character(current_milestone),
    current_milestone_date = as.Date(current_milestone_date_parsed),
    approval_date = as.Date(approval_date_parsed),
    completed_date = as.Date(completed_date_parsed),
    ulurp_numbers = as.character(ulurp_numbers),
    zap_actions = as.character(actions)
  )

assert_unique_keys(zap_raw_df, "project_id", "Staged ZAP project input")

hdb_link_df <- read_csv("../input/zap_housing_hdb_project_summary.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    project_id = as.character(project_id),
    across(matches("(^has_|_rate_|_units_|_count_|permit_year|permit_lag)"), ~ suppressWarnings(as.numeric(.x))),
    across(starts_with("has_"), ~ as.logical(.x))
  ) |>
  select(
    project_id,
    matched_bbl_count,
    has_any_hdb_match_exact_bbl,
    has_any_housing_job_exact_bbl,
    has_any_housing_job_0_5,
    has_any_housing_job_0_10,
    has_any_housing_job_neg2_10,
    has_any_housing_job_neg5_15,
    has_any_addition_job_0_10,
    has_any_nb_job_0_10,
    has_any_nb_50_plus_job_0_10,
    linked_housing_job_count_0_10,
    linked_addition_job_count_0_10,
    linked_nb_job_count_0_10,
    linked_nb_gross_units_0_10,
    linked_gross_add_units_0_10,
    linked_gross_loss_units_0_10,
    linked_net_units_0_10,
    first_housing_permit_year_0_10,
    first_housing_permit_lag_0_10
  )

assert_unique_keys(hdb_link_df, "project_id", "ZAP-HDB project summary input")

base_df <- cohort_base_df |>
  left_join(zap_raw_df, by = "project_id", relationship = "one-to-one") |>
  left_join(hdb_link_df, by = "project_id", relationship = "one-to-one") |>
  mutate(
    applicant_type = str_squish(applicant_type),
    primary_applicant = str_squish(primary_applicant),
    ceqr_leadagency = str_squish(ceqr_leadagency),
    actions = coalesce(cohort_actions, zap_actions),
    actions = str_squish(actions),
    private_applicant = applicant_type == "Private",
    public_applicant = applicant_type %in% c("Other Public Agency", "DCP"),
    hpd_led_proxy = str_detect(str_to_upper(str_squish(paste(coalesce(primary_applicant, ""), coalesce(ceqr_leadagency, "")))), "\\bHPD\\b") | has_action_code(actions, "HA"),
    rezoning_or_special_proxy = has_action_code(actions, "ZM") | has_action_code(actions, "ZR") | has_action_code(actions, "ZS"),
    public_land_or_disposition_proxy = has_action_code(actions, "HA") | has_action_code(actions, "PP") | has_action_code(actions, "PQ") | has_action_code(actions, "MM"),
    mixed_private_rezoning_proxy = private_applicant & rezoning_or_special_proxy,
    public_hpd_proxy = public_applicant & hpd_led_proxy,
    matched_bbl_count = coalesce(matched_bbl_count, 0),
    has_any_hdb_match_exact_bbl = coalesce(has_any_hdb_match_exact_bbl, FALSE),
    has_any_housing_job_exact_bbl = coalesce(has_any_housing_job_exact_bbl, FALSE),
    has_any_housing_job_0_5 = coalesce(has_any_housing_job_0_5, FALSE),
    has_any_housing_job_0_10 = coalesce(has_any_housing_job_0_10, FALSE),
    has_any_housing_job_neg2_10 = coalesce(has_any_housing_job_neg2_10, FALSE),
    has_any_housing_job_neg5_15 = coalesce(has_any_housing_job_neg5_15, FALSE),
    has_any_addition_job_0_10 = coalesce(has_any_addition_job_0_10, FALSE),
    has_any_nb_job_0_10 = coalesce(has_any_nb_job_0_10, FALSE),
    has_any_nb_50_plus_job_0_10 = coalesce(has_any_nb_50_plus_job_0_10, FALSE),
    linked_housing_job_count_0_10 = coalesce(linked_housing_job_count_0_10, 0),
    linked_addition_job_count_0_10 = coalesce(linked_addition_job_count_0_10, 0),
    linked_nb_job_count_0_10 = coalesce(linked_nb_job_count_0_10, 0),
    linked_nb_gross_units_0_10 = coalesce(linked_nb_gross_units_0_10, 0),
    linked_gross_add_units_0_10 = coalesce(linked_gross_add_units_0_10, 0),
    linked_gross_loss_units_0_10 = coalesce(linked_gross_loss_units_0_10, 0),
    linked_net_units_0_10 = coalesce(linked_net_units_0_10, 0)
  ) |>
  select(
    project_id,
    project_name,
    project_brief,
    borocd,
    borough_name,
    cert_year,
    cert_era,
    certified_referred_date,
    approval_date,
    completed_date,
    council_district_first,
    ulurp_numbers,
    current_milestone,
    current_milestone_date,
    project_status,
    public_status,
    applicant_type,
    primary_applicant,
    ceqr_leadagency,
    actions,
    private_applicant,
    public_applicant,
    hpd_led_proxy,
    rezoning_or_special_proxy,
    public_land_or_disposition_proxy,
    mixed_private_rezoning_proxy,
    public_hpd_proxy,
    is_complete,
    is_fail,
    is_unresolved,
    has_bbl,
    bbl_count,
    matched_bbl_count,
    has_any_hdb_match_exact_bbl,
    has_any_housing_job_exact_bbl,
    has_any_housing_job_0_5,
    has_any_housing_job_0_10,
    has_any_housing_job_neg2_10,
    has_any_housing_job_neg5_15,
    has_any_addition_job_0_10,
    has_any_nb_job_0_10,
    has_any_nb_50_plus_job_0_10,
    linked_housing_job_count_0_10,
    linked_addition_job_count_0_10,
    linked_nb_job_count_0_10,
    linked_nb_gross_units_0_10,
    linked_gross_add_units_0_10,
    linked_gross_loss_units_0_10,
    linked_net_units_0_10,
    first_housing_permit_year_0_10,
    first_housing_permit_lag_0_10,
    treat_pp,
    treat_z_boro
  ) |>
  arrange(cert_year, borocd, project_id)

write_csv_if_changed(base_df, "../output/zap_ulurp_project_base.csv")

cat("Wrote ZAP ULURP project base to ../output\n")
