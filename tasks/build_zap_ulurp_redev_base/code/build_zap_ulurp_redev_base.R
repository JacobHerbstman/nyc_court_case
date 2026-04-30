# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_zap_ulurp_redev_base/code")
# zap_project_parquet <- "../input/zap_project_data.parquet"
# zap_housing_cohort_base_csv <- "../input/zap_housing_cohort_base.csv"
# zap_housing_hdb_project_summary_csv <- "../input/zap_housing_hdb_project_summary.csv"
# cd_redevelopment_potential_baseline_csv <- "../input/cd_redevelopment_potential_baseline.csv"
# out_base_csv <- "../output/zap_ulurp_redev_project_base.csv"
# out_qc_csv <- "../output/zap_ulurp_redev_project_base_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 6) {
  stop("Expected 6 arguments: zap_project_parquet zap_housing_cohort_base_csv zap_housing_hdb_project_summary_csv cd_redevelopment_potential_baseline_csv out_base_csv out_qc_csv")
}

zap_project_parquet <- args[1]
zap_housing_cohort_base_csv <- args[2]
zap_housing_hdb_project_summary_csv <- args[3]
cd_redevelopment_potential_baseline_csv <- args[4]
out_base_csv <- args[5]
out_qc_csv <- args[6]

has_action_code <- function(actions_string, action_code) {
  str_detect(str_to_upper(coalesce(actions_string, "")), paste0("\\b", action_code, "\\b"))
}

assert_unique_keys <- function(df, key_cols, df_name) {
  duplicate_keys <- df %>%
    count(across(all_of(key_cols)), name = "source_row_count") %>%
    filter(source_row_count > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(df_name, " is not unique by ", paste(key_cols, collapse = ", "), ".")
  }
}

pretrend_control_cols <- c(
  "total_housing_units_growth_1980_1990_approx",
  "occupied_units_growth_1980_1990_approx",
  "vacancy_rate_change_1980_1990_pp_approx",
  "homeowner_share_change_1980_1990_pp_approx"
)

exact_control_cols <- c(
  "vacancy_rate_1990_exact",
  "structure_share_1_2_units_1990_exact",
  "structure_share_3_4_units_1990_exact",
  "structure_share_5_plus_units_1990_exact",
  "median_household_income_1990_1999_dollars_exact",
  "poverty_share_1990_exact",
  "median_housing_value_1990_2000_dollars_exact_filled",
  "foreign_born_share_1990_exact",
  "college_graduate_share_1990_exact",
  "unemployment_rate_1990_exact",
  "subway_commute_share_1990_exact",
  "mean_commute_time_1990_minutes_exact"
)

built_form_control_cols <- c(
  "cd_mean_built_far_lot_weighted",
  "cd_mean_max_resid_far_lot_weighted",
  "cd_share_lot_area_one_two_family",
  "cd_share_lot_area_vacant",
  "cd_share_lot_area_old_building",
  "cd_share_lot_area_protected",
  "cd_share_lot_area_parking_or_low_intensity"
)

redev_df <- read_csv(cd_redevelopment_potential_baseline_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  select(
    borocd,
    borough_name,
    occupied_units_1990,
    residential_acres,
    treat_pp,
    treat_z_boro,
    redev_potential_A_z_boro,
    redev_potential_C_z_boro,
    all_of(pretrend_control_cols),
    all_of(exact_control_cols),
    all_of(built_form_control_cols)
  ) %>%
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    borough_name = as.character(borough_name),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990)),
    residential_acres = suppressWarnings(as.numeric(residential_acres)),
    treat_pp = suppressWarnings(as.numeric(treat_pp)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro)),
    redev_potential_A_z_boro = suppressWarnings(as.numeric(redev_potential_A_z_boro)),
    redev_potential_C_z_boro = suppressWarnings(as.numeric(redev_potential_C_z_boro))
  ) %>%
  group_by(borough_name) %>%
  mutate(
    high_homeowner = treat_z_boro >= median(treat_z_boro, na.rm = TRUE),
    high_redev_A = redev_potential_A_z_boro >= median(redev_potential_A_z_boro, na.rm = TRUE),
    two_by_two_cell_A = case_when(
      is.na(high_homeowner) | is.na(high_redev_A) ~ NA_character_,
      !high_homeowner & !high_redev_A ~ "LL",
      !high_homeowner & high_redev_A ~ "LH",
      high_homeowner & !high_redev_A ~ "HL",
      high_homeowner & high_redev_A ~ "HH"
    ),
    two_by_two_label_A = case_when(
      is.na(two_by_two_cell_A) ~ NA_character_,
      two_by_two_cell_A == "LL" ~ "Low homeowner / Low redev",
      two_by_two_cell_A == "LH" ~ "Low homeowner / High redev",
      two_by_two_cell_A == "HL" ~ "High homeowner / Low redev",
      two_by_two_cell_A == "HH" ~ "High homeowner / High redev"
    )
  ) %>%
  ungroup()

assert_unique_keys(redev_df, c("borocd", "borough_name"), "Redevelopment baseline input")

cohort_base_df <- read_csv(zap_housing_cohort_base_csv, show_col_types = FALSE, na = c("", "NA")) %>%
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
  cohort_base_df <- cohort_base_df %>%
    rename(cohort_actions = actions)
} else {
  cohort_base_df <- cohort_base_df %>%
    mutate(cohort_actions = NA_character_)
}

assert_unique_keys(cohort_base_df, "project_id", "ZAP housing cohort base input")

zap_raw_df <- read_parquet(
  zap_project_parquet,
  col_select = c(
    "project_id", "applicant_type", "primary_applicant", "ceqr_leadagency",
    "council_district_first", "current_milestone", "current_milestone_date_parsed",
    "approval_date_parsed", "completed_date_parsed", "ulurp_numbers", "actions"
  )
) %>%
  as.data.frame() %>%
  as_tibble() %>%
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

hdb_link_df <- read_csv(zap_housing_hdb_project_summary_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    project_id = as.character(project_id),
    across(matches("(^has_|_rate_|_units_|_count_|permit_year|permit_lag)"), ~ suppressWarnings(as.numeric(.x))),
    across(starts_with("has_"), ~ as.logical(.x))
  ) %>%
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

base_df <- cohort_base_df %>%
  select(-any_of(c("treat_pp", "treat_z_boro", exact_control_cols, pretrend_control_cols, built_form_control_cols))) %>%
  left_join(zap_raw_df, by = "project_id", relationship = "one-to-one") %>%
  left_join(hdb_link_df, by = "project_id", relationship = "one-to-one") %>%
  left_join(redev_df, by = c("borocd", "borough_name"), relationship = "many-to-one") %>%
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
  ) %>%
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
    occupied_units_1990,
    residential_acres,
    treat_pp,
    treat_z_boro,
    redev_potential_A_z_boro,
    redev_potential_C_z_boro,
    high_homeowner,
    high_redev_A,
    two_by_two_cell_A,
    two_by_two_label_A,
    all_of(pretrend_control_cols),
    all_of(exact_control_cols),
    all_of(built_form_control_cols)
  ) %>%
  arrange(cert_year, borocd, project_id)

qc_df <- bind_rows(
  tibble(metric = "row_count", value = nrow(base_df), note = "Project rows in the ZAP ULURP redevelopment base."),
  tibble(metric = "distinct_project_id_count", value = n_distinct(base_df$project_id), note = "Distinct project IDs in the mechanism base."),
  tibble(metric = "distinct_borocd_count", value = n_distinct(base_df$borocd), note = "Should equal the 59 standard community districts."),
  tibble(metric = "missing_treat_z_boro_row_count", value = sum(is.na(base_df$treat_z_boro)), note = "Should be zero after redevelopment merge."),
  tibble(metric = "missing_redev_A_row_count", value = sum(is.na(base_df$redev_potential_A_z_boro)), note = "Should be zero after redevelopment merge."),
  tibble(metric = "missing_two_by_two_cell_A_row_count", value = sum(is.na(base_df$two_by_two_cell_A)), note = "Rows missing the homeowner/redevelopment two-by-two classification."),
  tibble(metric = "missing_applicant_type_row_count", value = sum(is.na(base_df$applicant_type) | base_df$applicant_type == ""), note = "Rows missing applicant type in staged ZAP."),
  tibble(metric = "private_applicant_project_count", value = sum(base_df$private_applicant, na.rm = TRUE), note = "Projects classified as private applicants."),
  tibble(metric = "public_applicant_project_count", value = sum(base_df$public_applicant, na.rm = TRUE), note = "Projects classified as public applicants."),
  tibble(metric = "private_plus_public_exceeds_total_flag", value = as.integer(sum(base_df$private_applicant, na.rm = TRUE) + sum(base_df$public_applicant, na.rm = TRUE) > nrow(base_df)), note = "Should be zero."),
  tibble(metric = "rezoning_or_special_project_count", value = sum(base_df$rezoning_or_special_proxy, na.rm = TRUE), note = "Projects with ZM/ZR/ZS actions."),
  tibble(metric = "public_land_or_disposition_project_count", value = sum(base_df$public_land_or_disposition_proxy, na.rm = TRUE), note = "Projects with HA/PP/PQ/MM actions."),
  tibble(metric = "mixed_private_rezoning_project_count", value = sum(base_df$mixed_private_rezoning_proxy, na.rm = TRUE), note = "Private projects with rezoning/special actions."),
  tibble(metric = "public_hpd_project_count", value = sum(base_df$public_hpd_proxy, na.rm = TRUE), note = "Public projects with HPD proxy."),
  tibble(metric = "high_homeowner_high_redev_project_count", value = sum(base_df$two_by_two_cell_A == "HH", na.rm = TRUE), note = "Projects in the high-homeowner/high-redevelopment cell.")
)

write_csv_if_changed(base_df, out_base_csv)
write_csv_if_changed(qc_df, out_qc_csv)

cat("Wrote ZAP ULURP redevelopment base outputs to", dirname(out_base_csv), "\n")
