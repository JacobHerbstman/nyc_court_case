suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tidyr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

summary_era_from_year <- function(x) {
  case_when(
    x >= 1976 & x <= 1979 ~ "1976-1979",
    x >= 1980 & x <= 1984 ~ "1980-1984",
    x >= 1985 & x <= 1989 ~ "1985-1989",
    x >= 1990 & x <= 1999 ~ "1990-1999",
    x >= 2000 & x <= 2009 ~ "2000-2009",
    x >= 2010 & x <= 2019 ~ "2010-2019",
    x >= 2020 & x <= 2025 ~ "2020-2025",
    TRUE ~ NA_character_
  )
}

yield_era_from_year <- function(x) {
  case_when(
    x >= 2010 & x <= 2015 ~ "2010-2015",
    x >= 2016 & x <= 2020 ~ "2016-2020",
    TRUE ~ NA_character_
  )
}

mature_era_from_year <- function(x) {
  case_when(
    x >= 1976 & x <= 1979 ~ "1976-1979",
    x >= 1980 & x <= 1984 ~ "1980-1984",
    x >= 1985 & x <= 1989 ~ "1985-1989",
    x >= 1990 & x <= 1999 ~ "1990-1999",
    x >= 2000 & x <= 2009 ~ "2000-2009",
    x >= 2010 & x <= 2015 ~ "2010-2015",
    TRUE ~ NA_character_
  )
}

assert_unique_keys <- function(df, keys, label) {
  duplicate_keys <- df %>%
    count(across(all_of(keys)), name = "n") %>%
    filter(n > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(label, " is not unique by ", paste(keys, collapse = ", "), ".")
  }
}

project_base_df <- read_csv("../input/zap_ulurp_redev_project_base.csv", show_col_types = FALSE, na = c("", "NA"), guess_max = Inf) %>%
  mutate(
    project_id = as.character(project_id),
    borocd = suppressWarnings(as.integer(borocd)),
    cert_year = suppressWarnings(as.integer(cert_year)),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990)),
    residential_acres = suppressWarnings(as.numeric(residential_acres))
  )

assert_unique_keys(project_base_df, "project_id", "ZAP ULURP redevelopment project base")

district_lookup <- project_base_df %>%
  distinct(
    borocd,
    borough_name,
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
    total_housing_units_growth_1980_1990_approx,
    occupied_units_growth_1980_1990_approx,
    vacancy_rate_change_1980_1990_pp_approx,
    homeowner_share_change_1980_1990_pp_approx,
    vacancy_rate_1990_exact,
    structure_share_1_2_units_1990_exact,
    structure_share_3_4_units_1990_exact,
    structure_share_5_plus_units_1990_exact,
    median_household_income_1990_1999_dollars_exact,
    poverty_share_1990_exact,
    median_housing_value_1990_2000_dollars_exact_filled,
    foreign_born_share_1990_exact,
    college_graduate_share_1990_exact,
    unemployment_rate_1990_exact,
    subway_commute_share_1990_exact,
    mean_commute_time_1990_minutes_exact,
    cd_mean_built_far_lot_weighted,
    cd_mean_max_resid_far_lot_weighted,
    cd_share_lot_area_one_two_family,
    cd_share_lot_area_vacant,
    cd_share_lot_area_old_building,
    cd_share_lot_area_protected,
    cd_share_lot_area_parking_or_low_intensity
  ) %>%
  arrange(borocd)

assert_unique_keys(district_lookup, "borocd", "ZAP ULURP redevelopment district lookup")

if (n_distinct(district_lookup$borocd) != 59) {
  stop("Expected the ZAP ULURP redevelopment district lookup to cover 59 community districts.")
}

apps_counts <- project_base_df %>%
  filter(cert_year >= 1976, cert_year <= 2025) %>%
  group_by(borocd, cert_year) %>%
  summarise(
    initial_apps = n(),
    private_initial_apps = sum(private_applicant, na.rm = TRUE),
    public_initial_apps = sum(public_applicant, na.rm = TRUE),
    mixed_private_rezoning_apps = sum(mixed_private_rezoning_proxy, na.rm = TRUE),
    public_hpd_apps = sum(public_hpd_proxy, na.rm = TRUE),
    rezoning_or_special_apps = sum(rezoning_or_special_proxy, na.rm = TRUE),
    public_land_or_disposition_apps = sum(public_land_or_disposition_proxy, na.rm = TRUE),
    .groups = "drop"
  )

assert_unique_keys(apps_counts, c("borocd", "cert_year"), "ZAP ULURP redevelopment application counts")

cd_year_panel <- crossing(
  borocd = district_lookup$borocd,
  cert_year = 1976:2025
) %>%
  left_join(district_lookup, by = "borocd", relationship = "many-to-one") %>%
  left_join(apps_counts, by = c("borocd", "cert_year"), relationship = "many-to-one") %>%
  mutate(
    across(c(initial_apps, private_initial_apps, public_initial_apps, mixed_private_rezoning_apps, public_hpd_apps, rezoning_or_special_apps, public_land_or_disposition_apps), ~ coalesce(.x, 0L)),
    era = summary_era_from_year(cert_year),
    initial_apps_per_10k = 10000 * initial_apps / occupied_units_1990,
    private_initial_apps_per_10k = 10000 * private_initial_apps / occupied_units_1990,
    public_initial_apps_per_10k = 10000 * public_initial_apps / occupied_units_1990,
    mixed_private_rezoning_apps_per_10k = 10000 * mixed_private_rezoning_apps / occupied_units_1990,
    public_hpd_apps_per_10k = 10000 * public_hpd_apps / occupied_units_1990,
    initial_apps_per_res_acre = initial_apps / residential_acres,
    private_initial_apps_per_res_acre = private_initial_apps / residential_acres,
    public_hpd_apps_per_res_acre = public_hpd_apps / residential_acres
  ) %>%
  group_by(borough_name, cert_year) %>%
  mutate(
    borough_initial_apps_total = sum(initial_apps, na.rm = TRUE),
    borough_initial_apps_share = ifelse(borough_initial_apps_total > 0, initial_apps / borough_initial_apps_total, NA_real_)
  ) %>%
  ungroup() %>%
  arrange(cert_year, borocd)

mature_counts <- project_base_df %>%
  filter(cert_year >= 1976, cert_year <= 2015) %>%
  group_by(borocd, cert_year) %>%
  summarise(
    initial_apps = n(),
    complete_apps = sum(is_complete, na.rm = TRUE),
    failed_apps = sum(is_fail, na.rm = TRUE),
    unresolved_apps = sum(is_unresolved, na.rm = TRUE),
    private_initial_apps = sum(private_applicant, na.rm = TRUE),
    private_complete_apps = sum(private_applicant & is_complete, na.rm = TRUE),
    private_failed_apps = sum(private_applicant & is_fail, na.rm = TRUE),
    private_unresolved_apps = sum(private_applicant & is_unresolved, na.rm = TRUE),
    public_initial_apps = sum(public_applicant, na.rm = TRUE),
    public_complete_apps = sum(public_applicant & is_complete, na.rm = TRUE),
    public_failed_apps = sum(public_applicant & is_fail, na.rm = TRUE),
    public_unresolved_apps = sum(public_applicant & is_unresolved, na.rm = TRUE),
    .groups = "drop"
  )

assert_unique_keys(mature_counts, c("borocd", "cert_year"), "ZAP ULURP redevelopment mature counts")

mature_panel <- crossing(
  borocd = district_lookup$borocd,
  cert_year = 1976:2015
) %>%
  left_join(district_lookup, by = "borocd", relationship = "many-to-one") %>%
  left_join(mature_counts, by = c("borocd", "cert_year"), relationship = "many-to-one") %>%
  mutate(
    across(c(initial_apps, complete_apps, failed_apps, unresolved_apps, private_initial_apps, private_complete_apps, private_failed_apps, private_unresolved_apps, public_initial_apps, public_complete_apps, public_failed_apps, public_unresolved_apps), ~ coalesce(.x, 0L)),
    era = mature_era_from_year(cert_year),
    completion_share = ifelse(initial_apps > 0, complete_apps / initial_apps, NA_real_),
    failure_share = ifelse(initial_apps > 0, failed_apps / initial_apps, NA_real_),
    unresolved_share = ifelse(initial_apps > 0, unresolved_apps / initial_apps, NA_real_),
    private_completion_share = ifelse(private_initial_apps > 0, private_complete_apps / private_initial_apps, NA_real_),
    private_failure_share = ifelse(private_initial_apps > 0, private_failed_apps / private_initial_apps, NA_real_),
    public_completion_share = ifelse(public_initial_apps > 0, public_complete_apps / public_initial_apps, NA_real_),
    public_failure_share = ifelse(public_initial_apps > 0, public_failed_apps / public_initial_apps, NA_real_)
  ) %>%
  arrange(cert_year, borocd)

if (any(mature_panel$cert_year > 2015, na.rm = TRUE)) {
  stop("Mature ZAP ULURP redevelopment cohort panel includes certification years after 2015.")
}

if (any(mature_panel$era %in% c("2010-2019", "2016-2020", "2020-2025"), na.rm = TRUE)) {
  stop("Mature ZAP ULURP redevelopment cohort panel includes an immature era label.")
}

candidate_05_source_df <- read_csv("../input/zap_housing_hdb_link_candidates.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    project_id = as.character(project_id),
    job_number = as.character(job_number),
    bbl_standardized = as.character(bbl_standardized),
    within_0_5 = as.logical(within_0_5),
    is_addition_job = as.logical(is_addition_job),
    is_nb_job = as.logical(is_nb_job),
    is_nb_50_plus_job = as.logical(is_nb_50_plus_job),
    gross_add_units = suppressWarnings(as.numeric(gross_add_units)),
    nb_gross_units = suppressWarnings(as.numeric(nb_gross_units))
  )

candidate_05_df <- candidate_05_source_df %>%
  filter(within_0_5, !is.na(job_number)) %>%
  arrange(project_id, job_number, bbl_standardized) %>%
  distinct(project_id, job_number, .keep_all = TRUE) %>%
  group_by(project_id) %>%
  summarise(
    has_any_addition_job_0_5 = any(is_addition_job %in% TRUE, na.rm = TRUE),
    has_any_nb_job_0_5 = any(is_nb_job %in% TRUE, na.rm = TRUE),
    has_any_nb_50_plus_job_0_5 = any(is_nb_50_plus_job %in% TRUE, na.rm = TRUE),
    linked_gross_add_units_0_5 = sum(gross_add_units, na.rm = TRUE),
    linked_nb_gross_units_0_5 = sum(nb_gross_units, na.rm = TRUE),
    .groups = "drop"
  )

assert_unique_keys(candidate_05_df, "project_id", "0-5-year HDB candidate summary")

yield_project_df <- project_base_df %>%
  left_join(candidate_05_df, by = "project_id", relationship = "many-to-one") %>%
  mutate(
    has_any_addition_job_0_5 = coalesce(has_any_addition_job_0_5, FALSE),
    has_any_nb_job_0_5 = coalesce(has_any_nb_job_0_5, FALSE),
    has_any_nb_50_plus_job_0_5 = coalesce(has_any_nb_50_plus_job_0_5, FALSE),
    linked_gross_add_units_0_5 = coalesce(linked_gross_add_units_0_5, 0),
    linked_nb_gross_units_0_5 = coalesce(linked_nb_gross_units_0_5, 0)
  )

yield_counts <- yield_project_df %>%
  filter(cert_year >= 2010, cert_year <= 2020) %>%
  group_by(borocd, cert_year) %>%
  summarise(
    initial_apps = n(),
    linked_addition_projects_0_10 = sum(has_any_addition_job_0_10, na.rm = TRUE),
    linked_nb_projects_0_10 = sum(has_any_nb_job_0_10, na.rm = TRUE),
    linked_nb_50_plus_projects_0_10 = sum(has_any_nb_50_plus_job_0_10, na.rm = TRUE),
    linked_gross_add_units_0_10 = sum(linked_gross_add_units_0_10, na.rm = TRUE),
    private_initial_apps = sum(private_applicant, na.rm = TRUE),
    private_linked_addition_projects_0_10 = sum(private_applicant & has_any_addition_job_0_10, na.rm = TRUE),
    private_linked_nb_projects_0_10 = sum(private_applicant & has_any_nb_job_0_10, na.rm = TRUE),
    private_linked_nb_50_plus_projects_0_10 = sum(private_applicant & has_any_nb_50_plus_job_0_10, na.rm = TRUE),
    private_linked_gross_add_units_0_10 = sum(ifelse(private_applicant, linked_gross_add_units_0_10, 0), na.rm = TRUE),
    public_initial_apps = sum(public_applicant, na.rm = TRUE),
    public_linked_addition_projects_0_10 = sum(public_applicant & has_any_addition_job_0_10, na.rm = TRUE),
    public_linked_nb_projects_0_10 = sum(public_applicant & has_any_nb_job_0_10, na.rm = TRUE),
    public_linked_nb_50_plus_projects_0_10 = sum(public_applicant & has_any_nb_50_plus_job_0_10, na.rm = TRUE),
    public_linked_gross_add_units_0_10 = sum(ifelse(public_applicant, linked_gross_add_units_0_10, 0), na.rm = TRUE),
    linked_addition_projects_0_5 = sum(has_any_addition_job_0_5, na.rm = TRUE),
    linked_nb_projects_0_5 = sum(has_any_nb_job_0_5, na.rm = TRUE),
    linked_nb_50_plus_projects_0_5 = sum(has_any_nb_50_plus_job_0_5, na.rm = TRUE),
    linked_gross_add_units_0_5 = sum(linked_gross_add_units_0_5, na.rm = TRUE),
    private_linked_addition_projects_0_5 = sum(private_applicant & has_any_addition_job_0_5, na.rm = TRUE),
    private_linked_nb_projects_0_5 = sum(private_applicant & has_any_nb_job_0_5, na.rm = TRUE),
    private_linked_nb_50_plus_projects_0_5 = sum(private_applicant & has_any_nb_50_plus_job_0_5, na.rm = TRUE),
    private_linked_gross_add_units_0_5 = sum(ifelse(private_applicant, linked_gross_add_units_0_5, 0), na.rm = TRUE),
    public_linked_addition_projects_0_5 = sum(public_applicant & has_any_addition_job_0_5, na.rm = TRUE),
    public_linked_nb_projects_0_5 = sum(public_applicant & has_any_nb_job_0_5, na.rm = TRUE),
    public_linked_nb_50_plus_projects_0_5 = sum(public_applicant & has_any_nb_50_plus_job_0_5, na.rm = TRUE),
    public_linked_gross_add_units_0_5 = sum(ifelse(public_applicant, linked_gross_add_units_0_5, 0), na.rm = TRUE),
    .groups = "drop"
  )

assert_unique_keys(yield_counts, c("borocd", "cert_year"), "ZAP ULURP redevelopment yield counts")

yield_0_10_cols <- c(
  "linked_addition_projects_0_10",
  "linked_nb_projects_0_10",
  "linked_nb_50_plus_projects_0_10",
  "linked_gross_add_units_0_10",
  "private_linked_addition_projects_0_10",
  "private_linked_nb_projects_0_10",
  "private_linked_nb_50_plus_projects_0_10",
  "private_linked_gross_add_units_0_10",
  "public_linked_addition_projects_0_10",
  "public_linked_nb_projects_0_10",
  "public_linked_nb_50_plus_projects_0_10",
  "public_linked_gross_add_units_0_10"
)

yield_panel <- crossing(
  borocd = district_lookup$borocd,
  cert_year = 2010:2020
) %>%
  left_join(district_lookup, by = "borocd", relationship = "many-to-one") %>%
  left_join(yield_counts, by = c("borocd", "cert_year"), relationship = "many-to-one") %>%
  mutate(
    across(c(initial_apps, linked_addition_projects_0_10, linked_nb_projects_0_10, linked_nb_50_plus_projects_0_10, linked_gross_add_units_0_10, private_initial_apps, private_linked_addition_projects_0_10, private_linked_nb_projects_0_10, private_linked_nb_50_plus_projects_0_10, private_linked_gross_add_units_0_10, public_initial_apps, public_linked_addition_projects_0_10, public_linked_nb_projects_0_10, public_linked_nb_50_plus_projects_0_10, public_linked_gross_add_units_0_10, linked_addition_projects_0_5, linked_nb_projects_0_5, linked_nb_50_plus_projects_0_5, linked_gross_add_units_0_5, private_linked_addition_projects_0_5, private_linked_nb_projects_0_5, private_linked_nb_50_plus_projects_0_5, private_linked_gross_add_units_0_5, public_linked_addition_projects_0_5, public_linked_nb_projects_0_5, public_linked_nb_50_plus_projects_0_5, public_linked_gross_add_units_0_5), ~ coalesce(.x, 0)),
    mature_0_10_window = cert_year <= 2015,
    across(all_of(yield_0_10_cols), ~ ifelse(mature_0_10_window, .x, NA_real_)),
    yield_era = yield_era_from_year(cert_year),
    linked_addition_rate_0_10 = ifelse(mature_0_10_window & initial_apps > 0, linked_addition_projects_0_10 / initial_apps, NA_real_),
    linked_nb_rate_0_10 = ifelse(mature_0_10_window & initial_apps > 0, linked_nb_projects_0_10 / initial_apps, NA_real_),
    linked_nb_50_plus_rate_0_10 = ifelse(mature_0_10_window & initial_apps > 0, linked_nb_50_plus_projects_0_10 / initial_apps, NA_real_),
    linked_gross_add_units_per_app_0_10 = ifelse(mature_0_10_window & initial_apps > 0, linked_gross_add_units_0_10 / initial_apps, NA_real_),
    private_linked_addition_rate_0_10 = ifelse(mature_0_10_window & private_initial_apps > 0, private_linked_addition_projects_0_10 / private_initial_apps, NA_real_),
    private_linked_nb_50_plus_rate_0_10 = ifelse(mature_0_10_window & private_initial_apps > 0, private_linked_nb_50_plus_projects_0_10 / private_initial_apps, NA_real_),
    private_linked_gross_add_units_per_app_0_10 = ifelse(mature_0_10_window & private_initial_apps > 0, private_linked_gross_add_units_0_10 / private_initial_apps, NA_real_),
    public_linked_addition_rate_0_10 = ifelse(mature_0_10_window & public_initial_apps > 0, public_linked_addition_projects_0_10 / public_initial_apps, NA_real_),
    public_linked_nb_50_plus_rate_0_10 = ifelse(mature_0_10_window & public_initial_apps > 0, public_linked_nb_50_plus_projects_0_10 / public_initial_apps, NA_real_),
    public_linked_gross_add_units_per_app_0_10 = ifelse(mature_0_10_window & public_initial_apps > 0, public_linked_gross_add_units_0_10 / public_initial_apps, NA_real_),
    linked_addition_rate_0_5 = ifelse(initial_apps > 0, linked_addition_projects_0_5 / initial_apps, NA_real_),
    linked_nb_rate_0_5 = ifelse(initial_apps > 0, linked_nb_projects_0_5 / initial_apps, NA_real_),
    linked_nb_50_plus_rate_0_5 = ifelse(initial_apps > 0, linked_nb_50_plus_projects_0_5 / initial_apps, NA_real_),
    linked_gross_add_units_per_app_0_5 = ifelse(initial_apps > 0, linked_gross_add_units_0_5 / initial_apps, NA_real_),
    private_linked_nb_50_plus_rate_0_5 = ifelse(private_initial_apps > 0, private_linked_nb_50_plus_projects_0_5 / private_initial_apps, NA_real_),
    public_linked_nb_50_plus_rate_0_5 = ifelse(public_initial_apps > 0, public_linked_nb_50_plus_projects_0_5 / public_initial_apps, NA_real_),
    linked_nb_50_plus_projects_per_10k_0_10 = ifelse(cert_year <= 2015, 10000 * linked_nb_50_plus_projects_0_10 / occupied_units_1990, NA_real_)
  ) %>%
  arrange(cert_year, borocd)

era_summary <- bind_rows(
  cd_year_panel %>%
    group_by(era, two_by_two_cell_A, two_by_two_label_A) %>%
    summarise(
      summary_family = "two_by_two",
      outcome_family = "initial_apps_per_10k",
      outcome_label = "Applications per 10,000 occupied units",
      numerator = sum(initial_apps, na.rm = TRUE),
      denominator = sum(occupied_units_1990, na.rm = TRUE),
      value = 10000 * numerator / denominator,
      .groups = "drop"
    ),
  cd_year_panel %>%
    group_by(era, two_by_two_cell_A, two_by_two_label_A) %>%
    summarise(
      summary_family = "two_by_two",
      outcome_family = "private_initial_apps_per_10k",
      outcome_label = "Private applications per 10,000 occupied units",
      numerator = sum(private_initial_apps, na.rm = TRUE),
      denominator = sum(occupied_units_1990, na.rm = TRUE),
      value = 10000 * numerator / denominator,
      .groups = "drop"
    ),
  mature_panel %>%
    group_by(era, two_by_two_cell_A, two_by_two_label_A) %>%
    summarise(
      summary_family = "two_by_two",
      outcome_family = "completion_share",
      outcome_label = "Completion share",
      numerator = sum(complete_apps, na.rm = TRUE),
      denominator = sum(initial_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    ),
  mature_panel %>%
    group_by(era, two_by_two_cell_A, two_by_two_label_A) %>%
    summarise(
      summary_family = "two_by_two",
      outcome_family = "failure_share",
      outcome_label = "Failure share",
      numerator = sum(failed_apps, na.rm = TRUE),
      denominator = sum(initial_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    ),
  yield_panel %>%
    filter(cert_year <= 2015) %>%
    group_by(yield_era, two_by_two_cell_A, two_by_two_label_A) %>%
    summarise(
      summary_family = "two_by_two",
      outcome_family = "linked_nb_50_plus_rate_0_10",
      outcome_label = "Linked 50+ build-out rate",
      numerator = sum(linked_nb_50_plus_projects_0_10, na.rm = TRUE),
      denominator = sum(initial_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    ) %>%
    rename(era = yield_era),
  yield_panel %>%
    filter(cert_year <= 2015) %>%
    group_by(yield_era, two_by_two_cell_A, two_by_two_label_A) %>%
    summarise(
      summary_family = "two_by_two",
      outcome_family = "linked_gross_add_units_per_app_0_10",
      outcome_label = "Linked gross-add units per app",
      numerator = sum(linked_gross_add_units_0_10, na.rm = TRUE),
      denominator = sum(initial_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    ) %>%
    rename(era = yield_era),
  cd_year_panel %>%
    group_by(era) %>%
    summarise(
      summary_family = "applicant_split",
      group_label = "All apps",
      outcome_family = "initial_apps_share",
      outcome_label = "Share of applications",
      numerator = sum(initial_apps, na.rm = TRUE),
      denominator = sum(initial_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    ),
  cd_year_panel %>%
    group_by(era) %>%
    summarise(
      summary_family = "applicant_split",
      group_label = "Private apps",
      outcome_family = "private_initial_apps_share",
      outcome_label = "Share of applications",
      numerator = sum(private_initial_apps, na.rm = TRUE),
      denominator = sum(initial_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    ),
  cd_year_panel %>%
    group_by(era) %>%
    summarise(
      summary_family = "applicant_split",
      group_label = "Public apps",
      outcome_family = "public_initial_apps_share",
      outcome_label = "Share of applications",
      numerator = sum(public_initial_apps, na.rm = TRUE),
      denominator = sum(initial_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    ),
  cd_year_panel %>%
    group_by(era) %>%
    summarise(
      summary_family = "applicant_split",
      group_label = "Public HPD apps",
      outcome_family = "public_hpd_apps_share",
      outcome_label = "Share of applications",
      numerator = sum(public_hpd_apps, na.rm = TRUE),
      denominator = sum(initial_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    ),
  cd_year_panel %>%
    group_by(era) %>%
    summarise(
      summary_family = "action_split",
      group_label = "Rezoning/special apps",
      outcome_family = "rezoning_or_special_apps_share",
      outcome_label = "Share of applications",
      numerator = sum(rezoning_or_special_apps, na.rm = TRUE),
      denominator = sum(initial_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    ),
  cd_year_panel %>%
    group_by(era) %>%
    summarise(
      summary_family = "action_split",
      group_label = "Public land/disposition apps",
      outcome_family = "public_land_or_disposition_apps_share",
      outcome_label = "Share of applications",
      numerator = sum(public_land_or_disposition_apps, na.rm = TRUE),
      denominator = sum(initial_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    )
) %>%
  arrange(summary_family, outcome_family, era, two_by_two_cell_A)

plot_counts_df <- era_summary %>%
  filter(summary_family == "two_by_two", outcome_family %in% c("initial_apps_per_10k", "private_initial_apps_per_10k", "completion_share", "failure_share", "linked_nb_50_plus_rate_0_10", "linked_gross_add_units_per_app_0_10")) %>%
  mutate(
    era = factor(era, levels = c("1976-1979", "1980-1984", "1985-1989", "1990-1999", "2000-2009", "2010-2015", "2010-2019", "2016-2020", "2020-2025")),
    two_by_two_label_A = factor(two_by_two_label_A, levels = c("Low homeowner / Low redev", "Low homeowner / High redev", "High homeowner / Low redev", "High homeowner / High redev"))
  )

private_split_df <- era_summary %>%
  filter(summary_family %in% c("applicant_split", "action_split")) %>%
  mutate(
    era = factor(era, levels = c("1976-1979", "1980-1984", "1985-1989", "1990-1999", "2000-2009", "2010-2019", "2020-2025"))
  )

plot_one <- ggplot(plot_counts_df, aes(x = era, y = value, color = two_by_two_label_A, group = two_by_two_label_A)) +
  geom_line(linewidth = 0.8, na.rm = TRUE) +
  geom_point(size = 1.8, na.rm = TRUE) +
  facet_wrap(~ outcome_label, scales = "free_y", ncol = 1) +
  scale_color_manual(values = c(
    "Low homeowner / Low redev" = "#7f7f7f",
    "Low homeowner / High redev" = "#1b9e77",
    "High homeowner / Low redev" = "#7570b3",
    "High homeowner / High redev" = "#d95f02"
  )) +
  labs(x = "Era", y = NULL, color = NULL, title = "ZAP mechanism outcomes by homeowner × redevelopment cell") +
  theme_minimal(base_size = 11) +
  theme(legend.position = "bottom")

plot_two <- ggplot(private_split_df, aes(x = era, y = value, color = group_label, group = group_label)) +
  geom_line(linewidth = 0.8, na.rm = TRUE) +
  geom_point(size = 1.8, na.rm = TRUE) +
  facet_wrap(~ summary_family, scales = "free_y", ncol = 1) +
  labs(x = "Era", y = NULL, color = NULL, title = "Application mix by applicant and action family") +
  theme_minimal(base_size = 11) +
  theme(legend.position = "bottom")

temp_pdf <- tempfile(fileext = ".pdf")
pdf(temp_pdf, width = 11, height = 8.5)
print(plot_one)
print(plot_two)
dev.off()

copy_if_changed(temp_pdf, "../output/zap_ulurp_redev_plots.pdf")
write_csv_if_changed(cd_year_panel, "../output/zap_ulurp_redev_cd_year_panel.csv")
write_csv_if_changed(mature_panel, "../output/zap_ulurp_redev_mature_cohort_panel.csv")
write_csv_if_changed(yield_panel, "../output/zap_ulurp_redev_yield_panel.csv")
write_csv_if_changed(era_summary, "../output/zap_ulurp_redev_2x2_era_summary.csv")

cat("Wrote ZAP ULURP redevelopment summary outputs to ../output\n")
