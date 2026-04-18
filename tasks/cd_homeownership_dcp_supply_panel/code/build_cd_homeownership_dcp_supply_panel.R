# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/cd_homeownership_dcp_supply_panel/code")
# cd_homeownership_1990_measure_csv <- "../input/cd_homeownership_1990_measure.csv"
# cd_baseline_1990_controls_csv <- "../input/cd_baseline_1990_controls.csv"
# dcp_housing_database_project_level_parquet <- "../input/dcp_housing_database_project_level_25q4.parquet"
# out_panel_csv <- "../output/cd_homeownership_dcp_supply_panel.csv"
# out_qc_csv <- "../output/cd_homeownership_dcp_supply_panel_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 5) {
  stop("Expected 5 arguments: cd_homeownership_1990_measure_csv cd_baseline_1990_controls_csv dcp_housing_database_project_level_parquet out_panel_csv out_qc_csv")
}

cd_homeownership_1990_measure_csv <- args[1]
cd_baseline_1990_controls_csv <- args[2]
dcp_housing_database_project_level_parquet <- args[3]
out_panel_csv <- args[4]
out_qc_csv <- args[5]

measure_df <- read_csv(cd_homeownership_1990_measure_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
    borocd = as.integer(borocd),
    borough_code = as.character(borough_code),
    borough_name = standardize_borough_name(borough_code)
  ) %>%
  arrange(borocd)

controls_df <- read_csv(cd_baseline_1990_controls_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
    borocd = as.integer(borocd),
    borough_code = as.character(borough_code),
    borough_name = standardize_borough_name(borough_code)
  ) %>%
  arrange(borocd)

housing_df <- read_parquet(
  dcp_housing_database_project_level_parquet,
  col_select = c("permit_year", "community_district", "job_type", "classa_init", "classa_prop", "classa_net")
) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(
    year = as.integer(permit_year),
    borocd = as.integer(community_district),
    standard_cd_flag = borocd %in% measure_df$borocd,
    job_type = str_squish(as.character(job_type)),
    classa_init = suppressWarnings(as.numeric(classa_init)),
    classa_prop = suppressWarnings(as.numeric(classa_prop)),
    classa_net = suppressWarnings(as.numeric(classa_net))
  )

panel_source_df <- housing_df %>%
  filter(standard_cd_flag, !is.na(year), year >= 2000, year <= 2025) %>%
  mutate(
    nb_project_count = as.numeric(job_type == "New Building"),
    nb_gross_units = ifelse(job_type == "New Building", pmax(coalesce(classa_prop, 0), 0), 0),
    gross_add_units = case_when(
      job_type == "New Building" ~ pmax(coalesce(classa_prop, 0), 0),
      job_type == "Alteration" ~ pmax(coalesce(classa_net, 0), 0),
      TRUE ~ 0
    ),
    gross_loss_units = case_when(
      job_type %in% c("Alteration", "Demolition") ~ pmax(-coalesce(classa_net, 0), 0),
      TRUE ~ 0
    ),
    net_units = coalesce(classa_net, 0),
    nb_size_bin = case_when(
      job_type == "New Building" & classa_prop >= 1 & classa_prop <= 2 ~ "1_2",
      job_type == "New Building" & classa_prop >= 3 & classa_prop <= 4 ~ "3_4",
      job_type == "New Building" & classa_prop >= 5 & classa_prop <= 9 ~ "5_9",
      job_type == "New Building" & classa_prop >= 10 & classa_prop <= 49 ~ "10_49",
      job_type == "New Building" & classa_prop >= 50 ~ "50_plus",
      TRUE ~ NA_character_
    )
  )

base_outcomes_df <- panel_source_df %>%
  group_by(borocd, year) %>%
  summarise(
    nb_project_count = sum(nb_project_count, na.rm = TRUE),
    nb_gross_units = sum(nb_gross_units, na.rm = TRUE),
    gross_add_units = sum(gross_add_units, na.rm = TRUE),
    gross_loss_units = sum(gross_loss_units, na.rm = TRUE),
    net_units = sum(net_units, na.rm = TRUE),
    .groups = "drop"
  )

size_outcomes_df <- panel_source_df %>%
  filter(job_type == "New Building", !is.na(nb_size_bin)) %>%
  group_by(borocd, year, nb_size_bin) %>%
  summarise(
    nb_project_count = sum(nb_project_count, na.rm = TRUE),
    nb_gross_units = sum(nb_gross_units, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  pivot_wider(
    names_from = nb_size_bin,
    values_from = c(nb_project_count, nb_gross_units),
    values_fill = 0,
    names_glue = "{.value}_{nb_size_bin}"
  )

year_outcomes_df <- base_outcomes_df %>%
  left_join(size_outcomes_df, by = c("borocd", "year")) %>%
  mutate(
    nb_project_count_1_2 = coalesce(nb_project_count_1_2, 0),
    nb_project_count_3_4 = coalesce(nb_project_count_3_4, 0),
    nb_project_count_5_9 = coalesce(nb_project_count_5_9, 0),
    nb_project_count_10_49 = coalesce(nb_project_count_10_49, 0),
    nb_project_count_50_plus = coalesce(nb_project_count_50_plus, 0),
    nb_gross_units_1_2 = coalesce(nb_gross_units_1_2, 0),
    nb_gross_units_3_4 = coalesce(nb_gross_units_3_4, 0),
    nb_gross_units_5_9 = coalesce(nb_gross_units_5_9, 0),
    nb_gross_units_10_49 = coalesce(nb_gross_units_10_49, 0),
    nb_gross_units_50_plus = coalesce(nb_gross_units_50_plus, 0),
    nb_project_count_1_4 = nb_project_count_1_2 + nb_project_count_3_4,
    nb_project_count_5_plus = nb_project_count_5_9 + nb_project_count_10_49 + nb_project_count_50_plus,
    nb_gross_units_1_4 = nb_gross_units_1_2 + nb_gross_units_3_4,
    nb_gross_units_5_plus = nb_gross_units_5_9 + nb_gross_units_10_49 + nb_gross_units_50_plus
  )

outcome_map <- tribble(
  ~outcome_family, ~outcome_group, ~outcome_label,
  "nb_project_count", "total_projects", "New-building projects",
  "nb_gross_units", "total_units", "New-building gross units",
  "gross_add_units", "total_units", "Gross residential additions",
  "gross_loss_units", "total_units", "Gross residential losses",
  "net_units", "total_units", "Net residential units",
  "nb_project_count_1_2", "size_bin_projects", "New-building projects: 1-2 units",
  "nb_project_count_3_4", "size_bin_projects", "New-building projects: 3-4 units",
  "nb_project_count_5_9", "size_bin_projects", "New-building projects: 5-9 units",
  "nb_project_count_10_49", "size_bin_projects", "New-building projects: 10-49 units",
  "nb_project_count_50_plus", "size_bin_projects", "New-building projects: 50+ units",
  "nb_project_count_1_4", "size_bin_projects", "New-building projects: 1-4 units",
  "nb_project_count_5_plus", "size_bin_projects", "New-building projects: 5+ units",
  "nb_gross_units_1_2", "size_bin_units", "New-building gross units: 1-2 unit projects",
  "nb_gross_units_3_4", "size_bin_units", "New-building gross units: 3-4 unit projects",
  "nb_gross_units_5_9", "size_bin_units", "New-building gross units: 5-9 unit projects",
  "nb_gross_units_10_49", "size_bin_units", "New-building gross units: 10-49 unit projects",
  "nb_gross_units_50_plus", "size_bin_units", "New-building gross units: 50+ unit projects",
  "nb_gross_units_1_4", "size_bin_units", "New-building gross units: 1-4 unit projects",
  "nb_gross_units_5_plus", "size_bin_units", "New-building gross units: 5+ unit projects"
)

year_outcomes_long <- year_outcomes_df %>%
  pivot_longer(
    cols = all_of(outcome_map$outcome_family),
    names_to = "outcome_family",
    values_to = "outcome_value"
  ) %>%
  left_join(outcome_map, by = "outcome_family")

panel_df <- crossing(
  measure_df %>%
    select(
      source_id,
      pull_date,
      district_id,
      borocd,
      borough_code,
      borough_name,
      owner_occupied_units_1990,
      occupied_units_1990,
      borough_owner_occupied_units_1990,
      borough_occupied_units_1990,
      h_cd_1990,
      h_cd_1990_pct,
      h_b_1990,
      h_b_1990_pct,
      cd_minus_borough_1990,
      treat_pp,
      treat_z_boro,
      owner_occupied_share_reported_1990_pct,
      owner_share_reported_gap_pp
    ) %>%
    left_join(
      controls_df %>%
        select(
          district_id,
          borocd,
          vacancy_rate_1990_exact,
          renter_share_1990_exact,
          structure_share_1_detached_1990_exact,
          structure_share_1_attached_1990_exact,
          structure_share_2_units_1990_exact,
          structure_share_3_4_units_1990_exact,
          structure_share_5_9_units_1990_exact,
          structure_share_10_19_units_1990_exact,
          structure_share_20_49_units_1990_exact,
          structure_share_50_plus_units_1990_exact,
          structure_share_1_2_units_1990_exact,
          structure_share_5_plus_units_1990_exact,
          median_household_income_1990_1999_dollars_exact,
          poverty_share_1990_exact,
          median_housing_value_1990_2000_dollars_exact,
          median_housing_value_1990_2000_dollars_exact_filled,
          median_housing_value_1990_missing_exact,
          subway_commute_share_1990_exact,
          public_transit_commute_share_1990_exact,
          mean_commute_time_1990_minutes_exact,
          total_housing_units_growth_1980_1990_approx,
          occupied_units_growth_1980_1990_approx,
          vacancy_rate_change_1980_1990_pp_approx,
          homeowner_share_change_1980_1990_pp_approx
        ),
      by = c("district_id", "borocd")
    ),
  year = 2000:2025,
  outcome_map
) %>%
  left_join(year_outcomes_long, by = c("borocd", "year", "outcome_family", "outcome_group", "outcome_label")) %>%
  mutate(
    outcome_source_id = "dcp_housing_database_project_level",
    outcome_value = coalesce(outcome_value, 0)
  ) %>%
  group_by(borough_code, borough_name, year, outcome_family) %>%
  mutate(
    borough_outcome_total = sum(outcome_value, na.rm = TRUE),
    borough_outcome_share = ifelse(borough_outcome_total > 0, outcome_value / borough_outcome_total, NA_real_)
  ) %>%
  ungroup() %>%
  arrange(outcome_family, year, borocd)

qc_df <- bind_rows(
  tibble(
    metric = "panel_row_count",
    value = nrow(panel_df),
    note = "Rows in the balanced CD-year-outcome panel for the DCP housing-supply decomposition, 2000-2025."
  ),
  tibble(
    metric = "district_count",
    value = n_distinct(panel_df$borocd),
    note = "Standard community districts represented in the panel."
  ),
  tibble(
    metric = "year_count",
    value = n_distinct(panel_df$year),
    note = "Calendar years represented in the panel."
  ),
  tibble(
    metric = "outcome_family_count",
    value = n_distinct(panel_df$outcome_family),
    note = "Distinct DCP housing-supply outcomes carried in the panel."
  ),
  tibble(
    metric = "source_row_count_total",
    value = nrow(housing_df),
    note = "Project rows in the staged DCP Housing Database file before year and standard-CD restrictions."
  ),
  tibble(
    metric = "source_row_share_standard_59_cd",
    value = mean(housing_df$standard_cd_flag, na.rm = TRUE),
    note = "Share of staged DCP Housing Database rows that carry one of the standard 59 community district codes."
  ),
  tibble(
    metric = "missing_permit_year_share",
    value = mean(is.na(housing_df$year)),
    note = "Share of staged DCP Housing Database rows with missing permit years."
  ),
  tibble(
    metric = "nb_gross_units_raw_sum",
    value = sum(panel_source_df$nb_gross_units, na.rm = TRUE),
    note = "Raw sum of classa_prop over New Building rows in the analytic DCP source panel."
  ),
  tibble(
    metric = "nb_gross_units_panel_sum",
    value = sum(year_outcomes_df$nb_gross_units, na.rm = TRUE),
    note = "Panel sum of New Building gross units after CD-year aggregation."
  ),
  tibble(
    metric = "nb_gross_units_gap",
    value = sum(year_outcomes_df$nb_gross_units, na.rm = TRUE) - sum(panel_source_df$nb_gross_units, na.rm = TRUE),
    note = "Difference between aggregated panel New Building gross units and the raw source sum."
  ),
  tibble(
    metric = "gross_loss_units_positive_only_violation_count",
    value = sum(panel_source_df$gross_loss_units < 0, na.rm = TRUE),
    note = "Rows where gross loss units are negative after construction; should be zero."
  ),
  tibble(
    metric = "size_bin_project_count_gap",
    value = sum(
      year_outcomes_df$nb_project_count_1_2 +
        year_outcomes_df$nb_project_count_3_4 +
        year_outcomes_df$nb_project_count_5_9 +
        year_outcomes_df$nb_project_count_10_49 +
        year_outcomes_df$nb_project_count_50_plus,
      na.rm = TRUE
    ) - sum(panel_source_df$job_type == "New Building" & !is.na(panel_source_df$nb_size_bin), na.rm = TRUE),
    note = "Difference between aggregated size-bin New Building project counts and the raw source count of size-binnable New Building rows."
  ),
  tibble(
    metric = "size_bin_gross_units_gap",
    value = sum(
      year_outcomes_df$nb_gross_units_1_2 +
        year_outcomes_df$nb_gross_units_3_4 +
        year_outcomes_df$nb_gross_units_5_9 +
        year_outcomes_df$nb_gross_units_10_49 +
        year_outcomes_df$nb_gross_units_50_plus,
      na.rm = TRUE
    ) - sum(panel_source_df$nb_gross_units[panel_source_df$job_type == "New Building" & !is.na(panel_source_df$nb_size_bin)], na.rm = TRUE),
    note = "Difference between aggregated size-bin New Building gross units and the raw source sum of size-binnable New Building classa_prop."
  ),
  tibble(
    metric = "new_building_zero_or_missing_unit_rows",
    value = sum(panel_source_df$job_type == "New Building" & is.na(panel_source_df$nb_size_bin), na.rm = TRUE),
    note = "New Building rows with zero or missing proposed Class A units, so they do not enter the requested size bins."
  ),
  tibble(
    metric = "status",
    value = ifelse(
      nrow(panel_df) == 59 * length(2000:2025) * nrow(outcome_map) &&
        all(!is.na(panel_df$treat_pp)) &&
        all(!is.na(panel_df$treat_z_boro)),
      1,
      0
    ),
    note = "One means the balanced DCP housing-supply decomposition panel is complete over the planned support."
  )
)

write_csv_if_changed(panel_df, out_panel_csv)
write_csv_if_changed(qc_df, out_qc_csv)

cat("Wrote CD-year DCP housing-supply decomposition outputs to", dirname(out_panel_csv), "\n")
