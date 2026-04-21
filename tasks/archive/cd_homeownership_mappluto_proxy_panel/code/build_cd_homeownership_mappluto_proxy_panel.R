# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/cd_homeownership_mappluto_proxy_panel/code")
# cd_homeownership_1990_measure_csv <- "../input/cd_homeownership_1990_measure.csv"
# cd_baseline_1990_controls_csv <- "../input/cd_baseline_1990_controls.csv"
# mappluto_construction_proxy_cd_year_csv <- "../input/mappluto_construction_proxy_cd_year.csv"
# out_panel_csv <- "../output/cd_homeownership_mappluto_proxy_panel.csv"
# out_qc_csv <- "../output/cd_homeownership_mappluto_proxy_panel_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tidyr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 5) {
  stop("Expected 5 arguments: cd_homeownership_1990_measure_csv cd_baseline_1990_controls_csv mappluto_construction_proxy_cd_year_csv out_panel_csv out_qc_csv")
}

cd_homeownership_1990_measure_csv <- args[1]
cd_baseline_1990_controls_csv <- args[2]
mappluto_construction_proxy_cd_year_csv <- args[3]
out_panel_csv <- args[4]
out_qc_csv <- args[5]

measure_df <- read_csv(cd_homeownership_1990_measure_csv, show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code))
  ) |>
  arrange(borocd)

controls_df <- read_csv(cd_baseline_1990_controls_csv, show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code))
  ) |>
  arrange(borocd)

proxy_df <- read_csv(mappluto_construction_proxy_cd_year_csv, show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    year = suppressWarnings(as.integer(yearbuilt))
  ) |>
  filter(year >= 1980, year <= 2009)

outcome_map <- tribble(
  ~outcome_family, ~outcome_group, ~outcome_label,
  "residential_units_proxy", "total_units_proxy", "PLUTO proxy residential units built",
  "units_1_4_proxy", "size_bin_units_proxy", "PLUTO proxy residential units built: 1-4 units",
  "units_5_plus_proxy", "size_bin_units_proxy", "PLUTO proxy residential units built: 5+ units",
  "units_50_plus_proxy", "size_bin_units_proxy", "PLUTO proxy residential units built: 50+ units"
)

year_outcomes_long <- proxy_df |>
  select(borocd, borough_code, borough_name, year, all_of(outcome_map$outcome_family)) |>
  pivot_longer(
    cols = all_of(outcome_map$outcome_family),
    names_to = "outcome_family",
    values_to = "outcome_value"
  ) |>
  left_join(outcome_map, by = "outcome_family")

panel_df <- crossing(
  measure_df |>
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
    ) |>
    left_join(
      controls_df |>
        select(
          district_id,
          borocd,
          total_housing_units_1990_exact,
          occupied_units_1990_exact,
          vacancy_rate_1990_exact,
          renter_share_1990_exact,
          structure_share_1_2_units_1990_exact,
          structure_share_3_4_units_1990_exact,
          structure_share_5_plus_units_1990_exact,
          median_household_income_1990_1999_dollars_exact,
          poverty_share_1990_exact,
          median_housing_value_1990_2000_dollars_exact,
          median_housing_value_1990_2000_dollars_exact_filled,
          median_housing_value_1990_missing_exact,
          foreign_born_share_1990_exact,
          college_graduate_share_1990_exact,
          unemployment_rate_1990_exact,
          subway_commute_share_1990_exact,
          public_transit_commute_share_1990_exact,
          mean_commute_time_1990_minutes_exact
        ),
      by = c("district_id", "borocd")
    ),
  year = 1980:2009,
  outcome_map
) |>
  left_join(year_outcomes_long, by = c("borocd", "borough_code", "borough_name", "year", "outcome_family", "outcome_group", "outcome_label")) |>
  mutate(outcome_value = coalesce(outcome_value, 0)) |>
  group_by(borough_code, borough_name, year, outcome_family) |>
  mutate(
    borough_outcome_total = sum(outcome_value, na.rm = TRUE),
    borough_outcome_share = if_else(borough_outcome_total > 0, outcome_value / borough_outcome_total, NA_real_)
  ) |>
  ungroup() |>
  arrange(outcome_family, borocd, year)

write_csv(panel_df, out_panel_csv, na = "")

raw_sums <- year_outcomes_long |>
  group_by(outcome_family) |>
  summarize(raw_sum = sum(outcome_value, na.rm = TRUE), .groups = "drop")

panel_sums <- panel_df |>
  group_by(outcome_family) |>
  summarize(panel_sum = sum(outcome_value, na.rm = TRUE), .groups = "drop")

write_csv(
  bind_rows(
    tibble(metric = "district_count", value = as.character(n_distinct(panel_df$borocd)), note = "Standard CDs in the MapPLUTO proxy panel."),
    tibble(metric = "year_min", value = as.character(min(panel_df$year, na.rm = TRUE)), note = "Minimum year in the proxy panel."),
    tibble(metric = "year_max", value = as.character(max(panel_df$year, na.rm = TRUE)), note = "Maximum year in the proxy panel."),
    tibble(metric = "row_count", value = as.character(nrow(panel_df)), note = "Rows in the long CD-year-outcome proxy panel."),
    tibble(metric = "missing_treat_z_boro_count", value = as.character(sum(is.na(panel_df$treat_z_boro))), note = "Rows missing the exact treatment."),
    raw_sums |>
      left_join(panel_sums, by = "outcome_family") |>
      transmute(
        metric = paste0(outcome_family, "_sum_gap"),
        value = as.character(panel_sum - raw_sum),
        note = "Panel sum minus raw proxy sum. Should equal zero."
      )
  ),
  out_qc_csv,
  na = ""
)

cat("Wrote MapPLUTO proxy panel outputs to", dirname(out_panel_csv), "\n")
