# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_cd_homeownership_long_units_series/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

measure_df <- read_csv("../input/cd_homeownership_1990_measure.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    district_id = sprintf("%03d", suppressWarnings(as.integer(district_id))),
    borocd = suppressWarnings(as.integer(borocd)),
    borough_code = as.character(borough_code)
  ) %>%
  arrange(borocd)

if (anyDuplicated(measure_df$district_id)) {
  stop("Homeownership measure is not unique by community district.")
}

proxy_map <- tribble(
  ~source_family, ~series_family, ~series_label, ~value_column,
  "mappluto_proxy_25v4", "units_built_total", "Units built: total", "residential_units_proxy",
  "mappluto_proxy_25v4", "units_built_1_2", "Units built: 1-2", "units_1_2_proxy",
  "mappluto_proxy_25v4", "units_built_1_4", "Units built: 1-4", "units_1_4_proxy",
  "mappluto_proxy_25v4", "units_built_5_plus", "Units built: 5+", "units_5_plus_proxy",
  "mappluto_proxy_25v4", "units_built_50_plus", "Units built: 50+", "units_50_plus_proxy",
  "mappluto_proxy_25v4", "projects_built_50_plus", "Projects built: 50+", "lots_50_plus_proxy"
)

district_skeleton <- measure_df %>%
  distinct(district_id, borocd, borough_code, borough_name)

proxy_values <- read_csv("../input/mappluto_construction_proxy_cd_year.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    district_id = sprintf("%03d", borocd),
    borough_code = as.character(borough_code),
    year = suppressWarnings(as.integer(yearbuilt))
  ) %>%
  filter(year >= 1910, year <= 2025) %>%
  select(district_id, borocd, borough_code, borough_name, year, all_of(proxy_map$value_column)) %>%
  pivot_longer(
    cols = all_of(proxy_map$value_column),
    names_to = "value_column",
    values_to = "outcome_value"
  )

duplicate_proxy_values <- proxy_values %>%
  count(district_id, year, value_column, name = "source_row_count") %>%
  filter(source_row_count > 1)

if (nrow(duplicate_proxy_values) > 0) {
  stop("MapPLUTO proxy series is not unique by community district, year, and value column.")
}

measure_attrs <- measure_df %>%
  select(
    district_id,
    borocd,
    borough_code,
    borough_name,
    owner_occupied_units_1990,
    occupied_units_1990,
    total_housing_units_1990,
    h_cd_1990,
    h_cd_1990_pct,
    h_b_1990,
    h_b_1990_pct,
    cd_minus_borough_1990,
    treat_pp,
    treat_z_boro,
    vacancy_rate_1990,
    median_household_income_1990,
    vacant_housing_units_1990
  )

series_df <- expand_grid(district_skeleton, year = 1910:2025, proxy_map) %>%
  left_join(
    proxy_values,
    by = c("district_id", "borocd", "borough_code", "borough_name", "year", "value_column"),
    relationship = "many-to-one"
  ) %>%
  mutate(
    source_label = "25v4 MapPLUTO yearbuilt proxy on community districts",
    series_kind = "preferred_long_series",
    outcome_value = coalesce(outcome_value, 0)
  ) %>%
  select(source_family, source_label, series_kind, series_family, series_label, district_id, borocd, borough_code, borough_name, year, outcome_value) %>%
  left_join(
    measure_attrs,
    by = c("district_id", "borocd", "borough_code", "borough_name"),
    relationship = "many-to-one"
  ) %>%
  group_by(series_family, year, borough_code, borough_name) %>%
  mutate(
    borough_outcome_total = sum(outcome_value, na.rm = TRUE),
    borough_outcome_share = if_else(borough_outcome_total > 0, outcome_value / borough_outcome_total, NA_real_)
  ) %>%
  ungroup() %>%
  arrange(series_kind, series_family, borocd, year)

write_csv_if_changed(series_df, "../output/cd_homeownership_long_units_series.csv")

cat("Wrote community district housing series to ../output\n")
