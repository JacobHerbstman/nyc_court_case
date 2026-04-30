# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_cd_homeownership_long_units_series/code")
# cd_homeownership_1990_measure_csv <- "../input/cd_homeownership_1990_measure.csv"
# cd_baseline_1990_controls_csv <- "../input/cd_baseline_1990_controls.csv"
# mappluto_construction_proxy_cd_year_csv <- "../input/mappluto_construction_proxy_cd_year.csv"
# dcp_housing_database_files_csv <- "../input/dcp_housing_database_files.csv"
# out_series_csv <- "../output/cd_homeownership_long_units_series.csv"
# out_qc_csv <- "../output/cd_homeownership_long_units_series_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tidyr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 6) {
  stop("Expected 6 arguments: cd_homeownership_1990_measure_csv cd_baseline_1990_controls_csv mappluto_construction_proxy_cd_year_csv dcp_housing_database_files_csv out_series_csv out_qc_csv")
}

cd_homeownership_1990_measure_csv <- args[1]
cd_baseline_1990_controls_csv <- args[2]
mappluto_construction_proxy_cd_year_csv <- args[3]
dcp_housing_database_files_csv <- args[4]
out_series_csv <- args[5]
out_qc_csv <- args[6]

measure_df <- read_csv(cd_homeownership_1990_measure_csv, show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code))
  ) |>
  arrange(borocd)

if (anyDuplicated(measure_df$borocd)) {
  stop("Homeownership measure is not unique by borocd.")
}

controls_df <- read_csv(cd_baseline_1990_controls_csv, show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code))
  ) |>
  arrange(borocd)

if (anyDuplicated(controls_df$borocd)) {
  stop("Baseline controls file is not unique by borocd.")
}

static_controls <- controls_df |>
  select(
    district_id,
    borocd,
    vacancy_rate_1990_exact,
    structure_share_1_2_units_1990_exact,
    structure_share_3_4_units_1990_exact,
    structure_share_5_plus_units_1990_exact,
    median_household_income_1990_1999_dollars_exact,
    poverty_share_1990_exact,
    median_housing_value_1990_2000_dollars_exact_filled,
    median_housing_value_1990_missing_exact,
    foreign_born_share_1990_exact,
    college_graduate_share_1990_exact,
    unemployment_rate_1990_exact,
    subway_commute_share_1990_exact,
    public_transit_commute_share_1990_exact,
    mean_commute_time_1990_minutes_exact
  )

proxy_map <- tribble(
  ~source_family, ~series_family, ~series_label, ~value_column,
  "mappluto_proxy", "units_built_total", "Units built: total", "residential_units_proxy",
  "mappluto_proxy", "units_built_1_4", "Units built: 1-4", "units_1_4_proxy",
  "mappluto_proxy", "units_built_5_plus", "Units built: 5+", "units_5_plus_proxy",
  "mappluto_proxy", "units_built_50_plus", "Units built: 50+", "units_50_plus_proxy"
)

cd_skeleton <- measure_df |>
  distinct(borocd, borough_code, borough_name)

proxy_values <- read_csv(mappluto_construction_proxy_cd_year_csv, show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    year = suppressWarnings(as.integer(yearbuilt))
  ) |>
  filter(year >= 1980, year <= 2009) |>
  select(borocd, borough_code, borough_name, year, all_of(proxy_map$value_column)) |>
  pivot_longer(
    cols = all_of(proxy_map$value_column),
    names_to = "value_column",
    values_to = "outcome_value"
  )

duplicate_proxy_values <- proxy_values |>
  count(borocd, borough_code, borough_name, year, value_column, name = "source_row_count") |>
  filter(source_row_count > 1)

if (nrow(duplicate_proxy_values) > 0) {
  stop("MapPLUTO proxy series is not unique by CD, year, and value column.")
}

proxy_long <- expand_grid(cd_skeleton, year = 1980:2009, proxy_map) |>
  left_join(
    proxy_values,
    by = c("borocd", "borough_code", "borough_name", "year", "value_column"),
    relationship = "many-to-one"
  ) |>
  mutate(
    source_label = "MapPLUTO yearbuilt proxy",
    series_kind = "preferred_long_series",
    outcome_value = coalesce(outcome_value, 0)
  ) |>
  select(source_family, source_label, series_kind, series_family, series_label, borocd, borough_code, borough_name, year, outcome_value)

hdb_file <- read_csv(dcp_housing_database_files_csv, show_col_types = FALSE, na = c("", "NA")) |>
  filter(source_id == "dcp_housing_database_project_level", !is.na(parquet_path), file.exists(parquet_path)) |>
  mutate(
    vintage = as.character(vintage),
    vintage_year = suppressWarnings(as.integer(str_extract(vintage, "^[0-9]{2}"))),
    vintage_quarter = suppressWarnings(as.integer(str_extract(vintage, "(?<=Q)[1-4]$"))),
    vintage_order = 4L * vintage_year + vintage_quarter
  ) |>
  arrange(desc(vintage_order), desc(vintage), parquet_path) |>
  slice_head(n = 1)

if (nrow(hdb_file) == 0) {
  stop("Could not find a staged DCP Housing Database project-level parquet in ", dcp_housing_database_files_csv)
}

hdb_source_df <- read_parquet(
  hdb_file$parquet_path[[1]],
  col_select = c("completion_year", "community_district", "borough_code", "borough_name", "job_type", "classa_prop", "classa_net")
) |>
  transmute(
    year = suppressWarnings(as.integer(completion_year)),
    borocd = sprintf("%03d", suppressWarnings(as.integer(community_district))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = as.character(borough_name),
    job_type = str_squish(as.character(job_type)),
    classa_prop = suppressWarnings(as.numeric(classa_prop)),
    classa_net = suppressWarnings(as.numeric(classa_net))
  ) |>
  filter(
    borocd %in% measure_df$borocd,
    !is.na(year),
    year >= 2010,
    year <= 2025
  ) |>
  mutate(
    nb_gross_units = if_else(job_type == "New Building", pmax(coalesce(classa_prop, 0), 0), 0),
    gross_add_units = case_when(
      job_type == "New Building" ~ pmax(coalesce(classa_prop, 0), 0),
      job_type == "Alteration" ~ pmax(coalesce(classa_net, 0), 0),
      TRUE ~ 0
    ),
    nb_size_bin = case_when(
      job_type == "New Building" & classa_prop >= 1 & classa_prop <= 2 ~ "1_2",
      job_type == "New Building" & classa_prop >= 3 & classa_prop <= 4 ~ "3_4",
      job_type == "New Building" & classa_prop >= 5 & classa_prop <= 9 ~ "5_9",
      job_type == "New Building" & classa_prop >= 10 & classa_prop <= 49 ~ "10_49",
      job_type == "New Building" & classa_prop >= 50 ~ "50_plus",
      TRUE ~ NA_character_
    )
  )

hdb_base <- hdb_source_df |>
  group_by(borocd, borough_code, borough_name, year) |>
  summarize(
    units_built_total = sum(nb_gross_units, na.rm = TRUE),
    gross_add_units_observed = sum(gross_add_units, na.rm = TRUE),
    .groups = "drop"
  )

hdb_size <- hdb_source_df |>
  filter(job_type == "New Building", !is.na(nb_size_bin)) |>
  group_by(borocd, borough_code, borough_name, year, nb_size_bin) |>
  summarize(units_built = sum(nb_gross_units, na.rm = TRUE), .groups = "drop") |>
  pivot_wider(
    names_from = nb_size_bin,
    values_from = units_built,
    values_fill = 0,
    names_glue = "units_built_{nb_size_bin}"
  )

for (missing_size_column in setdiff(
  c("units_built_1_2", "units_built_3_4", "units_built_5_9", "units_built_10_49", "units_built_50_plus"),
  names(hdb_size)
)) {
  hdb_size[[missing_size_column]] <- 0
}

hdb_size <- hdb_size |>
  mutate(
    units_built_1_4 = coalesce(units_built_1_2, 0) + coalesce(units_built_3_4, 0),
    units_built_5_plus = coalesce(units_built_5_9, 0) + coalesce(units_built_10_49, 0) + coalesce(units_built_50_plus, 0)
  )

hdb_panel_source <- expand_grid(cd_skeleton, year = 2010:2025) |>
  left_join(
    hdb_base,
    by = c("borocd", "borough_code", "borough_name", "year"),
    relationship = "one-to-one"
  ) |>
  left_join(
    hdb_size,
    by = c("borocd", "borough_code", "borough_name", "year"),
    relationship = "one-to-one"
  ) |>
  mutate(
    units_built_total = coalesce(units_built_total, 0),
    gross_add_units_observed = coalesce(gross_add_units_observed, 0),
    units_built_1_4 = coalesce(units_built_1_4, 0),
    units_built_5_plus = coalesce(units_built_5_plus, 0),
    units_built_50_plus = coalesce(units_built_50_plus, 0)
  )

hdb_map <- tribble(
  ~source_family, ~series_kind, ~series_family, ~series_label, ~value_column,
  "dcp_hdb_completion", "preferred_long_series", "units_built_total", "Units built: total", "units_built_total",
  "dcp_hdb_completion", "preferred_long_series", "units_built_1_4", "Units built: 1-4", "units_built_1_4",
  "dcp_hdb_completion", "preferred_long_series", "units_built_5_plus", "Units built: 5+", "units_built_5_plus",
  "dcp_hdb_completion", "preferred_long_series", "units_built_50_plus", "Units built: 50+", "units_built_50_plus",
  "dcp_hdb_completion", "observed_only", "gross_add_units_observed", "Gross additions observed", "gross_add_units_observed"
)

hdb_long <- hdb_panel_source |>
  select(borocd, borough_code, borough_name, year, all_of(unique(hdb_map$value_column))) |>
  pivot_longer(
    cols = all_of(unique(hdb_map$value_column)),
    names_to = "value_column",
    values_to = "outcome_value"
  ) |>
  left_join(hdb_map, by = "value_column", relationship = "many-to-one") |>
  mutate(
    source_label = "DCP Housing Database completion-year observed",
    outcome_value = coalesce(outcome_value, 0)
  ) |>
  select(source_family, source_label, series_kind, series_family, series_label, borocd, borough_code, borough_name, year, outcome_value)

measure_with_controls <- measure_df |>
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
  left_join(static_controls, by = c("district_id", "borocd"), relationship = "one-to-one")

series_df <- bind_rows(proxy_long, hdb_long) |>
  left_join(
    measure_with_controls,
    by = c("borocd", "borough_code", "borough_name"),
    relationship = "many-to-one"
  ) |>
  group_by(series_family, year, borough_code, borough_name) |>
  mutate(
    borough_outcome_total = sum(outcome_value, na.rm = TRUE),
    borough_outcome_share = if_else(borough_outcome_total > 0, outcome_value / borough_outcome_total, NA_real_)
  ) |>
  ungroup() |>
  arrange(series_kind, series_family, borocd, year)

write_csv(series_df, out_series_csv, na = "")

write_csv(
  bind_rows(
    tibble(metric = "district_count", value = as.character(n_distinct(series_df$borocd)), note = "Standard CDs in the long units series."),
    tibble(metric = "year_min", value = as.character(min(series_df$year, na.rm = TRUE)), note = "Minimum year in the long units series."),
    tibble(metric = "year_max", value = as.character(max(series_df$year, na.rm = TRUE)), note = "Maximum year in the long units series."),
    tibble(metric = "preferred_series_year_min", value = as.character(min(series_df$year[series_df$series_kind == "preferred_long_series"], na.rm = TRUE)), note = "Minimum year in the preferred compatible long series."),
    tibble(metric = "preferred_series_year_max", value = as.character(max(series_df$year[series_df$series_kind == "preferred_long_series"], na.rm = TRUE)), note = "Maximum year in the preferred compatible long series."),
    tibble(metric = "source_count", value = as.character(n_distinct(series_df$source_family)), note = "Distinct source families in the long units series."),
    tibble(metric = "missing_treat_count", value = as.character(sum(is.na(series_df$treat_z_boro))), note = "Rows missing the exact homeownership treatment."),
    tibble(metric = "preferred_units_built_total_sum", value = as.character(sum(series_df$outcome_value[series_df$series_family == "units_built_total"], na.rm = TRUE)), note = "Total units in the preferred compatible long series."),
    tibble(metric = "observed_gross_add_units_sum_2010_2025", value = as.character(sum(series_df$outcome_value[series_df$series_family == "gross_add_units_observed"], na.rm = TRUE)), note = "Observed gross additions over 2010-2025 from DCP Housing Database completion-year aggregation.")
  ),
  out_qc_csv,
  na = ""
)

cat("Wrote long units series outputs to", dirname(out_series_csv), "\n")
