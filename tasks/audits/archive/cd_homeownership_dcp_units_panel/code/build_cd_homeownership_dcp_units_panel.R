# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/cd_homeownership_dcp_units_panel/code")
# cd_homeownership_1990_measure_csv <- "../input/cd_homeownership_1990_measure.csv"
# dcp_housing_database_project_level_parquet <- "../input/dcp_housing_database_project_level_25q4.parquet"
# out_panel_csv <- "../output/cd_homeownership_dcp_units_panel.csv"
# out_qc_csv <- "../output/cd_homeownership_dcp_units_panel_qc.csv"

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

if (length(args) != 4) {
  stop("Expected 4 arguments: cd_homeownership_1990_measure_csv dcp_housing_database_project_level_parquet out_panel_csv out_qc_csv")
}

cd_homeownership_1990_measure_csv <- args[1]
dcp_housing_database_project_level_parquet <- args[2]
out_panel_csv <- args[3]
out_qc_csv <- args[4]

measure_df <- read_csv(cd_homeownership_1990_measure_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
    borocd = as.integer(borocd),
    borough_code = as.character(borough_code),
    borough_name = standardize_borough_name(borough_code)
  ) %>%
  arrange(borocd)

housing_df <- read_parquet(
  dcp_housing_database_project_level_parquet,
  col_select = c("permit_year", "community_district", "classa_net")
) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(
    year = as.integer(permit_year),
    borocd = as.integer(community_district),
    standard_cd_flag = borocd %in% measure_df$borocd,
    classa_net = as.numeric(classa_net)
  )

year_counts_df <- housing_df %>%
  filter(standard_cd_flag, !is.na(year), year >= 1998, year <= 2025) %>%
  group_by(borocd, year) %>%
  summarise(
    outcome_value = sum(coalesce(classa_net, 0), na.rm = TRUE),
    .groups = "drop"
  )

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
    ),
  year = 1998:2025
) %>%
  left_join(year_counts_df, by = c("borocd", "year")) %>%
  mutate(
    outcome_source_id = "dcp_housing_database_project_level",
    outcome_family = "dcp_net_units",
    outcome_value = coalesce(outcome_value, 0)
  ) %>%
  group_by(borough_code, borough_name, year) %>%
  mutate(
    borough_outcome_total = sum(outcome_value, na.rm = TRUE),
    borough_outcome_share = ifelse(borough_outcome_total > 0, outcome_value / borough_outcome_total, NA_real_)
  ) %>%
  ungroup() %>%
  arrange(year, borocd)

qc_df <- bind_rows(
  tibble(
    metric = "panel_row_count",
    value = nrow(panel_df),
    note = "Rows in the balanced 59-CD by year panel for DCP Housing Database net Class A unit changes, 1998-2025."
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
    metric = "classa_net_mass_share_standard_59_cd",
    value = sum(housing_df$classa_net[housing_df$standard_cd_flag], na.rm = TRUE) / sum(housing_df$classa_net, na.rm = TRUE),
    note = "Share of total Class A net unit change mass carried by the standard 59 community district codes."
  ),
  tibble(
    metric = "classa_net_abs_mass_share_standard_59_cd",
    value = sum(abs(housing_df$classa_net[housing_df$standard_cd_flag]), na.rm = TRUE) / sum(abs(housing_df$classa_net), na.rm = TRUE),
    note = "Share of absolute Class A net unit change mass carried by the standard 59 community district codes."
  ),
  tibble(
    metric = "borough_year_zero_denominator_count",
    value = panel_df %>%
      distinct(borough_name, year, borough_outcome_total) %>%
      summarise(value = sum(borough_outcome_total <= 0), .groups = "drop") %>%
      pull(value),
    note = "Borough-year cells where the within-borough share denominator is nonpositive."
  ),
  tibble(
    metric = "negative_cd_year_outcome_count",
    value = sum(panel_df$outcome_value < 0, na.rm = TRUE),
    note = "CD-year cells with negative net unit change values."
  ),
  tibble(
    metric = "status",
    value = ifelse(
      nrow(panel_df) == 59 * length(1998:2025) &&
        all(!is.na(panel_df$treat_pp)) &&
        all(!is.na(panel_df$treat_z_boro)),
      1,
      0
    ),
    note = "One means the DCP net-units panel is fully populated over the planned 59-CD by 1998-2025 support."
  )
)

write_csv_if_changed(panel_df, out_panel_csv)
write_csv_if_changed(qc_df, out_qc_csv)

cat("Wrote CD-year DCP net-units panel outputs to", dirname(out_panel_csv), "\n")
