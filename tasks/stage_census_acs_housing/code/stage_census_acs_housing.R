# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_census_acs_housing/code")
# census_acs_housing_raw_files_csv <- "../input/census_acs_housing_raw_files.csv"
# out_index_csv <- "../output/census_acs_housing_files.csv"
# out_qc_csv <- "../output/census_acs_housing_qc.csv"
# out_tract_year_parquet <- "../output/census_acs_housing_tract_year.parquet"
# out_borough_year_parquet <- "../output/census_acs_housing_borough_year.parquet"
# out_city_year_parquet <- "../output/census_acs_housing_city_year.parquet"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 6) {
  stop("Expected 6 arguments: census_acs_housing_raw_files_csv out_index_csv out_qc_csv out_tract_year_parquet out_borough_year_parquet out_city_year_parquet")
}

census_acs_housing_raw_files_csv <- args[1]
out_index_csv <- args[2]
out_qc_csv <- args[3]
out_tract_year_parquet <- args[4]
out_borough_year_parquet <- args[5]
out_city_year_parquet <- args[6]

county_lookup <- tibble(
  county = c("005", "047", "061", "081", "085"),
  borough_name = c("Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island")
)

raw_index <- read_csv(census_acs_housing_raw_files_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  filter(!is.na(raw_parquet_path), file.exists(raw_parquet_path))

if (nrow(raw_index) == 0) {
  write_csv(tibble(), out_index_csv, na = "")
  write_csv(tibble(), out_qc_csv, na = "")
  write_parquet_if_changed(tibble(), out_tract_year_parquet)
  write_parquet_if_changed(tibble(), out_borough_year_parquet)
  write_parquet_if_changed(tibble(), out_city_year_parquet)
  quit(save = "no")
}

raw_tables <- bind_rows(lapply(raw_index$raw_parquet_path, function(parquet_path) {
  read_parquet(parquet_path) %>%
    as.data.frame() %>%
    as_tibble()
}))

b25002_df <- raw_tables %>%
  filter(table_id == "B25002") %>%
  transmute(
    year = as.integer(vintage),
    geo_id = as.character(geo_id),
    name = as.character(name),
    state = str_pad(as.character(state), width = 2, side = "left", pad = "0"),
    county = str_pad(as.character(county), width = 3, side = "left", pad = "0"),
    tract = str_pad(as.character(tract), width = 6, side = "left", pad = "0"),
    total_housing_units = suppressWarnings(as.numeric(b25002_001e)),
    occupied_units = suppressWarnings(as.numeric(b25002_002e)),
    vacant_units = suppressWarnings(as.numeric(b25002_003e))
  )

b25003_df <- raw_tables %>%
  filter(table_id == "B25003") %>%
  transmute(
    year = as.integer(vintage),
    geo_id = as.character(geo_id),
    owner_occupied_units = suppressWarnings(as.numeric(b25003_002e)),
    renter_occupied_units = suppressWarnings(as.numeric(b25003_003e))
  )

b25024_df <- raw_tables %>%
  filter(table_id == "B25024") %>%
  transmute(
    year = as.integer(vintage),
    geo_id = as.character(geo_id),
    structure_1unit_detached = suppressWarnings(as.numeric(b25024_002e)),
    structure_1unit_attached = suppressWarnings(as.numeric(b25024_003e)),
    structure_2_unit = suppressWarnings(as.numeric(b25024_004e)),
    structure_3_4_unit = suppressWarnings(as.numeric(b25024_005e)),
    structure_5_9_unit = suppressWarnings(as.numeric(b25024_006e)),
    structure_10_19_unit = suppressWarnings(as.numeric(b25024_007e)),
    structure_20_49_unit = suppressWarnings(as.numeric(b25024_008e)),
    structure_50plus_unit = suppressWarnings(as.numeric(b25024_009e)),
    structure_mobile_home = suppressWarnings(as.numeric(b25024_010e)),
    structure_other = suppressWarnings(as.numeric(b25024_011e))
  )

tract_year_df <- b25002_df %>%
  left_join(b25003_df, by = c("year", "geo_id")) %>%
  left_join(b25024_df, by = c("year", "geo_id")) %>%
  left_join(county_lookup, by = "county") %>%
  mutate(
    homeowner_share = owner_occupied_units / occupied_units,
    vacancy_share = vacant_units / total_housing_units,
    structure_1unit = rowSums(cbind(structure_1unit_detached, structure_1unit_attached), na.rm = TRUE),
    structure_2_4_unit = rowSums(cbind(structure_2_unit, structure_3_4_unit), na.rm = TRUE),
    structure_5plus_unit = rowSums(cbind(structure_5_9_unit, structure_10_19_unit, structure_20_49_unit, structure_50plus_unit), na.rm = TRUE)
  ) %>%
  arrange(year, county, tract)

borough_year_df <- tract_year_df %>%
  group_by(year, borough_name) %>%
  summarise(
    total_housing_units = sum(total_housing_units, na.rm = TRUE),
    occupied_units = sum(occupied_units, na.rm = TRUE),
    vacant_units = sum(vacant_units, na.rm = TRUE),
    owner_occupied_units = sum(owner_occupied_units, na.rm = TRUE),
    renter_occupied_units = sum(renter_occupied_units, na.rm = TRUE),
    structure_1unit = sum(structure_1unit, na.rm = TRUE),
    structure_2_4_unit = sum(structure_2_4_unit, na.rm = TRUE),
    structure_5plus_unit = sum(structure_5plus_unit, na.rm = TRUE),
    homeowner_share = sum(owner_occupied_units, na.rm = TRUE) / sum(occupied_units, na.rm = TRUE),
    vacancy_share = sum(vacant_units, na.rm = TRUE) / sum(total_housing_units, na.rm = TRUE),
    .groups = "drop"
  )

city_year_df <- tract_year_df %>%
  group_by(year) %>%
  summarise(
    total_housing_units = sum(total_housing_units, na.rm = TRUE),
    occupied_units = sum(occupied_units, na.rm = TRUE),
    vacant_units = sum(vacant_units, na.rm = TRUE),
    owner_occupied_units = sum(owner_occupied_units, na.rm = TRUE),
    renter_occupied_units = sum(renter_occupied_units, na.rm = TRUE),
    structure_1unit = sum(structure_1unit, na.rm = TRUE),
    structure_2_4_unit = sum(structure_2_4_unit, na.rm = TRUE),
    structure_5plus_unit = sum(structure_5plus_unit, na.rm = TRUE),
    homeowner_share = sum(owner_occupied_units, na.rm = TRUE) / sum(occupied_units, na.rm = TRUE),
    vacancy_share = sum(vacant_units, na.rm = TRUE) / sum(total_housing_units, na.rm = TRUE),
    .groups = "drop"
  )

qc_df <- tract_year_df %>%
  group_by(year) %>%
  summarise(
    tract_count = n(),
    county_count = n_distinct(county),
    nonmissing_owner_share = mean(!is.na(homeowner_share)),
    nonmissing_vacancy_share = mean(!is.na(vacancy_share)),
    housing_balance_gap = sum(total_housing_units - occupied_units - vacant_units, na.rm = TRUE),
    status = if_else(n_distinct(county) == 5, "staged", "review_required"),
    .groups = "drop"
  )

index_df <- tibble(
  source_id = "census_acs_housing_tract_5yr",
  parquet_path = out_tract_year_parquet,
  borough_year_parquet = out_borough_year_parquet,
  city_year_parquet = out_city_year_parquet,
  year_start = min(tract_year_df$year, na.rm = TRUE),
  year_end = max(tract_year_df$year, na.rm = TRUE),
  status = "staged"
)

write_parquet_if_changed(tract_year_df, out_tract_year_parquet)
write_parquet_if_changed(borough_year_df, out_borough_year_parquet)
write_parquet_if_changed(city_year_df, out_city_year_parquet)
write_csv(index_df, out_index_csv, na = "")
write_csv(qc_df, out_qc_csv, na = "")
cat("Wrote Census ACS housing staging outputs to", dirname(out_index_csv), "\n")
