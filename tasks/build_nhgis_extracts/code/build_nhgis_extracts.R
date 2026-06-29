# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_nhgis_extracts/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

sum_codes <- function(df, codes) {
  hits <- normalize_names(codes)
  hits <- hits[hits %in% names(df)]

  if (length(hits) == 0) {
    return(rep(NA_real_, nrow(df)))
  }

  value_matrix <- sapply(hits, function(hit_name) suppressWarnings(as.numeric(df[[hit_name]])))
  if (length(hits) == 1) {
    value_matrix <- matrix(value_matrix, ncol = 1)
  }

  out <- rowSums(value_matrix, na.rm = TRUE)
  out[rowSums(!is.na(value_matrix)) == 0] <- NA_real_
  out
}

pull_code <- function(df, code) {
  hit <- normalize_names(code)
  if (!hit %in% names(df)) {
    return(rep(NA_real_, nrow(df)))
  }

  suppressWarnings(as.numeric(df[[hit]]))
}

sum_fields <- function(df, fields) {
  value_matrix <- sapply(fields, function(field_name) suppressWarnings(as.numeric(df[[field_name]])))
  if (length(fields) == 1) {
    value_matrix <- matrix(value_matrix, ncol = 1)
  }

  out <- rowSums(value_matrix, na.rm = TRUE)
  out[rowSums(!is.na(value_matrix)) == 0] <- NA_real_
  out
}

nhgis_table_map <- read_csv("nhgis_table_map.csv", show_col_types = FALSE, na = c("", "NA"))
nhgis_income_overrides <- read_csv("nhgis_income_overrides.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    year = as.integer(year),
    gisjoin = as.character(gisjoin),
    override_income_classification = as.character(override_income_classification),
    override_reason = as.character(override_reason)
  )

nhgis_raw_files <- read_csv("../output/nhgis_raw_files.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(year = as.integer(year))

index_rows <- list()

for (i in seq_len(nrow(nhgis_raw_files))) {
  row <- nhgis_raw_files[i, ]

  if (!file.exists(row$raw_parquet_path)) {
    index_rows[[i]] <- tibble(
      source_id = row$source_id,
      year = row$year,
      table_zip_path = row$table_zip_path,
      gis_zip_path = row$gis_zip_path,
      table_file_inside_zip = row$table_file_inside_zip,
      shapefile_inside_zip = row$shapefile_inside_zip,
      raw_parquet_path = row$raw_parquet_path,
      parquet_path = NA_character_,
      status = row$status
    )
    next
  }

  nhgis_df <- read_parquet(row$raw_parquet_path) %>%
    as.data.frame() %>%
    as_tibble()
  year_map <- nhgis_table_map %>% filter(year == row$year)

  extract_df <- tibble(
    source_id = row$source_id,
    year = row$year,
    gisjoin = pick_first_existing(nhgis_df, c("gisjoin")),
    statea = pick_first_existing(nhgis_df, c("statea")),
    countya = pick_first_existing(nhgis_df, c("countya")),
    tracta = pick_first_existing(nhgis_df, c("tracta", "tract")),
    households_total = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "households_total"]),
    total_housing_units = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "total_housing_units"]),
    owner_occupied_units = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "owner_occupied_units"]),
    renter_occupied_units = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "renter_occupied_units"]),
    vacant_units_status_sum = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "vacant_units"]),
    white_population = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "white_population"]),
    black_population = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "black_population"]),
    native_population = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "native_population"]),
    asian_pacific_islander_population = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "asian_pacific_islander_population"]),
    other_race_population = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "other_race_population"]),
    group_quarters_population = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "group_quarters_population"]),
    hispanic_any_race = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "hispanic_any_race"]),
    non_hispanic_population = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "non_hispanic_population"]),
    median_household_income = pull_code(nhgis_df, year_map$nhgis_code[year_map$staged_field == "median_household_income"][1]),
    structure_1unit_detached = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "structure_1unit_detached"]),
    structure_1unit_attached = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "structure_1unit_attached"]),
    structure_2_unit = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "structure_2_unit"]),
    structure_3_4_unit = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "structure_3_4_unit"]),
    structure_5plus_unit = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "structure_5plus_unit"]),
    structure_mobile_home_other = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "structure_mobile_home_other"]),
    structure_other = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "structure_other"])
  )

  extract_df$occupied_units <- sum_fields(extract_df, c("owner_occupied_units", "renter_occupied_units"))
  extract_df$total_population <- sum_fields(
    extract_df,
    c("white_population", "black_population", "native_population", "asian_pacific_islander_population", "other_race_population")
  )
  extract_df$non_group_quarters_population <- extract_df$total_population - extract_df$group_quarters_population
  extract_df$group_quarters_population_share <- extract_df$group_quarters_population / extract_df$total_population
  extract_df$household_count_gap <- extract_df$households_total - extract_df$occupied_units
  extract_df$structure_1unit <- sum_fields(extract_df, c("structure_1unit_detached", "structure_1unit_attached"))
  extract_df$structure_2_4_unit <- sum_fields(extract_df, c("structure_2_unit", "structure_3_4_unit"))
  extract_df$vacancy_status_gap <- extract_df$total_housing_units - extract_df$occupied_units - extract_df$vacant_units_status_sum
  extract_df$vacant_units <- extract_df$vacant_units_status_sum
  extract_df$vacant_units_source <- "nhgis_vacancy_status_table"
  extract_df$homeowner_share <- extract_df$owner_occupied_units / extract_df$occupied_units
  extract_df$reconciled_housing_balance_gap <- extract_df$total_housing_units - extract_df$occupied_units - extract_df$vacant_units
  extract_df$zero_population_flag <- !is.na(extract_df$total_population) & extract_df$total_population == 0
  extract_df$zero_housing_flag <- !is.na(extract_df$total_housing_units) & extract_df$total_housing_units == 0
  extract_df$zero_income_flag <- !is.na(extract_df$median_household_income) & extract_df$median_household_income == 0
  extract_df$housing_balance_classification <- ifelse(
    extract_df$vacancy_status_gap == 0,
    "balanced",
    "concept_mismatch"
  )
  extract_df$income_classification <- ifelse(
    is.na(extract_df$median_household_income),
    "missing_income",
    ifelse(
      extract_df$median_household_income > 0,
      "positive_income",
      ifelse(extract_df$occupied_units == 0, "valid_zero_universe", "unresolved")
    )
  )

  extract_df <- extract_df %>%
    left_join(
      nhgis_income_overrides %>%
        filter(year == row$year) %>%
        select(year, gisjoin, override_income_classification, override_reason),
      by = c("year", "gisjoin"),
      relationship = "many-to-one"
    ) %>%
    mutate(
      income_classification = coalesce(override_income_classification, income_classification),
      income_override_reason = override_reason
    ) %>%
    select(-override_income_classification, -override_reason)

  extract_df$unresolved_flag <- extract_df$housing_balance_classification == "concept_mismatch" | extract_df$income_classification == "unresolved"

  out_parquet_local <- file.path("..", "output", paste0(row$source_id, ".parquet"))
  out_parquet <- file.path("..", "..", "build_nhgis_extracts", "output", paste0(row$source_id, ".parquet"))
  write_parquet_if_changed(extract_df, out_parquet_local)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    year = row$year,
    table_zip_path = row$table_zip_path,
    gis_zip_path = row$gis_zip_path,
    table_file_inside_zip = row$table_file_inside_zip,
    shapefile_inside_zip = row$shapefile_inside_zip,
    raw_parquet_path = row$raw_parquet_path,
    parquet_path = out_parquet,
    status = "staged"
  )
}

write_csv(bind_rows(index_rows), "../output/nhgis_files.csv", na = "")
cat("Wrote NHGIS extract outputs to ../output\n")
