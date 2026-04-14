# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_nhgis/code")
# nhgis_raw_files_csv <- "../input/nhgis_raw_files.csv"
# nhgis_table_map_csv <- "../input/nhgis_table_map.csv"
# out_index_csv <- "../output/nhgis_files.csv"
# out_qc_csv <- "../output/nhgis_qc.csv"
# out_reconciliation_csv <- "../output/nhgis_1980_reconciliation.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 5) {
  stop("Expected 5 arguments: nhgis_raw_files_csv nhgis_table_map_csv out_index_csv out_qc_csv out_reconciliation_csv")
}

nhgis_raw_files_csv <- args[1]
nhgis_table_map_csv <- args[2]
out_index_csv <- args[3]
out_qc_csv <- args[4]
out_reconciliation_csv <- args[5]

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

nhgis_table_map <- read_csv(nhgis_table_map_csv, show_col_types = FALSE, na = c("", "NA"))
nhgis_raw_files <- read_csv(nhgis_raw_files_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(year = as.integer(year))

index_rows <- list()
qc_rows <- list()
reconciliation_rows <- list()

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

    qc_rows[[i]] <- tibble(
      source_id = row$source_id,
      year = row$year,
      status = row$status,
      row_count = NA_real_,
      homeowner_share_mean = NA_real_,
      missing_nhgis_codes = NA_character_,
      vacancy_status_gap_sum = NA_real_,
      reconciled_housing_balance_gap_sum = NA_real_,
      unresolved_tract_count = NA_real_
    )
    next
  }

  nhgis_df <- read_parquet(row$raw_parquet_path) %>%
    as.data.frame() %>%
    as_tibble()
  year_map <- nhgis_table_map %>% filter(year == row$year)
  missing_codes <- year_map$nhgis_code[!normalize_names(year_map$nhgis_code) %in% names(nhgis_df)]

  staged_df <- tibble(
    source_id = row$source_id,
    year = row$year,
    gisjoin = pick_first_existing(nhgis_df, c("gisjoin")),
    statea = pick_first_existing(nhgis_df, c("statea")),
    countya = pick_first_existing(nhgis_df, c("countya")),
    tracta = pick_first_existing(nhgis_df, c("tracta", "tract")),
    total_housing_units = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "total_housing_units"]),
    owner_occupied_units = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "owner_occupied_units"]),
    renter_occupied_units = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "renter_occupied_units"]),
    vacant_units_status_sum = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "vacant_units"]),
    white_population = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "white_population"]),
    black_population = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "black_population"]),
    native_population = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "native_population"]),
    asian_pacific_islander_population = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "asian_pacific_islander_population"]),
    other_race_population = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "other_race_population"]),
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

  staged_df$occupied_units <- sum_fields(staged_df, c("owner_occupied_units", "renter_occupied_units"))
  staged_df$total_population <- sum_fields(
    staged_df,
    c("white_population", "black_population", "native_population", "asian_pacific_islander_population", "other_race_population")
  )
  staged_df$structure_1unit <- sum_fields(staged_df, c("structure_1unit_detached", "structure_1unit_attached"))
  staged_df$structure_2_4_unit <- sum_fields(staged_df, c("structure_2_unit", "structure_3_4_unit"))
  staged_df$vacancy_status_gap <- staged_df$total_housing_units - staged_df$occupied_units - staged_df$vacant_units_status_sum
  if (row$year == 1980) {
    staged_df$vacant_units <- staged_df$total_housing_units - staged_df$occupied_units
  } else {
    staged_df$vacant_units <- staged_df$vacant_units_status_sum
  }
  staged_df$vacant_units_source <- ifelse(
    row$year == 1980 & !is.na(staged_df$vacancy_status_gap) & staged_df$vacancy_status_gap != 0,
    "reconciled_total_minus_occupied",
    "nhgis_vacancy_status_table"
  )
  staged_df$homeowner_share <- staged_df$owner_occupied_units / staged_df$occupied_units
  staged_df$reconciled_housing_balance_gap <- staged_df$total_housing_units - staged_df$occupied_units - staged_df$vacant_units
  staged_df$zero_population_flag <- !is.na(staged_df$total_population) & staged_df$total_population == 0
  staged_df$zero_housing_flag <- !is.na(staged_df$total_housing_units) & staged_df$total_housing_units == 0
  staged_df$zero_income_flag <- !is.na(staged_df$median_household_income) & staged_df$median_household_income == 0
  staged_df$housing_balance_classification <- ifelse(
    row$year != 1980 | staged_df$vacancy_status_gap == 0,
    "balanced",
    ifelse(
      staged_df$zero_population_flag & staged_df$zero_housing_flag,
      "nonstandard_tract",
      ifelse(staged_df$vacancy_status_gap > 0, "table_omission", "concept_mismatch")
    )
  )
  staged_df$income_classification <- ifelse(
    is.na(staged_df$median_household_income),
    "missing_income",
    ifelse(
      staged_df$median_household_income > 0,
      "positive_income",
      ifelse(staged_df$occupied_units == 0, "valid_zero_universe", "unresolved")
    )
  )
  staged_df$unresolved_flag <- staged_df$housing_balance_classification == "concept_mismatch" | staged_df$income_classification == "unresolved"

  out_parquet_local <- file.path("..", "output", paste0(row$source_id, ".parquet"))
  out_parquet <- file.path("..", "..", "stage_nhgis", "output", paste0(row$source_id, ".parquet"))
  write_parquet_if_changed(staged_df, out_parquet_local)

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

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    year = row$year,
    status = "staged",
    row_count = nrow(staged_df),
    homeowner_share_mean = mean(staged_df$homeowner_share, na.rm = TRUE),
    missing_nhgis_codes = if (length(missing_codes) == 0) NA_character_ else paste(missing_codes, collapse = ";"),
    vacancy_status_gap_sum = sum(staged_df$vacancy_status_gap, na.rm = TRUE),
    reconciled_housing_balance_gap_sum = sum(staged_df$reconciled_housing_balance_gap, na.rm = TRUE),
    unresolved_tract_count = sum(staged_df$unresolved_flag, na.rm = TRUE)
  )

  if (row$year == 1980) {
    reconciliation_rows[[length(reconciliation_rows) + 1L]] <- staged_df %>%
      transmute(
        year,
        gisjoin,
        countya,
        tracta,
        total_housing_units,
        occupied_units,
        vacant_units_status_sum,
        vacant_units_reconciled = vacant_units,
        vacancy_status_gap,
        housing_balance_classification,
        zero_population_flag,
        zero_housing_flag,
        zero_income_flag,
        income_classification,
        unresolved_flag
      )
  }
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
write_csv(bind_rows(reconciliation_rows), out_reconciliation_csv, na = "")
cat("Wrote NHGIS staging outputs to", dirname(out_index_csv), "\n")
