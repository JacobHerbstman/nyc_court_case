# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_nhgis/code")
# nhgis_raw_files_csv <- "../input/nhgis_raw_files.csv"
# nhgis_table_map_csv <- "../input/nhgis_table_map.csv"
# out_index_csv <- "../output/nhgis_files.csv"
# out_qc_csv <- "../output/nhgis_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4) {
  stop("Expected 4 arguments: nhgis_raw_files_csv nhgis_table_map_csv out_index_csv out_qc_csv")
}

nhgis_raw_files_csv <- args[1]
nhgis_table_map_csv <- args[2]
out_index_csv <- args[3]
out_qc_csv <- args[4]

nhgis_table_map <- read_csv(nhgis_table_map_csv, show_col_types = FALSE, na = c("", "NA"))
nhgis_raw_files <- read_csv(nhgis_raw_files_csv, show_col_types = FALSE, na = c("", "NA"))

sum_codes <- function(df, codes) {
  hits <- normalize_names(codes)
  hits <- hits[hits %in% names(df)]

  if (length(hits) == 0) {
    return(rep(NA_real_, nrow(df)))
  }

  value_matrix <- sapply(hits, function(x) suppressWarnings(as.numeric(df[[x]])))

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
  value_matrix <- sapply(fields, function(x) suppressWarnings(as.numeric(df[[x]])))

  if (length(fields) == 1) {
    value_matrix <- matrix(value_matrix, ncol = 1)
  }

  out <- rowSums(value_matrix, na.rm = TRUE)
  out[rowSums(!is.na(value_matrix)) == 0] <- NA_real_
  out
}

nhgis_rows <- nhgis_raw_files |>
  mutate(year = as.integer(year))

index_rows <- list()
qc_rows <- list()

for (i in seq_len(nrow(nhgis_rows))) {
  row <- nhgis_rows[i, ]
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
      status = row$status,
      row_count = NA_real_,
      homeowner_share_mean = NA_real_,
      missing_nhgis_codes = NA_character_,
      validation_notes = "Run tasks/load_nhgis_raw before staging NHGIS outputs."
    )
    next
  }
  nhgis_df <- read_parquet(row$raw_parquet_path) |>
    as.data.frame() |>
    as_tibble()
  year_map <- nhgis_table_map |> filter(year == row$year)
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
    vacant_units = sum_codes(nhgis_df, year_map$nhgis_code[year_map$staged_field == "vacant_units"]),
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
    c(
      "white_population",
      "black_population",
      "native_population",
      "asian_pacific_islander_population",
      "other_race_population"
    )
  )
  staged_df$structure_1unit <- sum_fields(staged_df, c("structure_1unit_detached", "structure_1unit_attached"))
  staged_df$structure_2_4_unit <- sum_fields(staged_df, c("structure_2_unit", "structure_3_4_unit"))
  staged_df$homeowner_share <- staged_df$owner_occupied_units / staged_df$occupied_units

  out_parquet <- file.path("..", "output", paste0(row$source_id, ".parquet"))
  write_parquet_if_changed(staged_df, out_parquet)

  validation_notes <- if (length(missing_codes) == 0) {
    "validated_table_payload_and_tl2000_shapefile"
  } else {
    paste("validated_table_payload_and_tl2000_shapefile", "missing_codes=", paste(missing_codes, collapse = ";"))
  }

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
    status = "staged",
    row_count = nrow(staged_df),
    homeowner_share_mean = mean(staged_df$homeowner_share, na.rm = TRUE),
    missing_nhgis_codes = if (length(missing_codes) == 0) NA_character_ else paste(missing_codes, collapse = ";"),
    validation_notes = validation_notes
  )
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote NHGIS staging outputs to", dirname(out_index_csv), "\n")
