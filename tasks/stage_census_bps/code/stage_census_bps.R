# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_census_bps/code")
# census_bps_raw_files_csv <- "../input/census_bps_raw_files.csv"
# out_index_csv <- "../output/census_bps_index.csv"
# out_qc_csv <- "../output/census_bps_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: census_bps_raw_files_csv out_index_csv out_qc_csv")
}

census_bps_raw_files_csv <- args[1]
out_index_csv <- args[2]
out_qc_csv <- args[3]

bps_files <- read_csv(census_bps_raw_files_csv, show_col_types = FALSE, na = c("", "NA"))
bps_files <- bps_files[!is.na(bps_files$raw_parquet_path) & file.exists(bps_files$raw_parquet_path), ]
bps_files <- bps_files |>
  mutate(year = as.integer(year)) |>
  arrange(year)

if (nrow(bps_files) == 0) {
  write_csv(tibble(), out_index_csv, na = "")
  write_csv(tibble(status = "no_bps_raw_files"), out_qc_csv, na = "")
  quit(save = "no")
}

borough_lookup <- tibble(
  county_code = c("005", "047", "061", "081", "085"),
  borough_name = c("Bronx borough", "Brooklyn borough", "Manhattan borough", "Queens borough", "Staten Island borough"),
  place_name_normalized = c("bronx borough", "brooklyn borough", "manhattan borough", "queens borough", "staten island borough")
)

index_rows <- list()
qc_rows <- list()
borough_rows <- list()
city_rows <- list()

for (i in seq_len(nrow(bps_files))) {
  row <- bps_files[i, ]
  parsed_df <- read_parquet(row$raw_parquet_path) |>
    as.data.frame() |>
    as_tibble()

  if (nrow(parsed_df) == 0) {
    qc_rows[[i]] <- tibble(
      year = row$year,
      schema_fields = NA_integer_,
      raw_row_count = 0L,
      matched_borough_count = 0L,
      duplicate_match_count = 0L,
      unmatched_expected_count = 5L,
      county_match_share = NA_real_,
      city_total_units = NA_real_,
      status = "no_parseable_rows"
    )
    next
  }

  schema_fields <- parsed_df$schema_fields[1]

  borough_df <- parsed_df |>
    filter(state_code == "36") |>
    left_join(borough_lookup |> rename(expected_county_code = county_code), by = "place_name_normalized") |>
    filter(!is.na(borough_name))

  borough_df <- borough_df |>
    mutate(county_matches_name = county_code == expected_county_code)

  out_parquet_local <- file.path("..", "output", paste0("census_bps_", row$year, ".parquet"))
  out_parquet <- file.path("..", "..", "stage_census_bps", "output", basename(out_parquet_local))
  write_parquet_if_changed(borough_df, out_parquet_local)

  borough_rows[[length(borough_rows) + 1L]] <- borough_df

  city_df <- if (nrow(borough_df) == 0) {
    tibble(
      year = row$year,
      source_raw_path = row$raw_path,
      city_total_units = NA_real_,
      city_one_unit_units = NA_real_,
      city_two_unit_units = NA_real_,
      city_three_four_unit_units = NA_real_,
      city_five_plus_unit_units = NA_real_
    )
  } else {
    borough_df |>
      summarise(
        year = first(year),
        source_raw_path = first(source_raw_path),
        city_total_units = sum(total_units, na.rm = TRUE),
        city_one_unit_units = sum(one_unit_units, na.rm = TRUE),
        city_two_unit_units = sum(two_unit_units, na.rm = TRUE),
        city_three_four_unit_units = sum(three_four_unit_units, na.rm = TRUE),
        city_five_plus_unit_units = sum(five_plus_unit_units, na.rm = TRUE),
        .groups = "drop"
      )
  }

  city_rows[[length(city_rows) + 1L]] <- city_df

  index_rows[[i]] <- tibble(
    year = row$year,
    raw_path = row$raw_path,
    raw_parquet_path = row$raw_parquet_path,
    parquet_path = out_parquet
  )

  qc_rows[[i]] <- tibble(
    year = row$year,
    schema_fields = schema_fields,
    raw_row_count = nrow(parsed_df),
    matched_borough_count = nrow(borough_df),
    duplicate_match_count = sum(duplicated(borough_df$borough_name)),
    unmatched_expected_count = 5L - n_distinct(borough_df$borough_name),
    county_match_share = if (nrow(borough_df) == 0) NA_real_ else mean(borough_df$county_matches_name),
    city_total_units = if (nrow(city_df) == 0) NA_real_ else city_df$city_total_units[1],
    status = if (nrow(borough_df) == 5 && !any(duplicated(borough_df$borough_name))) "ok" else "review_required"
  )
}

borough_year_df <- bind_rows(borough_rows) |>
  select(year, borough_name, county_code, place_name_raw, place_name_normalized, schema_fields, one_unit_units, two_unit_units, three_four_unit_units, five_plus_unit_units, total_units, county_matches_name, source_raw_path)

city_year_df <- bind_rows(city_rows)

write_parquet_if_changed(borough_year_df, file.path("..", "output", "census_bps_borough_year.parquet"))
write_parquet_if_changed(city_year_df, file.path("..", "output", "census_bps_city_year.parquet"))
write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")

cat("Wrote Census BPS staging outputs to", dirname(out_index_csv), "\n")
