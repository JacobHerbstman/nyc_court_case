# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/stage_census_bps/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

bps_files <- read_csv("../input/census_bps_raw_files.csv", show_col_types = FALSE, na = c("", "NA"))
bps_files <- bps_files[!is.na(bps_files$raw_parquet_path) & file.exists(bps_files$raw_parquet_path), ]
bps_files <- bps_files |>
  mutate(year = as.integer(year)) |>
  arrange(year)

if (nrow(bps_files) == 0) {
  write_parquet_if_changed(tibble(), "../output/census_bps_city_year.parquet")
  quit(save = "no")
}

borough_lookup <- tibble(
  county_code = c("005", "047", "061", "081", "085"),
  borough_name = c("Bronx borough", "Brooklyn borough", "Manhattan borough", "Queens borough", "Staten Island borough"),
  place_name_normalized = c("bronx borough", "brooklyn borough", "manhattan borough", "queens borough", "staten island borough")
)

city_rows <- list()

for (i in seq_len(nrow(bps_files))) {
  row <- bps_files[i, ]
  parsed_df <- read_parquet(row$raw_parquet_path) |>
    as.data.frame() |>
    as_tibble()

  if (nrow(parsed_df) == 0) {
    next
  }

  borough_df <- parsed_df |>
    filter(state_code == "36") |>
    left_join(borough_lookup |> rename(expected_county_code = county_code), by = "place_name_normalized", relationship = "many-to-one") |>
    filter(!is.na(borough_name))

  borough_df <- borough_df |>
    mutate(county_matches_name = county_code == expected_county_code)

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
}

city_year_df <- bind_rows(city_rows)

write_parquet_if_changed(city_year_df, "../output/census_bps_city_year.parquet")

cat("Wrote Census BPS staging outputs to ../output\n")
