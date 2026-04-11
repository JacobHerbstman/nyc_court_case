# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_nhgis/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# manual_manifest_csv <- "../input/manual_manifest.csv"
# out_index_csv <- "../output/nhgis_files.csv"
# out_qc_csv <- "../output/nhgis_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4) {
  stop("Expected 4 arguments: source_catalog_csv manual_manifest_csv out_index_csv out_qc_csv")
}

source_catalog_csv <- args[1]
manual_manifest_csv <- args[2]
out_index_csv <- args[3]
out_qc_csv <- args[4]

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
manual_manifest <- read_csv(manual_manifest_csv, show_col_types = FALSE, na = c("", "NA"))

summary_rows <- source_catalog |> filter(str_detect(source_id, "^nhgis_[0-9]{4}_tract_summary$"))

index_rows <- list()
qc_rows <- list()

for (i in seq_len(nrow(summary_rows))) {
  row <- summary_rows[i, ]
  raw_files <- collect_raw_files(row$source_id)
  raw_path <- raw_files[str_detect(raw_files, "\\.csv$")][1]

  if (is.na(raw_path)) {
    index_rows[[i]] <- tibble(
      source_id = row$source_id,
      parquet_path = NA_character_,
      raw_path = NA_character_,
      status = "manual_download_required"
    )
    qc_rows[[i]] <- tibble(
      source_id = row$source_id,
      status = "manual_download_required",
      row_count = NA_real_,
      homeowner_share_mean = NA_real_
    )
    next
  }

  nhgis_df <- read_csv(raw_path, show_col_types = FALSE, guess_max = 20000)
  names(nhgis_df) <- normalize_names(names(nhgis_df))

  year_value <- suppressWarnings(as.integer(str_extract(row$source_id, "[0-9]{4}")))

  staged_df <- tibble(
    source_id = row$source_id,
    year = year_value,
    tract_id = pick_first_existing(nhgis_df, c("gisjoin", "geoid", "tract_id", "tract")),
    total_population = pick_first_existing(nhgis_df, c("total_population", "population", "pop_total")),
    occupied_units = pick_first_existing(nhgis_df, c("occupied_units", "total_occupied_housing_units")),
    owner_occupied_units = pick_first_existing(nhgis_df, c("owner_occupied_units")),
    renter_occupied_units = pick_first_existing(nhgis_df, c("renter_occupied_units")),
    vacant_units = pick_first_existing(nhgis_df, c("vacant_units")),
    median_household_income = pick_first_existing(nhgis_df, c("median_household_income")),
    white_non_hispanic = pick_first_existing(nhgis_df, c("white_non_hispanic")),
    black_non_hispanic = pick_first_existing(nhgis_df, c("black_non_hispanic")),
    hispanic_any_race = pick_first_existing(nhgis_df, c("hispanic_any_race")),
    other_non_hispanic = pick_first_existing(nhgis_df, c("other_non_hispanic")),
    structure_1unit = pick_first_existing(nhgis_df, c("structure_1unit", "structure_1_unit")),
    structure_2_4_unit = pick_first_existing(nhgis_df, c("structure_2_4_unit", "structure_2_to_4_units")),
    structure_5plus_unit = pick_first_existing(nhgis_df, c("structure_5plus_unit", "structure_5_or_more_units"))
  )

  occupied_num <- suppressWarnings(as.numeric(staged_df$occupied_units))
  owner_num <- suppressWarnings(as.numeric(staged_df$owner_occupied_units))
  staged_df$homeowner_share <- owner_num / occupied_num

  out_parquet <- file.path("..", "output", paste0(row$source_id, ".parquet"))
  write_parquet_if_changed(staged_df, out_parquet)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    parquet_path = out_parquet,
    raw_path = raw_path,
    status = "staged"
  )

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    status = "staged",
    row_count = nrow(staged_df),
    homeowner_share_mean = mean(staged_df$homeowner_share, na.rm = TRUE)
  )
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote NHGIS staging outputs to", dirname(out_index_csv), "\n")
