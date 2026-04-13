# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_nhgis_raw/code")
# nhgis_extract_downloads_csv <- "../input/nhgis_extract_downloads.csv"
# nhgis_table_map_csv <- "../input/nhgis_table_map.csv"
# out_index_csv <- "../output/nhgis_raw_files.csv"
# out_qc_csv <- "../output/nhgis_raw_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4) {
  stop("Expected 4 arguments: nhgis_extract_downloads_csv nhgis_table_map_csv out_index_csv out_qc_csv")
}

nhgis_extract_downloads_csv <- args[1]
nhgis_table_map_csv <- args[2]
out_index_csv <- args[3]
out_qc_csv <- args[4]

nhgis_table_map <- read_csv(nhgis_table_map_csv, show_col_types = FALSE, na = c("", "NA"))
nhgis_extract_downloads <- if (file.exists(nhgis_extract_downloads_csv)) {
  read_csv(nhgis_extract_downloads_csv, show_col_types = FALSE, na = c("", "NA"))
} else {
  tibble(
    source_id = character(),
    year = integer(),
    extract_number = integer(),
    extract_status = character(),
    file_role = character(),
    raw_path = character(),
    checksum_sha256 = character(),
    status = character()
  )
}

expected_rows <- tibble(year = sort(unique(nhgis_table_map$year))) |>
  mutate(source_id = paste0("nhgis_", year, "_tract_extract"))

nyc_counties <- c("005", "047", "061", "081", "085")

index_rows <- list()
qc_rows <- list()

for (i in seq_len(nrow(expected_rows))) {
  row <- expected_rows[i, ]
  source_files <- nhgis_extract_downloads |>
    filter(source_id == row$source_id, !is.na(raw_path), file.exists(raw_path))

  table_zip <- source_files |>
    filter(file_role == "table_data") |>
    slice_head(n = 1) |>
    pull(raw_path)

  gis_zip <- source_files |>
    filter(file_role == "gis_data") |>
    slice_head(n = 1) |>
    pull(raw_path)

  missing_status <- if (nrow(source_files) == 0) {
    "fetch_required"
  } else if (any(str_detect(source_files$status, "failed"))) {
    "fetch_failed"
  } else {
    "bundle_incomplete"
  }

  if (length(table_zip) == 0 || length(gis_zip) == 0) {
    index_rows[[i]] <- tibble(
      source_id = row$source_id,
      year = row$year,
      table_zip_path = if (length(table_zip) == 0) NA_character_ else table_zip[[1]],
      gis_zip_path = if (length(gis_zip) == 0) NA_character_ else gis_zip[[1]],
      table_file_inside_zip = NA_character_,
      shapefile_inside_zip = NA_character_,
      raw_parquet_path = NA_character_,
      status = missing_status
    )

    qc_rows[[i]] <- tibble(
      source_id = row$source_id,
      status = missing_status,
      row_count = NA_real_,
      validation_notes = "Run tasks/fetch_nhgis_extracts before loading NHGIS outputs."
    )
    next
  }

  table_zip <- table_zip[[1]]
  gis_zip <- gis_zip[[1]]
  table_listing <- unzip(table_zip, list = TRUE)
  gis_listing <- unzip(gis_zip, list = TRUE)

  table_candidates <- table_listing$Name[
    str_detect(tolower(table_listing$Name), "\\.(csv|dat)$") &
      !str_detect(tolower(table_listing$Name), "(_datadict|_geog|_tables)\\.csv$")
  ]
  shapefile_candidates <- gis_listing$Name[
    str_detect(tolower(gis_listing$Name), "\\.shp$|shapefile.*\\.zip$")
  ]
  has_expected_shape <- any(str_detect(
    tolower(c(basename(gis_zip), shapefile_candidates)),
    paste0(
      "tract.*", row$year, ".*tl2000|",
      row$year, ".*tract.*tl2000|",
      "us_tract_", row$year, "_tl2000|",
      "shapefile.*tl2000.*tract.*", row$year
    )
  ))

  if (length(table_candidates) == 0 || length(shapefile_candidates) == 0 || !has_expected_shape) {
    validation_notes <- c()

    if (length(table_candidates) == 0) {
      validation_notes <- c(validation_notes, "missing_tabular_payload")
    }

    if (length(shapefile_candidates) == 0) {
      validation_notes <- c(validation_notes, "missing_shapefile_payload")
    }

    if (length(shapefile_candidates) > 0 && !has_expected_shape) {
      validation_notes <- c(validation_notes, "unexpected_shapefile_asset")
    }

    index_rows[[i]] <- tibble(
      source_id = row$source_id,
      year = row$year,
      table_zip_path = table_zip,
      gis_zip_path = gis_zip,
      table_file_inside_zip = if (length(table_candidates) == 0) NA_character_ else table_candidates[[1]],
      shapefile_inside_zip = if (length(shapefile_candidates) == 0) NA_character_ else shapefile_candidates[[1]],
      raw_parquet_path = NA_character_,
      status = "bundle_validation_failed"
    )

    qc_rows[[i]] <- tibble(
      source_id = row$source_id,
      status = "bundle_validation_failed",
      row_count = NA_real_,
      validation_notes = paste(validation_notes, collapse = ";")
    )
    next
  }

  table_dfs <- lapply(table_candidates, function(table_file) {
    out <- read_csv(unz(table_zip, table_file), show_col_types = FALSE, guess_max = 50000)
    names(out) <- normalize_names(names(out))
    out
  })

  nhgis_df <- table_dfs[[1]]

  if (length(table_dfs) > 1) {
    for (j in 2:length(table_dfs)) {
      join_keys <- intersect(
        c("gisjoin", "year", "state", "statea", "county", "countya", "tract", "tracta"),
        intersect(names(nhgis_df), names(table_dfs[[j]]))
      )

      if (length(join_keys) == 0) {
        stop("Could not identify NHGIS join keys across multiple dataset CSV files.")
      }

      nhgis_df <- nhgis_df |>
        left_join(
          table_dfs[[j]] |>
            select(any_of(join_keys), any_of(setdiff(names(table_dfs[[j]]), names(nhgis_df)))),
          by = join_keys
        )
    }
  }

  nhgis_df$statea <- pick_first_existing(nhgis_df, c("statea"))
  nhgis_df$countya <- pick_first_existing(nhgis_df, c("countya"))
  nhgis_df$statea_std <- str_pad(str_extract(as.character(nhgis_df$statea), "[0-9]+"), width = 2, side = "left", pad = "0")
  nhgis_df$countya_std <- str_pad(str_extract(as.character(nhgis_df$countya), "[0-9]+"), width = 3, side = "left", pad = "0")
  nhgis_df <- nhgis_df |>
    filter(statea_std == "36", countya_std %in% nyc_counties) |>
    select(-statea_std, -countya_std) |>
    mutate(source_id = row$source_id, source_year = row$year, table_zip_path = table_zip, gis_zip_path = gis_zip)

  out_parquet_local <- file.path("..", "output", paste0(row$source_id, "_raw.parquet"))
  out_parquet <- file.path("..", "..", "load_nhgis_raw", "output", basename(out_parquet_local))
  write_parquet_if_changed(nhgis_df, out_parquet_local)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    year = row$year,
    table_zip_path = table_zip,
    gis_zip_path = gis_zip,
    table_file_inside_zip = table_candidates[[1]],
    shapefile_inside_zip = shapefile_candidates[[1]],
    raw_parquet_path = out_parquet,
    status = "loaded"
  )

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    status = "loaded",
    row_count = nrow(nhgis_df),
    validation_notes = "validated_table_payload_and_tl2000_shapefile"
  )
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote NHGIS raw outputs to", dirname(out_index_csv), "\n")
