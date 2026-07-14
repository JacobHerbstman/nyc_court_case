# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_mappluto_archive/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

source_catalog <- read_csv("../input/source_catalog.csv", show_col_types = FALSE, na = c("", "NA"))
mappluto_row <- source_catalog |>
  filter(source_id == "dcp_mappluto_current", vintage == "25v4")

if (nrow(mappluto_row) != 1) {
  stop("Source catalog must contain one pinned dcp_mappluto_current 25v4 row.")
}

zip_is_valid <- function(path) {
  if (!file.exists(path)) {
    return(FALSE)
  }

  status <- suppressWarnings(system2("unzip", c("-Z1", path), stdout = FALSE, stderr = FALSE))
  identical(status, 0L)
}

download_mappluto_asset <- function(url, dest_path) {
  had_existing_file <- file.exists(dest_path)

  if (zip_is_valid(dest_path)) {
    return("already_present")
  }

  if (had_existing_file) {
    unlink(dest_path)
  }

  download_status <- download_with_status(url, dest_path)

  if (download_status == "download_failed" || !zip_is_valid(dest_path)) {
    if (file.exists(dest_path)) {
      unlink(dest_path)
    }
    return("download_failed_validation")
  }

  if (had_existing_file) {
    return("redownloaded_after_validation_failure")
  }

  download_status
}

mappluto_zip_path <- file.path(
  "..", "..", "..", mappluto_row$raw_subdir[[1]], mappluto_row$expected_filename[[1]]
)
mappluto_zip_status <- download_mappluto_asset(mappluto_row$official_url[[1]], mappluto_zip_path)

file_inventory <- tibble(
  source_id = "dcp_mappluto_current",
  vintage = "25v4",
  pull_date = NA_character_,
  file_role = "mappluto_shapefile_zip",
  raw_path = mappluto_zip_path,
  status = mappluto_zip_status,
  official_url = mappluto_row$official_url[[1]]
) |>
  arrange(source_id, vintage, file_role, raw_path)

write_csv_if_changed(file_inventory, "../output/mappluto_files.csv")

cat("Wrote DCP MapPLUTO file inventory to ../output\n")
