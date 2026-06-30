# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_mappluto_archive/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(jsonlite)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

source_catalog <- read_csv("../input/source_catalog.csv", show_col_types = FALSE, na = c("", "NA"))
mappluto_current_row <- source_catalog |>
  filter(source_id == "dcp_mappluto_current")

if (nrow(mappluto_current_row) != 1) {
  stop("Source catalog must contain one dcp_mappluto_current row.")
}

pull_date <- resolve_raw_pull_date(list(
  dcp_mappluto_current = "mappluto_planning_content.json"
))

zip_is_valid <- function(path) {
  if (!file.exists(path)) {
    return(FALSE)
  }

  status <- suppressWarnings(system2("unzip", c("-Z1", path), stdout = FALSE, stderr = FALSE))
  identical(status, 0L)
}

asset_is_valid <- function(path) {
  if (!file.exists(path)) {
    return(FALSE)
  }

  if (str_detect(tolower(path), "\\.zip$")) {
    return(zip_is_valid(path))
  }

  isTRUE(file.info(path)$size > 0)
}

download_mappluto_asset <- function(url, dest_path) {
  had_existing_file <- file.exists(dest_path)

  if (asset_is_valid(dest_path)) {
    return("already_present")
  }

  if (had_existing_file) {
    unlink(dest_path)
  }

  download_status <- download_with_status(url, dest_path)

  if (download_status == "download_failed" || !asset_is_valid(dest_path)) {
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

discovery_current_path <- file.path(
  "..", "..", "..", "data_raw", "dcp_mappluto_current", pull_date, "mappluto_planning_content.json"
)

discovery_current_status <- if (file.exists(discovery_current_path)) {
  "already_present"
} else {
  download_with_status(mappluto_current_row$official_url[[1]], discovery_current_path)
}

if (!file.exists(discovery_current_path)) {
  stop("Required DCP MapPLUTO discovery JSON was not downloaded successfully.")
}

discovery_json <- fromJSON(discovery_current_path, simplifyVector = FALSE)
description_text <- discovery_json$description
current_release <- str_match(description_text, "Latest Release:\\s*([0-9]{2}v[0-9](?:\\.[0-9]+)?)")[, 2]

if (is.na(current_release)) {
  stop("Could not parse the current DCP release tag from the Planning content API response.")
}

if (current_release != "25v4") {
  stop(paste0(
    "Expected DCP MapPLUTO current release 25v4 for the paper pipeline, but the Planning content API reports ",
    current_release,
    ". Update the pinned MapPLUTO release logic before rebuilding the paper."
  ))
}

mappluto_zip_urls <- str_extract_all(description_text, "https://[^\\\"]+nyc_mappluto_[^\\\"]+_shp\\.zip")[[1]]
mappluto_zip_url <- mappluto_zip_urls[!str_detect(mappluto_zip_urls, "unclipped")][1]

if (is.na(mappluto_zip_url)) {
  stop("Could not parse the current-release MapPLUTO shapefile URL from the Planning content API response.")
}

mappluto_zip_path <- file.path(
  "..", "..", "..", "data_raw", "dcp_mappluto_current", current_release, basename(mappluto_zip_url)
)
mappluto_zip_status <- download_mappluto_asset(mappluto_zip_url, mappluto_zip_path)

file_inventory <- bind_rows(
  tibble(
    source_id = "dcp_mappluto_current",
    vintage = pull_date,
    pull_date = pull_date,
    file_role = "discovery_json",
    raw_path = discovery_current_path,
    status = discovery_current_status,
    official_url = mappluto_current_row$official_url[[1]]
  ),
  tibble(
    source_id = "dcp_mappluto_current",
    vintage = current_release,
    pull_date = pull_date,
    file_role = "mappluto_shapefile_zip",
    raw_path = mappluto_zip_path,
    status = mappluto_zip_status,
    official_url = mappluto_zip_url
  )
) |>
  arrange(source_id, vintage, file_role, raw_path)

write_csv_if_changed(file_inventory, "../output/mappluto_files.csv")

cat("Wrote DCP MapPLUTO file inventory to ../output\n")
