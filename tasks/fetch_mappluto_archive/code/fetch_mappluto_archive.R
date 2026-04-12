# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_mappluto_archive/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# out_files_csv <- "../output/mappluto_files.csv"
# out_checksums_csv <- "../output/mappluto_checksums.csv"
# out_provenance_csv <- "../output/mappluto_provenance.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(jsonlite)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4) {
  stop("Expected 4 arguments: source_catalog_csv out_files_csv out_checksums_csv out_provenance_csv")
}

source_catalog_csv <- args[1]
out_files_csv <- args[2]
out_checksums_csv <- args[3]
out_provenance_csv <- args[4]

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
pluto_row <- source_catalog |> filter(source_id == "dcp_pluto_current")
mappluto_current_row <- source_catalog |> filter(source_id == "dcp_mappluto_current")
mappluto_archive_row <- source_catalog |> filter(source_id == "dcp_mappluto_archive")

if (nrow(pluto_row) != 1 || nrow(mappluto_current_row) != 1 || nrow(mappluto_archive_row) != 1) {
  stop("Source catalog must contain dcp_pluto_current, dcp_mappluto_current, and dcp_mappluto_archive.")
}

pull_date <- format(Sys.Date(), "%Y%m%d")

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

discovery_current_path <- file.path("..", "..", "..", "data_raw", "dcp_mappluto_current", pull_date, "mappluto_planning_content.json")
discovery_pluto_path <- file.path("..", "..", "..", "data_raw", "dcp_pluto_current", pull_date, "mappluto_planning_content.json")
archive_json_path <- file.path("..", "..", "..", "data_raw", "dcp_mappluto_archive", pull_date, "mappluto_archive_index.json")

discovery_current_status <- if (file.exists(discovery_current_path)) "already_present" else download_with_status(mappluto_current_row$official_url[1], discovery_current_path)
discovery_pluto_status <- if (file.exists(discovery_pluto_path)) "already_present" else download_with_status(pluto_row$official_url[1], discovery_pluto_path)
archive_json_status <- if (file.exists(archive_json_path)) "already_present" else download_with_status(mappluto_archive_row$official_url[1], archive_json_path)

if (!file.exists(discovery_current_path) || !file.exists(archive_json_path)) {
  stop("Required DCP provenance JSON files were not downloaded successfully.")
}

discovery_json <- fromJSON(discovery_current_path, simplifyVector = FALSE)
archive_json <- fromJSON(archive_json_path, simplifyVector = FALSE)
description_text <- discovery_json$description

current_release <- str_match(description_text, "Latest Release:\\s*([0-9]{2}v[0-9](?:\\.[0-9]+)?)")[, 2]

if (is.na(current_release)) {
  stop("Could not parse the current DCP release tag from the Planning content API response.")
}

pluto_zip_url <- str_extract(description_text, "https://[^\\\"]+nyc_pluto_[^\\\"]+_csv\\.zip")
pluto_dictionary_url <- str_extract(description_text, "https://[^\\\"]+pluto_datadictionary\\.pdf")
pluto_readme_url <- str_extract(description_text, "https://[^\\\"]+pluto_readme\\.pdf")
mappluto_zip_urls <- str_extract_all(description_text, "https://[^\\\"]+nyc_mappluto_[^\\\"]+_shp\\.zip")[[1]]
mappluto_zip_url <- mappluto_zip_urls[!str_detect(mappluto_zip_urls, "unclipped")][1]
mappluto_metadata_url <- str_extract(description_text, "https://[^\\\"]+meta_mappluto\\.pdf")

if (any(is.na(c(pluto_zip_url, pluto_dictionary_url, pluto_readme_url, mappluto_zip_url, mappluto_metadata_url)))) {
  stop("Could not parse one or more required current-release DCP asset URLs from the Planning content API response.")
}

archive_rows <- archive_json[vapply(archive_json, function(x) identical(x$dataset, "MapPLUTO™ - Shapefile"), logical(1))]

if (length(archive_rows) == 0) {
  stop("Could not find archived MapPLUTO shapefile releases in the DCP archive JSON.")
}

archive_release_rows <- bind_rows(lapply(archive_rows, function(row) {
  bind_rows(lapply(row$releases, function(release_row) {
    tibble(
      archive_year = as.character(row$year),
      release = as.character(release_row$text),
      official_url = as.character(release_row$link)
    )
  }))
}))

if (nrow(archive_release_rows) == 0) {
  stop("The DCP archive JSON returned zero archived MapPLUTO shapefile releases.")
}

inventory_rows <- list()
inventory_counter <- 0L

inventory_counter <- inventory_counter + 1L
inventory_rows[[inventory_counter]] <- tibble(
  source_id = "dcp_pluto_current",
  vintage = pull_date,
  pull_date = pull_date,
  file_role = "discovery_json",
  raw_path = discovery_pluto_path,
  status = discovery_pluto_status,
  official_url = pluto_row$official_url[1]
)

inventory_counter <- inventory_counter + 1L
inventory_rows[[inventory_counter]] <- tibble(
  source_id = "dcp_mappluto_current",
  vintage = pull_date,
  pull_date = pull_date,
  file_role = "discovery_json",
  raw_path = discovery_current_path,
  status = discovery_current_status,
  official_url = mappluto_current_row$official_url[1]
)

inventory_counter <- inventory_counter + 1L
inventory_rows[[inventory_counter]] <- tibble(
  source_id = "dcp_mappluto_archive",
  vintage = pull_date,
  pull_date = pull_date,
  file_role = "archive_json",
  raw_path = archive_json_path,
  status = archive_json_status,
  official_url = mappluto_archive_row$official_url[1]
)

current_asset_rows <- tribble(
  ~source_id, ~vintage, ~file_role, ~official_url, ~raw_path,
  "dcp_pluto_current", current_release, "pluto_csv_zip", pluto_zip_url, file.path("..", "..", "..", "data_raw", "dcp_pluto_current", current_release, basename(pluto_zip_url)),
  "dcp_pluto_current", current_release, "pluto_data_dictionary_pdf", pluto_dictionary_url, file.path("..", "..", "..", "data_raw", "dcp_pluto_current", current_release, basename(pluto_dictionary_url)),
  "dcp_pluto_current", current_release, "pluto_readme_pdf", pluto_readme_url, file.path("..", "..", "..", "data_raw", "dcp_pluto_current", current_release, basename(pluto_readme_url)),
  "dcp_mappluto_current", current_release, "mappluto_shapefile_zip", mappluto_zip_url, file.path("..", "..", "..", "data_raw", "dcp_mappluto_current", current_release, basename(mappluto_zip_url)),
  "dcp_mappluto_current", current_release, "mappluto_metadata_pdf", mappluto_metadata_url, file.path("..", "..", "..", "data_raw", "dcp_mappluto_current", current_release, basename(mappluto_metadata_url))
)

for (i in seq_len(nrow(current_asset_rows))) {
  asset_row <- current_asset_rows[i, ]
  asset_status <- download_mappluto_asset(asset_row$official_url, asset_row$raw_path)

  inventory_counter <- inventory_counter + 1L
  inventory_rows[[inventory_counter]] <- tibble(
    source_id = asset_row$source_id,
    vintage = asset_row$vintage,
    pull_date = pull_date,
    file_role = asset_row$file_role,
    raw_path = asset_row$raw_path,
    status = asset_status,
    official_url = asset_row$official_url
  )
}

for (i in seq_len(nrow(archive_release_rows))) {
  archive_release <- archive_release_rows[i, ]
  archive_raw_path <- file.path(
    "..", "..", "..", "data_raw", "dcp_mappluto_archive", archive_release$release, basename(archive_release$official_url)
  )
  archive_status <- download_mappluto_asset(archive_release$official_url, archive_raw_path)

  inventory_counter <- inventory_counter + 1L
  inventory_rows[[inventory_counter]] <- tibble(
    source_id = "dcp_mappluto_archive",
    vintage = archive_release$release,
    pull_date = pull_date,
    file_role = "mappluto_shapefile_zip",
    raw_path = archive_raw_path,
    status = archive_status,
    official_url = archive_release$official_url
  )
}

file_inventory <- bind_rows(inventory_rows) |>
  arrange(source_id, vintage, file_role, raw_path)

checksum_table <- file_inventory |>
  mutate(
    file_exists = file.exists(raw_path),
    checksum_sha256 = if_else(file_exists, vapply(raw_path, compute_sha256, character(1)), NA_character_)
  ) |>
  select(source_id, vintage, pull_date, file_role, raw_path, checksum_sha256)

provenance_table <- bind_rows(
  tibble(
    source_id = "dcp_pluto_current",
    pull_date = pull_date,
    current_release = current_release,
    metadata_path = discovery_pluto_path,
    metadata_kind = "planning_content_api",
    note = "Current PLUTO release discovered from the official DCP Planning content API page."
  ),
  tibble(
    source_id = "dcp_mappluto_current",
    pull_date = pull_date,
    current_release = current_release,
    metadata_path = discovery_current_path,
    metadata_kind = "planning_content_api",
    note = "Current MapPLUTO release discovered from the official DCP Planning content API page."
  ),
  tibble(
    source_id = "dcp_mappluto_archive",
    pull_date = pull_date,
    current_release = NA_character_,
    metadata_path = archive_json_path,
    metadata_kind = "dcp_archive_json",
    note = paste("Archive JSON listed", nrow(archive_release_rows), "official archived MapPLUTO shapefile releases.")
  )
)

write_csv(file_inventory, out_files_csv, na = "")
write_csv(checksum_table, out_checksums_csv, na = "")
write_csv(provenance_table, out_provenance_csv, na = "")

cat("Wrote DCP PLUTO and MapPLUTO fetch outputs to", dirname(out_files_csv), "\n")
