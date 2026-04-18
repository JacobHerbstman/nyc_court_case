# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_zap_datasets/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# out_files_csv <- "../output/zap_files.csv"
# out_checksums_csv <- "../output/zap_checksums.csv"
# out_provenance_csv <- "../output/zap_provenance.csv"

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
source_rows <- source_catalog |>
  filter(source_id %in% c("dcp_zap_project_data", "dcp_zap_bbl"))

if (nrow(source_rows) != 2) {
  stop("Source catalog must contain exactly dcp_zap_project_data and dcp_zap_bbl.")
}

pull_date <- format(Sys.Date(), "%Y%m%d")
inventory_rows <- list()
provenance_rows <- list()
inventory_counter <- 0L
provenance_counter <- 0L

for (i in seq_len(nrow(source_rows))) {
  source_row <- source_rows[i, ]
  source_id <- source_row$source_id[[1]]
  metadata_url <- source_row$official_url[[1]]
  dataset_id <- str_match(metadata_url, "([a-z0-9]{4}-[a-z0-9]{4})")[, 2]

  if (is.na(dataset_id)) {
    stop("Could not parse Socrata dataset id from ", metadata_url)
  }

  raw_dir <- file.path("..", "..", "..", "data_raw", source_id, pull_date)
  metadata_json_path <- file.path(raw_dir, paste0(dataset_id, "_metadata.json"))
  rows_csv_url <- paste0("https://data.cityofnewyork.us/api/views/", dataset_id, "/rows.csv?accessType=DOWNLOAD")
  rows_csv_path <- file.path(raw_dir, source_row$expected_filename[[1]])

  metadata_status <- if (file.exists(metadata_json_path)) "already_present" else download_with_status(metadata_url, metadata_json_path)
  csv_status <- if (file.exists(rows_csv_path)) "already_present" else download_with_status(rows_csv_url, rows_csv_path)

  if (!file.exists(metadata_json_path)) {
    stop("Could not download metadata JSON for ", source_id)
  }

  metadata_json <- fromJSON(metadata_json_path, simplifyVector = FALSE)
  attachment_rows <- metadata_json$metadata$attachments
  rows_updated_at <- if (!is.null(metadata_json$rowsUpdatedAt)) {
    as.character(as.POSIXct(as.numeric(metadata_json$rowsUpdatedAt), origin = "1970-01-01", tz = "UTC"))
  } else {
    NA_character_
  }
  view_last_modified <- if (!is.null(metadata_json$viewLastModified)) {
    as.character(as.POSIXct(as.numeric(metadata_json$viewLastModified), origin = "1970-01-01", tz = "UTC"))
  } else {
    NA_character_
  }

  inventory_counter <- inventory_counter + 1L
  inventory_rows[[inventory_counter]] <- tibble(
    source_id = source_id,
    vintage = pull_date,
    pull_date = pull_date,
    file_role = "metadata_json",
    raw_path = metadata_json_path,
    status = metadata_status,
    official_url = metadata_url
  )

  inventory_counter <- inventory_counter + 1L
  inventory_rows[[inventory_counter]] <- tibble(
    source_id = source_id,
    vintage = pull_date,
    pull_date = pull_date,
    file_role = "rows_csv",
    raw_path = rows_csv_path,
    status = csv_status,
    official_url = rows_csv_url
  )

  if (length(attachment_rows) > 0) {
    for (attachment_row in attachment_rows) {
      attachment_name <- as.character(attachment_row$filename)
      attachment_asset_id <- as.character(attachment_row$assetId)
      attachment_url <- paste0(
        "https://data.cityofnewyork.us/api/views/",
        dataset_id,
        "/files/",
        attachment_asset_id,
        "?download=true&filename=",
        URLencode(attachment_name, reserved = TRUE)
      )
      attachment_path <- file.path(raw_dir, attachment_name)
      attachment_status <- if (file.exists(attachment_path)) "already_present" else download_with_status(attachment_url, attachment_path)

      inventory_counter <- inventory_counter + 1L
      inventory_rows[[inventory_counter]] <- tibble(
        source_id = source_id,
        vintage = pull_date,
        pull_date = pull_date,
        file_role = "attachment_file",
        raw_path = attachment_path,
        status = attachment_status,
        official_url = attachment_url
      )
    }
  }

  provenance_counter <- provenance_counter + 1L
  provenance_rows[[provenance_counter]] <- tibble(
    source_id = source_id,
    dataset_id = dataset_id,
    pull_date = pull_date,
    dataset_name = as.character(metadata_json$name),
    rows_updated_at_utc = rows_updated_at,
    view_last_modified_utc = view_last_modified,
    row_count_reported = as.character(metadata_json$columns[[1]]$cachedContents$count),
    column_count = length(metadata_json$columns),
    attachment_count = length(attachment_rows),
    rows_csv_url = rows_csv_url,
    metadata_url = metadata_url,
    note = "Fetched the current NYC Open Data snapshot, metadata JSON, and attached data dictionary files for the ZAP dataset."
  )
}

file_inventory <- bind_rows(inventory_rows) |>
  arrange(source_id, file_role, raw_path)

checksum_table <- file_inventory |>
  mutate(checksum_sha256 = if_else(file.exists(raw_path), vapply(raw_path, compute_sha256, character(1)), NA_character_)) |>
  select(source_id, vintage, pull_date, file_role, raw_path, checksum_sha256)

provenance_table <- bind_rows(provenance_rows) |>
  arrange(source_id)

write_csv(file_inventory, out_files_csv, na = "")
write_csv(checksum_table, out_checksums_csv, na = "")
write_csv(provenance_table, out_provenance_csv, na = "")

cat("Wrote ZAP fetch outputs to", dirname(out_files_csv), "\n")
