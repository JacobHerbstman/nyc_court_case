# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_zap_datasets/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(jsonlite)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

source_catalog <- read_csv("../input/source_catalog.csv", show_col_types = FALSE, na = c("", "NA"))
source_rows <- source_catalog |>
  filter(source_id %in% c("dcp_zap_project_data", "dcp_zap_bbl"))

if (nrow(source_rows) != 2) {
  stop("Source catalog must contain exactly dcp_zap_project_data and dcp_zap_bbl.")
}

pull_date <- resolve_raw_pull_date(setNames(
  lapply(seq_len(nrow(source_rows)), function(i) {
    dataset_id <- str_match(source_rows$official_url[[i]], "([a-z0-9]{4}-[a-z0-9]{4})")[, 2]
    c(source_rows$expected_filename[[i]], paste0(dataset_id, "_metadata.json"))
  }),
  source_rows$source_id
))
inventory_rows <- list()
inventory_counter <- 0L

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
}

file_inventory <- bind_rows(inventory_rows) |>
  arrange(source_id, file_role, raw_path)

write_csv_if_changed(file_inventory, "../output/zap_files.csv")

cat("Wrote ZAP fetch outputs to ../output\n")
