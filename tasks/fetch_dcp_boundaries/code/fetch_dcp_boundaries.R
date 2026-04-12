# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_dcp_boundaries/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# out_files_csv <- "../output/dcp_boundary_files.csv"
# out_checksums_csv <- "../output/dcp_boundary_checksums.csv"
# out_provenance_csv <- "../output/dcp_boundary_provenance.csv"

suppressPackageStartupMessages({
  library(dplyr)
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
boundary_rows <- source_catalog |> filter(str_detect(source_id, "^dcp_boundary_"))

if (nrow(boundary_rows) != 2) {
  stop("Source catalog must contain the two scripted DCP boundary rows.")
}

dataset_ids <- c(
  dcp_boundary_community_districts = "5crt-au7u",
  dcp_boundary_city_council_districts = "872g-cjhh"
)

download_names <- c(
  dcp_boundary_community_districts = "community_districts.zip",
  dcp_boundary_city_council_districts = "city_council_districts.zip"
)

pull_date <- format(Sys.Date(), "%Y%m%d")
inventory_rows <- list()
provenance_rows <- list()

for (i in seq_len(nrow(boundary_rows))) {
  row <- boundary_rows[i, ]
  source_id <- row$source_id
  dataset_id <- dataset_ids[[source_id]]

  if (is.null(dataset_id)) {
    stop("Missing expected Socrata dataset id for ", source_id)
  }

  metadata_url <- paste0("https://data.cityofnewyork.us/api/views/", dataset_id)
  metadata_path <- file.path("..", "..", "..", "data_raw", source_id, pull_date, paste0(dataset_id, "_metadata.json"))
  metadata_status <- if (file.exists(metadata_path)) "already_present" else download_with_status(metadata_url, metadata_path)

  if (!file.exists(metadata_path)) {
    stop("Could not resolve Socrata metadata for ", source_id, " at dataset id ", dataset_id)
  }

  zip_path <- file.path("..", "..", "..", "data_raw", source_id, pull_date, download_names[[source_id]])
  zip_status <- if (file.exists(zip_path)) "already_present" else download_with_status(row$official_url, zip_path)

  inventory_rows[[length(inventory_rows) + 1L]] <- tibble(
    source_id = source_id,
    pull_date = pull_date,
    file_role = "metadata_json",
    raw_path = metadata_path,
    status = metadata_status,
    official_url = metadata_url
  )

  inventory_rows[[length(inventory_rows) + 1L]] <- tibble(
    source_id = source_id,
    pull_date = pull_date,
    file_role = "boundary_shapefile_zip",
    raw_path = zip_path,
    status = zip_status,
    official_url = row$official_url
  )

  provenance_rows[[length(provenance_rows) + 1L]] <- tibble(
    source_id = source_id,
    pull_date = pull_date,
    dataset_id = dataset_id,
    metadata_path = metadata_path,
    note = "Current official boundary geometry and matching Socrata metadata snapshot."
  )
}

file_inventory <- bind_rows(inventory_rows) |> arrange(source_id, file_role, raw_path)
checksum_table <- file_inventory |>
  mutate(checksum_sha256 = if_else(file.exists(raw_path), vapply(raw_path, compute_sha256, character(1)), NA_character_)) |>
  select(source_id, pull_date, file_role, raw_path, checksum_sha256)
provenance_table <- bind_rows(provenance_rows) |> arrange(source_id)

write_csv(file_inventory, out_files_csv, na = "")
write_csv(checksum_table, out_checksums_csv, na = "")
write_csv(provenance_table, out_provenance_csv, na = "")

cat("Wrote DCP boundary fetch outputs to", dirname(out_files_csv), "\n")
