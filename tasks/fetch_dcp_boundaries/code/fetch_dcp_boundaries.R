# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_dcp_boundaries/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

source_catalog <- read_csv("../input/source_catalog.csv", show_col_types = FALSE, na = c("", "NA"))
current_boundary_source_ids <- c(
  "dcp_boundary_community_districts",
  "dcp_boundary_city_council_districts"
)
boundary_rows <- source_catalog |>
  filter(source_id %in% current_boundary_source_ids) |>
  arrange(match(source_id, current_boundary_source_ids))

if (nrow(boundary_rows) != length(current_boundary_source_ids) || !setequal(boundary_rows$source_id, current_boundary_source_ids)) {
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

pull_date <- resolve_raw_pull_date(setNames(
  lapply(names(download_names), function(source_id) {
    c(download_names[[source_id]], paste0(dataset_ids[[source_id]], "_metadata.json"))
  }),
  names(download_names)
))
inventory_rows <- list()

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
}

file_inventory <- bind_rows(inventory_rows) |> arrange(source_id, file_role, raw_path)

write_csv_if_changed(file_inventory, "../output/dcp_boundary_files.csv")

cat("Wrote DCP boundary fetch outputs to ../output\n")
