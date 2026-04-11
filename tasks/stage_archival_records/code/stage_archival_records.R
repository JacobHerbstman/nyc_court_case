# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_archival_records/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# archive_requests_csv <- "../input/archive_requests.csv"
# out_csv <- "../output/archival_record_inventory.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: source_catalog_csv archive_requests_csv out_csv")
}

source_catalog_csv <- args[1]
archive_requests_csv <- args[2]
out_csv <- args[3]

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
archive_requests <- read_csv(archive_requests_csv, show_col_types = FALSE, na = c("", "NA"))

archive_sources <- source_catalog |> filter(str_detect(source_id, "^archives_"))
inventory_rows <- list()
row_id <- 1

for (i in seq_len(nrow(archive_sources))) {
  source_row <- archive_sources[i, ]
  raw_files <- collect_raw_files(source_row$source_id)

  if (length(raw_files) == 0) {
    inventory_rows[[row_id]] <- tibble(
      source_id = source_row$source_id,
      request_id = NA_character_,
      raw_path = NA_character_,
      checksum_sha256 = NA_character_,
      status = "no_returned_files"
    )
    row_id <- row_id + 1
    next
  }

  for (raw_path in raw_files) {
    request_id <- str_split(raw_path, .Platform$file.sep)[[1]]
    request_id <- request_id[length(request_id) - 1]

    inventory_rows[[row_id]] <- tibble(
      source_id = source_row$source_id,
      request_id = request_id,
      raw_path = raw_path,
      checksum_sha256 = compute_sha256(raw_path),
      status = "returned_file_present"
    )
    row_id <- row_id + 1
  }
}

write_csv(bind_rows(inventory_rows), out_csv, na = "")
cat("Wrote archival record inventory to", out_csv, "\n")
