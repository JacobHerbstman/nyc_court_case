# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_archival_records_raw/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# archive_requests_csv <- "../input/archive_requests.csv"
# out_index_csv <- "../output/archival_record_raw_files.csv"
# out_qc_csv <- "../output/archival_record_raw_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4) {
  stop("Expected 4 arguments: source_catalog_csv archive_requests_csv out_index_csv out_qc_csv")
}

source_catalog_csv <- args[1]
archive_requests_csv <- args[2]
out_index_csv <- args[3]
out_qc_csv <- args[4]

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
archive_requests <- read_csv(archive_requests_csv, show_col_types = FALSE, na = c("", "NA"))

archive_sources <- source_catalog |> filter(str_detect(source_id, "^archives_"))
index_rows <- list()
qc_rows <- list()
row_id <- 1

for (i in seq_len(nrow(archive_sources))) {
  source_row <- archive_sources[i, ]
  raw_files <- collect_raw_files(source_row$source_id)

  if (length(raw_files) == 0) {
    index_rows[[row_id]] <- tibble(
      source_id = source_row$source_id,
      request_id = NA_character_,
      raw_path = NA_character_,
      checksum_sha256 = NA_character_,
      file_extension = NA_character_,
      file_size_bytes = NA_real_,
      status = "no_returned_files"
    )
    qc_rows[[i]] <- tibble(
      source_id = source_row$source_id,
      returned_file_count = 0,
      status = "no_returned_files"
    )
    row_id <- row_id + 1
    next
  }

  qc_rows[[i]] <- tibble(
    source_id = source_row$source_id,
    returned_file_count = length(raw_files),
    status = "returned_file_present"
  )

  for (raw_path in raw_files) {
    request_id <- str_split(raw_path, .Platform$file.sep)[[1]]
    request_id <- request_id[length(request_id) - 1]
    raw_info <- file.info(raw_path)

    index_rows[[row_id]] <- tibble(
      source_id = source_row$source_id,
      request_id = request_id,
      raw_path = raw_path,
      checksum_sha256 = compute_sha256(raw_path),
      file_extension = tolower(tools::file_ext(raw_path)),
      file_size_bytes = as.numeric(raw_info$size),
      status = "returned_file_present"
    )
    row_id <- row_id + 1
  }
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote archival raw inventory to", out_index_csv, "\n")
