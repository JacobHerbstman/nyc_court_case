# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_dob_open_data/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# out_index_csv <- "../output/dob_open_data_files.csv"
# out_qc_csv <- "../output/dob_open_data_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: source_catalog_csv out_index_csv out_qc_csv")
}

source_catalog_csv <- args[1]
out_index_csv <- args[2]
out_qc_csv <- args[3]

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
dob_open_data_source_ids <- c(
  "dob_bis_job_filings",
  "dob_now_build_job_filings",
  "dob_bis_certificate_of_occupancy",
  "dob_now_certificate_of_occupancy"
)
dob_rows <- source_catalog |>
  filter(source_id %in% dob_open_data_source_ids) |>
  arrange(match(source_id, dob_open_data_source_ids))

if (nrow(dob_rows) != length(dob_open_data_source_ids) || !setequal(dob_rows$source_id, dob_open_data_source_ids)) {
  stop("Source catalog must contain the scripted non-permit DOB Open Data rows.")
}

index_rows <- list()
qc_rows <- list()
pull_date <- resolve_raw_pull_date(setNames(
  lapply(dob_rows$expected_filename, c),
  dob_rows$source_id
))

for (i in seq_len(nrow(dob_rows))) {
  row <- dob_rows[i, ]
  raw_dir <- file.path("..", "..", "..", "data_raw", row$source_id, pull_date)
  raw_path <- file.path(raw_dir, row$expected_filename)

  status <- if (file.exists(raw_path)) {
    "already_present"
  } else if (looks_downloadable(row$official_url)) {
    download_with_status(row$official_url, raw_path)
  } else {
    "non_downloadable_url"
  }

  if (!file.exists(raw_path)) {
    index_rows[[i]] <- tibble(
      source_id = row$source_id,
      raw_path = raw_path,
      pull_date = pull_date,
      status = status
    )

    qc_rows[[i]] <- tibble(
      source_id = row$source_id,
      status = status,
      raw_file_present = FALSE,
      pull_date = pull_date
    )
    next
  }

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    raw_path = raw_path,
    pull_date = pull_date,
    status = status
  )

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    status = status,
    raw_file_present = TRUE,
    pull_date = pull_date
  )
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote DOB Open Data fetch outputs to", dirname(out_index_csv), "\n")
