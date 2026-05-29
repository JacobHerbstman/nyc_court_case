# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_dob_open_data/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

source_catalog <- read_csv("../input/source_catalog.csv", show_col_types = FALSE, na = c("", "NA"))
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
    next
  }

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    raw_path = raw_path,
    pull_date = pull_date,
    status = status
  )
}

write_csv_if_changed(bind_rows(index_rows), "../output/dob_open_data_files.csv")
cat("Wrote DOB Open Data fetch outputs to ../output\n")
