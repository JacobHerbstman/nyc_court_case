# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_dob_permit_issuance_current/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

source_id <- "dob_permit_issuance_current"
source_catalog <- read_csv("../input/source_catalog.csv", show_col_types = FALSE, na = c("", "NA"))
source_row <- source_catalog %>% filter(source_id == !!source_id)

if (nrow(source_row) != 1) {
  stop("Expected exactly one source_catalog row for dob_permit_issuance_current")
}

dataset_id <- str_match(source_row$official_url[1], "views/([a-z0-9-]+)/")[, 2]
pull_date <- resolve_raw_pull_date(setNames(list(source_row$expected_filename[1]), source_id))
raw_dir <- file.path("..", "..", "..", "data_raw", source_id, pull_date)
raw_path <- file.path(raw_dir, source_row$expected_filename[1])
existing_index <- if (file.exists("../output/dob_permit_issuance_current_files.csv")) {
  read_csv("../output/dob_permit_issuance_current_files.csv", show_col_types = FALSE, na = c("", "NA"))
} else {
  tibble()
}
prior_failed <- nrow(existing_index) > 0 &&
  any(existing_index$raw_path == raw_path & existing_index$status == "download_failed", na.rm = TRUE)

if (prior_failed && file.exists(raw_path)) {
  unlink(raw_path)
}

status <- if (file.exists(raw_path)) {
  "already_present"
} else if (looks_downloadable(source_row$official_url[1])) {
  download_with_status(source_row$official_url[1], raw_path)
} else {
  "non_downloadable_url"
}

index_df <- tibble(
  source_id = source_id,
  dataset_id = dataset_id,
  official_url = source_row$official_url[1],
  raw_path = raw_path,
  pull_date = pull_date,
  checksum_sha256 = compute_sha256(raw_path),
  status = status
)

qc_df <- tibble(
  source_id = source_id,
  dataset_id = dataset_id,
  pull_date = pull_date,
  status = status,
  raw_file_present = file.exists(raw_path)
)

write_csv_if_changed(index_df, "../output/dob_permit_issuance_current_files.csv")
write_csv_if_changed(qc_df, "../output/dob_permit_issuance_current_qc.csv")
cat("Wrote DOB permit issuance current fetch outputs to ../output\n")
