# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_dob_permit_issuance_current/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# out_index_csv <- "../output/dob_permit_issuance_current_files.csv"
# out_qc_csv <- "../output/dob_permit_issuance_current_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
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

source_id <- "dob_permit_issuance_current"
source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
source_row <- source_catalog %>% filter(source_id == !!source_id)

if (nrow(source_row) != 1) {
  stop("Expected exactly one source_catalog row for dob_permit_issuance_current")
}

dataset_id <- str_match(source_row$official_url[1], "views/([a-z0-9-]+)/")[, 2]
pull_date <- format(Sys.Date(), "%Y%m%d")
raw_dir <- file.path("..", "..", "..", "data_raw", source_id, pull_date)
raw_path <- file.path(raw_dir, source_row$expected_filename[1])
existing_index <- if (file.exists(out_index_csv)) {
  read_csv(out_index_csv, show_col_types = FALSE, na = c("", "NA"))
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

write_csv(index_df, out_index_csv, na = "")
write_csv(qc_df, out_qc_csv, na = "")
cat("Wrote DOB permit issuance current fetch outputs to", dirname(out_index_csv), "\n")
