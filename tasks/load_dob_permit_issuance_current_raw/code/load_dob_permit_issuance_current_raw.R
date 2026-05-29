# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_dob_permit_issuance_current_raw/code")

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

fetch_df <- read_csv("../input/dob_permit_issuance_current_files.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  filter(file.exists(raw_path), !str_detect(as.character(status), "failed")) %>%
  mutate(pull_date_sort = suppressWarnings(as.integer(pull_date)))

if (nrow(fetch_df) == 0) {
  write_csv(tibble(), "../output/dob_permit_issuance_current_raw_files.csv", na = "")
  write_parquet_if_changed(tibble(), "../output/dob_permit_issuance_current_raw.parquet")
  quit(save = "no")
}

if (all(is.na(fetch_df$pull_date_sort))) {
  stop("DOB permit issuance fetch inventory has no parseable pull_date values.")
}

latest_pull_date <- max(fetch_df$pull_date_sort, na.rm = TRUE)
fetch_df <- fetch_df %>%
  filter(pull_date_sort == latest_pull_date) %>%
  arrange(raw_path)

if (nrow(fetch_df) != 1) {
  stop("Expected exactly one latest valid DOB permit issuance pull; found ", nrow(fetch_df), " rows for pull_date ", latest_pull_date, ".")
}

row <- fetch_df[1, ]
raw_df <- fread(row$raw_path[1], showProgress = FALSE, data.table = FALSE) %>%
  as_tibble()
names(raw_df) <- normalize_names(names(raw_df))

raw_df <- raw_df %>%
  mutate(
    source_id = row$source_id[1],
    dataset_id = row$dataset_id[1],
    pull_date = row$pull_date[1],
    source_raw_path = row$raw_path[1]
  ) %>%
  select(source_id, dataset_id, pull_date, source_raw_path, everything())

write_parquet_if_changed(raw_df, "../output/dob_permit_issuance_current_raw.parquet")

index_df <- tibble(
  source_id = row$source_id[1],
  dataset_id = row$dataset_id[1],
  official_url = row$official_url[1],
  raw_path = row$raw_path[1],
  raw_parquet_path = "../../load_dob_permit_issuance_current_raw/output/dob_permit_issuance_current_raw.parquet",
  pull_date = row$pull_date[1],
  checksum_sha256 = row$checksum_sha256[1],
  status = row$status[1]
)

write_csv(index_df, "../output/dob_permit_issuance_current_raw_files.csv", na = "")
cat("Wrote DOB permit issuance current raw outputs to ../output\n")
