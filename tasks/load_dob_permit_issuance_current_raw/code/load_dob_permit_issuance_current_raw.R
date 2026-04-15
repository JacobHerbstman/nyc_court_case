# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_dob_permit_issuance_current_raw/code")
# dob_permit_issuance_current_files_csv <- "../input/dob_permit_issuance_current_files.csv"
# out_index_csv <- "../output/dob_permit_issuance_current_raw_files.csv"
# out_qc_csv <- "../output/dob_permit_issuance_current_raw_qc.csv"

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: dob_permit_issuance_current_files_csv out_index_csv out_qc_csv")
}

dob_permit_issuance_current_files_csv <- args[1]
out_index_csv <- args[2]
out_qc_csv <- args[3]

fetch_df <- read_csv(dob_permit_issuance_current_files_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  filter(file.exists(raw_path))

if (nrow(fetch_df) == 0) {
  write_csv(tibble(), out_index_csv, na = "")
  write_csv(tibble(), out_qc_csv, na = "")
  quit(save = "no")
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

out_parquet_local <- file.path("..", "output", "dob_permit_issuance_current_raw.parquet")
out_parquet <- file.path("..", "..", "load_dob_permit_issuance_current_raw", "output", basename(out_parquet_local))
write_parquet_if_changed(raw_df, out_parquet_local)

index_df <- tibble(
  source_id = row$source_id[1],
  dataset_id = row$dataset_id[1],
  official_url = row$official_url[1],
  raw_path = row$raw_path[1],
  raw_parquet_path = out_parquet,
  pull_date = row$pull_date[1],
  checksum_sha256 = row$checksum_sha256[1],
  status = row$status[1]
)

qc_df <- tibble(
  source_id = row$source_id[1],
  dataset_id = row$dataset_id[1],
  pull_date = row$pull_date[1],
  row_count = nrow(raw_df),
  column_count = ncol(raw_df),
  status = row$status[1]
)

write_csv(index_df, out_index_csv, na = "")
write_csv(qc_df, out_qc_csv, na = "")
cat("Wrote DOB permit issuance current raw outputs to", dirname(out_index_csv), "\n")
