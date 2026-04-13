# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_dob_open_data_raw/code")
# dob_open_data_files_csv <- "../input/dob_open_data_files.csv"
# out_index_csv <- "../output/dob_open_data_raw_files.csv"
# out_qc_csv <- "../output/dob_open_data_raw_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: dob_open_data_files_csv out_index_csv out_qc_csv")
}

dob_open_data_files_csv <- args[1]
out_index_csv <- args[2]
out_qc_csv <- args[3]

dob_files <- read_csv(dob_open_data_files_csv, show_col_types = FALSE, na = c("", "NA")) |>
  filter(file.exists(raw_path))

if (nrow(dob_files) == 0) {
  write_csv(tibble(), out_index_csv, na = "")
  write_csv(tibble(), out_qc_csv, na = "")
  quit(save = "no")
}

index_rows <- list()
qc_rows <- list()

for (i in seq_len(nrow(dob_files))) {
  row <- dob_files[i, ]
  raw_df <- read_csv(row$raw_path, show_col_types = FALSE, guess_max = 50000)
  names(raw_df) <- normalize_names(names(raw_df))

  raw_df <- raw_df |>
    mutate(
      source_id = row$source_id,
      pull_date = row$pull_date,
      source_raw_path = row$raw_path
    ) |>
    select(source_id, pull_date, source_raw_path, everything())

  out_parquet_local <- file.path("..", "output", paste0(sanitize_file_stub(paste(row$source_id, "raw", sep = "_")), ".parquet"))
  out_parquet <- file.path("..", "..", "load_dob_open_data_raw", "output", basename(out_parquet_local))
  write_parquet_if_changed(raw_df, out_parquet_local)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    raw_path = row$raw_path,
    raw_parquet_path = out_parquet,
    pull_date = row$pull_date,
    status = row$status
  )

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    pull_date = row$pull_date,
    row_count = nrow(raw_df),
    column_count = ncol(raw_df),
    status = row$status
  )
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote DOB raw load outputs to", dirname(out_index_csv), "\n")
