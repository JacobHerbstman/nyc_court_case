# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_dob_open_data_raw/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

dob_files <- read_csv("../input/dob_open_data_files.csv", show_col_types = FALSE, na = c("", "NA")) |>
  filter(file.exists(raw_path)) |>
  mutate(
    source_id = as.character(source_id),
    pull_date = as.character(pull_date),
    raw_path = as.character(raw_path),
    status = as.character(status)
  )

if (nrow(dob_files) == 0) {
  write_csv_if_changed(tibble(), "../output/dob_open_data_raw_files.csv")
  quit(save = "no")
}

duplicate_source_pulls <- dob_files |>
  count(source_id, pull_date, name = "file_count") |>
  filter(file_count > 1)

if (nrow(duplicate_source_pulls) > 0) {
  stop("DOB open data loader expected one raw file per source_id/pull_date.")
}

multi_pull_sources <- dob_files |>
  distinct(source_id, pull_date) |>
  count(source_id, name = "pull_date_count") |>
  filter(pull_date_count > 1)

if (nrow(multi_pull_sources) > 0) {
  stop("DOB open data raw parquet filenames are source-stable; found multiple pull dates for at least one source_id.")
}

index_rows <- list()

for (i in seq_len(nrow(dob_files))) {
  row <- dob_files[i, ]
  raw_checksum <- compute_sha256(row$raw_path)
  raw_df <- read_csv(
    row$raw_path,
    col_types = cols(.default = col_character()),
    show_col_types = FALSE,
    guess_max = 50000
  )
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
    checksum_sha256 = raw_checksum,
    status = row$status
  )
}

write_csv_if_changed(bind_rows(index_rows), "../output/dob_open_data_raw_files.csv")
cat("Wrote DOB raw load outputs to ../output\n")
