# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_dcp_housing_database_raw/code")
# dcp_housing_database_files_csv <- "../input/dcp_housing_database_files.csv"
# out_index_csv <- "../output/dcp_housing_database_raw_files.csv"
# out_qc_csv <- "../output/dcp_housing_database_raw_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: dcp_housing_database_files_csv out_index_csv out_qc_csv")
}

dcp_housing_database_files_csv <- args[1]
out_index_csv <- args[2]
out_qc_csv <- args[3]

file_index <- read_csv(dcp_housing_database_files_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  filter(file_role == "project_level_csv_zip", file.exists(raw_path))

if (nrow(file_index) == 0) {
  write_csv(tibble(), out_index_csv, na = "")
  write_csv(tibble(), out_qc_csv, na = "")
  quit(save = "no")
}

index_rows <- list()
qc_rows <- list()

for (i in seq_len(nrow(file_index))) {
  row <- file_index[i, ]
  zip_listing <- unzip(row$raw_path, list = TRUE)
  csv_inside_zip <- zip_listing$Name[grepl("\\.csv$", zip_listing$Name, ignore.case = TRUE)][1]

  if (is.na(csv_inside_zip)) {
    index_rows[[i]] <- tibble(
      source_id = row$source_id,
      vintage = row$vintage,
      raw_path = row$raw_path,
      csv_inside_zip = NA_character_,
      raw_parquet_path = NA_character_,
      status = "csv_not_found_in_zip"
    )

    qc_rows[[i]] <- tibble(
      source_id = row$source_id,
      vintage = row$vintage,
      row_count = NA_real_,
      column_count = NA_real_,
      status = "csv_not_found_in_zip"
    )
    next
  }

  extracted_csv <- unzip(row$raw_path, files = csv_inside_zip, exdir = tempdir(), overwrite = TRUE)
  raw_df <- read_csv(extracted_csv, show_col_types = FALSE, guess_max = 50000)
  names(raw_df) <- normalize_names(names(raw_df))

  raw_df <- raw_df %>%
    mutate(
      source_id = row$source_id,
      vintage = row$vintage,
      source_raw_path = row$raw_path
    ) %>%
    select(source_id, vintage, source_raw_path, everything())

  out_parquet_local <- file.path("..", "output", paste0("dcp_housing_database_project_level_raw_", sanitize_file_stub(row$vintage), ".parquet"))
  out_parquet <- file.path("..", "..", "load_dcp_housing_database_raw", "output", basename(out_parquet_local))
  write_parquet_if_changed(raw_df, out_parquet_local)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    vintage = row$vintage,
    raw_path = row$raw_path,
    csv_inside_zip = csv_inside_zip,
    raw_parquet_path = out_parquet,
    status = "loaded"
  )

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    vintage = row$vintage,
    row_count = nrow(raw_df),
    column_count = ncol(raw_df),
    status = "loaded"
  )
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote DCP Housing Database raw load outputs to", dirname(out_index_csv), "\n")
