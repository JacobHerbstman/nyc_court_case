# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_census_acs_housing_raw/code")
# census_acs_housing_files_csv <- "../input/census_acs_housing_files.csv"
# out_index_csv <- "../output/census_acs_housing_raw_files.csv"
# out_qc_csv <- "../output/census_acs_housing_raw_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(jsonlite)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: census_acs_housing_files_csv out_index_csv out_qc_csv")
}

census_acs_housing_files_csv <- args[1]
out_index_csv <- args[2]
out_qc_csv <- args[3]

parse_census_json_table <- function(json_path) {
  raw_json <- fromJSON(paste(readLines(json_path, warn = FALSE), collapse = ""))
  header_row <- raw_json[1, ]
  data_matrix <- raw_json[-1, , drop = FALSE]
  data_df <- as_tibble(as.data.frame(data_matrix, stringsAsFactors = FALSE))
  normalized_names <- normalize_names(as.character(header_row))
  names(data_df) <- make.unique(normalized_names, sep = "_dup_")
  data_df
}

file_index <- read_csv(census_acs_housing_files_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  filter(file_role == "acs_group_json", file.exists(raw_path)) %>%
  arrange(as.integer(vintage), table_id, county_code)

if (nrow(file_index) == 0) {
  write_csv(tibble(), out_index_csv, na = "")
  write_csv(tibble(), out_qc_csv, na = "")
  quit(save = "no")
}

index_rows <- list()
qc_rows <- list()

for (i in seq_len(nrow(file_index))) {
  row <- file_index[i, ]
  raw_df <- parse_census_json_table(row$raw_path)

  raw_df <- raw_df %>%
    mutate(
      source_id = row$source_id,
      vintage = as.integer(row$vintage),
      table_id = row$table_id,
      county_code_requested = row$county_code,
      source_raw_path = row$raw_path
    ) %>%
    select(source_id, vintage, table_id, county_code_requested, source_raw_path, everything())

  out_parquet_local <- file.path("..", "output", paste0("census_acs_housing_raw_", row$vintage, "_", row$table_id, "_", row$county_code, ".parquet"))
  out_parquet <- file.path("..", "..", "load_census_acs_housing_raw", "output", basename(out_parquet_local))
  write_parquet_if_changed(raw_df, out_parquet_local)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    vintage = row$vintage,
    table_id = row$table_id,
    county_code = row$county_code,
    raw_path = row$raw_path,
    raw_parquet_path = out_parquet,
    status = "loaded"
  )

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    vintage = row$vintage,
    table_id = row$table_id,
    county_code = row$county_code,
    row_count = nrow(raw_df),
    column_count = ncol(raw_df),
    status = "loaded"
  )
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote Census ACS housing raw load outputs to", dirname(out_index_csv), "\n")
