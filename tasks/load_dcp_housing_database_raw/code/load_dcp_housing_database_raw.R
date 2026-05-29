# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_dcp_housing_database_raw/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

file_index <- read_csv("../input/dcp_housing_database_files.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  filter(file_role == "project_level_csv_zip", file.exists(raw_path))

if (nrow(file_index) == 0) {
  write_csv_if_changed(tibble(), "../output/dcp_housing_database_raw_files.csv")
  quit(save = "no")
}

index_rows <- list()

for (i in seq_len(nrow(file_index))) {
  row <- file_index[i, ]
  zip_listing <- unzip(row$raw_path, list = TRUE)
  csv_candidates <- zip_listing$Name[grepl("\\.csv$", zip_listing$Name, ignore.case = TRUE)]
  project_csv_candidates <- csv_candidates[
    str_detect(tolower(basename(csv_candidates)), "^(housingdb|nychdb).*[.]csv$")
  ]

  if (length(project_csv_candidates) != 1) {
    index_rows[[i]] <- tibble(
      source_id = row$source_id,
      vintage = row$vintage,
      raw_path = row$raw_path,
      csv_inside_zip = if (length(csv_candidates) == 0) NA_character_ else paste(csv_candidates, collapse = ";"),
      raw_parquet_path = NA_character_,
      status = if (length(csv_candidates) == 0) "csv_not_found_in_zip" else "unexpected_csv_payload"
    )
    next
  }

  csv_inside_zip <- project_csv_candidates[[1]]
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
}

write_csv_if_changed(bind_rows(index_rows), "../output/dcp_housing_database_raw_files.csv")
cat("Wrote DCP Housing Database raw load outputs to ../output\n")
