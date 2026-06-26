# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_zap_raw/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

zap_files <- read_csv("../input/zap_files.csv", show_col_types = FALSE, na = c("", "NA"))
csv_rows <- zap_files |>
  filter(file_role == "rows_csv", file.exists(raw_path)) |>
  mutate(raw_path = as.character(raw_path), vintage = as.character(vintage)) |>
  arrange(source_id, desc(vintage))

reference_rows <- zap_files |>
  filter(file_role %in% c("attachment_file", "metadata_json"), file.exists(raw_path)) |>
  mutate(raw_path = as.character(raw_path), vintage = as.character(vintage))

index_rows <- list()
row_id <- 1L

if (nrow(reference_rows) > 0) {
  for (i in seq_len(nrow(reference_rows))) {
    row <- reference_rows[i, ]

    index_rows[[row_id]] <- tibble(
      source_id = row$source_id,
      vintage = row$vintage,
      raw_path = row$raw_path,
      raw_parquet_path = NA_character_,
      file_role = row$file_role,
      status = row$status
    )

    row_id <- row_id + 1L
  }
}

if (nrow(csv_rows) == 0) {
  write_csv(bind_rows(index_rows), "../output/zap_raw_files.csv", na = "")
  quit(save = "no")
}

for (i in seq_len(nrow(csv_rows))) {
  row <- csv_rows[i, ]
  raw_df <- read_csv(
    row$raw_path,
    col_types = cols(.default = col_character()),
    show_col_types = FALSE,
    guess_max = 50000,
    na = c("", "NA")
  ) |>
    as_tibble()

  names(raw_df) <- normalize_names(names(raw_df))

  raw_df <- raw_df |>
    mutate(
      source_id = row$source_id,
      source_vintage = row$vintage,
      source_raw_path = row$raw_path
    ) |>
    select(source_id, source_vintage, source_raw_path, everything())

  out_parquet_local <- file.path("..", "output", paste0(sanitize_file_stub(paste(row$source_id, row$vintage, sep = "_")), "_raw.parquet"))
  out_parquet <- file.path("..", "..", "load_zap_raw", "output", basename(out_parquet_local))
  write_parquet_if_changed(raw_df, out_parquet_local)

  index_rows[[row_id]] <- tibble(
    source_id = row$source_id,
    vintage = row$vintage,
    raw_path = row$raw_path,
    raw_parquet_path = out_parquet,
    file_role = row$file_role,
    status = row$status
  )

  row_id <- row_id + 1L
}

write_csv(bind_rows(index_rows), "../output/zap_raw_files.csv", na = "")

cat("Wrote ZAP raw load outputs to ../output\n")
