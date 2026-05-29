# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_zap_raw/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(jsonlite)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

zap_files <- read_csv("../input/zap_files.csv", show_col_types = FALSE, na = c("", "NA"))
csv_rows <- zap_files |>
  filter(file_role == "rows_csv", file.exists(raw_path)) |>
  mutate(raw_path = as.character(raw_path), vintage = as.character(vintage)) |>
  arrange(source_id, desc(vintage))

metadata_rows <- zap_files |>
  filter(file_role == "metadata_json", file.exists(raw_path)) |>
  transmute(source_id, vintage, metadata_json_path = as.character(raw_path))

reference_rows <- zap_files |>
  filter(file_role %in% c("attachment_file", "metadata_json"), file.exists(raw_path)) |>
  mutate(raw_path = as.character(raw_path), vintage = as.character(vintage))

index_rows <- list()
column_rows <- list()
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
  write_csv(tibble(), "../output/zap_columns_metadata.csv", na = "")
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

  metadata_row <- metadata_rows |>
    filter(source_id == row$source_id, vintage == row$vintage) |>
    slice_head(n = 1)

  if (nrow(metadata_row) == 1 && file.exists(metadata_row$metadata_json_path[[1]])) {
    metadata_json <- fromJSON(metadata_row$metadata_json_path[[1]], simplifyVector = FALSE)
    if (length(metadata_json$columns) > 0) {
      column_rows[[i]] <- bind_rows(lapply(metadata_json$columns, function(column_row) {
        tibble(
          source_id = row$source_id,
          vintage = row$vintage,
          field_name = as.character(column_row$fieldName),
          column_name = as.character(column_row$name),
          data_type_name = as.character(column_row$dataTypeName),
          position = suppressWarnings(as.integer(column_row$position)),
          description = as.character(column_row$description),
          cached_non_null = as.character(column_row$cachedContents$non_null),
          cached_null = as.character(column_row$cachedContents$null),
          cached_cardinality = as.character(column_row$cachedContents$cardinality)
        )
      }))
    }
  }

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
write_csv(bind_rows(column_rows), "../output/zap_columns_metadata.csv", na = "")

cat("Wrote ZAP raw load outputs to ../output\n")
