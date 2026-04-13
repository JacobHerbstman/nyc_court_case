# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_furman_coredata/code")
# furman_coredata_raw_files_csv <- "../input/furman_coredata_raw_files.csv"
# out_index_csv <- "../output/furman_coredata_files.csv"
# out_qc_csv <- "../output/furman_coredata_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(readr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: furman_coredata_raw_files_csv out_index_csv out_qc_csv")
}

furman_coredata_raw_files_csv <- args[1]
out_index_csv <- args[2]
out_qc_csv <- args[3]

furman_raw_files <- read_csv(furman_coredata_raw_files_csv, show_col_types = FALSE, na = c("", "NA"))

if (nrow(furman_raw_files) == 0) {
  write_csv(tibble(), out_index_csv, na = "")
  write_csv(tibble(), out_qc_csv, na = "")
  quit(save = "no")
}

index_rows <- list()
qc_rows <- list()

for (i in seq_len(nrow(furman_raw_files))) {
  row <- furman_raw_files[i, ]
  parquet_path <- NA_character_
  status <- row$status
  row_count <- NA_real_
  column_count <- NA_real_

  if (!is.na(row$raw_parquet_path) && file.exists(row$raw_parquet_path)) {
    furman_df <- read_parquet(row$raw_parquet_path) |>
      as.data.frame() |>
      as_tibble()

    out_parquet_local <- file.path("..", "output", paste0(row$source_id, ".parquet"))
    parquet_path <- file.path("..", "..", "stage_furman_coredata", "output", basename(out_parquet_local))
    write_parquet_if_changed(furman_df, out_parquet_local)

    row_count <- nrow(furman_df)
    column_count <- ncol(furman_df)
    status <- "staged"
  }

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    status = status,
    raw_path = row$raw_path,
    raw_parquet_path = row$raw_parquet_path,
    parquet_path = parquet_path,
    checksum_sha256 = row$checksum_sha256,
    official_url = row$official_url,
    download_instructions = row$download_instructions
  )

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    status = status,
    row_count = row_count,
    column_count = column_count
  )
}

write_csv(dplyr::bind_rows(index_rows), out_index_csv, na = "")
write_csv(dplyr::bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote Furman CoreData staging outputs to", dirname(out_index_csv), "\n")
