# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_furman_coredata_raw/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# manual_manifest_csv <- "../input/manual_manifest.csv"
# out_index_csv <- "../output/furman_coredata_raw_files.csv"
# out_qc_csv <- "../output/furman_coredata_raw_qc.csv"

suppressPackageStartupMessages({
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4) {
  stop("Expected 4 arguments: source_catalog_csv manual_manifest_csv out_index_csv out_qc_csv")
}

source_catalog_csv <- args[1]
manual_manifest_csv <- args[2]
out_index_csv <- args[3]
out_qc_csv <- args[4]

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
manual_manifest <- read_csv(manual_manifest_csv, show_col_types = FALSE, na = c("", "NA"))

source_row <- source_catalog[source_catalog$source_id == "furman_coredata_neighborhood_indicators", ]
download_instructions <- manual_manifest$download_instructions[manual_manifest$source_id == source_row$source_id]
raw_files <- collect_raw_files("furman_coredata_neighborhood_indicators")

if (length(raw_files) == 0) {
  write_csv(
    tibble(
      source_id = source_row$source_id,
      raw_path = NA_character_,
      raw_parquet_path = NA_character_,
      checksum_sha256 = NA_character_,
      official_url = source_row$official_url,
      download_instructions = download_instructions,
      status = "manual_download_required"
    ),
    out_index_csv,
    na = ""
  )
  write_csv(
    tibble(
      source_id = source_row$source_id,
      status = "manual_download_required",
      row_count = NA_real_,
      column_count = NA_real_
    ),
    out_qc_csv,
    na = ""
  )
  quit(save = "no")
}

index_rows <- list()
qc_rows <- list()

for (i in seq_along(raw_files)) {
  raw_path <- raw_files[[i]]
  raw_ext <- tolower(tools::file_ext(raw_path))
  raw_parquet_path <- NA_character_
  row_count <- NA_real_
  column_count <- NA_real_
  status <- "unsupported_raw_extension"

  if (raw_ext == "csv") {
    raw_df <- read_csv(raw_path, show_col_types = FALSE)
    names(raw_df) <- normalize_names(names(raw_df))
    raw_df$source_id <- source_row$source_id
    raw_df$source_raw_path <- raw_path
    raw_df <- raw_df[, c("source_id", "source_raw_path", setdiff(names(raw_df), c("source_id", "source_raw_path")))]

    out_parquet_local <- file.path(
      "..",
      "output",
      paste0(
        sanitize_file_stub(
          paste(source_row$source_id, tools::file_path_sans_ext(basename(raw_path)), "raw", sep = "_")
        ),
        ".parquet"
      )
    )
    raw_parquet_path <- file.path("..", "..", "load_furman_coredata_raw", "output", basename(out_parquet_local))
    write_parquet_if_changed(raw_df, out_parquet_local)

    row_count <- nrow(raw_df)
    column_count <- ncol(raw_df)
    status <- "loaded"
  }

  index_rows[[i]] <- tibble(
    source_id = source_row$source_id,
    raw_path = raw_path,
    raw_parquet_path = raw_parquet_path,
    checksum_sha256 = compute_sha256(raw_path),
    official_url = source_row$official_url,
    download_instructions = download_instructions,
    status = status
  )

  qc_rows[[i]] <- tibble(
    source_id = source_row$source_id,
    status = status,
    row_count = row_count,
    column_count = column_count
  )
}

write_csv(dplyr::bind_rows(index_rows), out_index_csv, na = "")
write_csv(dplyr::bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote Furman CoreData raw outputs to", dirname(out_index_csv), "\n")
