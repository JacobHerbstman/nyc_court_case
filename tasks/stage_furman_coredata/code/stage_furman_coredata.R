# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_furman_coredata/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# manual_manifest_csv <- "../input/manual_manifest.csv"
# out_csv <- "../output/furman_coredata_files.csv"

suppressPackageStartupMessages({
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: source_catalog_csv manual_manifest_csv out_csv")
}

source_catalog_csv <- args[1]
manual_manifest_csv <- args[2]
out_csv <- args[3]

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
manual_manifest <- read_csv(manual_manifest_csv, show_col_types = FALSE, na = c("", "NA"))

row <- source_catalog[source_catalog$source_id == "furman_coredata_neighborhood_indicators", ]
raw_files <- collect_raw_files("furman_coredata_neighborhood_indicators")
raw_path <- raw_files[1]
parquet_path <- NA_character_
status <- "manual_download_required"

if (!is.na(raw_path) && grepl("\\.csv$", raw_path)) {
  furman_df <- read_csv(raw_path, show_col_types = FALSE)
  parquet_path <- file.path("..", "output", "furman_coredata_neighborhood_indicators.parquet")
  write_parquet_if_changed(furman_df, parquet_path)
  status <- "staged"
}

write_csv(
  tibble(
    source_id = row$source_id,
    status = status,
    raw_path = raw_path,
    parquet_path = parquet_path,
    checksum_sha256 = if (!is.na(raw_path)) compute_sha256(raw_path) else NA_character_,
    official_url = row$official_url,
    download_instructions = manual_manifest$download_instructions[manual_manifest$source_id == row$source_id]
  ),
  out_csv,
  na = ""
)

cat("Wrote Furman CoreData inventory to", out_csv, "\n")
