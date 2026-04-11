# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_census_bps/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# manual_manifest_csv <- "../input/manual_manifest.csv"
# out_csv <- "../output/census_bps_files.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
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

bps_row <- source_catalog |> filter(source_id == "census_bps_place_ascii")
bps_manual <- manual_manifest |> filter(source_id == "census_bps_place_ascii")
raw_files <- collect_raw_files("census_bps_place_ascii")
raw_path <- if (length(raw_files) > 0) raw_files[1] else NA_character_

write_csv(
  tibble(
    source_id = "census_bps_place_ascii",
    status = if (length(raw_files) > 0) "present" else "manual_download_required",
    raw_path = raw_path,
    checksum_sha256 = if (!is.na(raw_path)) compute_sha256(raw_path) else NA_character_,
    official_url = bps_row$official_url,
    download_instructions = bps_manual$download_instructions
  ),
  out_csv,
  na = ""
)

cat("Wrote Census BPS file inventory to", out_csv, "\n")
