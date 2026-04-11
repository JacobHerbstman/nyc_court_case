# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_dcp_boundaries/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# manual_manifest_csv <- "../input/manual_manifest.csv"
# out_csv <- "../output/dcp_boundary_files.csv"

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

boundary_rows <- source_catalog |>
  filter(str_detect(source_id, "^dcp_boundary_")) |>
  left_join(
    manual_manifest |> select(source_id, download_instructions),
    by = "source_id"
  )

audit_rows <- vector("list", nrow(boundary_rows))

for (i in seq_len(nrow(boundary_rows))) {
  row <- boundary_rows[i, ]
  raw_files <- collect_raw_files(row$source_id)
  raw_path <- if (length(raw_files) > 0) raw_files[1] else NA_character_

  audit_rows[[i]] <- tibble(
    source_id = row$source_id,
    status = if (length(raw_files) > 0) "present" else "manual_download_required",
    raw_path = raw_path,
    checksum_sha256 = if (!is.na(raw_path)) compute_sha256(raw_path) else NA_character_,
    official_url = row$official_url,
    download_instructions = row$download_instructions
  )
}

write_csv(bind_rows(audit_rows), out_csv, na = "")
cat("Wrote DCP boundary inventory to", out_csv, "\n")
