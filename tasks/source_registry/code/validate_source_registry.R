# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/source_registry/code")
# source_catalog_csv <- "source_catalog.csv"
# manual_manifest_csv <- "manual_manifest.csv"
# archive_requests_csv <- "archive_requests.csv"
# out_csv <- "../output/source_registry_checks.csv"

suppressPackageStartupMessages({
  library(readr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4) {
  stop("Expected 4 arguments: source_catalog_csv manual_manifest_csv archive_requests_csv out_csv")
}

source_catalog_csv <- args[1]
manual_manifest_csv <- args[2]
archive_requests_csv <- args[3]
out_csv <- args[4]

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
manual_manifest <- read_csv(manual_manifest_csv, show_col_types = FALSE, na = c("", "NA"))
archive_requests <- read_csv(archive_requests_csv, show_col_types = FALSE, na = c("", "NA"))

required_source_cols <- c(
  "source_id", "source_name", "access_mode", "official_url", "raw_subdir",
  "expected_filename", "vintage", "unit", "geography_fields", "date_field",
  "start_date", "end_date", "checksum_sha256", "notes"
)
required_manual_cols <- c(
  "source_id", "expected_filename", "download_instructions", "login_required",
  "date_placed", "checksum_sha256", "notes"
)
required_archive_cols <- c(
  "request_id", "custodian", "portal_or_contact", "records_requested",
  "date_range", "submitted_date", "status", "returned_filename", "notes"
)

checks <- tibble(
  table_name = c("source_catalog", "manual_manifest", "archive_requests"),
  required_columns_present = c(
    all(required_source_cols %in% names(source_catalog)),
    all(required_manual_cols %in% names(manual_manifest)),
    all(required_archive_cols %in% names(archive_requests))
  ),
  unique_primary_key = c(
    !anyDuplicated(source_catalog$source_id),
    !anyDuplicated(paste(manual_manifest$source_id, manual_manifest$expected_filename)),
    !anyDuplicated(archive_requests$request_id)
  ),
  referenced_source_ids_exist = c(
    TRUE,
    all(manual_manifest$source_id %in% source_catalog$source_id),
    TRUE
  ),
  row_count = c(nrow(source_catalog), nrow(manual_manifest), nrow(archive_requests))
)

write_csv(checks, out_csv, na = "")
cat("Wrote registry checks to", out_csv, "\n")
