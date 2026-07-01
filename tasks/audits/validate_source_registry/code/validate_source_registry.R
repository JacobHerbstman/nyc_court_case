suppressPackageStartupMessages({
  library(readr)
  library(tibble)
})

source_catalog <- read_csv("../../../source_registry/code/source_catalog.csv", show_col_types = FALSE, na = c("", "NA"))
manual_manifest <- read_csv("../../source_registry_context/code/manual_manifest.csv", show_col_types = FALSE, na = c("", "NA"))
archive_requests <- read_csv("../../source_registry_context/code/archive_requests.csv", show_col_types = FALSE, na = c("", "NA"))

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

write_csv(checks, "../output/source_registry_checks.csv", na = "")
cat("Wrote registry checks to ../output/source_registry_checks.csv\n")
