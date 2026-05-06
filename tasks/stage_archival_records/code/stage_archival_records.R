# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_archival_records/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

source_catalog <- read_csv("../input/source_catalog.csv", show_col_types = FALSE, na = c("", "NA"))
archive_requests <- read_csv("../input/archive_requests.csv", show_col_types = FALSE, na = c("", "NA"))
archival_raw_files <- read_csv("../input/archival_record_raw_files.csv", show_col_types = FALSE, na = c("", "NA"))

if (anyDuplicated(source_catalog$source_id)) {
  stop("source_catalog has duplicate source_id values.")
}

if (anyDuplicated(archive_requests$request_id)) {
  stop("archive_requests has duplicate request_id values.")
}

inventory_df <- source_catalog %>%
  filter(grepl("^archives_", source_id)) %>%
  select(source_id, official_url) %>%
  left_join(archival_raw_files, by = "source_id", relationship = "one-to-many") %>%
  left_join(
    archive_requests %>%
      select(request_id, custodian, portal_or_contact, records_requested, date_range, submitted_date, status, returned_filename),
    by = "request_id",
    suffix = c("_raw", "_request"),
    relationship = "many-to-one"
  ) %>%
  mutate(
    inventory_status = dplyr::coalesce(as.character(status_raw), "no_returned_files"),
    request_status = dplyr::coalesce(as.character(request_status), as.character(status_request)),
    submitted_date = dplyr::coalesce(as.character(submitted_date_raw), as.character(submitted_date_request)),
    returned_filename = dplyr::coalesce(as.character(returned_filename_raw), as.character(returned_filename_request))
  ) %>%
  select(
    source_id,
    request_id,
    raw_path,
    checksum_sha256,
    file_extension,
    file_size_bytes,
    inventory_status,
    request_status,
    custodian,
    portal_or_contact,
    records_requested,
    date_range,
    submitted_date,
    returned_filename,
    official_url
  )

qc_df <- inventory_df %>%
  group_by(source_id) %>%
  summarise(
    inventory_rows = n(),
    returned_file_rows = sum(inventory_status == "returned_file_present", na.rm = TRUE),
    distinct_request_ids = n_distinct(request_id[!is.na(request_id)]),
    planned_request_rows = sum(request_status == "planned", na.rm = TRUE),
    deferred_request_rows = sum(request_status == "defer_until_needed", na.rm = TRUE),
    submitted_no_return_rows = sum(inventory_status == "request_submitted_no_returned_files", na.rm = TRUE),
    .groups = "drop"
  )

if (nrow(inventory_df) == 0) {
  inventory_df <- tibble(
    source_id = character(),
    request_id = character(),
    raw_path = character(),
    checksum_sha256 = character(),
    file_extension = character(),
    file_size_bytes = double(),
    inventory_status = character(),
    request_status = character(),
    custodian = character(),
    portal_or_contact = character(),
    records_requested = character(),
    date_range = character(),
    submitted_date = character(),
    returned_filename = character(),
    official_url = character()
  )

  qc_df <- tibble(
    source_id = character(),
    inventory_rows = double(),
    returned_file_rows = double(),
    distinct_request_ids = double(),
    planned_request_rows = double(),
    deferred_request_rows = double(),
    submitted_no_return_rows = double()
  )
}

write_csv(inventory_df, "../output/archival_record_inventory.csv", na = "")
write_csv(qc_df, "../output/archival_record_inventory_qc.csv", na = "")
cat("Wrote archival record inventory to ../output/archival_record_inventory.csv\n")
