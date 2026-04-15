# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_archival_records/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# archive_requests_csv <- "../input/archive_requests.csv"
# archival_record_raw_files_csv <- "../input/archival_record_raw_files.csv"
# out_inventory_csv <- "../output/archival_record_inventory.csv"
# out_qc_csv <- "../output/archival_record_inventory_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 5) {
  stop("Expected 5 arguments: source_catalog_csv archive_requests_csv archival_record_raw_files_csv out_inventory_csv out_qc_csv")
}

source_catalog_csv <- args[1]
archive_requests_csv <- args[2]
archival_record_raw_files_csv <- args[3]
out_inventory_csv <- args[4]
out_qc_csv <- args[5]

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
archive_requests <- read_csv(archive_requests_csv, show_col_types = FALSE, na = c("", "NA"))
archival_raw_files <- read_csv(archival_record_raw_files_csv, show_col_types = FALSE, na = c("", "NA"))

inventory_df <- source_catalog %>%
  filter(grepl("^archives_", source_id)) %>%
  select(source_id, official_url) %>%
  left_join(archival_raw_files, by = "source_id") %>%
  left_join(
    archive_requests %>%
      select(request_id, custodian, portal_or_contact, records_requested, date_range, submitted_date, status, returned_filename),
    by = "request_id",
    suffix = c("_raw", "_request")
  ) %>%
  mutate(
    inventory_status = dplyr::coalesce(status_raw, "no_returned_files"),
    request_status = status_request
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
    distinct_request_ids = double()
  )
}

write_csv(inventory_df, out_inventory_csv, na = "")
write_csv(qc_df, out_qc_csv, na = "")
cat("Wrote archival record inventory to", out_inventory_csv, "\n")
