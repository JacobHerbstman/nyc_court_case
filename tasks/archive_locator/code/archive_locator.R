# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/archive_locator/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# archive_requests_csv <- "../input/archive_requests.csv"
# out_csv <- "../output/archive_locator_queue.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: source_catalog_csv archive_requests_csv out_csv")
}

source_catalog_csv <- args[1]
archive_requests_csv <- args[2]
out_csv <- args[3]

archive_requests <- read_csv(archive_requests_csv, show_col_types = FALSE, na = c("", "NA"))

queue <- archive_requests |>
  mutate(
    archive_lane = case_when(
      str_detect(request_id, "dob") ~ "dob_foil",
      str_detect(request_id, "archives") ~ "municipal_archives",
      str_detect(request_id, "library") ~ "municipal_library",
      str_detect(request_id, "acris") ~ "acris_followup",
      TRUE ~ "other"
    )
  ) |>
  select(request_id, archive_lane, custodian, portal_or_contact, records_requested, date_range, submitted_date, status, returned_filename, notes)

write_csv(queue, out_csv, na = "")
cat("Wrote archive locator queue to", out_csv, "\n")
