# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_archival_records_raw/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

source_catalog <- read_csv("../input/source_catalog.csv", show_col_types = FALSE, na = c("", "NA"))
archive_requests <- read_csv("../input/archive_requests.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    submitted_date = as.character(submitted_date),
    returned_filename = as.character(returned_filename)
  )

archive_source_requests <- tibble(
  source_id = c(
    "archives_dob_pre_2000_permits",
    "archives_municipal_archives_board_of_estimate",
    "archives_municipal_library_city_council",
    "archives_acris_followup"
  ),
  request_id = c(
    "req_dob_pre_2000_nb_permits",
    "req_municipal_archives_board_of_estimate",
    "req_municipal_library_city_council",
    "req_acris_continuity_followup"
  )
)

missing_request_manifest_rows <- archive_source_requests %>%
  anti_join(archive_requests %>% select(request_id), by = "request_id")

if (nrow(missing_request_manifest_rows) > 0) {
  stop("Archive source/request mapping references request IDs missing from archive_requests.csv.")
}

archive_sources <- source_catalog %>%
  filter(str_detect(source_id, "^archives_")) %>%
  left_join(archive_source_requests, by = "source_id", relationship = "many-to-one")

if (any(is.na(archive_sources$request_id))) {
  stop("Every archives_* source in source_catalog.csv needs an explicit request_id mapping.")
}

status_for_empty_request <- function(request_status) {
  case_when(
    request_status == "defer_until_needed" ~ "request_deferred_no_files",
    request_status == "planned" ~ "request_planned_no_files",
    request_status %in% c("submitted", "pending", "in_progress", "open") ~ "request_submitted_no_returned_files",
    TRUE ~ "no_returned_files"
  )
}

index_rows <- list()
qc_rows <- list()
row_id <- 1

for (i in seq_len(nrow(archive_sources))) {
  source_row <- archive_sources[i, ]
  raw_files <- collect_raw_files(source_row$source_id)
  request_row <- archive_requests %>% filter(request_id == source_row$request_id)
  empty_status <- status_for_empty_request(request_row$status[[1]])

  if (length(raw_files) == 0) {
    index_rows[[row_id]] <- tibble(
      source_id = source_row$source_id,
      request_id = source_row$request_id,
      request_status = request_row$status[[1]],
      submitted_date = request_row$submitted_date[[1]],
      returned_filename = request_row$returned_filename[[1]],
      raw_path = NA_character_,
      checksum_sha256 = NA_character_,
      file_extension = NA_character_,
      file_size_bytes = NA_real_,
      status = empty_status
    )
    qc_rows[[i]] <- tibble(
      source_id = source_row$source_id,
      request_id = source_row$request_id,
      request_status = request_row$status[[1]],
      submitted_date = request_row$submitted_date[[1]],
      returned_filename = request_row$returned_filename[[1]],
      returned_file_count = 0,
      status = empty_status
    )
    row_id <- row_id + 1
    next
  }

  qc_rows[[i]] <- tibble(
    source_id = source_row$source_id,
    request_id = source_row$request_id,
    request_status = request_row$status[[1]],
    submitted_date = request_row$submitted_date[[1]],
    returned_filename = request_row$returned_filename[[1]],
    returned_file_count = length(raw_files),
    status = "returned_file_present"
  )

  for (raw_path in raw_files) {
    raw_request_id <- str_split(raw_path, .Platform$file.sep)[[1]]
    raw_request_id <- raw_request_id[length(raw_request_id) - 1]
    raw_request_row <- archive_requests %>% filter(request_id == raw_request_id)
    raw_info <- file.info(raw_path)
    raw_status <- case_when(
      nrow(raw_request_row) == 0 ~ "returned_file_unmatched_request",
      raw_request_id != source_row$request_id ~ "returned_file_unexpected_request",
      TRUE ~ "returned_file_present"
    )

    index_rows[[row_id]] <- tibble(
      source_id = source_row$source_id,
      request_id = raw_request_id,
      request_status = if (nrow(raw_request_row) == 0) NA_character_ else raw_request_row$status[[1]],
      submitted_date = if (nrow(raw_request_row) == 0) NA_character_ else raw_request_row$submitted_date[[1]],
      returned_filename = if (nrow(raw_request_row) == 0) NA_character_ else raw_request_row$returned_filename[[1]],
      raw_path = raw_path,
      checksum_sha256 = compute_sha256(raw_path),
      file_extension = tolower(tools::file_ext(raw_path)),
      file_size_bytes = as.numeric(raw_info$size),
      status = raw_status
    )
    row_id <- row_id + 1
  }
}

write_csv_if_changed(bind_rows(index_rows), "../output/archival_record_raw_files.csv")
write_csv_if_changed(bind_rows(qc_rows), "../output/archival_record_raw_qc.csv")
cat("Wrote archival raw inventory to ../output\n")
