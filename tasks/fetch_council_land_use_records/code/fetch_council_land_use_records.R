# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_council_land_use_records/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

seed_sources <- read_csv("council_land_use_seed_sources.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    required_flag = str_to_upper(str_squish(as.character(required_flag))) == "TRUE",
    pull_date = format(Sys.Date(), "%Y%m%d"),
    raw_path = file.path("..", "..", "..", "data_raw", source_id, pull_date, expected_filename)
  )

fetch_rows <- vector("list", nrow(seed_sources))

for (i in seq_len(nrow(seed_sources))) {
  source_row <- seed_sources[i, ]
  status <- download_with_status(source_row$url, source_row$raw_path)
  file_exists <- file.exists(source_row$raw_path)

  fetch_rows[[i]] <- source_row %>%
    mutate(
      fetch_status = status,
      file_exists = file_exists,
      file_size_bytes = ifelse(file_exists, file.info(raw_path)$size, NA_real_),
      checksum_sha256 = ifelse(file_exists, compute_sha256(raw_path), NA_character_)
    )
}

fetch_files <- bind_rows(fetch_rows) %>%
  select(
    source_id, source_role, source_label, seed_id, matter_id, matter_guid,
    matter_file, project_name, lu_numbers, resolution_numbers, ulurp_numbers,
    vote_date, council_disposition, vote_margin, url, expected_filename,
    required_flag, pull_date, raw_path, fetch_status, file_exists,
    file_size_bytes, checksum_sha256, notes
  )

required_failures <- fetch_files %>%
  filter(required_flag, fetch_status != "downloaded" | !file_exists)

fetch_qc <- tibble(
  check_name = c(
    "required_sources_downloaded",
    "charter_report_registered",
    "dock_street_official_sources_registered",
    "broadway_triangle_official_sources_registered"
  ),
  passed = c(
    nrow(required_failures) == 0,
    any(fetch_files$source_id == "nyc_charter_land_use_history_2025" & fetch_files$file_exists),
    sum(fetch_files$seed_id == "dock_street_2009" & fetch_files$file_exists, na.rm = TRUE) >= 3,
    sum(fetch_files$seed_id == "broadway_triangle_2009" & fetch_files$file_exists, na.rm = TRUE) >= 3
  ),
  detail = c(
    ifelse(nrow(required_failures) == 0, "All required sources downloaded.", paste(required_failures$source_label, collapse = "; ")),
    "2025 Charter land-use history report is present in raw storage.",
    "Dock Street transcript/minutes/report source files are present.",
    "Broadway Triangle minutes/report/detail source files are present."
  )
)

write_csv_if_changed(fetch_files, "../output/council_land_use_fetch_files.csv")
write_csv_if_changed(fetch_qc, "../output/council_land_use_fetch_qc.csv")

if (any(!fetch_qc$passed)) {
  stop("Council land-use source fetch failed QC. See ../output/council_land_use_fetch_qc.csv")
}
