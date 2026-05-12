# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_council_land_use_records/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

collapse_values <- function(x) {
  values <- unique(str_squish(as.character(x)))
  values <- values[!is.na(values) & values != ""]
  if (length(values) == 0) {
    return(NA_character_)
  }

  paste(values, collapse = "; ")
}

source_files <- read_csv(
  "../input/council_land_use_fetch_files.csv",
  show_col_types = FALSE,
  na = c("", "NA")
) %>%
  mutate(
    matter_id = as.character(matter_id),
    vote_date = as.character(parse_mixed_date(vote_date)),
    file_available = fetch_status == "downloaded" & file_exists
  )

matter_sources <- source_files %>%
  filter(!is.na(seed_id), seed_id != "") %>%
  group_by(seed_id) %>%
  summarise(
    matter_id = collapse_values(matter_id),
    matter_guid = collapse_values(matter_guid),
    matter_file = collapse_values(matter_file),
    project_name = collapse_values(project_name),
    lu_numbers = collapse_values(lu_numbers),
    resolution_numbers = collapse_values(resolution_numbers),
    ulurp_numbers = collapse_values(ulurp_numbers),
    vote_date = collapse_values(vote_date),
    vote_year = suppressWarnings(as.integer(str_sub(vote_date, 1, 4))),
    council_disposition = collapse_values(council_disposition),
    vote_margin = collapse_values(vote_margin),
    source_roles = collapse_values(source_role),
    source_urls = collapse_values(url),
    source_raw_paths = collapse_values(raw_path[file_available]),
    source_file_count = sum(file_available),
    source_coverage = "seed_official_records",
    record_scope = "project_bundle_seed",
    .groups = "drop"
  ) %>%
  arrange(vote_date, project_name)

source_files_out <- source_files %>%
  select(
    source_id, source_role, source_label, seed_id, matter_id, matter_guid,
    matter_file, project_name, lu_numbers, resolution_numbers, ulurp_numbers,
    vote_date, council_disposition, vote_margin, url, raw_path, fetch_status,
    file_available, file_size_bytes, checksum_sha256, notes
  ) %>%
  arrange(seed_id, source_role, source_label)

records_qc <- tibble(
  check_name = c(
    "unique_seed_matter_rows",
    "required_seed_bundles_present",
    "dock_street_has_official_files",
    "broadway_triangle_has_official_files",
    "matter_rows_have_core_identifiers"
  ),
  passed = c(
    !anyDuplicated(matter_sources$seed_id),
    all(c("dock_street_2009", "broadway_triangle_2009") %in% matter_sources$seed_id),
    any(matter_sources$seed_id == "dock_street_2009" & matter_sources$source_file_count >= 3),
    any(matter_sources$seed_id == "broadway_triangle_2009" & matter_sources$source_file_count >= 3),
    all(!is.na(matter_sources$lu_numbers) & !is.na(matter_sources$ulurp_numbers) & !is.na(matter_sources$vote_date))
  ),
  detail = c(
    "One staged matter row per project-bundle seed.",
    "Seed official records include Dock Street and Broadway Triangle.",
    "Dock Street has transcript/minutes/report files staged.",
    "Broadway Triangle has minutes/report/detail files staged.",
    "Staged matter rows have LU numbers, ULURP numbers, and vote dates."
  )
)

write_csv_if_changed(matter_sources, "../output/council_land_use_matter.csv")
write_csv_if_changed(source_files_out, "../output/council_land_use_source_files.csv")
write_csv_if_changed(records_qc, "../output/council_land_use_records_qc.csv")

if (any(!records_qc$passed)) {
  stop("Council land-use staging failed QC. See ../output/council_land_use_records_qc.csv")
}
