suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

seed_sources_raw <- read_csv(
  "council_land_use_seed_sources.csv",
  show_col_types = FALSE,
  na = c("", "NA")
)

raw_root <- file.path("..", "..", "..", "..", "data_raw")
required_files_by_source <- split(seed_sources_raw$expected_filename, seed_sources_raw$source_id)
available_dates <- lapply(names(required_files_by_source), function(source_id) {
  source_dir <- file.path(raw_root, source_id)
  if (!dir.exists(source_dir)) {
    return(character())
  }
  date_dirs <- basename(list.dirs(source_dir, recursive = FALSE, full.names = TRUE))
  date_dirs <- date_dirs[str_detect(date_dirs, "^[0-9]{8}$")]
  date_dirs[vapply(
    date_dirs,
    function(date_value) all(file.exists(file.path(source_dir, date_value, required_files_by_source[[source_id]]))),
    logical(1)
  )]
})
common_dates <- Reduce(intersect, available_dates)
pull_date <- if (length(common_dates) == 0) format(Sys.Date(), "%Y%m%d") else max(common_dates)

seed_sources <- seed_sources_raw %>%
  mutate(
    required_flag = str_to_upper(str_squish(as.character(required_flag))) == "TRUE",
    pull_date = pull_date,
    raw_path = file.path(raw_root, source_id, pull_date, expected_filename)
  )

fetch_rows <- vector("list", nrow(seed_sources))

for (i in seq_len(nrow(seed_sources))) {
  source_row <- seed_sources[i, ]
  status <- if (file.exists(source_row$raw_path)) {
    "downloaded"
  } else {
    download_with_status(source_row$url, source_row$raw_path)
  }
  file_exists <- file.exists(source_row$raw_path)

  fetch_rows[[i]] <- source_row %>%
    mutate(
      fetch_status = status,
      file_exists = file_exists,
      file_size_bytes = ifelse(file_exists, file.info(raw_path)$size, NA_real_),
      checksum_sha256 = ifelse(file_exists, compute_sha256(raw_path), NA_character_)
    )
}

source_files <- bind_rows(fetch_rows) %>%
  select(
    source_id, source_role, source_label, seed_id, matter_id, matter_guid,
    matter_file, project_name, lu_numbers, resolution_numbers, ulurp_numbers,
    vote_date, council_disposition, vote_margin, url, expected_filename,
    required_flag, pull_date, raw_path, fetch_status, file_exists,
    file_size_bytes, checksum_sha256, notes
  )

required_failures <- source_files %>%
  filter(required_flag, fetch_status != "downloaded" | !file_exists)

if (nrow(required_failures) > 0) {
  stop("Required council land-use seed sources failed: ", paste(required_failures$source_label, collapse = "; "))
}

collapse_values <- function(x) {
  values <- unique(str_squish(as.character(x)))
  values <- values[!is.na(values) & values != ""]
  if (length(values) == 0) {
    return(NA_character_)
  }

  paste(values, collapse = "; ")
}

source_files <- source_files %>%
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

if (any(!records_qc$passed)) {
  stop("Council land-use staging failed: ", paste(records_qc$check_name[!records_qc$passed], collapse = ", "))
}
