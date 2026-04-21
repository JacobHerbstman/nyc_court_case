# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_dcp_council_boundary_archive/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# out_files_csv <- "../output/dcp_council_boundary_archive_files.csv"
# out_checksums_csv <- "../output/dcp_council_boundary_archive_checksums.csv"
# out_provenance_csv <- "../output/dcp_council_boundary_archive_provenance.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(jsonlite)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4) {
  stop("Expected 4 arguments: source_catalog_csv out_files_csv out_checksums_csv out_provenance_csv")
}

source_catalog_csv <- args[1]
out_files_csv <- args[2]
out_checksums_csv <- args[3]
out_provenance_csv <- args[4]

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
archive_row <- source_catalog |>
  filter(source_id == "dcp_boundary_city_council_districts_archive")

if (nrow(archive_row) != 1) {
  stop("Source catalog must contain exactly one dcp_boundary_city_council_districts_archive row.")
}

pull_date <- format(Sys.Date(), "%Y%m%d")
archive_json_path <- file.path(
  "..", "..", "..", "data_raw", "dcp_boundary_city_council_districts_archive", pull_date, "city_council_archive.json"
)

archive_json_status <- if (file.exists(archive_json_path)) {
  "already_present"
} else {
  download_with_status(archive_row$official_url[[1]], archive_json_path)
}

if (!file.exists(archive_json_path)) {
  stop("Could not download the official DCP council archive JSON from ", archive_row$official_url[[1]])
}

archive_json <- fromJSON(archive_json_path, simplifyVector = FALSE)
archive_rows <- archive_json[vapply(
  archive_json,
  function(x) identical(x$dataset, "City Council Districts (Clipped to Shoreline)"),
  logical(1)
)]

if (length(archive_rows) == 0) {
  stop("Could not find shoreline-clipped council releases in ", archive_json_path)
}

archive_release_rows <- bind_rows(lapply(archive_rows, function(row) {
  bind_rows(lapply(row$releases, function(release_row) {
    tibble(
      archive_year = suppressWarnings(as.integer(row$year)),
      release = str_to_upper(as.character(release_row$text)),
      official_url = as.character(release_row$link),
      source_filename = basename(as.character(release_row$link))
    )
  }))
})) |>
  mutate(
    source_format = case_when(
      str_detect(source_filename, regex("av\\.zip$", ignore_case = TRUE)) ~ "arcview_shapefile",
      str_detect(source_filename, regex("mi\\.zip$", ignore_case = TRUE)) ~ "mapinfo_tab",
      TRUE ~ "other"
    ),
    source_format_rank = case_when(
      source_format == "arcview_shapefile" ~ 1L,
      source_format == "mapinfo_tab" ~ 2L,
      TRUE ~ 3L
    )
  ) |>
  group_by(archive_year, release) |>
  arrange(source_format_rank, source_filename, .by_group = TRUE) |>
  slice_head(n = 1) |>
  ungroup() |>
  arrange(archive_year, release)

if (nrow(archive_release_rows) == 0) {
  stop("The official DCP council archive JSON returned zero shoreline-clipped releases.")
}

inventory_rows <- list(
  tibble(
    source_id = "dcp_boundary_city_council_districts_archive",
    archive_year = NA_integer_,
    release = pull_date,
    pull_date = pull_date,
    file_role = "archive_json",
    raw_path = archive_json_path,
    status = archive_json_status,
    official_url = archive_row$official_url[[1]]
  )
)

for (i in seq_len(nrow(archive_release_rows))) {
  release_row <- archive_release_rows[i, ]
  raw_path <- file.path(
    "..", "..", "..", "data_raw", "dcp_boundary_city_council_districts_archive",
    release_row$release,
    basename(release_row$official_url)
  )

  status_value <- if (file.exists(raw_path)) {
    "already_present"
  } else {
    download_with_status(release_row$official_url, raw_path)
  }

  inventory_rows[[length(inventory_rows) + 1L]] <- tibble(
    source_id = "dcp_boundary_city_council_districts_archive",
    archive_year = release_row$archive_year,
    release = release_row$release,
    pull_date = pull_date,
    file_role = "boundary_shapefile_zip",
    raw_path = raw_path,
    status = status_value,
    official_url = release_row$official_url,
    source_format = release_row$source_format
  )
}

file_inventory <- bind_rows(inventory_rows) |>
  arrange(file_role, archive_year, release, raw_path)

checksum_table <- file_inventory |>
  mutate(
    file_exists = file.exists(raw_path),
    checksum_sha256 = if_else(file_exists, vapply(raw_path, compute_sha256, character(1)), NA_character_)
  ) |>
  select(source_id, archive_year, release, pull_date, file_role, raw_path, checksum_sha256)

provenance_table <- tibble(
  source_id = "dcp_boundary_city_council_districts_archive",
  pull_date = pull_date,
  metadata_path = archive_json_path,
  metadata_kind = "dcp_archive_json",
  release_count = nrow(archive_release_rows),
  first_archive_year = min(archive_release_rows$archive_year, na.rm = TRUE),
  last_archive_year = max(archive_release_rows$archive_year, na.rm = TRUE),
  note = "Official DCP council-boundary archive JSON listing previous BYTES releases for the shoreline-clipped city council district shapefile."
)

write_csv(file_inventory, out_files_csv, na = "")
write_csv(checksum_table, out_checksums_csv, na = "")
write_csv(provenance_table, out_provenance_csv, na = "")

cat("Wrote DCP council boundary archive fetch outputs to", dirname(out_files_csv), "\n")
