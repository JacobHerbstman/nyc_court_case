# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_nhgis_extracts/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(ipumsr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

source_catalog <- read_csv("../input/source_catalog.csv", show_col_types = FALSE, na = c("", "NA"))
nhgis_table_map <- read_csv("nhgis_table_map.csv", show_col_types = FALSE, na = c("", "NA"))

identify_zip_role <- function(zip_path) {
  listing <- tryCatch(unzip(zip_path, list = TRUE), error = function(e) NULL)
  zip_name <- tolower(basename(zip_path))

  if (str_detect(zip_name, "_shape\\.zip$")) {
    return("gis_data")
  }

  if (is.null(listing)) {
    return("unknown")
  }

  listing_names <- tolower(listing$Name)

  if (any(str_detect(listing_names, "\\.(shp|dbf|shx|prj)$"))) {
    return("gis_data")
  }

  if (any(str_detect(listing_names, "shapefile.*\\.zip$"))) {
    return("gis_data")
  }

  if (any(str_detect(listing_names, "\\.(csv|dat)$"))) {
    return("table_data")
  }

  "unknown"
}

extract_number_from_path <- function(path) {
  suppressWarnings(as.integer(str_extract(basename(path), "(?<=nhgis)[0-9]{4}")))
}

read_zip_header_codes <- function(zip_path) {
  listing <- tryCatch(unzip(zip_path, list = TRUE), error = function(e) NULL)

  if (is.null(listing)) {
    return(character())
  }

  table_files <- listing$Name[
    str_detect(tolower(listing$Name), "\\.(csv|dat)$") &
      !str_detect(tolower(listing$Name), "(_datadict|_geog|_tables)\\.csv$")
  ]

  if (length(table_files) == 0) {
    return(character())
  }

  unique(unlist(lapply(table_files, function(table_file) {
    header_line <- read_lines(unz(zip_path, table_file), n_max = 1)
    normalize_names(str_split(header_line, ",", simplify = TRUE))
  })))
}

nhgis_specs <- tibble(
  source_id = "nhgis_1990_tract_extract",
  year = 1990L,
  spec_json = "nhgis_1990_extract.json"
)

nhgis_rows <- source_catalog %>%
  semi_join(nhgis_specs, by = "source_id") %>%
  left_join(nhgis_specs, by = "source_id", relationship = "many-to-one") %>%
  arrange(year)

if (nrow(nhgis_rows) != nrow(nhgis_specs) || !setequal(nhgis_rows$source_id, nhgis_specs$source_id)) {
  stop("Source catalog must contain the scripted NHGIS 1990 tract extract row.")
}

audit_rows <- list()

for (i in seq_len(nrow(nhgis_rows))) {
  row <- nhgis_rows[i, ]
  extract_spec <- define_extract_from_json(row$spec_json)
  raw_dir <- file.path("..", "..", "..", "data_raw", row$source_id, as.character(row$year))
  dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)

  existing_zips <- list.files(raw_dir, pattern = "\\.zip$", full.names = TRUE)

  if (length(existing_zips) > 0) {
    existing_roles <- vapply(existing_zips, identify_zip_role, character(1))
    expected_codes <- nhgis_table_map %>%
      filter(year == row$year) %>%
      pull(nhgis_code) %>%
      normalize_names() %>%
      unique()
    table_zips <- existing_zips[existing_roles == "table_data"]
    gis_zips <- existing_zips[existing_roles == "gis_data"]
    complete_table_bundle <- any(vapply(table_zips, function(path) {
      header_codes <- read_zip_header_codes(path)
      length(header_codes) > 0 && all(expected_codes %in% header_codes)
    }, logical(1)))
    complete_gis_bundle <- length(gis_zips) > 0

    if (complete_table_bundle && complete_gis_bundle) {
      audit_rows[[i]] <- tibble(
        source_id = row$source_id,
        year = row$year,
        extract_number = vapply(existing_zips, extract_number_from_path, integer(1)),
        extract_status = "not_queried",
        file_role = existing_roles,
        raw_path = existing_zips,
        checksum_sha256 = vapply(existing_zips, compute_sha256, character(1)),
        status = "already_present"
      )
      next
    }
  }

  fetch_result <- tryCatch(
    {
      api_key <- Sys.getenv("IPUMS_API_KEY")

      if (str_trim(api_key) == "") {
        stop(
          paste(
            "IPUMS_API_KEY is not set.",
            "Run ipumsr::set_ipums_api_key(\"<your key>\", save = TRUE) to write it to ~/.Renviron, then restart R and rerun this task."
          )
        )
      }

      submitted_extract <- submit_extract(extract_spec, api_key = api_key)
      ready_extract <- wait_for_extract(
        submitted_extract,
        initial_delay_seconds = 0,
        max_delay_seconds = 60,
        timeout_seconds = 10800,
        verbose = TRUE,
        api_key = api_key
      )
      downloaded_paths <- download_extract(
        ready_extract,
        download_dir = raw_dir,
        overwrite = FALSE,
        progress = TRUE,
        api_key = api_key
      )

      tibble(
        source_id = row$source_id,
        year = row$year,
        extract_number = ready_extract$number,
        extract_status = ready_extract$status,
        file_role = vapply(downloaded_paths, identify_zip_role, character(1)),
        raw_path = as.character(downloaded_paths),
        checksum_sha256 = vapply(as.character(downloaded_paths), compute_sha256, character(1)),
        status = "downloaded"
      )
    },
    error = function(e) {
      tibble(
        source_id = row$source_id,
        year = row$year,
        extract_number = NA_integer_,
        extract_status = "failed",
        file_role = NA_character_,
        raw_path = NA_character_,
        checksum_sha256 = NA_character_,
        status = paste0("fetch_failed:", str_replace_all(e$message, "[\\r\\n]+", " "))
      )
    }
  )

  audit_rows[[i]] <- fetch_result
}

write_csv_if_changed(bind_rows(audit_rows), "../output/nhgis_extract_downloads.csv")
cat("Wrote NHGIS extract downloads to ../output\n")
