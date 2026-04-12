# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_nhgis_extracts/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# nhgis_1980_extract_json <- "nhgis_1980_extract.json"
# nhgis_1990_extract_json <- "nhgis_1990_extract.json"
# nhgis_table_map_csv <- "nhgis_table_map.csv"
# out_audit_csv <- "../output/nhgis_extract_downloads.csv"
# out_roundtrip_csv <- "../output/nhgis_extract_roundtrip_checks.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(ipumsr)
  library(jsonlite)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 6) {
  stop("Expected 6 arguments: source_catalog_csv nhgis_1980_extract_json nhgis_1990_extract_json nhgis_table_map_csv out_audit_csv out_roundtrip_csv")
}

source_catalog_csv <- args[1]
nhgis_1980_extract_json <- args[2]
nhgis_1990_extract_json <- args[3]
nhgis_table_map_csv <- args[4]
out_audit_csv <- args[5]
out_roundtrip_csv <- args[6]

api_key <- Sys.getenv("IPUMS_API_KEY")

if (str_trim(api_key) == "") {
  stop(
    paste(
      "IPUMS_API_KEY is not set.",
      "Run ipumsr::set_ipums_api_key(\"<your key>\", save = TRUE) to write it to ~/.Renviron, then restart R and rerun this task."
    )
  )
}

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
nhgis_table_map <- read_csv(nhgis_table_map_csv, show_col_types = FALSE, na = c("", "NA"))

extract_tables_from_json <- function(path) {
  spec <- fromJSON(path, simplifyVector = FALSE)
  dataset_names <- names(spec$datasets)

  bind_rows(lapply(dataset_names, function(dataset_name) {
    tibble(
      dataset_name = dataset_name,
      data_table = unlist(spec$datasets[[dataset_name]]$dataTables)
    )
  }))
}

identify_zip_role <- function(zip_path) {
  listing <- tryCatch(unzip(zip_path, list = TRUE), error = function(e) NULL)

  if (is.null(listing)) {
    return("unknown")
  }

  listing_names <- tolower(listing$Name)

  if (any(str_detect(listing_names, "\\.(shp|dbf|shx|prj)$"))) {
    return("gis_data")
  }

  if (any(str_detect(listing_names, "\\.(csv|dat)$"))) {
    return("table_data")
  }

  "unknown"
}

nhgis_specs <- tibble(
  source_id = c("nhgis_1980_tract_extract", "nhgis_1990_tract_extract"),
  year = c(1980L, 1990L),
  spec_json = c(nhgis_1980_extract_json, nhgis_1990_extract_json)
)

nhgis_rows <- source_catalog |>
  semi_join(nhgis_specs, by = "source_id") |>
  left_join(nhgis_specs, by = "source_id")

audit_rows <- list()
roundtrip_rows <- list()

for (i in seq_len(nrow(nhgis_rows))) {
  row <- nhgis_rows[i, ]
  extract_spec <- define_extract_from_json(row$spec_json)
  roundtrip_json <- tempfile(fileext = ".json")
  save_extract_as_json(extract_spec, roundtrip_json, overwrite = TRUE)

  spec_pairs <- extract_tables_from_json(row$spec_json)
  table_map_pairs <- nhgis_table_map |>
    filter(year == row$year) |>
    distinct(dataset_name, data_table)

  roundtrip_rows[[i]] <- tibble(
    source_id = row$source_id,
    year = row$year,
    roundtrip_ok = isTRUE(all.equal(
      fromJSON(row$spec_json, simplifyVector = FALSE),
      fromJSON(roundtrip_json, simplifyVector = FALSE),
      check.attributes = FALSE
    )),
    spec_tables_match_table_map = setequal(
      paste(spec_pairs$dataset_name, spec_pairs$data_table),
      paste(table_map_pairs$dataset_name, table_map_pairs$data_table)
    ),
    datasets = paste(names(extract_spec$datasets), collapse = ";"),
    shapefiles = paste(extract_spec$shapefiles, collapse = ";")
  )

  raw_dir <- file.path("..", "..", "..", "data_raw", row$source_id, as.character(row$year))
  dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)

  existing_zips <- list.files(raw_dir, pattern = "\\.zip$", full.names = TRUE)

  if (length(existing_zips) > 0) {
    existing_roles <- vapply(existing_zips, identify_zip_role, character(1))
    existing_status <- if (all(c("table_data", "gis_data") %in% existing_roles)) "already_present" else "partial_bundle_present"

    audit_rows[[i]] <- tibble(
      source_id = row$source_id,
      year = row$year,
      extract_number = NA_integer_,
      extract_status = "not_queried",
      file_role = existing_roles,
      raw_path = existing_zips,
      checksum_sha256 = vapply(existing_zips, compute_sha256, character(1)),
      status = existing_status
    )
    next
  }

  fetch_result <- tryCatch(
    {
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

write_csv(bind_rows(audit_rows), out_audit_csv, na = "")
write_csv(bind_rows(roundtrip_rows), out_roundtrip_csv, na = "")
cat("Wrote NHGIS extract audit outputs to", dirname(out_audit_csv), "\n")
