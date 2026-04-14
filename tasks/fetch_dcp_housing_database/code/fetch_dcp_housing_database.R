# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_dcp_housing_database/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# out_files_csv <- "../output/dcp_housing_database_files.csv"
# out_checksums_csv <- "../output/dcp_housing_database_checksums.csv"
# out_provenance_csv <- "../output/dcp_housing_database_provenance.csv"

suppressPackageStartupMessages({
  library(jsonlite)
  library(readr)
  library(stringr)
  library(tibble)
  library(dplyr)
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
source_row <- source_catalog %>% filter(source_id == "dcp_housing_database_project_level")

if (nrow(source_row) != 1) {
  stop("Source catalog must contain exactly one dcp_housing_database_project_level row.")
}

content_api_url <- source_row$official_url[1]
archive_json_url <- "https://www.nyc.gov/assets/planning/json/content/resources/dataset-archives/housing-project-level.json"
pull_date <- format(Sys.Date(), "%Y%m%d")

content_json <- fromJSON(content_api_url, simplifyVector = FALSE)
description_html <- content_json$description

extract_first_match <- function(text_value, pattern) {
  hit <- str_match(text_value, pattern)[, 2]
  if (length(hit) == 0) {
    return(NA_character_)
  }
  hit[1]
}

release_tag <- extract_first_match(description_html, "Latest Version:\\s*([^<]+)<")
csv_zip_url <- extract_first_match(description_html, "href=\\\"([^\\\"]+nychdb_[^\\\"]+_csv\\.zip)\\\"")
dictionary_url <- extract_first_match(description_html, "href=\\\"([^\\\"]+Housing_Database_Data_Dictionary\\.xlsx)\\\"")

if (is.na(release_tag) || is.na(csv_zip_url) || is.na(dictionary_url)) {
  stop("Could not parse the current DCP Housing Database release metadata from the content API page.")
}

metadata_dir <- file.path("..", "..", "..", "data_raw", "dcp_housing_database_project_level", pull_date)
release_dir <- file.path("..", "..", "..", "data_raw", "dcp_housing_database_project_level", release_tag)

dir.create(metadata_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(release_dir, recursive = TRUE, showWarnings = FALSE)

content_json_path <- file.path(metadata_dir, "housing_project_level_content_api.json")
archive_json_path <- file.path(metadata_dir, "housing_project_level_archive.json")
csv_zip_path <- file.path(release_dir, paste0("nychdb_", str_to_lower(release_tag), "_csv.zip"))
dictionary_path <- file.path(release_dir, "Housing_Database_Data_Dictionary.xlsx")

if (!file.exists(content_json_path)) {
  writeLines(toJSON(content_json, auto_unbox = TRUE, pretty = TRUE), content_json_path, useBytes = TRUE)
}

archive_status <- if (file.exists(archive_json_path)) {
  "already_present"
} else {
  download_with_status(archive_json_url, archive_json_path)
}

csv_status <- if (file.exists(csv_zip_path)) {
  "already_present"
} else {
  download_with_status(csv_zip_url, csv_zip_path)
}

dictionary_status <- if (file.exists(dictionary_path)) {
  "already_present"
} else {
  download_with_status(dictionary_url, dictionary_path)
}

file_inventory <- bind_rows(
  tibble(
    source_id = "dcp_housing_database_project_level",
    vintage = pull_date,
    pull_date = pull_date,
    file_role = "content_api_json",
    raw_path = content_json_path,
    status = "saved_from_content_api",
    official_url = content_api_url
  ),
  tibble(
    source_id = "dcp_housing_database_project_level",
    vintage = pull_date,
    pull_date = pull_date,
    file_role = "archive_json",
    raw_path = archive_json_path,
    status = archive_status,
    official_url = archive_json_url
  ),
  tibble(
    source_id = "dcp_housing_database_project_level",
    vintage = release_tag,
    pull_date = pull_date,
    file_role = "project_level_csv_zip",
    raw_path = csv_zip_path,
    status = csv_status,
    official_url = csv_zip_url
  ),
  tibble(
    source_id = "dcp_housing_database_project_level",
    vintage = release_tag,
    pull_date = pull_date,
    file_role = "data_dictionary_xlsx",
    raw_path = dictionary_path,
    status = dictionary_status,
    official_url = dictionary_url
  )
) %>%
  arrange(file_role, vintage)

checksum_table <- file_inventory %>%
  mutate(checksum_sha256 = if_else(file.exists(raw_path), vapply(raw_path, compute_sha256, character(1)), NA_character_)) %>%
  select(source_id, vintage, pull_date, file_role, raw_path, checksum_sha256)

provenance_table <- tibble(
  source_id = "dcp_housing_database_project_level",
  pull_date = pull_date,
  current_release = release_tag,
  csv_zip_url = csv_zip_url,
  dictionary_url = dictionary_url,
  archive_json_url = archive_json_url,
  note = "Fetched the current DCP Housing Database project-level CSV zip, data dictionary, and both current/archive metadata from official DCP content API endpoints."
)

write_csv(file_inventory, out_files_csv, na = "")
write_csv(checksum_table, out_checksums_csv, na = "")
write_csv(provenance_table, out_provenance_csv, na = "")

cat("Wrote DCP Housing Database fetch outputs to", dirname(out_files_csv), "\n")
