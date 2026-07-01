suppressPackageStartupMessages({
  library(jsonlite)
  library(readr)
  library(stringr)
  library(tibble)
  library(dplyr)
})

source("../../../_lib/source_pipeline_utils.R")

source_catalog <- read_csv("../input/source_catalog.csv", show_col_types = FALSE, na = c("", "NA"))
source_row <- source_catalog %>% filter(source_id == "dcp_housing_database_project_level")

if (nrow(source_row) != 1) {
  stop("Source catalog must contain exactly one dcp_housing_database_project_level row.")
}

content_api_url <- source_row$official_url[1]
archive_json_url <- "https://www.nyc.gov/assets/planning/json/content/resources/dataset-archives/housing-project-level.json"
pull_date <- resolve_raw_pull_date(list(
  dcp_housing_database_project_level = c(
    "housing_project_level_content_api.json",
    "housing_project_level_archive.json"
  )
))
metadata_dir <- file.path("..", "..", "..", "..", "data_raw", "dcp_housing_database_project_level", pull_date)
content_json_path <- file.path(metadata_dir, "housing_project_level_content_api.json")
archive_json_path <- file.path(metadata_dir, "housing_project_level_archive.json")

content_json <- if (file.exists(content_json_path)) {
  fromJSON(content_json_path, simplifyVector = FALSE)
} else {
  fromJSON(content_api_url, simplifyVector = FALSE)
}
description_html <- content_json$description

extract_first_match <- function(text_value, pattern) {
  hit <- str_match(text_value, pattern)[, 2]
  if (length(hit) == 0) {
    return(NA_character_)
  }
  hit[1]
}

normalize_nyc_url <- function(url) {
  url <- str_squish(as.character(url))

  if (is.na(url) || url == "") {
    return(NA_character_)
  }

  if (str_detect(url, "^https?://")) {
    return(url)
  }

  if (str_starts(url, "//")) {
    return(paste0("https:", url))
  }

  if (str_starts(url, "/")) {
    return(paste0("https://www.nyc.gov", url))
  }

  paste0("https://www.nyc.gov/", url)
}

release_tag <- str_squish(extract_first_match(description_html, "Latest Version:\\s*([^<]+)<"))
csv_zip_url <- normalize_nyc_url(extract_first_match(description_html, "href=\\\"([^\\\"]+nychdb_[^\\\"]+_csv\\.zip)\\\""))
dictionary_url <- normalize_nyc_url(extract_first_match(description_html, "href=\\\"([^\\\"]+Housing_Database_Data_Dictionary\\.xlsx)\\\""))

if (is.na(release_tag) || is.na(csv_zip_url) || is.na(dictionary_url)) {
  stop("Could not parse the current DCP Housing Database release metadata from the content API page.")
}

if (basename(csv_zip_url) != paste0("nychdb_", str_to_lower(release_tag), "_csv.zip")) {
  stop("Parsed Housing Database CSV URL does not match the current release tag.")
}

if (basename(dictionary_url) != "Housing_Database_Data_Dictionary.xlsx") {
  stop("Parsed Housing Database data dictionary URL has an unexpected file name.")
}

release_dir <- file.path("..", "..", "..", "..", "data_raw", "dcp_housing_database_project_level", release_tag)

dir.create(metadata_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(release_dir, recursive = TRUE, showWarnings = FALSE)

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

write_csv_if_changed(file_inventory, "../output/dcp_housing_database_source_files.csv")

cat("Wrote DCP Housing Database fetch outputs to ../output\n")
