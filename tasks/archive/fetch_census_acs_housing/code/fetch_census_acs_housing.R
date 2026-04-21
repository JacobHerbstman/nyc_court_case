# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_census_acs_housing/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# out_files_csv <- "../output/census_acs_housing_files.csv"
# out_checksums_csv <- "../output/census_acs_housing_checksums.csv"
# out_provenance_csv <- "../output/census_acs_housing_provenance.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
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
source_row <- source_catalog %>% filter(source_id == "census_acs_housing_tract_5yr")

if (nrow(source_row) != 1) {
  stop("Source catalog must contain exactly one census_acs_housing_tract_5yr row.")
}

years <- 2009:2024
table_ids <- c("B25002", "B25003", "B25024")
county_codes <- c("005", "047", "061", "081", "085")
pull_date <- format(Sys.Date(), "%Y%m%d")

inventory_rows <- list()

for (year_value in years) {
  for (table_id in table_ids) {
    for (county_code in county_codes) {
      file_url <- paste0(
        "https://api.census.gov/data/", year_value,
        "/acs/acs5?get=NAME,GEO_ID,group(", table_id, ")&for=tract:*&in=state:36%20county:", county_code
      )
      raw_path <- file.path(
        "..", "..", "..", "data_raw", "census_acs_housing_tract_5yr", as.character(year_value),
        paste0("acs5_", year_value, "_", table_id, "_", county_code, ".json")
      )
      file_status <- if (file.exists(raw_path)) "already_present" else download_with_status(file_url, raw_path)

      inventory_rows[[length(inventory_rows) + 1L]] <- tibble(
        source_id = "census_acs_housing_tract_5yr",
        vintage = as.character(year_value),
        pull_date = pull_date,
        table_id = table_id,
        county_code = county_code,
        file_role = "acs_group_json",
        raw_path = raw_path,
        status = file_status,
        official_url = file_url
      )
    }
  }
}

file_inventory <- bind_rows(inventory_rows) %>%
  arrange(vintage, table_id, county_code)

checksum_table <- file_inventory %>%
  mutate(checksum_sha256 = if_else(file.exists(raw_path), vapply(raw_path, compute_sha256, character(1)), NA_character_)) %>%
  select(source_id, vintage, pull_date, table_id, county_code, file_role, raw_path, checksum_sha256)

provenance_table <- tibble(
  source_id = "census_acs_housing_tract_5yr",
  pull_date = pull_date,
  year_start = min(years),
  year_end = max(years),
  table_ids = paste(table_ids, collapse = ";"),
  county_codes = paste(county_codes, collapse = ";"),
  note = "Fetched NYC tract-level ACS 5-year group queries for B25002, B25003, and B25024 for all five NYC counties."
)

write_csv(file_inventory, out_files_csv, na = "")
write_csv(checksum_table, out_checksums_csv, na = "")
write_csv(provenance_table, out_provenance_csv, na = "")
cat("Wrote Census ACS housing fetch outputs to", dirname(out_files_csv), "\n")
