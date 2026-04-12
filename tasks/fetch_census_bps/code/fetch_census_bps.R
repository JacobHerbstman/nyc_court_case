# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_census_bps/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# out_files_csv <- "../output/census_bps_files.csv"
# out_checksums_csv <- "../output/census_bps_checksums.csv"
# out_provenance_csv <- "../output/census_bps_provenance.csv"

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
bps_row <- source_catalog |> filter(source_id == "census_bps_place_ascii")

if (nrow(bps_row) != 1) {
  stop("Source catalog must contain exactly one census_bps_place_ascii row.")
}

years <- 1980:2024
pull_date <- format(Sys.Date(), "%Y%m%d")
inventory_rows <- list()

for (year_value in years) {
  file_url <- paste0("https://www2.census.gov/econ/bps/Place/Northeast%20Region/ne", year_value, "a.txt")
  raw_path <- file.path("..", "..", "..", "data_raw", "census_bps_place_ascii", as.character(year_value), paste0("ne", year_value, "a.txt"))
  file_status <- if (file.exists(raw_path)) "already_present" else download_with_status(file_url, raw_path)

  inventory_rows[[length(inventory_rows) + 1L]] <- tibble(
    source_id = "census_bps_place_ascii",
    vintage = as.character(year_value),
    pull_date = pull_date,
    file_role = "annual_place_ascii",
    raw_path = raw_path,
    status = file_status,
    official_url = file_url
  )
}

documentation_url <- "https://www.census.gov/construction/bps/sample/placeasc.pdf"
documentation_path <- file.path("..", "..", "..", "data_raw", "census_bps_place_ascii", pull_date, "placeasc.pdf")
documentation_status <- if (file.exists(documentation_path)) "already_present" else download_with_status(documentation_url, documentation_path)

inventory_rows[[length(inventory_rows) + 1L]] <- tibble(
  source_id = "census_bps_place_ascii",
  vintage = pull_date,
  pull_date = pull_date,
  file_role = "documentation_pdf",
  raw_path = documentation_path,
  status = documentation_status,
  official_url = documentation_url
)

file_inventory <- bind_rows(inventory_rows) |> arrange(vintage, file_role)
checksum_table <- file_inventory |>
  mutate(checksum_sha256 = if_else(file.exists(raw_path), vapply(raw_path, compute_sha256, character(1)), NA_character_)) |>
  select(source_id, vintage, pull_date, file_role, raw_path, checksum_sha256)

provenance_table <- tibble(
  source_id = "census_bps_place_ascii",
  pull_date = pull_date,
  year_start = min(years),
  year_end = max(years),
  documentation_path = documentation_path,
  note = "Downloaded Northeast regional annual place ASCII files ne<YYYY>a.txt for 1980 through 2024 plus the official place ASCII documentation PDF."
)

write_csv(file_inventory, out_files_csv, na = "")
write_csv(checksum_table, out_checksums_csv, na = "")
write_csv(provenance_table, out_provenance_csv, na = "")

cat("Wrote Census BPS fetch outputs to", dirname(out_files_csv), "\n")
