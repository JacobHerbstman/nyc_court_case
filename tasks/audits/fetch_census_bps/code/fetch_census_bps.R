# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/fetch_census_bps/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

source_catalog <- read_csv("../input/source_catalog.csv", show_col_types = FALSE, na = c("", "NA"))
bps_row <- source_catalog |> filter(source_id == "census_bps_place_ascii")

if (nrow(bps_row) != 1) {
  stop("Source catalog must contain exactly one census_bps_place_ascii row.")
}

years <- 1980:2024
pull_date <- resolve_raw_pull_date(list(census_bps_place_ascii = "placeasc.pdf"))
inventory_rows <- list()

for (year_value in years) {
  file_url <- paste0("https://www2.census.gov/econ/bps/Place/Northeast%20Region/ne", year_value, "a.txt")
  raw_path <- file.path("..", "..", "..", "..", "data_raw", "census_bps_place_ascii", as.character(year_value), paste0("ne", year_value, "a.txt"))
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
documentation_path <- file.path("..", "..", "..", "..", "data_raw", "census_bps_place_ascii", pull_date, "placeasc.pdf")
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

write_csv_if_changed(file_inventory, "../output/census_bps_files.csv")

cat("Wrote Census BPS fetch outputs to ../output\n")
