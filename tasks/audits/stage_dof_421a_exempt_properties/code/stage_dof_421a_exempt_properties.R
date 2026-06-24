# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/stage_dof_421a_exempt_properties/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(readxl)
  library(stringr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

parse_421a_file <- function(row) {
  raw <- read_excel(row$raw_path_resolved, col_names = FALSE, .name_repair = "unique")
  names(raw) <- paste0("col_", seq_along(raw))
  raw_chr <- raw %>%
    mutate(across(everything(), as.character))

  header_hits <- apply(raw_chr, 1, function(x) {
    upper <- str_to_upper(str_squish(x))
    any(upper == "BOROUGH", na.rm = TRUE) && any(upper == "BLOCK", na.rm = TRUE) && any(upper == "LOT", na.rm = TRUE)
  })

  if (!any(header_hits)) {
    stop("Could not locate BOROUGH/BLOCK/LOT header row in ", row$raw_path_resolved)
  }

  header_row <- which(header_hits)[1]
  header <- normalize_names(as.character(unlist(raw_chr[header_row, ])))
  header[is.na(header) | header == ""] <- paste0("unnamed_", which(is.na(header) | header == ""))
  header <- make.unique(header, sep = "_")
  data <- raw_chr[(header_row + 1):nrow(raw_chr), , drop = FALSE]
  names(data) <- header

  borough_raw <- pick_first_existing(data, c("borough"))
  block_raw <- pick_first_existing(data, c("block"))
  lot_raw <- pick_first_existing(data, c("lot"))

  tibble(
    source_file = basename(row$raw_path),
    fiscal_year_start = row$fiscal_year_start,
    fiscal_year_end = row$fiscal_year_end,
    borough_file = row$borough_file,
    borough_code = suppressWarnings(as.integer(borough_raw)),
    neighborhood = pick_first_existing(data, c("neighborhood")),
    building_class_category = pick_first_existing(data, c("building_class_category", "bldg_class_category")),
    tax_class = pick_first_existing(data, c("tax_class", "taxclass")),
    block = suppressWarnings(as.integer(block_raw)),
    lot = suppressWarnings(as.integer(lot_raw)),
    bbl = build_bbl(borough_raw, block_raw, lot_raw),
    building_class = pick_first_existing(data, c("building_class", "bldg_class")),
    address = pick_first_existing(data, c("address")),
    zip_code = suppressWarnings(as.integer(pick_first_existing(data, c("zip_code", "zipcode", "zip")))),
    residential_units = suppressWarnings(as.numeric(pick_first_existing(data, c("residential_units", "res_units")))),
    commercial_units = suppressWarnings(as.numeric(pick_first_existing(data, c("commercial_units", "comm_units")))),
    total_units = suppressWarnings(as.numeric(pick_first_existing(data, c("total_units", "units")))),
    land_square_feet = suppressWarnings(as.numeric(pick_first_existing(data, c("land_square_feet", "land_sqft", "land_area")))),
    gross_square_feet = suppressWarnings(as.numeric(pick_first_existing(data, c("gross_square_feet", "gross_sqft", "gross_area")))),
    year_built = suppressWarnings(as.integer(pick_first_existing(data, c("year_built", "yrbuilt"))))
  ) %>%
    filter(!is.na(bbl))
}

inventory_csv_target <- Sys.readlink("../input/dof_421a_raw_files.csv")
if (is.na(inventory_csv_target) || inventory_csv_target == "") {
  inventory_csv_target <- "../input/dof_421a_raw_files.csv"
}

inventory <- read_csv("../input/dof_421a_raw_files.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    raw_path_resolved = ifelse(
      file.exists(raw_path),
      raw_path,
      file.path(dirname(inventory_csv_target), raw_path)
    )
  ) %>%
  filter(status %in% c("downloaded", "available"), file.exists(raw_path_resolved))

if (nrow(inventory) == 0) {
  stop("No downloaded 421-a Excel files were found at resolved paths.")
}

rows <- bind_rows(lapply(seq_len(nrow(inventory)), function(i) parse_421a_file(inventory[i, ]))) %>%
  arrange(fiscal_year_end, borough_file, block, lot)

bbl_year <- rows %>%
  group_by(bbl, fiscal_year_end) %>%
  summarise(
    fiscal_year_start = first(fiscal_year_start),
    borough_code = first(borough_code),
    borough_file = first(borough_file),
    row_count = n(),
    residential_units = sum(residential_units, na.rm = TRUE),
    total_units = sum(total_units, na.rm = TRUE),
    min_year_built = suppressWarnings(min(year_built, na.rm = TRUE)),
    max_year_built = suppressWarnings(max(year_built, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  mutate(
    min_year_built = ifelse(is.infinite(min_year_built), NA_integer_, min_year_built),
    max_year_built = ifelse(is.infinite(max_year_built), NA_integer_, max_year_built)
  )

if (nrow(rows) == 0) {
  stop("No DOF 421-a property rows were parsed from downloaded files.")
}

if (nrow(bbl_year) != n_distinct(paste(bbl_year$bbl, bbl_year$fiscal_year_end))) {
  stop("DOF 421-a BBL-fiscal-year output is not unique.")
}

if (sum(rows$residential_units < 0 | rows$total_units < 0, na.rm = TRUE) > 0) {
  stop("DOF 421-a staging found negative unit counts.")
}

if (min(rows$fiscal_year_end, na.rm = TRUE) > 2014 || max(rows$fiscal_year_end, na.rm = TRUE) < 2025) {
  stop("DOF 421-a fiscal-year coverage is outside the expected range.")
}

write_csv_if_changed(bbl_year, "../output/dof_421a_exempt_bbl_year.csv")

cat("Staged DOF 421-a exemption BBL-year data to ../output/dof_421a_exempt_bbl_year.csv\n")
