# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_dof_421a_exempt_properties/code")
# dof_421a_raw_files_csv <- "../input/dof_421a_raw_files.csv"
# out_rows_csv <- "../output/dof_421a_exempt_property_rows.csv"
# out_bbl_year_csv <- "../output/dof_421a_exempt_bbl_year.csv"
# out_qc_csv <- "../output/dof_421a_stage_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(readxl)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4) {
  stop("Expected 4 arguments: dof_421a_raw_files_csv out_rows_csv out_bbl_year_csv out_qc_csv")
}

dof_421a_raw_files_csv <- args[1]
out_rows_csv <- args[2]
out_bbl_year_csv <- args[3]
out_qc_csv <- args[4]

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

inventory_csv_target <- Sys.readlink(dof_421a_raw_files_csv)
if (is.na(inventory_csv_target) || inventory_csv_target == "") {
  inventory_csv_target <- dof_421a_raw_files_csv
}

inventory <- read_csv(dof_421a_raw_files_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    raw_path_resolved = ifelse(
      file.exists(raw_path),
      raw_path,
      file.path(dirname(inventory_csv_target), raw_path)
    )
  ) %>%
  filter(status == "downloaded", file.exists(raw_path_resolved))

if (nrow(inventory) == 0) {
  qc_df <- tibble(metric = "input_file_count", value = 0, status = "fail", note = "No downloaded 421-a Excel files were found at resolved paths.")
  write_csv_if_changed(qc_df, out_qc_csv)
  stop("DOF 421-a staging QC failed.")
}

rows <- bind_rows(lapply(seq_len(nrow(inventory)), function(i) parse_421a_file(inventory[i, ]))) %>%
  mutate(source_row_id = row_number()) %>%
  select(source_row_id, everything()) %>%
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

qc_df <- bind_rows(
  tibble(metric = "input_file_count", value = nrow(inventory), status = if_else(nrow(inventory) > 0, "pass", "fail"), note = "Downloaded 421-a Excel files staged."),
  tibble(metric = "staged_row_count", value = nrow(rows), status = if_else(nrow(rows) > 0, "pass", "fail"), note = "Property rows parsed from Excel files."),
  tibble(metric = "source_row_id_duplicate_count", value = nrow(rows) - n_distinct(rows$source_row_id), status = if_else(nrow(rows) == n_distinct(rows$source_row_id), "pass", "fail"), note = "Staged row id should be unique."),
  tibble(metric = "bbl_year_duplicate_count", value = nrow(bbl_year) - n_distinct(paste(bbl_year$bbl, bbl_year$fiscal_year_end)), status = if_else(nrow(bbl_year) == n_distinct(paste(bbl_year$bbl, bbl_year$fiscal_year_end)), "pass", "fail"), note = "Collapsed BBL-fiscal-year table should be unique."),
  tibble(metric = "negative_unit_count", value = sum(rows$residential_units < 0 | rows$total_units < 0, na.rm = TRUE), status = if_else(sum(rows$residential_units < 0 | rows$total_units < 0, na.rm = TRUE) == 0, "pass", "fail"), note = "Unit counts must be nonnegative."),
  tibble(metric = "fiscal_year_min", value = min(rows$fiscal_year_end, na.rm = TRUE), status = if_else(min(rows$fiscal_year_end, na.rm = TRUE) <= 2014, "pass", "fail"), note = "Expected support to FY2013/14."),
  tibble(metric = "fiscal_year_max", value = max(rows$fiscal_year_end, na.rm = TRUE), status = if_else(max(rows$fiscal_year_end, na.rm = TRUE) >= 2025, "pass", "fail"), note = "Expected support through at least FY2024/25.")
)

if (any(qc_df$status == "fail")) {
  write_csv_if_changed(qc_df, out_qc_csv)
  stop("DOF 421-a staging QC failed.")
}

write_csv_if_changed(rows, out_rows_csv)
write_csv_if_changed(bbl_year, out_bbl_year_csv)
write_csv_if_changed(qc_df, out_qc_csv)

cat("Staged DOF 421-a exemption records to", out_rows_csv, "\n")
