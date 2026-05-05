# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_hpd_affordable_housing_production/code")
# hpd_raw_csv <- "../input/hpd_affordable_housing_production_by_building.csv"
# out_staged_csv <- "../output/hpd_affordable_housing_building_staged.csv"
# out_staged_parquet <- "../output/hpd_affordable_housing_building_staged.parquet"
# out_bbl_year_csv <- "../output/hpd_affordable_housing_bbl_year.csv"
# out_qc_csv <- "../output/hpd_affordable_housing_stage_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 5) {
  stop("Expected 5 arguments: hpd_raw_csv out_staged_csv out_staged_parquet out_bbl_year_csv out_qc_csv")
}

hpd_raw_csv <- args[1]
out_staged_csv <- args[2]
out_staged_parquet <- args[3]
out_bbl_year_csv <- args[4]
out_qc_csv <- args[5]

safe_min_year <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) {
    return(NA_integer_)
  }

  as.integer(min(x))
}

safe_max_year <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) {
    return(NA_integer_)
  }

  as.integer(max(x))
}

raw_df <- read_csv(hpd_raw_csv, show_col_types = FALSE, na = c("", "NA"))
names(raw_df) <- normalize_names(names(raw_df))

required_cols <- c("project_id", "building_id", "bbl", "bin", "borough", "community_board", "project_start_date", "building_completion_date", "all_counted_units", "total_units")
missing_cols <- setdiff(required_cols, names(raw_df))

if (length(missing_cols) > 0) {
  stop("HPD raw file is missing required columns: ", paste(missing_cols, collapse = ", "))
}

staged <- raw_df %>%
  transmute(
    hpd_row_id = row_number(),
    project_id = as.character(project_id),
    project_name = as.character(project_name),
    project_start_date = parse_mixed_date(project_start_date),
    project_completion_date = parse_mixed_date(project_completion_date),
    building_id = as.character(building_id),
    bbl = ifelse(is.na(bbl), NA_character_, sprintf("%010.0f", suppressWarnings(as.numeric(bbl)))),
    bin = ifelse(is.na(bin), NA_character_, sprintf("%.0f", suppressWarnings(as.numeric(bin)))),
    borough_name = standardize_borough_name(borough),
    borocd = standardize_community_district(borough, community_board),
    council_district = standardize_council_district(council_district),
    address = combine_address(number, street),
    building_completion_date = parse_mixed_date(building_completion_date),
    construction_type = as.character(reporting_construction_type),
    extended_affordability_only = as.character(extended_affordability_only),
    counted_rental_units = suppressWarnings(as.numeric(counted_rental_units)),
    counted_homeownership_units = suppressWarnings(as.numeric(counted_homeownership_units)),
    all_counted_units = suppressWarnings(as.numeric(all_counted_units)),
    total_units = suppressWarnings(as.numeric(total_units))
  ) %>%
  mutate(
    project_start_year = suppressWarnings(as.integer(format(project_start_date, "%Y"))),
    project_completion_year = suppressWarnings(as.integer(format(project_completion_date, "%Y"))),
    building_completion_year = suppressWarnings(as.integer(format(building_completion_date, "%Y"))),
    hpd_year = coalesce(building_completion_year, project_completion_year, project_start_year)
  )

bbl_year <- staged %>%
  filter(!is.na(bbl), !is.na(hpd_year)) %>%
  group_by(bbl, hpd_year) %>%
  summarise(
    hpd_building_rows = n(),
    hpd_project_count = n_distinct(project_id),
    hpd_total_units = sum(total_units, na.rm = TRUE),
    hpd_counted_units = sum(all_counted_units, na.rm = TRUE),
    hpd_min_project_start_year = safe_min_year(project_start_year),
    hpd_max_completion_year = safe_max_year(coalesce(building_completion_year, project_completion_year)),
    .groups = "drop"
  )

qc_df <- bind_rows(
  tibble(metric = "raw_row_count", value = nrow(raw_df), status = if_else(nrow(raw_df) > 0, "pass", "fail"), note = "Raw HPD building rows."),
  tibble(metric = "staged_row_count", value = nrow(staged), status = if_else(nrow(staged) > 0, "pass", "fail"), note = "Staged HPD building rows."),
  tibble(metric = "source_row_id_duplicate_count", value = nrow(staged) - n_distinct(staged$hpd_row_id), status = if_else(nrow(staged) == n_distinct(staged$hpd_row_id), "pass", "fail"), note = "Staged row id should be unique."),
  tibble(metric = "nonmissing_bbl_share", value = mean(!is.na(staged$bbl)), status = if_else(mean(!is.na(staged$bbl)) > 0.70, "pass", "fail"), note = "Only HPD rows with BBL are available for BBL matching; missingness is reported rather than imputed."),
  tibble(metric = "missing_bbl_row_count", value = sum(is.na(staged$bbl)), status = "pass", note = "Rows without BBL remain in the staged building file but are excluded from BBL-year matching."),
  tibble(metric = "negative_units_count", value = sum(staged$total_units < 0 | staged$all_counted_units < 0, na.rm = TRUE), status = if_else(sum(staged$total_units < 0 | staged$all_counted_units < 0, na.rm = TRUE) == 0, "pass", "fail"), note = "Unit counts must be nonnegative."),
  tibble(metric = "bbl_year_duplicate_count", value = nrow(bbl_year) - n_distinct(paste(bbl_year$bbl, bbl_year$hpd_year)), status = if_else(nrow(bbl_year) == n_distinct(paste(bbl_year$bbl, bbl_year$hpd_year)), "pass", "fail"), note = "Collapsed HPD BBL-year table should be unique.")
)

if (any(qc_df$status == "fail")) {
  write_csv_if_changed(qc_df, out_qc_csv)
  stop("HPD affordable housing staging QC failed.")
}

write_csv_if_changed(staged, out_staged_csv)
write_parquet_if_changed(staged, out_staged_parquet)
write_csv_if_changed(bbl_year, out_bbl_year_csv)
write_csv_if_changed(qc_df, out_qc_csv)

cat("Staged HPD affordable housing production to", out_staged_csv, "\n")
