suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

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

raw_df <- read_csv("../input/hpd_affordable_housing_production_by_building.csv", show_col_types = FALSE, na = c("", "NA"))
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

if (nrow(raw_df) == 0 || nrow(staged) == 0) {
  stop("HPD affordable housing input produced no staged rows.")
}

if (nrow(staged) != n_distinct(staged$hpd_row_id)) {
  stop("HPD staged row id is not unique.")
}

if (mean(!is.na(staged$bbl)) <= 0.70) {
  stop("HPD staged BBL coverage is below the expected threshold.")
}

if (sum(staged$total_units < 0 | staged$all_counted_units < 0, na.rm = TRUE) > 0) {
  stop("HPD staging found negative unit counts.")
}

if (nrow(bbl_year) != n_distinct(paste(bbl_year$bbl, bbl_year$hpd_year))) {
  stop("HPD BBL-year output is not unique.")
}

write_csv_if_changed(bbl_year, "../output/hpd_affordable_housing_bbl_year.csv")

cat("Staged HPD affordable housing BBL-year data to ../output/hpd_affordable_housing_bbl_year.csv\n")
