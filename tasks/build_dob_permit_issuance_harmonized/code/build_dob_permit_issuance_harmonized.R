# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_dob_permit_issuance_harmonized/code")
# current_raw_files_csv <- "../input/dob_permit_issuance_current_raw_files.csv"
# current_raw_parquet <- "../input/dob_permit_issuance_current_raw.parquet"
# current_source_decision_csv <- "current_source_decision_1989_2013.csv"
# census_bps_city_year_parquet <- "../input/census_bps_city_year.parquet"
# out_parquet <- "../output/dob_permit_issuance_harmonized.parquet"
# out_qc_csv <- "../output/dob_permit_issuance_harmonized_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")
source("../../_lib/dob_permit_issuance_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 6) {
  stop("Expected 6 arguments for build_dob_permit_issuance_harmonized.R")
}

current_raw_files_csv <- args[1]
current_raw_parquet <- args[2]
current_source_decision_csv <- args[3]
census_bps_city_year_parquet <- args[4]
out_parquet <- args[5]
out_qc_csv <- args[6]

if (!file.exists(current_raw_parquet)) {
  stop("Current permit issuance raw parquet is required.")
}

current_index <- read_csv(current_raw_files_csv, show_col_types = FALSE, na = c("", "NA"))
comparison_audit <- read_csv(current_source_decision_csv, show_col_types = FALSE, na = c("", "NA"))

required_years <- tibble(record_year = 1989:2013)

comparison_check_df <- required_years %>%
  left_join(comparison_audit, by = "record_year")

if (any(is.na(comparison_check_df$current_row_count)) || any(is.na(comparison_check_df$historical_row_count))) {
  stop("Comparison audit must include current and historical row counts for every year from 1989 through 2013.")
}

if (any(!comparison_check_df$current_ge_historical_row_count_flag, na.rm = TRUE)) {
  stop("Current permit issuance source does not dominate historical row coverage in every year from 1989 through 2013.")
}

current_raw_df <- read_parquet(current_raw_parquet) %>%
  as.data.frame() %>%
  as_tibble()

current_canonical <- canonicalize_dob_permit_source(
  raw_df = current_raw_df,
  source_id = "dob_permit_issuance_current",
  dataset_id = unique(current_index$dataset_id)[1],
  pull_date = unique(current_index$pull_date)[1],
  source_raw_path = unique(current_index$raw_path)[1]
)

harmonized_df <- current_canonical %>%
  mutate(
    canonical_source_id = "dob_permit_issuance_current",
    issuance_date_missing_flag = is.na(issuance_date),
    harmonization_role = "canonical_current_source",
    source_precedence = "current_public_dataset",
    harmonization_reason = ifelse(
      issuance_date_missing_flag,
      "Retained from the canonical current public permit issuance dataset; issuance date is missing in source and remains un-imputed.",
      "Retained from the canonical current public permit issuance dataset after the 1989-2013 comparison audit showed current coverage is at least as large as historical in every year."
    )
  ) %>%
  select(
    canonical_source_id,
    issuance_date_missing_flag,
    harmonization_role,
    everything()
  )

duplicate_permit_identifier_rows <- sum(duplicated(harmonized_df$permit_identifier) | duplicated(harmonized_df$permit_identifier, fromLast = TRUE))

if (duplicate_permit_identifier_rows > 0) {
  stop("Current-source canonical permit identifiers are not unique; inspect the current permit raw source before writing the unified dataset.")
}

write_parquet_if_changed(harmonized_df, out_parquet)

bps_city_year <- if (file.exists(census_bps_city_year_parquet)) {
  read_parquet(census_bps_city_year_parquet) %>%
    as.data.frame() %>%
    as_tibble()
} else {
  tibble()
}

harmonized_nb_city_year <- harmonized_df %>%
  filter(job_type == "New Building", !is.na(record_year)) %>%
  group_by(record_year) %>%
  summarise(harmonized_nb_permit_rows = n(), .groups = "drop")

bps_compare_df <- if (nrow(bps_city_year) == 0) {
  tibble(record_year = integer(), harmonized_nb_permit_rows = double(), city_total_units = double())
} else {
  harmonized_nb_city_year %>%
    inner_join(
      bps_city_year %>%
        transmute(record_year = as.integer(year), city_total_units = as.numeric(city_total_units)),
      by = "record_year"
    )
}

qc_df <- bind_rows(
  tibble(metric = "canonical_source_id", value = "dob_permit_issuance_current", note = "Canonical public permit issuance source used for the unified dataset."),
  tibble(metric = "row_count_equals_current_source", value = as.character(nrow(harmonized_df) == nrow(current_canonical)), note = "Unified dataset row count should exactly equal the current-source canonical row count."),
  tibble(metric = "total_rows_harmonized", value = as.character(nrow(harmonized_df)), note = "Rows retained in the unified public permit issuance dataset."),
  tibble(metric = "earliest_issuance_date", value = as.character(safe_min_date(harmonized_df$issuance_date)), note = "Earliest nonmissing issuance date in the unified dataset."),
  tibble(metric = "latest_issuance_date", value = as.character(safe_max_date(harmonized_df$issuance_date)), note = "Latest nonmissing issuance date in the unified dataset."),
  tibble(metric = "duplicate_permit_identifier_rows_after_harmonization", value = as.character(duplicate_permit_identifier_rows), note = "Duplicate canonical permit identifiers should be zero."),
  tibble(metric = "missing_issuance_date_rows", value = as.character(sum(harmonized_df$issuance_date_missing_flag, na.rm = TRUE)), note = "Current-source rows retained without issuance dates."),
  tibble(metric = "missing_issuance_date_share", value = as.character(mean(harmonized_df$issuance_date_missing_flag, na.rm = TRUE)), note = "Share of unified rows retained without issuance dates."),
  tibble(metric = "record_year_missing_when_issuance_missing", value = as.character(all(is.na(harmonized_df$record_year[harmonized_df$issuance_date_missing_flag]))), note = "Rows missing issuance date should also have missing record year."),
  tibble(metric = "current_ge_historical_all_years_1989_2013", value = as.character(all(comparison_check_df$current_ge_historical_row_count_flag, na.rm = TRUE)), note = "Current source row counts are at least as large as historical in every year from 1989 through 2013."),
  tibble(metric = "comparison_years_checked", value = as.character(nrow(comparison_check_df)), note = "Number of audit years checked before writing the unified dataset."),
  tibble(metric = "bps_descriptive_overlap_years", value = as.character(nrow(bps_compare_df)), note = "Descriptive only: permit-row counts are not unit counts and should not be interpreted as a BPS replacement.")
)

write_csv(qc_df, out_qc_csv, na = "")
cat("Wrote current-primary DOB permit issuance outputs to", dirname(out_qc_csv), "\n")
