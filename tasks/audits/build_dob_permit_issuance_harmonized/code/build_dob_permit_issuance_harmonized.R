suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")
source("../../../_lib/dob_permit_issuance_utils.R")

current_index <- read_csv("../output/dob_permit_issuance_current_raw_files.csv", show_col_types = FALSE, na = c("", "NA"))
comparison_audit <- read_csv("current_source_decision_1989_2013.csv", show_col_types = FALSE, na = c("", "NA"))

if (
  nrow(current_index) != 1 ||
    n_distinct(current_index$dataset_id) != 1 ||
    n_distinct(current_index$pull_date) != 1 ||
    n_distinct(current_index$raw_path) != 1 ||
    n_distinct(current_index$raw_parquet_path) != 1
) {
  stop("Current permit issuance raw index must describe exactly one source snapshot.")
}

required_comparison_columns <- c(
  "record_year",
  "current_row_count",
  "current_nb_row_count",
  "current_residential_row_count",
  "historical_row_count",
  "historical_nb_row_count",
  "historical_residential_row_count",
  "current_ge_historical_row_count_flag",
  "current_ge_historical_nb_row_count_flag"
)
missing_comparison_columns <- setdiff(required_comparison_columns, names(comparison_audit))

if (length(missing_comparison_columns) > 0) {
  stop("Current-source comparison audit is missing required columns: ", paste(missing_comparison_columns, collapse = ", "))
}

comparison_duplicate_years <- comparison_audit %>%
  count(record_year, name = "source_row_count") %>%
  filter(source_row_count != 1)

if (nrow(comparison_duplicate_years) > 0) {
  stop("Current-source comparison audit must have exactly one row per record_year.")
}

required_years <- tibble(record_year = 1989:2013)

comparison_check_df <- required_years %>%
  left_join(comparison_audit, by = "record_year", relationship = "one-to-one") %>%
  mutate(
    current_ge_historical_residential_row_count_flag = current_residential_row_count >= historical_residential_row_count
  )

if (any(is.na(comparison_check_df$current_row_count)) || any(is.na(comparison_check_df$historical_row_count))) {
  stop("Comparison audit must include current and historical row counts for every year from 1989 through 2013.")
}

if (any(!comparison_check_df$current_ge_historical_row_count_flag, na.rm = TRUE)) {
  stop("Current permit issuance source does not dominate historical row coverage in every year from 1989 through 2013.")
}

if (any(!comparison_check_df$current_ge_historical_nb_row_count_flag, na.rm = TRUE)) {
  stop("Current permit issuance source does not dominate historical new-building row coverage in every year from 1989 through 2013.")
}

if (any(!comparison_check_df$current_ge_historical_residential_row_count_flag, na.rm = TRUE)) {
  stop("Current permit issuance source does not dominate historical residential row coverage in every year from 1989 through 2013.")
}

current_raw_df <- read_parquet("../output/dob_permit_issuance_current_raw.parquet") %>%
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

write_parquet_if_changed(harmonized_df, "../output/dob_permit_issuance_harmonized.parquet")

cat("Wrote current-primary DOB permit issuance output to ../output/dob_permit_issuance_harmonized.parquet\n")
