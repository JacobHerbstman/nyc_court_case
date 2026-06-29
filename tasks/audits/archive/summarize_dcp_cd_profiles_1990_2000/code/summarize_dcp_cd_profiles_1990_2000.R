# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_dcp_cd_profiles_1990_2000/code")
# dcp_cd_profiles_files_csv <- "../input/dcp_cd_profiles_1990_2000_files.csv"
# dcp_cd_profiles_qc_csv <- "../input/dcp_cd_profiles_1990_2000_qc.csv"
# out_summary_csv <- "../output/dcp_cd_profiles_1990_2000_audit_summary.csv"
# out_coverage_csv <- "../output/dcp_cd_profiles_1990_2000_parse_coverage.csv"
# out_unresolved_csv <- "../output/dcp_cd_profiles_1990_2000_unresolved_review.csv"
# out_known_checks_csv <- "../output/dcp_cd_profiles_1990_2000_known_value_checks.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 6) {
  stop("Expected 6 arguments: dcp_cd_profiles_files_csv dcp_cd_profiles_qc_csv out_summary_csv out_coverage_csv out_unresolved_csv out_known_checks_csv")
}

dcp_cd_profiles_files_csv <- args[1]
dcp_cd_profiles_qc_csv <- args[2]
out_summary_csv <- args[3]
out_coverage_csv <- args[4]
out_unresolved_csv <- args[5]
out_known_checks_csv <- args[6]

expected_page_types <- c(
  "social",
  "social_education",
  "labor_employment",
  "labor_income",
  "income_poverty",
  "housing",
  "housing_economic"
)

expected_borough_counts <- tibble(
  borough_name = c("Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"),
  expected_district_count = c(12L, 18L, 12L, 14L, 3L)
)

known_value_targets <- tribble(
  ~district_id, ~metric_label, ~expected_1990_number,
  "201", "Occupied housing units", 24645,
  "201", "Owner-occupied housing units", 651,
  "101", "Occupied housing units", 11576,
  "101", "Owner-occupied housing units", 3144,
  "401", "Occupied housing units", 71899,
  "401", "Owner-occupied housing units", 14404,
  "501", "Occupied housing units", 49951,
  "501", "Owner-occupied housing units", 26226
)

stage_files <- read_csv(dcp_cd_profiles_files_csv, show_col_types = FALSE, na = c("", "NA"))
stage_qc <- read_csv(dcp_cd_profiles_qc_csv, show_col_types = FALSE, na = c("", "NA"))

stage_files <- stage_files[
  !is.na(stage_files$parquet_path) & file.exists(stage_files$parquet_path) &
    !is.na(stage_files$page_index_csv_path) & file.exists(stage_files$page_index_csv_path),
]

if (nrow(stage_files) == 0) {
  write_csv(tibble(status = "no_staged_profile_files"), out_summary_csv, na = "")
  write_csv(tibble(), out_coverage_csv, na = "")
  write_csv(tibble(), out_unresolved_csv, na = "")
  write_csv(tibble(), out_known_checks_csv, na = "")
  quit(save = "no")
}

staged_df <- bind_rows(lapply(stage_files$parquet_path, function(path) {
  read_parquet(path) %>%
    as.data.frame() %>%
    as_tibble() %>%
    mutate(district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"))
}))

page_index_df <- bind_rows(lapply(stage_files$page_index_csv_path, function(path) {
  read_csv(path, show_col_types = FALSE, na = c("", "NA")) %>%
    mutate(district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"))
}))

unresolved_df <- bind_rows(lapply(stage_files$unresolved_csv_path[!is.na(stage_files$unresolved_csv_path) & file.exists(stage_files$unresolved_csv_path)], function(path) {
  read_csv(path, show_col_types = FALSE, na = c("", "NA")) %>%
    mutate(district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"))
}))

coverage_df <- page_index_df %>%
  group_by(pull_date, borough_name, district_id, profile_page_type) %>%
  summarise(
    page_count = n(),
    section_count = max(section_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(
    staged_df %>%
      group_by(pull_date, borough_name, district_id, profile_page_type) %>%
      summarise(
        metric_count = n(),
        parsed_with_footnotes_count = sum(!is.na(footnote_markers)),
        .groups = "drop"
      ),
    by = c("pull_date", "borough_name", "district_id", "profile_page_type")
  ) %>%
  mutate(
    metric_count = coalesce(metric_count, 0L),
    parsed_with_footnotes_count = coalesce(parsed_with_footnotes_count, 0L)
  ) %>%
  arrange(borough_name, district_id, profile_page_type)

borough_counts <- page_index_df %>%
  distinct(borough_name, district_id) %>%
  count(borough_name, name = "district_count") %>%
  right_join(expected_borough_counts, by = "borough_name") %>%
  mutate(
    district_count = coalesce(district_count, 0L),
    count_check = district_count == expected_district_count
  )

district_page_type_check <- page_index_df %>%
  distinct(district_id, profile_page_type) %>%
  count(district_id, name = "page_type_count") %>%
  mutate(page_type_check = page_type_count == length(expected_page_types))

housing_section_check <- staged_df %>%
  filter(profile_page_type == "housing") %>%
  distinct(section_name) %>%
  summarise(
    has_housing_occupancy = any(section_name == "housing_occupancy"),
    has_housing_tenure = any(section_name == "housing_tenure"),
    has_units_in_structure = any(section_name == "units_in_structure")
  )

known_value_checks <- known_value_targets %>%
  left_join(
    staged_df %>%
      filter(section_name == "housing_tenure") %>%
      select(district_id, metric_label, value_1990_number, pdf_page_number),
    by = c("district_id", "metric_label")
  ) %>%
  mutate(
    check_pass = !is.na(value_1990_number) & value_1990_number == expected_1990_number
  ) %>%
  arrange(district_id, metric_label)

summary_df <- bind_rows(
  tibble(metric = "unique_district_count", value = n_distinct(page_index_df$district_id), note = "Distinct community districts parsed from the raw page index."),
  tibble(metric = "parsed_page_count", value = nrow(page_index_df), note = "Parsed profile pages with a recognized CD title."),
  tibble(metric = "expected_page_count", value = 59 * 7, note = "Expected page count for 59 community districts and 7 profile pages each."),
  tibble(metric = "unique_page_type_count", value = n_distinct(page_index_df$profile_page_type), note = "Distinct parsed profile page types."),
  tibble(metric = "districts_with_seven_page_types", value = sum(district_page_type_check$page_type_check), note = "Districts with all seven expected profile page types."),
  tibble(metric = "borough_count_checks_passed", value = sum(borough_counts$count_check), note = "Boroughs matching the expected district counts 12, 18, 12, 14, and 3."),
  tibble(metric = "housing_sections_present", value = sum(unlist(housing_section_check)), note = "Count of required housing sections present: housing occupancy, housing tenure, and units in structure."),
  tibble(metric = "parsed_metric_count", value = nrow(staged_df), note = "Parsed metric rows in the staged long table."),
  tibble(metric = "unresolved_row_count", value = nrow(unresolved_df), note = "Rows routed to unresolved review rather than silently dropped."),
  tibble(metric = "known_value_checks_passed", value = sum(known_value_checks$check_pass, na.rm = TRUE), note = "Hand-checked 1990 housing tenure values that match the official PDF source.")
)

write_csv(summary_df, out_summary_csv, na = "")
write_csv(coverage_df, out_coverage_csv, na = "")
write_csv(unresolved_df, out_unresolved_csv, na = "")
write_csv(known_value_checks, out_known_checks_csv, na = "")

cat("Wrote DCP CD profile summary outputs to", dirname(out_summary_csv), "\n")
