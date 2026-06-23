# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/summarize_ulurp_modification_content_exploratory/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
})

source("../../../_lib/source_pipeline_utils.R")

spine <- read_csv("../input/ulurp_modification_project_spine.csv", show_col_types = FALSE, na = c("", "NA"))
project_summary <- read_csv("../input/ulurp_modification_project_summary.csv", show_col_types = FALSE, na = c("", "NA"))
discrete_modifications <- read_csv("../input/ulurp_modification_discrete_modifications.csv", show_col_types = FALSE, na = c("", "NA"))

category_summary <- discrete_modifications |>
  group_by(modification_stage, modification_category_code, confidence) |>
  summarise(
    modification_row_count = n(),
    project_count = n_distinct(project_id),
    source_gap_row_count = sum(source_gap_flag, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    modification_row_share = modification_row_count / sum(modification_row_count),
    project_share_among_spine = project_count / nrow(spine)
  ) |>
  arrange(modification_stage, modification_category_code, confidence)

stratum_year_summary <- project_summary |>
  group_by(cert_year, cert_era, stratum) |>
  summarise(
    project_count = n(),
    council_modification_project_count = sum(council_modification_signal, na.rm = TRUE),
    council_stage_modification_project_count = sum(council_stage_modification_any, na.rm = TRUE),
    source_gap_project_count = sum(source_gap_modification_any, na.rm = TRUE),
    local_member_objection_project_count = sum(local_member_objection_signal, na.rm = TRUE),
    deference_exception_project_count = sum(deference_exception_signal, na.rm = TRUE),
    mean_quantity_restriction_intensity = mean(quantity_restriction_intensity, na.rm = TRUE),
    mean_commitments_per_adopted_unit = mean(commitments_per_adopted_unit, na.rm = TRUE),
    mean_buildout_slippage_rate = mean(buildout_slippage_rate, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    mean_quantity_restriction_intensity = if_else(is.nan(mean_quantity_restriction_intensity), NA_real_, mean_quantity_restriction_intensity),
    mean_commitments_per_adopted_unit = if_else(is.nan(mean_commitments_per_adopted_unit), NA_real_, mean_commitments_per_adopted_unit),
    mean_buildout_slippage_rate = if_else(is.nan(mean_buildout_slippage_rate), NA_real_, mean_buildout_slippage_rate)
  ) |>
  arrange(cert_year, stratum)

if (nrow(category_summary) == 0 || nrow(stratum_year_summary) == 0) {
  stop("ULURP modification exploratory summaries are empty.")
}

write_csv_if_changed(category_summary, "../output/ulurp_modification_category_summary.csv")
write_csv_if_changed(stratum_year_summary, "../output/ulurp_modification_stratum_year_summary.csv")
