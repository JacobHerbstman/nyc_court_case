# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_ulurp_modification_content/code")
# summary_mode <- "first_pass"
# validation_case_set <- "known_cases"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

cli_args <- commandArgs(trailingOnly = TRUE)
if (length(cli_args) != 2) {
  stop("Usage: Rscript summarize_ulurp_modification_content.R <summary_mode> <validation_case_set>")
}

summary_mode <- as.character(cli_args[1])
validation_case_set <- as.character(cli_args[2])

if (!summary_mode %in% c("first_pass")) {
  stop("Unsupported summary_mode: ", summary_mode)
}
if (!validation_case_set %in% c("known_cases")) {
  stop("Unsupported validation_case_set: ", validation_case_set)
}

collapse_values <- function(x) {
  values <- unique(str_squish(as.character(x)))
  values <- values[!is.na(values) & values != ""]
  if (length(values) == 0) {
    return(NA_character_)
  }

  paste(values, collapse = "; ")
}

safe_max_numeric <- function(x) {
  values <- suppressWarnings(as.numeric(x))
  values <- values[!is.na(values)]
  if (length(values) == 0) {
    return(NA_real_)
  }

  max(values)
}

to_logical_flag <- function(x) {
  str_to_upper(str_squish(as.character(x))) %in% c("TRUE", "T", "1", "YES")
}

numeric_column <- function(df, col_name) {
  if (!col_name %in% names(df)) {
    return(rep(NA_real_, nrow(df)))
  }

  suppressWarnings(as.numeric(df[[col_name]]))
}

spine <- read_csv("../input/ulurp_modification_project_spine.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    project_id = as.character(project_id),
    cert_year = suppressWarnings(as.integer(cert_year)),
    council_modification_signal = to_logical_flag(council_modification_signal),
    linked_gross_add_units_0_10 = suppressWarnings(as.numeric(linked_gross_add_units_0_10))
  )

project_versions <- read_csv("../input/ulurp_modification_project_versions.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    project_id = as.character(project_id),
    quantity_value = suppressWarnings(as.numeric(quantity_value))
  )

discrete_modifications <- read_csv("../input/ulurp_modification_discrete_modifications.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    project_id = as.character(project_id),
    source_gap_flag = to_logical_flag(source_gap_flag)
  )

commitments <- read_csv("../input/ulurp_modification_commitments.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(project_id = as.character(project_id))

citywide_text <- read_csv("../input/ulurp_modification_citywide_text_district_modifications.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(project_id = as.character(project_id))

if (any(duplicated(spine$project_id))) {
  stop("Modification spine must be unique by project_id.")
}
if (any(duplicated(project_versions$project_version_id))) {
  stop("Project-version table must be unique by project_version_id.")
}
if (any(duplicated(discrete_modifications$modification_id))) {
  stop("Discrete modification table must be unique by modification_id.")
}
if (any(duplicated(commitments$commitment_id))) {
  stop("Commitment table must be unique by commitment_id.")
}
if (any(duplicated(citywide_text$citywide_text_modification_id))) {
  stop("Citywide text table must be unique by citywide_text_modification_id.")
}

quantity_by_project <- project_versions |>
  filter(quantity_missing_status %in% c("observed", "true_zero")) |>
  group_by(project_id, stage, quantity_field) |>
  summarise(
    quantity_value = safe_max_numeric(quantity_value),
    quantity_row_count = n(),
    quantity_sources = collapse_values(source_doc),
    quantity_confidence = collapse_values(confidence),
    .groups = "drop"
  ) |>
  mutate(quantity_name = paste(stage, quantity_field, sep = "__"))

quantity_wide <- quantity_by_project |>
  select(project_id, quantity_name, quantity_value) |>
  pivot_wider(names_from = quantity_name, values_from = quantity_value)

modifications_by_project <- discrete_modifications |>
  group_by(project_id) |>
  summarise(
    modification_row_count = n(),
    council_stage_modification_count = sum(modification_stage == "council_stage", na.rm = TRUE),
    source_gap_modification_count = sum(source_gap_flag, na.rm = TRUE),
    distinct_modification_category_count = n_distinct(modification_category_code),
    unit_or_bulk_modification_count = sum(str_detect(modification_category_code, "^Q-|^D-|^P-"), na.rm = TRUE),
    affordability_modification_count = sum(str_detect(modification_category_code, "^A-"), na.rm = TRUE),
    commitment_like_modification_count = sum(str_detect(modification_category_code, "^B-|^C-"), na.rm = TRUE),
    high_or_medium_confidence_modification_count = sum(confidence %in% c("high", "medium"), na.rm = TRUE),
    modification_categories = collapse_values(modification_category_code),
    .groups = "drop"
  )

commitments_by_project <- commitments |>
  group_by(project_id) |>
  summarise(
    commitment_row_count = n(),
    council_stage_commitment_count = sum(commitment_stage == "council_stage", na.rm = TRUE),
    high_or_medium_confidence_commitment_count = sum(confidence %in% c("high", "medium"), na.rm = TRUE),
    commitment_categories = collapse_values(commitment_category),
    .groups = "drop"
  )

citywide_by_project <- citywide_text |>
  group_by(project_id) |>
  summarise(
    citywide_text_district_row_count = n(),
    citywide_text_districts = collapse_values(affected_council_district),
    .groups = "drop"
  )

project_summary <- spine |>
  left_join(quantity_wide, by = "project_id", relationship = "one-to-one") |>
  left_join(modifications_by_project, by = "project_id", relationship = "one-to-one") |>
  left_join(commitments_by_project, by = "project_id", relationship = "one-to-one") |>
  left_join(citywide_by_project, by = "project_id", relationship = "one-to-one")

project_summary$certified_units_first_pass <- numeric_column(project_summary, "certified_project_brief_first_pass__units")
project_summary$cpc_units_first_pass <- numeric_column(project_summary, "cpc_docket_description_first_pass__units")
project_summary$adopted_units_first_pass <- numeric_column(project_summary, "council_adopted_first_pass__units")
project_summary$built_units_0_10 <- numeric_column(project_summary, "built_0_10__units_built_0_10")
project_summary$certified_affordable_units_first_pass <- numeric_column(project_summary, "certified_project_brief_first_pass__affordable_units")
project_summary$adopted_affordable_units_first_pass <- numeric_column(project_summary, "council_adopted_first_pass__affordable_units")

project_summary <- project_summary |>
  mutate(
    across(
      c(
        modification_row_count,
        council_stage_modification_count,
        source_gap_modification_count,
        distinct_modification_category_count,
        unit_or_bulk_modification_count,
        affordability_modification_count,
        commitment_like_modification_count,
        high_or_medium_confidence_modification_count,
        commitment_row_count,
        council_stage_commitment_count,
        high_or_medium_confidence_commitment_count,
        citywide_text_district_row_count
      ),
      ~ coalesce(as.numeric(.x), 0)
    ),
    effective_adopted_units_first_pass = coalesce(adopted_units_first_pass, cpc_units_first_pass, certified_units_first_pass),
    council_vs_cpc_unit_delta = if_else(
      !is.na(adopted_units_first_pass) & !is.na(cpc_units_first_pass),
      adopted_units_first_pass - cpc_units_first_pass,
      NA_real_
    ),
    council_vs_certified_unit_delta = if_else(
      !is.na(adopted_units_first_pass) & !is.na(certified_units_first_pass),
      adopted_units_first_pass - certified_units_first_pass,
      NA_real_
    ),
    quantity_restriction_intensity = if_else(
      !is.na(certified_units_first_pass) &
        certified_units_first_pass > 0 &
        !is.na(adopted_units_first_pass) &
        certified_units_first_pass > adopted_units_first_pass,
      (certified_units_first_pass - adopted_units_first_pass) / certified_units_first_pass,
      NA_real_
    ),
    affordability_extraction_delta = if_else(
      !is.na(adopted_affordable_units_first_pass) & !is.na(certified_affordable_units_first_pass),
      adopted_affordable_units_first_pass - certified_affordable_units_first_pass,
      NA_real_
    ),
    commitments_per_adopted_unit = if_else(
      !is.na(effective_adopted_units_first_pass) & effective_adopted_units_first_pass > 0,
      commitment_row_count / effective_adopted_units_first_pass,
      NA_real_
    ),
    buildout_slippage_units = if_else(
      !is.na(built_units_0_10) & !is.na(effective_adopted_units_first_pass),
      built_units_0_10 - effective_adopted_units_first_pass,
      NA_real_
    ),
    buildout_slippage_rate = if_else(
      !is.na(buildout_slippage_units) & !is.na(effective_adopted_units_first_pass) & effective_adopted_units_first_pass > 0,
      buildout_slippage_units / effective_adopted_units_first_pass,
      NA_real_
    ),
    council_stage_modification_any = council_stage_modification_count > 0,
    source_gap_modification_any = source_gap_modification_count > 0,
    local_member_objection_signal = str_detect(
      paste(local_member_vote_statuses, local_member_final_action_votes, sep = " "),
      regex("negative_or_abstain|Negative|Abstain", ignore_case = TRUE)
    ),
    deference_exception_signal = str_detect(
      member_deference_vote_signals,
      regex("over.*objection|negative|abstain|deference.*violation", ignore_case = TRUE)
    ),
    summary_mode = summary_mode,
    validation_case_set = validation_case_set
  ) |>
  select(
    project_id,
    project_name,
    cert_year,
    cert_era,
    borough_name,
    stratum,
    council_outcome,
    council_modification_signal,
    council_stage_modification_any,
    source_gap_modification_any,
    modification_row_count,
    council_stage_modification_count,
    source_gap_modification_count,
    distinct_modification_category_count,
    modification_categories,
    unit_or_bulk_modification_count,
    affordability_modification_count,
    commitment_like_modification_count,
    high_or_medium_confidence_modification_count,
    commitment_row_count,
    council_stage_commitment_count,
    high_or_medium_confidence_commitment_count,
    commitments_per_adopted_unit,
    citywide_text_district_row_count,
    citywide_text_districts,
    certified_units_first_pass,
    cpc_units_first_pass,
    adopted_units_first_pass,
    effective_adopted_units_first_pass,
    built_units_0_10,
    council_vs_cpc_unit_delta,
    council_vs_certified_unit_delta,
    quantity_restriction_intensity,
    certified_affordable_units_first_pass,
    adopted_affordable_units_first_pass,
    affordability_extraction_delta,
    buildout_slippage_units,
    buildout_slippage_rate,
    local_member_names,
    local_member_vote_statuses,
    member_deference_vote_signals,
    local_member_objection_signal,
    deference_exception_signal,
    summary_mode,
    validation_case_set
  )

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

delta_without_council_evidence <- project_summary |>
  filter(
    !is.na(council_vs_cpc_unit_delta),
    council_vs_cpc_unit_delta != 0,
    !council_stage_modification_any
  )

qc_rows <- tribble(
  ~check_name, ~check_value, ~status,
  "spine_project_rows", nrow(spine), if_else(nrow(spine) > 0, "pass", "fail"),
  "project_summary_rows", nrow(project_summary), if_else(nrow(project_summary) == nrow(spine), "pass", "fail"),
  "unique_project_summary_project_ids", n_distinct(project_summary$project_id), if_else(n_distinct(project_summary$project_id) == nrow(project_summary), "pass", "fail"),
  "category_summary_rows", nrow(category_summary), if_else(nrow(category_summary) > 0, "pass", "fail"),
  "stratum_year_summary_rows", nrow(stratum_year_summary), if_else(nrow(stratum_year_summary) > 0, "pass", "fail"),
  "unit_delta_without_council_stage_evidence", nrow(delta_without_council_evidence), "review",
  "source_gap_modification_project_count", sum(project_summary$source_gap_modification_any, na.rm = TRUE), "review"
)

write_csv_if_changed(project_summary, "../output/ulurp_modification_project_summary.csv")
write_csv_if_changed(category_summary, "../output/ulurp_modification_category_summary.csv")
write_csv_if_changed(stratum_year_summary, "../output/ulurp_modification_stratum_year_summary.csv")
write_csv_if_changed(qc_rows, "../output/ulurp_modification_summary_qc.csv")

if (any(qc_rows$status == "fail")) {
  stop("ULURP modification content summary QC failed.")
}
