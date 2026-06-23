# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/build_ulurp_modification_manual_review_queue/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

to_logical_flag <- function(x) {
  str_to_upper(str_squish(as.character(x))) %in% c("TRUE", "T", "1", "YES")
}

assert_unique_keys <- function(df, key_cols, df_name) {
  duplicate_keys <- df |>
    count(across(all_of(key_cols)), name = "source_row_count") |>
    filter(source_row_count > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(df_name, " is not unique by ", paste(key_cols, collapse = ", "), ".")
  }
}

spine <- read_csv("../input/ulurp_modification_project_spine.csv", show_col_types = FALSE, na = c("", "NA"))
discrete_modifications <- read_csv("../input/ulurp_modification_discrete_modifications.csv", show_col_types = FALSE, na = c("", "NA"))

known_case_patterns <- tribble(
  ~validation_case, ~pattern,
  "Dock Street", "DOCK STREET",
  "NY Blood Center", "BLOOD CENTER",
  "Industry City", "INDUSTRY CITY",
  "80 Flatbush", "80 FLATBUSH",
  "Innovation QNS", "INNOVATION QNS|INNOVATION QUEENS",
  "One45", "ONE45|ONE 45",
  "Bruckner Boulevard", "BRUCKNER",
  "East New York", "EAST NEW YORK",
  "Inwood", "INWOOD",
  "Haven Green", "HAVEN GREEN",
  "Greenpoint-Williamsburg", "GREENPOINT|WILLIAMSBURG",
  "Hudson Yards", "HUDSON YARDS"
)

known_case_source <- spine |>
  mutate(search_text = str_to_upper(str_squish(paste(project_name, project_brief, council_titles)))) |>
  select(project_id, project_name, search_text)

known_case_queue <- tidyr::expand_grid(known_case_source, known_case_patterns) |>
  filter(str_detect(search_text, regex(pattern, ignore_case = TRUE))) |>
  transmute(
    project_id,
    project_name,
    queue_reason = paste0("validation_case_", validation_case),
    source_gap_flag = FALSE,
    confidence = "manual_review_required",
    source_doc = "ulurp_modification_project_spine.csv",
    page = "NA_not_stated",
    snippet = str_squish(str_sub(search_text, 1, 500))
  )

source_gap_queue <- discrete_modifications |>
  filter(to_logical_flag(source_gap_flag)) |>
  transmute(
    project_id,
    project_name,
    queue_reason = "approve_with_mods_without_extracted_council_stage_modification",
    source_gap_flag = TRUE,
    confidence = "low",
    source_doc,
    page,
    snippet
  )

low_confidence_queue <- discrete_modifications |>
  filter(confidence == "low") |>
  transmute(
    project_id,
    project_name,
    queue_reason = paste0("low_confidence_", modification_category_code),
    source_gap_flag = to_logical_flag(source_gap_flag),
    confidence,
    source_doc,
    page,
    snippet
  )

manual_review_queue <- bind_rows(known_case_queue, source_gap_queue, low_confidence_queue) |>
  distinct() |>
  arrange(project_id, queue_reason, source_doc, snippet) |>
  group_by(project_id) |>
  mutate(manual_review_id = sprintf("%s_REVIEW_%03d", project_id, row_number())) |>
  ungroup() |>
  select(manual_review_id, everything())

assert_unique_keys(manual_review_queue, "manual_review_id", "ULURP modification manual review queue")

if (nrow(manual_review_queue) == 0) {
  stop("ULURP modification manual review queue is empty.")
}

write_csv_if_changed(manual_review_queue, "../output/ulurp_modification_manual_review_queue.csv")
