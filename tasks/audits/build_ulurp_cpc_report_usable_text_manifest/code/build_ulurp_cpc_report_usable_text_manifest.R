# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/build_ulurp_cpc_report_usable_text_manifest/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

dir.create("../output/usable_cpc_report_text", recursive = TRUE, showWarnings = FALSE)

count_duplicates <- function(x) {
  sum(duplicated(x) | duplicated(x, fromLast = TRUE), na.rm = TRUE)
}

safe_filename_part <- function(x) {
  out <- str_replace_all(str_squish(as.character(x)), "[^A-Za-z0-9_.-]+", "_")
  out <- str_sub(out, 1, 80)
  out[out == "" | is.na(out)] <- "missing"
  out
}

clean_text_count <- function(x) {
  nchar(str_squish(as.character(x)))
}

write_text_if_changed <- function(text, out_path) {
  old_text <- if (file.exists(out_path)) {
    paste(readLines(out_path, warn = FALSE), collapse = "\n")
  } else {
    NA_character_
  }

  if (is.na(old_text) || !identical(old_text, text)) {
    writeLines(text, out_path, useBytes = TRUE)
  }
}

adjust_build_output_path <- function(path) {
  str_replace_all(path, "(^|; )\\.\\./output/", "\\1../../build_ulurp_cpc_report_corpus/output/")
}

read_build_text_file <- function(path) {
  full_path <- file.path("../../build_ulurp_cpc_report_corpus/code", path)
  if (!file.exists(full_path)) {
    stop("Missing source text file: ", full_path)
  }
  paste(readLines(full_path, warn = FALSE), collapse = "\n")
}

manifest <- read_csv("../input/ulurp_cpc_report_manifest.csv", show_col_types = FALSE)
c_prefix_sibling_reports <- read_csv("../input/ulurp_cpc_report_c_prefix_missing_sibling_reports.csv", show_col_types = FALSE)

if (count_duplicates(manifest$document_id) > 0) {
  stop("ulurp_cpc_report_manifest.csv has duplicate document_id values.")
}

if (count_duplicates(paste(c_prefix_sibling_reports$project_id, c_prefix_sibling_reports$raw_application_number, sep = "||")) > 0) {
  stop("ulurp_cpc_report_c_prefix_missing_sibling_reports.csv is not unique by project_id and raw_application_number.")
}

direct_text_rows <- manifest |>
  filter(text_status == "text_extracted") |>
  transmute(
    document_id,
    project_id,
    project_name,
    corpus_reference_year,
    corpus_reference_date,
    raw_application_number,
    parsed_action_code,
    parsed_borough_code,
    borough_name,
    applicant_type,
    primary_applicant,
    community_district,
    project_page_url,
    usable_text_source_type = "direct_cpc_report",
    usable_text_status = "text_extracted_direct",
    usable_local_text_path = adjust_build_output_path(local_text_path),
    usable_text_char_count = as.integer(text_char_count),
    source_report_application_numbers = raw_application_number,
    source_report_action_codes = parsed_action_code,
    source_report_urls = source_doc,
    source_report_text_paths = adjust_build_output_path(local_text_path),
    sibling_text_mentions_missing_application = NA,
    sibling_text_mentions_missing_stem = NA
  )

fallback_source <- c_prefix_sibling_reports |>
  filter(sibling_text_mentions_missing_stem == TRUE) |>
  left_join(
    manifest |>
      select(
        document_id,
        project_id,
        project_name,
        corpus_reference_year,
        corpus_reference_date,
        raw_application_number,
        parsed_action_code,
        parsed_borough_code,
        borough_name,
        applicant_type,
        primary_applicant,
        community_district,
        project_page_url
      ),
    by = c("project_id", "raw_application_number"),
    relationship = "one-to-one",
    suffix = c("_sibling_audit", "")
  )

if (any(is.na(fallback_source$document_id))) {
  stop("Could not match every sibling fallback row back to the raw CPC manifest.")
}

fallback_text_rows <- fallback_source |>
  rowwise() |>
  mutate(
    usable_local_text_path = paste0(
      "../output/usable_cpc_report_text/",
      safe_filename_part(project_id), "_",
      safe_filename_part(raw_application_number), "_sibling_project_cpc_report.txt"
    ),
    source_text_paths_relative_to_build_code = sibling_report_text_paths,
    source_text_paths = adjust_build_output_path(sibling_report_text_paths),
    combined_text = paste(
      paste0(
        "Sibling CPC report fallback for missing application ", raw_application_number,
        " in project ", project_id, ".\n",
        "Sibling report application(s): ", sibling_report_applications, "\n",
        "Sibling report URL(s): ", sibling_report_urls, "\n\n"
      ),
      paste(
        vapply(
          str_split(source_text_paths_relative_to_build_code, "; ")[[1]],
          read_build_text_file,
          character(1)
        ),
        collapse = "\n\n"
      ),
      sep = ""
    ),
    usable_text_char_count = clean_text_count(combined_text)
  ) |>
  ungroup()

for (row_index in seq_len(nrow(fallback_text_rows))) {
  write_text_if_changed(
    fallback_text_rows$combined_text[row_index],
    fallback_text_rows$usable_local_text_path[row_index]
  )
}

fallback_text_rows <- fallback_text_rows |>
  transmute(
    document_id,
    project_id,
    project_name,
    corpus_reference_year,
    corpus_reference_date,
    raw_application_number,
    parsed_action_code,
    parsed_borough_code,
    borough_name,
    applicant_type,
    primary_applicant,
    community_district,
    project_page_url,
    usable_text_source_type = "sibling_project_cpc_report",
    usable_text_status = "text_extracted_sibling_project_fallback",
    usable_local_text_path,
    usable_text_char_count = as.integer(usable_text_char_count),
    source_report_application_numbers = sibling_report_applications,
    source_report_action_codes = sibling_report_action_codes,
    source_report_urls = sibling_report_urls,
    source_report_text_paths = source_text_paths,
    sibling_text_mentions_missing_application,
    sibling_text_mentions_missing_stem
  )

usable_text_manifest <- bind_rows(direct_text_rows, fallback_text_rows) |>
  arrange(corpus_reference_year, project_id, raw_application_number, usable_text_source_type)

if (count_duplicates(usable_text_manifest$document_id) > 0) {
  stop("Usable CPC report text manifest has duplicate document_id values.")
}

qc_rows <- tibble(
  metric = c(
    "raw_manifest_row_count",
    "direct_text_row_count",
    "sibling_project_fallback_row_count",
    "usable_text_row_count",
    "remaining_without_usable_text_count",
    "fallback_rows_with_missing_stem_mention",
    "fallback_rows_with_missing_application_mention"
  ),
  value = c(
    nrow(manifest),
    nrow(direct_text_rows),
    nrow(fallback_text_rows),
    nrow(usable_text_manifest),
    nrow(manifest) - nrow(usable_text_manifest),
    sum(fallback_text_rows$sibling_text_mentions_missing_stem == TRUE, na.rm = TRUE),
    sum(fallback_text_rows$sibling_text_mentions_missing_application == TRUE, na.rm = TRUE)
  ),
  status = c(
    "pass",
    if_else(nrow(direct_text_rows) > 0, "pass", "fail"),
    if_else(nrow(fallback_text_rows) == 17L, "pass", "warning"),
    if_else(nrow(usable_text_manifest) == nrow(direct_text_rows) + nrow(fallback_text_rows), "pass", "fail"),
    "pass",
    if_else(sum(fallback_text_rows$sibling_text_mentions_missing_stem == TRUE, na.rm = TRUE) == nrow(fallback_text_rows), "pass", "warning"),
    "pass"
  ),
  note = c(
    "Application/action rows in the raw CPC report manifest.",
    "Rows with directly extracted row-level CPC report text.",
    "Rows included through sibling project CPC report text.",
    "Rows with text usable for corpus analysis after sibling fallback.",
    "Raw manifest rows that still have no usable CPC report text.",
    "Sibling fallback rows where the sibling text includes the missing application's six-digit stem.",
    "Sibling fallback rows where the sibling text includes the full missing raw application number."
  )
)

write_csv_if_changed(usable_text_manifest, "../output/ulurp_cpc_report_usable_text_manifest.csv")
write_csv_if_changed(qc_rows, "../output/ulurp_cpc_report_usable_text_qc.csv")
