# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_ulurp_cpc_report_c_prefix_missing/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

count_duplicates <- function(x) {
  sum(duplicated(x) | duplicated(x, fromLast = TRUE), na.rm = TRUE)
}

label_missing <- function(x) {
  out <- str_squish(as.character(x))
  out[is.na(out) | out == ""] <- "missing"
  out
}

read_sibling_text <- function(text_paths) {
  if (is.na(text_paths) || text_paths == "") {
    return("")
  }

  paths <- str_split(text_paths, "; ")[[1]]
  full_paths <- file.path("../../build_ulurp_cpc_report_corpus/code", paths)
  missing_paths <- full_paths[!file.exists(full_paths)]

  if (length(missing_paths) > 0) {
    stop("Missing sibling text path: ", paste(missing_paths, collapse = "; "))
  }

  paste(
    vapply(full_paths, function(path) paste(readLines(path, warn = FALSE), collapse = "\n"), character(1)),
    collapse = "\n"
  )
}

manifest <- read_csv("../input/ulurp_cpc_report_manifest.csv", show_col_types = FALSE)
failure_recheck <- read_csv("../input/ulurp_cpc_report_failure_recheck.csv", show_col_types = FALSE)

if (count_duplicates(manifest$document_id) > 0) {
  stop("ulurp_cpc_report_manifest.csv has duplicate document_id values.")
}

if (count_duplicates(failure_recheck$document_id) > 0) {
  stop("ulurp_cpc_report_failure_recheck.csv has duplicate document_id values.")
}

project_report_coverage <- manifest |>
  mutate(has_text = text_status == "text_extracted") |>
  group_by(project_id) |>
  summarise(
    project_has_any_text = any(has_text, na.rm = TRUE),
    project_text_row_count = sum(has_text, na.rm = TRUE),
    project_manifest_row_count = n(),
    .groups = "drop"
  )

sibling_report_summary <- manifest |>
  filter(text_status == "text_extracted") |>
  group_by(project_id) |>
  summarise(
    sibling_report_applications = paste(raw_application_number, collapse = "; "),
    sibling_report_action_codes = paste(parsed_action_code, collapse = "; "),
    sibling_report_urls = paste(source_doc, collapse = "; "),
    sibling_report_text_paths = paste(local_text_path, collapse = "; "),
    .groups = "drop"
  )

c_prefix_missing <- manifest |>
  left_join(
    failure_recheck |>
      select(
        document_id,
        api_fetch_status,
        api_fetch_error,
        zap_http_status,
        zap_pdf_found,
        zap_action_cpc_url,
        final_verification_status
      ),
    by = "document_id",
    relationship = "one-to-one"
  ) |>
  left_join(project_report_coverage, by = "project_id", relationship = "many-to-one") |>
  left_join(sibling_report_summary, by = "project_id", relationship = "many-to-one") |>
  filter(
    str_extract(str_to_upper(raw_application_number), "^[A-Z]") == "C",
    text_status != "text_extracted" | is.na(text_status)
  ) |>
  mutate(
    official_cpc_report_url = paste0("https://www.nyc.gov/assets/planning/download/pdf/about/cpc/", base_report_stem, ".pdf"),
    zap_project_url = project_page_url,
    raw_application_prefix = str_extract(str_to_upper(raw_application_number), "^[A-Z]"),
    reference_decade = paste0(floor(corpus_reference_year / 10) * 10, "s"),
    parsed_action_code = label_missing(parsed_action_code),
    borough_name = label_missing(borough_name),
    applicant_type = label_missing(applicant_type),
    project_report_coverage = case_when(
      project_has_any_text ~ "missing_row_project_has_other_text",
      TRUE ~ "missing_row_project_has_no_text"
    ),
    missing_reason_bucket = case_when(
      project_has_any_text ~ "sibling_action_has_cpc_text",
      corpus_reference_year <= 1990 ~ "legacy_c_prefix_missing_from_public_routes",
      corpus_reference_year <= 2000 ~ "older_c_prefix_missing_from_public_routes",
      TRUE ~ "modern_c_prefix_missing_from_public_routes"
    )
  ) |>
  transmute(
    document_id,
    project_id,
    corpus_reference_year,
    reference_decade,
    raw_application_number,
    raw_application_prefix,
    parsed_action_code,
    parsed_borough_code,
    borough_name,
    project_name,
    applicant_type,
    primary_applicant,
    community_district,
    ceqr_number,
    actions,
    base_report_stem,
    candidate_report_stems,
    official_cpc_report_url,
    zap_project_url,
    project_report_coverage,
    project_manifest_row_count,
    project_text_row_count,
    sibling_report_applications,
    sibling_report_action_codes,
    sibling_report_urls,
    sibling_report_text_paths,
    missing_reason_bucket,
    download_status,
    download_error,
    zap_action_lookup_status,
    zap_action_lookup_error,
    api_fetch_status,
    zap_http_status,
    zap_pdf_found,
    zap_action_cpc_url,
    final_verification_status
  ) |>
  arrange(corpus_reference_year, raw_application_number, project_id)

c_prefix_missing_sibling_reports <- c_prefix_missing |>
  filter(project_report_coverage == "missing_row_project_has_other_text") |>
  rowwise() |>
  mutate(
    sibling_report_text_combined = str_to_upper(read_sibling_text(sibling_report_text_paths)),
    missing_application_stem = str_extract(raw_application_number, "[0-9]{6}"),
    sibling_text_mentions_missing_application = str_detect(sibling_report_text_combined, str_to_upper(raw_application_number)),
    sibling_text_mentions_missing_stem = str_detect(sibling_report_text_combined, missing_application_stem)
  ) |>
  ungroup() |>
  select(
    project_id,
    project_name,
    corpus_reference_year,
    raw_application_number,
    parsed_action_code,
    borough_name,
    applicant_type,
    zap_project_url,
    official_cpc_report_url,
    sibling_report_applications,
    sibling_report_action_codes,
    sibling_report_urls,
    sibling_report_text_paths,
    sibling_text_mentions_missing_application,
    sibling_text_mentions_missing_stem
  ) |>
  arrange(corpus_reference_year, raw_application_number, project_id)

summary_rows <- bind_rows(
  c_prefix_missing |>
    count(summary_dimension = "reference_decade", value = reference_decade, name = "missing_row_count"),
  c_prefix_missing |>
    count(summary_dimension = "corpus_reference_year", value = as.character(corpus_reference_year), name = "missing_row_count"),
  c_prefix_missing |>
    count(summary_dimension = "parsed_action_code", value = parsed_action_code, name = "missing_row_count"),
  c_prefix_missing |>
    count(summary_dimension = "borough_name", value = borough_name, name = "missing_row_count"),
  c_prefix_missing |>
    count(summary_dimension = "applicant_type", value = applicant_type, name = "missing_row_count"),
  c_prefix_missing |>
    count(summary_dimension = "project_report_coverage", value = project_report_coverage, name = "missing_row_count"),
  c_prefix_missing |>
    count(summary_dimension = "missing_reason_bucket", value = missing_reason_bucket, name = "missing_row_count"),
  c_prefix_missing |>
    count(summary_dimension = "final_verification_status", value = final_verification_status, name = "missing_row_count")
) |>
  group_by(summary_dimension) |>
  mutate(
    share_within_dimension = missing_row_count / sum(missing_row_count)
  ) |>
  ungroup() |>
  arrange(summary_dimension, desc(missing_row_count), value)

qc_rows <- tibble(
  metric = c(
    "c_prefix_missing_row_count",
    "c_prefix_missing_confirmed_missing_count",
    "c_prefix_missing_unverified_count",
    "c_prefix_missing_project_count",
    "c_prefix_missing_rows_with_sibling_project_text",
    "c_prefix_missing_rows_without_project_text",
    "c_prefix_missing_pre_1991_count",
    "c_prefix_missing_post_2000_count",
    "sibling_report_rows_with_missing_application_mention",
    "sibling_report_rows_with_missing_stem_mention"
  ),
  value = c(
    nrow(c_prefix_missing),
    sum(c_prefix_missing$final_verification_status == "confirmed_missing_from_checked_cpc_urls", na.rm = TRUE),
    sum(c_prefix_missing$final_verification_status != "confirmed_missing_from_checked_cpc_urls", na.rm = TRUE),
    n_distinct(c_prefix_missing$project_id),
    sum(c_prefix_missing$project_report_coverage == "missing_row_project_has_other_text"),
    sum(c_prefix_missing$project_report_coverage == "missing_row_project_has_no_text"),
    sum(c_prefix_missing$corpus_reference_year <= 1990, na.rm = TRUE),
    sum(c_prefix_missing$corpus_reference_year > 2000, na.rm = TRUE),
    sum(c_prefix_missing_sibling_reports$sibling_text_mentions_missing_application, na.rm = TRUE),
    sum(c_prefix_missing_sibling_reports$sibling_text_mentions_missing_stem, na.rm = TRUE)
  ),
  status = c(
    if_else(nrow(c_prefix_missing) == 164L, "pass", "warning"),
    if_else(sum(c_prefix_missing$final_verification_status == "confirmed_missing_from_checked_cpc_urls", na.rm = TRUE) == nrow(c_prefix_missing), "pass", "warning"),
    if_else(sum(c_prefix_missing$final_verification_status != "confirmed_missing_from_checked_cpc_urls", na.rm = TRUE) == 0, "pass", "warning"),
    "pass",
    "pass",
    "pass",
    "pass",
    "pass",
    "pass",
    if_else(
      sum(c_prefix_missing_sibling_reports$sibling_text_mentions_missing_stem, na.rm = TRUE) == nrow(c_prefix_missing_sibling_reports),
      "pass",
      "warning"
    )
  ),
  note = c(
    "C-prefixed manifest rows with no extracted CPC report text.",
    "C-prefixed missing rows independently confirmed missing from checked public CPC report routes.",
    "C-prefixed missing rows with any unresolved or recoverable verifier status.",
    "Distinct projects represented among C-prefixed missing rows.",
    "Missing C-prefixed rows whose project has at least one sibling row with extracted CPC report text.",
    "Missing C-prefixed rows whose project has no extracted CPC report text.",
    "Residual C-prefixed missing rows with reference year 1990 or earlier.",
    "Residual C-prefixed missing rows with reference year after 2000.",
    "Sibling-report cases where the sibling report text includes the full missing raw application number.",
    "Sibling-report cases where the sibling report text includes the missing application's six-digit stem."
  )
)

write_csv_if_changed(c_prefix_missing, "../output/ulurp_cpc_report_c_prefix_missing_cases.csv")
write_csv_if_changed(c_prefix_missing_sibling_reports, "../output/ulurp_cpc_report_c_prefix_missing_sibling_reports.csv")
write_csv_if_changed(summary_rows, "../output/ulurp_cpc_report_c_prefix_missing_summary.csv")
write_csv_if_changed(qc_rows, "../output/ulurp_cpc_report_c_prefix_missing_qc.csv")
