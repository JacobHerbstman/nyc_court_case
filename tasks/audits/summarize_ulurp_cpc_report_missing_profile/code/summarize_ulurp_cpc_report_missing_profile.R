# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/summarize_ulurp_cpc_report_missing_profile/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

label_missing <- function(x) {
  out <- str_squish(as.character(x))
  out[is.na(out) | out == ""] <- "missing"
  out
}

count_duplicates <- function(x) {
  sum(duplicated(x) | duplicated(x, fromLast = TRUE), na.rm = TRUE)
}

build_profile <- function(df, dimension_name) {
  total_missing <- sum(df$report_outcome == "verified_missing_checked_sources")
  total_success <- sum(df$report_outcome == "text_extracted")

  profile <- df |>
    count(
      dimension = dimension_name,
      value = .data[[dimension_name]],
      report_outcome,
      name = "row_count"
    ) |>
    mutate(value = as.character(value)) |>
    pivot_wider(
      names_from = report_outcome,
      values_from = row_count,
      values_fill = 0
    )

  for (outcome_column in c("text_extracted", "verified_missing_checked_sources", "other_unresolved")) {
    if (!outcome_column %in% names(profile)) {
      profile[[outcome_column]] <- 0L
    }
  }

  profile |>
    mutate(
      text_extracted = coalesce(text_extracted, 0L),
      verified_missing_checked_sources = coalesce(verified_missing_checked_sources, 0L),
      other_unresolved = coalesce(other_unresolved, 0L),
      total_rows = text_extracted + verified_missing_checked_sources + other_unresolved,
      missing_rate = verified_missing_checked_sources / total_rows,
      share_of_missing = verified_missing_checked_sources / total_missing,
      share_of_text_extracted = text_extracted / total_success,
      missing_overrepresentation_ratio = if_else(
        share_of_text_extracted > 0,
        share_of_missing / share_of_text_extracted,
        NA_real_
      )
    ) |>
    select(
      dimension,
      value,
      total_rows,
      text_extracted,
      verified_missing_checked_sources,
      other_unresolved,
      missing_rate,
      share_of_missing,
      share_of_text_extracted,
      missing_overrepresentation_ratio
    ) |>
    arrange(
      dimension,
      desc(verified_missing_checked_sources),
      desc(missing_rate),
      value
    )
}

manifest <- read_csv("../input/ulurp_cpc_report_manifest.csv", show_col_types = FALSE)
failure_recheck <- read_csv("../input/ulurp_cpc_report_failure_recheck.csv", show_col_types = FALSE)
project_spine <- read_csv("../input/ulurp_corpus_project_spine.csv", show_col_types = FALSE)

if (count_duplicates(manifest$document_id) > 0) {
  stop("ulurp_cpc_report_manifest.csv has duplicate document_id values.")
}

if (count_duplicates(failure_recheck$document_id) > 0) {
  stop("ulurp_cpc_report_failure_recheck.csv has duplicate document_id values.")
}

if (count_duplicates(project_spine$project_id) > 0) {
  stop("ulurp_corpus_project_spine.csv has duplicate project_id values.")
}

profile_source <- manifest |>
  left_join(
    failure_recheck |>
      select(
        document_id,
        api_fetch_status,
        api_fetch_error,
        zap_http_status,
        final_verification_status
      ),
    by = "document_id",
    relationship = "one-to-one"
  ) |>
  left_join(
    project_spine |>
      select(project_id, project_status, public_status),
    by = "project_id",
    relationship = "many-to-one"
  ) |>
  mutate(
    report_outcome = case_when(
      text_status == "text_extracted" ~ "text_extracted",
      final_verification_status == "confirmed_missing_from_checked_cpc_urls" ~ "verified_missing_checked_sources",
      TRUE ~ "other_unresolved"
    ),
    reference_decade = paste0(floor(corpus_reference_year / 10) * 10, "s"),
    raw_application_prefix = label_missing(str_extract(str_to_upper(raw_application_number), "^[A-Z]")),
    application_prefix = label_missing(application_prefix),
    parsed_action_code = label_missing(parsed_action_code),
    parsed_borough_code = label_missing(parsed_borough_code),
    borough_name = label_missing(borough_name),
    applicant_type = label_missing(applicant_type),
    project_status = label_missing(project_status),
    public_status = label_missing(public_status),
    download_error_type = case_when(
      report_outcome == "text_extracted" ~ "text_extracted",
      str_detect(download_error, "404") ~ "official_url_404",
      str_detect(download_error, "all_candidate_urls_failed") ~ "all_candidate_urls_failed",
      is.na(download_error) | download_error == "" ~ "missing_error_note",
      TRUE ~ "other_download_error"
    ),
    zap_verification_type = case_when(
      report_outcome == "text_extracted" ~ "text_extracted",
      final_verification_status == "confirmed_missing_from_checked_cpc_urls" ~ "confirmed_missing_from_checked_cpc_urls",
      is.na(final_verification_status) | final_verification_status == "" ~ "not_rechecked",
      TRUE ~ final_verification_status
    ),
    zap_api_status = case_when(
      report_outcome == "text_extracted" ~ "text_extracted",
      is.na(api_fetch_status) | api_fetch_status == "" ~ "missing_api_status",
      TRUE ~ api_fetch_status
    ),
    action_family = case_when(
      parsed_action_code %in% c("ZA", "ZC", "ZL", "ZM", "ZP", "ZR", "ZS") ~ "zoning",
      parsed_action_code == "MM" ~ "city_map_change",
      parsed_action_code %in% c("BF", "CM", "DM", "EB", "EC", "EE", "EF", "EM", "EN", "EU", "GF", "MA", "MC", "MD", "ME", "MF", "ML", "PE", "RA", "RC", "RS", "SG", "TC", "TL", "VT") ~ "franchise_concession_or_certification",
      parsed_action_code %in% c("DL", "LD", "PC", "PI", "PL", "PN", "PP", "PQ", "PS", "PX") ~ "property_disposition_or_site_selection",
      str_starts(parsed_action_code, "H") ~ "housing_or_urban_renewal",
      parsed_action_code == "missing" ~ "missing_action_code",
      TRUE ~ "other_or_unclear"
    ),
    project_name_pattern = case_when(
      str_detect(str_to_upper(project_name), "SIDEWALK|CAFE|REVOCABLE CONSENT") ~ "sidewalk_cafe_or_consent",
      str_detect(str_to_upper(project_name), "FRANCHISE|CONCESSION|NEWSSTAND|KIOSK|BUS SHELTER") ~ "franchise_or_concession",
      str_detect(str_to_upper(project_name), "UDAAP|URBAN RENEWAL|HOUSING|HPD") ~ "housing_or_urban_renewal",
      str_detect(str_to_upper(project_name), "DISPOSITION|ACQUISITION|CITY.?OWNED|SALE") ~ "public_property_disposition",
      str_detect(str_to_upper(project_name), "MAP|DEMAP|STREET|AVENUE|AVE|BOULEVARD|BLVD|PLACE|ROAD|PARK") ~ "map_street_or_public_space",
      TRUE ~ "other"
    )
  ) |>
  group_by(project_id) |>
  mutate(
    project_has_any_text = any(report_outcome == "text_extracted", na.rm = TRUE),
    project_report_coverage = case_when(
      report_outcome == "text_extracted" ~ "row_has_text",
      project_has_any_text ~ "missing_row_project_has_other_text",
      TRUE ~ "missing_row_project_has_no_text"
    )
  ) |>
  ungroup()

unverified_failures <- profile_source |>
  filter(report_outcome == "other_unresolved")

profile_dimensions <- bind_rows(
  build_profile(profile_source, "reference_decade"),
  build_profile(profile_source, "corpus_reference_year"),
  build_profile(profile_source, "raw_application_prefix"),
  build_profile(profile_source, "application_prefix"),
  build_profile(profile_source, "parsed_action_code"),
  build_profile(profile_source, "action_family"),
  build_profile(profile_source, "parsed_borough_code"),
  build_profile(profile_source, "borough_name"),
  build_profile(profile_source, "applicant_type"),
  build_profile(profile_source, "project_status"),
  build_profile(profile_source, "public_status"),
  build_profile(profile_source, "project_name_pattern"),
  build_profile(profile_source, "project_report_coverage"),
  build_profile(profile_source, "download_error_type"),
  build_profile(profile_source, "zap_verification_type"),
  build_profile(profile_source, "zap_api_status")
)

year_profile <- profile_source |>
  count(corpus_reference_year, report_outcome, name = "row_count") |>
  pivot_wider(
    names_from = report_outcome,
    values_from = row_count,
    values_fill = 0
  )

for (outcome_column in c("text_extracted", "verified_missing_checked_sources", "other_unresolved")) {
  if (!outcome_column %in% names(year_profile)) {
    year_profile[[outcome_column]] <- 0L
  }
}

year_profile <- year_profile |>
  mutate(
    text_extracted = coalesce(text_extracted, 0L),
    verified_missing_checked_sources = coalesce(verified_missing_checked_sources, 0L),
    other_unresolved = coalesce(other_unresolved, 0L),
    total_rows = text_extracted + verified_missing_checked_sources + other_unresolved,
    missing_rate = verified_missing_checked_sources / total_rows
  ) |>
  select(
    corpus_reference_year,
    total_rows,
    text_extracted,
    verified_missing_checked_sources,
    other_unresolved,
    missing_rate
  ) |>
  arrange(corpus_reference_year)

missing_examples <- profile_source |>
  filter(report_outcome == "verified_missing_checked_sources") |>
  transmute(
    project_id,
    document_id,
    corpus_reference_year,
    raw_application_number,
    raw_application_prefix,
    application_prefix,
    parsed_action_code,
    action_family,
    parsed_borough_code,
    borough_name,
    project_name,
    applicant_type,
    primary_applicant,
    project_status,
    public_status,
    project_report_coverage,
    project_name_pattern,
    base_report_stem,
    candidate_report_stems,
    download_error,
    zap_api_status = api_fetch_status,
    zap_http_status,
    final_verification_status,
    project_page_url
  ) |>
  arrange(corpus_reference_year, parsed_action_code, project_id, raw_application_number)

qc_rows <- tibble(
  metric = c(
    "manifest_row_count",
    "text_extracted_row_count",
    "verified_missing_checked_sources_row_count",
    "other_unresolved_row_count",
    "verified_missing_share",
    "manifest_project_count",
    "manifest_projects_with_text",
    "manifest_projects_without_text",
    "verified_missing_rows_in_projects_with_text",
    "verified_missing_rows_in_projects_without_text",
    "failure_recheck_row_count",
    "failure_recheck_confirmed_missing_row_count",
    "failure_recheck_recoverable_or_uncertain_row_count",
    "project_spine_row_count"
  ),
  value = c(
    nrow(manifest),
    sum(profile_source$report_outcome == "text_extracted"),
    sum(profile_source$report_outcome == "verified_missing_checked_sources"),
    nrow(unverified_failures),
    sum(profile_source$report_outcome == "verified_missing_checked_sources") / nrow(profile_source),
    n_distinct(profile_source$project_id),
    n_distinct(profile_source$project_id[profile_source$project_has_any_text]),
    n_distinct(profile_source$project_id[!profile_source$project_has_any_text]),
    sum(profile_source$report_outcome == "verified_missing_checked_sources" & profile_source$project_has_any_text),
    sum(profile_source$report_outcome == "verified_missing_checked_sources" & !profile_source$project_has_any_text),
    nrow(failure_recheck),
    sum(failure_recheck$final_verification_status == "confirmed_missing_from_checked_cpc_urls", na.rm = TRUE),
    sum(failure_recheck$final_verification_status != "confirmed_missing_from_checked_cpc_urls", na.rm = TRUE),
    nrow(project_spine)
  ),
  status = c(
    "pass",
    "pass",
    "pass",
    if_else(nrow(unverified_failures) == 0, "pass", "warning"),
    if_else(sum(profile_source$report_outcome == "verified_missing_checked_sources") / nrow(profile_source) <= 0.15, "pass", "warning"),
    "pass",
    "pass",
    if_else(n_distinct(profile_source$project_id[!profile_source$project_has_any_text]) / n_distinct(profile_source$project_id) <= 0.12, "pass", "warning"),
    "pass",
    "pass",
    "pass",
    if_else(
      sum(failure_recheck$final_verification_status == "confirmed_missing_from_checked_cpc_urls", na.rm = TRUE) == nrow(failure_recheck),
      "pass",
      "warning"
    ),
    if_else(
      sum(failure_recheck$final_verification_status != "confirmed_missing_from_checked_cpc_urls", na.rm = TRUE) == 0,
      "pass",
      "warning"
    ),
    "pass"
  ),
  note = c(
    "Application/action rows in the CPC report manifest.",
    "Rows with downloaded CPC report text.",
    "Rows independently verified missing from the checked public CPC report routes.",
    "Rows that are neither extracted text nor verified missing after recheck.",
    "Verified-missing rows divided by manifest rows.",
    "Distinct projects represented by parsed application/action rows in the manifest.",
    "Distinct manifest projects with at least one extracted CPC report text row.",
    "Distinct manifest projects with no extracted CPC report text rows.",
    "Verified-missing rows in projects where another application/action row has extracted CPC report text.",
    "Verified-missing rows in projects with no extracted CPC report text rows.",
    "Rows in the independent failure recheck table.",
    "Rows whose independent recheck status confirms no report in the checked official routes.",
    "Rows whose independent recheck found a recoverable or uncertain result.",
    "ULURP project rows in the project spine."
  )
)

write_csv_if_changed(profile_dimensions, "../output/ulurp_cpc_report_missing_profile_dimension.csv")
write_csv_if_changed(year_profile, "../output/ulurp_cpc_report_missing_profile_year.csv")
write_csv_if_changed(missing_examples, "../output/ulurp_cpc_report_missing_profile_examples.csv")
write_csv_if_changed(qc_rows, "../output/ulurp_cpc_report_missing_profile_qc.csv")
