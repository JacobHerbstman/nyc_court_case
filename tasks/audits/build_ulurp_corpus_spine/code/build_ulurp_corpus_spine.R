# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/build_ulurp_corpus_spine/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

collapse_values <- function(x) {
  values <- unique(str_squish(as.character(x)))
  values <- values[!is.na(values) & values != ""]
  if (length(values) == 0) {
    return(NA_character_)
  }

  paste(values, collapse = "; ")
}

normalize_application_key <- function(x) {
  raw_value <- str_to_upper(str_replace_all(str_squish(as.character(x)), "[^A-Z0-9]", ""))
  raw_value[raw_value == ""] <- NA_character_
  str_replace(raw_value, "^[CNM](?=[0-9])", "")
}

extract_year <- function(x) {
  suppressWarnings(as.integer(format(as.Date(x), "%Y")))
}

zap_project <- read_parquet("../input/zap_project_data.parquet") |>
  as.data.frame() |>
  as_tibble() |>
  mutate(
    project_id = as.character(project_id),
    project_name = str_squish(as.character(project_name)),
    project_brief = str_squish(as.character(project_brief)),
    project_status = str_squish(as.character(project_status)),
    public_status = str_squish(as.character(public_status)),
    ulurp_group = str_squish(as.character(ulurp_group)),
    ulurp_numbers = str_squish(as.character(ulurp_numbers)),
    actions = str_squish(as.character(actions)),
    ceqr_number = str_squish(as.character(ceqr_number)),
    applicant_type = str_squish(as.character(applicant_type)),
    primary_applicant = str_squish(as.character(primary_applicant)),
    borough_name_standardized = str_squish(as.character(borough_name_standardized)),
    community_district_standardized = str_squish(as.character(community_district_standardized)),
    council_district_first = suppressWarnings(as.integer(council_district_first)),
    app_filed_date = as.Date(app_filed_date_parsed),
    noticed_date = as.Date(noticed_date_parsed),
    certified_referred_date = as.Date(certified_referred_date_parsed),
    approval_date = as.Date(approval_date_parsed),
    completed_date = as.Date(completed_date_parsed),
    project_reference_date = as.Date(project_reference_date),
    corpus_reference_date = coalesce(certified_referred_date, app_filed_date, noticed_date, approval_date, completed_date, project_reference_date),
    corpus_reference_year = extract_year(corpus_reference_date),
    certified_referred_year = extract_year(certified_referred_date),
    completed_year = extract_year(completed_date),
    has_ulurp_number = !is.na(ulurp_numbers) & ulurp_numbers != "",
    manual_sample_cohort = case_when(
      corpus_reference_year >= 1990L & corpus_reference_year <= 1999L ~ "1990s",
      corpus_reference_year >= 2000L & corpus_reference_year <= 2006L ~ "early_mid_2000s",
      corpus_reference_year >= 2015L ~ "2015_onward",
      TRUE ~ NA_character_
    )
  )

ulurp_project <- zap_project |>
  filter(ulurp_group == "ULURP", !is.na(corpus_reference_year), corpus_reference_year >= 1975L) |>
  arrange(corpus_reference_year, borough_name_standardized, project_id) |>
  transmute(
    project_id,
    project_name,
    project_brief,
    project_status,
    public_status,
    corpus_reference_date,
    corpus_reference_year,
    certified_referred_date,
    certified_referred_year,
    completed_date,
    completed_year,
    manual_sample_cohort,
    ulurp_numbers,
    has_ulurp_number,
    actions,
    ceqr_number,
    applicant_type,
    primary_applicant,
    borough_name = borough_name_standardized,
    community_district = community_district_standardized,
    council_district_first,
    project_page_url = paste0("https://zap.planning.nyc.gov/projects/", project_id),
    api_url = paste0("https://zap-api-production.herokuapp.com/projects/", project_id)
  )

application_rows <- ulurp_project |>
  select(project_id, corpus_reference_year, raw_ulurp_numbers = ulurp_numbers) |>
  mutate(raw_application_number = str_split(coalesce(raw_ulurp_numbers, ""), "\\s*;\\s*")) |>
  unnest(raw_application_number) |>
  mutate(
    raw_application_number = str_squish(str_to_upper(raw_application_number)),
    application_key = normalize_application_key(raw_application_number),
    application_prefix = str_extract(str_replace_all(raw_application_number, "\\s+", ""), "^[CNM]"),
    application_digits = str_extract(raw_application_number, "\\d{6,8}"),
    action_borough_code = str_extract(str_replace_all(raw_application_number, "\\s+", ""), "[A-Z]{2,4}[A-Z]?$"),
    a_application_flag = str_detect(raw_application_number, "\\(A\\)")
  ) |>
  filter(!is.na(application_digits), !is.na(application_key), application_key != "") |>
  distinct(project_id, application_key, raw_application_number, .keep_all = TRUE) |>
  arrange(corpus_reference_year, project_id, raw_application_number) |>
  select(
    project_id,
    corpus_reference_year,
    raw_application_number,
    application_key,
    application_prefix,
    application_digits,
    action_borough_code,
    a_application_flag
  )

qc_rows <- bind_rows(
  tibble(
    metric = "zap_project_row_count",
    value = nrow(zap_project),
    note = "Rows in the standardized ZAP project table."
  ),
  tibble(
    metric = "ulurp_project_row_count",
    value = nrow(ulurp_project),
    note = "ZAP project rows marked ULURP with reference year >= 1975."
  ),
  tibble(
    metric = "ulurp_application_row_count",
    value = nrow(application_rows),
    note = "Distinct parsed application-number rows from ULURP projects."
  ),
  tibble(
    metric = "ulurp_project_missing_number_count",
    value = sum(!ulurp_project$has_ulurp_number),
    note = "ULURP project rows without a populated ulurp_numbers field."
  ),
  tibble(
    metric = "ulurp_first_reference_year",
    value = min(ulurp_project$corpus_reference_year, na.rm = TRUE),
    note = "Earliest reference year in the ULURP project spine."
  ),
  tibble(
    metric = "ulurp_latest_reference_year",
    value = max(ulurp_project$corpus_reference_year, na.rm = TRUE),
    note = "Latest reference year in the ULURP project spine."
  ),
  ulurp_project |>
    count(manual_sample_cohort, name = "value") |>
    filter(!is.na(manual_sample_cohort)) |>
    transmute(
      metric = paste0("manual_sample_frame_project_count_", manual_sample_cohort),
      value,
      note = "ULURP project rows in a manual-reading sample cohort."
    ),
  ulurp_project |>
    mutate(reference_decade = paste0(floor(corpus_reference_year / 10) * 10, "s")) |>
    count(reference_decade, name = "value") |>
    arrange(reference_decade) |>
    transmute(
      metric = paste0("ulurp_project_count_", reference_decade),
      value,
      note = "ULURP project rows by reference decade."
    )
)

write_csv_if_changed(ulurp_project, "../output/ulurp_corpus_project_spine.csv")
write_csv_if_changed(application_rows, "../output/ulurp_corpus_application_spine.csv")
write_csv_if_changed(qc_rows, "../output/ulurp_corpus_spine_qc.csv")

cat("Wrote ULURP corpus spine outputs to ../output\n")
