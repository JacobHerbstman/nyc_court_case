# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_ulurp_corpus_spine/code")
# start_year <- 1975L
# end_year <- 2025L

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

cli_args <- commandArgs(trailingOnly = TRUE)
if (interactive()) {
  cli_args <- c(start_year, end_year)
}
if (length(cli_args) != 2) {
  stop("Usage: Rscript build_ulurp_corpus_spine.R <start_year> <end_year>")
}
start_year <- as.integer(cli_args[[1]])
end_year <- as.integer(cli_args[[2]])
if (is.na(start_year) || is.na(end_year) || start_year > end_year) {
  stop("Invalid corpus year range.")
}

normalize_application_key <- function(x) {
  raw_value <- str_to_upper(str_replace_all(str_squish(as.character(x)), "[^A-Z0-9]", ""))
  raw_value[raw_value == ""] <- NA_character_
  str_replace(raw_value, "^[CNMI](?=[0-9])", "")
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
    completed_year = extract_year(completed_date)
  ) |>
  filter(
    ulurp_group == "ULURP",
    !is.na(corpus_reference_year),
    between(corpus_reference_year, start_year, end_year)
  ) |>
  arrange(corpus_reference_year, borough_name_standardized, project_id)

application_rows <- zap_project |>
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
    ulurp_numbers,
    actions,
    ceqr_number,
    applicant_type,
    primary_applicant,
    borough_name = borough_name_standardized,
    community_district = community_district_standardized,
    council_district_first,
    project_page_url = paste0("https://zap.planning.nyc.gov/projects/", project_id),
    api_url = paste0("https://zap-api-production.herokuapp.com/projects/", project_id)
  ) |>
  mutate(
    raw_application_number = str_extract_all(
      coalesce(ulurp_numbers, ""),
      regex("(?:[CNMI]\\s*)?\\d{6}(?:\\s*\\([A-Z]\\)|[A-Z])?\\s*[A-Z]{2,4}[A-Z](?:\\s*\\([A-Z]\\))?", ignore_case = TRUE)
    )
  ) |>
  unnest(raw_application_number) |>
  mutate(
    raw_application_number = str_squish(str_to_upper(raw_application_number)),
    compact_application_number = str_replace_all(raw_application_number, "[^A-Z0-9]", ""),
    application_key = normalize_application_key(raw_application_number),
    application_prefix = str_extract(compact_application_number, "^[CNMI](?=[0-9])"),
    application_digits = str_extract(raw_application_number, "\\d{6,8}"),
    action_borough_code = str_match(
      compact_application_number,
      "^(?:[CNMI])?\\d{6}A?([A-Z]{3,5})$"
    )[, 2],
    a_application_flag = str_detect(
      compact_application_number,
      "^(?:[CNMI])?\\d{6}A[A-Z]{3,5}$"
    )
  ) |>
  filter(!is.na(application_digits), !is.na(application_key), application_key != "") |>
  distinct(project_id, application_key, raw_application_number, .keep_all = TRUE) |>
  arrange(corpus_reference_year, project_id, raw_application_number) |>
  select(
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
    raw_application_number,
    application_key,
    application_prefix,
    application_digits,
    action_borough_code,
    a_application_flag,
    ulurp_numbers,
    actions,
    ceqr_number,
    applicant_type,
    primary_applicant,
    borough_name,
    community_district,
    council_district_first,
    project_page_url,
    api_url
  )

write_csv_if_changed(application_rows, "../output/ulurp_corpus_application_spine.csv")
