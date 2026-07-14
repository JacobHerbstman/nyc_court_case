# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/benchmark_ulurp_corpus_universe/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

socrata_count <- function(where_clause = NA_character_) {
  query_url <- "https://data.cityofnewyork.us/resource/hgx4-8ukb.json?%24select=count%28%2A%29"
  if (!is.na(where_clause) && where_clause != "") {
    query_url <- paste0(query_url, "&%24where=", URLencode(where_clause, reserved = TRUE))
  }

  response <- system2("curl", args = c("-sL", shQuote(query_url)), stdout = TRUE, stderr = TRUE)
  response_text <- paste(response, collapse = " ")
  count_match <- str_match(response_text, '"count"\\s*:\\s*"([0-9]+)"')
  if (is.na(count_match[1, 2])) {
    stop("Could not parse Socrata count response: ", response_text)
  }

  as.integer(count_match[1, 2])
}

fmt_pct <- function(numerator, denominator) {
  if (is.na(denominator) || denominator == 0) {
    return(NA_character_)
  }
  paste0(formatC(100 * numerator / denominator, format = "f", digits = 1), "%")
}

zap_project <- read_parquet("../input/zap_project_data.parquet") |>
  as.data.frame() |>
  as_tibble() |>
  mutate(
    project_id = as.character(project_id),
    ulurp_group = str_squish(as.character(ulurp_group)),
    ulurp_numbers = str_squish(as.character(ulurp_numbers)),
    source_vintage = str_squish(as.character(source_vintage)),
    project_reference_year = suppressWarnings(as.integer(project_reference_year))
  )

project_spine <- read_csv("../input/ulurp_corpus_project_spine.csv", show_col_types = FALSE)
application_spine <- read_csv("../input/ulurp_corpus_application_spine.csv", show_col_types = FALSE)
cpc_manifest <- read_csv("../input/ulurp_cpc_report_manifest.csv", show_col_types = FALSE)
cpc_failures <- read_csv("../input/ulurp_cpc_report_fetch_failures.csv", show_col_types = FALSE)
council_qc <- read_csv("../input/council_zap_land_use_volume_comparison_qc.csv", show_col_types = FALSE)
council_period <- read_csv("../input/council_zap_land_use_volume_comparison_period_summary.csv", show_col_types = FALSE)

live_total <- socrata_count()
live_ulurp <- socrata_count("ulurp_non='ULURP'")
live_ulurp_with_number <- socrata_count("ulurp_non='ULURP' AND ulurp_numbers IS NOT NULL")
live_ulurp_certified_1975 <- socrata_count("ulurp_non='ULURP' AND certified_referred >= '1975-01-01T00:00:00'")

staged_total <- nrow(zap_project)
staged_ulurp <- sum(zap_project$ulurp_group == "ULURP", na.rm = TRUE)
staged_ulurp_missing_reference_year <- sum(zap_project$ulurp_group == "ULURP" & is.na(zap_project$project_reference_year), na.rm = TRUE)
staged_ulurp_reference_year_1975_plus <- sum(zap_project$ulurp_group == "ULURP" & !is.na(zap_project$project_reference_year) & zap_project$project_reference_year >= 1975L, na.rm = TRUE)
staged_ulurp_with_number <- sum(zap_project$ulurp_group == "ULURP" & !is.na(zap_project$ulurp_numbers) & zap_project$ulurp_numbers != "", na.rm = TRUE)

corpus_project_count <- nrow(project_spine)
corpus_project_with_number <- sum(project_spine$has_ulurp_number == TRUE | str_to_upper(as.character(project_spine$has_ulurp_number)) == "TRUE", na.rm = TRUE)
corpus_application_count <- nrow(application_spine)
cpc_downloaded_count <- sum(cpc_manifest$download_status == "downloaded", na.rm = TRUE)
cpc_text_count <- sum(cpc_manifest$text_status == "text_extracted", na.rm = TRUE)
cpc_failure_count <- nrow(cpc_failures)

council_corr <- council_qc |>
  filter(metric %in% c("annual_count_correlation_1998_2025", "rolling_5_index_correlation_2002_2025")) |>
  transmute(metric, value)

summary_rows <- bind_rows(
  tibble(
    benchmark_group = "live_nyc_open_data",
    metric = c(
      "live_total_project_rows",
      "live_ulurp_project_rows",
      "live_ulurp_project_rows_with_ulurp_numbers",
      "live_ulurp_project_rows_certified_1975_plus"
    ),
    value = c(live_total, live_ulurp, live_ulurp_with_number, live_ulurp_certified_1975),
    comparison_value = NA_real_,
    difference = NA_real_,
    note = c(
      "Live Socrata count from official NYC Open Data ZAP project dataset.",
      "Live Socrata count where ulurp_non is ULURP.",
      "Live Socrata count where ulurp_non is ULURP and ulurp_numbers is nonmissing.",
      "Live Socrata count where ulurp_non is ULURP and certified_referred is 1975 or later."
    )
  ),
  tibble(
    benchmark_group = "staged_vs_live",
    metric = c(
      "staged_total_project_rows_minus_live",
      "staged_ulurp_project_rows_minus_live",
      "staged_ulurp_with_numbers_minus_live"
    ),
    value = c(staged_total, staged_ulurp, staged_ulurp_with_number),
    comparison_value = c(live_total, live_ulurp, live_ulurp_with_number),
    difference = value - comparison_value,
    note = c(
      paste0("Staged ZAP total rows from source vintage ", paste(unique(zap_project$source_vintage), collapse = "; "), " compared with live Socrata total."),
      "Staged ZAP rows flagged ULURP compared with live Socrata ULURP rows.",
      "Staged ZAP rows flagged ULURP with nonmissing ulurp_numbers compared with live Socrata rows."
    )
  ),
  tibble(
    benchmark_group = "corpus_from_staged",
    metric = c(
      "staged_ulurp_project_rows",
      "staged_ulurp_missing_reference_year",
      "corpus_ulurp_project_rows_1975_plus",
      "corpus_ulurp_projects_with_numbers",
      "corpus_application_rows",
      "cpc_downloaded_report_rows",
      "cpc_text_extracted_report_rows",
      "cpc_verified_missing_report_rows"
    ),
    value = c(
      staged_ulurp,
      staged_ulurp_missing_reference_year,
      corpus_project_count,
      corpus_project_with_number,
      corpus_application_count,
      cpc_downloaded_count,
      cpc_text_count,
      cpc_failure_count
    ),
    comparison_value = c(
      NA_real_,
      staged_ulurp,
      staged_ulurp_reference_year_1975_plus,
      corpus_project_count,
      corpus_application_count,
      corpus_application_count,
      corpus_application_count,
      corpus_application_count
    ),
    difference = value - comparison_value,
    note = c(
      "Staged ZAP rows flagged ULURP before corpus year/reference-year restrictions.",
      paste0("Share of staged ULURP rows missing project reference year: ", fmt_pct(staged_ulurp_missing_reference_year, staged_ulurp), "."),
      "Corpus ULURP project spine rows with usable reference year 1975 or later.",
      paste0("Share of corpus project rows with populated ULURP number: ", fmt_pct(corpus_project_with_number, corpus_project_count), "."),
      "Distinct parsed application/action rows from corpus project spine.",
      paste0("Share of parsed application rows with downloaded CPC PDF: ", fmt_pct(cpc_downloaded_count, corpus_application_count), "."),
      paste0("Share of parsed application rows with extracted CPC text: ", fmt_pct(cpc_text_count, corpus_application_count), "."),
      paste0("Share of parsed application rows verified missing from checked CPC sources: ", fmt_pct(cpc_failure_count, corpus_application_count), ".")
    )
  ),
  tibble(
    benchmark_group = "council_overlap",
    metric = council_corr$metric,
    value = suppressWarnings(as.numeric(council_corr$value)),
    comparison_value = NA_real_,
    difference = NA_real_,
    note = c(
      "Existing audit correlation between Council Legistar land-use matter counts and ZAP ULURP project counts over 1998-2025.",
      "Existing audit correlation between trailing 5-year indexed Council and ZAP series over 2002-2025."
    )
  )
) |>
  mutate(
    value = as.numeric(value),
    comparison_value = as.numeric(comparison_value),
    difference = as.numeric(difference)
  )

period_rows <- council_period |>
  filter(series_id == "zap_ulurp_project_records") |>
  transmute(
    benchmark_group = "zap_period_volume",
    metric = paste0("zap_mean_annual_project_records_", period),
    value = as.numeric(annual_count_mean),
    comparison_value = NA_real_,
    difference = NA_real_,
    note = paste0("Existing ZAP period-volume audit: ", period, " mean annual project-record count.")
  )

summary_rows <- bind_rows(summary_rows, period_rows)

qc_rows <- tibble(
  metric = c(
    "live_count_query_count",
    "staged_total_close_to_live",
    "staged_ulurp_close_to_live",
    "corpus_project_matches_staged_1975_plus",
    "cpc_report_text_coverage_rate",
    "cpc_report_failure_rate",
    "council_zap_rolling_index_correlation"
  ),
  value = c(
    "4",
    as.character(staged_total - live_total),
    as.character(staged_ulurp - live_ulurp),
    as.character(corpus_project_count - staged_ulurp_reference_year_1975_plus),
    fmt_pct(cpc_text_count, corpus_application_count),
    fmt_pct(cpc_failure_count, corpus_application_count),
    council_qc$value[council_qc$metric == "rolling_5_index_correlation_2002_2025"]
  ),
  status = c(
    "pass",
    if_else(abs(staged_total - live_total) <= 100L, "pass", "warning"),
    if_else(abs(staged_ulurp - live_ulurp) <= 150L, "pass", "warning"),
    if_else(corpus_project_count == staged_ulurp_reference_year_1975_plus, "pass", "fail"),
    if_else(cpc_text_count / corpus_application_count >= 0.85, "pass", "warning"),
    if_else(cpc_failure_count / corpus_application_count <= 0.15, "pass", "warning"),
    if_else(as.numeric(council_qc$value[council_qc$metric == "rolling_5_index_correlation_2002_2025"]) >= 0.85, "pass", "warning")
  ),
  note = c(
    "Four live aggregate queries were sent to the official NYC Open Data Socrata endpoint.",
    "Difference between local staged ZAP rows and live total rows; small differences are expected across rolling vintages.",
    "Difference between local staged ULURP rows and live ULURP rows; small differences are expected across rolling vintages.",
    "Corpus project spine should match staged ULURP rows with usable 1975-plus reference year.",
    "CPC report text coverage across parsed application/action rows.",
    "Verified missing CPC report share across parsed application/action rows.",
    "Existing Council/ZAP overlap benchmark should move similarly in the post-1998 period."
  )
)

write_csv_if_changed(summary_rows, "../output/ulurp_corpus_universe_benchmark_summary.csv")
write_csv_if_changed(qc_rows, "../output/ulurp_corpus_universe_benchmark_qc.csv")

if (any(qc_rows$status == "fail")) {
  stop("ULURP corpus universe benchmark QC failed.")
}

cat("Wrote ULURP corpus universe benchmark outputs to ../output\n")
