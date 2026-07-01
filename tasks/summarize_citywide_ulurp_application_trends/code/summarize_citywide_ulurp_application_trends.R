suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

extract_ulurp_numbers <- function(x) {
  raw_value <- str_replace_all(str_to_upper(coalesce(as.character(x), "")), "\\s+", "")
  str_extract_all(raw_value, "\\b[CN]?[0-9]{6,7}A?[A-Z]{3}\\b")
}

centered_ma3 <- function(x) {
  out <- rep(NA_real_, length(x))
  if (length(x) < 3) {
    return(out)
  }

  for (i in 2:(length(x) - 1)) {
    window <- x[(i - 1):(i + 1)]
    if (all(!is.na(window))) {
      out[i] <- mean(window)
    }
  }

  out
}

project_df <- read_parquet("../input/zap_project_data.parquet") |>
  mutate(
    project_id = as.character(project_id),
    ulurp_flag = str_to_upper(str_squish(coalesce(as.character(ulurp_non), ""))) == "ULURP",
    cert_year = suppressWarnings(as.integer(format(certified_referred_date_parsed, "%Y"))),
    ulurp_application_number = extract_ulurp_numbers(ulurp_numbers)
  )

if (nrow(project_df) != n_distinct(project_df$project_id)) {
  stop("Staged ZAP project data are not unique by project_id.")
}

ulurp_df <- project_df |>
  filter(ulurp_flag, cert_year >= 1976, cert_year <= 2025)

project_year_counts <- tibble(cert_year = 1976:2025) |>
  left_join(
    ulurp_df |>
      count(cert_year, name = "application_count"),
    by = "cert_year",
    relationship = "one-to-one"
  ) |>
  mutate(
    application_count = coalesce(application_count, 0L),
    count_unit = "zap_project_records"
  )

number_year_counts <- tibble(cert_year = 1976:2025) |>
  left_join(
    ulurp_df |>
      select(project_id, cert_year, ulurp_application_number) |>
      unnest_longer(ulurp_application_number, keep_empty = FALSE) |>
      filter(!is.na(ulurp_application_number), str_squish(ulurp_application_number) != "") |>
      arrange(ulurp_application_number, cert_year, project_id) |>
      distinct(ulurp_application_number, .keep_all = TRUE) |>
      count(cert_year, name = "application_count"),
    by = "cert_year",
    relationship = "one-to-one"
  ) |>
  mutate(
    application_count = coalesce(application_count, 0L),
    count_unit = "parsed_ulurp_numbers"
  )

citywide_year_counts <- bind_rows(project_year_counts, number_year_counts) |>
  mutate(
    count_unit_label = case_when(
      count_unit == "zap_project_records" ~ "ZAP project records",
      count_unit == "parsed_ulurp_numbers" ~ "Parsed ULURP numbers",
      TRUE ~ count_unit
    )
  ) |>
  group_by(count_unit) |>
  arrange(cert_year, .by_group = TRUE) |>
  mutate(application_count_ma3 = centered_ma3(application_count)) |>
  ungroup() |>
  arrange(count_unit, cert_year)

if (nrow(citywide_year_counts) != nrow(distinct(citywide_year_counts, count_unit, cert_year))) {
  stop("Citywide ULURP yearly series is not unique by count unit and year.")
}

if (min(citywide_year_counts$cert_year) != 1976 || max(citywide_year_counts$cert_year) != 2025) {
  stop("Citywide ULURP yearly series does not cover 1976-2025.")
}

write_csv_if_changed(citywide_year_counts, "../output/citywide_ulurp_application_year.csv")
