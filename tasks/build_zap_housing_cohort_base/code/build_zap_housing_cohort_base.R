# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_zap_housing_cohort_base/code")
# zap_project_parquet <- "../input/zap_project_data.parquet"
# zap_project_bbl_parquet <- "../input/zap_project_bbl.parquet"
# cd_homeownership_1990_measure_csv <- "../input/cd_homeownership_1990_measure.csv"
# cd_baseline_1990_controls_csv <- "../input/cd_baseline_1990_controls.csv"
# out_base_csv <- "../output/zap_housing_cohort_base.csv"
# out_qc_csv <- "../output/zap_housing_cohort_base_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 6) {
  stop("Expected 6 arguments: zap_project_parquet zap_project_bbl_parquet cd_homeownership_1990_measure_csv cd_baseline_1990_controls_csv out_base_csv out_qc_csv")
}

zap_project_parquet <- args[1]
zap_project_bbl_parquet <- args[2]
cd_homeownership_1990_measure_csv <- args[3]
cd_baseline_1990_controls_csv <- args[4]
out_base_csv <- args[5]
out_qc_csv <- args[6]

static_control_cols <- c(
  "vacancy_rate_1990_exact",
  "structure_share_1_2_units_1990_exact",
  "structure_share_3_4_units_1990_exact",
  "structure_share_5_plus_units_1990_exact",
  "median_household_income_1990_1999_dollars_exact",
  "poverty_share_1990_exact",
  "median_housing_value_1990_2000_dollars_exact_filled",
  "median_housing_value_1990_missing_exact",
  "subway_commute_share_1990_exact",
  "mean_commute_time_1990_minutes_exact",
  "foreign_born_share_1990_exact",
  "college_graduate_share_1990_exact",
  "unemployment_rate_1990_exact"
)

measure_df <- read_csv(cd_homeownership_1990_measure_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  transmute(
    borocd = as.integer(borocd),
    borough_name = as.character(borough_name),
    treat_pp = suppressWarnings(as.numeric(treat_pp)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro))
  )

controls_df <- read_csv(cd_baseline_1990_controls_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  transmute(
    borocd = as.integer(borocd),
    across(all_of(static_control_cols), ~ suppressWarnings(as.numeric(.x)))
  )

bbl_df <- read_parquet(zap_project_bbl_parquet) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(
    project_id = as.character(project_id),
    bbl_standardized = as.character(bbl_standardized)
  ) %>%
  filter(!is.na(project_id))

bbl_summary <- bbl_df %>%
  group_by(project_id) %>%
  summarise(
    bbl_count = n_distinct(bbl_standardized[!is.na(bbl_standardized)]),
    has_bbl = bbl_count > 0,
    .groups = "drop"
  )

base_df <- read_parquet(zap_project_parquet) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(
    project_id = as.character(project_id),
    project_name = as.character(project_name),
    project_brief = as.character(project_brief),
    project_status = as.character(project_status),
    public_status = as.character(public_status),
    actions = as.character(actions),
    mih_flag = as.character(mih_flag),
    ulurp_non = as.character(ulurp_non),
    borocd = suppressWarnings(as.integer(community_district_standardized)),
    certified_referred_date = as.Date(certified_referred_date_parsed),
    cert_year = suppressWarnings(as.integer(format(certified_referred_date, "%Y"))),
    combined_text = str_to_upper(str_squish(paste(coalesce(project_name, ""), coalesce(project_brief, "")))),
    housing_text_flag = str_detect(combined_text, "HOUSING|RESIDENTIAL|RESIDENT\\b|APART|DWELL|AFFORD|MIXED[ -]?USE|INCLUSIONARY|\\bMIH\\b"),
    mih_housing_flag = str_to_lower(coalesce(mih_flag, "")) == "true",
    housing_flag = housing_text_flag | mih_housing_flag,
    housing_flag_reason = case_when(
      housing_text_flag & mih_housing_flag ~ "text_and_mih",
      housing_text_flag ~ "text",
      mih_housing_flag ~ "mih",
      TRUE ~ NA_character_
    ),
    cert_era = case_when(
      cert_year >= 1976 & cert_year <= 1979 ~ "1976-1979",
      cert_year >= 1980 & cert_year <= 1984 ~ "1980-1984",
      cert_year >= 1985 & cert_year <= 1989 ~ "1985-1989",
      cert_year >= 1990 & cert_year <= 1999 ~ "1990-1999",
      cert_year >= 2000 & cert_year <= 2009 ~ "2000-2009",
      cert_year >= 2010 & cert_year <= 2015 ~ "2010-2015",
      cert_year >= 2016 & cert_year <= 2025 ~ "2016-2025",
      cert_year >= 2026 ~ "2026_plus",
      TRUE ~ NA_character_
    ),
    is_complete = project_status == "Complete",
    is_fail = project_status %in% c("Withdrawn-Other", "Terminated", "Terminated-Applicant Unresponsive"),
    is_unresolved = !is_complete & !is_fail
  ) %>%
  filter(
    borocd %in% measure_df$borocd,
    ulurp_non == "ULURP",
    !is.na(cert_year),
    cert_year >= 1976,
    housing_flag
  ) %>%
  left_join(bbl_summary, by = "project_id") %>%
  left_join(measure_df, by = "borocd") %>%
  left_join(controls_df, by = "borocd") %>%
  mutate(
    bbl_count = coalesce(bbl_count, 0L),
    has_bbl = coalesce(has_bbl, FALSE),
    borough_name = ifelse(is.na(borough_name), standardize_borough_name(borocd %/% 100), borough_name)
  ) %>%
  select(
    project_id,
    project_name,
    project_brief,
    borocd,
    borough_name,
    certified_referred_date,
    cert_year,
    cert_era,
    ulurp_non,
    project_status,
    public_status,
    actions,
    mih_flag,
    housing_flag,
    housing_flag_reason,
    housing_text_flag,
    mih_housing_flag,
    is_complete,
    is_fail,
    is_unresolved,
    has_bbl,
    bbl_count,
    treat_pp,
    treat_z_boro,
    all_of(static_control_cols)
  ) %>%
  arrange(cert_year, borocd, project_id)

qc_df <- bind_rows(
  tibble(
    metric = "row_count",
    value = nrow(base_df),
    note = "Housing-oriented ULURP project rows in the analysis base."
  ),
  tibble(
    metric = "unique_project_id_count",
    value = n_distinct(base_df$project_id),
    note = "Distinct project IDs in the analysis base."
  ),
  tibble(
    metric = "min_cert_year",
    value = min(base_df$cert_year, na.rm = TRUE),
    note = "Minimum certification year in the analysis base."
  ),
  tibble(
    metric = "max_cert_year",
    value = max(base_df$cert_year, na.rm = TRUE),
    note = "Maximum certification year in the analysis base."
  ),
  tibble(
    metric = "non_ulurp_row_count",
    value = sum(base_df$ulurp_non != "ULURP", na.rm = TRUE),
    note = "Should be zero after the ULURP filter."
  ),
  tibble(
    metric = "nonstandard_cd_row_count",
    value = sum(!base_df$borocd %in% measure_df$borocd, na.rm = TRUE),
    note = "Should be zero after restricting to the standard 59 community districts."
  ),
  tibble(
    metric = "invalid_housing_flag_reason_row_count",
    value = sum(!base_df$housing_flag_reason %in% c("text", "mih", "text_and_mih"), na.rm = TRUE),
    note = "Should be zero after building the housing flag reason."
  ),
  tibble(
    metric = "has_bbl_share",
    value = mean(base_df$has_bbl, na.rm = TRUE),
    note = "Share of housing-oriented ULURP projects with at least one linked BBL."
  ),
  tibble(
    metric = "mean_bbl_count",
    value = mean(base_df$bbl_count, na.rm = TRUE),
    note = "Average linked BBL count across housing-oriented ULURP projects."
  ),
  tibble(
    metric = "mature_cohort_row_count",
    value = sum(base_df$cert_year <= 2015, na.rm = TRUE),
    note = "Projects in the mature cohort window used for completion/failure outcomes."
  )
)

write_csv_if_changed(base_df, out_base_csv)
write_csv_if_changed(qc_df, out_qc_csv)

cat("Wrote ZAP housing cohort base outputs to", dirname(out_base_csv), "\n")
