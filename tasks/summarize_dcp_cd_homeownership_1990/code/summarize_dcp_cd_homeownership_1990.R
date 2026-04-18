# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_dcp_cd_homeownership_1990/code")
# dcp_cd_profiles_1990_2000_files_csv <- "../input/dcp_cd_profiles_1990_2000_files.csv"
# out_cd_csv <- "../output/dcp_cd_homeownership_1990.csv"
# out_borough_summary_csv <- "../output/dcp_cd_homeownership_1990_borough_summary.csv"
# out_qc_csv <- "../output/dcp_cd_homeownership_1990_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4) {
  stop("Expected 4 arguments: dcp_cd_profiles_1990_2000_files_csv out_cd_csv out_borough_summary_csv out_qc_csv")
}

dcp_cd_profiles_1990_2000_files_csv <- args[1]
out_cd_csv <- args[2]
out_borough_summary_csv <- args[3]
out_qc_csv <- args[4]

stage_files <- read_csv(dcp_cd_profiles_1990_2000_files_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(pull_date = as.character(pull_date)) %>%
  filter(!is.na(parquet_path), file.exists(parquet_path))

if (nrow(stage_files) == 0) {
  write_csv(tibble(), out_cd_csv, na = "")
  write_csv(tibble(), out_borough_summary_csv, na = "")
  write_csv(tibble(), out_qc_csv, na = "")
  quit(save = "no")
}

stage_file <- stage_files %>%
  arrange(desc(pull_date), parquet_path) %>%
  slice_head(n = 1)

staged_df <- read_parquet(stage_file$parquet_path[[1]]) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(
    district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
    borough_code = substr(district_id, 1, 1),
    borough_name = standardize_borough_name(borough_code)
  )

homeownership_df <- staged_df %>%
  filter(
    section_name == "housing_tenure",
    metric_label %in% c("Occupied housing units", "Owner-occupied housing units")
  ) %>%
  mutate(metric_key = case_when(
    metric_label == "Occupied housing units" ~ "occupied",
    metric_label == "Owner-occupied housing units" ~ "owner",
    TRUE ~ NA_character_
  )) %>%
  select(district_id, borough_code, borough_name, metric_key, value_1990_number, value_1990_percent, footnote_markers) %>%
  distinct() %>%
  pivot_wider(
    names_from = metric_key,
    values_from = c(value_1990_number, value_1990_percent, footnote_markers),
    names_glue = "{metric_key}_{.value}"
  ) %>%
  transmute(
    source_id = stage_file$source_id[[1]],
    pull_date = stage_file$pull_date[[1]],
    district_id,
    borough_code,
    borough_name,
    owner_occupied_units_1990 = owner_value_1990_number,
    occupied_units_1990 = occupied_value_1990_number,
    owner_occupied_share_reported_1990_pct = owner_value_1990_percent,
    occupied_share_reported_1990_pct = occupied_value_1990_percent,
    occupied_footnote_markers = occupied_footnote_markers,
    owner_footnote_markers = owner_footnote_markers
  ) %>%
  arrange(district_id) %>%
  mutate(
    homeowner_share_1990 = owner_occupied_units_1990 / occupied_units_1990,
    homeowner_share_1990_pct = 100 * homeowner_share_1990
  )

borough_df <- homeownership_df %>%
  group_by(borough_code, borough_name) %>%
  summarise(
    borough_owner_occupied_units_1990 = sum(owner_occupied_units_1990, na.rm = TRUE),
    borough_occupied_units_1990 = sum(occupied_units_1990, na.rm = TRUE),
    borough_homeowner_share_1990 = borough_owner_occupied_units_1990 / borough_occupied_units_1990,
    borough_homeowner_share_1990_pct = 100 * borough_homeowner_share_1990,
    district_count = n(),
    .groups = "drop"
  )

cd_df <- homeownership_df %>%
  left_join(borough_df, by = c("borough_code", "borough_name")) %>%
  mutate(
    homeowner_share_minus_borough = homeowner_share_1990 - borough_homeowner_share_1990,
    treat_pp = 100 * homeowner_share_minus_borough,
    owner_share_reported_gap_pp = homeowner_share_1990_pct - owner_occupied_share_reported_1990_pct
  ) %>%
  arrange(district_id)

borough_summary_df <- cd_df %>%
  group_by(borough_code, borough_name) %>%
  summarise(
    district_count = first(district_count),
    borough_owner_occupied_units_1990 = first(borough_owner_occupied_units_1990),
    borough_occupied_units_1990 = first(borough_occupied_units_1990),
    borough_homeowner_share_1990 = first(borough_homeowner_share_1990),
    borough_homeowner_share_1990_pct = first(borough_homeowner_share_1990_pct),
    cd_homeowner_share_1990_pct_min = min(homeowner_share_1990_pct, na.rm = TRUE),
    cd_homeowner_share_1990_pct_median = median(homeowner_share_1990_pct, na.rm = TRUE),
    cd_homeowner_share_1990_pct_max = max(homeowner_share_1990_pct, na.rm = TRUE),
    cd_homeowner_share_1990_pct_sd = sd(homeowner_share_1990_pct, na.rm = TRUE),
    treat_pp_min = min(treat_pp, na.rm = TRUE),
    treat_pp_median = median(treat_pp, na.rm = TRUE),
    treat_pp_max = max(treat_pp, na.rm = TRUE),
    treat_pp_sd = sd(treat_pp, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(borough_code)

qc_df <- bind_rows(
  tibble(
    metric = "district_count",
    value = nrow(cd_df),
    note = "Community districts with exact 1990 housing tenure counts from the staged DCP profiles."
  ),
  tibble(
    metric = "borough_count",
    value = n_distinct(cd_df$borough_name),
    note = "Boroughs present in the exact CD homeownership table."
  ),
  tibble(
    metric = "missing_occupied_units_count",
    value = sum(is.na(cd_df$occupied_units_1990)),
    note = "Community districts with missing occupied housing units."
  ),
  tibble(
    metric = "missing_owner_occupied_units_count",
    value = sum(is.na(cd_df$owner_occupied_units_1990)),
    note = "Community districts with missing owner-occupied housing units."
  ),
  tibble(
    metric = "max_abs_reported_owner_share_gap_pp",
    value = max(abs(cd_df$owner_share_reported_gap_pp), na.rm = TRUE),
    note = "Maximum absolute difference between the computed homeownership rate and the reported DCP owner-occupied percent."
  ),
  tibble(
    metric = "status",
    value = ifelse(nrow(cd_df) == 59 && all(!is.na(cd_df$homeowner_share_1990)), 1, 0),
    note = "One means the exact DCP homeownership table has 59 complete community districts."
  )
)

write_csv_if_changed(cd_df, out_cd_csv)
write_csv_if_changed(borough_summary_df, out_borough_summary_csv)
write_csv_if_changed(qc_df, out_qc_csv)

cat("Wrote exact DCP CD homeownership outputs to", dirname(out_cd_csv), "\n")
