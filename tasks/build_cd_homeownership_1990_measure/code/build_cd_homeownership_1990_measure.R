# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_cd_homeownership_1990_measure/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

stage_files <- read_csv("../input/dcp_cd_profiles_1990_2000_files.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(pull_date = as.character(pull_date)) %>%
  filter(!is.na(parquet_path), file.exists(parquet_path))

if (nrow(stage_files) == 0) {
  write_csv(tibble(), "../output/cd_homeownership_1990_measure.csv", na = "")
  write_csv(tibble(), "../output/cd_homeownership_1990_measure_qc.csv", na = "")
  quit(save = "no")
}

stage_file <- stage_files %>%
  arrange(desc(pull_date), parquet_path) %>%
  slice_head(n = 1)

homeownership_cells <- read_parquet(stage_file$parquet_path[[1]]) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(
    district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
    borough_code = substr(district_id, 1, 1),
    borough_name = standardize_borough_name(borough_code)
  ) %>%
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
  distinct()

duplicate_homeownership_cells <- homeownership_cells %>%
  count(district_id, metric_key, name = "source_row_count") %>%
  filter(source_row_count > 1)

if (nrow(duplicate_homeownership_cells) > 0) {
  stop("DCP homeownership cells are not unique by district_id and metric_key; fix staged profiles before building the canonical measure.")
}

exact_df <- homeownership_cells %>%
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

borough_df <- exact_df %>%
  group_by(borough_code, borough_name) %>%
  summarise(
    borough_owner_occupied_units_1990 = sum(owner_occupied_units_1990, na.rm = TRUE),
    borough_occupied_units_1990 = sum(occupied_units_1990, na.rm = TRUE),
    borough_homeowner_share_1990 = borough_owner_occupied_units_1990 / borough_occupied_units_1990,
    borough_homeowner_share_1990_pct = 100 * borough_homeowner_share_1990,
    district_count = n(),
    .groups = "drop"
  )

measure_df <- exact_df %>%
  left_join(
    borough_df,
    by = c("borough_code", "borough_name"),
    relationship = "many-to-one"
  ) %>%
  mutate(
    homeowner_share_minus_borough = homeowner_share_1990 - borough_homeowner_share_1990,
    treat_pp = 100 * homeowner_share_minus_borough,
    owner_share_reported_gap_pp = homeowner_share_1990_pct - owner_occupied_share_reported_1990_pct
  ) %>%
  group_by(borough_code, borough_name) %>%
  mutate(
    treat_pp_boro_mean = mean(treat_pp, na.rm = TRUE),
    treat_pp_boro_sd = sd(treat_pp, na.rm = TRUE),
    treat_z_boro = (treat_pp - treat_pp_boro_mean) / treat_pp_boro_sd,
    treat_z_boro = ifelse(is.finite(treat_z_boro), treat_z_boro, NA_real_)
  ) %>%
  ungroup() %>%
  transmute(
    source_id,
    pull_date,
    district_id,
    borocd = as.integer(district_id),
    borough_code,
    borough_name,
    owner_occupied_units_1990,
    occupied_units_1990,
    borough_owner_occupied_units_1990,
    borough_occupied_units_1990,
    h_cd_1990 = homeowner_share_1990,
    h_cd_1990_pct = homeowner_share_1990_pct,
    h_b_1990 = borough_homeowner_share_1990,
    h_b_1990_pct = borough_homeowner_share_1990_pct,
    cd_minus_borough_1990 = homeowner_share_minus_borough,
    treat_pp,
    treat_z_boro,
    owner_occupied_share_reported_1990_pct,
    owner_share_reported_gap_pp
  ) %>%
  arrange(district_id)

qc_df <- bind_rows(
  tibble(
    metric = "district_count",
    value = nrow(measure_df),
    note = "Community districts in the canonical exact 1990 homeownership exposure measure."
  ),
  tibble(
    metric = "borough_count",
    value = n_distinct(measure_df$borough_name),
    note = "Boroughs represented in the canonical exact 1990 homeownership exposure measure."
  ),
  tibble(
    metric = "missing_h_cd_count",
    value = sum(is.na(measure_df$h_cd_1990)),
    note = "Community districts with missing CD-level 1990 homeownership rates."
  ),
  tibble(
    metric = "missing_h_b_count",
    value = sum(is.na(measure_df$h_b_1990)),
    note = "Community districts with missing borough-level 1990 homeownership rates."
  ),
  tibble(
    metric = "missing_treat_z_boro_count",
    value = sum(is.na(measure_df$treat_z_boro)),
    note = "Community districts with missing within-borough standardized treatment z-scores."
  ),
  tibble(
    metric = "weighted_mean_treat_pp",
    value = weighted.mean(measure_df$treat_pp, w = measure_df$occupied_units_1990, na.rm = TRUE),
    note = "Occupied-unit-weighted mean of treat_pp; this should be approximately zero by construction."
  ),
  tibble(
    metric = "max_abs_reported_owner_share_gap_pp",
    value = max(abs(measure_df$owner_share_reported_gap_pp), na.rm = TRUE),
    note = "Maximum absolute gap between computed H_cd and the reported DCP owner-occupied percent."
  ),
  tibble(
    metric = "status",
    value = ifelse(
      nrow(measure_df) == 59 &&
        all(!is.na(measure_df$h_cd_1990)) &&
        all(!is.na(measure_df$h_b_1990)),
      1,
      0
    ),
    note = "One means the exact canonical 1990 homeownership exposure measure is complete."
  )
)

write_csv_if_changed(measure_df, "../output/cd_homeownership_1990_measure.csv")
write_csv_if_changed(qc_df, "../output/cd_homeownership_1990_measure_qc.csv")

cat("Wrote canonical CD homeownership measure outputs to ../output\n")
