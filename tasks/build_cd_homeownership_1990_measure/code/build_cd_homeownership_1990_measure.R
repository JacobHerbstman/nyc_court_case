# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_cd_homeownership_1990_measure/code")
# dcp_cd_homeownership_1990_csv <- "../input/dcp_cd_homeownership_1990.csv"
# out_measure_csv <- "../output/cd_homeownership_1990_measure.csv"
# out_qc_csv <- "../output/cd_homeownership_1990_measure_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: dcp_cd_homeownership_1990_csv out_measure_csv out_qc_csv")
}

dcp_cd_homeownership_1990_csv <- args[1]
out_measure_csv <- args[2]
out_qc_csv <- args[3]

exact_df <- read_csv(dcp_cd_homeownership_1990_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
    borough_code = as.character(borough_code),
    borough_name = standardize_borough_name(borough_code),
    pull_date = as.character(pull_date)
  )

measure_df <- exact_df %>%
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
    owner_occupied_share_reported_1990_pct,
    owner_share_reported_gap_pp
  ) %>%
  group_by(borough_code, borough_name) %>%
  mutate(
    treat_pp_boro_mean = mean(treat_pp, na.rm = TRUE),
    treat_pp_boro_sd = sd(treat_pp, na.rm = TRUE),
    treat_z_boro = (treat_pp - treat_pp_boro_mean) / treat_pp_boro_sd,
    treat_z_boro = ifelse(is.finite(treat_z_boro), treat_z_boro, NA_real_)
  ) %>%
  ungroup() %>%
  select(-treat_pp_boro_mean, -treat_pp_boro_sd) %>%
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

write_csv_if_changed(measure_df, out_measure_csv)
write_csv_if_changed(qc_df, out_qc_csv)

cat("Wrote canonical CD homeownership measure outputs to", dirname(out_measure_csv), "\n")
