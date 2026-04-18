# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/compare_cd_homeownership_1990_measures/code")
# dcp_cd_homeownership_1990_csv <- "../input/dcp_cd_homeownership_1990.csv"
# nhgis_cd_homeownership_1990_csv <- "../input/nhgis_cd_homeownership_1990.csv"
# out_comparison_csv <- "../output/cd_homeownership_1990_measure_comparison.csv"
# out_summary_csv <- "../output/cd_homeownership_1990_measure_comparison_summary.csv"
# out_top_gap_review_csv <- "../output/cd_homeownership_1990_measure_top_gap_review.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 5) {
  stop("Expected 5 arguments: dcp_cd_homeownership_1990_csv nhgis_cd_homeownership_1990_csv out_comparison_csv out_summary_csv out_top_gap_review_csv")
}

dcp_cd_homeownership_1990_csv <- args[1]
nhgis_cd_homeownership_1990_csv <- args[2]
out_comparison_csv <- args[3]
out_summary_csv <- args[4]
out_top_gap_review_csv <- args[5]

dcp_geography_note_corrected_districts <- c(
  "203", "204", "209", "210", "211",
  "306", "308",
  "407", "408", "409", "412"
)

exact_df <- read_csv(dcp_cd_homeownership_1990_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
    borough_code = as.character(borough_code),
    borough_name = standardize_borough_name(borough_code)
  ) %>%
  transmute(
    district_id,
    borough_code,
    borough_name,
    exact_owner_occupied_units_1990 = owner_occupied_units_1990,
    exact_occupied_units_1990 = occupied_units_1990,
    exact_homeowner_share_1990 = homeowner_share_1990,
    exact_homeowner_share_1990_pct = homeowner_share_1990_pct,
    exact_borough_homeowner_share_1990 = borough_homeowner_share_1990,
    exact_borough_homeowner_share_1990_pct = borough_homeowner_share_1990_pct,
    exact_treat_pp = treat_pp
  )

approx_df <- read_csv(nhgis_cd_homeownership_1990_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
    borough_code = as.character(borough_code),
    borough_name = standardize_borough_name(borough_code)
  ) %>%
  transmute(
    district_id,
    borough_code,
    borough_name,
    approx_owner_occupied_units_1990 = owner_occupied_units_1990,
    approx_occupied_units_1990 = occupied_units_1990,
    approx_homeowner_share_1990 = homeowner_share_1990,
    approx_homeowner_share_1990_pct = homeowner_share_1990_pct,
    approx_borough_homeowner_share_1990 = borough_homeowner_share_1990,
    approx_borough_homeowner_share_1990_pct = borough_homeowner_share_1990_pct,
    approx_treat_pp = treat_pp
  )

comparison_df <- exact_df %>%
  inner_join(approx_df, by = c("district_id", "borough_code", "borough_name")) %>%
  mutate(
    dcp_geography_note_corrected_flag = district_id %in% dcp_geography_note_corrected_districts,
    owner_occupied_units_gap = approx_owner_occupied_units_1990 - exact_owner_occupied_units_1990,
    occupied_units_gap = approx_occupied_units_1990 - exact_occupied_units_1990,
    homeowner_share_gap = approx_homeowner_share_1990 - exact_homeowner_share_1990,
    homeowner_share_gap_pp = approx_homeowner_share_1990_pct - exact_homeowner_share_1990_pct,
    abs_homeowner_share_gap_pp = abs(homeowner_share_gap_pp),
    borough_homeowner_share_gap_pp = approx_borough_homeowner_share_1990_pct - exact_borough_homeowner_share_1990_pct,
    treat_gap_pp = approx_treat_pp - exact_treat_pp,
    abs_treat_gap_pp = abs(treat_gap_pp),
    rank_abs_homeowner_share_gap_pp = min_rank(desc(abs_homeowner_share_gap_pp)),
    rank_abs_treat_gap_pp = min_rank(desc(abs_treat_gap_pp))
  ) %>%
  arrange(desc(abs_homeowner_share_gap_pp), district_id)

summary_df <- bind_rows(
  comparison_df %>%
    summarise(
      scope = "overall",
      borough_name = "All",
      district_count = n(),
      correlation_homeowner_share = cor(exact_homeowner_share_1990, approx_homeowner_share_1990),
      mae_homeowner_share_gap_pp = mean(abs_homeowner_share_gap_pp),
      rmse_homeowner_share_gap_pp = sqrt(mean(homeowner_share_gap_pp^2)),
      median_abs_homeowner_share_gap_pp = median(abs_homeowner_share_gap_pp),
      max_abs_homeowner_share_gap_pp = max(abs_homeowner_share_gap_pp),
      homeowner_share_gap_within_0_5pp_count = sum(abs_homeowner_share_gap_pp <= 0.5),
      homeowner_share_gap_within_1_0pp_count = sum(abs_homeowner_share_gap_pp <= 1.0),
      correlation_treat_pp = cor(exact_treat_pp, approx_treat_pp),
      mae_treat_gap_pp = mean(abs_treat_gap_pp),
      rmse_treat_gap_pp = sqrt(mean(treat_gap_pp^2)),
      max_abs_treat_gap_pp = max(abs_treat_gap_pp),
      treat_gap_within_0_5pp_count = sum(abs_treat_gap_pp <= 0.5),
      treat_gap_within_1_0pp_count = sum(abs_treat_gap_pp <= 1.0),
      mean_abs_borough_homeowner_share_gap_pp = mean(abs(borough_homeowner_share_gap_pp)),
      corrected_district_count = sum(dcp_geography_note_corrected_flag),
      corrected_in_top_10_homeowner_share_gap_count = sum(rank_abs_homeowner_share_gap_pp <= 10 & dcp_geography_note_corrected_flag)
    ),
  comparison_df %>%
    group_by(borough_name) %>%
    summarise(
      scope = "borough",
      district_count = n(),
      correlation_homeowner_share = cor(exact_homeowner_share_1990, approx_homeowner_share_1990),
      mae_homeowner_share_gap_pp = mean(abs_homeowner_share_gap_pp),
      rmse_homeowner_share_gap_pp = sqrt(mean(homeowner_share_gap_pp^2)),
      median_abs_homeowner_share_gap_pp = median(abs_homeowner_share_gap_pp),
      max_abs_homeowner_share_gap_pp = max(abs_homeowner_share_gap_pp),
      homeowner_share_gap_within_0_5pp_count = sum(abs_homeowner_share_gap_pp <= 0.5),
      homeowner_share_gap_within_1_0pp_count = sum(abs_homeowner_share_gap_pp <= 1.0),
      correlation_treat_pp = cor(exact_treat_pp, approx_treat_pp),
      mae_treat_gap_pp = mean(abs_treat_gap_pp),
      rmse_treat_gap_pp = sqrt(mean(treat_gap_pp^2)),
      max_abs_treat_gap_pp = max(abs_treat_gap_pp),
      treat_gap_within_0_5pp_count = sum(abs_treat_gap_pp <= 0.5),
      treat_gap_within_1_0pp_count = sum(abs_treat_gap_pp <= 1.0),
      mean_abs_borough_homeowner_share_gap_pp = mean(abs(borough_homeowner_share_gap_pp)),
      corrected_district_count = sum(dcp_geography_note_corrected_flag),
      corrected_in_top_10_homeowner_share_gap_count = sum(rank_abs_homeowner_share_gap_pp <= 10 & dcp_geography_note_corrected_flag),
      .groups = "drop"
    )
) %>%
  select(
    scope,
    borough_name,
    district_count,
    correlation_homeowner_share,
    mae_homeowner_share_gap_pp,
    rmse_homeowner_share_gap_pp,
    median_abs_homeowner_share_gap_pp,
    max_abs_homeowner_share_gap_pp,
    homeowner_share_gap_within_0_5pp_count,
    homeowner_share_gap_within_1_0pp_count,
    correlation_treat_pp,
    mae_treat_gap_pp,
    rmse_treat_gap_pp,
    max_abs_treat_gap_pp,
    treat_gap_within_0_5pp_count,
    treat_gap_within_1_0pp_count,
    mean_abs_borough_homeowner_share_gap_pp,
    corrected_district_count,
    corrected_in_top_10_homeowner_share_gap_count
  )

top_gap_review_df <- comparison_df %>%
  slice_head(n = 10) %>%
  transmute(
    rank_abs_homeowner_share_gap_pp,
    district_id,
    borough_code,
    borough_name,
    dcp_geography_note_corrected_flag,
    exact_homeowner_share_1990_pct,
    approx_homeowner_share_1990_pct,
    homeowner_share_gap_pp,
    abs_homeowner_share_gap_pp,
    exact_treat_pp,
    approx_treat_pp,
    treat_gap_pp,
    abs_treat_gap_pp,
    exact_occupied_units_1990,
    approx_occupied_units_1990,
    occupied_units_gap
  )

write_csv_if_changed(comparison_df, out_comparison_csv)
write_csv_if_changed(summary_df, out_summary_csv)
write_csv_if_changed(top_gap_review_df, out_top_gap_review_csv)

cat("Wrote CD homeownership comparison outputs to", dirname(out_comparison_csv), "\n")
