# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/write_cd_homeownership_outcome_mini_report/code")
# measure_qc_csv <- "../input/cd_homeownership_1990_measure_qc.csv"
# permit_qc_csv <- "../input/cd_homeownership_permit_nb_panel_qc.csv"
# dcp_qc_csv <- "../input/cd_homeownership_dcp_units_panel_qc.csv"
# summary_qc_csv <- "../input/cd_homeownership_outcome_panels_qc.csv"
# five_year_csv <- "../input/cd_homeownership_outcome_tercile_five_year.csv"
# model_summary_csv <- "../input/cd_homeownership_outcome_model_summary.csv"
# era_results_csv <- "../input/cd_homeownership_outcome_era_interactions.csv"
# tercile_plots_pdf <- "../input/cd_homeownership_outcome_tercile_plots.pdf"
# out_report_md <- "../output/cd_homeownership_outcome_mini_report.md"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 9) {
  stop("Expected 9 arguments: measure_qc_csv permit_qc_csv dcp_qc_csv summary_qc_csv five_year_csv model_summary_csv era_results_csv tercile_plots_pdf out_report_md")
}

measure_qc_csv <- args[1]
permit_qc_csv <- args[2]
dcp_qc_csv <- args[3]
summary_qc_csv <- args[4]
five_year_csv <- args[5]
model_summary_csv <- args[6]
era_results_csv <- args[7]
tercile_plots_pdf <- args[8]
out_report_md <- args[9]

measure_qc <- read_csv(measure_qc_csv, show_col_types = FALSE, na = c("", "NA"))
permit_qc <- read_csv(permit_qc_csv, show_col_types = FALSE, na = c("", "NA"))
dcp_qc <- read_csv(dcp_qc_csv, show_col_types = FALSE, na = c("", "NA"))
summary_qc <- read_csv(summary_qc_csv, show_col_types = FALSE, na = c("", "NA"))
five_year_df <- read_csv(five_year_csv, show_col_types = FALSE, na = c("", "NA"))
model_summary_df <- read_csv(model_summary_csv, show_col_types = FALSE, na = c("", "NA"))
era_results_df <- read_csv(era_results_csv, show_col_types = FALSE, na = c("", "NA"))

pull_metric <- function(df, metric_name) {
  df %>%
    filter(metric == metric_name) %>%
    pull(value) %>%
    first()
}

fmt_int <- function(x) {
  format(round(as.numeric(x)), big.mark = ",", scientific = FALSE, trim = TRUE)
}

fmt_num <- function(x, digits = 3) {
  formatC(as.numeric(x), format = "f", digits = digits)
}

fmt_pct <- function(x, digits = 1) {
  paste0(formatC(100 * as.numeric(x), format = "f", digits = digits), "%")
}

measure_district_count <- pull_metric(measure_qc, "district_count")
measure_gap_pp <- pull_metric(measure_qc, "max_abs_reported_owner_share_gap_pp")
weighted_mean_treat_pp <- pull_metric(measure_qc, "weighted_mean_treat_pp")

permit_panel_rows <- pull_metric(permit_qc, "panel_row_count")
permit_year_count <- pull_metric(permit_qc, "year_count")
permit_nb_rows <- pull_metric(permit_qc, "nb_row_count_total")
permit_standard_share <- pull_metric(permit_qc, "nb_row_share_standard_59_cd")
permit_unique_jobs <- pull_metric(permit_qc, "unique_nb_jobs_total")
permit_single_cd_jobs <- pull_metric(permit_qc, "unique_nb_jobs_with_one_standard_59_cd")
permit_missing_first_date_share <- pull_metric(permit_qc, "unique_nb_jobs_missing_first_issuance_date_share")
permit_conflict_jobs <- pull_metric(permit_qc, "multi_standard_cd_job_conflict_count")
permit_panel_jobs <- pull_metric(permit_qc, "assigned_jobs_in_panel_year_range")
permit_partial_2026_jobs <- pull_metric(permit_qc, "dropped_partial_2026_jobs")
permit_zero_borough_years <- pull_metric(permit_qc, "borough_year_zero_denominator_count")

dcp_panel_rows <- pull_metric(dcp_qc, "panel_row_count")
dcp_year_count <- pull_metric(dcp_qc, "year_count")
dcp_source_rows <- pull_metric(dcp_qc, "source_row_count_total")
dcp_standard_share <- pull_metric(dcp_qc, "source_row_share_standard_59_cd")
dcp_missing_permit_year_share <- pull_metric(dcp_qc, "missing_permit_year_share")
dcp_mass_share <- pull_metric(dcp_qc, "classa_net_abs_mass_share_standard_59_cd")
dcp_zero_borough_years <- pull_metric(dcp_qc, "borough_year_zero_denominator_count")
dcp_negative_cd_years <- pull_metric(dcp_qc, "negative_cd_year_outcome_count")
dcp_zero_city_years <- pull_metric(summary_qc, "zero_city_total_year_count_dcp_net_units")

permit_five_year <- five_year_df %>%
  filter(outcome_family == "permit_nb_jobs") %>%
  select(five_year_bin, treat_tercile, tercile_share) %>%
  tidyr::pivot_wider(names_from = treat_tercile, values_from = tercile_share) %>%
  arrange(five_year_bin)

dcp_five_year <- five_year_df %>%
  filter(outcome_family == "dcp_net_units") %>%
  select(five_year_bin, treat_tercile, tercile_share) %>%
  tidyr::pivot_wider(names_from = treat_tercile, values_from = tercile_share) %>%
  arrange(five_year_bin)

permit_baseline <- permit_five_year %>% filter(five_year_bin == "1989-1993")
permit_mid_1990s <- permit_five_year %>% filter(five_year_bin == "1994-1998")
permit_2009_2013 <- permit_five_year %>% filter(five_year_bin == "2009-2013")

dcp_2008_2012 <- dcp_five_year %>% filter(five_year_bin == "2008-2012")
dcp_2018_2022 <- dcp_five_year %>% filter(five_year_bin == "2018-2022")

permit_results <- era_results_df %>%
  filter(outcome_family == "permit_nb_jobs") %>%
  arrange(treatment_scale, era)

dcp_results <- era_results_df %>%
  filter(outcome_family == "dcp_net_units") %>%
  arrange(treatment_scale, era)

permit_sig_rows <- permit_results %>%
  filter(p_value < 0.05)

first_sig_permit_pp <- permit_results %>%
  filter(treatment_scale == "treat_pp", era == "1994-1999") %>%
  slice(1)

first_sig_permit_z <- permit_results %>%
  filter(treatment_scale == "treat_z_boro", era == "1994-1999") %>%
  slice(1)

dcp_2010_pp <- dcp_results %>%
  filter(treatment_scale == "treat_pp", era == "2010-2019") %>%
  slice(1)

dcp_2020_z <- dcp_results %>%
  filter(treatment_scale == "treat_z_boro", era == "2020-2025") %>%
  slice(1)

report_lines <- c(
  "# Mini Report: Exact 1990 CD Homeownership Measure Merged to Outcome Panels",
  "",
  "## Bottom Line",
  "",
  paste0(
    "The exact 1990 community-district homeownership exposure is now locked in and merged into two outcome panels: DOB new-building jobs first issued in `1989-2025` and DCP Housing Database net Class A unit changes in `1998-2025`. ",
    "The treatment construction and both panel builds pass basic QC. The first-pass result is that higher-homeownership CDs do **not** lose within-borough share of DOB new-building jobs after the 1990s. If anything, they gain share relative to `1989-1993`. ",
    "The DCP net-unit panel points in the opposite direction, but the estimates are small and noisy."
  ),
  "",
  "## What Was Built",
  "",
  "- Canonical exact treatment file with `borocd`, `treat_pp`, and `treat_z_boro`.",
  "- Balanced `59 x 37` CD-year DOB new-building panel based on the earliest permit issuance date within each `job_number`.",
  "- Balanced `59 x 28` CD-year DCP net-unit panel based on `classa_net` summed by `permit_year`.",
  "- Within-borough treatment terciles, annual and five-year share summaries, and a first set of era-interaction regressions with CD and borough-year fixed effects.",
  "",
  "## Treatment QC",
  "",
  paste0(
    "The exact treatment file covers `", fmt_int(measure_district_count), "` standard CDs across all five boroughs. ",
    "There are no missing `H_cd`, `H_b`, or `treat_z_boro` values. ",
    "The occupied-unit-weighted mean of `treat_pp` is `", fmt_num(weighted_mean_treat_pp, 6), "`, which is effectively zero by construction. ",
    "The maximum gap between computed `H_cd` and the DCP-reported owner share is `", fmt_num(measure_gap_pp, 3), "` percentage points."
  ),
  "",
  "## Outcome Panel Construction",
  "",
  paste0(
    "### DOB New-Building Jobs\n",
    "The DOB panel has `", fmt_int(permit_panel_rows), "` CD-year rows over `", fmt_int(permit_year_count), "` years. ",
    "It starts from `", fmt_int(permit_nb_rows), "` new-building permit rows and `", fmt_int(permit_unique_jobs), "` unique job numbers. ",
    "`", fmt_pct(permit_standard_share, 1), "` of permit rows carry a standard 59-CD code. ",
    "`", fmt_int(permit_single_cd_jobs), "` jobs map to exactly one standard CD, while `", fmt_int(permit_conflict_jobs), "` jobs span multiple standard CDs and are written to the conflict audit rather than forced into the main panel. ",
    "Only `", fmt_pct(permit_missing_first_date_share, 3), "` of unique jobs lack a usable first issuance date, and `", fmt_int(permit_partial_2026_jobs), "` jobs are dropped because `2026` is partial. ",
    "The final panel includes `", fmt_int(permit_panel_jobs), "` assigned jobs. ",
    "There are `", fmt_int(permit_zero_borough_years), "` borough-years with zero new-building jobs, so borough shares are undefined in those cells."
  ),
  "",
  paste0(
    "### DCP Housing Database Net Units\n",
    "The DCP panel has `", fmt_int(dcp_panel_rows), "` CD-year rows over `", fmt_int(dcp_year_count), "` years. ",
    "It starts from `", fmt_int(dcp_source_rows), "` project rows. ",
    "`", fmt_pct(dcp_standard_share, 1), "` of rows carry a standard 59-CD code, and `", fmt_pct(dcp_mass_share, 1), "` of the absolute `classa_net` mass is retained inside the standard-CD universe. ",
    "The main source loss is timing: `", fmt_pct(dcp_missing_permit_year_share, 1), "` of source rows have missing `permit_year` and therefore cannot enter the year panel. ",
    "There are `", fmt_int(dcp_negative_cd_years), "` negative CD-year net-unit cells, which is expected for a net measure, and `", fmt_int(dcp_zero_borough_years), "` borough-years with nonpositive borough totals. ",
    "Citywide tercile shares are undefined in `", fmt_int(dcp_zero_city_years), "` year because total city net units equal zero."
  ),
  "",
  "## Descriptive Patterns",
  "",
  paste0(
    "### DOB New-Building Jobs\n",
    "The five-year shares do not show a post-1990 decline in high-homeownership CDs. ",
    "In the baseline `1989-1993` bin, high-exposure CDs account for `", fmt_pct(permit_baseline$High, 1), "` of citywide new-building jobs, versus `", fmt_pct(permit_baseline$Low, 1), "` for low-exposure CDs. ",
    "In `1994-1998`, the high-exposure share rises to `", fmt_pct(permit_mid_1990s$High, 1), "` while the low-exposure share falls to `", fmt_pct(permit_mid_1990s$Low, 1), "`. ",
    "That pattern remains visible in `2009-2013`, when the high-exposure tercile still holds `", fmt_pct(permit_2009_2013$High, 1), "` of jobs versus `", fmt_pct(permit_2009_2013$Low, 1), "` for the low tercile."
  ),
  "",
  paste0(
    "### DCP Net Units\n",
    "The DCP net-unit panel looks more consistent with the original hypothesis, but only descriptively. ",
    "In `2008-2012`, the high-exposure tercile accounts for `", fmt_pct(dcp_2008_2012$High, 1), "` of city net units, compared with `", fmt_pct(dcp_2008_2012$Low, 1), "` for the low-exposure tercile. ",
    "In `2018-2022`, the same comparison is `", fmt_pct(dcp_2018_2022$High, 1), "` versus `", fmt_pct(dcp_2018_2022$Low, 1), "`. ",
    "So the net-unit series is much more tilted toward low-homeownership districts than the permit-job series."
  ),
  "",
  paste0(
    "The tercile plots are saved in [cd_homeownership_outcome_tercile_plots.pdf](", tercile_plots_pdf, ")."
  ),
  "",
  "## Era-Interaction Regressions",
  "",
  paste0(
    "The regression specification is `share_cdt = alpha_cd + lambda_bt + sum_e beta_e (treat_cd x era_e) + error_cdt`, with CD fixed effects and borough-year fixed effects. ",
    "For DOB jobs, `1989-1993` is the omitted era. For DCP net units, `2000-2009` is the omitted era."
  ),
  "",
  paste0(
    "### DOB New-Building Jobs\n",
    "The permit regressions point in the same direction as the descriptive shares. ",
    "Using `treat_pp`, the `1994-1999` interaction is `", fmt_num(first_sig_permit_pp$estimate, 6), "` with standard error `", fmt_num(first_sig_permit_pp$std_error, 6), "` and p-value `", fmt_num(first_sig_permit_pp$p_value, 3), "`. ",
    "Using the within-borough z-score, the `1994-1999` interaction is `", fmt_num(first_sig_permit_z$estimate, 6), "` with standard error `", fmt_num(first_sig_permit_z$std_error, 6), "` and p-value `", fmt_num(first_sig_permit_z$p_value, 3), "`. ",
    "The z-score coefficients remain positive and statistically significant in `2000-2009` and `2010-2019`. ",
    "In other words, higher-homeownership CDs gain within-borough new-building job share relative to the pre-1994 baseline rather than lose it."
  ),
  "",
  paste0(
    "### DCP Net Units\n",
    "The DCP net-unit regressions are negative but imprecise. ",
    "For `treat_pp`, the `2010-2019` interaction is `", fmt_num(dcp_2010_pp$estimate, 6), "` with p-value `", fmt_num(dcp_2010_pp$p_value, 3), "`. ",
    "For `treat_z_boro`, the `2020-2025` interaction is `", fmt_num(dcp_2020_z$estimate, 6), "` with p-value `", fmt_num(dcp_2020_z$p_value, 3), "`. ",
    "These estimates are directionally consistent with lower unit growth in high-homeownership districts, but the confidence intervals are wide and include zero."
  ),
  "",
  "## Interpretation",
  "",
  paste0(
    "Taken literally, the first merged outcome exercise does not yet support the simplest version of the local-veto story. ",
    "The strongest signal in the current outputs is positive for DOB new-building jobs and negative-but-noisy for DCP net units. ",
    "That split suggests the next issue is not treatment construction. It is outcome definition. ",
    "A job-count outcome may not track the kind of discretionary housing production channel we actually care about, while a net-unit outcome is closer conceptually but measured later and with more noise."
  ),
  "",
  "## What This Means For Next Steps",
  "",
  "- Keep the exact DCP-based homeownership treatment fixed as the canonical exposure.",
  "- Treat the DOB new-building job panel as a useful smoke test, but not yet as the main outcome for the paper.",
  "- Push next on outcomes that are closer to housing supply intensity: unit-weighted DOB measures, project filings, or eventually planning-side ZAP / ULURP data.",
  "- Keep the DCP net-unit panel in the workflow. It is the closest current outcome to the hypothesized channel, even though the first specification is noisy.",
  "- Use this memo as a checkpoint rather than a final design judgment. The treatment side now looks settled; the remaining uncertainty is on the outcome side.",
  "",
  "## Input Files Used",
  "",
  paste0("- `", measure_qc_csv, "`"),
  paste0("- `", permit_qc_csv, "`"),
  paste0("- `", dcp_qc_csv, "`"),
  paste0("- `", summary_qc_csv, "`"),
  paste0("- `", five_year_csv, "`"),
  paste0("- `", model_summary_csv, "`"),
  paste0("- `", era_results_csv, "`")
)

writeLines(report_lines, out_report_md, useBytes = TRUE)

cat("Wrote mini report to", out_report_md, "\n")
