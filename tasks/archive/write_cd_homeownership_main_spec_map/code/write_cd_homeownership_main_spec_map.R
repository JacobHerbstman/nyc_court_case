# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/write_cd_homeownership_main_spec_map/code")
# dcp_era_results_csv <- "../input/cd_homeownership_dcp_supply_era_interactions.csv"
# outcome_era_results_csv <- "../input/cd_homeownership_outcome_era_interactions.csv"
# out_spec_map_csv <- "../output/cd_homeownership_main_spec_map.csv"
# out_spec_map_md <- "../output/cd_homeownership_main_spec_map.md"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4) {
  stop("Expected 4 arguments: dcp_era_results_csv outcome_era_results_csv out_spec_map_csv out_spec_map_md")
}

dcp_era_results_csv <- args[1]
outcome_era_results_csv <- args[2]
out_spec_map_csv <- args[3]
out_spec_map_md <- args[4]

dcp_results <- read_csv(dcp_era_results_csv, show_col_types = FALSE, na = c("", "NA"))
outcome_results <- read_csv(outcome_era_results_csv, show_col_types = FALSE, na = c("", "NA"))

dob_original <- outcome_results %>%
  filter(
    outcome_family == "permit_nb_jobs",
    treatment_scale == "treat_z_boro"
  ) %>%
  select(
    era,
    estimate,
    std_error,
    p_value,
    conf_low,
    conf_high
  ) %>%
  mutate(spec_id = "dob_original_positive")

dcp_gross_add_uncontrolled <- dcp_results %>%
  filter(
    analysis_family == "dcp_primary",
    outcome_family == "gross_add_units",
    treatment_scale == "treat_z_boro",
    control_layer == "uncontrolled"
  ) %>%
  select(
    era,
    estimate,
    std_error,
    p_value,
    conf_low,
    conf_high
  ) %>%
  mutate(spec_id = "dcp_gross_add_main")

dcp_large_uncontrolled <- dcp_results %>%
  filter(
    analysis_family == "dcp_primary",
    outcome_family == "nb_gross_units_50_plus",
    treatment_scale == "treat_z_boro",
    control_layer == "uncontrolled"
  ) %>%
  select(
    era,
    estimate,
    std_error,
    p_value,
    conf_low,
    conf_high
  ) %>%
  mutate(spec_id = "dcp_large_50_plus_mechanism")

dcp_gross_add_controlled <- dcp_results %>%
  filter(
    analysis_family == "dcp_primary",
    outcome_family == "gross_add_units",
    treatment_scale == "treat_z_boro",
    control_layer == "controlled"
  ) %>%
  select(
    era,
    estimate,
    std_error,
    p_value,
    conf_low,
    conf_high
  ) %>%
  mutate(spec_id = "dcp_gross_add_controlled")

coef_rows <- bind_rows(
  dob_original,
  dcp_gross_add_uncontrolled,
  dcp_large_uncontrolled,
  dcp_gross_add_controlled
) %>%
  mutate(era_stub = gsub("-", "_", era, fixed = TRUE)) %>%
  select(spec_id, era_stub, estimate, std_error, p_value, conf_low, conf_high) %>%
  tidyr::pivot_wider(
    names_from = era_stub,
    values_from = c(estimate, std_error, p_value, conf_low, conf_high),
    names_glue = "{.value}_{era_stub}"
  )

spec_map <- tribble(
  ~spec_id, ~role, ~source_family, ~raw_source_level, ~regression_unit, ~sample_years, ~reference_era, ~outcome_family, ~outcome_label, ~treatment_scale, ~control_layer, ~fixed_effects, ~why_it_matters, ~reading,
  "dob_original_positive", "motivation", "DOB permit panel", "Permit/job records aggregated to CD-year", "CD-year", "1989-2025", "1989-1993", "permit_nb_jobs", "DOB new-building jobs within-borough share", "treat_z_boro", "uncontrolled", "CD FE + borough-year FE", "Shows the original positive building-count result.", "Structures/jobs do not fall in the simple early specification; this is the motivating contrast.",
  "dcp_gross_add_main", "headline", "DCP Housing Database", "Project records aggregated to CD-year", "CD-year", "2000-2025", "2000-2009", "gross_add_units", "Gross residential additions", "treat_z_boro", "uncontrolled", "CD FE + borough-year FE", "Best broad units outcome.", "High-homeownership CDs add fewer total units after 2010.",
  "dcp_large_50_plus_mechanism", "mechanism", "DCP Housing Database", "Project records aggregated to CD-year", "CD-year", "2000-2025", "2000-2009", "nb_gross_units_50_plus", "New-building gross units: 50+ unit projects", "treat_z_boro", "uncontrolled", "CD FE + borough-year FE", "Most direct test of large-project reallocation.", "The negative pattern is strongest for large, unit-intensive new buildings.",
  "dcp_gross_add_controlled", "stress_test", "DCP Housing Database", "Project records aggregated to CD-year", "CD-year", "2000-2025", "2000-2009", "gross_add_units", "Gross residential additions", "treat_z_boro", "controlled", "CD FE + borough-year FE", "Checks whether the broad unit result survives baseline controls.", "The sign stays negative, but precision drops sharply once baseline composition/developability controls are added."
) %>%
  left_join(coef_rows, by = "spec_id")

write_csv(spec_map, out_spec_map_csv, na = "")

fmt_num <- function(x, digits = 3) {
  ifelse(is.na(x), "", formatC(as.numeric(x), format = "f", digits = digits))
}

fmt_p <- function(x) {
  ifelse(is.na(x), "", formatC(as.numeric(x), format = "f", digits = 3))
}

spec_lines <- c(
  "# Main Spec Map: CD Homeownership and Housing Supply",
  "",
  "These are the four regressions to focus on.",
  "",
  "| Spec | Role | Outcome | Sample | Reference era | Controls | Key coefficients | Read |",
  "| --- | --- | --- | --- | --- | --- | --- | --- |",
  paste0(
    "| Original DOB building result | motivation | DOB new-building jobs within-borough share | 1989-2025 | 1989-1993 | none | ",
    "`1994-1999: ", fmt_num(spec_map$estimate_1994_1999[spec_map$spec_id == "dob_original_positive"], 4), "` (p=", fmt_p(spec_map$p_value_1994_1999[spec_map$spec_id == "dob_original_positive"]), "), ",
    "`2000-2009: ", fmt_num(spec_map$estimate_2000_2009[spec_map$spec_id == "dob_original_positive"], 4), "` (p=", fmt_p(spec_map$p_value_2000_2009[spec_map$spec_id == "dob_original_positive"]), "), ",
    "`2010-2019: ", fmt_num(spec_map$estimate_2010_2019[spec_map$spec_id == "dob_original_positive"], 4), "` (p=", fmt_p(spec_map$p_value_2010_2019[spec_map$spec_id == "dob_original_positive"]), ") | Positive building/job-share result in the original setup. |"
  ),
  paste0(
    "| DCP gross additions | headline | Gross residential additions | 2000-2025 | 2000-2009 | none | ",
    "`2010-2019: ", fmt_num(spec_map$estimate_2010_2019[spec_map$spec_id == "dcp_gross_add_main"], 1), "` (p=", fmt_p(spec_map$p_value_2010_2019[spec_map$spec_id == "dcp_gross_add_main"]), "), ",
    "`2020-2025: ", fmt_num(spec_map$estimate_2020_2025[spec_map$spec_id == "dcp_gross_add_main"], 1), "` (p=", fmt_p(spec_map$p_value_2020_2025[spec_map$spec_id == "dcp_gross_add_main"]), ") | Best broad units outcome; this is the main headline. |"
  ),
  paste0(
    "| DCP 50+ unit projects | mechanism | New-building gross units: 50+ unit projects | 2000-2025 | 2000-2009 | none | ",
    "`2010-2019: ", fmt_num(spec_map$estimate_2010_2019[spec_map$spec_id == "dcp_large_50_plus_mechanism"], 1), "` (p=", fmt_p(spec_map$p_value_2010_2019[spec_map$spec_id == "dcp_large_50_plus_mechanism"]), "), ",
    "`2020-2025: ", fmt_num(spec_map$estimate_2020_2025[spec_map$spec_id == "dcp_large_50_plus_mechanism"], 1), "` (p=", fmt_p(spec_map$p_value_2020_2025[spec_map$spec_id == "dcp_large_50_plus_mechanism"]), ") | Strongest evidence that composition shifts toward fewer large, unit-intensive projects. |"
  ),
  paste0(
    "| DCP gross additions, controlled | stress test | Gross residential additions | 2000-2025 | 2000-2009 | baseline controls x era | ",
    "`2010-2019: ", fmt_num(spec_map$estimate_2010_2019[spec_map$spec_id == "dcp_gross_add_controlled"], 1), "` (p=", fmt_p(spec_map$p_value_2010_2019[spec_map$spec_id == "dcp_gross_add_controlled"]), "), ",
    "`2020-2025: ", fmt_num(spec_map$estimate_2020_2025[spec_map$spec_id == "dcp_gross_add_controlled"], 1), "` (p=", fmt_p(spec_map$p_value_2020_2025[spec_map$spec_id == "dcp_gross_add_controlled"]), ") | Sign remains negative, but the result becomes much less precise. |"
  ),
  "",
  "Recommended reading order:",
  "",
  "1. Start with `dcp_gross_add_main`.",
  "2. Then check `dcp_large_50_plus_mechanism` to see whether the action is concentrated in large projects.",
  "3. Use `dcp_gross_add_controlled` as the main robustness/stress test.",
  "4. Keep `dob_original_positive` as motivation: project counts did not simply collapse, but the composition of supply appears to have shifted."
)

writeLines(spec_lines, out_spec_map_md)
