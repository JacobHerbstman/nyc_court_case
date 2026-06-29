setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/estimate_cd_homeownership_outcome_panels/code")
permit_panel_csv <- "../input/cd_homeownership_permit_nb_panel.csv"
dcp_units_panel_csv <- "../input/cd_homeownership_dcp_units_panel.csv"
out_results_csv <- "../output/cd_homeownership_outcome_era_interactions.csv"
out_summary_csv <- "../output/cd_homeownership_outcome_model_summary.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(fixest)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4) {
  stop("Expected 4 arguments: permit_panel_csv dcp_units_panel_csv out_results_csv out_summary_csv")
}

permit_panel_csv <- args[1]
dcp_units_panel_csv <- args[2]
out_results_csv <- args[3]
out_summary_csv <- args[4]

permit_panel <- read_csv(permit_panel_csv, show_col_types = FALSE, na = c("", "NA"))
dcp_units_panel <- read_csv(dcp_units_panel_csv, show_col_types = FALSE, na = c("", "NA"))

permit_reg_df <- permit_panel %>%
  filter(!is.na(borough_outcome_share)) %>%
  mutate(
    outcome_label = "DOB new-building jobs",
    era = case_when(
      year >= 1989 & year <= 1993 ~ "1989-1993",
      year >= 1994 & year <= 1999 ~ "1994-1999",
      year >= 2000 & year <= 2009 ~ "2000-2009",
      year >= 2010 & year <= 2019 ~ "2010-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    ),
    borough_year = interaction(borough_name, year, drop = TRUE),
    treat_pp_x_1994_1999 = treat_pp * as.integer(era == "1994-1999"),
    treat_pp_x_2000_2009 = treat_pp * as.integer(era == "2000-2009"),
    treat_pp_x_2010_2019 = treat_pp * as.integer(era == "2010-2019"),
    treat_pp_x_2020_2025 = treat_pp * as.integer(era == "2020-2025"),
    treat_z_boro_x_1994_1999 = treat_z_boro * as.integer(era == "1994-1999"),
    treat_z_boro_x_2000_2009 = treat_z_boro * as.integer(era == "2000-2009"),
    treat_z_boro_x_2010_2019 = treat_z_boro * as.integer(era == "2010-2019"),
    treat_z_boro_x_2020_2025 = treat_z_boro * as.integer(era == "2020-2025")
  ) %>%
  filter(!is.na(era))

dcp_reg_df <- dcp_units_panel %>%
  filter(year >= 2000, !is.na(borough_outcome_share)) %>%
  mutate(
    outcome_label = "DCP Housing Database net units",
    era = case_when(
      year >= 2000 & year <= 2009 ~ "2000-2009",
      year >= 2010 & year <= 2019 ~ "2010-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    ),
    borough_year = interaction(borough_name, year, drop = TRUE),
    treat_pp_x_2010_2019 = treat_pp * as.integer(era == "2010-2019"),
    treat_pp_x_2020_2025 = treat_pp * as.integer(era == "2020-2025"),
    treat_z_boro_x_2010_2019 = treat_z_boro * as.integer(era == "2010-2019"),
    treat_z_boro_x_2020_2025 = treat_z_boro * as.integer(era == "2020-2025")
  ) %>%
  filter(!is.na(era))

permit_pp_model <- feols(
  borough_outcome_share ~ treat_pp_x_1994_1999 + treat_pp_x_2000_2009 + treat_pp_x_2010_2019 + treat_pp_x_2020_2025 | borocd + borough_year,
  data = permit_reg_df,
  cluster = ~borocd
)

permit_z_model <- feols(
  borough_outcome_share ~ treat_z_boro_x_1994_1999 + treat_z_boro_x_2000_2009 + treat_z_boro_x_2010_2019 + treat_z_boro_x_2020_2025 | borocd + borough_year,
  data = permit_reg_df,
  cluster = ~borocd
)

dcp_pp_model <- feols(
  borough_outcome_share ~ treat_pp_x_2010_2019 + treat_pp_x_2020_2025 | borocd + borough_year,
  data = dcp_reg_df,
  cluster = ~borocd
)

dcp_z_model <- feols(
  borough_outcome_share ~ treat_z_boro_x_2010_2019 + treat_z_boro_x_2020_2025 | borocd + borough_year,
  data = dcp_reg_df,
  cluster = ~borocd
)

extract_model_rows <- function(model, outcome_family, outcome_label, treatment_scale, reference_era, year_min, year_max) {
  coef_table <- as.data.frame(coeftable(model))
  coef_table$term <- rownames(coef_table)
  rownames(coef_table) <- NULL

  confint_df <- as.data.frame(confint(model))
  confint_df$term <- rownames(confint_df)
  rownames(confint_df) <- NULL
  names(confint_df)[1:2] <- c("conf_low", "conf_high")

  tibble(term = coef_table$term) %>%
    left_join(coef_table, by = "term") %>%
    left_join(confint_df, by = "term") %>%
    mutate(
      outcome_family = outcome_family,
      outcome_label = outcome_label,
      treatment_scale = treatment_scale,
      reference_era = reference_era,
      era = str_replace(term, "^treat_(pp|z_boro)_x_", ""),
      era = str_replace_all(era, "_", "-"),
      coefficient_label = str_c(treatment_scale, " x ", era),
      sample_year_min = year_min,
      sample_year_max = year_max
    ) %>%
    transmute(
      outcome_family,
      outcome_label,
      treatment_scale,
      reference_era,
      era,
      coefficient_label,
      estimate = Estimate,
      std_error = `Std. Error`,
      statistic = `t value`,
      p_value = `Pr(>|t|)`,
      conf_low,
      conf_high,
      sample_year_min,
      sample_year_max
    )
}

results_df <- bind_rows(
  extract_model_rows(permit_pp_model, "permit_nb_jobs", "DOB new-building jobs", "treat_pp", "1989-1993", 1989, 2025),
  extract_model_rows(permit_z_model, "permit_nb_jobs", "DOB new-building jobs", "treat_z_boro", "1989-1993", 1989, 2025),
  extract_model_rows(dcp_pp_model, "dcp_net_units", "DCP Housing Database net units", "treat_pp", "2000-2009", 2000, 2025),
  extract_model_rows(dcp_z_model, "dcp_net_units", "DCP Housing Database net units", "treat_z_boro", "2000-2009", 2000, 2025)
) %>%
  arrange(outcome_family, treatment_scale, era)

summary_df <- bind_rows(
  tibble(
    outcome_family = "permit_nb_jobs",
    outcome_label = "DOB new-building jobs",
    treatment_scale = "treat_pp",
    reference_era = "1989-1993",
    nobs = nobs(permit_pp_model),
    district_count = n_distinct(permit_reg_df$borocd),
    year_count = n_distinct(permit_reg_df$year),
    borough_year_count = n_distinct(permit_reg_df$borough_year),
    outcome_mean = mean(permit_reg_df$borough_outcome_share, na.rm = TRUE),
    within_r2 = as.numeric(r2(permit_pp_model, type = "wr2"))
  ),
  tibble(
    outcome_family = "permit_nb_jobs",
    outcome_label = "DOB new-building jobs",
    treatment_scale = "treat_z_boro",
    reference_era = "1989-1993",
    nobs = nobs(permit_z_model),
    district_count = n_distinct(permit_reg_df$borocd),
    year_count = n_distinct(permit_reg_df$year),
    borough_year_count = n_distinct(permit_reg_df$borough_year),
    outcome_mean = mean(permit_reg_df$borough_outcome_share, na.rm = TRUE),
    within_r2 = as.numeric(r2(permit_z_model, type = "wr2"))
  ),
  tibble(
    outcome_family = "dcp_net_units",
    outcome_label = "DCP Housing Database net units",
    treatment_scale = "treat_pp",
    reference_era = "2000-2009",
    nobs = nobs(dcp_pp_model),
    district_count = n_distinct(dcp_reg_df$borocd),
    year_count = n_distinct(dcp_reg_df$year),
    borough_year_count = n_distinct(dcp_reg_df$borough_year),
    outcome_mean = mean(dcp_reg_df$borough_outcome_share, na.rm = TRUE),
    within_r2 = as.numeric(r2(dcp_pp_model, type = "wr2"))
  ),
  tibble(
    outcome_family = "dcp_net_units",
    outcome_label = "DCP Housing Database net units",
    treatment_scale = "treat_z_boro",
    reference_era = "2000-2009",
    nobs = nobs(dcp_z_model),
    district_count = n_distinct(dcp_reg_df$borocd),
    year_count = n_distinct(dcp_reg_df$year),
    borough_year_count = n_distinct(dcp_reg_df$borough_year),
    outcome_mean = mean(dcp_reg_df$borough_outcome_share, na.rm = TRUE),
    within_r2 = as.numeric(r2(dcp_z_model, type = "wr2"))
  )
)

write_csv_if_changed(results_df, out_results_csv)
write_csv_if_changed(summary_df, out_summary_csv)

cat("Wrote homeownership outcome regression outputs to", dirname(out_results_csv), "\n")
