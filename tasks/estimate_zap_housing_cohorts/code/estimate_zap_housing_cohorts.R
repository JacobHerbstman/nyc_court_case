# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/estimate_zap_housing_cohorts/code")
# zap_housing_initial_panel_csv <- "../input/zap_housing_initial_panel.csv"
# zap_housing_mature_cohort_panel_csv <- "../input/zap_housing_mature_cohort_panel.csv"
# out_results_csv <- "../output/zap_housing_era_interactions.csv"
# out_summary_csv <- "../output/zap_housing_model_summary.csv"

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
  stop("Expected 4 arguments: zap_housing_initial_panel_csv zap_housing_mature_cohort_panel_csv out_results_csv out_summary_csv")
}

zap_housing_initial_panel_csv <- args[1]
zap_housing_mature_cohort_panel_csv <- args[2]
out_results_csv <- args[3]
out_summary_csv <- args[4]

z_score <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  spread <- stats::sd(x, na.rm = TRUE)

  if (is.na(spread) || spread == 0) {
    return(rep(NA_real_, length(x)))
  }

  (x - mean(x, na.rm = TRUE)) / spread
}

add_era_interactions <- function(df, variable_names, era_values) {
  out_df <- df

  for (variable_name in variable_names) {
    for (era_value in era_values) {
      out_df[[paste0(variable_name, "_x_", str_replace_all(era_value, "-", "_"))]] <- out_df[[variable_name]] * as.integer(out_df$era == era_value)
    }
  }

  out_df
}

extract_model_rows <- function(model, outcome_family, outcome_label, control_layer, sample_label, reference_era, sample_year_min, sample_year_max, term_names, eras) {
  coef_table <- as.data.frame(coeftable(model))
  coef_table$term <- rownames(coef_table)
  rownames(coef_table) <- NULL

  confint_df <- as.data.frame(confint(model))
  confint_df$term <- rownames(confint_df)
  rownames(confint_df) <- NULL
  names(confint_df)[1:2] <- c("conf_low", "conf_high")

  tibble(term = term_names, era = eras) %>%
    left_join(coef_table, by = "term") %>%
    left_join(confint_df, by = "term") %>%
    transmute(
      outcome_family,
      outcome_label,
      control_layer,
      sample_label,
      reference_era,
      era,
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

initial_panel <- read_csv(zap_housing_initial_panel_csv, show_col_types = FALSE, na = c("", "NA"))
mature_panel <- read_csv(zap_housing_mature_cohort_panel_csv, show_col_types = FALSE, na = c("", "NA"))

control_lookup <- initial_panel %>%
  distinct(borocd, across(all_of(static_control_cols))) %>%
  transmute(
    borocd,
    vacancy_rate_1990_exact_z = z_score(vacancy_rate_1990_exact),
    structure_share_1_2_units_1990_exact_z = z_score(structure_share_1_2_units_1990_exact),
    structure_share_3_4_units_1990_exact_z = z_score(structure_share_3_4_units_1990_exact),
    structure_share_5_plus_units_1990_exact_z = z_score(structure_share_5_plus_units_1990_exact),
    median_household_income_1990_1999_dollars_exact_z = z_score(median_household_income_1990_1999_dollars_exact),
    poverty_share_1990_exact_z = z_score(poverty_share_1990_exact),
    median_housing_value_1990_2000_dollars_exact_filled_z = z_score(median_housing_value_1990_2000_dollars_exact_filled),
    median_housing_value_1990_missing_exact_static = suppressWarnings(as.integer(median_housing_value_1990_missing_exact)),
    subway_commute_share_1990_exact_z = z_score(subway_commute_share_1990_exact),
    mean_commute_time_1990_minutes_exact_z = z_score(mean_commute_time_1990_minutes_exact),
    foreign_born_share_1990_exact_z = z_score(foreign_born_share_1990_exact),
    college_graduate_share_1990_exact_z = z_score(college_graduate_share_1990_exact),
    unemployment_rate_1990_exact_z = z_score(unemployment_rate_1990_exact)
  )

static_control_vars <- c(
  "vacancy_rate_1990_exact_z",
  "structure_share_1_2_units_1990_exact_z",
  "structure_share_3_4_units_1990_exact_z",
  "structure_share_5_plus_units_1990_exact_z",
  "median_household_income_1990_1999_dollars_exact_z",
  "poverty_share_1990_exact_z",
  "median_housing_value_1990_2000_dollars_exact_filled_z",
  "median_housing_value_1990_missing_exact_static",
  "subway_commute_share_1990_exact_z",
  "mean_commute_time_1990_minutes_exact_z",
  "foreign_born_share_1990_exact_z",
  "college_graduate_share_1990_exact_z",
  "unemployment_rate_1990_exact_z"
)

count_eras <- c("1976-1979", "1985-1989", "1990-1999", "2000-2009", "2010-2019", "2020-2025")
share_eras <- c("1976-1979", "1985-1989", "1990-1999", "2000-2009", "2010-2015")

count_df <- initial_panel %>%
  left_join(control_lookup, by = "borocd") %>%
  mutate(
    outcome_value = initial_apps,
    era = case_when(
      cert_year >= 1976 & cert_year <= 1979 ~ "1976-1979",
      cert_year >= 1980 & cert_year <= 1984 ~ "1980-1984",
      cert_year >= 1985 & cert_year <= 1989 ~ "1985-1989",
      cert_year >= 1990 & cert_year <= 1999 ~ "1990-1999",
      cert_year >= 2000 & cert_year <= 2009 ~ "2000-2009",
      cert_year >= 2010 & cert_year <= 2019 ~ "2010-2019",
      cert_year >= 2020 & cert_year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    ),
    borough_year = interaction(borough_name, cert_year, drop = TRUE)
  ) %>%
  filter(!is.na(era))

completion_df <- mature_panel %>%
  filter(initial_apps > 0) %>%
  left_join(control_lookup, by = "borocd") %>%
  mutate(
    outcome_value = completion_share,
    era = case_when(
      cert_year >= 1976 & cert_year <= 1979 ~ "1976-1979",
      cert_year >= 1980 & cert_year <= 1984 ~ "1980-1984",
      cert_year >= 1985 & cert_year <= 1989 ~ "1985-1989",
      cert_year >= 1990 & cert_year <= 1999 ~ "1990-1999",
      cert_year >= 2000 & cert_year <= 2009 ~ "2000-2009",
      cert_year >= 2010 & cert_year <= 2015 ~ "2010-2015",
      TRUE ~ NA_character_
    ),
    borough_year = interaction(borough_name, cert_year, drop = TRUE)
  ) %>%
  filter(!is.na(era))

failure_df <- mature_panel %>%
  filter(initial_apps > 0) %>%
  left_join(control_lookup, by = "borocd") %>%
  mutate(
    outcome_value = failure_share,
    era = case_when(
      cert_year >= 1976 & cert_year <= 1979 ~ "1976-1979",
      cert_year >= 1980 & cert_year <= 1984 ~ "1980-1984",
      cert_year >= 1985 & cert_year <= 1989 ~ "1985-1989",
      cert_year >= 1990 & cert_year <= 1999 ~ "1990-1999",
      cert_year >= 2000 & cert_year <= 2009 ~ "2000-2009",
      cert_year >= 2010 & cert_year <= 2015 ~ "2010-2015",
      TRUE ~ NA_character_
    ),
    borough_year = interaction(borough_name, cert_year, drop = TRUE)
  ) %>%
  filter(!is.na(era))

specs <- tribble(
  ~outcome_family, ~outcome_label, ~sample_label, ~sample_year_min, ~sample_year_max, ~weight_var, ~era_values,
  "initial_apps", "Initial housing ULURP applications", "all_certifications", 1976, 2025, NA_character_, list(count_eras),
  "completion_share", "Completion share among mature housing ULURP cohorts", "mature_cohorts", 1976, 2015, "initial_apps", list(share_eras),
  "failure_share", "Failure share among mature housing ULURP cohorts", "mature_cohorts", 1976, 2015, "initial_apps", list(share_eras)
)

data_lookup <- list(
  initial_apps = count_df,
  completion_share = completion_df,
  failure_share = failure_df
)

results_rows <- list()
summary_rows <- list()
result_idx <- 1L
summary_idx <- 1L

for (spec_idx in seq_len(nrow(specs))) {
  outcome_family <- specs$outcome_family[spec_idx]
  outcome_label <- specs$outcome_label[spec_idx]
  sample_label <- specs$sample_label[spec_idx]
  sample_year_min <- specs$sample_year_min[spec_idx]
  sample_year_max <- specs$sample_year_max[spec_idx]
  weight_var <- specs$weight_var[spec_idx]
  era_values <- unlist(specs$era_values[[spec_idx]], use.names = FALSE)

  work_df <- data_lookup[[outcome_family]]
  treat_terms <- paste0("treat_z_boro_x_", str_replace_all(era_values, "-", "_"))

  uncontrolled_df <- add_era_interactions(work_df, "treat_z_boro", era_values)
  uncontrolled_formula <- as.formula(
    paste("outcome_value ~", paste(treat_terms, collapse = " + "), "| borocd + borough_year")
  )

  model_uncontrolled <- if (is.na(weight_var)) {
    feols(uncontrolled_formula, data = uncontrolled_df, cluster = ~borocd)
  } else {
    feols(uncontrolled_formula, data = uncontrolled_df, weights = as.formula(paste0("~", weight_var)), cluster = ~borocd)
  }

  results_rows[[result_idx]] <- extract_model_rows(
    model_uncontrolled,
    outcome_family = outcome_family,
    outcome_label = outcome_label,
    control_layer = "uncontrolled",
    sample_label = sample_label,
    reference_era = "1980-1984",
    sample_year_min = sample_year_min,
    sample_year_max = sample_year_max,
    term_names = treat_terms,
    eras = era_values
  )
  result_idx <- result_idx + 1L

  summary_rows[[summary_idx]] <- tibble(
    outcome_family = outcome_family,
    outcome_label = outcome_label,
    control_layer = "uncontrolled",
    sample_label = sample_label,
    nobs = nobs(model_uncontrolled),
    positive_outcome_share = mean(work_df$outcome_value > 0, na.rm = TRUE),
    outcome_mean = mean(work_df$outcome_value, na.rm = TRUE),
    weight_sum = if (is.na(weight_var)) NA_real_ else sum(work_df[[weight_var]], na.rm = TRUE)
  )
  summary_idx <- summary_idx + 1L

  controlled_df <- add_era_interactions(uncontrolled_df, static_control_vars, era_values)
  control_terms <- unlist(lapply(static_control_vars, function(x) paste0(x, "_x_", str_replace_all(era_values, "-", "_"))))
  controlled_formula <- as.formula(
    paste("outcome_value ~", paste(c(treat_terms, control_terms), collapse = " + "), "| borocd + borough_year")
  )

  model_controlled <- if (is.na(weight_var)) {
    feols(controlled_formula, data = controlled_df, cluster = ~borocd)
  } else {
    feols(controlled_formula, data = controlled_df, weights = as.formula(paste0("~", weight_var)), cluster = ~borocd)
  }

  results_rows[[result_idx]] <- extract_model_rows(
    model_controlled,
    outcome_family = outcome_family,
    outcome_label = outcome_label,
    control_layer = "static_1990",
    sample_label = sample_label,
    reference_era = "1980-1984",
    sample_year_min = sample_year_min,
    sample_year_max = sample_year_max,
    term_names = treat_terms,
    eras = era_values
  )
  result_idx <- result_idx + 1L

  summary_rows[[summary_idx]] <- tibble(
    outcome_family = outcome_family,
    outcome_label = outcome_label,
    control_layer = "static_1990",
    sample_label = sample_label,
    nobs = nobs(model_controlled),
    positive_outcome_share = mean(work_df$outcome_value > 0, na.rm = TRUE),
    outcome_mean = mean(work_df$outcome_value, na.rm = TRUE),
    weight_sum = if (is.na(weight_var)) NA_real_ else sum(work_df[[weight_var]], na.rm = TRUE)
  )
  summary_idx <- summary_idx + 1L
}

results_df <- bind_rows(results_rows) %>%
  arrange(outcome_family, control_layer, era)

summary_df <- bind_rows(summary_rows) %>%
  arrange(outcome_family, control_layer)

write_csv_if_changed(results_df, out_results_csv)
write_csv_if_changed(summary_df, out_summary_csv)

cat("Wrote ZAP housing cohort regression outputs to", dirname(out_results_csv), "\n")
