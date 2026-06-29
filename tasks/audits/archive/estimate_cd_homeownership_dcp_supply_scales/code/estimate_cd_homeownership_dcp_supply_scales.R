# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/estimate_cd_homeownership_dcp_supply_scales/code")
# cd_homeownership_dcp_supply_panel_csv <- "../input/cd_homeownership_dcp_supply_panel.csv"
# cd_baseline_1990_controls_csv <- "../input/cd_baseline_1990_controls.csv"
# out_results_csv <- "../output/cd_homeownership_dcp_supply_scale_results.csv"
# out_summary_csv <- "../output/cd_homeownership_dcp_supply_scale_model_summary.csv"
# out_eligibility_csv <- "../output/cd_homeownership_dcp_supply_transform_eligibility.csv"
# out_report_md <- "../output/cd_homeownership_dcp_supply_scale_summary.md"

suppressPackageStartupMessages({
  library(dplyr)
  library(fixest)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 6) {
  stop("Expected 6 arguments: cd_homeownership_dcp_supply_panel_csv cd_baseline_1990_controls_csv out_results_csv out_summary_csv out_eligibility_csv out_report_md")
}

cd_homeownership_dcp_supply_panel_csv <- args[1]
cd_baseline_1990_controls_csv <- args[2]
out_results_csv <- args[3]
out_summary_csv <- args[4]
out_eligibility_csv <- args[5]
out_report_md <- args[6]

z_score <- function(x) {
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

extract_model_rows <- function(model, outcome_family, outcome_label, model_family, control_layer, term_names, eras, transform_scale, sample_year_min, sample_year_max) {
  coef_table <- as.data.frame(coeftable(model))
  coef_table$term <- rownames(coef_table)
  rownames(coef_table) <- NULL
  coef_table$statistic <- if ("z value" %in% names(coef_table)) coef_table$`z value` else coef_table$`t value`
  coef_table$p_value <- if ("Pr(>|z|)" %in% names(coef_table)) coef_table$`Pr(>|z|)` else coef_table$`Pr(>|t|)`

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
      model_family,
      control_layer,
      transform_scale,
      reference_era = "2000-2009",
      era,
      estimate = Estimate,
      std_error = `Std. Error`,
      statistic,
      p_value,
      conf_low,
      conf_high,
      pct_effect = ifelse(
        model_family %in% c("log_ols", "ppml"),
        100 * (exp(estimate) - 1),
        NA_real_
      ),
      pct_effect_conf_low = ifelse(
        model_family %in% c("log_ols", "ppml"),
        100 * (exp(conf_low) - 1),
        NA_real_
      ),
      pct_effect_conf_high = ifelse(
        model_family %in% c("log_ols", "ppml"),
        100 * (exp(conf_high) - 1),
        NA_real_
      ),
      sample_year_min,
      sample_year_max
    )
}

supply_panel <- read_csv(cd_homeownership_dcp_supply_panel_csv, show_col_types = FALSE, na = c("", "NA"))
baseline_controls <- read_csv(cd_baseline_1990_controls_csv, show_col_types = FALSE, na = c("", "NA"))

main_outcomes <- tribble(
  ~outcome_family, ~outcome_label,
  "gross_add_units", "Gross residential additions",
  "nb_gross_units_50_plus", "New-building gross units: 50+ unit projects",
  "nb_project_count", "New-building projects"
)

eligibility_df <- supply_panel %>%
  filter(year >= 2000, year <= 2025) %>%
  group_by(outcome_family, outcome_label) %>%
  summarise(
    min_outcome = min(outcome_value, na.rm = TRUE),
    max_outcome = max(outcome_value, na.rm = TRUE),
    zero_share = mean(outcome_value == 0, na.rm = TRUE),
    negative_share = mean(outcome_value < 0, na.rm = TRUE),
    positive_n = sum(outcome_value > 0, na.rm = TRUE),
    total_n = n(),
    log_ols_eligible = min_outcome >= 0 && positive_n > 0,
    ppml_eligible = min_outcome >= 0,
    .groups = "drop"
  ) %>%
  mutate(
    eligibility_note = case_when(
      negative_share > 0 ~ "Contains negative values; do not use log OLS or PPML.",
      zero_share > 0 ~ "Nonnegative with zeros; log OLS is possible after dropping zeros, PPML keeps zeros.",
      TRUE ~ "Strictly positive; both log OLS and PPML are admissible."
    )
  ) %>%
  arrange(outcome_family)

static_control_lookup <- baseline_controls %>%
  transmute(
    borocd = as.integer(borocd),
    vacancy_rate_1990_exact_z = z_score(vacancy_rate_1990_exact),
    structure_share_1_2_units_1990_exact_z = z_score(structure_share_1_2_units_1990_exact),
    structure_share_3_4_units_1990_exact_z = z_score(structure_share_3_4_units_1990_exact),
    structure_share_5_plus_units_1990_exact_z = z_score(structure_share_5_plus_units_1990_exact),
    median_household_income_1990_1999_dollars_exact_z = z_score(median_household_income_1990_1999_dollars_exact),
    poverty_share_1990_exact_z = z_score(poverty_share_1990_exact),
    median_housing_value_1990_2000_dollars_exact_filled_z = z_score(median_housing_value_1990_2000_dollars_exact_filled),
    median_housing_value_1990_missing_exact_static = as.integer(median_housing_value_1990_missing_exact),
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

reg_df <- supply_panel %>%
  filter(year >= 2000, year <= 2025, outcome_family %in% main_outcomes$outcome_family) %>%
  left_join(static_control_lookup, by = "borocd") %>%
  mutate(
    era = case_when(
      year >= 2000 & year <= 2009 ~ "2000-2009",
      year >= 2010 & year <= 2019 ~ "2010-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    ),
    borough_year = interaction(borough_name, year, drop = TRUE)
  ) %>%
  filter(!is.na(era))

results_rows <- list()
summary_rows <- list()
result_idx <- 1L
summary_idx <- 1L

for (outcome_idx in seq_len(nrow(main_outcomes))) {
  outcome_family <- main_outcomes$outcome_family[outcome_idx]
  outcome_label <- main_outcomes$outcome_label[outcome_idx]
  outcome_df <- reg_df %>%
    filter(outcome_family == !!outcome_family)

  treat_terms <- paste0("treat_z_boro_x_", c("2010_2019", "2020_2025"))
  base_df <- add_era_interactions(outcome_df, "treat_z_boro", c("2010-2019", "2020-2025"))

  level_uncontrolled <- feols(
    outcome_value ~ treat_z_boro_x_2010_2019 + treat_z_boro_x_2020_2025 | borocd + borough_year,
    data = base_df,
    cluster = ~borocd
  )

  results_rows[[result_idx]] <- extract_model_rows(
    model = level_uncontrolled,
    outcome_family = outcome_family,
    outcome_label = outcome_label,
    model_family = "level_ols",
    control_layer = "uncontrolled",
    term_names = treat_terms,
    eras = c("2010-2019", "2020-2025"),
    transform_scale = "levels",
    sample_year_min = 2000,
    sample_year_max = 2025
  )
  result_idx <- result_idx + 1L

  summary_rows[[summary_idx]] <- tibble(
    outcome_family = outcome_family,
    outcome_label = outcome_label,
    model_family = "level_ols",
    control_layer = "uncontrolled",
    transform_scale = "levels",
    nobs = nobs(level_uncontrolled),
    positive_n = sum(base_df$outcome_value > 0, na.rm = TRUE),
    zero_share = mean(base_df$outcome_value == 0, na.rm = TRUE),
    outcome_mean = mean(base_df$outcome_value, na.rm = TRUE)
  )
  summary_idx <- summary_idx + 1L

  controlled_df <- add_era_interactions(base_df, static_control_vars, c("2010-2019", "2020-2025"))
  control_terms <- unlist(lapply(static_control_vars, function(x) paste0(x, "_x_", c("2010_2019", "2020_2025"))))
  controlled_formula <- as.formula(
    paste("outcome_value ~", paste(c(treat_terms, control_terms), collapse = " + "), "| borocd + borough_year")
  )

  level_static <- feols(
    controlled_formula,
    data = controlled_df,
    cluster = ~borocd
  )

  results_rows[[result_idx]] <- extract_model_rows(
    model = level_static,
    outcome_family = outcome_family,
    outcome_label = outcome_label,
    model_family = "level_ols",
    control_layer = "static_1990",
    term_names = treat_terms,
    eras = c("2010-2019", "2020-2025"),
    transform_scale = "levels",
    sample_year_min = 2000,
    sample_year_max = 2025
  )
  result_idx <- result_idx + 1L

  summary_rows[[summary_idx]] <- tibble(
    outcome_family = outcome_family,
    outcome_label = outcome_label,
    model_family = "level_ols",
    control_layer = "static_1990",
    transform_scale = "levels",
    nobs = nobs(level_static),
    positive_n = sum(controlled_df$outcome_value > 0, na.rm = TRUE),
    zero_share = mean(controlled_df$outcome_value == 0, na.rm = TRUE),
    outcome_mean = mean(controlled_df$outcome_value, na.rm = TRUE)
  )
  summary_idx <- summary_idx + 1L

  eligibility_row <- eligibility_df %>% filter(outcome_family == !!outcome_family)

  if (isTRUE(eligibility_row$log_ols_eligible[[1]])) {
    log_df <- controlled_df %>%
      filter(outcome_value > 0) %>%
      mutate(log_outcome_value = log(outcome_value))

    log_model <- feols(
      as.formula(
        paste("log_outcome_value ~", paste(c(treat_terms, control_terms), collapse = " + "), "| borocd + borough_year")
      ),
      data = log_df,
      cluster = ~borocd
    )

    results_rows[[result_idx]] <- extract_model_rows(
      model = log_model,
      outcome_family = outcome_family,
      outcome_label = outcome_label,
      model_family = "log_ols",
      control_layer = "static_1990",
      term_names = treat_terms,
      eras = c("2010-2019", "2020-2025"),
      transform_scale = "log_positive_only",
      sample_year_min = 2000,
      sample_year_max = 2025
    )
    result_idx <- result_idx + 1L

    summary_rows[[summary_idx]] <- tibble(
      outcome_family = outcome_family,
      outcome_label = outcome_label,
      model_family = "log_ols",
      control_layer = "static_1990",
      transform_scale = "log_positive_only",
      nobs = nobs(log_model),
      positive_n = nrow(log_df),
      zero_share = mean(controlled_df$outcome_value == 0, na.rm = TRUE),
      outcome_mean = mean(log_df$outcome_value, na.rm = TRUE)
    )
    summary_idx <- summary_idx + 1L
  }

  if (isTRUE(eligibility_row$ppml_eligible[[1]])) {
    ppml_model <- fepois(
      controlled_formula,
      data = controlled_df,
      cluster = ~borocd
    )

    results_rows[[result_idx]] <- extract_model_rows(
      model = ppml_model,
      outcome_family = outcome_family,
      outcome_label = outcome_label,
      model_family = "ppml",
      control_layer = "static_1990",
      term_names = treat_terms,
      eras = c("2010-2019", "2020-2025"),
      transform_scale = "multiplicative",
      sample_year_min = 2000,
      sample_year_max = 2025
    )
    result_idx <- result_idx + 1L

    summary_rows[[summary_idx]] <- tibble(
      outcome_family = outcome_family,
      outcome_label = outcome_label,
      model_family = "ppml",
      control_layer = "static_1990",
      transform_scale = "multiplicative",
      nobs = nobs(ppml_model),
      positive_n = sum(controlled_df$outcome_value > 0, na.rm = TRUE),
      zero_share = mean(controlled_df$outcome_value == 0, na.rm = TRUE),
      outcome_mean = mean(controlled_df$outcome_value, na.rm = TRUE)
    )
    summary_idx <- summary_idx + 1L
  }
}

results_df <- bind_rows(results_rows) %>%
  arrange(outcome_family, model_family, control_layer, era)

summary_df <- bind_rows(summary_rows) %>%
  arrange(outcome_family, model_family, control_layer)

fmt_num <- function(x, digits = 3) {
  formatC(as.numeric(x), format = "f", digits = digits)
}

fmt_pct <- function(x, digits = 1) {
  paste0(formatC(as.numeric(x), format = "f", digits = digits), "%")
}

pull_result <- function(outcome_key, model_key, control_key, era_key, value_name) {
  results_df %>%
    filter(
      outcome_family == outcome_key,
      model_family == model_key,
      control_layer == control_key,
      era == era_key
    ) %>%
    pull(.data[[value_name]]) %>%
    first()
}

report_lines <- c(
  "# Static-1990 Control And Outcome-Scale Check",
  "",
  "This file isolates the main DCP outcomes and compares level OLS, log OLS, and PPML.",
  "",
  "## Static 1990 Control Set",
  "",
  "The `static_1990` specification uses only exact 1990 CD-level controls from the DCP profile source:",
  "",
  "- vacancy rate",
  "- housing structure mix (`1-2`, `3-4`, `5+` shares)",
  "- median household income",
  "- poverty share",
  "- median housing value plus a missing-value flag",
  "- subway commute share",
  "- mean commute time",
  "- foreign-born share",
  "- college-graduate share",
  "- unemployment rate",
  "",
  "No `1980-1990` change controls are used in these models.",
  "",
  "## Outcome Eligibility",
  ""
)

for (i in seq_len(nrow(eligibility_df))) {
  report_lines <- c(
    report_lines,
    paste0(
      "- `", eligibility_df$outcome_family[i], "`: zero share `", fmt_pct(100 * eligibility_df$zero_share[i], 1), 
      "`, negative share `", fmt_pct(100 * eligibility_df$negative_share[i], 1),
      "`, log eligible = `", eligibility_df$log_ols_eligible[i],
      "`, PPML eligible = `", eligibility_df$ppml_eligible[i], "`. ",
      eligibility_df$eligibility_note[i]
    )
  )
}

report_lines <- c(
  report_lines,
  "",
  "## Main Outcomes",
  "",
  paste0(
    "### Gross Residential Additions\n",
    "- Level OLS, no controls: `2010-2019 = ", fmt_num(pull_result("gross_add_units", "level_ols", "uncontrolled", "2010-2019", "estimate"), 1), "`; `2020-2025 = ", fmt_num(pull_result("gross_add_units", "level_ols", "uncontrolled", "2020-2025", "estimate"), 1), "`.\n",
    "- Level OLS, static 1990 controls: `2010-2019 = ", fmt_num(pull_result("gross_add_units", "level_ols", "static_1990", "2010-2019", "estimate"), 1), "`; `2020-2025 = ", fmt_num(pull_result("gross_add_units", "level_ols", "static_1990", "2020-2025", "estimate"), 1), "`.\n",
    "- Log OLS, static 1990 controls: `2010-2019 = ", fmt_num(pull_result("gross_add_units", "log_ols", "static_1990", "2010-2019", "pct_effect"), 1), "%`; `2020-2025 = ", fmt_num(pull_result("gross_add_units", "log_ols", "static_1990", "2020-2025", "pct_effect"), 1), "%`.\n",
    "- PPML, static 1990 controls: `2010-2019 = ", fmt_num(pull_result("gross_add_units", "ppml", "static_1990", "2010-2019", "pct_effect"), 1), "%`; `2020-2025 = ", fmt_num(pull_result("gross_add_units", "ppml", "static_1990", "2020-2025", "pct_effect"), 1), "%`."
  ),
  "",
  paste0(
    "### Large New-Building Units (50+ Unit Projects)\n",
    "- Level OLS, no controls: `2010-2019 = ", fmt_num(pull_result("nb_gross_units_50_plus", "level_ols", "uncontrolled", "2010-2019", "estimate"), 1), "`; `2020-2025 = ", fmt_num(pull_result("nb_gross_units_50_plus", "level_ols", "uncontrolled", "2020-2025", "estimate"), 1), "`.\n",
    "- Level OLS, static 1990 controls: `2010-2019 = ", fmt_num(pull_result("nb_gross_units_50_plus", "level_ols", "static_1990", "2010-2019", "estimate"), 1), "`; `2020-2025 = ", fmt_num(pull_result("nb_gross_units_50_plus", "level_ols", "static_1990", "2020-2025", "estimate"), 1), "`.\n",
    "- Log OLS, static 1990 controls: `2010-2019 = ", fmt_num(pull_result("nb_gross_units_50_plus", "log_ols", "static_1990", "2010-2019", "pct_effect"), 1), "%`; `2020-2025 = ", fmt_num(pull_result("nb_gross_units_50_plus", "log_ols", "static_1990", "2020-2025", "pct_effect"), 1), "%`.\n",
    "- PPML, static 1990 controls: `2010-2019 = ", fmt_num(pull_result("nb_gross_units_50_plus", "ppml", "static_1990", "2010-2019", "pct_effect"), 1), "%`; `2020-2025 = ", fmt_num(pull_result("nb_gross_units_50_plus", "ppml", "static_1990", "2020-2025", "pct_effect"), 1), "%`."
  ),
  "",
  paste0(
    "### New-Building Project Counts\n",
    "- Level OLS, no controls: `2010-2019 = ", fmt_num(pull_result("nb_project_count", "level_ols", "uncontrolled", "2010-2019", "estimate"), 2), "`; `2020-2025 = ", fmt_num(pull_result("nb_project_count", "level_ols", "uncontrolled", "2020-2025", "estimate"), 2), "`.\n",
    "- Level OLS, static 1990 controls: `2010-2019 = ", fmt_num(pull_result("nb_project_count", "level_ols", "static_1990", "2010-2019", "estimate"), 2), "`; `2020-2025 = ", fmt_num(pull_result("nb_project_count", "level_ols", "static_1990", "2020-2025", "estimate"), 2), "`.\n",
    "- Log OLS, static 1990 controls: `2010-2019 = ", fmt_num(pull_result("nb_project_count", "log_ols", "static_1990", "2010-2019", "pct_effect"), 1), "%`; `2020-2025 = ", fmt_num(pull_result("nb_project_count", "log_ols", "static_1990", "2020-2025", "pct_effect"), 1), "%`.\n",
    "- PPML, static 1990 controls: `2010-2019 = ", fmt_num(pull_result("nb_project_count", "ppml", "static_1990", "2010-2019", "pct_effect"), 1), "%`; `2020-2025 = ", fmt_num(pull_result("nb_project_count", "ppml", "static_1990", "2020-2025", "pct_effect"), 1), "%`."
  )
)

write_csv_if_changed(results_df, out_results_csv)
write_csv_if_changed(summary_df, out_summary_csv)
write_csv_if_changed(eligibility_df, out_eligibility_csv)
writeLines(report_lines, out_report_md)

cat("Wrote DCP supply scale-check outputs to", dirname(out_results_csv), "\n")
