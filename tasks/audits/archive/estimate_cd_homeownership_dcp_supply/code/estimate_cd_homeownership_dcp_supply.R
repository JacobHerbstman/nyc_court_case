# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/estimate_cd_homeownership_dcp_supply/code")
# cd_homeownership_dcp_supply_panel_csv <- "../input/cd_homeownership_dcp_supply_panel.csv"
# cd_homeownership_permit_nb_panel_csv <- "../input/cd_homeownership_permit_nb_panel.csv"
# out_results_csv <- "../output/cd_homeownership_dcp_supply_era_interactions.csv"
# out_summary_csv <- "../output/cd_homeownership_dcp_supply_model_summary.csv"

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
  stop("Expected 4 arguments: cd_homeownership_dcp_supply_panel_csv cd_homeownership_permit_nb_panel_csv out_results_csv out_summary_csv")
}

cd_homeownership_dcp_supply_panel_csv <- args[1]
cd_homeownership_permit_nb_panel_csv <- args[2]
out_results_csv <- args[3]
out_summary_csv <- args[4]

add_era_interactions <- function(df, variable_names, era_values) {
  out_df <- df

  for (variable_name in variable_names) {
    for (era_value in era_values) {
      out_df[[paste0(variable_name, "_x_", str_replace_all(era_value, "-", "_"))]] <- out_df[[variable_name]] * as.integer(out_df$era == era_value)
    }
  }

  out_df
}

extract_model_rows <- function(model, outcome_family, outcome_label, analysis_family, treatment_scale, control_layer, sample_label, proxy_name, reference_era, sample_year_min, sample_year_max, term_names, eras) {
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
      analysis_family,
      outcome_family,
      outcome_label,
      treatment_scale,
      control_layer,
      sample_label,
      proxy_name,
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

z_score <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  spread <- stats::sd(x, na.rm = TRUE)

  if (is.na(spread) || spread == 0) {
    return(rep(NA_real_, length(x)))
  }

  (x - mean(x, na.rm = TRUE)) / spread
}

supply_panel <- read_csv(cd_homeownership_dcp_supply_panel_csv, show_col_types = FALSE, na = c("", "NA"))
permit_panel <- read_csv(cd_homeownership_permit_nb_panel_csv, show_col_types = FALSE, na = c("", "NA"))

control_lookup <- supply_panel %>%
  distinct(
    borocd,
    borough_code,
    borough_name,
    vacancy_rate_1990_exact,
    renter_share_1990_exact,
    structure_share_1_2_units_1990_exact,
    structure_share_3_4_units_1990_exact,
    structure_share_5_plus_units_1990_exact,
    median_household_income_1990_1999_dollars_exact,
    poverty_share_1990_exact,
    median_housing_value_1990_2000_dollars_exact_filled,
    median_housing_value_1990_missing_exact,
    subway_commute_share_1990_exact,
    mean_commute_time_1990_minutes_exact,
    total_housing_units_growth_1980_1990_approx,
    occupied_units_growth_1980_1990_approx,
    vacancy_rate_change_1980_1990_pp_approx,
    homeowner_share_change_1980_1990_pp_approx
  ) %>%
  mutate(
    vacancy_rate_1990_exact_z = z_score(vacancy_rate_1990_exact),
    renter_share_1990_exact_z = z_score(renter_share_1990_exact),
    structure_share_1_2_units_1990_exact_z = z_score(structure_share_1_2_units_1990_exact),
    structure_share_3_4_units_1990_exact_z = z_score(structure_share_3_4_units_1990_exact),
    structure_share_5_plus_units_1990_exact_z = z_score(structure_share_5_plus_units_1990_exact),
    median_household_income_1990_1999_dollars_exact_z = z_score(median_household_income_1990_1999_dollars_exact),
    poverty_share_1990_exact_z = z_score(poverty_share_1990_exact),
    median_housing_value_1990_2000_dollars_exact_filled_z = z_score(median_housing_value_1990_2000_dollars_exact_filled),
    subway_commute_share_1990_exact_z = z_score(subway_commute_share_1990_exact),
    mean_commute_time_1990_minutes_exact_z = z_score(mean_commute_time_1990_minutes_exact),
    total_housing_units_growth_1980_1990_approx_z = z_score(total_housing_units_growth_1980_1990_approx),
    occupied_units_growth_1980_1990_approx_z = z_score(occupied_units_growth_1980_1990_approx),
    vacancy_rate_change_1980_1990_pp_approx_z = z_score(vacancy_rate_change_1980_1990_pp_approx),
    homeowner_share_change_1980_1990_pp_approx_z = z_score(homeowner_share_change_1980_1990_pp_approx)
  ) %>%
  group_by(borough_code, borough_name) %>%
  mutate(
    high_subway_share_flag = subway_commute_share_1990_exact >= median(subway_commute_share_1990_exact, na.rm = TRUE),
    short_commute_flag = mean_commute_time_1990_minutes_exact <= median(mean_commute_time_1990_minutes_exact, na.rm = TRUE),
    high_income_flag = median_household_income_1990_1999_dollars_exact >= median(median_household_income_1990_1999_dollars_exact, na.rm = TRUE),
    high_housing_value_flag = median_housing_value_1990_2000_dollars_exact_filled >= median(median_housing_value_1990_2000_dollars_exact_filled, na.rm = TRUE)
  ) %>%
  ungroup()

dcp_outcomes <- tribble(
  ~outcome_family, ~outcome_label,
  "gross_add_units", "Gross residential additions",
  "gross_loss_units", "Gross residential losses",
  "net_units", "Net residential units",
  "nb_gross_units", "New-building gross units",
  "nb_gross_units_1_2", "NB gross units: 1-2 unit projects",
  "nb_gross_units_3_4", "NB gross units: 3-4 unit projects",
  "nb_gross_units_5_9", "NB gross units: 5-9 unit projects",
  "nb_gross_units_10_49", "NB gross units: 10-49 unit projects",
  "nb_gross_units_50_plus", "NB gross units: 50+ unit projects"
)

proxy_defs <- tribble(
  ~proxy_name, ~high_flag, ~low_flag, ~high_label, ~low_label,
  "subway_share", "high_subway_share_flag", "high_subway_share_flag", "high_subway_share", "low_subway_share",
  "commute_time", "short_commute_flag", "short_commute_flag", "short_commute", "long_commute",
  "income", "high_income_flag", "high_income_flag", "high_income", "low_income",
  "housing_value", "high_housing_value_flag", "high_housing_value_flag", "high_housing_value", "low_housing_value"
)

controlled_control_vars <- c(
  "vacancy_rate_1990_exact_z",
  "structure_share_1_2_units_1990_exact_z",
  "structure_share_3_4_units_1990_exact_z",
  "structure_share_5_plus_units_1990_exact_z",
  "median_household_income_1990_1999_dollars_exact_z",
  "poverty_share_1990_exact_z",
  "median_housing_value_1990_2000_dollars_exact_filled_z",
  "median_housing_value_1990_missing_exact",
  "subway_commute_share_1990_exact_z",
  "mean_commute_time_1990_minutes_exact_z",
  "total_housing_units_growth_1980_1990_approx_z",
  "occupied_units_growth_1980_1990_approx_z",
  "vacancy_rate_change_1980_1990_pp_approx_z",
  "homeowner_share_change_1980_1990_pp_approx_z"
)

dcp_reg_df <- supply_panel %>%
  left_join(
    control_lookup %>%
      select(
        borocd,
        borough_code,
        borough_name,
        ends_with("_z"),
        high_subway_share_flag,
        short_commute_flag,
        high_income_flag,
        high_housing_value_flag
      ),
    by = c("borocd", "borough_code", "borough_name")
  ) %>%
  filter(year >= 2000, year <= 2025, outcome_family %in% dcp_outcomes$outcome_family) %>%
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

permit_reg_df <- permit_panel %>%
  filter(year >= 1994, !is.na(borough_outcome_share)) %>%
  mutate(
    outcome_family = "permit_nb_jobs_share",
    outcome_label = "DOB new-building jobs within-borough share",
    era = case_when(
      year >= 1994 & year <= 1999 ~ "1994-1999",
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

for (outcome_idx in seq_len(nrow(dcp_outcomes))) {
  outcome_family <- dcp_outcomes$outcome_family[outcome_idx]
  outcome_label <- dcp_outcomes$outcome_label[outcome_idx]

  for (treatment_scale in c("treat_pp", "treat_z_boro")) {
    for (control_layer in c("uncontrolled", "controlled")) {
      work_df <- dcp_reg_df %>%
        filter(outcome_family == !!outcome_family)

      control_vars <- if (control_layer == "controlled") controlled_control_vars else character()
      work_df <- add_era_interactions(work_df, c(treatment_scale, control_vars), c("2010-2019", "2020-2025"))

      treat_terms <- paste0(treatment_scale, "_x_", c("2010_2019", "2020_2025"))
      control_terms <- unlist(lapply(control_vars, function(control_var) paste0(control_var, "_x_", c("2010_2019", "2020_2025"))))
      formula_terms <- c(treat_terms, control_terms)
      model_formula <- as.formula(paste("outcome_value ~", paste(formula_terms, collapse = " + "), "| borocd + borough_year"))

      model <- feols(model_formula, data = work_df, cluster = ~borocd)

      results_rows[[result_idx]] <- extract_model_rows(
        model = model,
        outcome_family = outcome_family,
        outcome_label = outcome_label,
        analysis_family = "dcp_primary",
        treatment_scale = treatment_scale,
        control_layer = control_layer,
        sample_label = "full_sample",
        proxy_name = NA_character_,
        reference_era = "2000-2009",
        sample_year_min = 2000,
        sample_year_max = 2025,
        term_names = treat_terms,
        eras = c("2010-2019", "2020-2025")
      )
      result_idx <- result_idx + 1L

      summary_rows[[summary_idx]] <- tibble(
        analysis_family = "dcp_primary",
        outcome_family = outcome_family,
        outcome_label = outcome_label,
        treatment_scale = treatment_scale,
        control_layer = control_layer,
        sample_label = "full_sample",
        proxy_name = NA_character_,
        reference_era = "2000-2009",
        nobs = nobs(model),
        district_count = n_distinct(work_df$borocd),
        year_count = n_distinct(work_df$year),
        outcome_mean = mean(work_df$outcome_value, na.rm = TRUE),
        within_r2 = as.numeric(r2(model, type = "wr2"))
      )
      summary_idx <- summary_idx + 1L
    }
  }
}

for (proxy_idx in seq_len(nrow(proxy_defs))) {
  proxy_name <- proxy_defs$proxy_name[proxy_idx]
  high_flag <- proxy_defs$high_flag[proxy_idx]
  high_label <- proxy_defs$high_label[proxy_idx]
  low_label <- proxy_defs$low_label[proxy_idx]

  for (sample_label in c("high", "low")) {
    if (sample_label == "high") {
      split_df <- dcp_reg_df %>% filter(.data[[high_flag]])
      sample_name <- high_label
    } else {
      split_df <- dcp_reg_df %>% filter(!.data[[high_flag]])
      sample_name <- low_label
    }

    for (outcome_family in c("gross_add_units", "nb_gross_units_10_49", "nb_gross_units_50_plus")) {
      outcome_label <- dcp_outcomes$outcome_label[dcp_outcomes$outcome_family == outcome_family]
      work_df <- split_df %>%
        filter(outcome_family == !!outcome_family)

      work_df <- add_era_interactions(work_df, "treat_z_boro", c("2010-2019", "2020-2025"))
      treat_terms <- paste0("treat_z_boro_x_", c("2010_2019", "2020_2025"))
      model <- feols(
        outcome_value ~ treat_z_boro_x_2010_2019 + treat_z_boro_x_2020_2025 | borocd + borough_year,
        data = work_df,
        cluster = ~borocd
      )

      results_rows[[result_idx]] <- extract_model_rows(
        model = model,
        outcome_family = outcome_family,
        outcome_label = outcome_label,
        analysis_family = "dcp_proxy_split",
        treatment_scale = "treat_z_boro",
        control_layer = "uncontrolled",
        sample_label = sample_name,
        proxy_name = proxy_name,
        reference_era = "2000-2009",
        sample_year_min = 2000,
        sample_year_max = 2025,
        term_names = treat_terms,
        eras = c("2010-2019", "2020-2025")
      )
      result_idx <- result_idx + 1L

      summary_rows[[summary_idx]] <- tibble(
        analysis_family = "dcp_proxy_split",
        outcome_family = outcome_family,
        outcome_label = outcome_label,
        treatment_scale = "treat_z_boro",
        control_layer = "uncontrolled",
        sample_label = sample_name,
        proxy_name = proxy_name,
        reference_era = "2000-2009",
        nobs = nobs(model),
        district_count = n_distinct(work_df$borocd),
        year_count = n_distinct(work_df$year),
        outcome_mean = mean(work_df$outcome_value, na.rm = TRUE),
        within_r2 = as.numeric(r2(model, type = "wr2"))
      )
      summary_idx <- summary_idx + 1L
    }
  }
}

for (treatment_scale in c("treat_pp", "treat_z_boro")) {
  work_df <- add_era_interactions(permit_reg_df, treatment_scale, c("2000-2009", "2010-2019", "2020-2025"))
  treat_terms <- paste0(treatment_scale, "_x_", c("2000_2009", "2010_2019", "2020_2025"))

  model <- feols(
    as.formula(paste("borough_outcome_share ~", paste(treat_terms, collapse = " + "), "| borocd + borough_year")),
    data = work_df,
    cluster = ~borocd
  )

  results_rows[[result_idx]] <- extract_model_rows(
    model = model,
    outcome_family = "permit_nb_jobs_share",
    outcome_label = "DOB new-building jobs within-borough share",
    analysis_family = "permit_rebaseline",
    treatment_scale = treatment_scale,
    control_layer = "uncontrolled",
    sample_label = "full_sample",
    proxy_name = NA_character_,
    reference_era = "1994-1999",
    sample_year_min = 1994,
    sample_year_max = 2025,
    term_names = treat_terms,
    eras = c("2000-2009", "2010-2019", "2020-2025")
  )
  result_idx <- result_idx + 1L

  summary_rows[[summary_idx]] <- tibble(
    analysis_family = "permit_rebaseline",
    outcome_family = "permit_nb_jobs_share",
    outcome_label = "DOB new-building jobs within-borough share",
    treatment_scale = treatment_scale,
    control_layer = "uncontrolled",
    sample_label = "full_sample",
    proxy_name = NA_character_,
    reference_era = "1994-1999",
    nobs = nobs(model),
    district_count = n_distinct(work_df$borocd),
    year_count = n_distinct(work_df$year),
    outcome_mean = mean(work_df$borough_outcome_share, na.rm = TRUE),
    within_r2 = as.numeric(r2(model, type = "wr2"))
  )
  summary_idx <- summary_idx + 1L
}

results_df <- bind_rows(results_rows) %>%
  arrange(analysis_family, outcome_family, control_layer, sample_label, treatment_scale, era)

summary_df <- bind_rows(summary_rows) %>%
  arrange(analysis_family, outcome_family, control_layer, sample_label, treatment_scale)

write_csv_if_changed(results_df, out_results_csv)
write_csv_if_changed(summary_df, out_summary_csv)

cat("Wrote DCP housing-supply regression outputs to", dirname(out_results_csv), "\n")
