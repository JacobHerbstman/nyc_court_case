# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/estimate_cd_homeownership_mappluto_proxy/code")
# cd_homeownership_mappluto_proxy_panel_csv <- "../input/cd_homeownership_mappluto_proxy_panel.csv"
# out_results_csv <- "../output/cd_homeownership_mappluto_proxy_era_interactions.csv"
# out_summary_csv <- "../output/cd_homeownership_mappluto_proxy_model_summary.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(fixest)
  library(readr)
  library(stringr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: cd_homeownership_mappluto_proxy_panel_csv out_results_csv out_summary_csv")
}

cd_homeownership_mappluto_proxy_panel_csv <- args[1]
out_results_csv <- args[2]
out_summary_csv <- args[3]

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

extract_model_rows <- function(model, outcome_family, outcome_label, control_layer, term_names, eras) {
  coef_table <- as.data.frame(coeftable(model))
  coef_table$term <- rownames(coef_table)
  rownames(coef_table) <- NULL

  confint_df <- as.data.frame(confint(model))
  confint_df$term <- rownames(confint_df)
  rownames(confint_df) <- NULL
  names(confint_df)[1:2] <- c("conf_low", "conf_high")

  tibble(term = term_names, era = eras) |>
    left_join(coef_table, by = "term") |>
    left_join(confint_df, by = "term") |>
    transmute(
      outcome_family,
      outcome_label,
      treatment_scale = "treat_z_boro",
      control_layer,
      reference_era = "1985-1989",
      era,
      estimate = Estimate,
      std_error = `Std. Error`,
      statistic = `t value`,
      p_value = `Pr(>|t|)`,
      conf_low,
      conf_high
    )
}

panel_df <- read_csv(cd_homeownership_mappluto_proxy_panel_csv, show_col_types = FALSE, na = c("", "NA"))

control_lookup <- panel_df |>
  distinct(
    borocd,
    borough_code,
    borough_name,
    vacancy_rate_1990_exact,
    structure_share_1_2_units_1990_exact,
    structure_share_3_4_units_1990_exact,
    structure_share_5_plus_units_1990_exact,
    median_household_income_1990_1999_dollars_exact,
    poverty_share_1990_exact,
    median_housing_value_1990_2000_dollars_exact_filled,
    median_housing_value_1990_missing_exact,
    foreign_born_share_1990_exact,
    college_graduate_share_1990_exact,
    unemployment_rate_1990_exact,
    subway_commute_share_1990_exact,
    mean_commute_time_1990_minutes_exact
  ) |>
  mutate(
    vacancy_rate_1990_exact_z = z_score(vacancy_rate_1990_exact),
    structure_share_1_2_units_1990_exact_z = z_score(structure_share_1_2_units_1990_exact),
    structure_share_3_4_units_1990_exact_z = z_score(structure_share_3_4_units_1990_exact),
    structure_share_5_plus_units_1990_exact_z = z_score(structure_share_5_plus_units_1990_exact),
    median_household_income_1990_1999_dollars_exact_z = z_score(median_household_income_1990_1999_dollars_exact),
    poverty_share_1990_exact_z = z_score(poverty_share_1990_exact),
    median_housing_value_1990_2000_dollars_exact_filled_z = z_score(median_housing_value_1990_2000_dollars_exact_filled),
    foreign_born_share_1990_exact_z = z_score(foreign_born_share_1990_exact),
    college_graduate_share_1990_exact_z = z_score(college_graduate_share_1990_exact),
    unemployment_rate_1990_exact_z = z_score(unemployment_rate_1990_exact),
    subway_commute_share_1990_exact_z = z_score(subway_commute_share_1990_exact),
    mean_commute_time_1990_minutes_exact_z = z_score(mean_commute_time_1990_minutes_exact)
  )

outcomes <- tribble(
  ~outcome_family, ~outcome_label,
  "residential_units_proxy", "PLUTO proxy residential units built",
  "units_1_4_proxy", "PLUTO proxy residential units built: 1-4 units",
  "units_5_plus_proxy", "PLUTO proxy residential units built: 5+ units",
  "units_50_plus_proxy", "PLUTO proxy residential units built: 50+ units"
)

control_vars <- c(
  "vacancy_rate_1990_exact_z",
  "structure_share_1_2_units_1990_exact_z",
  "structure_share_3_4_units_1990_exact_z",
  "structure_share_5_plus_units_1990_exact_z",
  "median_household_income_1990_1999_dollars_exact_z",
  "poverty_share_1990_exact_z",
  "median_housing_value_1990_2000_dollars_exact_filled_z",
  "median_housing_value_1990_missing_exact",
  "foreign_born_share_1990_exact_z",
  "college_graduate_share_1990_exact_z",
  "unemployment_rate_1990_exact_z",
  "subway_commute_share_1990_exact_z",
  "mean_commute_time_1990_minutes_exact_z"
)

reg_df <- panel_df |>
  left_join(
    control_lookup |>
      select(borocd, borough_code, borough_name, ends_with("_z")),
    by = c("borocd", "borough_code", "borough_name")
  ) |>
  filter(outcome_family %in% outcomes$outcome_family, year >= 1980, year <= 2009) |>
  mutate(
    era = case_when(
      year >= 1980 & year <= 1984 ~ "1980-1984",
      year >= 1985 & year <= 1989 ~ "1985-1989",
      year >= 1990 & year <= 1999 ~ "1990-1999",
      year >= 2000 & year <= 2009 ~ "2000-2009",
      TRUE ~ NA_character_
    ),
    borough_year = interaction(borough_name, year, drop = TRUE)
  ) |>
  filter(!is.na(era))

results_rows <- list()
summary_rows <- list()
result_idx <- 1L
summary_idx <- 1L

for (i in seq_len(nrow(outcomes))) {
  outcome_family <- outcomes$outcome_family[i]
  outcome_label <- outcomes$outcome_label[i]

  for (control_layer in c("uncontrolled", "static_1990")) {
    work_df <- reg_df |>
      filter(outcome_family == !!outcome_family)

    active_control_vars <- if (control_layer == "static_1990") control_vars else character()
    work_df <- add_era_interactions(work_df, c("treat_z_boro", active_control_vars), c("1980-1984", "1990-1999", "2000-2009"))

    treat_terms <- paste0("treat_z_boro_x_", c("1980_1984", "1990_1999", "2000_2009"))
    control_terms <- unlist(lapply(active_control_vars, function(control_var) paste0(control_var, "_x_", c("1980_1984", "1990_1999", "2000_2009"))))
    model_formula <- as.formula(paste("outcome_value ~", paste(c(treat_terms, control_terms), collapse = " + "), "| borocd + borough_year"))

    model <- feols(model_formula, data = work_df, cluster = ~borocd)

    results_rows[[result_idx]] <- extract_model_rows(
      model = model,
      outcome_family = outcome_family,
      outcome_label = outcome_label,
      control_layer = control_layer,
      term_names = treat_terms,
      eras = c("1980-1984", "1990-1999", "2000-2009")
    )
    result_idx <- result_idx + 1L

    summary_rows[[summary_idx]] <- tibble(
      outcome_family = outcome_family,
      outcome_label = outcome_label,
      treatment_scale = "treat_z_boro",
      control_layer = control_layer,
      reference_era = "1985-1989",
      sample_year_min = min(work_df$year, na.rm = TRUE),
      sample_year_max = max(work_df$year, na.rm = TRUE),
      observation_count = nobs(model),
      district_count = n_distinct(work_df$borocd),
      mean_outcome = mean(work_df$outcome_value, na.rm = TRUE),
      sd_outcome = sd(work_df$outcome_value, na.rm = TRUE),
      r2_within = as.numeric(unname(fitstat(model, "wr2")))
    )
    summary_idx <- summary_idx + 1L
  }
}

write_csv(bind_rows(results_rows), out_results_csv, na = "")
write_csv(bind_rows(summary_rows), out_summary_csv, na = "")

cat("Wrote MapPLUTO proxy regression outputs to", dirname(out_results_csv), "\n")
