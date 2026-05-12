# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/estimate_cd_homeownership_long_units_event_study/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

write_lines_if_changed <- function(lines, out_path) {
  temp_path <- tempfile(fileext = ".tex")
  writeLines(lines, temp_path, useBytes = TRUE)
  copy_if_changed(temp_path, out_path)
}

assert_required_columns <- function(df, required_cols, df_name) {
  missing_cols <- setdiff(required_cols, names(df))

  if (length(missing_cols) > 0) {
    stop(df_name, " is missing required columns: ", paste(missing_cols, collapse = ", "))
  }
}

assert_unique_keys <- function(df, key_cols, df_name) {
  duplicate_keys <- df |>
    count(across(all_of(key_cols)), name = "row_count") |>
    filter(row_count > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(df_name, " is not unique by ", paste(key_cols, collapse = ", "), ".")
  }
}

z_score <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  x_sd <- stats::sd(x, na.rm = TRUE)

  if (is.na(x_sd) || x_sd == 0) {
    return(rep(0, length(x)))
  }

  (x - mean(x, na.rm = TRUE)) / x_sd
}

sanitize_period <- function(x) {
  str_replace_all(x, "-", "_")
}

add_period_terms <- function(df, variable_names, period_values) {
  out_df <- df

  for (variable_name in variable_names) {
    for (period_value in period_values) {
      out_df[[paste0(variable_name, "_x_", sanitize_period(period_value))]] <- out_df[[variable_name]] * as.integer(out_df$event_period == period_value)
    }
  }

  out_df
}

coeftable_df <- function(model) {
  coef_table <- as.data.frame(coeftable(model))
  coef_table$term <- rownames(coef_table)
  rownames(coef_table) <- NULL

  statistic_col <- if ("t value" %in% names(coef_table)) "t value" else "z value"
  p_value_col <- if ("Pr(>|t|)" %in% names(coef_table)) "Pr(>|t|)" else "Pr(>|z|)"

  coef_table |>
    transmute(
      term,
      estimate = Estimate,
      std_error = `Std. Error`,
      statistic = .data[[statistic_col]],
      p_value = .data[[p_value_col]]
    )
}

confint_df <- function(model) {
  out <- as.data.frame(confint(model))
  out$term <- rownames(out)
  rownames(out) <- NULL
  names(out)[1:2] <- c("conf_low", "conf_high")
  out
}

extract_model_terms <- function(model, requested_terms_df) {
  requested_terms_df |>
    left_join(coeftable_df(model), by = "term", relationship = "many-to-one") |>
    left_join(confint_df(model), by = "term", relationship = "many-to-one")
}

log_point_to_percent <- function(x) {
  100 * (exp(x) - 1)
}

format_decimal <- function(x, digits = 1) {
  if_else(is.na(x), "", formatC(x, format = "f", digits = digits))
}

format_p_value <- function(x) {
  case_when(
    is.na(x) ~ "",
    x < 0.001 ~ "$<0.001$",
    TRUE ~ formatC(x, format = "f", digits = 3)
  )
}

format_p_value_plain <- function(x) {
  case_when(
    is.na(x) ~ "",
    x < 0.001 ~ "<0.001",
    TRUE ~ formatC(x, format = "f", digits = 3)
  )
}

significance_stars <- function(x) {
  case_when(
    is.na(x) ~ "",
    x < 0.01 ~ "***",
    x < 0.05 ~ "**",
    x < 0.1 ~ "*",
    TRUE ~ ""
  )
}

within_r2 <- function(model) {
  tryCatch(as.numeric(r2(model, type = "wr2")), error = function(e) NA_real_)
}

control_layer_label <- function(x) {
  case_when(
    x == "0_fe_only" ~ "FE only",
    x == "1_light_income_poverty" ~ "Income + poverty",
    x == "1_light_no_preprod" ~ "Income + poverty + log occ",
    x == "1_light_no_size" ~ "Income + poverty + pre-prod",
    x == "1_light_controls" ~ "Income + poverty + log occ + pre-prod",
    x == "1_baseline_1990" ~ "Baseline 1990",
    x == "2_baseline_1990_plus_pretrends" ~ "Baseline + pretrends",
    x == "3_land_opportunity_robustness" ~ "Land opportunity robustness",
    TRUE ~ x
  )
}

event_periods <- c(
  "1980-1984",
  "1985-1989",
  "1990-1999",
  "2000-2009",
  "2010-2019",
  "2020-2025"
)
reference_event_period <- "1985-1989"
estimated_event_periods <- event_periods[event_periods != reference_event_period]

event_periods_5yr <- c(
  "1980-1984",
  "1985-1989",
  "1990-1994",
  "1995-1999",
  "2000-2004",
  "2005-2009",
  "2010-2014",
  "2015-2019",
  "2020-2025"
)
reference_event_period_5yr <- "1985-1989"
estimated_event_periods_5yr <- event_periods_5yr[event_periods_5yr != reference_event_period_5yr]

outcome_defs <- tribble(
  ~outcome_id, ~outcome_label, ~margin_role,
  "units_built_1_4", "1-4 unit buildings", "placebo",
  "units_built_5_49", "5-49 unit buildings", "intermediate",
  "units_built_5_plus", "5+ unit buildings", "broad_large",
  "units_built_50_plus", "50+ unit buildings", "main",
  "projects_built_50_plus", "50+ unit projects", "supplemental"
)

scale_defs <- tribble(
  ~outcome_scale, ~outcome_scale_label,
  "per_10000_occupied_1990", "Per 10,000 occupied units",
  "per_residential_acre", "Per residential acre"
)

baseline_raw_controls <- c(
  "log_occupied_units_1990_exact",
  "vacancy_rate_1990_exact",
  "structure_share_1_2_units_1990_exact",
  "structure_share_5_plus_units_1990_exact",
  "median_household_income_1990_1999_dollars_exact",
  "poverty_share_1990_exact",
  "subway_commute_share_1990_exact",
  "mean_commute_time_1990_minutes_exact"
)

pretrend_raw_controls <- c(
  "total_housing_units_growth_1980_1990_approx",
  "vacancy_rate_change_1980_1990_pp_approx",
  "homeowner_share_change_1980_1990_pp_approx"
)

land_raw_controls <- c(
  "density_1990_occ_per_res_acre",
  "cd_mean_built_far_lot_weighted",
  "cd_mean_max_resid_far_lot_weighted",
  "cd_share_lot_area_vacant",
  "cd_share_lot_area_parking_or_low_intensity",
  "cd_share_lot_area_protected"
)

baseline_control_cols <- paste0(baseline_raw_controls, "_z")
pretrend_control_cols <- paste0(pretrend_raw_controls, "_z")
land_control_cols <- paste0(land_raw_controls, "_z")
light_income_poverty_control_cols <- c(
  "median_household_income_1990_1999_dollars_exact_z",
  "poverty_share_1990_exact_z"
)
light_no_preprod_control_cols <- c(
  "log_occupied_units_1990_exact_z",
  "median_household_income_1990_1999_dollars_exact_z",
  "poverty_share_1990_exact_z"
)
light_no_size_control_cols <- c(
  "median_household_income_1990_1999_dollars_exact_z",
  "poverty_share_1990_exact_z",
  "pre_1980_1988_rate_z"
)
light_control_cols <- c(
  "log_occupied_units_1990_exact_z",
  "median_household_income_1990_1999_dollars_exact_z",
  "poverty_share_1990_exact_z",
  "pre_1980_1988_rate_z"
)

control_blocks <- list(
  `0_fe_only` = character(),
  `1_light_income_poverty` = light_income_poverty_control_cols,
  `1_light_no_preprod` = light_no_preprod_control_cols,
  `1_light_no_size` = light_no_size_control_cols,
  `1_light_controls` = light_control_cols,
  `1_baseline_1990` = baseline_control_cols,
  `2_baseline_1990_plus_pretrends` = c(baseline_control_cols, pretrend_control_cols, "pre_1980_1988_rate_z"),
  `3_land_opportunity_robustness` = c(baseline_control_cols, pretrend_control_cols, "pre_1980_1988_rate_z", land_control_cols)
)

series_df <- read_csv("../input/cd_homeownership_long_units_series.csv", show_col_types = FALSE, na = c("", "NA"))
controls_df <- read_csv("../input/cd_baseline_1990_controls.csv", show_col_types = FALSE, na = c("", "NA"))
redev_df <- read_csv("../input/cd_redevelopment_potential_baseline.csv", show_col_types = FALSE, na = c("", "NA"))

assert_required_columns(
  series_df,
  c("series_kind", "series_family", "borocd", "borough_code", "borough_name", "year", "outcome_value", "treat_z_boro"),
  "Long units series"
)

assert_required_columns(
  controls_df,
  c(
    "borocd", "borough_code", "borough_name", "occupied_units_1990_exact", "homeowner_share_1980_approx",
    "vacancy_rate_1990_exact", "structure_share_1_2_units_1990_exact", "structure_share_5_plus_units_1990_exact",
    "median_household_income_1990_1999_dollars_exact", "poverty_share_1990_exact",
    "subway_commute_share_1990_exact", "mean_commute_time_1990_minutes_exact",
    pretrend_raw_controls
  ),
  "Baseline controls"
)

assert_required_columns(
  redev_df,
  c("borocd", "residential_acres", "cd_mean_built_far_lot_weighted", "cd_mean_max_resid_far_lot_weighted", "cd_share_lot_area_vacant", "cd_share_lot_area_parking_or_low_intensity", "cd_share_lot_area_protected"),
  "Redevelopment-potential baseline"
)

redev_clean <- redev_df |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    residential_acres = suppressWarnings(as.numeric(residential_acres)),
    cd_mean_built_far_lot_weighted = suppressWarnings(as.numeric(cd_mean_built_far_lot_weighted)),
    cd_mean_max_resid_far_lot_weighted = suppressWarnings(as.numeric(cd_mean_max_resid_far_lot_weighted)),
    cd_share_lot_area_vacant = suppressWarnings(as.numeric(cd_share_lot_area_vacant)),
    cd_share_lot_area_parking_or_low_intensity = suppressWarnings(as.numeric(cd_share_lot_area_parking_or_low_intensity)),
    cd_share_lot_area_protected = suppressWarnings(as.numeric(cd_share_lot_area_protected))
  )

assert_unique_keys(redev_clean, "borocd", "Redevelopment-potential baseline")

controls_clean <- controls_df |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = as.character(borough_name),
    occupied_units_1990_exact = suppressWarnings(as.numeric(occupied_units_1990_exact)),
    homeowner_share_1980_approx = suppressWarnings(as.numeric(homeowner_share_1980_approx)),
    log_occupied_units_1990_exact = log(suppressWarnings(as.numeric(occupied_units_1990_exact))),
    vacancy_rate_1990_exact = suppressWarnings(as.numeric(vacancy_rate_1990_exact)),
    structure_share_1_2_units_1990_exact = suppressWarnings(as.numeric(structure_share_1_2_units_1990_exact)),
    structure_share_5_plus_units_1990_exact = suppressWarnings(as.numeric(structure_share_5_plus_units_1990_exact)),
    median_household_income_1990_1999_dollars_exact = suppressWarnings(as.numeric(median_household_income_1990_1999_dollars_exact)),
    poverty_share_1990_exact = suppressWarnings(as.numeric(poverty_share_1990_exact)),
    subway_commute_share_1990_exact = suppressWarnings(as.numeric(subway_commute_share_1990_exact)),
    mean_commute_time_1990_minutes_exact = suppressWarnings(as.numeric(mean_commute_time_1990_minutes_exact)),
    total_housing_units_growth_1980_1990_approx = suppressWarnings(as.numeric(total_housing_units_growth_1980_1990_approx)),
    vacancy_rate_change_1980_1990_pp_approx = suppressWarnings(as.numeric(vacancy_rate_change_1980_1990_pp_approx)),
    homeowner_share_change_1980_1990_pp_approx = suppressWarnings(as.numeric(homeowner_share_change_1980_1990_pp_approx))
  ) |>
  left_join(redev_clean, by = "borocd", relationship = "one-to-one") |>
  mutate(
    density_1990_occ_per_res_acre = if_else(residential_acres > 0, occupied_units_1990_exact / residential_acres, NA_real_)
  ) |>
  group_by(borough_code, borough_name) |>
  mutate(homeowner_share_1980_z_boro = z_score(homeowner_share_1980_approx)) |>
  ungroup()

assert_unique_keys(controls_clean, "borocd", "Baseline controls")

raw_control_cols <- c(baseline_raw_controls, pretrend_raw_controls, land_raw_controls)
missing_control_cell_count <- sum(is.na(controls_clean[, raw_control_cols]))

controls_z <- controls_clean |>
  mutate(across(all_of(raw_control_cols), z_score, .names = "{.col}_z")) |>
  select(
    borocd,
    borough_code,
    borough_name,
    occupied_units_1990_exact,
    residential_acres,
    homeowner_share_1980_z_boro,
    all_of(c(baseline_control_cols, pretrend_control_cols, land_control_cols))
  )

series_clean <- series_df |>
  filter(
    series_kind == "preferred_long_series",
    series_family %in% c("units_built_1_4", "units_built_5_plus", "units_built_50_plus", "projects_built_50_plus"),
    year >= 1980,
    year <= 2025
  ) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = as.character(borough_name),
    year = suppressWarnings(as.integer(year)),
    series_family = as.character(series_family),
    outcome_value = suppressWarnings(as.numeric(outcome_value)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro))
  )

assert_unique_keys(series_clean, c("series_family", "borocd", "year"), "Preferred long-units series")

wide_df <- series_clean |>
  select(borocd, borough_code, borough_name, year, treat_z_boro, series_family, outcome_value) |>
  pivot_wider(
    names_from = series_family,
    values_from = outcome_value,
    values_fill = 0
  ) |>
  mutate(
    units_built_5_49 = units_built_5_plus - units_built_50_plus
  )

negative_5_49_count <- sum(wide_df$units_built_5_49 < -1e-8, na.rm = TRUE)

panel_df <- wide_df |>
  mutate(units_built_5_49 = pmax(units_built_5_49, 0)) |>
  left_join(controls_z, by = c("borocd", "borough_code", "borough_name"), relationship = "many-to-one") |>
  select(
    borocd,
    borough_code,
    borough_name,
    year,
    treat_z_boro,
    homeowner_share_1980_z_boro,
    occupied_units_1990_exact,
    residential_acres,
    all_of(c(baseline_control_cols, pretrend_control_cols, land_control_cols)),
    all_of(outcome_defs$outcome_id)
  ) |>
  pivot_longer(
    cols = all_of(outcome_defs$outcome_id),
    names_to = "outcome_id",
    values_to = "outcome_value"
  ) |>
  left_join(outcome_defs, by = "outcome_id", relationship = "many-to-one") |>
  mutate(
    event_period = case_when(
      year >= 1980 & year <= 1984 ~ "1980-1984",
      year >= 1985 & year <= 1989 ~ "1985-1989",
      year >= 1990 & year <= 1999 ~ "1990-1999",
      year >= 2000 & year <= 2009 ~ "2000-2009",
      year >= 2010 & year <= 2019 ~ "2010-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    ),
    event_period = factor(event_period, levels = event_periods),
    borough_period = interaction(borough_name, event_period, drop = TRUE),
    outcome_per_10000_occupied_1990 = if_else(occupied_units_1990_exact > 0, 10000 * outcome_value / occupied_units_1990_exact, NA_real_),
    outcome_per_residential_acre = if_else(residential_acres > 0, outcome_value / residential_acres, NA_real_)
  ) |>
  filter(!is.na(event_period))

assert_unique_keys(panel_df, c("borocd", "year", "outcome_id"), "Event-study margin panel")

analysis_panel <- panel_df |>
  select(
    borocd,
    borough_code,
    borough_name,
    year,
    event_period,
    borough_period,
    outcome_id,
    outcome_label,
    margin_role,
    treat_z_boro,
    homeowner_share_1980_z_boro,
    all_of(c(baseline_control_cols, pretrend_control_cols, land_control_cols)),
    outcome_per_10000_occupied_1990,
    outcome_per_residential_acre
  ) |>
  pivot_longer(
    cols = c(outcome_per_10000_occupied_1990, outcome_per_residential_acre),
    names_to = "outcome_scale",
    values_to = "outcome_rate"
  ) |>
  mutate(
    outcome_scale = case_when(
      outcome_scale == "outcome_per_10000_occupied_1990" ~ "per_10000_occupied_1990",
      TRUE ~ "per_residential_acre"
    )
  ) |>
  left_join(scale_defs, by = "outcome_scale", relationship = "many-to-one")

pre_rate_lookup <- analysis_panel |>
  filter(year >= 1980, year <= 1988) |>
  group_by(outcome_id, outcome_scale, borocd) |>
  summarize(pre_1980_1988_rate = mean(outcome_rate, na.rm = TRUE), .groups = "drop") |>
  group_by(outcome_id, outcome_scale) |>
  mutate(pre_1980_1988_rate_z = z_score(pre_1980_1988_rate)) |>
  ungroup()

analysis_panel <- analysis_panel |>
  left_join(
    pre_rate_lookup |>
      select(outcome_id, outcome_scale, borocd, pre_1980_1988_rate_z),
    by = c("outcome_id", "outcome_scale", "borocd"),
    relationship = "many-to-one"
  )

event_rows <- list()
summary_rows <- list()
event_index <- 1L
summary_index <- 1L

for (outcome_value in outcome_defs$outcome_id) {
  for (scale_value in scale_defs$outcome_scale) {
    outcome_df <- analysis_panel |>
      filter(outcome_id == outcome_value, outcome_scale == scale_value, !is.na(outcome_rate))

    for (control_layer in names(control_blocks)) {
      control_vars <- control_blocks[[control_layer]]
      work_df <- add_period_terms(outcome_df, c("treat_z_boro", control_vars), estimated_event_periods)
      treatment_terms <- paste0("treat_z_boro_x_", sanitize_period(estimated_event_periods))
      control_terms <- unlist(lapply(control_vars, function(control_var) paste0(control_var, "_x_", sanitize_period(estimated_event_periods))))
      model_formula <- as.formula(paste0("outcome_rate ~ ", paste(c(treatment_terms, control_terms), collapse = " + "), " | borocd + borough_period"))

      model <- feols(model_formula, data = work_df, cluster = ~borocd)

      requested_terms_df <- tibble(term = treatment_terms, event_period = estimated_event_periods, is_reference = FALSE)
      event_rows[[event_index]] <- bind_rows(
        tibble(
          term = NA_character_,
          event_period = reference_event_period,
          is_reference = TRUE,
          estimate = 0,
          std_error = NA_real_,
          statistic = NA_real_,
          p_value = NA_real_,
          conf_low = NA_real_,
          conf_high = NA_real_
        ),
        extract_model_terms(model, requested_terms_df)
      ) |>
        mutate(
          outcome_id = first(work_df$outcome_id),
          outcome_label = first(work_df$outcome_label),
          margin_role = first(work_df$margin_role),
          outcome_scale = first(work_df$outcome_scale),
          outcome_scale_label = first(work_df$outcome_scale_label),
          control_layer = control_layer,
          control_layer_label = control_layer_label(control_layer),
          reference_event_period = reference_event_period,
          sample_year_min = min(work_df$year, na.rm = TRUE),
          sample_year_max = max(work_df$year, na.rm = TRUE)
        ) |>
        select(
          outcome_id,
          outcome_label,
          margin_role,
          outcome_scale,
          outcome_scale_label,
          control_layer,
          control_layer_label,
          reference_event_period,
          event_period,
          is_reference,
          term,
          estimate,
          std_error,
          statistic,
          p_value,
          conf_low,
          conf_high,
          sample_year_min,
          sample_year_max
        )
      event_index <- event_index + 1L

      summary_rows[[summary_index]] <- tibble(
        model_family = "event_study",
        outcome_id = first(work_df$outcome_id),
        outcome_scale = first(work_df$outcome_scale),
        treatment = "treat_z_boro",
        control_layer = control_layer,
        observation_count = nobs(model),
        district_count = n_distinct(work_df$borocd),
        year_count = n_distinct(work_df$year),
        period_count = n_distinct(work_df$event_period),
        control_count = length(control_vars),
        requested_treat_term_count = length(treatment_terms),
        present_treat_term_count = sum(treatment_terms %in% names(coef(model))),
        within_r2 = within_r2(model)
      )
      summary_index <- summary_index + 1L
    }
  }
}

event_coefficients_df <- bind_rows(event_rows) |>
  mutate(
    event_period = factor(event_period, levels = event_periods),
    control_layer = factor(control_layer, levels = names(control_blocks))
  ) |>
  arrange(outcome_id, outcome_scale, control_layer, event_period)

analysis_panel_5yr <- analysis_panel |>
  mutate(
    event_period = case_when(
      year >= 1980 & year <= 1984 ~ "1980-1984",
      year >= 1985 & year <= 1989 ~ "1985-1989",
      year >= 1990 & year <= 1994 ~ "1990-1994",
      year >= 1995 & year <= 1999 ~ "1995-1999",
      year >= 2000 & year <= 2004 ~ "2000-2004",
      year >= 2005 & year <= 2009 ~ "2005-2009",
      year >= 2010 & year <= 2014 ~ "2010-2014",
      year >= 2015 & year <= 2019 ~ "2015-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    ),
    event_period = factor(event_period, levels = event_periods_5yr),
    borough_period = interaction(borough_name, event_period, drop = TRUE)
  ) |>
  filter(!is.na(event_period))

event_5yr_rows <- list()
event_5yr_index <- 1L
event_5yr_control_layer <- "1_light_controls"
event_5yr_control_vars <- control_blocks[[event_5yr_control_layer]]

for (outcome_value in c("units_built_1_4", "units_built_5_plus")) {
  for (scale_value in scale_defs$outcome_scale) {
    outcome_df <- analysis_panel_5yr |>
      filter(outcome_id == outcome_value, outcome_scale == scale_value, !is.na(outcome_rate))

    work_df <- add_period_terms(outcome_df, c("treat_z_boro", event_5yr_control_vars), estimated_event_periods_5yr)
    treatment_terms <- paste0("treat_z_boro_x_", sanitize_period(estimated_event_periods_5yr))
    control_terms <- unlist(lapply(event_5yr_control_vars, function(control_var) paste0(control_var, "_x_", sanitize_period(estimated_event_periods_5yr))))
    model_formula <- as.formula(paste0("outcome_rate ~ ", paste(c(treatment_terms, control_terms), collapse = " + "), " | borocd + borough_period"))

    model <- feols(model_formula, data = work_df, cluster = ~borocd)

    requested_terms_df <- tibble(term = treatment_terms, event_period = estimated_event_periods_5yr, is_reference = FALSE)
    event_5yr_rows[[event_5yr_index]] <- bind_rows(
      tibble(
        term = NA_character_,
        event_period = reference_event_period_5yr,
        is_reference = TRUE,
        estimate = 0,
        std_error = NA_real_,
        statistic = NA_real_,
        p_value = NA_real_,
        conf_low = NA_real_,
        conf_high = NA_real_
      ),
      extract_model_terms(model, requested_terms_df)
    ) |>
      mutate(
        period_scheme = "five_year_bins",
        outcome_id = first(work_df$outcome_id),
        outcome_label = first(work_df$outcome_label),
        margin_role = first(work_df$margin_role),
        outcome_scale = first(work_df$outcome_scale),
        outcome_scale_label = first(work_df$outcome_scale_label),
        control_layer = event_5yr_control_layer,
        control_layer_label = control_layer_label(event_5yr_control_layer),
        reference_event_period = reference_event_period_5yr,
        sample_year_min = min(work_df$year, na.rm = TRUE),
        sample_year_max = max(work_df$year, na.rm = TRUE)
      ) |>
      select(
        period_scheme,
        outcome_id,
        outcome_label,
        margin_role,
        outcome_scale,
        outcome_scale_label,
        control_layer,
        control_layer_label,
        reference_event_period,
        event_period,
        is_reference,
        term,
        estimate,
        std_error,
        statistic,
        p_value,
        conf_low,
        conf_high,
        sample_year_min,
        sample_year_max
      )
    event_5yr_index <- event_5yr_index + 1L

    summary_rows[[summary_index]] <- tibble(
      model_family = "event_study_5yr",
      outcome_id = first(work_df$outcome_id),
      outcome_scale = first(work_df$outcome_scale),
      treatment = "treat_z_boro",
      control_layer = event_5yr_control_layer,
      observation_count = nobs(model),
      district_count = n_distinct(work_df$borocd),
      year_count = n_distinct(work_df$year),
      period_count = n_distinct(work_df$event_period),
      control_count = length(event_5yr_control_vars),
      requested_treat_term_count = length(treatment_terms),
      present_treat_term_count = sum(treatment_terms %in% names(coef(model))),
      within_r2 = within_r2(model)
    )
    summary_index <- summary_index + 1L
  }
}

event_coefficients_5yr_df <- bind_rows(event_5yr_rows) |>
  mutate(
    event_period = factor(event_period, levels = event_periods_5yr),
    control_layer = factor(control_layer, levels = names(control_blocks))
  ) |>
  arrange(outcome_id, outcome_scale, control_layer, event_period)

poisson_rows <- list()
poisson_index <- 1L
poisson_specs <- bind_rows(
  tibble(outcome_id = outcome_defs$outcome_id, control_layer = "0_fe_only"),
  tibble(outcome_id = "units_built_5_plus", control_layer = "1_light_controls")
)

for (poisson_spec_index in seq_len(nrow(poisson_specs))) {
  poisson_outcome_id <- poisson_specs$outcome_id[poisson_spec_index]
  poisson_control_layer <- poisson_specs$control_layer[poisson_spec_index]
  poisson_control_vars <- control_blocks[[poisson_control_layer]]

  outcome_df <- panel_df |>
    filter(
      outcome_id == poisson_outcome_id,
      !is.na(outcome_value),
      outcome_value >= 0,
      occupied_units_1990_exact > 0
    ) |>
    left_join(
      pre_rate_lookup |>
        filter(outcome_scale == "per_10000_occupied_1990") |>
        select(outcome_id, borocd, pre_1980_1988_rate_z),
      by = c("outcome_id", "borocd"),
      relationship = "many-to-one"
    )

  work_df <- add_period_terms(outcome_df, c("treat_z_boro", poisson_control_vars), estimated_event_periods)
  treatment_terms <- paste0("treat_z_boro_x_", sanitize_period(estimated_event_periods))
  control_terms <- unlist(lapply(poisson_control_vars, function(control_var) paste0(control_var, "_x_", sanitize_period(estimated_event_periods))))
  model_formula <- as.formula(paste0("outcome_value ~ ", paste(c(treatment_terms, control_terms), collapse = " + "), " | borocd + borough_period"))

  model <- fepois(
    model_formula,
    data = work_df,
    cluster = ~borocd,
    offset = log(work_df$occupied_units_1990_exact)
  )

  requested_terms_df <- tibble(term = treatment_terms, event_period = estimated_event_periods, is_reference = FALSE)
  poisson_rows[[poisson_index]] <- bind_rows(
    tibble(
      term = NA_character_,
      event_period = reference_event_period,
      is_reference = TRUE,
      estimate = 0,
      std_error = NA_real_,
      statistic = NA_real_,
      p_value = NA_real_,
      conf_low = NA_real_,
      conf_high = NA_real_
    ),
    extract_model_terms(model, requested_terms_df)
  ) |>
    transmute(
      outcome_id = first(work_df$outcome_id),
      outcome_label = first(work_df$outcome_label),
      margin_role = first(work_df$margin_role),
      poisson_scale = "count_offset_occupied_1990",
      poisson_scale_label = "Count with 1990 occupied-units exposure",
      control_layer = poisson_control_layer,
      control_layer_label = control_layer_label(poisson_control_layer),
      reference_event_period = reference_event_period,
      event_period,
      is_reference,
      term,
      log_estimate = estimate,
      log_std_error = std_error,
      log_statistic = statistic,
      p_value,
      log_conf_low = conf_low,
      log_conf_high = conf_high,
      percent_estimate = log_point_to_percent(estimate),
      percent_conf_low = log_point_to_percent(conf_low),
      percent_conf_high = log_point_to_percent(conf_high),
      sample_year_min = min(work_df$year, na.rm = TRUE),
      sample_year_max = max(work_df$year, na.rm = TRUE)
    )
  poisson_index <- poisson_index + 1L

  summary_rows[[summary_index]] <- tibble(
    model_family = "event_study_poisson",
    outcome_id = first(work_df$outcome_id),
    outcome_scale = "count_offset_occupied_1990",
    treatment = "treat_z_boro",
    control_layer = poisson_control_layer,
    observation_count = nobs(model),
    district_count = n_distinct(work_df$borocd),
    year_count = n_distinct(work_df$year),
    period_count = n_distinct(work_df$event_period),
    control_count = length(poisson_control_vars),
    requested_treat_term_count = length(treatment_terms),
    present_treat_term_count = sum(treatment_terms %in% names(coef(model))),
    within_r2 = within_r2(model)
  )
  summary_index <- summary_index + 1L
}

event_poisson_coefficients_df <- bind_rows(poisson_rows) |>
  mutate(event_period = factor(event_period, levels = event_periods)) |>
  arrange(outcome_id, control_layer, event_period)

panel_df_5yr <- panel_df |>
  mutate(
    event_period = case_when(
      year >= 1980 & year <= 1984 ~ "1980-1984",
      year >= 1985 & year <= 1989 ~ "1985-1989",
      year >= 1990 & year <= 1994 ~ "1990-1994",
      year >= 1995 & year <= 1999 ~ "1995-1999",
      year >= 2000 & year <= 2004 ~ "2000-2004",
      year >= 2005 & year <= 2009 ~ "2005-2009",
      year >= 2010 & year <= 2014 ~ "2010-2014",
      year >= 2015 & year <= 2019 ~ "2015-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    ),
    event_period = factor(event_period, levels = event_periods_5yr),
    borough_period = interaction(borough_name, event_period, drop = TRUE)
  ) |>
  filter(!is.na(event_period))

poisson_5yr_rows <- list()
poisson_5yr_index <- 1L
poisson_5yr_control_layer <- "1_light_controls"
poisson_5yr_control_vars <- control_blocks[[poisson_5yr_control_layer]]

for (poisson_5yr_outcome_id in c("units_built_1_4", "units_built_5_plus")) {
  outcome_df <- panel_df_5yr |>
    filter(
      outcome_id == poisson_5yr_outcome_id,
      !is.na(outcome_value),
      outcome_value >= 0,
      occupied_units_1990_exact > 0
    ) |>
    left_join(
      pre_rate_lookup |>
        filter(outcome_scale == "per_10000_occupied_1990") |>
        select(outcome_id, borocd, pre_1980_1988_rate_z),
      by = c("outcome_id", "borocd"),
      relationship = "many-to-one"
    )

  work_df <- add_period_terms(outcome_df, c("treat_z_boro", poisson_5yr_control_vars), estimated_event_periods_5yr)
  treatment_terms <- paste0("treat_z_boro_x_", sanitize_period(estimated_event_periods_5yr))
  control_terms <- unlist(lapply(poisson_5yr_control_vars, function(control_var) paste0(control_var, "_x_", sanitize_period(estimated_event_periods_5yr))))
  model_formula <- as.formula(paste0("outcome_value ~ ", paste(c(treatment_terms, control_terms), collapse = " + "), " | borocd + borough_period"))

  model <- fepois(
    model_formula,
    data = work_df,
    cluster = ~borocd,
    offset = log(work_df$occupied_units_1990_exact)
  )

  requested_terms_df <- tibble(term = treatment_terms, event_period = estimated_event_periods_5yr, is_reference = FALSE)
  poisson_5yr_rows[[poisson_5yr_index]] <- bind_rows(
    tibble(
      term = NA_character_,
      event_period = reference_event_period_5yr,
      is_reference = TRUE,
      estimate = 0,
      std_error = NA_real_,
      statistic = NA_real_,
      p_value = NA_real_,
      conf_low = NA_real_,
      conf_high = NA_real_
    ),
    extract_model_terms(model, requested_terms_df)
  ) |>
    transmute(
      period_scheme = "five_year_bins",
      outcome_id = first(work_df$outcome_id),
      outcome_label = first(work_df$outcome_label),
      margin_role = first(work_df$margin_role),
      poisson_scale = "count_offset_occupied_1990",
      poisson_scale_label = "Count with 1990 occupied-units exposure",
      control_layer = poisson_5yr_control_layer,
      control_layer_label = control_layer_label(poisson_5yr_control_layer),
      reference_event_period = reference_event_period_5yr,
      event_period,
      is_reference,
      term,
      log_estimate = estimate,
      log_std_error = std_error,
      log_statistic = statistic,
      p_value,
      log_conf_low = conf_low,
      log_conf_high = conf_high,
      percent_estimate = log_point_to_percent(estimate),
      percent_conf_low = log_point_to_percent(conf_low),
      percent_conf_high = log_point_to_percent(conf_high),
      sample_year_min = min(work_df$year, na.rm = TRUE),
      sample_year_max = max(work_df$year, na.rm = TRUE)
    )
  poisson_5yr_index <- poisson_5yr_index + 1L

  summary_rows[[summary_index]] <- tibble(
    model_family = "event_study_poisson_5yr",
    outcome_id = first(work_df$outcome_id),
    outcome_scale = "count_offset_occupied_1990",
    treatment = "treat_z_boro",
    control_layer = poisson_5yr_control_layer,
    observation_count = nobs(model),
    district_count = n_distinct(work_df$borocd),
    year_count = n_distinct(work_df$year),
    period_count = n_distinct(work_df$event_period),
    control_count = length(poisson_5yr_control_vars),
    requested_treat_term_count = length(treatment_terms),
    present_treat_term_count = sum(treatment_terms %in% names(coef(model))),
    within_r2 = within_r2(model)
  )
  summary_index <- summary_index + 1L
}

event_poisson_coefficients_5yr_df <- bind_rows(poisson_5yr_rows) |>
  mutate(event_period = factor(event_period, levels = event_periods_5yr)) |>
  arrange(outcome_id, control_layer, event_period)

window_defs <- tribble(
  ~comparison_id, ~comparison_type, ~pre_start, ~pre_end, ~post_start, ~post_end,
  "placebo_1985_1989_minus_1980_1984", "pretrend_placebo", 1980L, 1984L, 1985L, 1989L,
  "post_1990_1999_minus_1980_1988", "post_vs_pre", 1980L, 1988L, 1990L, 1999L,
  "post_2000_2009_minus_1980_1988", "post_vs_pre", 1980L, 1988L, 2000L, 2009L,
  "post_2010_2019_minus_1980_1988", "post_vs_pre", 1980L, 1988L, 2010L, 2019L,
  "post_2020_2025_minus_1980_1988", "post_vs_pre", 1980L, 1988L, 2020L, 2025L
) |>
  mutate(
    pre_window = paste0(pre_start, "-", pre_end),
    post_window = paste0(post_start, "-", post_end)
  )

treatment_defs <- tribble(
  ~treatment, ~treatment_label,
  "treat_z_boro", "1990 homeownership z-score within borough",
  "homeowner_share_1980_z_boro", "1980 homeownership z-score within borough"
)

long_difference_rows <- list()
long_index <- 1L

cd_scale_lookup <- analysis_panel |>
  select(
    borocd,
    borough_code,
    borough_name,
    outcome_id,
    outcome_label,
    margin_role,
    outcome_scale,
    outcome_scale_label,
    treat_z_boro,
    homeowner_share_1980_z_boro,
    all_of(c(baseline_control_cols, pretrend_control_cols, land_control_cols)),
    pre_1980_1988_rate_z
  ) |>
  distinct()

for (window_index in seq_len(nrow(window_defs))) {
  window_row <- window_defs[window_index, ]

  pre_df <- analysis_panel |>
    filter(year >= window_row$pre_start, year <= window_row$pre_end) |>
    group_by(borocd, outcome_id, outcome_scale) |>
    summarize(pre_avg = mean(outcome_rate, na.rm = TRUE), pre_year_count = n_distinct(year), .groups = "drop")

  post_df <- analysis_panel |>
    filter(year >= window_row$post_start, year <= window_row$post_end) |>
    group_by(borocd, outcome_id, outcome_scale) |>
    summarize(post_avg = mean(outcome_rate, na.rm = TRUE), post_year_count = n_distinct(year), .groups = "drop")

  diff_df <- cd_scale_lookup |>
    left_join(pre_df, by = c("borocd", "outcome_id", "outcome_scale"), relationship = "one-to-one") |>
    left_join(post_df, by = c("borocd", "outcome_id", "outcome_scale"), relationship = "one-to-one") |>
    mutate(delta_value = post_avg - pre_avg)

  for (outcome_value in outcome_defs$outcome_id) {
    for (scale_value in scale_defs$outcome_scale) {
      for (treatment_index in seq_len(nrow(treatment_defs))) {
        treatment_var <- treatment_defs$treatment[treatment_index]
        treatment_label <- treatment_defs$treatment_label[treatment_index]

        for (control_layer in names(control_blocks)) {
          control_vars <- control_blocks[[control_layer]]
          work_df <- diff_df |>
            filter(outcome_id == outcome_value, outcome_scale == scale_value) |>
            mutate(treatment_value = .data[[treatment_var]])

          work_df$comparison_id <- window_row$comparison_id
          work_df$comparison_type <- window_row$comparison_type
          work_df$pre_window <- window_row$pre_window
          work_df$post_window <- window_row$post_window

          model_df <- work_df |>
            select(delta_value, pre_avg, treatment_value, borough_name, all_of(control_vars)) |>
            filter(if_all(everything(), ~ !is.na(.x)))

          model_formula <- as.formula(paste0("delta_value ~ treatment_value", if (length(control_vars) > 0) paste0(" + ", paste(control_vars, collapse = " + ")) else "", " | borough_name"))
          model <- feols(model_formula, data = model_df, vcov = "hetero")
          requested_terms_df <- tibble(term = "treatment_value")
          term_df <- extract_model_terms(model, requested_terms_df)

          long_difference_rows[[long_index]] <- term_df |>
            transmute(
              comparison_id = window_row$comparison_id,
              comparison_type = window_row$comparison_type,
              pre_window = window_row$pre_window,
              post_window = window_row$post_window,
              outcome_id = outcome_value,
              outcome_label = first(work_df$outcome_label),
              margin_role = first(work_df$margin_role),
              outcome_scale = scale_value,
              outcome_scale_label = first(work_df$outcome_scale_label),
              treatment = treatment_var,
              treatment_label = treatment_label,
              control_layer = control_layer,
              control_layer_label = control_layer_label(control_layer),
              term,
              estimate,
              std_error,
              statistic,
              p_value,
              conf_low,
              conf_high,
              observation_count = nobs(model),
              district_count = nrow(model_df),
              dep_var_mean = mean(model_df$delta_value),
              initial_outcome_mean = mean(model_df$pre_avg),
              control_count = length(control_vars),
              pre_year_count_min = min(work_df$pre_year_count, na.rm = TRUE),
              post_year_count_min = min(work_df$post_year_count, na.rm = TRUE)
            )
          long_index <- long_index + 1L

          summary_rows[[summary_index]] <- tibble(
            model_family = "long_difference",
            outcome_id = outcome_value,
            outcome_scale = scale_value,
            treatment = treatment_var,
            control_layer = control_layer,
            observation_count = nobs(model),
            district_count = nrow(model_df),
            year_count = NA_integer_,
            period_count = NA_integer_,
            control_count = length(control_vars),
            requested_treat_term_count = 1L,
            present_treat_term_count = as.integer("treatment_value" %in% names(coef(model))),
            within_r2 = NA_real_
          )
          summary_index <- summary_index + 1L
        }
      }
    }
  }
}

long_difference_df <- bind_rows(long_difference_rows) |>
  arrange(comparison_type, outcome_id, outcome_scale, treatment, control_layer, post_window)

triple_df <- analysis_panel |>
  filter(
    outcome_scale == "per_10000_occupied_1990",
    outcome_id %in% c("units_built_1_4", "units_built_5_49", "units_built_50_plus"),
    !is.na(outcome_rate)
  ) |>
  mutate(
    cd_margin = interaction(borocd, outcome_id, drop = TRUE),
    borough_period_margin = interaction(borough_name, event_period, outcome_id, drop = TRUE),
    cd_year = interaction(borocd, year, drop = TRUE)
  )

triple_terms <- character()
triple_term_rows <- list()
triple_term_index <- 1L

for (margin_value in c("units_built_5_49", "units_built_50_plus")) {
  for (period_value in estimated_event_periods) {
    term_name <- paste0("treat_z_boro_x_", sanitize_period(period_value), "_x_", margin_value)
    triple_df[[term_name]] <- triple_df$treat_z_boro * as.integer(triple_df$event_period == period_value) * as.integer(triple_df$outcome_id == margin_value)
    triple_terms <- c(triple_terms, term_name)
    triple_term_rows[[triple_term_index]] <- tibble(term = term_name, event_period = period_value, outcome_id = margin_value, is_reference = FALSE)
    triple_term_index <- triple_term_index + 1L
  }
}

triple_model <- feols(
  as.formula(paste0("outcome_rate ~ ", paste(triple_terms, collapse = " + "), " | cd_margin + borough_period_margin + cd_year")),
  data = triple_df,
  cluster = ~borocd
)

triple_estimated_df <- extract_model_terms(triple_model, bind_rows(triple_term_rows)) |>
  left_join(outcome_defs |> select(outcome_id, outcome_label, margin_role), by = "outcome_id", relationship = "many-to-one")

triple_reference_df <- outcome_defs |>
  filter(outcome_id %in% c("units_built_5_49", "units_built_50_plus")) |>
  transmute(
    term = NA_character_,
    event_period = reference_event_period,
    outcome_id,
    is_reference = TRUE,
    estimate = 0,
    std_error = NA_real_,
    statistic = NA_real_,
    p_value = NA_real_,
    conf_low = NA_real_,
    conf_high = NA_real_,
    outcome_label,
    margin_role
  )

triple_diff_df <- bind_rows(triple_reference_df, triple_estimated_df) |>
  mutate(
    reference_event_period = reference_event_period,
    outcome_scale = "per_10000_occupied_1990",
    outcome_scale_label = "Per 10,000 occupied units",
    base_margin = "units_built_1_4",
    base_margin_label = "1-4 unit buildings"
  ) |>
  select(
    outcome_id,
    outcome_label,
    margin_role,
    base_margin,
    base_margin_label,
    outcome_scale,
    outcome_scale_label,
    reference_event_period,
    event_period,
    is_reference,
    term,
    estimate,
    std_error,
    statistic,
    p_value,
    conf_low,
    conf_high
  ) |>
  mutate(event_period = factor(event_period, levels = event_periods)) |>
  arrange(outcome_id, event_period)

summary_rows[[summary_index]] <- tibble(
  model_family = "triple_difference",
  outcome_id = "stacked_size_margins",
  outcome_scale = "per_10000_occupied_1990",
  treatment = "treat_z_boro",
  control_layer = "size_margin_fe",
  observation_count = nobs(triple_model),
  district_count = n_distinct(triple_df$borocd),
  year_count = n_distinct(triple_df$year),
  period_count = n_distinct(triple_df$event_period),
  control_count = 0L,
  requested_treat_term_count = length(triple_terms),
  present_treat_term_count = sum(triple_terms %in% names(coef(triple_model))),
  within_r2 = within_r2(triple_model)
)
summary_index <- summary_index + 1L

permute_within_borough <- function(x, borough) {
  out <- x
  for (borough_value in unique(borough)) {
    idx <- which(borough == borough_value)
    out[idx] <- sample(x[idx], length(idx), replace = FALSE)
  }
  out
}

randomization_rows <- list()
randomization_index <- 1L
randomization_control_layer <- "2_baseline_1990_plus_pretrends"
randomization_controls <- control_blocks[[randomization_control_layer]]
randomization_permutations <- 999L
set.seed(20260502)

for (window_index in seq_len(nrow(filter(window_defs, comparison_type == "post_vs_pre")))) {
  window_row <- filter(window_defs, comparison_type == "post_vs_pre")[window_index, ]

  pre_df <- analysis_panel |>
    filter(year >= window_row$pre_start, year <= window_row$pre_end) |>
    group_by(borocd, outcome_id, outcome_scale) |>
    summarize(pre_avg = mean(outcome_rate, na.rm = TRUE), .groups = "drop")

  post_df <- analysis_panel |>
    filter(year >= window_row$post_start, year <= window_row$post_end) |>
    group_by(borocd, outcome_id, outcome_scale) |>
    summarize(post_avg = mean(outcome_rate, na.rm = TRUE), .groups = "drop")

  ri_df <- cd_scale_lookup |>
    filter(outcome_id == "units_built_50_plus", outcome_scale == "per_10000_occupied_1990") |>
    left_join(pre_df, by = c("borocd", "outcome_id", "outcome_scale"), relationship = "one-to-one") |>
    left_join(post_df, by = c("borocd", "outcome_id", "outcome_scale"), relationship = "one-to-one") |>
    mutate(
      delta_value = post_avg - pre_avg,
      treatment_value = treat_z_boro
    ) |>
    select(delta_value, treatment_value, borough_name, all_of(randomization_controls)) |>
    filter(if_all(everything(), ~ !is.na(.x)))

  ri_formula <- reformulate(c("treatment_value", "factor(borough_name)", randomization_controls), response = "delta_value")
  observed_estimate <- unname(coef(lm(ri_formula, data = ri_df))["treatment_value"])
  permutation_estimates <- rep(NA_real_, randomization_permutations)

  for (perm_index in seq_len(randomization_permutations)) {
    perm_df <- ri_df |>
      mutate(treatment_value = permute_within_borough(treatment_value, borough_name))
    permutation_estimates[perm_index] <- unname(coef(lm(ri_formula, data = perm_df))["treatment_value"])
  }

  valid_estimates <- permutation_estimates[is.finite(permutation_estimates)]
  more_extreme_count <- sum(abs(valid_estimates) >= abs(observed_estimate), na.rm = TRUE)
  randomization_p_value <- (1 + more_extreme_count) / (1 + length(valid_estimates))

  randomization_rows[[randomization_index]] <- tibble(
    comparison_id = window_row$comparison_id,
    comparison_type = window_row$comparison_type,
    pre_window = window_row$pre_window,
    post_window = window_row$post_window,
    outcome_id = "units_built_50_plus",
    outcome_scale = "per_10000_occupied_1990",
    treatment = "treat_z_boro",
    control_layer = randomization_control_layer,
    permutation_scheme = "within_borough",
    permutation_count = length(valid_estimates),
    observed_estimate = observed_estimate,
    permutation_mean = mean(valid_estimates, na.rm = TRUE),
    permutation_sd = sd(valid_estimates, na.rm = TRUE),
    more_extreme_count = more_extreme_count,
    randomization_p_value = randomization_p_value
  )
  randomization_index <- randomization_index + 1L
}

randomization_df <- bind_rows(randomization_rows)
model_summary_df <- bind_rows(summary_rows) |>
  arrange(model_family, outcome_id, outcome_scale, treatment, control_layer)

write_csv_if_changed(event_coefficients_df, "../output/cd_homeownership_long_units_event_coefficients.csv")
write_csv_if_changed(event_coefficients_5yr_df, "../output/cd_homeownership_long_units_event_coefficients_5yr_bins.csv")
write_csv_if_changed(event_poisson_coefficients_df, "../output/cd_homeownership_long_units_event_poisson_coefficients.csv")
write_csv_if_changed(event_poisson_coefficients_5yr_df, "../output/cd_homeownership_long_units_event_poisson_coefficients_5yr_bins.csv")
write_csv_if_changed(long_difference_df, "../output/cd_homeownership_long_units_long_difference_estimates.csv")
write_csv_if_changed(triple_diff_df, "../output/cd_homeownership_long_units_triple_diff_coefficients.csv")
write_csv_if_changed(randomization_df, "../output/cd_homeownership_long_units_randomization_inference.csv")
write_csv_if_changed(model_summary_df, "../output/cd_homeownership_long_units_model_summary.csv")

long_difference_table_df <- long_difference_df |>
  filter(
    outcome_id == "units_built_5_plus",
    outcome_scale == "per_10000_occupied_1990",
    treatment == "treat_z_boro",
    control_layer == "1_light_controls"
  ) |>
  mutate(
    row_order = case_when(
      comparison_id == "placebo_1985_1989_minus_1980_1984" ~ 1L,
      comparison_id == "post_1990_1999_minus_1980_1988" ~ 2L,
      comparison_id == "post_2000_2009_minus_1980_1988" ~ 3L,
      comparison_id == "post_2010_2019_minus_1980_1988" ~ 4L,
      comparison_id == "post_2020_2025_minus_1980_1988" ~ 5L,
      TRUE ~ NA_integer_
    ),
    comparison_label = case_when(
      comparison_id == "placebo_1985_1989_minus_1980_1984" ~ "1985--1989 minus 1980--1984",
      comparison_id == "post_1990_1999_minus_1980_1988" ~ "1990--1999 minus 1980--1988",
      comparison_id == "post_2000_2009_minus_1980_1988" ~ "2000--2009 minus 1980--1988",
      comparison_id == "post_2010_2019_minus_1980_1988" ~ "2010--2019 minus 1980--1988",
      comparison_id == "post_2020_2025_minus_1980_1988" ~ "2020--2025 minus 1980--1988",
      TRUE ~ comparison_id
    ),
    estimate_label = paste0(format_decimal(estimate, 1), significance_stars(p_value)),
    std_error_label = format_decimal(std_error, 1),
    initial_outcome_mean_label = format_decimal(initial_outcome_mean, 1),
    p_value_label = format_p_value(p_value),
    column_label = case_when(
      comparison_id == "placebo_1985_1989_minus_1980_1984" ~ "Placebo",
      comparison_id == "post_1990_1999_minus_1980_1988" ~ "1990--1999",
      comparison_id == "post_2000_2009_minus_1980_1988" ~ "2000--2009",
      comparison_id == "post_2010_2019_minus_1980_1988" ~ "2010--2019",
      comparison_id == "post_2020_2025_minus_1980_1988" ~ "2020--2025",
      TRUE ~ comparison_id
    )
  ) |>
  arrange(row_order)

if (nrow(long_difference_table_df) != 5 || any(is.na(long_difference_table_df$row_order))) {
  stop("Long-difference table expected exactly five 5+ four-control rows.")
}

regression_table_row <- function(row_label, values) {
  paste0("    ", row_label, " & ", paste(values, collapse = " & "), " \\\\")
}

checkmark_values <- rep("\\checkmark", nrow(long_difference_table_df))

long_difference_table_lines <- c(
  "\\begin{table}[htbp]",
  "    \\centering",
  "    \\begin{threeparttable}",
  "    \\caption{Long-Difference Estimates for 5+ Unit Housing Production}",
  "    \\label{tab:homeownership_long_units_long_difference}",
  "    \\small",
  "    \\begin{tabular}{lccccc}",
  "    \\toprule",
  regression_table_row("", paste0("(", seq_len(nrow(long_difference_table_df)), ")")),
  regression_table_row("", long_difference_table_df$column_label),
  "    \\midrule",
  regression_table_row("Homeownership exposure", long_difference_table_df$estimate_label),
  regression_table_row("", paste0("(", long_difference_table_df$std_error_label, ")")),
  "    \\midrule",
  regression_table_row("N", long_difference_table_df$district_count),
  regression_table_row("Initial outcome mean", long_difference_table_df$initial_outcome_mean_label),
  regression_table_row("Borough FE", checkmark_values),
  regression_table_row("Controls", checkmark_values),
  "    \\bottomrule",
  "    \\end{tabular}",
  "    \\begin{tablenotes}[flushleft]",
  "    \\footnotesize",
  "    \\item \\textit{Notes:} Table reports coefficients on within-borough standardized 1990 homeownership from CD-level long-difference regressions. The outcome is average $5+$ unit new-building units per 10,000 1990 occupied units. Column (1) compares 1985--1989 to 1980--1984. Columns (2)--(5) compare the listed post window to the 1980--1988 pre-period. The initial outcome mean is the sample mean of the pre-period outcome level: 1980--1984 in column (1) and 1980--1988 in columns (2)--(5). Controls include 1990 median household income, 1990 poverty share, log 1990 occupied units, and pre-period production on the same outcome scale. Standard errors are heteroskedasticity-robust and shown in parentheses. * $p < 0.10$, ** $p < 0.05$, *** $p < 0.01$.",
  "    \\end{tablenotes}",
  "    \\end{threeparttable}",
  "\\end{table}"
)

write_lines_if_changed(long_difference_table_lines, "../output/cd_homeownership_long_units_long_difference_5_plus_four_controls.tex")

four_controls_event_df <- event_coefficients_df |>
  filter(
    outcome_scale == "per_10000_occupied_1990",
    control_layer == "1_light_controls"
  ) |>
  mutate(event_period = as.character(event_period))

lookup_event_value <- function(outcome_value, period_value, value_col, digits) {
  value <- four_controls_event_df |>
    filter(outcome_id == outcome_value, event_period == period_value) |>
    pull(all_of(value_col))

  if (length(value) != 1) {
    stop("Expected one event-study value for outcome ", outcome_value, ", period ", period_value, " and column ", value_col, ".")
  }

  if (value_col == "p_value") {
    return(format_p_value_plain(value))
  }

  format_decimal(value, digits)
}

four_controls_event_5yr_df <- event_coefficients_5yr_df |>
  filter(
    outcome_scale == "per_10000_occupied_1990",
    control_layer == "1_light_controls"
  ) |>
  mutate(event_period = as.character(event_period))

lookup_event_5yr_value <- function(outcome_value, period_value, value_col, digits) {
  value <- four_controls_event_5yr_df |>
    filter(outcome_id == outcome_value, event_period == period_value) |>
    pull(all_of(value_col))

  if (length(value) != 1) {
    stop("Expected one five-year event-study value for outcome ", outcome_value, ", period ", period_value, " and column ", value_col, ".")
  }

  if (value_col == "p_value") {
    return(format_p_value_plain(value))
  }

  format_decimal(value, digits)
}

lookup_long_difference_value <- function(comparison_value, value_col, digits) {
  value <- long_difference_table_df |>
    filter(comparison_id == comparison_value) |>
    pull(all_of(value_col))

  if (length(value) != 1) {
    stop("Expected one long-difference value for comparison ", comparison_value, " and column ", value_col, ".")
  }

  if (value_col == "p_value") {
    return(format_p_value_plain(value))
  }

  format_decimal(value, digits)
}

latex_macro_line <- function(macro_name, macro_value) {
  paste0("\\newcommand{\\", macro_name, "}{", macro_value, "}")
}

write_lines_if_changed(
  c(
    latex_macro_line("HomeownFivePlusEventEarlyEightiesCoef", lookup_event_value("units_built_5_plus", "1980-1984", "estimate", 1)),
    latex_macro_line("HomeownFivePlusEventNinetiesCoef", lookup_event_value("units_built_5_plus", "1990-1999", "estimate", 1)),
    latex_macro_line("HomeownFivePlusEventTwoThousandsCoef", lookup_event_value("units_built_5_plus", "2000-2009", "estimate", 1)),
    latex_macro_line("HomeownFivePlusEventTwentyTensCoef", lookup_event_value("units_built_5_plus", "2010-2019", "estimate", 1)),
    latex_macro_line("HomeownFivePlusEventTwentyTensP", lookup_event_value("units_built_5_plus", "2010-2019", "p_value", 3)),
    latex_macro_line("HomeownFivePlusEventTwentyTwentiesCoef", lookup_event_value("units_built_5_plus", "2020-2025", "estimate", 1)),
    latex_macro_line("HomeownFivePlusEventTwentyTwentiesP", lookup_event_value("units_built_5_plus", "2020-2025", "p_value", 3)),
    latex_macro_line("HomeownOneFourEventTwentyTensCoef", lookup_event_value("units_built_1_4", "2010-2019", "estimate", 1)),
    latex_macro_line("HomeownOneFourEventTwentyTwentiesCoef", lookup_event_value("units_built_1_4", "2020-2025", "estimate", 1)),
    latex_macro_line("HomeownFivePlusEventFiveyrEarlyEightiesCoef", lookup_event_5yr_value("units_built_5_plus", "1980-1984", "estimate", 1)),
    latex_macro_line("HomeownFivePlusEventFiveyrNinetiesEarlyCoef", lookup_event_5yr_value("units_built_5_plus", "1990-1994", "estimate", 1)),
    latex_macro_line("HomeownFivePlusEventFiveyrNinetiesLateCoef", lookup_event_5yr_value("units_built_5_plus", "1995-1999", "estimate", 1)),
    latex_macro_line("HomeownFivePlusEventFiveyrTwoThousandsEarlyCoef", lookup_event_5yr_value("units_built_5_plus", "2000-2004", "estimate", 1)),
    latex_macro_line("HomeownFivePlusEventFiveyrTwoThousandsLateCoef", lookup_event_5yr_value("units_built_5_plus", "2005-2009", "estimate", 1)),
    latex_macro_line("HomeownFivePlusEventFiveyrTwentyTensEarlyCoef", lookup_event_5yr_value("units_built_5_plus", "2010-2014", "estimate", 1)),
    latex_macro_line("HomeownFivePlusEventFiveyrTwentyTensEarlyP", lookup_event_5yr_value("units_built_5_plus", "2010-2014", "p_value", 3)),
    latex_macro_line("HomeownFivePlusEventFiveyrTwentyTensLateCoef", lookup_event_5yr_value("units_built_5_plus", "2015-2019", "estimate", 1)),
    latex_macro_line("HomeownFivePlusEventFiveyrTwentyTensLateP", lookup_event_5yr_value("units_built_5_plus", "2015-2019", "p_value", 3)),
    latex_macro_line("HomeownFivePlusEventFiveyrTwentyTwentiesCoef", lookup_event_5yr_value("units_built_5_plus", "2020-2025", "estimate", 1)),
    latex_macro_line("HomeownFivePlusEventFiveyrTwentyTwentiesP", lookup_event_5yr_value("units_built_5_plus", "2020-2025", "p_value", 3)),
    latex_macro_line("HomeownOneFourEventFiveyrTwentyTensEarlyCoef", lookup_event_5yr_value("units_built_1_4", "2010-2014", "estimate", 1)),
    latex_macro_line("HomeownOneFourEventFiveyrTwentyTensLateCoef", lookup_event_5yr_value("units_built_1_4", "2015-2019", "estimate", 1)),
    latex_macro_line("HomeownOneFourEventFiveyrTwentyTwentiesCoef", lookup_event_5yr_value("units_built_1_4", "2020-2025", "estimate", 1)),
    latex_macro_line("HomeownFivePlusLongDiffPlaceboCoef", lookup_long_difference_value("placebo_1985_1989_minus_1980_1984", "estimate", 1)),
    latex_macro_line("HomeownFivePlusLongDiffPlaceboP", lookup_long_difference_value("placebo_1985_1989_minus_1980_1984", "p_value", 3)),
    latex_macro_line("HomeownFivePlusLongDiffTwentyTensCoef", lookup_long_difference_value("post_2010_2019_minus_1980_1988", "estimate", 1)),
    latex_macro_line("HomeownFivePlusLongDiffTwentyTensP", lookup_long_difference_value("post_2010_2019_minus_1980_1988", "p_value", 3)),
    latex_macro_line("HomeownFivePlusLongDiffTwentyTwentiesCoef", lookup_long_difference_value("post_2020_2025_minus_1980_1988", "estimate", 1)),
    latex_macro_line("HomeownFivePlusLongDiffTwentyTwentiesP", lookup_long_difference_value("post_2020_2025_minus_1980_1988", "p_value", 3))
  ),
  "../output/cd_homeownership_long_units_event_5_plus_four_controls_macros.tex"
)

event_plot_df <- event_coefficients_df |>
  mutate(
    event_period = as.character(event_period),
    event_period_index = match(event_period, event_periods)
  )

pdf("../output/cd_homeownership_long_units_event_coefficients.pdf", width = 11, height = 8.5)
print(
  ggplot(
    filter(event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_10000_occupied_1990"),
    aes(x = event_period_index, y = estimate, color = control_layer_label, group = control_layer_label)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_10000_occupied_1990", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.35, alpha = 0.75) +
    geom_line(linewidth = 0.65) +
    geom_point(size = 1.8) +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "Continuous-treatment event study: 50+ unit buildings", x = NULL, y = "Coefficient on homeowner exposure", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(event_plot_df, outcome_id %in% c("units_built_1_4", "units_built_5_49", "units_built_50_plus"), outcome_scale == "per_10000_occupied_1990"),
    aes(x = event_period_index, y = estimate, color = control_layer_label, group = control_layer_label)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(event_plot_df, outcome_id %in% c("units_built_1_4", "units_built_5_49", "units_built_50_plus"), outcome_scale == "per_10000_occupied_1990", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.35, alpha = 0.75) +
    geom_line(linewidth = 0.65) +
    geom_point(size = 1.8) +
    facet_wrap(~outcome_label, scales = "free_y") +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "Size-margin event-study diagnostics", x = NULL, y = "Coefficient on homeowner exposure", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_residential_acre"),
    aes(x = event_period_index, y = estimate, color = control_layer_label, group = control_layer_label)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_residential_acre", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.35, alpha = 0.75) +
    geom_line(linewidth = 0.65) +
    geom_point(size = 1.8) +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "Residential-acre robustness: 50+ unit buildings", x = NULL, y = "Coefficient on homeowner exposure", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

fe_only_event_plot_df <- event_plot_df |>
  filter(control_layer == "0_fe_only")

pdf("../output/cd_homeownership_long_units_event_coefficients_fe_only.pdf", width = 11, height = 8.5)
print(
  ggplot(
    filter(fe_only_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_10000_occupied_1990"),
    aes(x = event_period_index, y = estimate, group = 1)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(fe_only_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_10000_occupied_1990", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.45, color = "#1f5fbf") +
    geom_line(linewidth = 0.75, color = "#1f5fbf") +
    geom_point(size = 2.1, color = "#1f5fbf") +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "FE-only event study: 50+ unit buildings", x = NULL, y = "Coefficient on homeowner exposure") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(fe_only_event_plot_df, outcome_id %in% c("units_built_1_4", "units_built_5_49", "units_built_50_plus"), outcome_scale == "per_10000_occupied_1990"),
    aes(x = event_period_index, y = estimate, group = 1)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(fe_only_event_plot_df, outcome_id %in% c("units_built_1_4", "units_built_5_49", "units_built_50_plus"), outcome_scale == "per_10000_occupied_1990", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.45, color = "#1f5fbf") +
    geom_line(linewidth = 0.75, color = "#1f5fbf") +
    geom_point(size = 2.1, color = "#1f5fbf") +
    facet_wrap(~outcome_label, scales = "free_y") +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "FE-only size-margin event-study diagnostics", x = NULL, y = "Coefficient on homeowner exposure") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(fe_only_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_residential_acre"),
    aes(x = event_period_index, y = estimate, group = 1)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(fe_only_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_residential_acre", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.45, color = "#1f5fbf") +
    geom_line(linewidth = 0.75, color = "#1f5fbf") +
    geom_point(size = 2.1, color = "#1f5fbf") +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "FE-only residential-acre robustness: 50+ unit buildings", x = NULL, y = "Coefficient on homeowner exposure") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

light_mix_control_layers <- c(
  "1_light_income_poverty",
  "1_light_no_preprod",
  "1_light_no_size",
  "1_light_controls"
)

light_mix_control_labels <- control_layer_label(light_mix_control_layers)
light_mix_colors <- c(
  "Income + poverty" = "#666666",
  "Income + poverty + log occ" = "#b45f06",
  "Income + poverty + pre-prod" = "#7b3294",
  "Income + poverty + log occ + pre-prod" = "#2f7d32"
)

light_mix_event_plot_df <- event_plot_df |>
  filter(control_layer %in% light_mix_control_layers) |>
  mutate(control_layer_label = factor(control_layer_label, levels = light_mix_control_labels))

pdf("../output/cd_homeownership_long_units_event_coefficients_light_mix.pdf", width = 11, height = 8.5)
print(
  ggplot(
    filter(light_mix_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_10000_occupied_1990"),
    aes(x = event_period_index, y = estimate, color = control_layer_label, group = control_layer_label)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(light_mix_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_10000_occupied_1990", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.35, alpha = 0.75) +
    geom_line(linewidth = 0.65) +
    geom_point(size = 1.8) +
    scale_color_manual(values = light_mix_colors) +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    guides(color = guide_legend(nrow = 2, byrow = TRUE)) +
    labs(title = "Light control mixes: 50+ unit buildings", x = NULL, y = "Coefficient on homeowner exposure", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(light_mix_event_plot_df, outcome_id %in% c("units_built_1_4", "units_built_5_49", "units_built_50_plus"), outcome_scale == "per_10000_occupied_1990"),
    aes(x = event_period_index, y = estimate, color = control_layer_label, group = control_layer_label)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(light_mix_event_plot_df, outcome_id %in% c("units_built_1_4", "units_built_5_49", "units_built_50_plus"), outcome_scale == "per_10000_occupied_1990", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.35, alpha = 0.75) +
    geom_line(linewidth = 0.65) +
    geom_point(size = 1.8) +
    facet_wrap(~outcome_label, scales = "free_y") +
    scale_color_manual(values = light_mix_colors) +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    guides(color = guide_legend(nrow = 2, byrow = TRUE)) +
    labs(title = "Light control mixes: size-margin diagnostics", x = NULL, y = "Coefficient on homeowner exposure", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(light_mix_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_residential_acre"),
    aes(x = event_period_index, y = estimate, color = control_layer_label, group = control_layer_label)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(light_mix_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_residential_acre", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.35, alpha = 0.75) +
    geom_line(linewidth = 0.65) +
    geom_point(size = 1.8) +
    scale_color_manual(values = light_mix_colors) +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    guides(color = guide_legend(nrow = 2, byrow = TRUE)) +
    labs(title = "Light control mixes: residential-acre robustness", x = NULL, y = "Coefficient on homeowner exposure", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

five_plus_light_mix_event_plot_df <- event_plot_df |>
  filter(
    outcome_id == "units_built_5_plus",
    control_layer %in% light_mix_control_layers
  ) |>
  mutate(control_layer_label = factor(control_layer_label, levels = light_mix_control_labels))

pdf("../output/cd_homeownership_long_units_event_coefficients_5_plus_light_mix.pdf", width = 11, height = 8.5)
print(
  ggplot(
    filter(five_plus_light_mix_event_plot_df, outcome_scale == "per_10000_occupied_1990"),
    aes(x = event_period_index, y = estimate, color = control_layer_label, group = control_layer_label)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(five_plus_light_mix_event_plot_df, outcome_scale == "per_10000_occupied_1990", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.35, alpha = 0.75) +
    geom_line(linewidth = 0.65) +
    geom_point(size = 1.8) +
    scale_color_manual(values = light_mix_colors) +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    guides(color = guide_legend(nrow = 2, byrow = TRUE)) +
    labs(title = "Light control mixes: 5+ unit buildings", x = NULL, y = "Coefficient on homeowner exposure", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(event_plot_df, outcome_id == "units_built_5_plus", outcome_scale == "per_10000_occupied_1990", control_layer %in% c("0_fe_only", light_mix_control_layers)),
    aes(x = event_period_index, y = estimate, color = control_layer_label, group = control_layer_label)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(event_plot_df, outcome_id == "units_built_5_plus", outcome_scale == "per_10000_occupied_1990", control_layer %in% c("0_fe_only", light_mix_control_layers), !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.35, alpha = 0.75) +
    geom_line(linewidth = 0.65) +
    geom_point(size = 1.8) +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    guides(color = guide_legend(nrow = 2, byrow = TRUE)) +
    labs(title = "FE-only and light mixes: 5+ unit buildings", x = NULL, y = "Coefficient on homeowner exposure", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(five_plus_light_mix_event_plot_df, outcome_scale == "per_residential_acre"),
    aes(x = event_period_index, y = estimate, color = control_layer_label, group = control_layer_label)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(five_plus_light_mix_event_plot_df, outcome_scale == "per_residential_acre", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.35, alpha = 0.75) +
    geom_line(linewidth = 0.65) +
    geom_point(size = 1.8) +
    scale_color_manual(values = light_mix_colors) +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    guides(color = guide_legend(nrow = 2, byrow = TRUE)) +
    labs(title = "Light control mixes: 5+ unit buildings per residential acre", x = NULL, y = "Coefficient on homeowner exposure", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

five_plus_four_controls_event_plot_df <- event_plot_df |>
  filter(
    outcome_id == "units_built_5_plus",
    control_layer == "1_light_controls"
  )

one_four_five_plus_four_controls_event_plot_df <- event_plot_df |>
  filter(
    outcome_id %in% c("units_built_1_4", "units_built_5_plus"),
    outcome_scale == "per_10000_occupied_1990",
    control_layer == "1_light_controls"
  ) |>
  mutate(outcome_label = factor(outcome_label, levels = c("1-4 unit buildings", "5+ unit buildings")))

margin_dodge <- position_dodge(width = 0.28)
margin_colors <- c("1-4 unit buildings" = "#666666", "5+ unit buildings" = "#2f7d32")

pdf("../output/cd_homeownership_long_units_event_coefficients_5_plus_four_controls.pdf", width = 11, height = 8.5)
print(
  ggplot(
    one_four_five_plus_four_controls_event_plot_df,
    aes(x = event_period_index, y = estimate, color = outcome_label, group = outcome_label)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(one_four_five_plus_four_controls_event_plot_df, !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.45, position = margin_dodge) +
    geom_line(linewidth = 0.75, position = margin_dodge) +
    geom_point(size = 2.1, position = margin_dodge) +
    scale_color_manual(values = margin_colors) +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "Four-control event study: 1-4 vs 5+ unit buildings", x = NULL, y = "Coefficient on homeowner exposure", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(five_plus_four_controls_event_plot_df, outcome_scale == "per_residential_acre"),
    aes(x = event_period_index, y = estimate, group = 1)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(five_plus_four_controls_event_plot_df, outcome_scale == "per_residential_acre", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.45, color = "#2f7d32") +
    geom_line(linewidth = 0.75, color = "#2f7d32") +
    geom_point(size = 2.1, color = "#2f7d32") +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "Four-control event study: 5+ unit buildings per residential acre", x = NULL, y = "Coefficient on homeowner exposure") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

event_5yr_plot_df <- event_coefficients_5yr_df |>
  mutate(
    event_period = as.character(event_period),
    event_period_index = match(event_period, event_periods_5yr)
  )

one_four_five_plus_four_controls_event_5yr_plot_df <- event_5yr_plot_df |>
  filter(
    outcome_id %in% c("units_built_1_4", "units_built_5_plus"),
    outcome_scale == "per_10000_occupied_1990",
    control_layer == "1_light_controls"
  ) |>
  mutate(outcome_label = factor(outcome_label, levels = c("1-4 unit buildings", "5+ unit buildings")))

five_plus_four_controls_event_5yr_plot_df <- event_5yr_plot_df |>
  filter(
    outcome_id == "units_built_5_plus",
    control_layer == "1_light_controls"
  )

pdf("../output/cd_homeownership_long_units_event_coefficients_5_plus_four_controls_5yr_bins.pdf", width = 11, height = 8.5)
print(
  ggplot(
    one_four_five_plus_four_controls_event_5yr_plot_df,
    aes(x = event_period_index, y = estimate, color = outcome_label, group = outcome_label)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(one_four_five_plus_four_controls_event_5yr_plot_df, !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.45, position = margin_dodge) +
    geom_line(linewidth = 0.75, position = margin_dodge) +
    geom_point(size = 2.1, position = margin_dodge) +
    scale_color_manual(values = margin_colors) +
    scale_x_continuous(breaks = seq_along(event_periods_5yr), labels = event_periods_5yr) +
    labs(title = "Four-control event study, five-year bins: 1-4 vs 5+ unit buildings", x = NULL, y = "Coefficient on homeowner exposure", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(five_plus_four_controls_event_5yr_plot_df, outcome_scale == "per_residential_acre"),
    aes(x = event_period_index, y = estimate, group = 1)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(five_plus_four_controls_event_5yr_plot_df, outcome_scale == "per_residential_acre", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.45, color = "#2f7d32") +
    geom_line(linewidth = 0.75, color = "#2f7d32") +
    geom_point(size = 2.1, color = "#2f7d32") +
    scale_x_continuous(breaks = seq_along(event_periods_5yr), labels = event_periods_5yr) +
    labs(title = "Four-control event study, five-year bins: 5+ unit buildings per residential acre", x = NULL, y = "Coefficient on homeowner exposure") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

light_no_preprod_event_plot_df <- event_plot_df |>
  filter(control_layer == "1_light_no_preprod")

pdf("../output/cd_homeownership_long_units_event_coefficients_light_no_preprod.pdf", width = 11, height = 8.5)
print(
  ggplot(
    filter(light_no_preprod_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_10000_occupied_1990"),
    aes(x = event_period_index, y = estimate, group = 1)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(light_no_preprod_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_10000_occupied_1990", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.45, color = "#b45f06") +
    geom_line(linewidth = 0.75, color = "#b45f06") +
    geom_point(size = 2.1, color = "#b45f06") +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "Light-controls event study without pre-period production: 50+ unit buildings", x = NULL, y = "Coefficient on homeowner exposure") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(light_no_preprod_event_plot_df, outcome_id %in% c("units_built_1_4", "units_built_5_49", "units_built_50_plus"), outcome_scale == "per_10000_occupied_1990"),
    aes(x = event_period_index, y = estimate, group = 1)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(light_no_preprod_event_plot_df, outcome_id %in% c("units_built_1_4", "units_built_5_49", "units_built_50_plus"), outcome_scale == "per_10000_occupied_1990", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.45, color = "#b45f06") +
    geom_line(linewidth = 0.75, color = "#b45f06") +
    geom_point(size = 2.1, color = "#b45f06") +
    facet_wrap(~outcome_label, scales = "free_y") +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "Light-controls size-margin diagnostics without pre-period production", x = NULL, y = "Coefficient on homeowner exposure") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(light_no_preprod_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_residential_acre"),
    aes(x = event_period_index, y = estimate, group = 1)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(light_no_preprod_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_residential_acre", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.45, color = "#b45f06") +
    geom_line(linewidth = 0.75, color = "#b45f06") +
    geom_point(size = 2.1, color = "#b45f06") +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "Light-controls residential-acre robustness without pre-period production", x = NULL, y = "Coefficient on homeowner exposure") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

light_event_plot_df <- event_plot_df |>
  filter(control_layer == "1_light_controls")

pdf("../output/cd_homeownership_long_units_event_coefficients_light_controls.pdf", width = 11, height = 8.5)
print(
  ggplot(
    filter(light_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_10000_occupied_1990"),
    aes(x = event_period_index, y = estimate, group = 1)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(light_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_10000_occupied_1990", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.45, color = "#2f7d32") +
    geom_line(linewidth = 0.75, color = "#2f7d32") +
    geom_point(size = 2.1, color = "#2f7d32") +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "Light-controls event study: 50+ unit buildings", x = NULL, y = "Coefficient on homeowner exposure") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(light_event_plot_df, outcome_id %in% c("units_built_1_4", "units_built_5_49", "units_built_50_plus"), outcome_scale == "per_10000_occupied_1990"),
    aes(x = event_period_index, y = estimate, group = 1)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(light_event_plot_df, outcome_id %in% c("units_built_1_4", "units_built_5_49", "units_built_50_plus"), outcome_scale == "per_10000_occupied_1990", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.45, color = "#2f7d32") +
    geom_line(linewidth = 0.75, color = "#2f7d32") +
    geom_point(size = 2.1, color = "#2f7d32") +
    facet_wrap(~outcome_label, scales = "free_y") +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "Light-controls size-margin event-study diagnostics", x = NULL, y = "Coefficient on homeowner exposure") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(light_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_residential_acre"),
    aes(x = event_period_index, y = estimate, group = 1)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(light_event_plot_df, outcome_id == "units_built_50_plus", outcome_scale == "per_residential_acre", !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.45, color = "#2f7d32") +
    geom_line(linewidth = 0.75, color = "#2f7d32") +
    geom_point(size = 2.1, color = "#2f7d32") +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "Light-controls residential-acre robustness: 50+ unit buildings", x = NULL, y = "Coefficient on homeowner exposure") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

event_poisson_plot_df <- event_poisson_coefficients_df |>
  mutate(
    event_period = as.character(event_period),
    event_period_index = match(event_period, event_periods)
  )

event_poisson_fe_only_plot_df <- event_poisson_plot_df |>
  filter(control_layer == "0_fe_only")

pdf("../output/cd_homeownership_long_units_event_poisson_coefficients_fe_only.pdf", width = 11, height = 8.5)
print(
  ggplot(
    filter(event_poisson_fe_only_plot_df, outcome_id == "units_built_50_plus"),
    aes(x = event_period_index, y = percent_estimate, group = 1)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(event_poisson_fe_only_plot_df, outcome_id == "units_built_50_plus", !is_reference), aes(ymin = percent_conf_low, ymax = percent_conf_high), width = 0.12, linewidth = 0.45, color = "#1f5fbf") +
    geom_line(linewidth = 0.75, color = "#1f5fbf") +
    geom_point(size = 2.1, color = "#1f5fbf") +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "FE-only PPML event study: 50+ unit buildings", x = NULL, y = "Percent change in expected count") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(event_poisson_fe_only_plot_df, outcome_id %in% c("units_built_1_4", "units_built_5_49", "units_built_50_plus")),
    aes(x = event_period_index, y = percent_estimate, group = 1)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(event_poisson_fe_only_plot_df, outcome_id %in% c("units_built_1_4", "units_built_5_49", "units_built_50_plus"), !is_reference), aes(ymin = percent_conf_low, ymax = percent_conf_high), width = 0.12, linewidth = 0.45, color = "#1f5fbf") +
    geom_line(linewidth = 0.75, color = "#1f5fbf") +
    geom_point(size = 2.1, color = "#1f5fbf") +
    facet_wrap(~outcome_label, scales = "free_y") +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "FE-only PPML size-margin event-study diagnostics", x = NULL, y = "Percent change in expected count") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
print(
  ggplot(
    filter(event_poisson_fe_only_plot_df, outcome_id == "projects_built_50_plus"),
    aes(x = event_period_index, y = percent_estimate, group = 1)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(event_poisson_fe_only_plot_df, outcome_id == "projects_built_50_plus", !is_reference), aes(ymin = percent_conf_low, ymax = percent_conf_high), width = 0.12, linewidth = 0.45, color = "#1f5fbf") +
    geom_line(linewidth = 0.75, color = "#1f5fbf") +
    geom_point(size = 2.1, color = "#1f5fbf") +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "FE-only PPML event study: 50+ unit projects", x = NULL, y = "Percent change in expected count") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

event_poisson_5_plus_four_controls_plot_df <- event_poisson_plot_df |>
  filter(
    outcome_id == "units_built_5_plus",
    control_layer == "1_light_controls"
  )

pdf("../output/cd_homeownership_long_units_event_poisson_coefficients_5_plus_four_controls.pdf", width = 11, height = 8.5)
print(
  ggplot(event_poisson_5_plus_four_controls_plot_df, aes(x = event_period_index, y = percent_estimate, group = 1)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(event_poisson_5_plus_four_controls_plot_df, !is_reference), aes(ymin = percent_conf_low, ymax = percent_conf_high), width = 0.12, linewidth = 0.45, color = "#2f7d32") +
    geom_line(linewidth = 0.75, color = "#2f7d32") +
    geom_point(size = 2.1, color = "#2f7d32") +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "Four-control PPML event study: 5+ unit buildings", x = NULL, y = "Percent change in expected count") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

event_poisson_5yr_plot_df <- event_poisson_coefficients_5yr_df |>
  mutate(
    event_period = as.character(event_period),
    event_period_index = match(event_period, event_periods_5yr)
  )

one_four_five_plus_four_controls_poisson_5yr_plot_df <- event_poisson_5yr_plot_df |>
  filter(outcome_id %in% c("units_built_1_4", "units_built_5_plus")) |>
  mutate(outcome_label = factor(outcome_label, levels = c("1-4 unit buildings", "5+ unit buildings")))

pdf("../output/cd_homeownership_long_units_event_poisson_coefficients_5_plus_four_controls_5yr_bins.pdf", width = 11, height = 8.5)
print(
  ggplot(
    one_four_five_plus_four_controls_poisson_5yr_plot_df,
    aes(x = event_period_index, y = percent_estimate, color = outcome_label, group = outcome_label)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(one_four_five_plus_four_controls_poisson_5yr_plot_df, !is_reference), aes(ymin = percent_conf_low, ymax = percent_conf_high), width = 0.12, linewidth = 0.45, position = margin_dodge) +
    geom_line(linewidth = 0.75, position = margin_dodge) +
    geom_point(size = 2.1, position = margin_dodge) +
    scale_color_manual(values = margin_colors) +
    scale_x_continuous(breaks = seq_along(event_periods_5yr), labels = event_periods_5yr) +
    labs(title = "Four-control PPML event study, five-year bins: 1-4 vs 5+ unit buildings", x = NULL, y = "Percent change in expected count", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

triple_plot_df <- triple_diff_df |>
  mutate(
    event_period = as.character(event_period),
    event_period_index = match(event_period, event_periods)
  )

pdf("../output/cd_homeownership_long_units_triple_diff_coefficients.pdf", width = 10, height = 7)
print(
  ggplot(triple_plot_df, aes(x = event_period_index, y = estimate, color = outcome_label, group = outcome_label)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(data = filter(triple_plot_df, !is_reference), aes(ymin = conf_low, ymax = conf_high), width = 0.12, linewidth = 0.35, alpha = 0.75) +
    geom_line(linewidth = 0.65) +
    geom_point(size = 1.8) +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(title = "Triple-difference size-margin diagnostic", x = NULL, y = "Differential coefficient versus 1-4 unit buildings", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

expected_output_paths <- c(
  "../output/cd_homeownership_long_units_event_coefficients.csv",
  "../output/cd_homeownership_long_units_event_coefficients.pdf",
  "../output/cd_homeownership_long_units_event_coefficients_fe_only.pdf",
  "../output/cd_homeownership_long_units_event_coefficients_light_mix.pdf",
  "../output/cd_homeownership_long_units_event_coefficients_5_plus_light_mix.pdf",
  "../output/cd_homeownership_long_units_event_coefficients_5_plus_four_controls.pdf",
  "../output/cd_homeownership_long_units_event_coefficients_5yr_bins.csv",
  "../output/cd_homeownership_long_units_event_coefficients_5_plus_four_controls_5yr_bins.pdf",
  "../output/cd_homeownership_long_units_event_coefficients_light_no_preprod.pdf",
  "../output/cd_homeownership_long_units_event_coefficients_light_controls.pdf",
  "../output/cd_homeownership_long_units_event_poisson_coefficients.csv",
  "../output/cd_homeownership_long_units_event_poisson_coefficients_fe_only.pdf",
  "../output/cd_homeownership_long_units_event_poisson_coefficients_5_plus_four_controls.pdf",
  "../output/cd_homeownership_long_units_event_poisson_coefficients_5yr_bins.csv",
  "../output/cd_homeownership_long_units_event_poisson_coefficients_5_plus_four_controls_5yr_bins.pdf",
  "../output/cd_homeownership_long_units_long_difference_estimates.csv",
  "../output/cd_homeownership_long_units_long_difference_5_plus_four_controls.tex",
  "../output/cd_homeownership_long_units_event_5_plus_four_controls_macros.tex",
  "../output/cd_homeownership_long_units_triple_diff_coefficients.csv",
  "../output/cd_homeownership_long_units_triple_diff_coefficients.pdf",
  "../output/cd_homeownership_long_units_randomization_inference.csv",
  "../output/cd_homeownership_long_units_model_summary.csv"
)

output_nonempty_count <- sum(file.exists(expected_output_paths) & file.info(expected_output_paths)$size > 0)
missing_event_terms <- event_coefficients_df |> filter(!is_reference, is.na(estimate)) |> nrow()
missing_event_5yr_terms <- event_coefficients_5yr_df |> filter(!is_reference, is.na(estimate)) |> nrow()
missing_poisson_event_terms <- event_poisson_coefficients_df |> filter(!is_reference, is.na(percent_estimate)) |> nrow()
missing_poisson_event_5yr_terms <- event_poisson_coefficients_5yr_df |> filter(!is_reference, is.na(percent_estimate)) |> nrow()
missing_triple_terms <- triple_diff_df |> filter(!is_reference, is.na(estimate)) |> nrow()
missing_long_diff_terms <- long_difference_df |> filter(is.na(estimate)) |> nrow()
missing_expected_event_periods <- setdiff(event_periods, as.character(unique(analysis_panel$event_period)))
missing_expected_event_periods_5yr <- setdiff(event_periods_5yr, as.character(unique(analysis_panel_5yr$event_period)))

qc_df <- bind_rows(
  tibble(metric = "district_count", value = as.character(n_distinct(panel_df$borocd)), note = "Standard community districts in the design panel."),
  tibble(metric = "year_min", value = as.character(min(panel_df$year, na.rm = TRUE)), note = "Minimum year in the design panel."),
  tibble(metric = "year_max", value = as.character(max(panel_df$year, na.rm = TRUE)), note = "Maximum year in the design panel."),
  tibble(metric = "event_period_count", value = as.character(n_distinct(panel_df$event_period)), note = "Distinct event-study bins represented."),
  tibble(metric = "missing_expected_event_period_count", value = as.character(length(missing_expected_event_periods)), note = "Expected event-study bins absent from the panel."),
  tibble(metric = "event_period_5yr_count", value = as.character(n_distinct(analysis_panel_5yr$event_period)), note = "Distinct five-year event-study bins represented."),
  tibble(metric = "missing_expected_event_period_5yr_count", value = as.character(length(missing_expected_event_periods_5yr)), note = "Expected five-year event-study bins absent from the panel."),
  tibble(metric = "missing_treatment_1990_count", value = as.character(sum(is.na(panel_df$treat_z_boro))), note = "Rows missing 1990 within-borough homeownership exposure."),
  tibble(metric = "missing_treatment_1980_count", value = as.character(sum(is.na(panel_df$homeowner_share_1980_z_boro))), note = "Rows missing 1980 within-borough homeownership exposure."),
  tibble(metric = "missing_control_cell_count", value = as.character(missing_control_cell_count), note = "Raw control cells missing before standardization."),
  tibble(metric = "negative_5_49_count", value = as.character(negative_5_49_count), note = "CD-years where 5+ units minus 50+ units was negative before truncation."),
  tibble(metric = "event_missing_treat_term_count", value = as.character(missing_event_terms), note = "Requested event-study treatment terms missing from output."),
  tibble(metric = "event_5yr_missing_treat_term_count", value = as.character(missing_event_5yr_terms), note = "Requested five-year event-study treatment terms missing from output."),
  tibble(metric = "poisson_event_missing_treat_term_count", value = as.character(missing_poisson_event_terms), note = "Requested PPML event-study treatment terms missing from output."),
  tibble(metric = "poisson_event_5yr_missing_treat_term_count", value = as.character(missing_poisson_event_5yr_terms), note = "Requested five-year PPML event-study treatment terms missing from output."),
  tibble(metric = "triple_diff_missing_treat_term_count", value = as.character(missing_triple_terms), note = "Requested triple-difference treatment terms missing from output."),
  tibble(metric = "long_difference_missing_treat_term_count", value = as.character(missing_long_diff_terms), note = "Long-difference treatment rows missing estimates."),
  tibble(metric = "randomization_row_count", value = as.character(nrow(randomization_df)), note = "Randomization-inference rows for main 50+ long differences."),
  tibble(metric = "output_nonempty_count", value = as.character(output_nonempty_count), note = "Expected non-QC outputs that exist and are nonempty."),
  tibble(metric = "expected_output_count", value = as.character(length(expected_output_paths)), note = "Expected non-QC outputs.")
)

status_flag <- n_distinct(panel_df$borocd) == 59 &&
  min(panel_df$year, na.rm = TRUE) == 1980 &&
  max(panel_df$year, na.rm = TRUE) == 2025 &&
  length(missing_expected_event_periods) == 0 &&
  length(missing_expected_event_periods_5yr) == 0 &&
  sum(is.na(panel_df$treat_z_boro)) == 0 &&
  sum(is.na(panel_df$homeowner_share_1980_z_boro)) == 0 &&
  missing_control_cell_count == 0 &&
  negative_5_49_count == 0 &&
  missing_event_terms == 0 &&
  missing_event_5yr_terms == 0 &&
  missing_poisson_event_terms == 0 &&
  missing_poisson_event_5yr_terms == 0 &&
  missing_triple_terms == 0 &&
  missing_long_diff_terms == 0 &&
  nrow(randomization_df) == 4 &&
  output_nonempty_count == length(expected_output_paths)

qc_df <- bind_rows(
  qc_df,
  tibble(metric = "status", value = as.character(as.integer(status_flag)), note = "One means the revised long-units design task passed all QC checks.")
)

write_csv_if_changed(qc_df, "../output/cd_homeownership_long_units_design_qc.csv")

if (!status_flag) {
  stop("Revised long-units design QC failed; see ", "../output/cd_homeownership_long_units_design_qc.csv")
}

cat("Wrote revised homeownership long-units design outputs to", dirname("../output/cd_homeownership_long_units_event_coefficients.csv"), "\n")
