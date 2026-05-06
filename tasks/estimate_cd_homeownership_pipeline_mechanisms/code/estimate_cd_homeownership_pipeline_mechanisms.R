# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/estimate_cd_homeownership_pipeline_mechanisms/code")

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

zap_period_from_year <- function(year_value) {
  case_when(
    year_value >= 1976 & year_value <= 1979 ~ "1976-1979",
    year_value >= 1980 & year_value <= 1984 ~ "1980-1984",
    year_value >= 1985 & year_value <= 1989 ~ "1985-1989",
    year_value >= 1990 & year_value <= 1999 ~ "1990-1999",
    year_value >= 2000 & year_value <= 2009 ~ "2000-2009",
    year_value >= 2010 & year_value <= 2019 ~ "2010-2019",
    year_value >= 2020 & year_value <= 2025 ~ "2020-2025",
    TRUE ~ NA_character_
  )
}

status_period_from_year <- function(year_value) {
  case_when(
    year_value >= 1976 & year_value <= 1979 ~ "1976-1979",
    year_value >= 1980 & year_value <= 1984 ~ "1980-1984",
    year_value >= 1985 & year_value <= 1989 ~ "1985-1989",
    year_value >= 1990 & year_value <= 1999 ~ "1990-1999",
    year_value >= 2000 & year_value <= 2009 ~ "2000-2009",
    year_value >= 2010 & year_value <= 2015 ~ "2010-2015",
    TRUE ~ NA_character_
  )
}

permit_period_from_year <- function(year_value) {
  case_when(
    year_value == 1989 ~ "1989",
    year_value >= 1990 & year_value <= 1999 ~ "1990-1999",
    year_value >= 2000 & year_value <= 2009 ~ "2000-2009",
    year_value >= 2010 & year_value <= 2019 ~ "2010-2019",
    year_value >= 2020 & year_value <= 2025 ~ "2020-2025",
    TRUE ~ NA_character_
  )
}

sanitize_period <- function(x) {
  str_replace_all(x, "-", "_")
}

nonempty_text <- function(x) {
  !is.na(x) & str_squish(as.character(x)) != ""
}

has_ulurp_number_action_code <- function(ulurp_numbers, action_codes) {
  action_pattern <- paste(action_codes, collapse = "|")
  str_detect(
    str_to_upper(coalesce(ulurp_numbers, "")),
    paste0("[A-Z][0-9]{6}A?(", action_pattern, ")[MKQXR]")
  )
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

within_r2 <- function(model) {
  tryCatch(as.numeric(r2(model, type = "wr2")), error = function(e) NA_real_)
}

requested_term_df <- function(period_values) {
  tibble(
    event_period = period_values,
    term = paste0("treat_z_boro_x_", sanitize_period(period_values))
  )
}

extract_terms <- function(model, requested_terms) {
  requested_terms |>
    left_join(coeftable_df(model), by = "term") |>
    left_join(confint_df(model), by = "term") |>
    mutate(
      model_status = if_else(is.na(estimate), "requested_term_dropped", "estimated"),
      model_message = if_else(
        model_status == "requested_term_dropped",
        paste0("Requested term was dropped by fixest: ", term),
        NA_character_
      )
    )
}

reference_rows <- function(template_df, reference_period) {
  template_df |>
    distinct(
      analysis_family, outcome_id, outcome_label, outcome_scale, outcome_scale_label,
      control_layer, control_label, reference_period, n_obs, n_cd
    ) |>
    mutate(
      event_period = reference_period,
      term = paste0("reference_", sanitize_period(reference_period)),
      estimate = 0,
      std_error = NA_real_,
      statistic = NA_real_,
      p_value = NA_real_,
      conf_low = NA_real_,
      conf_high = NA_real_,
      model_status = "reference_period",
      model_message = NA_character_
    )
}

failed_term_rows <- function(requested_terms, analysis_family, outcome_id, outcome_label, outcome_scale, outcome_scale_label, control_layer, control_label, reference_period, n_obs, n_cd, model_status, model_message) {
  requested_terms |>
    mutate(
      analysis_family = analysis_family,
      outcome_id = outcome_id,
      outcome_label = outcome_label,
      outcome_scale = outcome_scale,
      outcome_scale_label = outcome_scale_label,
      control_layer = control_layer,
      control_label = control_label,
      reference_period = reference_period,
      n_obs = n_obs,
      n_cd = n_cd,
      estimate = NA_real_,
      std_error = NA_real_,
      statistic = NA_real_,
      p_value = NA_real_,
      conf_low = NA_real_,
      conf_high = NA_real_,
      model_status = model_status,
      model_message = model_message
    )
}

run_event_model <- function(df, analysis_family, target_outcome_id, target_outcome_label, target_outcome_scale, target_outcome_scale_label, period_values, reference_period, control_layer, control_label, control_cols) {
  model_df <- df |>
    filter(
      .data$outcome_id == .env$target_outcome_id,
      .data$outcome_scale == .env$target_outcome_scale,
      .data$event_period %in% period_values,
      !is.na(.data$outcome_value),
      !is.na(.data$treat_z_boro),
      !is.na(.data$borough_period),
      !is.na(.data$borocd)
    )

  if (length(control_cols) > 0) {
    model_df <- model_df |>
      filter(if_all(all_of(control_cols), ~ !is.na(.x)))
  }

  estimated_periods <- setdiff(period_values, reference_period)
  requested_terms <- requested_term_df(estimated_periods)
  n_obs <- nrow(model_df)
  n_cd <- n_distinct(model_df$borocd)

  if (n_obs == 0 || n_cd < 2 || !(reference_period %in% model_df$event_period)) {
    return(failed_term_rows(
      requested_terms, analysis_family, target_outcome_id, target_outcome_label, target_outcome_scale, target_outcome_scale_label,
      control_layer, control_label, reference_period, n_obs, n_cd,
      "insufficient_sample",
      "Model was not estimated because the sample has no observations, fewer than two CDs, or no reference period."
    ))
  }

  if (n_distinct(model_df$outcome_value, na.rm = TRUE) < 2) {
    return(failed_term_rows(
      requested_terms, analysis_family, target_outcome_id, target_outcome_label, target_outcome_scale, target_outcome_scale_label,
      control_layer, control_label, reference_period, n_obs, n_cd,
      "constant_outcome",
      "Model was not estimated because the dependent variable is constant in the analysis sample."
    ))
  }

  model_df <- add_period_terms(model_df, c("treat_z_boro", control_cols), estimated_periods)
  requested_treat_terms <- requested_terms$term
  requested_control_terms <- unlist(lapply(control_cols, function(control_col) paste0(control_col, "_x_", sanitize_period(estimated_periods))))
  rhs_terms <- c(requested_treat_terms, requested_control_terms)

  model_formula <- as.formula(paste("outcome_value ~", paste(rhs_terms, collapse = " + "), "| borocd + borough_period"))

  model <- tryCatch(
    feols(model_formula, data = model_df, cluster = ~borocd, warn = FALSE, notes = FALSE),
    error = function(e) e
  )

  if (inherits(model, "error")) {
    return(failed_term_rows(
      requested_terms, analysis_family, target_outcome_id, target_outcome_label, target_outcome_scale, target_outcome_scale_label,
      control_layer, control_label, reference_period, n_obs, n_cd,
      "model_error",
      model$message
    ))
  }

  out <- extract_terms(model, requested_terms) |>
    mutate(
      analysis_family = analysis_family,
      outcome_id = target_outcome_id,
      outcome_label = target_outcome_label,
      outcome_scale = target_outcome_scale,
      outcome_scale_label = target_outcome_scale_label,
      control_layer = control_layer,
      control_label = control_label,
      reference_period = reference_period,
      n_obs = nobs(model),
      n_cd = n_distinct(model_df$borocd),
      within_r2 = within_r2(model)
    )

  bind_rows(
    out,
    reference_rows(out, reference_period)
  ) |>
    select(
      analysis_family, outcome_id, outcome_label, outcome_scale, outcome_scale_label,
      control_layer, control_label, reference_period, event_period, term,
      estimate, std_error, statistic, p_value, conf_low, conf_high,
      n_obs, n_cd, within_r2, model_status, model_message
    )
}

build_plot <- function(coef_df, out_path, title_text, y_axis_text) {
  plot_df <- coef_df |>
    filter(model_status %in% c("estimated", "reference_period")) |>
    mutate(
      event_period = factor(event_period, levels = unique(event_period[order(match(event_period, c("1976-1979", "1980-1984", "1985-1989", "1990-1999", "2000-2009", "2010-2015", "2010-2019", "2020-2025")))])),
      control_label = factor(control_label, levels = c("FE only", "Baseline controls"))
    )

  dodge <- position_dodge(width = 0.45)

  ggplot(plot_df, aes(x = event_period, y = estimate, color = control_label, group = control_label)) +
    geom_hline(yintercept = 0, linetype = "dashed", linewidth = 0.3, color = "gray45") +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), position = dodge, width = 0.15, linewidth = 0.35, na.rm = TRUE) +
    geom_line(position = dodge, linewidth = 0.45) +
    geom_point(position = dodge, size = 1.25) +
    facet_grid(outcome_label ~ outcome_scale_label, scales = "free_y") +
    scale_color_manual(values = c("FE only" = "#666666", "Baseline controls" = "#2C7FB8")) +
    labs(
      title = title_text,
      x = NULL,
      y = y_axis_text,
      color = NULL
    ) +
    theme_minimal(base_size = 9) +
    theme(
      panel.grid.minor = element_blank(),
      legend.position = "bottom",
      axis.text.x = element_text(angle = 45, hjust = 1),
      strip.text.y = element_text(angle = 0, hjust = 0),
      plot.title = element_text(face = "bold", size = 11)
    )

  ggsave(out_path, width = 13, height = 14, units = "in")
}

zap_periods <- c("1976-1979", "1980-1984", "1985-1989", "1990-1999", "2000-2009", "2010-2019", "2020-2025")
zap_reference_period <- "1985-1989"
status_periods <- c("1976-1979", "1980-1984", "1985-1989", "1990-1999", "2000-2009", "2010-2015")
permit_periods <- c("1990-1999", "2000-2009", "2010-2019", "2020-2025")
permit_reference_period <- "1990-1999"

baseline_df <- read_csv("../input/cd_redevelopment_potential_baseline.csv", show_col_types = FALSE, na = c("", "NA"))
zap_project_df <- read_csv("../input/zap_ulurp_redev_project_base.csv", col_types = cols(.default = col_character()), na = c("", "NA"))
zap_cd_year_df <- read_csv("../input/zap_ulurp_redev_cd_year_panel.csv", show_col_types = FALSE, na = c("", "NA"))
zap_mature_df <- read_csv("../input/zap_ulurp_redev_mature_cohort_panel.csv", show_col_types = FALSE, na = c("", "NA"))
zap_yield_df <- read_csv("../input/zap_ulurp_redev_yield_panel.csv", show_col_types = FALSE, na = c("", "NA"))
permit_df <- read_csv("../input/cd_homeownership_permit_nb_panel.csv", show_col_types = FALSE, na = c("", "NA"))

assert_required_columns(
  baseline_df,
  c("borocd", "residential_acres", "occupied_units_1990", "median_household_income_1990_1999_dollars_exact", "poverty_share_1990_exact"),
  "CD redevelopment-potential baseline"
)

assert_required_columns(
  zap_cd_year_df,
  c(
    "borocd", "cert_year", "borough_name", "occupied_units_1990", "residential_acres", "treat_z_boro",
    "initial_apps", "private_initial_apps", "public_initial_apps", "mixed_private_rezoning_apps",
    "public_hpd_apps", "rezoning_or_special_apps", "public_land_or_disposition_apps"
  ),
  "ZAP CD-year panel"
)

assert_required_columns(
  zap_project_df,
  c(
    "project_id", "borocd", "cert_year", "borough_name", "certified_referred_date", "approval_date",
    "completed_date", "treat_z_boro", "project_status", "public_status", "actions", "ulurp_numbers",
    "rezoning_or_special_proxy", "public_land_or_disposition_proxy"
  ),
  "ZAP project base"
)

assert_required_columns(
  zap_mature_df,
  c("borocd", "cert_year", "borough_name", "occupied_units_1990", "residential_acres", "treat_z_boro", "complete_apps", "failed_apps", "unresolved_apps", "completion_share", "failure_share", "unresolved_share"),
  "ZAP mature-cohort panel"
)

assert_required_columns(
  zap_yield_df,
  c("borocd", "cert_year", "borough_name", "initial_apps", "linked_nb_50_plus_rate_0_10", "linked_gross_add_units_per_app_0_10"),
  "ZAP yield panel"
)

assert_required_columns(
  permit_df,
  c("borocd", "borough_name", "year", "outcome_value", "outcome_family", "occupied_units_1990", "treat_z_boro"),
  "DOB new-building permit panel"
)

baseline_clean <- baseline_df |>
  transmute(
    borocd = suppressWarnings(as.integer(borocd)),
    residential_acres_baseline = suppressWarnings(as.numeric(residential_acres)),
    occupied_units_1990_baseline = suppressWarnings(as.numeric(occupied_units_1990)),
    median_household_income_1990_1999_dollars_exact = suppressWarnings(as.numeric(median_household_income_1990_1999_dollars_exact)),
    poverty_share_1990_exact = suppressWarnings(as.numeric(poverty_share_1990_exact)),
    log_occupied_units_1990 = log(occupied_units_1990_baseline)
  ) |>
  mutate(
    median_household_income_1990_z = z_score(median_household_income_1990_1999_dollars_exact),
    poverty_share_1990_z = z_score(poverty_share_1990_exact),
    log_occupied_units_1990_z = z_score(log_occupied_units_1990)
  )

assert_unique_keys(baseline_clean, "borocd", "CD redevelopment-potential baseline")
assert_unique_keys(zap_cd_year_df, c("borocd", "cert_year"), "ZAP CD-year panel")
assert_unique_keys(zap_mature_df, c("borocd", "cert_year"), "ZAP mature-cohort panel")
assert_unique_keys(permit_df, c("borocd", "year", "outcome_family"), "DOB new-building permit panel")

application_defs <- tribble(
  ~outcome_id, ~outcome_label, ~count_col,
  "all_housing_ulurp_apps", "All housing ULURP applications", "initial_apps",
  "private_apps", "Private applicant applications", "private_initial_apps",
  "public_apps", "Public applicant applications", "public_initial_apps",
  "public_hpd_apps", "Public/HPD proxy applications", "public_hpd_apps",
  "private_rezoning_special_apps", "Private rezoning/special permit proxy", "mixed_private_rezoning_apps",
  "rezoning_special_apps", "Rezoning/special permit proxy", "rezoning_or_special_apps",
  "public_land_disposition_apps", "Public land/disposition proxy", "public_land_or_disposition_apps"
)

scale_defs <- tribble(
  ~outcome_scale, ~outcome_scale_label,
  "per_10000_occupied_1990", "Per 10,000 occupied units",
  "per_residential_acre", "Per residential acre"
)

zap_application_panel <- zap_cd_year_df |>
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    cert_year = suppressWarnings(as.integer(cert_year)),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990)),
    residential_acres = suppressWarnings(as.numeric(residential_acres)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro)),
    event_period = zap_period_from_year(cert_year),
    borough_period = interaction(borough_name, event_period, drop = TRUE)
  ) |>
  left_join(
    baseline_clean |> select(borocd, median_household_income_1990_z, poverty_share_1990_z, log_occupied_units_1990_z),
    by = "borocd",
    relationship = "many-to-one"
  ) |>
  select(
    borocd, cert_year, borough_name, event_period, borough_period, occupied_units_1990, residential_acres,
    treat_z_boro, median_household_income_1990_z, poverty_share_1990_z, log_occupied_units_1990_z,
    all_of(application_defs$count_col)
  ) |>
  pivot_longer(all_of(application_defs$count_col), names_to = "count_col", values_to = "count_value") |>
  left_join(application_defs, by = "count_col", relationship = "many-to-one") |>
  mutate(
    count_value = suppressWarnings(as.numeric(count_value)),
    per_10000_occupied_1990 = if_else(occupied_units_1990 > 0, 10000 * count_value / occupied_units_1990, NA_real_),
    per_residential_acre = if_else(residential_acres > 0, count_value / residential_acres, NA_real_)
  ) |>
  pivot_longer(c(per_10000_occupied_1990, per_residential_acre), names_to = "outcome_scale", values_to = "outcome_value") |>
  left_join(scale_defs, by = "outcome_scale", relationship = "many-to-one") |>
  filter(!is.na(event_period))

zap_pre_controls <- zap_application_panel |>
  filter(cert_year >= 1980, cert_year <= 1988) |>
  group_by(borocd, outcome_id, outcome_scale) |>
  summarise(pre_1980_1988_outcome = mean(outcome_value, na.rm = TRUE), .groups = "drop") |>
  group_by(outcome_id, outcome_scale) |>
  mutate(pre_1980_1988_outcome_z = z_score(pre_1980_1988_outcome)) |>
  ungroup()

zap_application_panel <- zap_application_panel |>
  left_join(zap_pre_controls, by = c("borocd", "outcome_id", "outcome_scale"), relationship = "many-to-one")

application_control_layers <- list(
  `0_fe_only` = list(label = "FE only", controls = character()),
  `1_baseline_controls` = list(label = "Baseline controls", controls = c("median_household_income_1990_z", "poverty_share_1990_z", "log_occupied_units_1990_z", "pre_1980_1988_outcome_z"))
)

event_coefficients <- tibble()

for (outcome_row in seq_len(nrow(application_defs))) {
  for (scale_row in seq_len(nrow(scale_defs))) {
    for (control_name in names(application_control_layers)) {
      event_coefficients <- bind_rows(
        event_coefficients,
        run_event_model(
          zap_application_panel,
          "zap_application_counts",
          application_defs$outcome_id[outcome_row],
          application_defs$outcome_label[outcome_row],
          scale_defs$outcome_scale[scale_row],
          scale_defs$outcome_scale_label[scale_row],
          zap_periods,
          zap_reference_period,
          control_name,
          application_control_layers[[control_name]]$label,
          application_control_layers[[control_name]]$controls
        )
      )
    }
  }
}

project_base <- zap_project_df |>
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    cert_year = suppressWarnings(as.integer(cert_year)),
    certified_referred_date = as.Date(certified_referred_date),
    approval_date = as.Date(approval_date),
    completed_date = as.Date(completed_date),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro)),
    event_period = zap_period_from_year(cert_year),
    borough_period = interaction(borough_name, event_period, drop = TRUE),
    days_cert_to_approval = as.numeric(approval_date - certified_referred_date),
    days_cert_to_zap_completion = as.numeric(completed_date - certified_referred_date),
    invalid_approval_duration = !is.na(days_cert_to_approval) & days_cert_to_approval < 0,
    invalid_completion_duration = !is.na(days_cert_to_zap_completion) & days_cert_to_zap_completion < 0
  ) |>
  left_join(
    baseline_clean |> select(borocd, median_household_income_1990_z, poverty_share_1990_z, log_occupied_units_1990_z),
    by = "borocd",
    relationship = "many-to-one"
  )

timing_defs <- tribble(
  ~outcome_id, ~outcome_label, ~source_col,
  "days_cert_to_approval", "Days from certification/referral to approval", "days_cert_to_approval",
  "days_cert_to_zap_completion", "Days from certification/referral to ZAP completion", "days_cert_to_zap_completion"
)

timing_panel <- project_base |>
  filter(!invalid_approval_duration, !invalid_completion_duration) |>
  select(
    project_id, borocd, cert_year, borough_name, event_period, borough_period, treat_z_boro,
    median_household_income_1990_z, poverty_share_1990_z, log_occupied_units_1990_z,
    all_of(timing_defs$source_col)
  ) |>
  pivot_longer(all_of(timing_defs$source_col), names_to = "source_col", values_to = "outcome_value") |>
  left_join(timing_defs, by = "source_col", relationship = "many-to-one") |>
  mutate(
    outcome_scale = "days",
    outcome_scale_label = "Days"
  ) |>
  filter(!is.na(event_period), !is.na(outcome_value))

timing_control_layers <- list(
  `0_fe_only` = list(label = "FE only", controls = character()),
  `1_baseline_controls` = list(label = "Baseline controls", controls = c("median_household_income_1990_z", "poverty_share_1990_z", "log_occupied_units_1990_z"))
)

timing_estimates <- tibble()

for (outcome_row in seq_len(nrow(timing_defs))) {
  for (control_name in names(timing_control_layers)) {
    timing_estimates <- bind_rows(
      timing_estimates,
      run_event_model(
        timing_panel,
        "zap_project_timing",
        timing_defs$outcome_id[outcome_row],
        timing_defs$outcome_label[outcome_row],
        "days",
        "Days",
        zap_periods,
        zap_reference_period,
        control_name,
        timing_control_layers[[control_name]]$label,
        timing_control_layers[[control_name]]$controls
      )
    )
  }
}

status_defs <- tribble(
  ~outcome_id, ~outcome_label, ~outcome_scale, ~outcome_scale_label, ~source_col,
  "completion_share", "Completed share", "share", "Share of applications", "completion_share",
  "failure_share", "Withdrawn/terminated share", "share", "Share of applications", "failure_share",
  "unresolved_share", "Unresolved share", "share", "Share of applications", "unresolved_share",
  "failed_apps_per_10000_occupied_1990", "Withdrawn/terminated count", "per_10000_occupied_1990", "Per 10,000 occupied units", "failed_apps_per_10000_occupied_1990",
  "unresolved_apps_per_10000_occupied_1990", "Unresolved count", "per_10000_occupied_1990", "Per 10,000 occupied units", "unresolved_apps_per_10000_occupied_1990",
  "failed_apps_per_residential_acre", "Withdrawn/terminated count", "per_residential_acre", "Per residential acre", "failed_apps_per_residential_acre",
  "unresolved_apps_per_residential_acre", "Unresolved count", "per_residential_acre", "Per residential acre", "unresolved_apps_per_residential_acre"
)

status_panel <- zap_mature_df |>
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    cert_year = suppressWarnings(as.integer(cert_year)),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990)),
    residential_acres = suppressWarnings(as.numeric(residential_acres)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro)),
    complete_apps = suppressWarnings(as.numeric(complete_apps)),
    failed_apps = suppressWarnings(as.numeric(failed_apps)),
    unresolved_apps = suppressWarnings(as.numeric(unresolved_apps)),
    completion_share = suppressWarnings(as.numeric(completion_share)),
    failure_share = suppressWarnings(as.numeric(failure_share)),
    unresolved_share = suppressWarnings(as.numeric(unresolved_share)),
    failed_apps_per_10000_occupied_1990 = if_else(occupied_units_1990 > 0, 10000 * failed_apps / occupied_units_1990, NA_real_),
    unresolved_apps_per_10000_occupied_1990 = if_else(occupied_units_1990 > 0, 10000 * unresolved_apps / occupied_units_1990, NA_real_),
    failed_apps_per_residential_acre = if_else(residential_acres > 0, failed_apps / residential_acres, NA_real_),
    unresolved_apps_per_residential_acre = if_else(residential_acres > 0, unresolved_apps / residential_acres, NA_real_),
    event_period = status_period_from_year(cert_year),
    borough_period = interaction(borough_name, event_period, drop = TRUE)
  ) |>
  left_join(
    baseline_clean |> select(borocd, median_household_income_1990_z, poverty_share_1990_z, log_occupied_units_1990_z),
    by = "borocd",
    relationship = "many-to-one"
  ) |>
  select(
    borocd, cert_year, borough_name, event_period, borough_period, treat_z_boro,
    median_household_income_1990_z, poverty_share_1990_z, log_occupied_units_1990_z,
    all_of(status_defs$source_col)
  ) |>
  pivot_longer(all_of(status_defs$source_col), names_to = "source_col", values_to = "outcome_value") |>
  left_join(status_defs, by = "source_col", relationship = "many-to-one") |>
  filter(!is.na(event_period))

status_estimates <- tibble()

for (outcome_row in seq_len(nrow(status_defs))) {
  for (control_name in names(timing_control_layers)) {
    status_estimates <- bind_rows(
      status_estimates,
      run_event_model(
        status_panel,
        "zap_mature_status",
        status_defs$outcome_id[outcome_row],
        status_defs$outcome_label[outcome_row],
        status_defs$outcome_scale[outcome_row],
        status_defs$outcome_scale_label[outcome_row],
        status_periods,
        zap_reference_period,
        control_name,
        timing_control_layers[[control_name]]$label,
        timing_control_layers[[control_name]]$controls
      )
    )
  }
}

permit_panel <- permit_df |>
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    year = suppressWarnings(as.integer(year)),
    outcome_value_raw = suppressWarnings(as.numeric(outcome_value)),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro)),
    event_period = permit_period_from_year(year),
    borough_period = interaction(borough_name, event_period, drop = TRUE)
  ) |>
  left_join(
    baseline_clean |> select(borocd, residential_acres_baseline, median_household_income_1990_z, poverty_share_1990_z, log_occupied_units_1990_z),
    by = "borocd",
    relationship = "many-to-one"
  ) |>
  mutate(
    outcome_id = "permit_nb_jobs",
    outcome_label = "Aggregate new-building permit jobs",
    per_10000_occupied_1990 = if_else(occupied_units_1990 > 0, 10000 * outcome_value_raw / occupied_units_1990, NA_real_),
    per_residential_acre = if_else(residential_acres_baseline > 0, outcome_value_raw / residential_acres_baseline, NA_real_)
  ) |>
  select(-outcome_value) |>
  pivot_longer(c(per_10000_occupied_1990, per_residential_acre), names_to = "outcome_scale", values_to = "outcome_value") |>
  left_join(scale_defs, by = "outcome_scale", relationship = "many-to-one") |>
  filter(event_period %in% c("1989", permit_periods))

permit_control_layers <- list(
  `0_fe_only` = list(label = "FE only", controls = character()),
  `1_baseline_controls` = list(label = "Baseline controls", controls = c("median_household_income_1990_z", "poverty_share_1990_z", "log_occupied_units_1990_z"))
)

permit_coefficients <- tibble()

for (scale_row in seq_len(nrow(scale_defs))) {
  for (control_name in names(permit_control_layers)) {
    permit_coefficients <- bind_rows(
      permit_coefficients,
      run_event_model(
        permit_panel |> filter(event_period %in% permit_periods),
        "dob_permit_stage",
        "permit_nb_jobs",
        "Aggregate new-building permit jobs",
        scale_defs$outcome_scale[scale_row],
        scale_defs$outcome_scale_label[scale_row],
        permit_periods,
        permit_reference_period,
        control_name,
        permit_control_layers[[control_name]]$label,
        permit_control_layers[[control_name]]$controls
      )
    )
  }
}

zap_action_audit <- zap_project_df |>
  mutate(
    cert_year = suppressWarnings(as.integer(cert_year)),
    event_period = zap_period_from_year(cert_year),
    actions_nonmissing = nonempty_text(actions),
    ulurp_numbers_nonmissing = nonempty_text(ulurp_numbers),
    current_rezoning_or_special_proxy = str_to_upper(coalesce(rezoning_or_special_proxy, "")) == "TRUE",
    current_public_land_or_disposition_proxy = str_to_upper(coalesce(public_land_or_disposition_proxy, "")) == "TRUE",
    ulurp_rezoning_or_special_proxy = has_ulurp_number_action_code(ulurp_numbers, c("ZM", "ZR", "ZS")),
    ulurp_public_land_or_disposition_proxy = has_ulurp_number_action_code(ulurp_numbers, c("HA", "PP", "PQ", "MM")),
    ulurp_public_housing_or_land_proxy = has_ulurp_number_action_code(
      ulurp_numbers,
      c("HA", "HD", "HO", "HU", "HP", "HG", "HC", "HL", "HM", "PP", "PQ", "MM")
    )
  )

zap_action_audit_summary <- zap_action_audit |>
  filter(!is.na(event_period)) |>
  group_by(event_period) |>
  summarise(
    projects = n(),
    actions_nonmissing = sum(actions_nonmissing, na.rm = TRUE),
    ulurp_numbers_nonmissing = sum(ulurp_numbers_nonmissing, na.rm = TRUE),
    current_rezoning_or_special_proxy = sum(current_rezoning_or_special_proxy, na.rm = TRUE),
    ulurp_rezoning_or_special_proxy = sum(ulurp_rezoning_or_special_proxy, na.rm = TRUE),
    current_public_land_or_disposition_proxy = sum(current_public_land_or_disposition_proxy, na.rm = TRUE),
    ulurp_public_land_or_disposition_proxy = sum(ulurp_public_land_or_disposition_proxy, na.rm = TRUE),
    ulurp_public_housing_or_land_proxy = sum(ulurp_public_housing_or_land_proxy, na.rm = TRUE),
    .groups = "drop"
  )

permit_year_audit <- permit_df |>
  mutate(
    year = suppressWarnings(as.integer(year)),
    outcome_value = suppressWarnings(as.numeric(outcome_value))
  ) |>
  group_by(year) |>
  summarise(citywide_first_issuance_jobs = sum(outcome_value, na.rm = TRUE), .groups = "drop")

permit_recent_average <- permit_year_audit |>
  filter(year >= 2023, year <= 2025) |>
  summarise(value = mean(citywide_first_issuance_jobs, na.rm = TRUE)) |>
  pull(value)

permit_prior_average <- permit_year_audit |>
  filter(year >= 2020, year <= 2022) |>
  summarise(value = mean(citywide_first_issuance_jobs, na.rm = TRUE)) |>
  pull(value)

permit_recent_to_prior_ratio <- if_else(
  !is.na(permit_prior_average) && permit_prior_average > 0,
  permit_recent_average / permit_prior_average,
  NA_real_
)

model_summary <- bind_rows(
  event_coefficients |> distinct(analysis_family, outcome_id, outcome_label, outcome_scale, outcome_scale_label, control_layer, control_label, reference_period, n_obs, n_cd, within_r2, model_status, model_message),
  timing_estimates |> distinct(analysis_family, outcome_id, outcome_label, outcome_scale, outcome_scale_label, control_layer, control_label, reference_period, n_obs, n_cd, within_r2, model_status, model_message),
  status_estimates |> distinct(analysis_family, outcome_id, outcome_label, outcome_scale, outcome_scale_label, control_layer, control_label, reference_period, n_obs, n_cd, within_r2, model_status, model_message),
  permit_coefficients |> distinct(analysis_family, outcome_id, outcome_label, outcome_scale, outcome_scale_label, control_layer, control_label, reference_period, n_obs, n_cd, within_r2, model_status, model_message)
) |>
  arrange(analysis_family, outcome_id, outcome_scale, control_layer, model_status)

qc_df <- bind_rows(
  tibble(metric = "zap_cd_year_cd_count", value = as.character(n_distinct(zap_cd_year_df$borocd)), status = if_else(n_distinct(zap_cd_year_df$borocd) == 59, "pass", "fail"), note = "Standard community districts in the ZAP CD-year panel."),
  tibble(metric = "zap_cd_year_min_year", value = as.character(min(zap_cd_year_df$cert_year, na.rm = TRUE)), status = if_else(min(zap_cd_year_df$cert_year, na.rm = TRUE) == 1976, "pass", "fail"), note = "Earliest certification year in the ZAP CD-year panel."),
  tibble(metric = "zap_cd_year_max_year", value = as.character(max(zap_cd_year_df$cert_year, na.rm = TRUE)), status = if_else(max(zap_cd_year_df$cert_year, na.rm = TRUE) == 2025, "pass", "fail"), note = "Latest certification year in the ZAP CD-year panel."),
  tibble(metric = "zap_mature_cd_count", value = as.character(n_distinct(zap_mature_df$borocd)), status = if_else(n_distinct(zap_mature_df$borocd) == 59, "pass", "fail"), note = "Standard community districts in the mature ZAP status panel."),
  tibble(metric = "permit_cd_count", value = as.character(n_distinct(permit_df$borocd)), status = if_else(n_distinct(permit_df$borocd) == 59, "pass", "fail"), note = "Standard community districts in the permit panel."),
  tibble(metric = "permit_min_year", value = as.character(min(permit_df$year, na.rm = TRUE)), status = if_else(min(permit_df$year, na.rm = TRUE) == 1989, "pass", "fail"), note = "Earliest year in the permit panel."),
  tibble(metric = "permit_max_year", value = as.character(max(permit_df$year, na.rm = TRUE)), status = if_else(max(permit_df$year, na.rm = TRUE) == 2025, "pass", "fail"), note = "Latest year in the permit panel."),
  tibble(metric = "zap_pre_2010_actions_nonmissing_count", value = as.character(sum(zap_action_audit$cert_year < 2010 & zap_action_audit$actions_nonmissing, na.rm = TRUE)), status = if_else(sum(zap_action_audit$cert_year < 2010 & zap_action_audit$actions_nonmissing, na.rm = TRUE) > 0, "pass", "fail"), note = "The action-code proxies are not credible before 2010 if the actions field is entirely missing."),
  tibble(metric = "zap_pre_2010_ulurp_numbers_nonmissing_count", value = as.character(sum(zap_action_audit$cert_year < 2010 & zap_action_audit$ulurp_numbers_nonmissing, na.rm = TRUE)), status = if_else(sum(zap_action_audit$cert_year < 2010 & zap_action_audit$ulurp_numbers_nonmissing, na.rm = TRUE) > 0, "pass", "fail"), note = "Pre-2010 ULURP numbers are available and should be parsed as the fallback action-code source."),
  tibble(metric = "zap_pre_2010_current_rezoning_special_count", value = as.character(sum(zap_action_audit$cert_year < 2010 & zap_action_audit$current_rezoning_or_special_proxy, na.rm = TRUE)), status = if_else(sum(zap_action_audit$cert_year < 2010 & zap_action_audit$current_rezoning_or_special_proxy, na.rm = TRUE) > 0 || sum(zap_action_audit$cert_year < 2010 & zap_action_audit$ulurp_rezoning_or_special_proxy, na.rm = TRUE) == 0, "pass", "fail"), note = "Current ZM/ZR/ZS proxy is invalid if it is zero while pre-2010 ULURP numbers imply rezoning/special-permit actions."),
  tibble(metric = "zap_pre_2010_ulurp_rezoning_special_count", value = as.character(sum(zap_action_audit$cert_year < 2010 & zap_action_audit$ulurp_rezoning_or_special_proxy, na.rm = TRUE)), status = "pass", note = "Diagnostic count of pre-2010 ZM/ZR/ZS-like actions recoverable from ULURP numbers."),
  tibble(metric = "zap_pre_2010_current_public_land_disposition_count", value = as.character(sum(zap_action_audit$cert_year < 2010 & zap_action_audit$current_public_land_or_disposition_proxy, na.rm = TRUE)), status = if_else(sum(zap_action_audit$cert_year < 2010 & zap_action_audit$current_public_land_or_disposition_proxy, na.rm = TRUE) > 0 || sum(zap_action_audit$cert_year < 2010 & zap_action_audit$ulurp_public_land_or_disposition_proxy, na.rm = TRUE) == 0, "pass", "fail"), note = "Current HA/PP/PQ/MM proxy is invalid if it is zero while pre-2010 ULURP numbers imply public-land/disposition actions."),
  tibble(metric = "zap_pre_2010_ulurp_public_land_disposition_count", value = as.character(sum(zap_action_audit$cert_year < 2010 & zap_action_audit$ulurp_public_land_or_disposition_proxy, na.rm = TRUE)), status = "pass", note = "Diagnostic count of pre-2010 HA/PP/PQ/MM-like actions recoverable from ULURP numbers."),
  tibble(metric = "zap_pre_2010_ulurp_public_housing_or_land_count", value = as.character(sum(zap_action_audit$cert_year < 2010 & zap_action_audit$ulurp_public_housing_or_land_proxy, na.rm = TRUE)), status = "pass", note = "Broader diagnostic count of pre-2010 public housing/land-like actions recoverable from ULURP numbers."),
  tibble(metric = "zap_pre_2020_approval_date_nonmissing_count", value = as.character(sum(project_base$cert_year < 2020 & !is.na(project_base$approval_date), na.rm = TRUE)), status = if_else(sum(project_base$cert_year < 2020 & !is.na(project_base$approval_date), na.rm = TRUE) > 0, "pass", "fail"), note = "Approval-date delay models are not credible if approval dates are unavailable before 2020."),
  tibble(metric = "permit_2023_2025_to_2020_2022_first_issuance_ratio", value = as.character(permit_recent_to_prior_ratio), status = if_else(!is.na(permit_recent_to_prior_ratio) & permit_recent_to_prior_ratio >= 0.5, "pass", "fail"), note = "The current permit input is first-issuance job counts; a sharp recent collapse indicates this is not a usable annual permit-activity measure without rebuilding/auditing the permit outcome."),
  tibble(metric = "baseline_duplicate_borocd_count", value = as.character(nrow(baseline_clean) - n_distinct(baseline_clean$borocd)), status = if_else(nrow(baseline_clean) == n_distinct(baseline_clean$borocd), "pass", "fail"), note = "Duplicate baseline rows by borocd after cleaning."),
  tibble(metric = "zap_cd_year_duplicate_key_count", value = as.character(nrow(zap_cd_year_df) - nrow(distinct(zap_cd_year_df, borocd, cert_year))), status = if_else(nrow(zap_cd_year_df) == nrow(distinct(zap_cd_year_df, borocd, cert_year)), "pass", "fail"), note = "Duplicate ZAP CD-year rows."),
  tibble(metric = "zap_mature_duplicate_key_count", value = as.character(nrow(zap_mature_df) - nrow(distinct(zap_mature_df, borocd, cert_year))), status = if_else(nrow(zap_mature_df) == nrow(distinct(zap_mature_df, borocd, cert_year)), "pass", "fail"), note = "Duplicate mature status CD-year rows."),
  tibble(metric = "permit_duplicate_key_count", value = as.character(nrow(permit_df) - nrow(distinct(permit_df, borocd, year, outcome_family))), status = if_else(nrow(permit_df) == nrow(distinct(permit_df, borocd, year, outcome_family)), "pass", "fail"), note = "Duplicate permit CD-year-outcome rows."),
  tibble(metric = "zap_missing_treatment_count", value = as.character(sum(is.na(zap_application_panel$treat_z_boro))), status = if_else(sum(is.na(zap_application_panel$treat_z_boro)) == 0, "pass", "fail"), note = "Missing treatment cells in the long ZAP application panel."),
  tibble(metric = "zap_missing_denominator_count", value = as.character(sum(is.na(zap_application_panel$occupied_units_1990) | is.na(zap_application_panel$residential_acres))), status = if_else(sum(is.na(zap_application_panel$occupied_units_1990) | is.na(zap_application_panel$residential_acres)) == 0, "pass", "fail"), note = "Missing occupied-unit or residential-acre denominators in the ZAP application panel."),
  tibble(metric = "permit_missing_treatment_count", value = as.character(sum(is.na(permit_panel$treat_z_boro))), status = if_else(sum(is.na(permit_panel$treat_z_boro)) == 0, "pass", "fail"), note = "Missing treatment cells in the permit panel."),
  tibble(metric = "permit_missing_residential_acres_count", value = as.character(sum(is.na(permit_panel$residential_acres_baseline))), status = if_else(sum(is.na(permit_panel$residential_acres_baseline)) == 0, "pass", "fail"), note = "Missing residential-acre denominators after joining permits to baseline."),
  tibble(metric = "zap_negative_count_or_rate_count", value = as.character(sum(zap_application_panel$count_value < 0 | zap_application_panel$outcome_value < 0, na.rm = TRUE)), status = if_else(sum(zap_application_panel$count_value < 0 | zap_application_panel$outcome_value < 0, na.rm = TRUE) == 0, "pass", "fail"), note = "Negative count or scaled rate cells in the ZAP application panel."),
  tibble(metric = "status_negative_count_or_rate_count", value = as.character(sum(status_panel$outcome_value < 0, na.rm = TRUE)), status = if_else(sum(status_panel$outcome_value < 0, na.rm = TRUE) == 0, "pass", "fail"), note = "Negative status outcome cells."),
  tibble(metric = "permit_negative_count_or_rate_count", value = as.character(sum(permit_panel$outcome_value_raw < 0 | permit_panel$outcome_value < 0, na.rm = TRUE)), status = if_else(sum(permit_panel$outcome_value_raw < 0 | permit_panel$outcome_value < 0, na.rm = TRUE) == 0, "pass", "fail"), note = "Negative count or scaled rate cells in the permit panel."),
  tibble(metric = "invalid_approval_duration_count", value = as.character(sum(project_base$invalid_approval_duration, na.rm = TRUE)), status = "pass", note = "Negative certification-to-approval durations excluded from timing models."),
  tibble(metric = "invalid_completion_duration_count", value = as.character(sum(project_base$invalid_completion_duration, na.rm = TRUE)), status = "pass", note = "Negative certification-to-ZAP-completion durations excluded from timing models."),
  tibble(metric = "zap_expected_periods_present", value = paste(sort(unique(zap_application_panel$event_period)), collapse = ";"), status = if_else(all(zap_periods %in% unique(zap_application_panel$event_period)), "pass", "fail"), note = "Expected ZAP application event bins."),
  tibble(metric = "status_expected_periods_present", value = paste(sort(unique(status_panel$event_period)), collapse = ";"), status = if_else(all(status_periods %in% unique(status_panel$event_period)), "pass", "fail"), note = "Expected mature status event bins."),
  tibble(metric = "permit_expected_periods_present", value = paste(sort(unique(permit_panel$event_period)), collapse = ";"), status = if_else(all(c("1989", permit_periods) %in% unique(permit_panel$event_period)), "pass", "fail"), note = "Expected permit bins; 1989 is retained for QC only."),
  tibble(metric = "application_requested_terms_missing", value = as.character(sum(event_coefficients$model_status == "requested_term_dropped", na.rm = TRUE)), status = if_else(sum(event_coefficients$model_status == "requested_term_dropped", na.rm = TRUE) == 0, "pass", "fail"), note = "Dropped requested treatment-period terms in ZAP application models."),
  tibble(metric = "permit_requested_terms_missing", value = as.character(sum(permit_coefficients$model_status == "requested_term_dropped", na.rm = TRUE)), status = if_else(sum(permit_coefficients$model_status == "requested_term_dropped", na.rm = TRUE) == 0, "pass", "fail"), note = "Dropped requested treatment-period terms in permit models."),
  tibble(metric = "timing_requested_terms_missing", value = as.character(sum(timing_estimates$model_status == "requested_term_dropped", na.rm = TRUE)), status = if_else(sum(timing_estimates$model_status == "requested_term_dropped", na.rm = TRUE) == 0, "pass", "warning"), note = "Dropped requested treatment-period terms in project timing models; approval-date support is sparse before 2020 in the current ZAP export."),
  tibble(metric = "status_requested_terms_missing", value = as.character(sum(status_estimates$model_status == "requested_term_dropped", na.rm = TRUE)), status = if_else(sum(status_estimates$model_status == "requested_term_dropped", na.rm = TRUE) == 0, "pass", "fail"), note = "Dropped requested treatment-period terms in mature status models."),
  tibble(metric = "pipeline_model_error_count", value = as.character(sum(c(event_coefficients$model_status, timing_estimates$model_status, status_estimates$model_status, permit_coefficients$model_status) == "model_error", na.rm = TRUE)), status = if_else(sum(c(event_coefficients$model_status, timing_estimates$model_status, status_estimates$model_status, permit_coefficients$model_status) == "model_error", na.rm = TRUE) == 0, "pass", "fail"), note = "Unexpected model errors across all exploratory mechanism estimates."),
  tibble(metric = "pipeline_constant_outcome_count", value = as.character(sum(c(event_coefficients$model_status, timing_estimates$model_status, status_estimates$model_status, permit_coefficients$model_status) == "constant_outcome", na.rm = TRUE)), status = if_else(sum(c(event_coefficients$model_status, timing_estimates$model_status, status_estimates$model_status, permit_coefficients$model_status) == "constant_outcome", na.rm = TRUE) == 0, "pass", "warning"), note = "Requested models not estimated because the dependent variable is constant; this currently flags unresolved ZAP outcomes."),
  tibble(metric = "pipeline_insufficient_sample_count", value = as.character(sum(c(event_coefficients$model_status, timing_estimates$model_status, status_estimates$model_status, permit_coefficients$model_status) == "insufficient_sample", na.rm = TRUE)), status = if_else(sum(c(event_coefficients$model_status, timing_estimates$model_status, status_estimates$model_status, permit_coefficients$model_status) == "insufficient_sample", na.rm = TRUE) == 0, "pass", "warning"), note = "Requested models not estimated because there is no usable reference-period sample; this currently flags approval-date timing."),
  tibble(metric = "event_coefficients_nonempty", value = as.character(file.exists("../output/cd_homeownership_pipeline_event_coefficients.csv") && file.info("../output/cd_homeownership_pipeline_event_coefficients.csv")$size > 0), status = "pending_file_write", note = "Checked after file write by the script producer target."),
  tibble(metric = "permit_coefficients_nonempty", value = as.character(file.exists("../output/cd_homeownership_pipeline_permit_coefficients.csv") && file.info("../output/cd_homeownership_pipeline_permit_coefficients.csv")$size > 0), status = "pending_file_write", note = "Checked after file write by the script producer target.")
)

hard_fail_count <- sum(qc_df$status == "fail", na.rm = TRUE)

if (hard_fail_count > 0) {
  write_csv_if_changed(qc_df, "../output/cd_homeownership_pipeline_design_qc.csv")
  stop("Pipeline mechanism QC failed; inspect ../output/cd_homeownership_pipeline_design_qc.csv")
}

write_csv_if_changed(event_coefficients |> arrange(analysis_family, outcome_id, outcome_scale, control_layer, event_period), "../output/cd_homeownership_pipeline_event_coefficients.csv")
write_csv_if_changed(timing_estimates |> arrange(analysis_family, outcome_id, control_layer, event_period), "../output/cd_homeownership_pipeline_timing_delay_estimates.csv")
write_csv_if_changed(status_estimates |> arrange(analysis_family, outcome_id, outcome_scale, control_layer, event_period), "../output/cd_homeownership_pipeline_status_estimates.csv")
write_csv_if_changed(permit_coefficients |> arrange(analysis_family, outcome_id, outcome_scale, control_layer, event_period), "../output/cd_homeownership_pipeline_permit_coefficients.csv")
write_csv_if_changed(model_summary, "../output/cd_homeownership_pipeline_model_summary.csv")

build_plot(
  event_coefficients,
  "../output/cd_homeownership_pipeline_event_coefficients.pdf",
  "ZAP/ULURP application gradients by homeownership exposure",
  "Coefficient on homeowner exposure"
)

build_plot(
  permit_coefficients,
  "../output/cd_homeownership_pipeline_permit_coefficients.pdf",
  "DOB new-building permit gradients by homeownership exposure",
  "Coefficient on homeowner exposure"
)

qc_df <- qc_df |>
  mutate(
    status = case_when(
      metric == "event_coefficients_nonempty" ~ if_else(file.exists("../output/cd_homeownership_pipeline_event_coefficients.csv") && file.info("../output/cd_homeownership_pipeline_event_coefficients.csv")$size > 0, "pass", "fail"),
      metric == "permit_coefficients_nonempty" ~ if_else(file.exists("../output/cd_homeownership_pipeline_permit_coefficients.csv") && file.info("../output/cd_homeownership_pipeline_permit_coefficients.csv")$size > 0, "pass", "fail"),
      TRUE ~ status
    ),
    value = case_when(
      metric == "event_coefficients_nonempty" ~ as.character(file.exists("../output/cd_homeownership_pipeline_event_coefficients.csv") && file.info("../output/cd_homeownership_pipeline_event_coefficients.csv")$size > 0),
      metric == "permit_coefficients_nonempty" ~ as.character(file.exists("../output/cd_homeownership_pipeline_permit_coefficients.csv") && file.info("../output/cd_homeownership_pipeline_permit_coefficients.csv")$size > 0),
      TRUE ~ value
    )
  )

if (sum(qc_df$status == "fail", na.rm = TRUE) > 0) {
  write_csv_if_changed(qc_df, "../output/cd_homeownership_pipeline_design_qc.csv")
  stop("Pipeline mechanism output QC failed; inspect ../output/cd_homeownership_pipeline_design_qc.csv")
}

write_csv_if_changed(qc_df, "../output/cd_homeownership_pipeline_design_qc.csv")

cat("Wrote exploratory ZAP/ULURP and permit pipeline diagnostics to ../output\n")
