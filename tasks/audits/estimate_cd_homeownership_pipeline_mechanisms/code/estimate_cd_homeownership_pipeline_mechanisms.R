suppressPackageStartupMessages({
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

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
    left_join(coeftable_df(model), by = "term", relationship = "many-to-one") |>
    left_join(confint_df(model), by = "term", relationship = "many-to-one") |>
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

model_summary <- bind_rows(
  event_coefficients |> distinct(analysis_family, outcome_id, outcome_label, outcome_scale, outcome_scale_label, control_layer, control_label, reference_period, n_obs, n_cd, within_r2, model_status, model_message),
  timing_estimates |> distinct(analysis_family, outcome_id, outcome_label, outcome_scale, outcome_scale_label, control_layer, control_label, reference_period, n_obs, n_cd, within_r2, model_status, model_message),
  status_estimates |> distinct(analysis_family, outcome_id, outcome_label, outcome_scale, outcome_scale_label, control_layer, control_label, reference_period, n_obs, n_cd, within_r2, model_status, model_message),
  permit_coefficients |> distinct(analysis_family, outcome_id, outcome_label, outcome_scale, outcome_scale_label, control_layer, control_label, reference_period, n_obs, n_cd, within_r2, model_status, model_message)
) |>
  arrange(analysis_family, outcome_id, outcome_scale, control_layer, model_status)

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

cat("Wrote CD homeownership pipeline mechanism outputs to ../output\n")
