# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/estimate_cd_homeownership_long_units_event_study/code")
# bin_scheme <- "5yr_bins"

suppressPackageStartupMessages({
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/data_reports.R")

if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  stopifnot(length(args) == 1)
  bin_scheme <- args[1]
}

# Bin starts, omitted bin, and the pre-production control window for each scheme.
# 5yr_bins keeps the paper's 1970-1988 control. The decade schemes measure the
# control in 1960-1969, before the first estimated bin, so it does not overlap
# any estimated or reference bin.
bin_starts <- switch(bin_scheme,
  "5yr_bins" = seq(1970, 2020, by = 5),
  "decade_bins" = seq(1970, 2020, by = 10),
  "decade_pre_bins" = c(1970, 1980, seq(1985, 2020, by = 5)),
  stop("Unknown bin scheme: ", bin_scheme)
)
reference_event_period <- switch(bin_scheme,
  "5yr_bins" = "1985-1989",
  "decade_bins" = "1980-1989",
  "decade_pre_bins" = "1985-1989"
)
pre_count_years <- switch(bin_scheme,
  "5yr_bins" = 1970:1988,
  "decade_bins" = 1960:1969,
  "decade_pre_bins" = 1960:1969
)
plot_title <- switch(bin_scheme,
  "5yr_bins" = "Raw-unit event study, five-year bins: 1-4 vs 5+ unit buildings",
  "decade_bins" = "Raw-unit event study, decade bins: 1-4 vs 5+ unit buildings",
  "decade_pre_bins" = "Raw-unit event study, 1970s decade then five-year bins: 1-4 vs 5+ unit buildings"
)
plot_caption <- switch(bin_scheme,
  "5yr_bins" = NULL,
  "decade_bins" = "Omitted bin 1980-1989. Pre-production control is mean annual units in 1960-1969. 95% CIs clustered by community district.",
  "decade_pre_bins" = "Omitted bin 1985-1989. Pre-production control is mean annual units in 1960-1969. 95% CIs clustered by community district."
)

bin_ends <- c(bin_starts[-1] - 1, 2025)
event_periods <- paste0(bin_starts, "-", bin_ends)
estimated_event_periods <- event_periods[event_periods != reference_event_period]
stopifnot(reference_event_period %in% event_periods)

sanitize_period <- function(x) {
  str_replace_all(x, "-", "_")
}

z_score <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  x_sd <- sd(x, na.rm = TRUE)

  if (is.na(x_sd) || x_sd == 0) {
    return(rep(0, length(x)))
  }

  (x - mean(x, na.rm = TRUE)) / x_sd
}

coeftable_df <- function(model) {
  coef_table <- as.data.frame(coeftable(model))
  coef_table$term <- rownames(coef_table)
  rownames(coef_table) <- NULL

  statistic_col <- if ("t value" %in% names(coef_table)) "t value" else "z value"
  p_value_col <- if ("Pr(>|t|)" %in% names(coef_table)) "Pr(>|t|)" else "Pr(>|z|)"

  coef_table %>%
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
  requested_terms_df %>%
    left_join(coeftable_df(model), by = "term", relationship = "many-to-one") %>%
    left_join(confint_df(model), by = "term", relationship = "many-to-one")
}

model_nobs <- function(model) {
  if (!is.null(model$nobs)) {
    return(as.integer(model$nobs))
  }

  length(model$residuals)
}

outcome_defs <- tribble(
  ~outcome_id, ~outcome_label,
  "units_built_1_4", "1-4 unit buildings",
  "units_built_5_plus", "5+ unit buildings"
)

full_series_df <- read_csv("../input/cd_homeownership_long_units_series.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    district_id = sprintf("%03d", suppressWarnings(as.integer(district_id))),
    borocd = suppressWarnings(as.integer(borocd)),
    borough_code = as.character(borough_code),
    year = suppressWarnings(as.integer(year)),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990)),
    vacancy_rate_1990 = suppressWarnings(as.numeric(vacancy_rate_1990)),
    median_household_income_1990 = suppressWarnings(as.numeric(median_household_income_1990)),
    outcome_value = suppressWarnings(as.numeric(outcome_value)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro))
  ) %>%
  filter(
    series_kind == "preferred_long_series",
    source_family == "mappluto_proxy_25v4",
    series_family %in% outcome_defs$outcome_id,
    !is.na(year),
    occupied_units_1990 > 0
  )

series_df <- full_series_df %>%
  filter(year >= 1970, year <= 2025) %>%
  mutate(
    event_period = event_periods[findInterval(year, bin_starts)],
    event_period = factor(event_period, levels = event_periods),
    borough_period = interaction(borough_code, event_period, drop = TRUE)
  ) %>%
  left_join(outcome_defs, by = c("series_family" = "outcome_id"), relationship = "many-to-one")

if (n_distinct(series_df$district_id) != 59) {
  stop("Expected 59 community districts in the event-study input.")
}

control_lookup <- series_df %>%
  distinct(district_id, borough_code, borough_name, occupied_units_1990, vacancy_rate_1990, median_household_income_1990) %>%
  mutate(
    log_occupied_units_1990 = log(occupied_units_1990)
  ) %>%
  group_by(borough_code, borough_name) %>%
  mutate(
    log_occupied_units_1990_z = z_score(log_occupied_units_1990),
    vacancy_rate_1990_z = z_score(vacancy_rate_1990),
    median_household_income_1990_z = z_score(median_household_income_1990)
  ) %>%
  ungroup() %>%
  select(district_id, log_occupied_units_1990_z, vacancy_rate_1990_z, median_household_income_1990_z)

treat_terms <- paste0("treat_z_boro_x_", sanitize_period(estimated_event_periods))

pre_count_df <- full_series_df %>%
  filter(year %in% pre_count_years) %>%
  group_by(series_family, district_id) %>%
  summarise(pre_count = mean(outcome_value, na.rm = TRUE), pre_year_count = n_distinct(year), .groups = "drop") %>%
  group_by(series_family) %>%
  mutate(pre_count_z = z_score(pre_count)) %>%
  ungroup() %>%
  select(series_family, district_id, pre_year_count, pre_count_z)

if (nrow(pre_count_df) != 59 * nrow(outcome_defs) || any(pre_count_df$pre_year_count != length(pre_count_years))) {
  stop("Pre-production control does not cover every district and year in its window.")
}

raw_design_df <- series_df %>%
  left_join(pre_count_df, by = c("series_family", "district_id"), relationship = "many-to-one") %>%
  left_join(control_lookup, by = "district_id", relationship = "many-to-one") %>%
  mutate(
    pre_count_z = coalesce(pre_count_z, 0),
    log_occupied_units_1990_z = coalesce(log_occupied_units_1990_z, 0),
    vacancy_rate_1990_z = coalesce(vacancy_rate_1990_z, 0),
    median_household_income_1990_z = coalesce(median_household_income_1990_z, 0)
  )

raw_control_vars <- c("log_occupied_units_1990_z", "median_household_income_1990_z", "vacancy_rate_1990_z", "pre_count_z")

for (period_value in estimated_event_periods) {
  raw_design_df[[paste0("treat_z_boro_x_", sanitize_period(period_value))]] <- raw_design_df$treat_z_boro * as.integer(as.character(raw_design_df$event_period) == period_value)

  for (control_var in raw_control_vars) {
    raw_design_df[[paste0(control_var, "_x_", sanitize_period(period_value))]] <- raw_design_df[[control_var]] * as.integer(as.character(raw_design_df$event_period) == period_value)
  }
}

raw_control_terms <- unlist(lapply(raw_control_vars, function(control_var) paste0(control_var, "_x_", sanitize_period(estimated_event_periods))))
raw_event_rows <- list()

for (outcome_id in outcome_defs$outcome_id) {
  raw_outcome_design <- raw_design_df %>%
    filter(series_family == outcome_id)

  raw_model <- feols(
    as.formula(paste0("outcome_value ~ ", paste(c(treat_terms, raw_control_terms), collapse = " + "), " | district_id + borough_period")),
    cluster = ~district_id,
    data = raw_outcome_design
  )
  raw_event_model_nobs <- model_nobs(raw_model)
  raw_event_model_within_r2 <- tryCatch(as.numeric(r2(raw_model, type = "wr2")), error = function(e) NA_real_)

  raw_event_rows[[outcome_id]] <- bind_rows(
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
    extract_model_terms(
      raw_model,
      tibble(term = treat_terms, event_period = estimated_event_periods, is_reference = FALSE)
    )
  ) %>%
    mutate(
      event_period = factor(event_period, levels = event_periods),
      event_period_index = match(as.character(event_period), event_periods),
      source_family = "mappluto_proxy_25v4",
      source_label = "25v4 MapPLUTO yearbuilt proxy on community districts",
      series_family = outcome_id,
      outcome_label = outcome_defs$outcome_label[outcome_defs$outcome_id == outcome_id],
      outcome_scale = "raw_units",
      reference_period = reference_event_period,
      model = "district_fe_borough_period_fe_controls",
      control_label = "log occupied units + median income + vacancy + raw pre-production",
      n_obs = raw_event_model_nobs,
      within_r2 = raw_event_model_within_r2
    )
}

raw_event_df <- bind_rows(raw_event_rows) %>%
  arrange(series_family, event_period)

save_csv(raw_event_df, paste0("../output/cd_homeownership_long_units_event_coefficients_raw_units_", bin_scheme, ".csv"), c("source_family", "series_family", "event_period"))

raw_plot_df <- raw_event_df %>%
  mutate(outcome_label = factor(outcome_label, levels = c("1-4 unit buildings", "5+ unit buildings")))

pdf(paste0("../output/cd_homeownership_long_units_event_coefficients_raw_units_", bin_scheme, ".pdf"), width = 11, height = 8.5)
print(
  ggplot(raw_plot_df, aes(x = event_period_index, y = estimate, color = outcome_label, group = outcome_label)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(
      data = filter(raw_plot_df, !is_reference),
      aes(ymin = conf_low, ymax = conf_high),
      width = 0.12,
      linewidth = 0.45,
      position = position_dodge(width = 0.28)
    ) +
    geom_line(linewidth = 0.75, position = position_dodge(width = 0.28)) +
    geom_point(size = 2.1, position = position_dodge(width = 0.28)) +
    scale_color_manual(values = c("1-4 unit buildings" = "#666666", "5+ unit buildings" = "#2f7d32")) +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(
      title = plot_title,
      caption = plot_caption,
      x = NULL,
      y = "Coefficient on homeowner exposure (units built)",
      color = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

cat("Wrote", bin_scheme, "community district event-study outputs to ../output\n")
