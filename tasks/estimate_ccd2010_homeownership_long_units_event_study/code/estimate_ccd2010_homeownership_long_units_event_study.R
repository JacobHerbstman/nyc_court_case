# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/estimate_ccd2010_homeownership_long_units_event_study/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

write_lines_if_changed <- function(lines, out_path) {
  temp_path <- tempfile(fileext = ".tex")
  writeLines(lines, temp_path, useBytes = TRUE)
  copy_if_changed(temp_path, out_path)
}

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

significance_stars <- function(x) {
  case_when(
    is.na(x) ~ "",
    x < 0.01 ~ "***",
    x < 0.05 ~ "**",
    x < 0.1 ~ "*",
    TRUE ~ ""
  )
}

model_nobs <- function(model) {
  if (!is.null(model$nobs)) {
    return(as.integer(model$nobs))
  }

  length(model$residuals)
}

regression_table_row <- function(row_label, values) {
  paste0("    ", row_label, " & ", paste(values, collapse = " & "), " \\\\")
}

event_periods <- c(
  "1970-1974",
  "1975-1979",
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
reference_event_period <- "1985-1989"
estimated_event_periods <- event_periods[event_periods != reference_event_period]

outcome_defs <- tribble(
  ~outcome_id, ~outcome_label,
  "units_built_1_4", "1-4 unit buildings",
  "units_built_5_plus", "5+ unit buildings"
)

series_df <- read_csv("../input/ccdist2010_homeownership_long_units_series.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    council_district = suppressWarnings(as.integer(council_district)),
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
    year >= 1970,
    year <= 2025,
    occupied_units_1990 > 0
  ) %>%
  mutate(
    event_period = case_when(
      year >= 1970 & year <= 1974 ~ "1970-1974",
      year >= 1975 & year <= 1979 ~ "1975-1979",
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
    event_period = factor(event_period, levels = event_periods),
    borough_period = interaction(borough_code, event_period, drop = TRUE),
    outcome_rate = 10000 * outcome_value / occupied_units_1990
  ) %>%
  left_join(outcome_defs, by = c("series_family" = "outcome_id"), relationship = "many-to-one")

if (n_distinct(series_df$district_id) != 51) {
  stop("Expected 51 Council districts in the PLUTO-only event-study input.")
}

pre_rate_df <- series_df %>%
  filter(year >= 1970, year <= 1988) %>%
  group_by(series_family, district_id) %>%
  summarise(pre_1970_1988_rate = mean(outcome_rate, na.rm = TRUE), .groups = "drop") %>%
  group_by(series_family) %>%
  mutate(pre_1970_1988_rate_z = z_score(pre_1970_1988_rate)) %>%
  ungroup() %>%
  select(series_family, district_id, pre_1970_1988_rate_z)

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

design_df <- series_df %>%
  left_join(pre_rate_df, by = c("series_family", "district_id"), relationship = "many-to-one") %>%
  left_join(control_lookup, by = "district_id", relationship = "many-to-one") %>%
  mutate(
    pre_1970_1988_rate_z = coalesce(pre_1970_1988_rate_z, 0),
    log_occupied_units_1990_z = coalesce(log_occupied_units_1990_z, 0),
    vacancy_rate_1990_z = coalesce(vacancy_rate_1990_z, 0),
    median_household_income_1990_z = coalesce(median_household_income_1990_z, 0)
  )

control_vars <- c("log_occupied_units_1990_z", "median_household_income_1990_z", "vacancy_rate_1990_z", "pre_1970_1988_rate_z")

for (period_value in estimated_event_periods) {
  design_df[[paste0("treat_z_boro_x_", sanitize_period(period_value))]] <- design_df$treat_z_boro * as.integer(as.character(design_df$event_period) == period_value)

  for (control_var in control_vars) {
    design_df[[paste0(control_var, "_x_", sanitize_period(period_value))]] <- design_df[[control_var]] * as.integer(as.character(design_df$event_period) == period_value)
  }
}

treat_terms <- paste0("treat_z_boro_x_", sanitize_period(estimated_event_periods))
control_terms <- unlist(lapply(control_vars, function(control_var) paste0(control_var, "_x_", sanitize_period(estimated_event_periods))))

event_rows <- list()

for (outcome_id in outcome_defs$outcome_id) {
  outcome_design <- design_df %>%
    filter(series_family == outcome_id)

  model <- feols(
    as.formula(paste0("outcome_rate ~ ", paste(c(treat_terms, control_terms), collapse = " + "), " | district_id + borough_period")),
    cluster = ~district_id,
    data = outcome_design
  )
  event_model_nobs <- model_nobs(model)
  event_model_within_r2 <- tryCatch(as.numeric(r2(model, type = "wr2")), error = function(e) NA_real_)

  event_rows[[outcome_id]] <- bind_rows(
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
      model,
      tibble(term = treat_terms, event_period = estimated_event_periods, is_reference = FALSE)
    )
  ) %>%
    mutate(
      event_period = factor(event_period, levels = event_periods),
      event_period_index = match(as.character(event_period), event_periods),
      source_family = "mappluto_proxy_25v4",
      source_label = "25v4 MapPLUTO yearbuilt proxy on 2010 Council districts",
      series_family = outcome_id,
      outcome_label = outcome_defs$outcome_label[outcome_defs$outcome_id == outcome_id],
      outcome_scale = "per_10000_occupied_1990",
      reference_period = reference_event_period,
      model = "district_fe_borough_period_fe_controls",
      control_label = "log occupied units + median income + vacancy + pre-production",
      n_obs = event_model_nobs,
      within_r2 = event_model_within_r2
    )
}

event_df <- bind_rows(event_rows) %>%
  arrange(series_family, event_period)

write_csv_if_changed(event_df, "../output/ccdist2010_homeownership_long_units_event_coefficients_5yr_bins.csv")

plot_df <- event_df %>%
  mutate(outcome_label = factor(outcome_label, levels = c("1-4 unit buildings", "5+ unit buildings")))

pdf("../output/ccdist2010_homeownership_long_units_event_coefficients_5yr_bins.pdf", width = 11, height = 8.5)
print(
  ggplot(plot_df, aes(x = event_period_index, y = estimate, color = outcome_label, group = outcome_label)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(
      data = filter(plot_df, !is_reference),
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
      title = "Four-control event study, five-year bins: 1-4 vs 5+ unit buildings",
      x = NULL,
      y = "Coefficient on homeowner exposure",
      color = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

pre_count_df <- series_df %>%
  filter(year >= 1970, year <= 1988) %>%
  group_by(series_family, district_id) %>%
  summarise(pre_1970_1988_count = mean(outcome_value, na.rm = TRUE), .groups = "drop") %>%
  group_by(series_family) %>%
  mutate(pre_1970_1988_count_z = z_score(pre_1970_1988_count)) %>%
  ungroup() %>%
  select(series_family, district_id, pre_1970_1988_count_z)

raw_design_df <- series_df %>%
  left_join(pre_count_df, by = c("series_family", "district_id"), relationship = "many-to-one") %>%
  left_join(control_lookup, by = "district_id", relationship = "many-to-one") %>%
  mutate(
    pre_1970_1988_count_z = coalesce(pre_1970_1988_count_z, 0),
    log_occupied_units_1990_z = coalesce(log_occupied_units_1990_z, 0),
    vacancy_rate_1990_z = coalesce(vacancy_rate_1990_z, 0),
    median_household_income_1990_z = coalesce(median_household_income_1990_z, 0)
  )

raw_control_vars <- c("log_occupied_units_1990_z", "median_household_income_1990_z", "vacancy_rate_1990_z", "pre_1970_1988_count_z")

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
      source_label = "25v4 MapPLUTO yearbuilt proxy on 2010 Council districts",
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

write_csv_if_changed(raw_event_df, "../output/ccdist2010_homeownership_long_units_event_coefficients_raw_units_5yr_bins.csv")

raw_plot_df <- raw_event_df %>%
  mutate(outcome_label = factor(outcome_label, levels = c("1-4 unit buildings", "5+ unit buildings")))

pdf("../output/ccdist2010_homeownership_long_units_event_coefficients_raw_units_5yr_bins.pdf", width = 11, height = 8.5)
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
      title = "Raw-unit event study, five-year bins: 1-4 vs 5+ unit buildings",
      x = NULL,
      y = "Coefficient on homeowner exposure (units built)",
      color = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

window_defs <- tribble(
  ~comparison_id, ~column_label, ~row_order, ~pre_start, ~pre_end, ~post_start, ~post_end,
  "placebo_1980_1984_minus_1985_1989", "Placebo", 1L, 1985L, 1989L, 1980L, 1984L,
  "post_1990_1994_minus_1985_1989", "1990--1994", 2L, 1985L, 1989L, 1990L, 1994L,
  "post_1995_1999_minus_1985_1989", "1995--1999", 3L, 1985L, 1989L, 1995L, 1999L,
  "post_2000_2004_minus_1985_1989", "2000--2004", 4L, 1985L, 1989L, 2000L, 2004L,
  "post_2005_2009_minus_1985_1989", "2005--2009", 5L, 1985L, 1989L, 2005L, 2009L,
  "post_2010_2014_minus_1985_1989", "2010--2014", 6L, 1985L, 1989L, 2010L, 2014L,
  "post_2015_2019_minus_1985_1989", "2015--2019", 7L, 1985L, 1989L, 2015L, 2019L,
  "post_2020_2025_minus_1985_1989", "2020--2025", 8L, 1985L, 1989L, 2020L, 2025L
) %>%
  mutate(
    pre_window = paste0(pre_start, "-", pre_end),
    post_window = paste0(post_start, "-", post_end)
  )

long_diff_rows <- list()

for (i in seq_len(nrow(window_defs))) {
  window_row <- window_defs[i, ]

  pre_df <- design_df %>%
    filter(series_family == "units_built_5_plus", year >= window_row$pre_start, year <= window_row$pre_end) %>%
    group_by(district_id) %>%
    summarise(pre_avg = mean(outcome_rate, na.rm = TRUE), pre_year_count = n_distinct(year), .groups = "drop")

  post_df <- design_df %>%
    filter(series_family == "units_built_5_plus", year >= window_row$post_start, year <= window_row$post_end) %>%
    group_by(district_id) %>%
    summarise(post_avg = mean(outcome_rate, na.rm = TRUE), post_year_count = n_distinct(year), .groups = "drop")

  diff_df <- design_df %>%
    filter(series_family == "units_built_5_plus") %>%
    distinct(
      district_id,
      council_district,
      borough_code,
      borough_name,
      treat_z_boro,
      log_occupied_units_1990_z,
      median_household_income_1990_z,
      vacancy_rate_1990_z,
      pre_1970_1988_rate_z
    ) %>%
    left_join(pre_df, by = "district_id", relationship = "one-to-one") %>%
    left_join(post_df, by = "district_id", relationship = "one-to-one") %>%
    mutate(delta_value = post_avg - pre_avg)

  model_df <- diff_df %>%
    select(delta_value, pre_avg, treat_z_boro, borough_code, all_of(control_vars)) %>%
    filter(if_all(everything(), ~ !is.na(.x)))

  model <- feols(
    as.formula(paste0("delta_value ~ treat_z_boro + ", paste(control_vars, collapse = " + "), " | borough_code")),
    data = model_df,
    vcov = "hetero"
  )
  term_df <- extract_model_terms(model, tibble(term = "treat_z_boro"))

  long_diff_rows[[window_row$comparison_id]] <- term_df %>%
    transmute(
      source_family = "mappluto_proxy_25v4",
      comparison_id = window_row$comparison_id,
      row_order = window_row$row_order,
      column_label = window_row$column_label,
      pre_window = window_row$pre_window,
      post_window = window_row$post_window,
      series_family = "units_built_5_plus",
      outcome_label = "5+ unit buildings",
      outcome_scale = "per_10000_occupied_1990",
      term,
      estimate,
      std_error,
      statistic,
      p_value,
      conf_low,
      conf_high,
      n_districts = model_nobs(model),
      initial_outcome_mean = mean(model_df$pre_avg),
      pre_year_count_min = min(diff_df$pre_year_count, na.rm = TRUE),
      post_year_count_min = min(diff_df$post_year_count, na.rm = TRUE),
      model = "long_difference_borough_fe_controls"
    )
}

long_diff_df <- bind_rows(long_diff_rows) %>%
  mutate(
    estimate_label = paste0(format_decimal(estimate, 1), significance_stars(p_value)),
    std_error_label = format_decimal(std_error, 1),
    initial_outcome_mean_label = format_decimal(initial_outcome_mean, 1),
    p_value_label = format_p_value(p_value)
  ) %>%
  arrange(row_order)

if (nrow(long_diff_df) != nrow(window_defs) || any(is.na(long_diff_df$row_order))) {
  stop("Long-difference table row count did not match the declared windows.")
}

write_csv_if_changed(long_diff_df, "../output/ccdist2010_homeownership_long_units_long_difference_estimates.csv")

checkmark_values <- rep("\\checkmark", nrow(long_diff_df))
table_col_spec <- paste0("l", strrep("c", nrow(long_diff_df)))

table_lines <- c(
  "\\begin{table}[htbp]",
  "    \\centering",
  "    \\begin{threeparttable}",
  "    \\caption{Long-Difference Estimates for 5+ Unit Housing Production}",
  "    \\label{tab:ccdist2010_homeownership_long_units_long_difference}",
  "    \\scriptsize",
  "    \\setlength{\\tabcolsep}{3pt}",
  paste0("    \\begin{tabular}{", table_col_spec, "}"),
  "    \\toprule",
  regression_table_row("", paste0("(", seq_len(nrow(long_diff_df)), ")")),
  regression_table_row("", long_diff_df$column_label),
  "    \\midrule",
  regression_table_row("Homeownership exposure", long_diff_df$estimate_label),
  regression_table_row("", paste0("(", long_diff_df$std_error_label, ")")),
  "    \\midrule",
  regression_table_row("N", long_diff_df$n_districts),
  regression_table_row("Initial outcome mean", long_diff_df$initial_outcome_mean_label),
  regression_table_row("Borough FE", checkmark_values),
  regression_table_row("Controls", checkmark_values),
  "    \\bottomrule",
  "    \\end{tabular}",
  "    \\begin{tablenotes}[flushleft]",
  "    \\footnotesize",
  paste0("    \\item \\textit{Notes:} Table reports coefficients on within-borough standardized 1990 homeownership from Council-district long-difference regressions. The outcome is average $5+$ unit new-building units per 10,000 1990 occupied units, measured with the 25v4 MapPLUTO yearbuilt proxy in all years. All columns use 1985--1989 as the reference period. Column (1) compares 1980--1984 to 1985--1989. Columns (2)--(", nrow(long_diff_df), ") compare the listed five-year post window to 1985--1989. The initial outcome mean is the sample mean of the 1985--1989 outcome level. Controls include log 1990 occupied units, 1990 median household income, 1990 vacancy rate, and 1970--1988 pre-period production on the same outcome scale. Standard errors are heteroskedasticity-robust and shown in parentheses. * $p < 0.10$, ** $p < 0.05$, *** $p < 0.01$."),
  "    \\end{tablenotes}",
  "    \\end{threeparttable}",
  "\\end{table}"
)

write_lines_if_changed(table_lines, "../output/ccdist2010_homeownership_long_units_long_difference.tex")

raw_long_diff_rows <- list()

for (i in seq_len(nrow(window_defs))) {
  window_row <- window_defs[i, ]

  pre_df <- raw_design_df %>%
    filter(series_family == "units_built_5_plus", year >= window_row$pre_start, year <= window_row$pre_end) %>%
    group_by(district_id) %>%
    summarise(pre_avg = mean(outcome_value, na.rm = TRUE), pre_year_count = n_distinct(year), .groups = "drop")

  post_df <- raw_design_df %>%
    filter(series_family == "units_built_5_plus", year >= window_row$post_start, year <= window_row$post_end) %>%
    group_by(district_id) %>%
    summarise(post_avg = mean(outcome_value, na.rm = TRUE), post_year_count = n_distinct(year), .groups = "drop")

  diff_df <- raw_design_df %>%
    filter(series_family == "units_built_5_plus") %>%
    distinct(
      district_id,
      council_district,
      borough_code,
      borough_name,
      treat_z_boro,
      log_occupied_units_1990_z,
      median_household_income_1990_z,
      vacancy_rate_1990_z,
      pre_1970_1988_count_z
    ) %>%
    left_join(pre_df, by = "district_id", relationship = "one-to-one") %>%
    left_join(post_df, by = "district_id", relationship = "one-to-one") %>%
    mutate(delta_value = post_avg - pre_avg)

  model_df <- diff_df %>%
    select(delta_value, pre_avg, treat_z_boro, borough_code, all_of(raw_control_vars)) %>%
    filter(if_all(everything(), ~ !is.na(.x)))

  model <- feols(
    as.formula(paste0("delta_value ~ treat_z_boro + ", paste(raw_control_vars, collapse = " + "), " | borough_code")),
    data = model_df,
    vcov = "hetero"
  )
  term_df <- extract_model_terms(model, tibble(term = "treat_z_boro"))

  raw_long_diff_rows[[window_row$comparison_id]] <- term_df %>%
    transmute(
      source_family = "mappluto_proxy_25v4",
      comparison_id = window_row$comparison_id,
      row_order = window_row$row_order,
      column_label = window_row$column_label,
      pre_window = window_row$pre_window,
      post_window = window_row$post_window,
      series_family = "units_built_5_plus",
      outcome_label = "5+ unit buildings",
      outcome_scale = "raw_units_annual_average",
      term,
      estimate,
      std_error,
      statistic,
      p_value,
      conf_low,
      conf_high,
      n_districts = model_nobs(model),
      initial_outcome_mean = mean(model_df$pre_avg),
      pre_year_count_min = min(diff_df$pre_year_count, na.rm = TRUE),
      post_year_count_min = min(diff_df$post_year_count, na.rm = TRUE),
      model = "long_difference_borough_fe_controls"
    )
}

raw_long_diff_df <- bind_rows(raw_long_diff_rows) %>%
  mutate(
    estimate_label = paste0(format_decimal(estimate, 1), significance_stars(p_value)),
    std_error_label = format_decimal(std_error, 1),
    initial_outcome_mean_label = format_decimal(initial_outcome_mean, 1),
    p_value_label = format_p_value(p_value)
  ) %>%
  arrange(row_order)

if (nrow(raw_long_diff_df) != nrow(window_defs) || any(is.na(raw_long_diff_df$row_order))) {
  stop("Raw-unit long-difference table row count did not match the declared windows.")
}

write_csv_if_changed(raw_long_diff_df, "../output/ccdist2010_homeownership_long_units_long_difference_raw_units_estimates.csv")

raw_checkmark_values <- rep("\\checkmark", nrow(raw_long_diff_df))
raw_table_col_spec <- paste0("l", strrep("c", nrow(raw_long_diff_df)))

table_lines <- c(
  "\\begin{table}[htbp]",
  "    \\centering",
  "    \\begin{threeparttable}",
  "    \\caption{Raw-Unit Long-Difference Estimates for 5+ Unit Housing Production}",
  "    \\label{tab:ccdist2010_homeownership_long_units_long_difference_raw_units}",
  "    \\scriptsize",
  "    \\setlength{\\tabcolsep}{3pt}",
  paste0("    \\begin{tabular}{", raw_table_col_spec, "}"),
  "    \\toprule",
  regression_table_row("", paste0("(", seq_len(nrow(raw_long_diff_df)), ")")),
  regression_table_row("", raw_long_diff_df$column_label),
  "    \\midrule",
  regression_table_row("Homeownership exposure", raw_long_diff_df$estimate_label),
  regression_table_row("", paste0("(", raw_long_diff_df$std_error_label, ")")),
  "    \\midrule",
  regression_table_row("N", raw_long_diff_df$n_districts),
  regression_table_row("Initial outcome mean", raw_long_diff_df$initial_outcome_mean_label),
  regression_table_row("Borough FE", raw_checkmark_values),
  regression_table_row("Controls", raw_checkmark_values),
  "    \\bottomrule",
  "    \\end{tabular}",
  "    \\begin{tablenotes}[flushleft]",
  "    \\footnotesize",
  paste0("    \\item \\textit{Notes:} Table reports coefficients on within-borough standardized 1990 homeownership from Council-district long-difference regressions. The outcome is average annual $5+$ unit new-building units, measured in raw unit counts with the 25v4 MapPLUTO yearbuilt proxy in all years. All columns use 1985--1989 as the reference period. Column (1) compares 1980--1984 to 1985--1989. Columns (2)--(", nrow(raw_long_diff_df), ") compare the listed five-year post window to 1985--1989. The initial outcome mean is the sample mean of the 1985--1989 outcome level. Controls include log 1990 occupied units, 1990 median household income, 1990 vacancy rate, and 1970--1988 raw pre-period production. Standard errors are heteroskedasticity-robust and shown in parentheses. * $p < 0.10$, ** $p < 0.05$, *** $p < 0.01$."),
  "    \\end{tablenotes}",
  "    \\end{threeparttable}",
  "\\end{table}"
)

write_lines_if_changed(table_lines, "../output/ccdist2010_homeownership_long_units_long_difference_raw_units.tex")

cat("Wrote PLUTO-only 2010 Council district event-study outputs to ../output\n")
