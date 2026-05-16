# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/estimate_zap_zoning_actions_event_study/code")

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

format_decimal <- function(x, digits = 2) {
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

project_df <- read_csv("../input/zap_zoning_map_special_permit_project_classification.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    project_id = as.character(project_id),
    completed_year = suppressWarnings(as.integer(completed_year)),
    included_zm_plus_residential_zs = as.logical(included_zm_plus_residential_zs),
    increased_residential_proxy = as.logical(increased_residential_proxy),
    mixed_use_text_flag = as.logical(mixed_use_text_flag)
  ) |>
  filter(!is.na(project_id), project_id != "", !is.na(completed_year))

if (nrow(project_df) != n_distinct(project_df$project_id)) {
  stop("ZAP project classification input is not unique by project_id.")
}

project_ccd2010 <- read_csv("../input/zap_zoning_map_special_permit_project_ccd2010_fractional.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    project_id = as.character(project_id),
    district_id = sprintf("%02d", suppressWarnings(as.integer(ccd2010_district_id))),
    ccd2010_council_district = suppressWarnings(as.integer(ccd2010_council_district)),
    ccd2010_assignment_weight = suppressWarnings(as.numeric(ccd2010_assignment_weight))
  ) |>
  filter(!is.na(project_id), project_id != "", !is.na(district_id), !is.na(ccd2010_assignment_weight))

if (nrow(project_ccd2010) != nrow(distinct(project_ccd2010, project_id, district_id))) {
  stop("ZAP project to 2010 Council district assignment is not unique by project_id and district_id.")
}

project_ccd2010_weight_bad_count <- project_ccd2010 |>
  group_by(project_id) |>
  summarize(weight_sum = sum(ccd2010_assignment_weight), .groups = "drop") |>
  filter(abs(weight_sum - 1) > 1e-8) |>
  nrow()

district_lookup <- read_csv("../input/ccdist2010_homeownership_1990_measure.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    council_district = suppressWarnings(as.integer(council_district)),
    borough_code = as.character(suppressWarnings(as.integer(borough_code))),
    borough_name = as.character(borough_name),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990)),
    vacancy_rate_1990 = suppressWarnings(as.numeric(vacancy_rate_1990)),
    median_household_income_1990 = suppressWarnings(as.numeric(median_household_income_1990)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro))
  ) |>
  filter(!is.na(district_id), !is.na(council_district), occupied_units_1990 > 0)

if (nrow(district_lookup) != 51 || nrow(district_lookup) != n_distinct(district_lookup$district_id)) {
  stop("Expected exactly 51 unique 2010 Council districts in the treatment lookup.")
}

increased_residential_projects <- project_df |>
  filter(
    completed_year >= 1980,
    completed_year <= 2025,
    included_zm_plus_residential_zs,
    increased_residential_proxy
  ) |>
  select(project_id, completed_year, mixed_use_text_flag)

assigned_increased_residential_projects <- increased_residential_projects |>
  inner_join(project_ccd2010, by = "project_id", relationship = "one-to-many")

district_year_outcomes <- assigned_increased_residential_projects |>
  group_by(district_id, completed_year) |>
  summarize(outcome_value = sum(ccd2010_assignment_weight), .groups = "drop") |>
  rename(year = completed_year)

design_df <- expand_grid(
  district_lookup,
  year = 1980:2025
) |>
  left_join(district_year_outcomes, by = c("district_id", "year"), relationship = "one-to-one") |>
  mutate(
    outcome_value = coalesce(outcome_value, 0),
    source_family = "zap_zoning_map_special_permit",
    source_label = "ZAP completed ZM plus residential-ZS project records assigned by BBL to 2010 Council districts",
    series_family = "increased_residential_project_records",
    outcome_label = "Increased residential ZAP actions",
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
    event_period = factor(event_period, levels = event_periods),
    borough_period = interaction(borough_code, event_period, drop = TRUE),
    outcome_rate = 10000 * outcome_value / occupied_units_1990
  )

if (nrow(design_df) != 51 * 46 || nrow(design_df) != nrow(distinct(design_df, district_id, year))) {
  stop("ZAP event-study design must be unique and complete by 51 districts and 46 years.")
}

pre_rate_df <- design_df |>
  filter(year >= 1980, year <= 1988) |>
  group_by(series_family, district_id) |>
  summarize(pre_1980_1988_rate = mean(outcome_rate, na.rm = TRUE), .groups = "drop") |>
  group_by(series_family) |>
  mutate(pre_1980_1988_rate_z = z_score(pre_1980_1988_rate)) |>
  ungroup() |>
  select(series_family, district_id, pre_1980_1988_rate_z)

control_lookup <- design_df |>
  distinct(district_id, borough_code, borough_name, occupied_units_1990, vacancy_rate_1990, median_household_income_1990) |>
  mutate(log_occupied_units_1990 = log(occupied_units_1990)) |>
  group_by(borough_code, borough_name) |>
  mutate(
    log_occupied_units_1990_z = z_score(log_occupied_units_1990),
    vacancy_rate_1990_z = z_score(vacancy_rate_1990),
    median_household_income_1990_z = z_score(median_household_income_1990)
  ) |>
  ungroup() |>
  select(district_id, log_occupied_units_1990_z, vacancy_rate_1990_z, median_household_income_1990_z)

design_df <- design_df |>
  left_join(pre_rate_df, by = c("series_family", "district_id"), relationship = "many-to-one") |>
  left_join(control_lookup, by = "district_id", relationship = "many-to-one") |>
  mutate(
    pre_1980_1988_rate_z = coalesce(pre_1980_1988_rate_z, 0),
    log_occupied_units_1990_z = coalesce(log_occupied_units_1990_z, 0),
    vacancy_rate_1990_z = coalesce(vacancy_rate_1990_z, 0),
    median_household_income_1990_z = coalesce(median_household_income_1990_z, 0)
  )

control_vars <- c("log_occupied_units_1990_z", "median_household_income_1990_z", "vacancy_rate_1990_z", "pre_1980_1988_rate_z")

for (period_value in estimated_event_periods) {
  design_df[[paste0("treat_z_boro_x_", sanitize_period(period_value))]] <- design_df$treat_z_boro * as.integer(as.character(design_df$event_period) == period_value)

  for (control_var in control_vars) {
    design_df[[paste0(control_var, "_x_", sanitize_period(period_value))]] <- design_df[[control_var]] * as.integer(as.character(design_df$event_period) == period_value)
  }
}

treat_terms <- paste0("treat_z_boro_x_", sanitize_period(estimated_event_periods))
control_terms <- unlist(lapply(control_vars, function(control_var) paste0(control_var, "_x_", sanitize_period(estimated_event_periods))))

event_model <- feols(
  as.formula(paste0("outcome_rate ~ ", paste(c(treat_terms, control_terms), collapse = " + "), " | district_id + borough_period")),
  cluster = ~district_id,
  data = design_df
)

event_df <- bind_rows(
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
    event_model,
    tibble(term = treat_terms, event_period = estimated_event_periods, is_reference = FALSE)
  )
) |>
  mutate(
    event_period = factor(event_period, levels = event_periods),
    event_period_index = match(as.character(event_period), event_periods),
    source_family = "zap_zoning_map_special_permit",
    source_label = "ZAP completed ZM plus residential-ZS project records assigned by BBL to 2010 Council districts",
    series_family = "increased_residential_project_records",
    outcome_label = "Increased residential ZAP actions",
    outcome_scale = "per_10000_occupied_1990",
    reference_period = reference_event_period,
    model = "district_fe_borough_period_fe_controls",
    control_label = "log occupied units + median income + vacancy + pre-production",
    n_obs = model_nobs(event_model),
    within_r2 = tryCatch(as.numeric(r2(event_model, type = "wr2")), error = function(e) NA_real_)
  ) |>
  arrange(event_period)

missing_event_terms <- event_df |>
  filter(!is_reference, is.na(estimate)) |>
  nrow()

if (missing_event_terms > 0) {
  stop("ZAP event study has missing treatment terms.")
}

write_csv_if_changed(
  design_df |>
    select(
      source_family,
      source_label,
      series_family,
      outcome_label,
      district_id,
      council_district,
      borough_code,
      borough_name,
      year,
      event_period,
      outcome_value,
      outcome_rate,
      occupied_units_1990,
      treat_z_boro,
      log_occupied_units_1990_z,
      median_household_income_1990_z,
      vacancy_rate_1990_z,
      pre_1980_1988_rate_z
    ) |>
    arrange(district_id, year),
  "../output/zap_zoning_actions_event_design_panel.csv"
)

write_csv_if_changed(event_df, "../output/zap_zoning_actions_event_coefficients_5yr_bins.csv")

pdf("../output/zap_zoning_actions_event_coefficients_5yr_bins.pdf", width = 11, height = 8.5)
print(
  ggplot(event_df, aes(x = event_period_index, y = estimate, color = outcome_label, group = outcome_label)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(
      data = filter(event_df, !is_reference),
      aes(ymin = conf_low, ymax = conf_high),
      width = 0.12,
      linewidth = 0.45
    ) +
    geom_line(linewidth = 0.75) +
    geom_point(size = 2.1) +
    scale_color_manual(values = c("Increased residential ZAP actions" = "#2f7d32")) +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(
      title = "ZAP increased-residential actions event study, five-year bins",
      x = NULL,
      y = "Coefficient on homeowner exposure",
      color = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

window_defs <- tribble(
  ~comparison_id, ~pre_start, ~pre_end, ~post_start, ~post_end,
  "placebo_1985_1989_minus_1980_1984", 1980L, 1984L, 1985L, 1989L,
  "post_1990_1999_minus_1980_1988", 1980L, 1988L, 1990L, 1999L,
  "post_2000_2009_minus_1980_1988", 1980L, 1988L, 2000L, 2009L,
  "post_2010_2019_minus_1980_1988", 1980L, 1988L, 2010L, 2019L,
  "post_2020_2025_minus_1980_1988", 1980L, 1988L, 2020L, 2025L
) |>
  mutate(
    pre_window = paste0(pre_start, "-", pre_end),
    post_window = paste0(post_start, "-", post_end)
  )

long_diff_rows <- list()

for (i in seq_len(nrow(window_defs))) {
  window_row <- window_defs[i, ]

  pre_df <- design_df |>
    filter(year >= window_row$pre_start, year <= window_row$pre_end) |>
    group_by(district_id) |>
    summarize(pre_avg = mean(outcome_rate, na.rm = TRUE), pre_year_count = n_distinct(year), .groups = "drop")

  post_df <- design_df |>
    filter(year >= window_row$post_start, year <= window_row$post_end) |>
    group_by(district_id) |>
    summarize(post_avg = mean(outcome_rate, na.rm = TRUE), post_year_count = n_distinct(year), .groups = "drop")

  diff_df <- design_df |>
    distinct(
      district_id,
      council_district,
      borough_code,
      borough_name,
      treat_z_boro,
      log_occupied_units_1990_z,
      median_household_income_1990_z,
      vacancy_rate_1990_z,
      pre_1980_1988_rate_z
    ) |>
    left_join(pre_df, by = "district_id", relationship = "one-to-one") |>
    left_join(post_df, by = "district_id", relationship = "one-to-one") |>
    mutate(delta_value = post_avg - pre_avg)

  model_df <- diff_df |>
    select(delta_value, pre_avg, treat_z_boro, borough_code, all_of(control_vars)) |>
    filter(if_all(everything(), ~ !is.na(.x)))

  model <- feols(
    as.formula(paste0("delta_value ~ treat_z_boro + ", paste(control_vars, collapse = " + "), " | borough_code")),
    data = model_df,
    vcov = "hetero"
  )

  long_diff_rows[[window_row$comparison_id]] <- extract_model_terms(model, tibble(term = "treat_z_boro")) |>
    transmute(
      source_family = "zap_zoning_map_special_permit",
      comparison_id = window_row$comparison_id,
      pre_window = window_row$pre_window,
      post_window = window_row$post_window,
      series_family = "increased_residential_project_records",
      outcome_label = "Increased residential ZAP actions",
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

long_diff_df <- bind_rows(long_diff_rows) |>
  mutate(
    row_order = case_when(
      comparison_id == "placebo_1985_1989_minus_1980_1984" ~ 1L,
      comparison_id == "post_1990_1999_minus_1980_1988" ~ 2L,
      comparison_id == "post_2000_2009_minus_1980_1988" ~ 3L,
      comparison_id == "post_2010_2019_minus_1980_1988" ~ 4L,
      comparison_id == "post_2020_2025_minus_1980_1988" ~ 5L,
      TRUE ~ NA_integer_
    ),
    column_label = case_when(
      comparison_id == "placebo_1985_1989_minus_1980_1984" ~ "Placebo",
      comparison_id == "post_1990_1999_minus_1980_1988" ~ "1990--1999",
      comparison_id == "post_2000_2009_minus_1980_1988" ~ "2000--2009",
      comparison_id == "post_2010_2019_minus_1980_1988" ~ "2010--2019",
      comparison_id == "post_2020_2025_minus_1980_1988" ~ "2020--2025",
      TRUE ~ comparison_id
    ),
    estimate_label = paste0(format_decimal(estimate, 2), significance_stars(p_value)),
    std_error_label = format_decimal(std_error, 2),
    initial_outcome_mean_label = format_decimal(initial_outcome_mean, 2),
    p_value_label = format_p_value(p_value)
  ) |>
  arrange(row_order)

if (nrow(long_diff_df) != 5 || any(is.na(long_diff_df$row_order))) {
  stop("Long-difference table expected exactly five rows.")
}

write_csv_if_changed(long_diff_df, "../output/zap_zoning_actions_long_difference_estimates.csv")

checkmark_values <- rep("\\checkmark", nrow(long_diff_df))

table_lines <- c(
  "\\begin{table}[htbp]",
  "    \\centering",
  "    \\begin{threeparttable}",
  "    \\caption{Long-Difference Estimates for ZAP Increased-Residential Actions}",
  "    \\label{tab:zap_zoning_actions_long_difference}",
  "    \\small",
  "    \\begin{tabular}{lccccc}",
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
  "    \\item \\textit{Notes:} Table reports coefficients on within-borough standardized 1990 homeownership from 2010 Council-district long-difference regressions. The outcome is average ZAP increased-residential completed zoning action project records per 10,000 1990 occupied units. Project records are completed ZM records plus residential-ZS records, classified with the ZAP text proxy and fractionally assigned by BBL to 2010 Council districts. Column (1) compares 1985--1989 to 1980--1984. Columns (2)--(5) compare the listed post window to the 1980--1988 pre-period. Controls match the main housing event-study specification: log 1990 occupied units, 1990 median household income, 1990 vacancy rate, and pre-period ZAP increased-residential action rate. Standard errors are heteroskedasticity-robust and shown in parentheses. * $p < 0.10$, ** $p < 0.05$, *** $p < 0.01$.",
  "    \\end{tablenotes}",
  "    \\end{threeparttable}",
  "\\end{table}"
)

write_lines_if_changed(table_lines, "../output/zap_zoning_actions_long_difference.tex")

write_csv_if_changed(
  bind_rows(
    tibble(metric = "district_count", value = as.character(n_distinct(design_df$district_id)), note = "2010 Council districts in the ZAP event-study design."),
    tibble(metric = "year_min", value = as.character(min(design_df$year, na.rm = TRUE)), note = "Minimum event-study year."),
    tibble(metric = "year_max", value = as.character(max(design_df$year, na.rm = TRUE)), note = "Maximum event-study year."),
    tibble(metric = "design_row_count", value = as.character(nrow(design_df)), note = "Rows in the district-year panel."),
    tibble(metric = "event_coefficient_rows", value = as.character(nrow(event_df)), note = "Rows in the event-study coefficient output, including reference periods."),
    tibble(metric = "long_difference_rows", value = as.character(nrow(long_diff_df)), note = "Rows in the long-difference output."),
    tibble(metric = "missing_treat_count", value = as.character(sum(is.na(design_df$treat_z_boro))), note = "Design rows missing the treatment."),
    tibble(metric = "project_ccd2010_fractional_weight_bad_count", value = as.character(project_ccd2010_weight_bad_count), note = "Assigned project weights should sum to one across 2010 Council districts."),
    tibble(metric = "increased_residential_project_count_1980_2025", value = as.character(n_distinct(increased_residential_projects$project_id)), note = "Input increased-residential projects in 1980-2025 before requiring BBL-based Council-district assignment."),
    tibble(metric = "increased_residential_assigned_project_count_1980_2025", value = as.character(n_distinct(assigned_increased_residential_projects$project_id)), note = "Increased-residential projects in 1980-2025 with at least one BBL-based 2010 Council-district assignment."),
    tibble(metric = "increased_residential_missing_ccd2010_project_count_1980_2025", value = as.character(n_distinct(increased_residential_projects$project_id) - n_distinct(assigned_increased_residential_projects$project_id)), note = "Increased-residential projects excluded from the event-study outcome because no BBL-based 2010 Council-district assignment is available."),
    tibble(metric = "missing_event_treatment_terms", value = as.character(missing_event_terms), note = "Requested five-year treatment terms missing from output."),
    tibble(metric = "status", value = as.character(as.integer(n_distinct(design_df$district_id) == 51 && nrow(design_df) == 51 * 46 && missing_event_terms == 0 && project_ccd2010_weight_bad_count == 0)), note = "One means the ZAP event-study design passes core structural checks.")
  ),
  "../output/zap_zoning_actions_design_qc.csv"
)

cat("Wrote ZAP zoning action event-study outputs to ../output\n")
