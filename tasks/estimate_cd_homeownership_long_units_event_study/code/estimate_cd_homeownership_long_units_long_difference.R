# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/estimate_cd_homeownership_long_units_event_study/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(fixest)
  library(readr)
  library(tibble)
})

source("../../_lib/data_reports.R")

z_score <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  x_sd <- sd(x, na.rm = TRUE)

  if (is.na(x_sd) || x_sd == 0) {
    return(rep(0, length(x)))
  }

  (x - mean(x, na.rm = TRUE)) / x_sd
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

regression_table_row <- function(row_label, values) {
  paste0("    ", row_label, " & ", paste(values, collapse = " & "), " \\\\")
}

series_df <- read_csv("../input/cd_homeownership_long_units_series.csv", show_col_types = FALSE, na = c("", "NA")) %>%
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
    series_family == "units_built_5_plus",
    !is.na(year),
    year >= 1970,
    year <= 2025,
    occupied_units_1990 > 0
  )

if (n_distinct(series_df$district_id) != 59) {
  stop("Expected 59 community districts in the long-difference input.")
}

# District controls: within-borough z-scores of 1990 characteristics and the
# citywide z-score of 1970-1988 mean annual 5+ unit production.
district_df <- series_df %>%
  group_by(district_id, borocd, borough_code, borough_name, treat_z_boro, occupied_units_1990, vacancy_rate_1990, median_household_income_1990) %>%
  summarise(pre_1970_1988_count = mean(outcome_value[year <= 1988], na.rm = TRUE), .groups = "drop") %>%
  group_by(borough_code, borough_name) %>%
  mutate(
    log_occupied_units_1990_z = z_score(log(occupied_units_1990)),
    vacancy_rate_1990_z = z_score(vacancy_rate_1990),
    median_household_income_1990_z = z_score(median_household_income_1990)
  ) %>%
  ungroup() %>%
  mutate(pre_1970_1988_count_z = z_score(pre_1970_1988_count))

if (anyDuplicated(district_df$district_id)) {
  stop("Long-difference district table is not unique by district_id.")
}

raw_control_vars <- c("log_occupied_units_1990_z", "median_household_income_1990_z", "vacancy_rate_1990_z", "pre_1970_1988_count_z")

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

raw_long_diff_rows <- list()

for (i in seq_len(nrow(window_defs))) {
  window_row <- window_defs[i, ]

  pre_df <- series_df %>%
    filter(year >= window_row$pre_start, year <= window_row$pre_end) %>%
    group_by(district_id) %>%
    summarise(pre_avg = mean(outcome_value, na.rm = TRUE), pre_year_count = n_distinct(year), .groups = "drop")

  post_df <- series_df %>%
    filter(year >= window_row$post_start, year <= window_row$post_end) %>%
    group_by(district_id) %>%
    summarise(post_avg = mean(outcome_value, na.rm = TRUE), post_year_count = n_distinct(year), .groups = "drop")

  diff_df <- district_df %>%
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
  coef_row <- coeftable(model)["treat_z_boro", ]
  ci_row <- confint(model)["treat_z_boro", ]

  raw_long_diff_rows[[window_row$comparison_id]] <- tibble(
    source_family = "mappluto_proxy_25v4",
    comparison_id = window_row$comparison_id,
    row_order = window_row$row_order,
    column_label = window_row$column_label,
    pre_window = window_row$pre_window,
    post_window = window_row$post_window,
    series_family = "units_built_5_plus",
    outcome_label = "5+ unit buildings",
    outcome_scale = "raw_units_annual_average",
    term = "treat_z_boro",
    estimate = unname(coef_row["Estimate"]),
    std_error = unname(coef_row["Std. Error"]),
    statistic = unname(coef_row["t value"]),
    p_value = unname(coef_row["Pr(>|t|)"]),
    conf_low = unname(ci_row[[1]]),
    conf_high = unname(ci_row[[2]]),
    n_districts = as.integer(model$nobs),
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

save_csv(raw_long_diff_df, "../output/cd_homeownership_long_units_long_difference_raw_units_estimates.csv", c("source_family", "series_family", "comparison_id", "term"))

raw_checkmark_values <- rep("\\checkmark", nrow(raw_long_diff_df))
raw_table_col_spec <- paste0("l", strrep("c", nrow(raw_long_diff_df)))

table_lines <- c(
  "\\begin{table}[htbp]",
  "    \\centering",
  "    \\begin{threeparttable}",
  "    \\caption{Raw-Unit Long-Difference Estimates for 5+ Unit Housing Production}",
  "    \\label{tab:cd_homeownership_long_units_long_difference_raw_units}",
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
  paste0("    \\item \\textit{Notes:} Table reports coefficients on within-borough standardized 1990 homeownership from community-district long-difference regressions. The outcome is average annual $5+$ unit new-building units, measured in raw unit counts with the 25v4 MapPLUTO yearbuilt proxy in all years. All columns use 1985--1989 as the reference period. Column (1) compares 1980--1984 to 1985--1989. Columns (2)--(", nrow(raw_long_diff_df), ") compare the listed five-year post window to 1985--1989. The initial outcome mean is the sample mean of the 1985--1989 outcome level. Controls include log 1990 occupied units, 1990 median household income, 1990 vacancy rate, and 1970--1988 raw pre-period production. Standard errors are heteroskedasticity-robust and shown in parentheses. * $p < 0.10$, ** $p < 0.05$, *** $p < 0.01$."),
  "    \\end{tablenotes}",
  "    \\end{threeparttable}",
  "\\end{table}"
)

writeLines(table_lines, "../output/cd_homeownership_long_units_long_difference_raw_units.tex")

cat("Wrote community district long-difference outputs to ../output\n")
