# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/summarize_cd_homeownership_long_units_drivers/code")
# bin_scheme <- "5yr_bins"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(readr)
  library(tibble)
  library(tidyr)
})

source("../../../_lib/data_reports.R")

if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  stopifnot(length(args) == 1)
  bin_scheme <- args[1]
}

# Bins, omitted bin, and default pre-production window match the production
# event study (tasks/estimate_cd_homeownership_long_units_event_study).
bin_starts <- switch(bin_scheme,
  "5yr_bins" = seq(1970, 2020, by = 5),
  "decade_bins" = seq(1970, 2020, by = 10),
  "decade_pre_bins" = c(1970, 1980, seq(1985, 2020, by = 5)),
  stop("Unknown bin scheme: ", bin_scheme)
)
reference_period <- switch(bin_scheme, "5yr_bins" = "1985-1989", "decade_bins" = "1980-1989", "decade_pre_bins" = "1985-1989")
default_pre_years <- switch(bin_scheme, "5yr_bins" = 1970:1988, "decade_bins" = 1960:1969, "decade_pre_bins" = 1960:1969)
event_periods <- paste0(bin_starts, "-", c(bin_starts[-1] - 1, 2025))
four_cds <- c(301L, 302L, 401L, 402L)

z_score <- function(x) (x - mean(x)) / sd(x)

# ---- District table: treatment, 1990 controls, and pre-2010 upzoning flags ----

series <- read_csv("../input/cd_homeownership_long_units_series.csv", show_col_types = FALSE) %>%
  filter(series_kind == "preferred_long_series", source_family == "mappluto_proxy_25v4") %>%
  mutate(district_id = sprintf("%03d", as.integer(district_id)), borough_code = as.character(borough_code))

# Hand-coded list of major 2001-2009 residential upzonings; each row names its
# ZAP project, and the listed district must be one ZAP records for the project.
upzonings <- read_csv("major_upzonings_2001_2009.csv", show_col_types = FALSE)
zap_projects <- read_parquet("../input/zap_project_data.parquet", col_select = c("project_id", "community_district")) %>%
  filter(project_id %in% upzonings$project_id)
upzoning_check <- upzonings %>%
  left_join(zap_projects, by = "project_id", relationship = "one-to-one") %>%
  mutate(zap_code = paste0(c("M", "X", "K", "Q", "R")[borocd %/% 100], sprintf("%02d", borocd %% 100)))
stopifnot(!anyNA(upzoning_check$community_district), all(mapply(grepl, upzoning_check$zap_code, upzoning_check$community_district)))

districts <- series %>%
  distinct(district_id, borocd, borough_code, borough_name, treat_z_boro, occupied_units_1990, total_housing_units_1990,
           vacancy_rate_1990, median_household_income_1990) %>%
  group_by(borough_code) %>%
  mutate(
    log_occupied_units_1990_z = z_score(log(occupied_units_1990)),
    vacancy_rate_1990_z = z_score(vacancy_rate_1990),
    median_household_income_1990_z = z_score(median_household_income_1990)
  ) %>%
  ungroup() %>%
  mutate(
    large_upzoning = as.integer(borocd %in% upzonings$borocd[upzonings$scale == "large"]),
    any_upzoning = as.integer(borocd %in% upzonings$borocd)
  )
stopifnot(nrow(districts) == 59, !anyDuplicated(districts$district_id), !anyNA(districts))

# ---- Outcomes: raw units by size, 5-49 as 5+ minus 50+, 5+/50+ per 1,000 1990 housing units,
# and each district's percent of its borough's 5+ units in the year. Staten Island
# has years with no 5+ units (1996, 2013), so the percent outcome omits its 3 districts;
# it also starts in 1960, the earliest pre-production window, because the Bronx
# and Manhattan each have one 1940s year with no 5+ units. ----

outcomes <- series %>%
  filter(series_family %in% c("units_built_1_4", "units_built_5_plus", "units_built_50_plus")) %>%
  select(district_id, year, series_family, outcome_value) %>%
  pivot_wider(names_from = series_family, values_from = outcome_value) %>%
  left_join(select(districts, district_id, borough_code, total_housing_units_1990), by = "district_id", relationship = "many-to-one") %>%
  group_by(borough_code, year) %>%
  mutate(pct_of_borough_5_plus_units = 100 * units_built_5_plus / sum(units_built_5_plus)) %>%
  ungroup() %>%
  mutate(
    units_built_5_49 = units_built_5_plus - units_built_50_plus,
    units_5_plus_per_1000_units_1990 = 1000 * units_built_5_plus / total_housing_units_1990,
    units_50_plus_per_1000_units_1990 = 1000 * units_built_50_plus / total_housing_units_1990
  ) %>%
  select(-borough_code, -total_housing_units_1990) %>%
  pivot_longer(-c(district_id, year), names_to = "outcome", values_to = "outcome_value") %>%
  filter(!(outcome == "pct_of_borough_5_plus_units" & (substr(district_id, 1, 1) == "5" | year < 1960)))
stopifnot(!anyNA(outcomes$outcome_value), all(outcomes$outcome_value >= 0))
stopifnot(nrow(outcomes) == 59 * 6 * n_distinct(outcomes$year) + 56 * length(1960:2025))

panel <- outcomes %>%
  filter(year >= 1970, year <= 2025) %>%
  mutate(event_period = factor(event_periods[findInterval(year, bin_starts)], levels = event_periods)) %>%
  left_join(districts, by = "district_id", relationship = "many-to-one")

# Pre-production control: citywide z-score of mean annual units in the window.
pre_count_z <- function(outcome_id, pre_years) {
  outcomes %>%
    filter(outcome == outcome_id, year %in% pre_years) %>%
    group_by(district_id) %>%
    summarise(pre_count = mean(outcome_value), years = n(), .groups = "drop") %>%
    mutate(pre_count_z = z_score(pre_count)) %>%
    select(district_id, pre_count_z)
}

# ---- Event-study fit: the production specification with optional changes ----

controls_1990 <- c("log_occupied_units_1990_z", "median_household_income_1990_z", "vacancy_rate_1990_z")

fit_event <- function(outcome_id, pre_years = default_pre_years, drop_borocd = integer(), boroughs = NULL, controls = controls_1990) {
  model_df <- panel %>%
    filter(outcome == outcome_id, !borocd %in% drop_borocd, is.null(boroughs) | borough_name %in% boroughs)

  if (!is.null(pre_years)) {
    model_df <- left_join(model_df, pre_count_z(outcome_id, pre_years), by = "district_id", relationship = "many-to-one")
    controls <- c(controls, "pre_count_z")
  }

  model <- feols(
    as.formula(paste0(
      "outcome_value ~ ",
      paste0("i(event_period, ", c("treat_z_boro", controls), ", ref = '", reference_period, "')", collapse = " + "),
      " | district_id + borough_code^event_period"
    )),
    cluster = ~district_id,
    data = model_df
  )
  terms <- paste0("event_period::", setdiff(event_periods, reference_period), ":treat_z_boro")
  ci <- confint(model)[terms, ]

  tibble(
    event_period = event_periods,
    estimate = unname(coef(model)[terms])[match(event_periods, setdiff(event_periods, reference_period))],
    std_error = unname(se(model)[terms])[match(event_periods, setdiff(event_periods, reference_period))],
    conf_low = ci[[1]][match(event_periods, setdiff(event_periods, reference_period))],
    conf_high = ci[[2]][match(event_periods, setdiff(event_periods, reference_period))],
    n_districts = n_distinct(model_df$district_id)
  ) %>%
    mutate(estimate = if_else(event_period == reference_period, 0, estimate))
}

run <- function(scenario, outcome_id, ...) {
  fit_event(outcome_id, ...) %>%
    mutate(bin_scheme = bin_scheme, scenario = scenario, outcome = outcome_id, .before = 1)
}

large_cds <- districts$borocd[districts$large_upzoning == 1]
any_cds <- districts$borocd[districts$any_upzoning == 1]

coefficients <- bind_rows(
  run("baseline", "units_built_1_4"),
  run("baseline", "units_built_5_plus"),
  run("baseline", "units_built_5_49"),
  run("baseline", "units_built_50_plus"),
  run("baseline", "units_5_plus_per_1000_units_1990"),
  run("baseline", "units_50_plus_per_1000_units_1990"),
  run("no_pre_control", "units_built_5_plus", pre_years = NULL),
  run("treatment_only_no_controls", "units_built_5_plus", pre_years = NULL, controls = character()),
  run("no_income_control", "units_built_5_plus", controls = c("log_occupied_units_1990_z", "vacancy_rate_1990_z")),
  run("pre_control_1960_1969", "units_built_5_plus", pre_years = 1960:1969),
  run("pre_control_1970_1984", "units_built_5_plus", pre_years = 1970:1984),
  run("pre_control_1970_1988", "units_built_5_plus", pre_years = 1970:1988),
  run("drop_four_cds", "units_built_1_4", drop_borocd = four_cds),
  run("drop_four_cds", "units_built_5_plus", drop_borocd = four_cds),
  run("drop_four_cds", "units_built_5_49", drop_borocd = four_cds),
  run("drop_four_cds", "units_built_50_plus", drop_borocd = four_cds),
  run("drop_four_cds", "units_5_plus_per_1000_units_1990", drop_borocd = four_cds),
  run("baseline", "pct_of_borough_5_plus_units"),
  run("drop_four_cds", "pct_of_borough_5_plus_units", drop_borocd = four_cds),
  run("drop_four_cds_and_306", "units_built_5_plus", drop_borocd = c(four_cds, 306L)),
  run("drop_large_upzoning_cds", "units_built_5_plus", drop_borocd = large_cds),
  run("drop_any_upzoning_cds", "units_built_5_plus", drop_borocd = any_cds),
  run("large_upzoning_x_period_control", "units_built_5_plus", controls = c(controls_1990, "large_upzoning")),
  run("any_upzoning_x_period_control", "units_built_5_plus", controls = c(controls_1990, "any_upzoning")),
  run("drop_manhattan", "units_built_5_plus", boroughs = c("Bronx", "Brooklyn", "Queens", "Staten Island")),
  run("drop_manhattan_and_four_cds", "units_built_5_plus", boroughs = c("Bronx", "Brooklyn", "Queens", "Staten Island"), drop_borocd = four_cds),
  run("only_manhattan", "units_built_5_plus", boroughs = "Manhattan"),
  run("only_bronx", "units_built_5_plus", boroughs = "Bronx"),
  run("only_brooklyn", "units_built_5_plus", boroughs = "Brooklyn"),
  run("only_queens", "units_built_5_plus", boroughs = "Queens")
)

# The baseline must reproduce the production coefficients exactly.
production <- read_csv(paste0("../input/cd_homeownership_long_units_event_coefficients_raw_units_", bin_scheme, ".csv"), show_col_types = FALSE)
reproduced <- coefficients %>%
  filter(scenario == "baseline", outcome %in% c("units_built_1_4", "units_built_5_plus")) %>%
  inner_join(production, by = c("outcome" = "series_family", "event_period"), suffix = c("", "_production"), relationship = "one-to-one")
stopifnot(nrow(reproduced) == 2 * length(event_periods), max(abs(reproduced$estimate - reproduced$estimate_production)) < 1e-8,
          max(abs(reproduced$std_error - reproduced$std_error_production), na.rm = TRUE) < 1e-8)

save_csv(coefficients, paste0("../output/cd_homeownership_long_units_drivers_coefficients_", bin_scheme, ".csv"), c("bin_scheme", "scenario", "outcome", "event_period"))

# ---- Leave one district out, 5+ units ----

leave_one_out <- bind_rows(lapply(districts$borocd, function(cd) {
  fit_event("units_built_5_plus", drop_borocd = cd) %>% mutate(dropped_borocd = cd, .before = 1)
})) %>%
  left_join(filter(coefficients, scenario == "baseline", outcome == "units_built_5_plus") %>% select(event_period, baseline_estimate = estimate),
            by = "event_period", relationship = "many-to-one") %>%
  mutate(bin_scheme = bin_scheme, shift = estimate - baseline_estimate, .before = 1) %>%
  left_join(select(districts, dropped_borocd = borocd, borough_name, treat_z_boro), by = "dropped_borocd", relationship = "many-to-one")

save_csv(leave_one_out, paste0("../output/cd_homeownership_long_units_drivers_leave_one_out_", bin_scheme, ".csv"), c("bin_scheme", "dropped_borocd", "event_period"))

# ---- Exact district contributions ----
# Each coefficient equals sum_d r_d * (bin mean - reference mean)_d / sum_d r_d^2,
# where r_d is the treatment residualized on the controls and borough dummies.

contributions <- bind_rows(lapply(c("units_built_5_plus", "units_built_50_plus"), function(outcome_id) {
  cross_section <- districts %>% left_join(pre_count_z(outcome_id, default_pre_years), by = "district_id", relationship = "one-to-one")
  treat_residual <- resid(lm(treat_z_boro ~ log_occupied_units_1990_z + median_household_income_1990_z + vacancy_rate_1990_z + pre_count_z + borough_code, data = cross_section))
  sum_squared_residuals <- sum(treat_residual^2)

  panel %>%
    filter(outcome == outcome_id) %>%
    group_by(district_id, event_period) %>%
    summarise(period_mean = mean(outcome_value), .groups = "drop") %>%
    group_by(district_id) %>%
    mutate(change_from_reference = period_mean - period_mean[event_period == reference_period]) %>%
    ungroup() %>%
    left_join(tibble(district_id = cross_section$district_id, treat_residual = treat_residual), by = "district_id", relationship = "many-to-one") %>%
    mutate(contribution = treat_residual * change_from_reference / sum_squared_residuals, outcome = outcome_id)
})) %>%
  left_join(select(districts, district_id, borocd, borough_name, treat_z_boro, large_upzoning, any_upzoning), by = "district_id", relationship = "many-to-one") %>%
  mutate(bin_scheme = bin_scheme, event_period = as.character(event_period), .before = 1)

contribution_check <- contributions %>%
  group_by(outcome, event_period) %>%
  summarise(total = sum(contribution), .groups = "drop") %>%
  inner_join(filter(coefficients, scenario == "baseline"), by = c("outcome", "event_period"), relationship = "one-to-one")
stopifnot(nrow(contribution_check) == 2 * length(event_periods), max(abs(contribution_check$total - contribution_check$estimate)) < 1e-6)

save_csv(contributions, paste0("../output/cd_homeownership_long_units_drivers_contributions_", bin_scheme, ".csv"), c("bin_scheme", "outcome", "district_id", "event_period"))

# ---- Plots ----

period_axis <- scale_x_discrete(limits = event_periods)
theme_drivers <- theme_minimal(base_size = 10) +
  theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1), panel.grid.minor = element_blank())
coef_plot <- function(plot_df, title_text) {
  ggplot(plot_df, aes(x = event_period, y = estimate, color = label, group = label)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.3) +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.1, linewidth = 0.35, position = position_dodge(width = 0.5), na.rm = TRUE) +
    geom_line(linewidth = 0.6, position = position_dodge(width = 0.5)) +
    geom_point(size = 1.6, position = position_dodge(width = 0.5)) +
    period_axis +
    labs(title = title_text, subtitle = paste0("Bin scheme ", bin_scheme, "; omitted bin ", reference_period, "; 95% CIs clustered by community district"),
         x = NULL, y = "Coefficient on homeowner exposure", color = NULL) +
    theme_drivers
}

pdf(paste0("../output/cd_homeownership_long_units_drivers_plots_", bin_scheme, ".pdf"), width = 11, height = 8.5)
print(coef_plot(
  coefficients %>%
    filter(scenario == "baseline", outcome %in% c("units_built_1_4", "units_built_5_49", "units_built_50_plus")) %>%
    mutate(label = recode(outcome, units_built_1_4 = "1-4 unit buildings", units_built_5_49 = "5-49 unit buildings", units_built_50_plus = "50+ unit buildings")),
  "Size decomposition of the homeowner gradient (raw annual units)"
))
print(coef_plot(
  coefficients %>%
    filter(outcome == "units_built_5_plus", scenario %in% c("baseline", "drop_four_cds", "drop_four_cds_and_306", "drop_large_upzoning_cds", "drop_manhattan")) %>%
    mutate(label = recode(scenario, baseline = "Baseline", drop_four_cds = "Drop CDs 301, 302, 401, 402", drop_four_cds_and_306 = "Drop CDs 301, 302, 306, 401, 402",
                          drop_large_upzoning_cds = "Drop large 2001-09 upzoning CDs", drop_manhattan = "Drop Manhattan")),
  "5+ unit buildings: which districts carry the gradient"
))
print(coef_plot(
  coefficients %>%
    filter(outcome == "units_built_5_plus", scenario %in% c("baseline", "no_pre_control", "no_income_control", "treatment_only_no_controls", "large_upzoning_x_period_control")) %>%
    mutate(label = recode(scenario, baseline = "Baseline", no_pre_control = "No pre-production control", no_income_control = "No income control",
                          treatment_only_no_controls = "No controls", large_upzoning_x_period_control = "Large upzoning x period control")),
  "5+ unit buildings: sensitivity to controls"
))
print(coef_plot(
  coefficients %>%
    filter(outcome %in% c("units_5_plus_per_1000_units_1990", "units_50_plus_per_1000_units_1990"), scenario %in% c("baseline", "drop_four_cds")) %>%
    mutate(label = paste(recode(outcome, units_5_plus_per_1000_units_1990 = "5+", units_50_plus_per_1000_units_1990 = "50+"),
                         recode(scenario, baseline = "baseline", drop_four_cds = "drop four CDs"))),
  "Units per 1,000 1990 housing units"
))
print(coef_plot(
  coefficients %>%
    filter(outcome == "pct_of_borough_5_plus_units", scenario %in% c("baseline", "drop_four_cds")) %>%
    mutate(label = recode(scenario, baseline = "Baseline", drop_four_cds = "Drop CDs 301, 302, 401, 402")),
  "5+ units as percent of the borough's 5+ units that year (Staten Island omitted)"
))
print(
  contributions %>%
    filter(outcome == "units_built_5_plus") %>%
    mutate(group = case_when(borocd %in% four_cds ~ "CDs 301, 302, 401, 402", borocd == 306 ~ "CD 306", TRUE ~ paste("Other", borough_name))) %>%
    group_by(event_period, group) %>%
    summarise(contribution = sum(contribution), .groups = "drop") %>%
    ggplot(aes(x = event_period, y = contribution, fill = group)) +
    geom_col(width = 0.7) +
    geom_hline(yintercept = 0, color = "#333333", linewidth = 0.3) +
    stat_summary(aes(group = 1), fun = sum, geom = "point", shape = 21, fill = "white", size = 2.2) +
    scale_fill_manual(values = c("CDs 301, 302, 401, 402" = "#c44e52", "CD 306" = "#f2a7a9", "Other Manhattan" = "#4c72b0", "Other Bronx" = "#8172b2",
                                 "Other Brooklyn" = "#dd8452", "Other Queens" = "#55a868", "Other Staten Island" = "#937860")) +
    period_axis +
    labs(title = "Where the 5+ unit coefficient comes from: exact district contributions",
         subtitle = "Bars sum to the baseline coefficient (white points); contribution = residualized treatment x change from omitted bin / sum of squared residuals",
         x = NULL, y = "Contribution to coefficient (units)", fill = NULL) +
    theme_drivers
)
print(
  ggplot(filter(leave_one_out, event_period != reference_period), aes(x = event_period, y = estimate)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.3) +
    geom_jitter(width = 0.12, height = 0, size = 1, alpha = 0.5, color = "#555555") +
    geom_point(aes(y = baseline_estimate), shape = 95, size = 8, color = "#2f7d32") +
    geom_text(data = leave_one_out %>% filter(event_period != reference_period) %>% group_by(event_period) %>% slice_max(abs(shift), n = 3),
              aes(label = dropped_borocd), size = 2.6, nudge_x = 0.28, color = "#c44e52") +
    period_axis +
    labs(title = "5+ unit buildings: leave one community district out",
         subtitle = "Grey points are estimates dropping one district; green bars are the baseline; labels mark the three largest shifts",
         x = NULL, y = "Coefficient on homeowner exposure") +
    theme_drivers
)
dev.off()

cat("Wrote", bin_scheme, "drivers outputs to ../output\n")
