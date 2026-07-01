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
  ~outcome_id, ~outcome_label,
  "units_built_1_4", "1-4 unit buildings",
  "units_built_5_plus", "5+ unit buildings"
)

series_lookup <- read_csv("../input/cd_homeownership_long_units_series.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = as.character(borough_name),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro))
  ) |>
  distinct()

assert_unique_keys(series_lookup, "borocd", "Long-units treatment lookup")

controls_clean <- read_csv("../input/cd_baseline_1990_controls.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = as.character(borough_name),
    occupied_units_1990_exact = suppressWarnings(as.numeric(occupied_units_1990_exact)),
    log_occupied_units_1990_exact = log(suppressWarnings(as.numeric(occupied_units_1990_exact))),
    median_household_income_1990_1999_dollars_exact = suppressWarnings(as.numeric(median_household_income_1990_1999_dollars_exact)),
    poverty_share_1990_exact = suppressWarnings(as.numeric(poverty_share_1990_exact))
  )

assert_unique_keys(controls_clean, "borocd", "Baseline controls")

residential_acre_lookup <- read_csv("../input/cd_redevelopment_potential_baseline.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    residential_acres = suppressWarnings(as.numeric(residential_acres))
  )

assert_unique_keys(residential_acre_lookup, "borocd", "Redevelopment-potential baseline")

controls_z <- controls_clean |>
  left_join(residential_acre_lookup, by = "borocd", relationship = "one-to-one") |>
  mutate(
    log_occupied_units_1990_exact_z = z_score(log_occupied_units_1990_exact),
    median_household_income_1990_1999_dollars_exact_z = z_score(median_household_income_1990_1999_dollars_exact),
    poverty_share_1990_exact_z = z_score(poverty_share_1990_exact)
  )

pluto_panel <- read_csv("../input/mappluto_construction_proxy_cd_year.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    year = suppressWarnings(as.integer(yearbuilt)),
    units_built_1_4 = suppressWarnings(as.numeric(units_1_4_proxy)),
    units_built_5_plus = suppressWarnings(as.numeric(units_5_plus_proxy))
  ) |>
  filter(year >= 1980, year <= 2025)

assert_unique_keys(pluto_panel, c("borocd", "year"), "MapPLUTO full-period construction proxy")

analysis_panel <- expand_grid(
  series_lookup |> select(borocd, borough_code, borough_name, treat_z_boro),
  year = 1980:2025
) |>
  left_join(
    pluto_panel,
    by = c("borocd", "year"),
    relationship = "one-to-one"
  ) |>
  mutate(
    units_built_1_4 = coalesce(units_built_1_4, 0),
    units_built_5_plus = coalesce(units_built_5_plus, 0)
  ) |>
  left_join(
    controls_z |>
      select(-borough_code, -borough_name),
    by = "borocd",
    relationship = "many-to-one"
  ) |>
  pivot_longer(
    cols = c(units_built_1_4, units_built_5_plus),
    names_to = "outcome_id",
    values_to = "outcome_value"
  ) |>
  left_join(outcome_defs, by = "outcome_id", relationship = "many-to-one") |>
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
    borough_period = interaction(borough_name, event_period, drop = TRUE),
    outcome_rate = if_else(occupied_units_1990_exact > 0, 10000 * outcome_value / occupied_units_1990_exact, NA_real_)
  ) |>
  filter(!is.na(event_period))

assert_unique_keys(analysis_panel, c("borocd", "year", "outcome_id"), "PLUTO-only event-study panel")

pre_rate_lookup <- analysis_panel |>
  filter(year >= 1980, year <= 1988) |>
  group_by(outcome_id, borocd) |>
  summarize(pre_1980_1988_rate = mean(outcome_rate, na.rm = TRUE), .groups = "drop") |>
  group_by(outcome_id) |>
  mutate(pre_1980_1988_rate_z = z_score(pre_1980_1988_rate)) |>
  ungroup()

analysis_panel <- analysis_panel |>
  left_join(
    pre_rate_lookup |> select(outcome_id, borocd, pre_1980_1988_rate_z),
    by = c("outcome_id", "borocd"),
    relationship = "many-to-one"
  )

control_vars <- c(
  "log_occupied_units_1990_exact_z",
  "median_household_income_1990_1999_dollars_exact_z",
  "poverty_share_1990_exact_z",
  "pre_1980_1988_rate_z"
)

event_rows <- list()
event_index <- 1L

for (outcome_id_value in outcome_defs$outcome_id) {
  outcome_df <- analysis_panel |>
    filter(outcome_id == outcome_id_value, !is.na(outcome_rate))

  work_df <- add_period_terms(outcome_df, c("treat_z_boro", control_vars), estimated_event_periods_5yr)
  treatment_terms <- paste0("treat_z_boro_x_", sanitize_period(estimated_event_periods_5yr))
  control_terms <- unlist(lapply(control_vars, function(control_var) paste0(control_var, "_x_", sanitize_period(estimated_event_periods_5yr))))

  model <- feols(
    as.formula(paste0("outcome_rate ~ ", paste(c(treatment_terms, control_terms), collapse = " + "), " | borocd + borough_period")),
    data = work_df,
    cluster = ~borocd
  )

  event_rows[[event_index]] <- bind_rows(
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
    extract_model_terms(
      model,
      tibble(term = treatment_terms, event_period = estimated_event_periods_5yr, is_reference = FALSE)
    )
  ) |>
    mutate(
      data_source = "MapPLUTO-only yearbuilt proxy",
      period_scheme = "five_year_bins",
      outcome_id = first(work_df$outcome_id),
      outcome_label = first(work_df$outcome_label),
      outcome_scale = "per_10000_occupied_1990",
      outcome_scale_label = "Per 10,000 occupied units",
      control_layer = "1_light_controls",
      control_layer_label = "Income + poverty + log occ + pre-prod",
      reference_event_period = reference_event_period_5yr,
      observation_count = nobs(model),
      district_count = n_distinct(work_df$borocd),
      year_count = n_distinct(work_df$year)
    ) |>
    select(
      data_source,
      period_scheme,
      outcome_id,
      outcome_label,
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
      observation_count,
      district_count,
      year_count
    )

  event_index <- event_index + 1L
}

event_coefficients_df <- bind_rows(event_rows) |>
  mutate(
    event_period = factor(event_period, levels = event_periods_5yr),
    event_period_index = match(as.character(event_period), event_periods_5yr)
  ) |>
  arrange(outcome_id, event_period)

write_csv_if_changed(event_coefficients_df, "../output/cd_homeownership_long_units_event_coefficients_pluto_full_5yr_bins.csv")

plot_df <- event_coefficients_df |>
  mutate(outcome_label = factor(outcome_label, levels = c("1-4 unit buildings", "5+ unit buildings")))

pdf("../output/cd_homeownership_long_units_event_coefficients_pluto_full_5yr_bins.pdf", width = 11, height = 8.5)
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
    scale_x_continuous(breaks = seq_along(event_periods_5yr), labels = event_periods_5yr) +
    labs(
      title = "PLUTO-only four-control event study, five-year bins: 1-4 vs 5+ unit buildings",
      x = NULL,
      y = "Coefficient on homeowner exposure",
      color = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

missing_event_terms <- event_coefficients_df |> filter(!is_reference, is.na(estimate)) |> nrow()
missing_treatment_count <- sum(is.na(analysis_panel$treat_z_boro))
missing_control_count <- analysis_panel |>
  select(all_of(control_vars)) |>
  is.na() |>
  sum()
negative_outcome_count <- sum(analysis_panel$outcome_value < 0, na.rm = TRUE)
output_nonempty_count <- sum(file.exists(c(
  "../output/cd_homeownership_long_units_event_coefficients_pluto_full_5yr_bins.csv",
  "../output/cd_homeownership_long_units_event_coefficients_pluto_full_5yr_bins.pdf"
)) & file.info(c(
  "../output/cd_homeownership_long_units_event_coefficients_pluto_full_5yr_bins.csv",
  "../output/cd_homeownership_long_units_event_coefficients_pluto_full_5yr_bins.pdf"
))$size > 0)

qc_df <- bind_rows(
  tibble(metric = "district_count", value = as.character(n_distinct(analysis_panel$borocd)), note = "Standard community districts in the PLUTO-only event-study panel."),
  tibble(metric = "year_min", value = as.character(min(analysis_panel$year, na.rm = TRUE)), note = "Minimum year in the PLUTO-only event-study panel."),
  tibble(metric = "year_max", value = as.character(max(analysis_panel$year, na.rm = TRUE)), note = "Maximum year in the PLUTO-only event-study panel."),
  tibble(metric = "event_period_count", value = as.character(n_distinct(analysis_panel$event_period)), note = "Distinct five-year event-study bins represented."),
  tibble(metric = "missing_treatment_count", value = as.character(missing_treatment_count), note = "Rows missing 1990 within-borough homeownership exposure."),
  tibble(metric = "missing_control_count", value = as.character(missing_control_count), note = "Rows missing four-control event-study fields."),
  tibble(metric = "negative_outcome_count", value = as.character(negative_outcome_count), note = "Rows with negative MapPLUTO proxy outcomes."),
  tibble(metric = "missing_event_term_count", value = as.character(missing_event_terms), note = "Requested five-year treatment terms missing from output."),
  tibble(metric = "output_nonempty_count", value = as.character(output_nonempty_count), note = "Expected PLUTO-only diagnostic outputs that exist and are nonempty.")
)

status_flag <- n_distinct(analysis_panel$borocd) == 59 &&
  min(analysis_panel$year, na.rm = TRUE) == 1980 &&
  max(analysis_panel$year, na.rm = TRUE) == 2025 &&
  n_distinct(analysis_panel$event_period) == length(event_periods_5yr) &&
  missing_treatment_count == 0 &&
  missing_control_count == 0 &&
  negative_outcome_count == 0 &&
  missing_event_terms == 0 &&
  output_nonempty_count == 2

qc_df <- bind_rows(
  qc_df,
  tibble(metric = "status", value = as.character(as.integer(status_flag)), note = "One means the PLUTO-only event-study diagnostic passed all QC checks.")
)

write_csv_if_changed(qc_df, "../output/cd_homeownership_long_units_event_pluto_full_5yr_qc.csv")

if (!status_flag) {
  stop("PLUTO-only event-study diagnostic QC failed; see ../output/cd_homeownership_long_units_event_pluto_full_5yr_qc.csv")
}

cat("Wrote PLUTO-only event-study diagnostic outputs to ../output\n")
