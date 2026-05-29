# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/diagnose_ccd2010_homeownership_size_shift/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

assert_unique_keys <- function(df, keys, label) {
  duplicate_keys <- df |>
    count(across(all_of(keys)), name = "n") |>
    filter(n > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(label, " is not unique by ", paste(keys, collapse = ", "), ".")
  }
}

safe_divide <- function(numerator, denominator) {
  out_length <- max(length(numerator), length(denominator))
  numerator <- rep(numerator, length.out = out_length)
  denominator <- rep(denominator, length.out = out_length)
  out <- numerator / denominator
  out[is.na(denominator) | denominator == 0] <- NA_real_
  out
}

make_centered_moving_average <- function(df, window_years) {
  half_window <- (window_years - 1) / 2

  df |>
    group_by(series_family, series_label, measure_type, treat_tercile, treat_tercile_label) |>
    arrange(year, .by_group = TRUE) |>
    mutate(
      smoothing_window_years = window_years,
      smoothing_alignment = "centered",
      smoothing_window_start_year = year - half_window,
      smoothing_window_end_year = year + half_window,
      smoothing_window_count = vapply(
        year,
        function(center_year) {
          sum(year >= center_year - half_window & year <= center_year + half_window & !is.na(outcome_value))
        },
        integer(1)
      ),
      full_smoothing_window = smoothing_window_count == window_years,
      outcome_value_ma3 = vapply(
        year,
        function(center_year) {
          in_window <- year >= center_year - half_window & year <= center_year + half_window

          if (sum(in_window) != window_years || any(is.na(outcome_value[in_window]))) {
            NA_real_
          } else {
            mean(outcome_value[in_window])
          }
        },
        numeric(1)
      ),
      borough_outcome_share_ma3 = vapply(
        year,
        function(center_year) {
          in_window <- year >= center_year - half_window & year <= center_year + half_window

          if (sum(in_window) != window_years || any(is.na(borough_outcome_share[in_window]))) {
            NA_real_
          } else {
            mean(borough_outcome_share[in_window])
          }
        },
        numeric(1)
      )
    ) |>
    ungroup()
}

classify_size_shift <- function(units_1_4_change, units_5_49_change, units_50_plus_change, share_1_4_change, share_5_49_change, share_50_plus_change) {
  case_when(
    units_50_plus_change < 0 & (units_1_4_change > 0 | units_5_49_change > 0) ~ "true_substitution",
    units_50_plus_change < 0 &
      units_1_4_change < 0 &
      units_5_49_change < 0 &
      share_50_plus_change < 0 &
      (share_1_4_change > 0 | share_5_49_change > 0) ~ "composition_shift_only",
    units_50_plus_change < 0 & units_1_4_change < 0 & units_5_49_change < 0 ~ "general_downturn",
    TRUE ~ "other"
  )
}

period_levels <- c("1985-1989", "1990-1994", "1995-1999", "2000-2004", "2005-2009")

district_lookup <- read_csv("../input/ccdist2010_homeownership_1990_measure.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    council_district = suppressWarnings(as.integer(council_district)),
    borough_code = as.character(borough_code),
    borough_name = borough_name,
    treat_pp = suppressWarnings(as.numeric(treat_pp)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro)),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990)),
    vacancy_rate_1990 = suppressWarnings(as.numeric(vacancy_rate_1990)),
    median_household_income_1990 = suppressWarnings(as.numeric(median_household_income_1990))
  ) |>
  distinct() |>
  group_by(borough_code, borough_name) |>
  mutate(
    treat_tercile = ntile(treat_pp, 3),
    treat_tercile_label = case_when(
      treat_tercile == 1 ~ "Low",
      treat_tercile == 2 ~ "Middle",
      TRUE ~ "High"
    )
  ) |>
  ungroup()

assert_unique_keys(district_lookup, "district_id", "2010 Council district treatment lookup")

if (n_distinct(district_lookup$district_id) != 51) {
  stop("Expected exactly 51 2010 Council districts.")
}

proxy_values <- read_csv("../input/ccdist2010_mappluto_construction_proxy_district_year.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    council_district = suppressWarnings(as.integer(council_district)),
    borough_code = as.character(borough_code),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(yearbuilt)),
    units_built_total = coalesce(suppressWarnings(as.numeric(residential_units_proxy)), 0),
    units_built_1_4 = coalesce(suppressWarnings(as.numeric(units_1_4_proxy)), 0),
    units_built_5_plus = coalesce(suppressWarnings(as.numeric(units_5_plus_proxy)), 0),
    units_built_50_plus = coalesce(suppressWarnings(as.numeric(units_50_plus_proxy)), 0),
    units_built_5_49_direct = coalesce(suppressWarnings(as.numeric(units_5_9_proxy)), 0) + coalesce(suppressWarnings(as.numeric(units_10_49_proxy)), 0),
    projects_built_50_plus = coalesce(suppressWarnings(as.numeric(lots_50_plus_proxy)), 0)
  ) |>
  filter(year >= 1980, year <= 2025) |>
  mutate(
    units_built_5_49 = units_built_5_plus - units_built_50_plus,
    units_size_sum_gap = units_built_total - units_built_1_4 - units_built_5_49 - units_built_50_plus,
    units_built_5_49_direct_gap = units_built_5_49 - units_built_5_49_direct
  )

assert_unique_keys(proxy_values, c("district_id", "year"), "2010 Council district MapPLUTO proxy values")

district_year_df <- expand_grid(
  district_lookup,
  year = 1980:2025
) |>
  left_join(
    proxy_values |>
      select(
        district_id,
        council_district,
        borough_code,
        borough_name,
        year,
        units_built_total,
        units_built_1_4,
        units_built_5_plus,
        units_built_5_49,
        units_built_50_plus,
        projects_built_50_plus,
        units_size_sum_gap,
        units_built_5_49_direct_gap
      ),
    by = c("district_id", "council_district", "borough_code", "borough_name", "year"),
    relationship = "one-to-one"
  ) |>
  mutate(
    across(
      c(units_built_total, units_built_1_4, units_built_5_plus, units_built_5_49, units_built_50_plus, projects_built_50_plus, units_size_sum_gap, units_built_5_49_direct_gap),
      ~ coalesce(.x, 0)
    )
  )

size_map <- tribble(
  ~series_family, ~series_label, ~measure_type, ~sort_order,
  "units_built_total", "Total units", "units", 1L,
  "units_built_1_4", "1-4 unit building units", "units", 2L,
  "units_built_5_49", "5-49 unit building units", "units", 3L,
  "units_built_50_plus", "50+ unit building units", "units", 4L,
  "projects_built_50_plus", "50+ unit building projects", "projects", 5L
)

district_year_long_df <- district_year_df |>
  select(
    district_id,
    council_district,
    borough_code,
    borough_name,
    year,
    treat_tercile,
    treat_tercile_label,
    treat_pp,
    treat_z_boro,
    occupied_units_1990,
    units_built_total,
    units_built_1_4,
    units_built_5_49,
    units_built_50_plus,
    projects_built_50_plus
  ) |>
  pivot_longer(
    cols = c(units_built_total, units_built_1_4, units_built_5_49, units_built_50_plus, projects_built_50_plus),
    names_to = "series_family",
    values_to = "outcome_value"
  ) |>
  left_join(size_map, by = "series_family", relationship = "many-to-one")

tercile_borough_year_df <- district_year_long_df |>
  group_by(series_family, series_label, measure_type, sort_order, year, borough_code, borough_name, treat_tercile, treat_tercile_label) |>
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    district_count = n_distinct(district_id),
    .groups = "drop"
  ) |>
  group_by(series_family, series_label, measure_type, sort_order, year, borough_code, borough_name) |>
  mutate(
    borough_outcome_total = sum(outcome_value, na.rm = TRUE),
    borough_outcome_share = safe_divide(outcome_value, borough_outcome_total)
  ) |>
  ungroup()

annual_df <- tercile_borough_year_df |>
  group_by(series_family, series_label, measure_type, sort_order, year, treat_tercile, treat_tercile_label) |>
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = sum(distinct(data.frame(borough_code, borough_name, borough_outcome_total))$borough_outcome_total, na.rm = TRUE),
    borough_outcome_share = safe_divide(outcome_value, borough_outcome_total),
    district_count = sum(district_count),
    .groups = "drop"
  ) |>
  arrange(sort_order, year, treat_tercile) |>
  make_centered_moving_average(3) |>
  mutate(
    period = case_when(
      year >= 1985 & year <= 1989 ~ "1985-1989",
      year >= 1990 & year <= 1994 ~ "1990-1994",
      year >= 1995 & year <= 1999 ~ "1995-1999",
      year >= 2000 & year <= 2004 ~ "2000-2004",
      year >= 2005 & year <= 2009 ~ "2005-2009",
      TRUE ~ NA_character_
    ),
    period = factor(period, levels = period_levels)
  ) |>
  select(
    series_family,
    series_label,
    measure_type,
    sort_order,
    year,
    period,
    treat_tercile,
    treat_tercile_label,
    district_count,
    outcome_value,
    outcome_value_ma3,
    borough_outcome_total,
    borough_outcome_share,
    borough_outcome_share_ma3,
    smoothing_window_years,
    smoothing_alignment,
    smoothing_window_start_year,
    smoothing_window_end_year,
    smoothing_window_count,
    full_smoothing_window
  )

period_counts_df <- annual_df |>
  filter(!is.na(period)) |>
  group_by(series_family, series_label, measure_type, sort_order, period, treat_tercile, treat_tercile_label) |>
  summarize(
    year_count = n_distinct(year),
    period_total_count = sum(outcome_value, na.rm = TRUE),
    annual_avg_count = period_total_count / year_count,
    .groups = "drop"
  )

period_counts_df <- period_counts_df |>
  left_join(
    period_counts_df |>
      filter(period == "1985-1989") |>
      select(series_family, treat_tercile, baseline_annual_avg_count = annual_avg_count),
    by = c("series_family", "treat_tercile"),
    relationship = "many-to-one"
  ) |>
  mutate(
    change_from_1985_1989 = annual_avg_count - baseline_annual_avg_count,
    percent_change_from_1985_1989 = 100 * safe_divide(change_from_1985_1989, baseline_annual_avg_count)
  ) |>
  arrange(sort_order, period, treat_tercile)

relative_declines_df <- period_counts_df |>
  select(
    series_family,
    series_label,
    measure_type,
    sort_order,
    period,
    treat_tercile_label,
    annual_avg_count,
    baseline_annual_avg_count,
    change_from_1985_1989,
    percent_change_from_1985_1989
  ) |>
  pivot_wider(
    names_from = treat_tercile_label,
    values_from = c(
      annual_avg_count,
      baseline_annual_avg_count,
      change_from_1985_1989,
      percent_change_from_1985_1989
    )
  ) |>
  mutate(
    high_percent_change_gap_vs_low = percent_change_from_1985_1989_High - percent_change_from_1985_1989_Low,
    high_percent_change_gap_vs_middle = percent_change_from_1985_1989_High - percent_change_from_1985_1989_Middle,
    high_raw_change_gap_vs_low = change_from_1985_1989_High - change_from_1985_1989_Low,
    high_raw_change_gap_vs_middle = change_from_1985_1989_High - change_from_1985_1989_Middle,
    high_fell_more_than_low = percent_change_from_1985_1989_High < percent_change_from_1985_1989_Low,
    high_fell_more_than_middle = percent_change_from_1985_1989_High < percent_change_from_1985_1989_Middle
  ) |>
  arrange(sort_order, period)

period_shares_df <- annual_df |>
  filter(!is.na(period)) |>
  group_by(series_family, series_label, measure_type, sort_order, period, treat_tercile, treat_tercile_label) |>
  summarize(
    year_count = n_distinct(year),
    period_total_count = sum(outcome_value, na.rm = TRUE),
    period_borough_total = sum(borough_outcome_total, na.rm = TRUE),
    period_borough_share = safe_divide(period_total_count, period_borough_total),
    .groups = "drop"
  )

period_shares_df <- period_shares_df |>
  left_join(
    period_shares_df |>
      filter(period == "1985-1989") |>
      select(series_family, treat_tercile, baseline_period_borough_share = period_borough_share),
    by = c("series_family", "treat_tercile"),
    relationship = "many-to-one"
  ) |>
  mutate(
    share_change_from_1985_1989 = period_borough_share - baseline_period_borough_share,
    share_pp_change_from_1985_1989 = 100 * share_change_from_1985_1989
  ) |>
  arrange(sort_order, period, treat_tercile)

high_units_wide <- period_counts_df |>
  filter(
    treat_tercile_label == "High",
    series_family %in% c("units_built_total", "units_built_1_4", "units_built_5_49", "units_built_50_plus")
  ) |>
  select(period, series_family, annual_avg_count) |>
  pivot_wider(names_from = series_family, values_from = annual_avg_count) |>
  arrange(period)

high_share_wide <- high_units_wide |>
  transmute(
    period,
    share_1_4_of_high = safe_divide(units_built_1_4, units_built_total),
    share_5_49_of_high = safe_divide(units_built_5_49, units_built_total),
    share_50_plus_of_high = safe_divide(units_built_50_plus, units_built_total)
  )

composition_df <- high_units_wide |>
  left_join(high_share_wide, by = "period", relationship = "one-to-one") |>
  mutate(
    component_total_annual_avg = units_built_1_4 + units_built_5_49 + units_built_50_plus,
    component_total_gap = units_built_total - component_total_annual_avg
  )

baseline_composition <- composition_df |>
  filter(period == "1985-1989")

composition_df <- composition_df |>
  mutate(
    baseline_total_annual_avg = baseline_composition$units_built_total[[1]],
    baseline_1_4_annual_avg = baseline_composition$units_built_1_4[[1]],
    baseline_5_49_annual_avg = baseline_composition$units_built_5_49[[1]],
    baseline_50_plus_annual_avg = baseline_composition$units_built_50_plus[[1]],
    baseline_share_1_4_of_high = baseline_composition$share_1_4_of_high[[1]],
    baseline_share_5_49_of_high = baseline_composition$share_5_49_of_high[[1]],
    baseline_share_50_plus_of_high = baseline_composition$share_50_plus_of_high[[1]],
    percent_change_total_from_1985_1989 = 100 * safe_divide(units_built_total - baseline_total_annual_avg, baseline_total_annual_avg),
    percent_change_1_4_from_1985_1989 = 100 * safe_divide(units_built_1_4 - baseline_1_4_annual_avg, baseline_1_4_annual_avg),
    percent_change_5_49_from_1985_1989 = 100 * safe_divide(units_built_5_49 - baseline_5_49_annual_avg, baseline_5_49_annual_avg),
    percent_change_50_plus_from_1985_1989 = 100 * safe_divide(units_built_50_plus - baseline_50_plus_annual_avg, baseline_50_plus_annual_avg),
    share_1_4_change_from_1985_1989 = share_1_4_of_high - baseline_share_1_4_of_high,
    share_5_49_change_from_1985_1989 = share_5_49_of_high - baseline_share_5_49_of_high,
    share_50_plus_change_from_1985_1989 = share_50_plus_of_high - baseline_share_50_plus_of_high
  )

period_classification_df <- composition_df |>
  mutate(
    units_1_4_change = units_built_1_4 - baseline_1_4_annual_avg,
    units_5_49_change = units_built_5_49 - baseline_5_49_annual_avg,
    units_50_plus_change = units_built_50_plus - baseline_50_plus_annual_avg,
    all_unit_bins_decline = units_1_4_change < 0 & units_5_49_change < 0 & units_50_plus_change < 0,
    size_shift_classification = if_else(
      period == "1985-1989",
      "baseline",
      classify_size_shift(
        units_1_4_change,
        units_5_49_change,
        units_50_plus_change,
        share_1_4_change_from_1985_1989,
        share_5_49_change_from_1985_1989,
        share_50_plus_change_from_1985_1989
      )
    )
  ) |>
  select(period, units_1_4_change, units_5_49_change, units_50_plus_change, all_unit_bins_decline, size_shift_classification)

decomposition_df <- composition_df |>
  filter(period != "1985-1989") |>
  select(
    period,
    baseline_total_annual_avg,
    units_built_total,
    baseline_1_4_annual_avg,
    baseline_5_49_annual_avg,
    baseline_50_plus_annual_avg,
    units_built_1_4,
    units_built_5_49,
    units_built_50_plus,
    share_1_4_change_from_1985_1989,
    share_5_49_change_from_1985_1989,
    share_50_plus_change_from_1985_1989
  ) |>
  mutate(total_change_from_1985_1989 = units_built_total - baseline_total_annual_avg) |>
  pivot_longer(
    cols = c(units_built_1_4, units_built_5_49, units_built_50_plus),
    names_to = "series_family",
    values_to = "annual_avg_count"
  ) |>
  mutate(
    baseline_annual_avg_count = case_when(
      series_family == "units_built_1_4" ~ baseline_1_4_annual_avg,
      series_family == "units_built_5_49" ~ baseline_5_49_annual_avg,
      TRUE ~ baseline_50_plus_annual_avg
    ),
    annual_avg_change_from_1985_1989 = annual_avg_count - baseline_annual_avg_count,
    contribution_to_total_change = safe_divide(annual_avg_change_from_1985_1989, total_change_from_1985_1989),
    share_change_from_1985_1989 = case_when(
      series_family == "units_built_1_4" ~ share_1_4_change_from_1985_1989,
      series_family == "units_built_5_49" ~ share_5_49_change_from_1985_1989,
      TRUE ~ share_50_plus_change_from_1985_1989
    )
  ) |>
  left_join(size_map |> select(series_family, series_label, sort_order), by = "series_family", relationship = "many-to-one") |>
  left_join(period_classification_df, by = "period", relationship = "many-to-one") |>
  select(
    period,
    series_family,
    series_label,
    sort_order,
    baseline_annual_avg_count,
    annual_avg_count,
    annual_avg_change_from_1985_1989,
    total_change_from_1985_1989,
    contribution_to_total_change,
    share_change_from_1985_1989,
    all_unit_bins_decline,
    size_shift_classification
  ) |>
  arrange(period, sort_order)

high_50_borough_period <- tercile_borough_year_df |>
  mutate(
    period = case_when(
      year >= 1985 & year <= 1989 ~ "1985-1989",
      year >= 1990 & year <= 1994 ~ "1990-1994",
      year >= 1995 & year <= 1999 ~ "1995-1999",
      year >= 2000 & year <= 2004 ~ "2000-2004",
      year >= 2005 & year <= 2009 ~ "2005-2009",
      TRUE ~ NA_character_
    )
  ) |>
  filter(series_family == "units_built_50_plus", treat_tercile_label == "High", !is.na(period)) |>
  group_by(period, borough_code, borough_name) |>
  summarize(
    year_count = n_distinct(year),
    outcome_value = sum(outcome_value, na.rm = TRUE),
    denominator = sum(borough_outcome_total, na.rm = TRUE),
    annual_avg_count = outcome_value / year_count,
    within_scope_share = safe_divide(outcome_value, denominator),
    .groups = "drop"
  ) |>
  group_by(period) |>
  mutate(
    share_of_high_tercile_period_total = safe_divide(outcome_value, sum(outcome_value, na.rm = TRUE)),
    rank_within_period = min_rank(desc(outcome_value))
  ) |>
  ungroup() |>
  transmute(
    scenario_type = "borough_specific",
    scenario_name = borough_name,
    period,
    series_family = "units_built_50_plus",
    district_id = NA_character_,
    council_district = NA_integer_,
    borough_code,
    borough_name,
    outcome_value,
    denominator,
    annual_avg_count,
    within_scope_share,
    share_of_high_tercile_period_total,
    rank_within_period
  )

all_boroughs_concentration <- period_shares_df |>
  filter(series_family == "units_built_50_plus", treat_tercile_label == "High") |>
  transmute(
    scenario_type = "all_boroughs",
    scenario_name = "all_boroughs",
    period = as.character(period),
    series_family,
    district_id = NA_character_,
    council_district = NA_integer_,
    borough_code = NA_character_,
    borough_name = NA_character_,
    outcome_value = period_total_count,
    denominator = period_borough_total,
    annual_avg_count = period_total_count / year_count,
    within_scope_share = period_borough_share,
    share_of_high_tercile_period_total = 1,
    rank_within_period = NA_integer_
  )

leave_one_out_concentration <- bind_rows(lapply(
  sort(unique(tercile_borough_year_df$borough_name)),
  function(excluded_borough) {
    tercile_borough_year_df |>
      mutate(
        period = case_when(
          year >= 1985 & year <= 1989 ~ "1985-1989",
          year >= 1990 & year <= 1994 ~ "1990-1994",
          year >= 1995 & year <= 1999 ~ "1995-1999",
          year >= 2000 & year <= 2004 ~ "2000-2004",
          year >= 2005 & year <= 2009 ~ "2005-2009",
          TRUE ~ NA_character_
        )
      ) |>
      filter(series_family == "units_built_50_plus", borough_name != excluded_borough, !is.na(period)) |>
      group_by(period, treat_tercile_label) |>
      summarize(
        year_count = n_distinct(year),
        outcome_value = sum(outcome_value, na.rm = TRUE),
        denominator = sum(distinct(data.frame(year, borough_code, borough_name, borough_outcome_total))$borough_outcome_total, na.rm = TRUE),
        .groups = "drop"
      ) |>
      filter(treat_tercile_label == "High") |>
      transmute(
        scenario_type = "leave_one_borough_out",
        scenario_name = paste0("drop_", excluded_borough),
        period,
        series_family = "units_built_50_plus",
        district_id = NA_character_,
        council_district = NA_integer_,
        borough_code = NA_character_,
        borough_name = NA_character_,
        outcome_value,
        denominator,
        annual_avg_count = outcome_value / year_count,
        within_scope_share = safe_divide(outcome_value, denominator),
        share_of_high_tercile_period_total = NA_real_,
        rank_within_period = NA_integer_
      )
  }
))

district_concentration <- district_year_long_df |>
  mutate(
    period = case_when(
      year >= 1985 & year <= 1989 ~ "1985-1989",
      year >= 1990 & year <= 1994 ~ "1990-1994",
      year >= 1995 & year <= 1999 ~ "1995-1999",
      year >= 2000 & year <= 2004 ~ "2000-2004",
      year >= 2005 & year <= 2009 ~ "2005-2009",
      TRUE ~ NA_character_
    )
  ) |>
  filter(series_family == "units_built_50_plus", treat_tercile_label == "High", !is.na(period)) |>
  group_by(period, district_id, council_district, borough_code, borough_name) |>
  summarize(
    year_count = n_distinct(year),
    outcome_value = sum(outcome_value, na.rm = TRUE),
    annual_avg_count = outcome_value / year_count,
    .groups = "drop"
  ) |>
  group_by(period) |>
  mutate(
    share_of_high_tercile_period_total = safe_divide(outcome_value, sum(outcome_value, na.rm = TRUE)),
    rank_within_period = min_rank(desc(outcome_value))
  ) |>
  ungroup() |>
  transmute(
    scenario_type = "district",
    scenario_name = paste0("Council ", council_district),
    period,
    series_family = "units_built_50_plus",
    district_id,
    council_district,
    borough_code,
    borough_name,
    outcome_value,
    denominator = NA_real_,
    annual_avg_count,
    within_scope_share = NA_real_,
    share_of_high_tercile_period_total,
    rank_within_period
  )

concentration_df <- bind_rows(
  all_boroughs_concentration,
  high_50_borough_period,
  leave_one_out_concentration,
  district_concentration
) |>
  arrange(period, scenario_type, rank_within_period, scenario_name)

cd_raw_df <- read_csv("../input/cd_homeownership_long_units_tercile_year.csv", show_col_types = FALSE, na = c("", "NA")) |>
  filter(series_family %in% c("units_built_total", "units_built_1_4", "units_built_5_plus", "units_built_50_plus")) |>
  select(series_family, year, treat_tercile, treat_tercile_label, outcome_value, borough_outcome_total)

cd_wide_df <- cd_raw_df |>
  pivot_wider(
    names_from = series_family,
    values_from = c(outcome_value, borough_outcome_total)
  ) |>
  mutate(
    outcome_value_units_built_5_49 = outcome_value_units_built_5_plus - outcome_value_units_built_50_plus,
    borough_outcome_total_units_built_5_49 = borough_outcome_total_units_built_5_plus - borough_outcome_total_units_built_50_plus
  )

cd_annual_df <- bind_rows(
  cd_wide_df |>
    transmute(series_family = "units_built_total", year, treat_tercile, treat_tercile_label, outcome_value = outcome_value_units_built_total, borough_outcome_total = borough_outcome_total_units_built_total),
  cd_wide_df |>
    transmute(series_family = "units_built_1_4", year, treat_tercile, treat_tercile_label, outcome_value = outcome_value_units_built_1_4, borough_outcome_total = borough_outcome_total_units_built_1_4),
  cd_wide_df |>
    transmute(series_family = "units_built_5_49", year, treat_tercile, treat_tercile_label, outcome_value = outcome_value_units_built_5_49, borough_outcome_total = borough_outcome_total_units_built_5_49),
  cd_wide_df |>
    transmute(series_family = "units_built_50_plus", year, treat_tercile, treat_tercile_label, outcome_value = outcome_value_units_built_50_plus, borough_outcome_total = borough_outcome_total_units_built_50_plus)
) |>
  mutate(
    borough_outcome_share = safe_divide(outcome_value, borough_outcome_total),
    period = case_when(
      year >= 1985 & year <= 1989 ~ "1985-1989",
      year >= 1990 & year <= 1994 ~ "1990-1994",
      year >= 1995 & year <= 1999 ~ "1995-1999",
      year >= 2000 & year <= 2004 ~ "2000-2004",
      year >= 2005 & year <= 2009 ~ "2005-2009",
      TRUE ~ NA_character_
    )
  ) |>
  left_join(size_map |> select(series_family, series_label, sort_order), by = "series_family", relationship = "many-to-one")

make_high_reconciliation_summary <- function(df, geography_label) {
  high_period <- df |>
    filter(!is.na(period), treat_tercile_label == "High", series_family %in% c("units_built_total", "units_built_1_4", "units_built_5_49", "units_built_50_plus")) |>
    group_by(period, series_family, series_label, sort_order) |>
    summarize(
      year_count = n_distinct(year),
      period_total_count = sum(outcome_value, na.rm = TRUE),
      annual_avg_count = period_total_count / year_count,
      period_borough_total = sum(borough_outcome_total, na.rm = TRUE),
      period_borough_share = safe_divide(period_total_count, period_borough_total),
      .groups = "drop"
    )

  high_total <- high_period |>
    filter(series_family == "units_built_total") |>
    select(period, high_total_annual_avg_count = annual_avg_count)

  high_period |>
    left_join(high_total, by = "period", relationship = "many-to-one") |>
    mutate(
      geography = geography_label,
      high_composition_share = safe_divide(annual_avg_count, high_total_annual_avg_count)
    ) |>
    select(geography, period, series_family, series_label, annual_avg_count, period_borough_share, high_composition_share)
}

ccd_reconciliation_input <- annual_df |>
  filter(series_family %in% c("units_built_total", "units_built_1_4", "units_built_5_49", "units_built_50_plus")) |>
  select(series_family, series_label, sort_order, year, period, treat_tercile, treat_tercile_label, outcome_value, borough_outcome_total, borough_outcome_share)

reconciliation_long <- bind_rows(
  make_high_reconciliation_summary(ccd_reconciliation_input, "2010_council_district"),
  make_high_reconciliation_summary(cd_annual_df, "community_district")
)

reconciliation_wide <- reconciliation_long |>
  pivot_wider(
    names_from = geography,
    values_from = c(annual_avg_count, period_borough_share, high_composition_share)
  ) |>
  mutate(
    annual_avg_count_gap_cd_minus_ccd = annual_avg_count_community_district - annual_avg_count_2010_council_district,
    period_borough_share_gap_cd_minus_ccd = period_borough_share_community_district - period_borough_share_2010_council_district,
    high_composition_share_gap_cd_minus_ccd = high_composition_share_community_district - high_composition_share_2010_council_district
  ) |>
  arrange(period, match(series_family, size_map$series_family))

share_sum_df <- annual_df |>
  group_by(series_family, year) |>
  summarize(
    borough_outcome_total = first(borough_outcome_total),
    share_sum = sum(borough_outcome_share, na.rm = TRUE),
    share_sum_gap = share_sum - 1,
    .groups = "drop"
  ) |>
  filter(borough_outcome_total > 0)

annual_high_composition_df <- annual_df |>
  filter(treat_tercile_label == "High", series_family %in% c("units_built_total", "units_built_1_4", "units_built_5_49", "units_built_50_plus")) |>
  select(year, series_family, outcome_value) |>
  pivot_wider(names_from = series_family, values_from = outcome_value) |>
  transmute(
    year,
    `1-4` = safe_divide(units_built_1_4, units_built_total),
    `5-49` = safe_divide(units_built_5_49, units_built_total),
    `50+` = safe_divide(units_built_50_plus, units_built_total)
  ) |>
  pivot_longer(cols = c(`1-4`, `5-49`, `50+`), names_to = "size_bin", values_to = "composition_share")

plot_count_df <- annual_df |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = factor(series_label, levels = size_map$series_label)
  )

plot_unit_count_df <- plot_count_df |>
  filter(series_family %in% c("units_built_total", "units_built_1_4", "units_built_5_49", "units_built_50_plus"))

decomposition_plot_df <- decomposition_df |>
  mutate(
    series_label = factor(series_label, levels = c("1-4 unit building units", "5-49 unit building units", "50+ unit building units")),
    period = factor(period, levels = period_levels)
  )

pdf("../output/ccdist2010_homeownership_size_shift_plots.pdf", width = 11, height = 8.5)
print(
  ggplot(filter(plot_count_df, series_family %in% c("units_built_total", "units_built_50_plus")), aes(x = year, y = outcome_value, color = treat_tercile_label)) +
    geom_line(linewidth = 0.8) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    facet_wrap(~series_label, scales = "free_y", ncol = 1) +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(title = "Replication: total and 50+ unit count panels", x = NULL, y = "Raw count", color = "Treat tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(plot_unit_count_df, aes(x = year, y = outcome_value, color = treat_tercile_label)) +
    geom_line(linewidth = 0.75) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    facet_wrap(~series_label, scales = "free_y", ncol = 1) +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(title = "Raw units by size bin", x = NULL, y = "Raw units", color = "Treat tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(plot_unit_count_df, aes(x = year, y = outcome_value_ma3, color = treat_tercile_label)) +
    geom_line(linewidth = 0.85) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    facet_wrap(~series_label, scales = "free_y", ncol = 1) +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(title = "3-year centered moving average counts by size bin", x = NULL, y = "Raw units (3-year MA)", color = "Treat tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(filter(plot_count_df, series_family == "projects_built_50_plus"), aes(x = year, y = outcome_value, color = treat_tercile_label)) +
    geom_line(linewidth = 0.8) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(title = "Raw 50+ project counts", x = NULL, y = "50+ projects", color = "Treat tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(plot_unit_count_df, aes(x = year, y = borough_outcome_share, color = treat_tercile_label)) +
    geom_line(linewidth = 0.75) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    facet_wrap(~series_label, scales = "free_y", ncol = 1) +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(title = "Within-borough shares by size bin", x = NULL, y = "Within-borough share", color = "Treat tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(annual_high_composition_df, aes(x = year, y = composition_share, color = size_bin)) +
    geom_line(linewidth = 0.8) +
    scale_color_manual(values = c("1-4" = "#666666", "5-49" = "#1b6ca8", "50+" = "#CC3311")) +
    labs(title = "High-homeowner tercile composition", x = NULL, y = "Share of high-tercile units", color = "Size bin") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(decomposition_plot_df, aes(x = period, y = annual_avg_change_from_1985_1989, fill = series_label)) +
    geom_col() +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666") +
    scale_fill_manual(values = c("1-4 unit building units" = "#999999", "5-49 unit building units" = "#1b6ca8", "50+ unit building units" = "#CC3311")) +
    labs(title = "High-tercile raw-count change decomposition", x = NULL, y = "Annual average unit change vs 1985-1989", fill = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(filter(concentration_df, scenario_type == "borough_specific"), aes(x = period, y = share_of_high_tercile_period_total, fill = borough_name)) +
    geom_col() +
    labs(title = "Borough contribution to high-tercile 50+ units", x = NULL, y = "Share of high-tercile 50+ units", fill = "Borough") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
dev.off()

write_csv_if_changed(annual_df, "../output/ccdist2010_homeownership_size_shift_annual.csv")
write_csv_if_changed(period_counts_df, "../output/ccdist2010_homeownership_size_shift_period_counts.csv")
write_csv_if_changed(relative_declines_df, "../output/ccdist2010_homeownership_size_shift_relative_declines.csv")
write_csv_if_changed(period_shares_df, "../output/ccdist2010_homeownership_size_shift_period_shares.csv")
write_csv_if_changed(composition_df |> left_join(period_classification_df, by = "period", relationship = "one-to-one"), "../output/ccdist2010_homeownership_size_shift_composition.csv")
write_csv_if_changed(decomposition_df, "../output/ccdist2010_homeownership_size_shift_decomposition.csv")
write_csv_if_changed(concentration_df, "../output/ccdist2010_homeownership_size_shift_concentration.csv")
write_csv_if_changed(reconciliation_wide, "../output/ccdist2010_homeownership_size_shift_cd_reconciliation.csv")

write_csv_if_changed(
  bind_rows(
    tibble(metric = "district_count", value = as.character(n_distinct(district_lookup$district_id)), note = "2010 Council districts assigned to homeowner terciles."),
    tibble(metric = "year_min", value = as.character(min(district_year_df$year, na.rm = TRUE)), note = "Minimum year in the diagnostic panel."),
    tibble(metric = "year_max", value = as.character(max(district_year_df$year, na.rm = TRUE)), note = "Maximum year in the diagnostic panel."),
    tibble(metric = "district_year_row_count", value = as.character(nrow(district_year_df)), note = "Balanced Council-district by year rows."),
    tibble(metric = "annual_row_count", value = as.character(nrow(annual_df)), note = "Rows in annual tercile summary across size bins."),
    tibble(metric = "missing_treat_count", value = as.character(sum(is.na(district_lookup$treat_pp))), note = "Districts missing homeowner treatment."),
    tibble(metric = "max_abs_units_size_sum_gap", value = as.character(max(abs(district_year_df$units_size_sum_gap), na.rm = TRUE)), note = "Maximum absolute gap between total units and the sum of 1-4, 5-49, and 50+ units."),
    tibble(metric = "max_abs_units_5_49_direct_gap", value = as.character(max(abs(district_year_df$units_built_5_49_direct_gap), na.rm = TRUE)), note = "Maximum absolute gap between direct 5-49 units and 5+ minus 50+ units."),
    tibble(metric = "annual_positive_share_sum_min", value = as.character(min(share_sum_df$share_sum, na.rm = TRUE)), note = "Minimum annual tercile-share sum among positive-denominator series-year cells."),
    tibble(metric = "annual_positive_share_sum_max", value = as.character(max(share_sum_df$share_sum, na.rm = TRUE)), note = "Maximum annual tercile-share sum among positive-denominator series-year cells."),
    tibble(metric = "period_count_row_count", value = as.character(nrow(period_counts_df)), note = "Rows in period count output."),
    tibble(metric = "relative_decline_row_count", value = as.character(nrow(relative_declines_df)), note = "Rows in cross-tercile relative decline output."),
    tibble(metric = "period_share_row_count", value = as.character(nrow(period_shares_df)), note = "Rows in period share output."),
    tibble(metric = "early_1990s_size_shift_classification", value = period_classification_df$size_shift_classification[period_classification_df$period == "1990-1994"], note = "Raw-count-first classification for 1990-1994 versus 1985-1989."),
    tibble(metric = "status", value = as.character(as.integer(
      n_distinct(district_lookup$district_id) == 51 &&
        nrow(district_year_df) == 51 * length(1980:2025) &&
        max(abs(district_year_df$units_size_sum_gap), na.rm = TRUE) == 0 &&
        max(abs(district_year_df$units_built_5_49_direct_gap), na.rm = TRUE) == 0
    )), note = "One means core panel and size-bin checks passed.")
  ),
  "../output/ccdist2010_homeownership_size_shift_qc.csv"
)

cat("Wrote 2010 Council district homeowner size-shift diagnostics to ../output\n")
