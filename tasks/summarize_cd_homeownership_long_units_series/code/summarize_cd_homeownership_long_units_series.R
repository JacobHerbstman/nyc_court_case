# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_cd_homeownership_long_units_series/code")
# cd_homeownership_long_units_series_csv <- "../input/cd_homeownership_long_units_series.csv"
# out_city_year_csv <- "../output/cd_homeownership_long_units_city_year.csv"
# out_tercile_year_csv <- "../output/cd_homeownership_long_units_tercile_year.csv"
# out_tercile_era_csv <- "../output/cd_homeownership_long_units_tercile_era.csv"
# out_qc_csv <- "../output/cd_homeownership_long_units_summary_qc.csv"
# out_plots_pdf <- "../output/cd_homeownership_long_units_plots.pdf"
# out_tercile_year_ma3_csv <- "../output/cd_homeownership_long_units_tercile_year_ma3.csv"
# out_tercile_year_ma5_csv <- "../output/cd_homeownership_long_units_tercile_year_ma5.csv"
# out_tercile_plot_ma3_pdf <- "../output/cd_homeownership_long_units_tercile_plot_ma3.pdf"
# out_tercile_plot_ma5_pdf <- "../output/cd_homeownership_long_units_tercile_plot_ma5.pdf"
# out_project_count_plot_pdf <- "../output/cd_homeownership_long_units_projects_50_plus_counts.pdf"
# out_project_count_plot_ma3_pdf <- "../output/cd_homeownership_long_units_projects_50_plus_counts_ma3.pdf"
# out_project_count_plot_ma5_pdf <- "../output/cd_homeownership_long_units_projects_50_plus_counts_ma5.pdf"
# out_total_unit_count_plot_pdf <- "../output/cd_homeownership_long_units_total_unit_counts.pdf"
# out_total_unit_count_plot_ma3_pdf <- "../output/cd_homeownership_long_units_total_unit_counts_ma3.pdf"
# out_total_unit_count_plot_ma5_pdf <- "../output/cd_homeownership_long_units_total_unit_counts_ma5.pdf"

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tidyr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 16) {
  stop("Expected 16 arguments: cd_homeownership_long_units_series_csv out_city_year_csv out_tercile_year_csv out_tercile_era_csv out_qc_csv out_plots_pdf out_tercile_year_ma3_csv out_tercile_year_ma5_csv out_tercile_plot_ma3_pdf out_tercile_plot_ma5_pdf out_project_count_plot_pdf out_project_count_plot_ma3_pdf out_project_count_plot_ma5_pdf out_total_unit_count_plot_pdf out_total_unit_count_plot_ma3_pdf out_total_unit_count_plot_ma5_pdf")
}

cd_homeownership_long_units_series_csv <- args[1]
out_city_year_csv <- args[2]
out_tercile_year_csv <- args[3]
out_tercile_era_csv <- args[4]
out_qc_csv <- args[5]
out_plots_pdf <- args[6]
out_tercile_year_ma3_csv <- args[7]
out_tercile_year_ma5_csv <- args[8]
out_tercile_plot_ma3_pdf <- args[9]
out_tercile_plot_ma5_pdf <- args[10]
out_project_count_plot_pdf <- args[11]
out_project_count_plot_ma3_pdf <- args[12]
out_project_count_plot_ma5_pdf <- args[13]
out_total_unit_count_plot_pdf <- args[14]
out_total_unit_count_plot_ma3_pdf <- args[15]
out_total_unit_count_plot_ma5_pdf <- args[16]

assert_unique_keys <- function(df, keys, label) {
  duplicate_keys <- df |>
    count(across(all_of(keys)), name = "n") |>
    filter(n > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(label, " is not unique by ", paste(keys, collapse = ", "), ".")
  }
}

series_df <- read_csv(cd_homeownership_long_units_series_csv, show_col_types = FALSE, na = c("", "NA"))

district_lookup <- series_df |>
  distinct(borocd, borough_code, borough_name, treat_pp) |>
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

assert_unique_keys(district_lookup, "borocd", "long-units district lookup")

if (n_distinct(district_lookup$borocd) != 59) {
  stop("Expected the long-units district lookup to cover 59 community districts.")
}

city_year_df <- series_df |>
  group_by(series_kind, source_family, source_label, series_family, series_label, year) |>
  summarize(
    city_outcome_total = sum(outcome_value, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(series_kind, series_family, year)

preferred_df <- series_df |>
  filter(series_kind == "preferred_long_series") |>
  left_join(
    district_lookup |>
      select(borocd, treat_tercile, treat_tercile_label),
    by = "borocd",
    relationship = "many-to-one"
  ) |>
  mutate(
    era = case_when(
      year >= 1980 & year <= 1984 ~ "1980-1984",
      year >= 1985 & year <= 1989 ~ "1985-1989",
      year >= 1990 & year <= 1999 ~ "1990-1999",
      year >= 2000 & year <= 2009 ~ "2000-2009",
      year >= 2010 & year <= 2019 ~ "2010-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    )
  )

tercile_year_df <- preferred_df |>
  group_by(series_family, series_label, year, borough_code, borough_name, treat_tercile, treat_tercile_label) |>
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = first(borough_outcome_total),
    .groups = "drop"
  ) |>
  group_by(series_family, series_label, year, treat_tercile, treat_tercile_label) |>
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = sum(borough_outcome_total, na.rm = TRUE),
    borough_outcome_share = if_else(borough_outcome_total > 0, outcome_value / borough_outcome_total, NA_real_),
    .groups = "drop"
  ) |>
  arrange(series_family, year, treat_tercile)

required_tercile_series <- c("units_built_total", "units_built_50_plus", "projects_built_50_plus")
missing_tercile_series <- setdiff(required_tercile_series, unique(tercile_year_df$series_family))

if (length(missing_tercile_series) > 0) {
  stop("Missing required long-units tercile series: ", paste(missing_tercile_series, collapse = ", "))
}

required_tercile_series_gaps <- tercile_year_df |>
  filter(series_family %in% required_tercile_series) |>
  count(series_family, year, name = "tercile_count") |>
  filter(tercile_count != 3)

if (nrow(required_tercile_series_gaps) > 0) {
  stop("Required long-units tercile series do not have exactly three terciles in every year.")
}

make_centered_moving_average <- function(df, window_years) {
  if (window_years %% 2 != 1) {
    stop("Moving-average windows must have an odd number of years.")
  }

  half_window <- (window_years - 1) / 2

  df |>
    mutate(source_period = if_else(year < 2010, "pre_2010_proxy", "post_2010_observed")) |>
    group_by(series_family, series_label, treat_tercile, treat_tercile_label, source_period) |>
    arrange(year, .by_group = TRUE) |>
    mutate(
      smoothing_window_years = window_years,
      smoothing_alignment = "centered",
      smoothing_window_start_year = year - half_window,
      smoothing_window_end_year = year + half_window,
      smoothing_window_count = vapply(
        year,
        function(center_year) {
          sum(
            year >= center_year - half_window &
              year <= center_year + half_window &
              !is.na(outcome_value)
          )
        },
        integer(1)
      ),
      full_smoothing_window = smoothing_window_count == window_years,
      outcome_value_ma = vapply(
        year,
        function(center_year) {
          in_window <- year >= center_year - half_window &
            year <= center_year + half_window

          if (sum(in_window) != window_years || any(is.na(outcome_value[in_window]))) {
            NA_real_
          } else {
            mean(outcome_value[in_window])
          }
        },
        numeric(1)
      ),
      borough_outcome_share_ma = vapply(
        year,
        function(center_year) {
          in_window <- year >= center_year - half_window &
            year <= center_year + half_window

          if (sum(in_window) != window_years || any(is.na(borough_outcome_share[in_window]))) {
            NA_real_
          } else {
            mean(borough_outcome_share[in_window])
          }
        },
        numeric(1)
      )
    ) |>
    ungroup() |>
    filter(full_smoothing_window) |>
    select(
      series_family,
      series_label,
      year,
      treat_tercile,
      treat_tercile_label,
      source_period,
      smoothing_window_years,
      smoothing_alignment,
      smoothing_window_start_year,
      smoothing_window_end_year,
      smoothing_window_count,
      outcome_value,
      outcome_value_ma,
      borough_outcome_total,
      borough_outcome_share,
      borough_outcome_share_ma
    ) |>
    arrange(series_family, year, treat_tercile)
}

tercile_year_ma3_df <- make_centered_moving_average(tercile_year_df, 3)
tercile_year_ma5_df <- make_centered_moving_average(tercile_year_df, 5)

tercile_era_df <- preferred_df |>
  filter(!is.na(era)) |>
  group_by(series_family, series_label, era, borough_code, borough_name, treat_tercile, treat_tercile_label) |>
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = sum(distinct(data.frame(year, borough_outcome_total))$borough_outcome_total, na.rm = TRUE),
    .groups = "drop"
  ) |>
  group_by(series_family, series_label, era, treat_tercile, treat_tercile_label) |>
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = sum(borough_outcome_total, na.rm = TRUE),
    borough_outcome_share = if_else(borough_outcome_total > 0, outcome_value / borough_outcome_total, NA_real_),
    .groups = "drop"
  ) |>
  arrange(series_family, era, treat_tercile)

write_csv(city_year_df, out_city_year_csv, na = "")
write_csv(tercile_year_df, out_tercile_year_csv, na = "")
write_csv(tercile_year_ma3_df, out_tercile_year_ma3_csv, na = "")
write_csv(tercile_year_ma5_df, out_tercile_year_ma5_csv, na = "")
write_csv(tercile_era_df, out_tercile_era_csv, na = "")

write_csv(
  bind_rows(
    tibble(metric = "district_count", value = n_distinct(district_lookup$borocd), note = "Community districts assigned to treatment terciles."),
    tibble(metric = "preferred_series_year_gap_count", value = nrow(preferred_df |>
      count(series_family, year, name = "cd_count") |>
      filter(cd_count != 59)), note = "Preferred series-family-year cells not covering all 59 CDs."),
    tibble(metric = "city_year_row_count", value = nrow(city_year_df), note = "Rows in the city-year long units summary."),
    tibble(metric = "tercile_year_row_count", value = nrow(tercile_year_df), note = "Rows in the annual tercile summary for the preferred series."),
    tibble(metric = "tercile_year_ma3_row_count", value = nrow(tercile_year_ma3_df), note = "Rows in the 3-year centered moving-average tercile summary."),
    tibble(metric = "tercile_year_ma5_row_count", value = nrow(tercile_year_ma5_df), note = "Rows in the 5-year centered moving-average tercile summary."),
    tibble(metric = "total_units_tercile_year_row_count", value = nrow(tercile_year_df |> filter(series_family == "units_built_total")), note = "Rows in the annual total-unit-count tercile summary."),
    tibble(metric = "project_50_plus_tercile_year_row_count", value = nrow(tercile_year_df |> filter(series_family == "projects_built_50_plus")), note = "Rows in the annual 50+ unit project-count tercile summary."),
    tibble(metric = "required_tercile_series_gap_count", value = nrow(required_tercile_series_gaps), note = "Required annual series-family-year cells with other than three treatment terciles."),
    tibble(metric = "tercile_era_row_count", value = nrow(tercile_era_df), note = "Rows in the era tercile summary for the preferred series.")
  ),
  out_qc_csv,
  na = ""
)

city_plot_df <- city_year_df |>
  filter(series_family %in% c("units_built_total", "gross_add_units_observed")) |>
  mutate(
    series_label = factor(series_label, levels = c("Units built: total", "Gross additions observed"))
  )

tercile_plot_df <- tercile_year_df |>
  filter(series_family %in% c("units_built_total", "units_built_50_plus")) |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = factor(series_label, levels = c("Units built: total", "Units built: 50+"))
  )

make_smoothed_tercile_plot <- function(plot_df, y_label) {
  ggplot(plot_df, aes(x = year, y = borough_outcome_share_ma, color = treat_tercile_label, group = interaction(treat_tercile_label, source_period))) +
    geom_line(linewidth = 0.9) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    facet_wrap(~series_label, scales = "free_y", ncol = 1) +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = y_label, color = "Treat tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
}

make_count_plot <- function(plot_df, y_column, y_label, split_source_period = FALSE) {
  plot_mapping <- if (split_source_period) {
    aes(x = year, y = .data[[y_column]], color = treat_tercile_label, group = interaction(treat_tercile_label, source_period))
  } else {
    aes(x = year, y = .data[[y_column]], color = treat_tercile_label, group = treat_tercile_label)
  }

  ggplot(plot_df, plot_mapping) +
    geom_line(linewidth = 0.9) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    facet_wrap(~series_label, scales = "free_y", ncol = 1) +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = y_label, color = "Treat tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
}

pdf(out_plots_pdf, width = 11, height = 8.5)
print(
  ggplot(city_plot_df, aes(x = year, y = city_outcome_total, color = series_label)) +
    geom_line(linewidth = 0.9) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    scale_color_manual(values = c("Units built: total" = "#1b6ca8", "Gross additions observed" = "#d65f0e")) +
    labs(x = NULL, y = "City total units", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(tercile_plot_df, aes(x = year, y = borough_outcome_share, color = treat_tercile_label)) +
    geom_line(linewidth = 0.8) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    facet_wrap(~series_label, scales = "free_y", ncol = 1) +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = "Within-borough share", color = "Treat tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
dev.off()

tercile_plot_ma3_df <- tercile_year_ma3_df |>
  filter(series_family %in% c("units_built_total", "units_built_50_plus")) |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = factor(series_label, levels = c("Units built: total", "Units built: 50+"))
  )

tercile_plot_ma5_df <- tercile_year_ma5_df |>
  filter(series_family %in% c("units_built_total", "units_built_50_plus")) |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = factor(series_label, levels = c("Units built: total", "Units built: 50+"))
  )

pdf(out_tercile_plot_ma3_pdf, width = 11, height = 8.5)
print(make_smoothed_tercile_plot(tercile_plot_ma3_df, "Within-borough share (3-year centered MA)"))
dev.off()

pdf(out_tercile_plot_ma5_pdf, width = 11, height = 8.5)
print(make_smoothed_tercile_plot(tercile_plot_ma5_df, "Within-borough share (5-year centered MA)"))
dev.off()

total_unit_count_plot_df <- tercile_year_df |>
  filter(series_family == "units_built_total") |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = factor(series_label, levels = "Units built: total")
  )

total_unit_count_plot_ma3_df <- tercile_year_ma3_df |>
  filter(series_family == "units_built_total") |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = factor(series_label, levels = "Units built: total")
  )

total_unit_count_plot_ma5_df <- tercile_year_ma5_df |>
  filter(series_family == "units_built_total") |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = factor(series_label, levels = "Units built: total")
  )

pdf(out_total_unit_count_plot_pdf, width = 11, height = 8.5)
print(make_count_plot(total_unit_count_plot_df, "outcome_value", "Total units built"))
dev.off()

pdf(out_total_unit_count_plot_ma3_pdf, width = 11, height = 8.5)
print(make_count_plot(total_unit_count_plot_ma3_df, "outcome_value_ma", "Total units built (3-year centered MA)", TRUE))
dev.off()

pdf(out_total_unit_count_plot_ma5_pdf, width = 11, height = 8.5)
print(make_count_plot(total_unit_count_plot_ma5_df, "outcome_value_ma", "Total units built (5-year centered MA)", TRUE))
dev.off()

project_count_plot_df <- tercile_year_df |>
  filter(series_family == "projects_built_50_plus") |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = factor(series_label, levels = "Projects built: 50+")
  )

project_count_plot_ma3_df <- tercile_year_ma3_df |>
  filter(series_family == "projects_built_50_plus") |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = factor(series_label, levels = "Projects built: 50+")
  )

project_count_plot_ma5_df <- tercile_year_ma5_df |>
  filter(series_family == "projects_built_50_plus") |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = factor(series_label, levels = "Projects built: 50+")
  )

pdf(out_project_count_plot_pdf, width = 11, height = 8.5)
print(make_count_plot(project_count_plot_df, "outcome_value", "50+ unit project count"))
dev.off()

pdf(out_project_count_plot_ma3_pdf, width = 11, height = 8.5)
print(make_count_plot(project_count_plot_ma3_df, "outcome_value_ma", "50+ unit project count (3-year centered MA)", TRUE))
dev.off()

pdf(out_project_count_plot_ma5_pdf, width = 11, height = 8.5)
print(make_count_plot(project_count_plot_ma5_df, "outcome_value_ma", "50+ unit project count (5-year centered MA)", TRUE))
dev.off()

cat("Wrote long units summaries to", dirname(out_city_year_csv), "\n")
