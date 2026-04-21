# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_cd_homeownership_long_units_series/code")
# cd_homeownership_long_units_series_csv <- "../input/cd_homeownership_long_units_series.csv"
# out_city_year_csv <- "../output/cd_homeownership_long_units_city_year.csv"
# out_tercile_year_csv <- "../output/cd_homeownership_long_units_tercile_year.csv"
# out_tercile_era_csv <- "../output/cd_homeownership_long_units_tercile_era.csv"
# out_qc_csv <- "../output/cd_homeownership_long_units_summary_qc.csv"
# out_plots_pdf <- "../output/cd_homeownership_long_units_plots.pdf"

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tidyr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 6) {
  stop("Expected 6 arguments: cd_homeownership_long_units_series_csv out_city_year_csv out_tercile_year_csv out_tercile_era_csv out_qc_csv out_plots_pdf")
}

cd_homeownership_long_units_series_csv <- args[1]
out_city_year_csv <- args[2]
out_tercile_year_csv <- args[3]
out_tercile_era_csv <- args[4]
out_qc_csv <- args[5]
out_plots_pdf <- args[6]

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
    by = "borocd"
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
write_csv(tercile_era_df, out_tercile_era_csv, na = "")

write_csv(
  bind_rows(
    tibble(metric = "district_count", value = n_distinct(district_lookup$borocd), note = "Community districts assigned to treatment terciles."),
    tibble(metric = "city_year_row_count", value = nrow(city_year_df), note = "Rows in the city-year long units summary."),
    tibble(metric = "tercile_year_row_count", value = nrow(tercile_year_df), note = "Rows in the annual tercile summary for the preferred series."),
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

cat("Wrote long units summaries to", dirname(out_city_year_csv), "\n")
