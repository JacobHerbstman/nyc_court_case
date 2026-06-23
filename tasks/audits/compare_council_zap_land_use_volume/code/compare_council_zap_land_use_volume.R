# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/compare_council_zap_land_use_volume/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

theme_set(theme_minimal(base_size = 11))
plot_year_breaks <- seq(1990, 2025, 5)

rolling_average_5 <- function(x) {
  vapply(
    seq_along(x),
    function(i) {
      if (i < 5L || any(is.na(x[(i - 4L):i]))) {
        return(NA_real_)
      }
      mean(x[(i - 4L):i])
    },
    numeric(1)
  )
}

period_from_year <- function(year) {
  case_when(
    year >= 1990 & year <= 1997 ~ "1990-1997",
    year >= 1998 & year <= 2002 ~ "1998-2002",
    year >= 2003 & year <= 2009 ~ "2003-2009",
    year >= 2010 & year <= 2017 ~ "2010-2017",
    year >= 2018 & year <= 2025 ~ "2018-2025",
    TRUE ~ NA_character_
  )
}

council_year <- read_csv("../input/council_land_use_decision_trends_year.csv", show_col_types = FALSE) |>
  transmute(
    year = suppressWarnings(as.integer(query_year)),
    series_id = "council_land_use_matters",
    series_label = "Council Legistar land-use matters",
    annual_count = suppressWarnings(as.numeric(matter_rows))
  ) |>
  filter(year >= 1998, year <= 2025)

zap_year <- read_csv("../input/citywide_ulurp_application_year.csv", show_col_types = FALSE) |>
  filter(
    count_unit == "zap_project_records",
    outcome_id == "all_ulurp_applications"
  ) |>
  transmute(
    year = suppressWarnings(as.integer(cert_year)),
    series_id = "zap_ulurp_project_records",
    series_label = "ZAP ULURP project records",
    annual_count = suppressWarnings(as.numeric(application_count))
  ) |>
  filter(year >= 1990, year <= 2025)

if (nrow(council_year) != n_distinct(council_year$year)) {
  stop("Council trends input must be unique by year.")
}

if (nrow(zap_year) != n_distinct(zap_year$year)) {
  stop("ZAP citywide trend input must be unique by year for all ULURP project records.")
}

comparison_year <- expand_grid(
  year = 1990:2025,
  series_id = c("council_land_use_matters", "zap_ulurp_project_records")
) |>
  mutate(
    series_label = case_when(
      series_id == "council_land_use_matters" ~ "Council Legistar land-use matters",
      series_id == "zap_ulurp_project_records" ~ "ZAP ULURP project records",
      TRUE ~ series_id
    )
  ) |>
  left_join(
    bind_rows(council_year, zap_year) |>
      select(year, series_id, annual_count),
    by = c("year", "series_id"),
    relationship = "one-to-one"
  ) |>
  arrange(series_id, year) |>
  group_by(series_id) |>
  mutate(
    rolling_5_count = rolling_average_5(annual_count),
    baseline_count = mean(annual_count[year >= 1998 & year <= 2002], na.rm = TRUE),
    annual_index_1998_2002 = 100 * annual_count / baseline_count,
    rolling_5_index_1998_2002 = 100 * rolling_5_count / baseline_count
  ) |>
  ungroup() |>
  mutate(period = period_from_year(year)) |>
  arrange(series_id, year)

period_summary <- comparison_year |>
  filter(!is.na(period), !is.na(annual_count)) |>
  group_by(series_id, series_label, period) |>
  summarize(
    years_observed = n_distinct(year),
    annual_count_sum = sum(annual_count),
    annual_count_mean = mean(annual_count),
    annual_index_mean = mean(annual_index_1998_2002),
    .groups = "drop"
  ) |>
  arrange(series_id, period)

overlap_year <- comparison_year |>
  filter(year >= 1998, year <= 2025) |>
  select(year, series_id, annual_count, rolling_5_count, annual_index_1998_2002, rolling_5_index_1998_2002) |>
  pivot_wider(
    names_from = series_id,
    values_from = c(annual_count, rolling_5_count, annual_index_1998_2002, rolling_5_index_1998_2002)
  )

qc <- tibble(
  metric = c(
    "council_year_count",
    "zap_year_count",
    "overlap_year_count",
    "council_1998_2002_baseline_count",
    "zap_1998_2002_baseline_count",
    "annual_count_correlation_1998_2025",
    "rolling_5_index_correlation_2002_2025"
  ),
  value = c(
    as.character(nrow(council_year)),
    as.character(nrow(zap_year)),
    as.character(nrow(overlap_year)),
    formatC(unique(comparison_year$baseline_count[comparison_year$series_id == "council_land_use_matters"])[1], format = "f", digits = 3),
    formatC(unique(comparison_year$baseline_count[comparison_year$series_id == "zap_ulurp_project_records"])[1], format = "f", digits = 3),
    formatC(
      cor(
        overlap_year$annual_count_council_land_use_matters,
        overlap_year$annual_count_zap_ulurp_project_records,
        use = "complete.obs"
      ),
      format = "f",
      digits = 3
    ),
    formatC(
      cor(
        overlap_year$rolling_5_index_1998_2002_council_land_use_matters,
        overlap_year$rolling_5_index_1998_2002_zap_ulurp_project_records,
        use = "complete.obs"
      ),
      format = "f",
      digits = 3
    )
  ),
  status = c(
    if_else(nrow(council_year) == 28, "pass", "warning"),
    if_else(nrow(zap_year) == 36, "pass", "warning"),
    if_else(nrow(overlap_year) == 28, "pass", "warning"),
    "pass",
    "pass",
    "pass",
    "pass"
  ),
  note = c(
    "Council Legistar matter counts cover 1998-2025.",
    "ZAP project-record counts cover 1990-2025.",
    "Common annual comparison window.",
    "Mean Council matter count over 1998-2002.",
    "Mean ZAP ULURP project-record count over 1998-2002.",
    "Correlation of annual raw counts in the overlapping window.",
    "Correlation of trailing 5-year indexed series where both are observed."
  )
)

write_csv_if_changed(comparison_year, "../output/council_zap_land_use_volume_comparison_year.csv")
write_csv_if_changed(period_summary, "../output/council_zap_land_use_volume_comparison_period_summary.csv")
write_csv_if_changed(qc, "../output/council_zap_land_use_volume_comparison_qc.csv")

raw_facets_plot <- comparison_year |>
  filter(!is.na(annual_count)) |>
  ggplot(aes(x = year)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(aes(y = annual_count), color = "grey70", linewidth = 0.55) +
  geom_point(aes(y = annual_count), color = "grey60", size = 1.4, alpha = 0.8) +
  geom_line(aes(y = rolling_5_count), color = "#1f78b4", linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = rolling_5_count), color = "#1f78b4", size = 1.5, na.rm = TRUE) +
  facet_wrap(~series_label, scales = "free_y", ncol = 1) +
  scale_x_continuous(breaks = plot_year_breaks) +
  labs(
    x = NULL,
    y = "Annual count",
    caption = "Grey series is the annual raw count. Blue series is the trailing 5-year average. Units differ across panels."
  )

ggsave(
  "../output/council_zap_land_use_volume_raw_facets.pdf",
  raw_facets_plot,
  width = 7.5,
  height = 6.2
)

indexed_plot <- comparison_year |>
  filter(!is.na(annual_index_1998_2002)) |>
  ggplot(aes(x = year, color = series_label)) +
  geom_hline(yintercept = 100, linewidth = 0.3, color = "grey70") +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(aes(y = annual_index_1998_2002), linewidth = 0.45, alpha = 0.28) +
  geom_point(aes(y = annual_index_1998_2002), size = 1.2, alpha = 0.28) +
  geom_line(aes(y = rolling_5_index_1998_2002), linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = rolling_5_index_1998_2002), size = 1.5, na.rm = TRUE) +
  scale_x_continuous(breaks = plot_year_breaks) +
  scale_color_manual(
    values = c(
      "Council Legistar land-use matters" = "#1f78b4",
      "ZAP ULURP project records" = "#b15928"
    )
  ) +
  labs(
    x = NULL,
    y = "Index, 1998-2002 average = 100",
    color = NULL,
    caption = "Faint lines are annual values. Solid lines are trailing 5-year averages."
  )

ggsave(
  "../output/council_zap_land_use_volume_indexed.pdf",
  indexed_plot,
  width = 7.5,
  height = 4.7
)
