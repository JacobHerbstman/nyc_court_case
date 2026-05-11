# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_ccd2010_homeownership_long_units_series/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tibble)
  library(tidyr)
})

assert_unique_keys <- function(df, keys, label) {
  duplicate_keys <- df %>%
    count(across(all_of(keys)), name = "n") %>%
    filter(n > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(label, " is not unique by ", paste(keys, collapse = ", "), ".")
  }
}

make_centered_moving_average <- function(df, window_years) {
  half_window <- (window_years - 1) / 2

  df %>%
    group_by(series_family, series_label, treat_tercile, treat_tercile_label) %>%
    arrange(year, .by_group = TRUE) %>%
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
      outcome_value_ma = vapply(
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
      borough_outcome_share_ma = vapply(
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
    ) %>%
    ungroup() %>%
    filter(full_smoothing_window) %>%
    arrange(series_family, year, treat_tercile)
}

series_df <- read_csv("../input/ccdist2010_homeownership_long_units_series.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    council_district = suppressWarnings(as.integer(council_district)),
    borough_code = as.character(borough_code)
  )

district_lookup <- series_df %>%
  distinct(district_id, council_district, borough_code, borough_name, treat_pp, treat_z_boro, occupied_units_1990) %>%
  group_by(borough_code, borough_name) %>%
  mutate(
    treat_tercile = ntile(treat_pp, 3),
    treat_tercile_label = case_when(
      treat_tercile == 1 ~ "Low",
      treat_tercile == 2 ~ "Middle",
      TRUE ~ "High"
    )
  ) %>%
  ungroup()

assert_unique_keys(district_lookup, "district_id", "long-units district lookup")

if (n_distinct(district_lookup$district_id) != 51) {
  stop("Expected the long-units district lookup to cover 51 Council districts.")
}

city_year_df <- series_df %>%
  group_by(series_kind, source_family, source_label, series_family, series_label, year) %>%
  summarize(city_outcome_total = sum(outcome_value, na.rm = TRUE), .groups = "drop") %>%
  arrange(series_kind, series_family, year)

preferred_df <- series_df %>%
  filter(series_kind == "preferred_long_series") %>%
  left_join(
    district_lookup %>% select(district_id, treat_tercile, treat_tercile_label),
    by = "district_id",
    relationship = "many-to-one"
  ) %>%
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

tercile_year_df <- preferred_df %>%
  group_by(series_family, series_label, year, borough_code, borough_name, treat_tercile, treat_tercile_label) %>%
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = first(borough_outcome_total),
    .groups = "drop"
  ) %>%
  group_by(series_family, series_label, year, treat_tercile, treat_tercile_label) %>%
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = sum(borough_outcome_total, na.rm = TRUE),
    borough_outcome_share = if_else(borough_outcome_total > 0, outcome_value / borough_outcome_total, NA_real_),
    .groups = "drop"
  ) %>%
  arrange(series_family, year, treat_tercile)

required_tercile_series <- c("units_built_total", "units_built_1_2", "units_built_50_plus", "projects_built_50_plus")
missing_tercile_series <- setdiff(required_tercile_series, unique(tercile_year_df$series_family))

if (length(missing_tercile_series) > 0) {
  stop("Missing required long-units tercile series: ", paste(missing_tercile_series, collapse = ", "))
}

required_tercile_series_gaps <- tercile_year_df %>%
  filter(series_family %in% required_tercile_series) %>%
  count(series_family, year, name = "tercile_count") %>%
  filter(tercile_count != 3)

if (nrow(required_tercile_series_gaps) > 0) {
  stop("Required long-units tercile series do not have exactly three terciles in every year.")
}

tercile_year_ma3_df <- make_centered_moving_average(tercile_year_df, 3)

tercile_era_df <- preferred_df %>%
  filter(!is.na(era)) %>%
  group_by(series_family, series_label, era, borough_code, borough_name, treat_tercile, treat_tercile_label) %>%
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = sum(distinct(data.frame(year, borough_outcome_total))$borough_outcome_total, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  group_by(series_family, series_label, era, treat_tercile, treat_tercile_label) %>%
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = sum(borough_outcome_total, na.rm = TRUE),
    borough_outcome_share = if_else(borough_outcome_total > 0, outcome_value / borough_outcome_total, NA_real_),
    .groups = "drop"
  ) %>%
  arrange(series_family, era, treat_tercile)

brooklyn_rank_df <- preferred_df %>%
  filter(
    borough_name == "Brooklyn",
    year >= 2020,
    year <= 2025,
    series_family %in% c("units_built_total", "units_built_1_2", "units_built_50_plus")
  ) %>%
  group_by(district_id, council_district, borough_code, borough_name, treat_z_boro, occupied_units_1990, series_family) %>%
  summarize(
    annual_rate_per_10000_occupied = sum(outcome_value, na.rm = TRUE) / n_distinct(year) * 10000 / first(occupied_units_1990),
    .groups = "drop"
  ) %>%
  pivot_wider(
    names_from = series_family,
    values_from = annual_rate_per_10000_occupied,
    names_glue = "{series_family}_2020_2025_per_10000_occupied"
  ) %>%
  mutate(district_label = paste0("Council ", council_district)) %>%
  arrange(desc(treat_z_boro))

if (nrow(brooklyn_rank_df) != 16) {
  stop("Expected exactly 16 2010 Council districts assigned to Brooklyn for the Brooklyn rank plot.")
}

write_csv(city_year_df, "../output/ccdist2010_homeownership_long_units_city_year.csv", na = "")
write_csv(tercile_year_df, "../output/ccdist2010_homeownership_long_units_tercile_year.csv", na = "")
write_csv(tercile_year_ma3_df, "../output/ccdist2010_homeownership_long_units_tercile_year_ma3.csv", na = "")
write_csv(tercile_era_df, "../output/ccdist2010_homeownership_long_units_tercile_era.csv", na = "")
write_csv(brooklyn_rank_df, "../output/ccdist2010_homeownership_long_units_brooklyn_rank.csv", na = "")

write_csv(
  bind_rows(
    tibble(metric = "district_count", value = n_distinct(district_lookup$district_id), note = "2010 Council districts assigned to treatment terciles."),
    tibble(metric = "preferred_series_year_gap_count", value = nrow(preferred_df %>% count(series_family, year, name = "district_count") %>% filter(district_count != 51)), note = "Preferred series-family-year cells not covering all 51 Council districts."),
    tibble(metric = "city_year_row_count", value = nrow(city_year_df), note = "Rows in the city-year long units summary."),
    tibble(metric = "tercile_year_row_count", value = nrow(tercile_year_df), note = "Rows in the annual tercile summary for the preferred series."),
    tibble(metric = "tercile_year_ma3_row_count", value = nrow(tercile_year_ma3_df), note = "Rows in the 3-year centered moving-average tercile summary."),
    tibble(metric = "required_tercile_series_gap_count", value = nrow(required_tercile_series_gaps), note = "Required annual series-family-year cells with other than three treatment terciles."),
    tibble(metric = "tercile_era_row_count", value = nrow(tercile_era_df), note = "Rows in the era tercile summary for the preferred series."),
    tibble(metric = "brooklyn_rank_row_count", value = nrow(brooklyn_rank_df), note = "Rows in the Brooklyn Council-district rank summary.")
  ),
  "../output/ccdist2010_homeownership_long_units_summary_qc.csv",
  na = ""
)

city_plot_df <- city_year_df %>%
  filter(series_family %in% c("units_built_total", "gross_add_units_observed")) %>%
  mutate(series_label = factor(series_label, levels = c("Units built: total", "Gross additions observed")))

tercile_plot_df <- tercile_year_df %>%
  filter(series_family %in% c("units_built_total", "units_built_50_plus")) %>%
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = factor(series_label, levels = c("Units built: total", "Units built: 50+"))
  )

tercile_plot_ma3_df <- tercile_year_ma3_df %>%
  filter(series_family %in% c("units_built_total", "units_built_50_plus")) %>%
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = factor(series_label, levels = c("Units built: total", "Units built: 50+"))
  )

brooklyn_rank_plot_df <- brooklyn_rank_df %>%
  transmute(
    district_label,
    treat_z_boro,
    `1990 homeowner exposure` = treat_z_boro,
    `Total units, 2020-2025` = units_built_total_2020_2025_per_10000_occupied,
    `1-2 unit building units, 2020-2025` = units_built_1_2_2020_2025_per_10000_occupied,
    `50+ units, 2020-2025` = units_built_50_plus_2020_2025_per_10000_occupied
  ) %>%
  pivot_longer(
    cols = -c(district_label, treat_z_boro),
    names_to = "metric",
    values_to = "metric_value"
  ) %>%
  mutate(
    district_label = factor(district_label, levels = rev(brooklyn_rank_df$district_label)),
    metric = factor(
      metric,
      levels = c(
        "1990 homeowner exposure",
        "Total units, 2020-2025",
        "1-2 unit building units, 2020-2025",
        "50+ units, 2020-2025"
      )
    )
  )

pdf("../output/ccdist2010_homeownership_long_units_plots.pdf", width = 11, height = 8.5)
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
print(
  ggplot(tercile_plot_ma3_df, aes(x = year, y = borough_outcome_share_ma, color = treat_tercile_label, group = treat_tercile_label)) +
    geom_line(linewidth = 0.9) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    facet_wrap(~series_label, scales = "free_y", ncol = 1) +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = "Within-borough share (3-year centered MA)", color = "Treat tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(brooklyn_rank_plot_df, aes(x = district_label, y = metric_value)) +
    geom_col(fill = "#c44e52") +
    coord_flip() +
    facet_wrap(~metric, scales = "free_x", ncol = 2) +
    labs(
      title = "Brooklyn rank plot",
      subtitle = "Council districts ordered by homeowner exposure, with later construction margins alongside",
      x = NULL,
      y = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(
      plot.background = element_rect(fill = "white", color = NA),
      panel.background = element_rect(fill = "white", color = NA),
      strip.background = element_blank(),
      strip.text = element_text(face = "bold"),
      panel.grid.minor = element_blank(),
      panel.grid.major.y = element_blank()
    )
)
dev.off()

cat("Wrote 2010 Council district long units summaries to ../output\n")
