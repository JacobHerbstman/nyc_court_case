# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_ccd2010_homeownership_long_units_series/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
})

series_df <- read_csv("../input/ccdist2010_homeownership_long_units_series.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    year = suppressWarnings(as.integer(year))
  )

district_lookup <- series_df %>%
  distinct(district_id, borough_code, borough_name, treat_pp) %>%
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

if (anyDuplicated(district_lookup$district_id)) {
  stop("Historical-plot district lookup is not unique by district_id.")
}

if (n_distinct(district_lookup$district_id) != 51) {
  stop("Expected the historical-plot district lookup to cover 51 Council districts.")
}

historical_decade_df <- series_df %>%
  filter(
    series_kind == "preferred_long_series",
    source_family == "mappluto_proxy_25v4",
    series_family %in% c("units_built_total", "units_built_50_plus"),
    year >= 1910,
    year <= 2025
  ) %>%
  left_join(
    district_lookup %>% select(district_id, treat_tercile, treat_tercile_label),
    by = "district_id",
    relationship = "many-to-one"
  ) %>%
  mutate(
    period_start = floor(year / 10) * 10,
    period_end = pmin(period_start + 9, 2025),
    period_midpoint = (period_start + period_end) / 2,
    period_label = if_else(period_start == 2020, "2020-2025", paste0(period_start, "s"))
  ) %>%
  group_by(
    series_family,
    series_label,
    treat_tercile,
    treat_tercile_label,
    period_start,
    period_end,
    period_midpoint,
    period_label
  ) %>%
  summarise(
    years_in_period = n_distinct(year),
    average_annual_units = sum(outcome_value, na.rm = TRUE) / years_in_period,
    .groups = "drop"
  ) %>%
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = factor(series_label, levels = c("Units built: total", "Units built: 50+"))
  ) %>%
  arrange(series_family, period_start, treat_tercile)

if (nrow(historical_decade_df) != 72) {
  stop("Expected 72 historical decade-by-series-by-tercile cells.")
}

make_historical_plot <- function(plot_df, title_text) {
  period_lookup <- plot_df %>%
    distinct(period_start, period_midpoint, period_label) %>%
    arrange(period_start)

  ggplot(
    plot_df,
    aes(
      x = period_midpoint,
      y = average_annual_units,
      color = treat_tercile_label,
      group = treat_tercile_label
    )
  ) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    geom_vline(xintercept = 1990, linetype = "dashed", color = "#666666") +
    facet_wrap(~series_label, scales = "free_y", ncol = 1) +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    scale_x_continuous(
      breaks = period_lookup$period_midpoint,
      labels = period_lookup$period_label
    ) +
    scale_y_continuous(labels = scales::label_comma()) +
    labs(
      title = title_text,
      subtitle = "Average annual units within YearBuilt decade bins; dashed line marks the 1990 treatment measure",
      x = NULL,
      y = "Average annual units built",
      color = "Treat tercile",
      caption = paste(
        "Source: 25v4 MapPLUTO current-stock YearBuilt proxy on fixed 2010 Council districts.",
        "DCP describes YearBuilt as generally accurate to the decade from 1910-1985; older counts are subject to survivor bias. The 2020-2025 bin contains six years.",
        sep = "\n"
      )
    ) +
    theme_minimal(base_size = 11) +
    theme(
      legend.position = "bottom",
      panel.grid.minor = element_blank(),
      axis.text.x = element_text(angle = 35, hjust = 1),
      plot.caption = element_text(hjust = 0)
    )
}

historical_plot_full <- make_historical_plot(
  historical_decade_df,
  "Historical housing production by homeownership tercile, 1910-2025"
)

historical_plot_zoom <- make_historical_plot(
  historical_decade_df %>% filter(period_start >= 1940),
  "Historical housing production by homeownership tercile, 1940-2025 zoom"
)

pdf("../output/ccdist2010_homeownership_historical_decade_units_plot.pdf", width = 11, height = 8.5)
print(historical_plot_full)
print(historical_plot_zoom)
dev.off()

cat("Wrote historical decade-binned housing-production plot to ../output\n")
