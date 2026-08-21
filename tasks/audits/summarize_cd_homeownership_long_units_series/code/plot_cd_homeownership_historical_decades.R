# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/summarize_cd_homeownership_long_units_series/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tidyr)
})

measure_df <- read_csv("../input/cd_homeownership_1990_measure.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code))
  )

district_lookup <- measure_df |>
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

if (anyDuplicated(district_lookup$borocd)) {
  stop("Historical-plot district lookup is not unique by borocd.")
}

if (n_distinct(district_lookup$borocd) != 59) {
  stop("Expected the historical-plot district lookup to cover 59 community districts.")
}

historical_decade_df <- read_csv("../input/mappluto_construction_proxy_cd_year.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    year = suppressWarnings(as.integer(yearbuilt)),
    `Units built: total` = suppressWarnings(as.numeric(residential_units_proxy)),
    `Units built: 50+` = suppressWarnings(as.numeric(units_50_plus_proxy))
  ) |>
  filter(year >= 1910, year <= 2025) |>
  pivot_longer(
    cols = c(`Units built: total`, `Units built: 50+`),
    names_to = "series_label",
    values_to = "outcome_value"
  ) |>
  left_join(
    district_lookup |> select(borocd, treat_tercile, treat_tercile_label),
    by = "borocd",
    relationship = "many-to-one"
  ) |>
  mutate(
    period_start = floor(year / 10) * 10,
    period_end = pmin(period_start + 9, 2025),
    period_midpoint = (period_start + period_end) / 2,
    period_label = if_else(period_start == 2020, "2020-2025", paste0(period_start, "s"))
  ) |>
  group_by(
    series_label,
    treat_tercile,
    treat_tercile_label,
    period_start,
    period_end,
    period_midpoint,
    period_label
  ) |>
  summarise(
    years_in_period = n_distinct(year),
    average_annual_units = sum(outcome_value, na.rm = TRUE) / years_in_period,
    .groups = "drop"
  ) |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = factor(series_label, levels = c("Units built: total", "Units built: 50+"))
  ) |>
  arrange(series_label, period_start, treat_tercile)

if (nrow(historical_decade_df) != 72) {
  stop("Expected 72 historical decade-by-series-by-tercile cells.")
}

make_historical_plot <- function(plot_df, title_text) {
  period_lookup <- plot_df |>
    distinct(period_start, period_midpoint, period_label) |>
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
        "Source: 25v4 MapPLUTO current-stock YearBuilt proxy on fixed current community-district boundaries.",
        "The 59 community districts were established in 1975; the current boundary map was revised in 1995.",
        "Pre-1995 bins are fixed-geography backcasts, not contemporaneous administrative districts.",
        "DCP describes YearBuilt as generally accurate to the decade from 1910-1985; older counts are subject to survivor bias.",
        "The 2020-2025 bin contains six years.",
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
  "Historical housing production by community-district homeownership tercile, 1910-2025"
)

historical_plot_zoom <- make_historical_plot(
  historical_decade_df |> filter(period_start >= 1940),
  "Historical housing production by community-district homeownership tercile, 1940-2025 zoom"
)

pdf("../output/cd_homeownership_historical_decade_units_plot.pdf", width = 11, height = 8.5)
print(historical_plot_full)
print(historical_plot_zoom)
dev.off()

cat("Wrote historical decade-binned community-district housing-production plot to ../output\n")
