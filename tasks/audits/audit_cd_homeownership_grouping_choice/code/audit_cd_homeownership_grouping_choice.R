# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_cd_homeownership_grouping_choice/code")
# start_year <- 1970
# end_year <- 2025
# moving_window_years <- 3

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tidyr)
})

cli_args <- commandArgs(trailingOnly = TRUE)
if (length(cli_args) != 3) {
  stop("Expected START_YEAR, END_YEAR, and MOVING_WINDOW_YEARS.")
}

start_year <- suppressWarnings(as.integer(cli_args[[1]]))
end_year <- suppressWarnings(as.integer(cli_args[[2]]))
moving_window_years <- suppressWarnings(as.integer(cli_args[[3]]))
if (
  any(is.na(c(start_year, end_year, moving_window_years))) ||
  end_year < start_year || moving_window_years < 1L || moving_window_years %% 2L == 0L
) {
  stop("Years must be valid and MOVING_WINDOW_YEARS must be a positive odd integer.")
}

series <- read_csv(
  "../input/cd_homeownership_long_units_series.csv",
  show_col_types = FALSE,
  na = c("", "NA")
) |>
  filter(
    series_kind == "preferred_long_series",
    series_family %in% c("units_built_total", "units_built_50_plus"),
    year >= start_year,
    year <= end_year
  )

districts <- series |>
  distinct(district_id, borough_code, treat_pp) |>
  arrange(borough_code, treat_pp, district_id) |>
  group_by(borough_code) |>
  mutate(
    tercile = ntile(treat_pp, 3),
    `Terciles` = c("Low", "Middle", "High")[tercile],
    median_group = ntile(treat_pp, 2),
    `Median split` = c("Below median", "Above median")[median_group]
  ) |>
  ungroup()

if (nrow(districts) != 59 || anyDuplicated(districts$district_id)) {
  stop("Expected 59 unique community districts.")
}

grouped_year <- series |>
  left_join(
    districts |> select(district_id, `Terciles`, `Median split`),
    by = "district_id",
    relationship = "many-to-one"
  ) |>
  pivot_longer(
    cols = c(`Terciles`, `Median split`),
    names_to = "grouping",
    values_to = "homeowner_group"
  ) |>
  group_by(grouping, homeowner_group, series_family, series_label, year) |>
  summarize(
    community_district_count = n_distinct(district_id),
    outcome_value = mean(outcome_value, na.rm = TRUE),
    .groups = "drop"
  )

half_window <- (moving_window_years - 1L) / 2L
grouped_year <- grouped_year |>
  group_by(grouping, homeowner_group, series_family, series_label) |>
  arrange(year, .by_group = TRUE) |>
  mutate(
    outcome_value_moving_average = vapply(year, function(current_year) {
      in_window <- abs(year - current_year) <= half_window
      if (sum(in_window) != moving_window_years) {
        return(NA_real_)
      }
      mean(outcome_value[in_window])
    }, numeric(1))
  ) |>
  ungroup() |>
  mutate(
    homeowner_group = factor(
      homeowner_group,
      levels = c("Low", "Middle", "High", "Below median", "Above median")
    ),
    series_label = factor(
      series_label,
      levels = c("Units built: total", "Units built: 50+")
    )
  ) |>
  arrange(grouping, series_family, homeowner_group, year)

write_csv(grouped_year, "../output/cd_homeownership_grouping_comparison.csv", na = "")

group_colors <- c(
  "Low" = "#3366CC",
  "Middle" = "#999999",
  "High" = "#CC3311",
  "Below median" = "#3366CC",
  "Above median" = "#CC3311"
)

pdf("../output/cd_homeownership_grouping_comparison.pdf", width = 11, height = 8.5)
for (split in c("Terciles", "Median split")) {
  print(
    grouped_year |>
      filter(grouping == split) |>
      ggplot(aes(
        x = year,
        y = outcome_value_moving_average,
        color = homeowner_group,
        group = homeowner_group
      )) +
      geom_line(linewidth = 0.9) +
      geom_vline(xintercept = 1990, linetype = "dashed", color = "#666666") +
      facet_wrap(~series_label, scales = "free_y", ncol = 1) +
      scale_color_manual(values = group_colors, drop = TRUE) +
      scale_y_continuous(labels = scales::comma) +
      labs(
        title = split,
        x = NULL,
        y = paste0("Mean units per community district (", moving_window_years, "-year centered MA)"),
        color = NULL
      ) +
      theme_minimal(base_size = 11) +
      theme(
        legend.position = "bottom",
        panel.grid.minor = element_blank()
      )
  )
}
dev.off()
