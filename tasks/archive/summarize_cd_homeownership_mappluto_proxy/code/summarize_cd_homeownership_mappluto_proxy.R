# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_cd_homeownership_mappluto_proxy/code")
# cd_homeownership_mappluto_proxy_panel_csv <- "../input/cd_homeownership_mappluto_proxy_panel.csv"
# out_tercile_year_csv <- "../output/cd_homeownership_mappluto_proxy_tercile_year.csv"
# out_tercile_era_csv <- "../output/cd_homeownership_mappluto_proxy_tercile_era.csv"
# out_qc_csv <- "../output/cd_homeownership_mappluto_proxy_summary_qc.csv"
# out_plots_pdf <- "../output/cd_homeownership_mappluto_proxy_plots.pdf"

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tidyr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 5) {
  stop("Expected 5 arguments: cd_homeownership_mappluto_proxy_panel_csv out_tercile_year_csv out_tercile_era_csv out_qc_csv out_plots_pdf")
}

cd_homeownership_mappluto_proxy_panel_csv <- args[1]
out_tercile_year_csv <- args[2]
out_tercile_era_csv <- args[3]
out_qc_csv <- args[4]
out_plots_pdf <- args[5]

panel_df <- read_csv(cd_homeownership_mappluto_proxy_panel_csv, show_col_types = FALSE, na = c("", "NA"))

plot_outcomes <- tribble(
  ~outcome_family, ~plot_family,
  "residential_units_proxy", "All residential units",
  "units_1_4_proxy", "Units in 1-4 unit buildings",
  "units_5_plus_proxy", "Units in 5+ unit buildings",
  "units_50_plus_proxy", "Units in 50+ unit buildings"
)

district_lookup <- panel_df |>
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

plot_panel <- panel_df |>
  inner_join(plot_outcomes, by = "outcome_family") |>
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
      TRUE ~ NA_character_
    )
  )

tercile_year_df <- plot_panel |>
  group_by(outcome_family, plot_family, year, borough_code, borough_name, treat_tercile, treat_tercile_label) |>
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = first(borough_outcome_total),
    .groups = "drop"
  ) |>
  group_by(outcome_family, plot_family, year, treat_tercile, treat_tercile_label) |>
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = sum(borough_outcome_total, na.rm = TRUE),
    borough_outcome_share = if_else(borough_outcome_total > 0, outcome_value / borough_outcome_total, NA_real_),
    borough_count = n_distinct(borough_code),
    .groups = "drop"
  ) |>
  arrange(outcome_family, year, treat_tercile)

tercile_era_df <- plot_panel |>
  filter(!is.na(era)) |>
  group_by(outcome_family, plot_family, era, borough_code, borough_name, treat_tercile, treat_tercile_label) |>
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = sum(distinct(data.frame(year, borough_outcome_total))$borough_outcome_total, na.rm = TRUE),
    .groups = "drop"
  ) |>
  group_by(outcome_family, plot_family, era, treat_tercile, treat_tercile_label) |>
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = sum(borough_outcome_total, na.rm = TRUE),
    borough_outcome_share = if_else(borough_outcome_total > 0, outcome_value / borough_outcome_total, NA_real_),
    borough_count = n_distinct(borough_code),
    .groups = "drop"
  ) |>
  arrange(outcome_family, era, treat_tercile)

write_csv(tercile_year_df, out_tercile_year_csv, na = "")
write_csv(tercile_era_df, out_tercile_era_csv, na = "")

write_csv(
  bind_rows(
    tibble(metric = "district_count", value = n_distinct(district_lookup$borocd), note = "Community districts assigned to treatment terciles."),
    tibble(metric = "borough_min_tercile_size", value = district_lookup |>
      count(borough_code, treat_tercile) |>
      summarize(value = min(n), .groups = "drop") |>
      pull(value), note = "Minimum number of CDs in any within-borough treatment tercile."),
    tibble(metric = "tercile_year_row_count", value = nrow(tercile_year_df), note = "Rows in the annual tercile summary."),
    tibble(metric = "tercile_era_row_count", value = nrow(tercile_era_df), note = "Rows in the era tercile summary.")
  ),
  out_qc_csv,
  na = ""
)

plot_df <- tercile_year_df |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    plot_family = factor(plot_family, levels = c("All residential units", "Units in 1-4 unit buildings", "Units in 5+ unit buildings", "Units in 50+ unit buildings"))
  )

pdf(out_plots_pdf, width = 11, height = 8.5)
print(
  ggplot(plot_df, aes(x = year, y = borough_outcome_share, color = treat_tercile_label)) +
    geom_line(linewidth = 0.8) +
    facet_wrap(~plot_family, scales = "free_y", ncol = 2) +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(
      x = NULL,
      y = "Within-borough share",
      color = "Treat tercile"
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
dev.off()

cat("Wrote MapPLUTO proxy summaries to", dirname(out_tercile_year_csv), "\n")
