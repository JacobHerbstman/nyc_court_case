# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_cd_homeownership_outcome_panels/code")
# permit_panel_csv <- "../input/cd_homeownership_permit_nb_panel.csv"
# dcp_units_panel_csv <- "../input/cd_homeownership_dcp_units_panel.csv"
# out_membership_csv <- "../output/cd_homeownership_outcome_tercile_membership.csv"
# out_year_csv <- "../output/cd_homeownership_outcome_tercile_year.csv"
# out_five_year_csv <- "../output/cd_homeownership_outcome_tercile_five_year.csv"
# out_qc_csv <- "../output/cd_homeownership_outcome_panels_qc.csv"
# out_figures_pdf <- "../output/cd_homeownership_outcome_tercile_plots.pdf"

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 7) {
  stop("Expected 7 arguments: permit_panel_csv dcp_units_panel_csv out_membership_csv out_year_csv out_five_year_csv out_qc_csv out_figures_pdf")
}

permit_panel_csv <- args[1]
dcp_units_panel_csv <- args[2]
out_membership_csv <- args[3]
out_year_csv <- args[4]
out_five_year_csv <- args[5]
out_qc_csv <- args[6]
out_figures_pdf <- args[7]

permit_panel <- read_csv(permit_panel_csv, show_col_types = FALSE, na = c("", "NA"))
dcp_units_panel <- read_csv(dcp_units_panel_csv, show_col_types = FALSE, na = c("", "NA"))

membership_df <- permit_panel %>%
  distinct(borocd, district_id, borough_code, borough_name, treat_pp, treat_z_boro) %>%
  arrange(borough_code, treat_pp, borocd) %>%
  group_by(borough_code, borough_name) %>%
  mutate(
    treat_tercile_n = ntile(treat_pp, 3),
    treat_tercile = dplyr::recode(
      as.character(treat_tercile_n),
      `1` = "Low",
      `2` = "Middle",
      `3` = "High"
    )
  ) %>%
  ungroup() %>%
  mutate(treat_tercile = factor(treat_tercile, levels = c("Low", "Middle", "High")))

panel_df <- bind_rows(permit_panel, dcp_units_panel) %>%
  left_join(
    membership_df %>%
      select(borocd, treat_tercile_n, treat_tercile),
    by = "borocd"
  ) %>%
  mutate(
    outcome_label = dplyr::recode(
      outcome_family,
      permit_nb_jobs = "DOB new-building jobs",
      dcp_net_units = "DCP Housing Database net units"
    )
  ) %>%
  group_by(outcome_family) %>%
  mutate(
    family_start_year = min(year, na.rm = TRUE),
    family_end_year = max(year, na.rm = TRUE),
    five_year_bin_start = family_start_year + 5L * ((year - family_start_year) %/% 5L),
    five_year_bin_end = pmin(five_year_bin_start + 4L, family_end_year),
    five_year_bin = str_c(five_year_bin_start, "-", five_year_bin_end)
  ) %>%
  ungroup()

year_totals_df <- panel_df %>%
  group_by(outcome_family, outcome_label, year) %>%
  summarise(city_outcome_total = sum(outcome_value, na.rm = TRUE), .groups = "drop")

year_share_df <- panel_df %>%
  group_by(outcome_family, outcome_label, year, treat_tercile_n, treat_tercile) %>%
  summarise(tercile_outcome_total = sum(outcome_value, na.rm = TRUE), .groups = "drop") %>%
  left_join(year_totals_df, by = c("outcome_family", "outcome_label", "year")) %>%
  mutate(tercile_share = ifelse(city_outcome_total == 0, NA_real_, tercile_outcome_total / city_outcome_total)) %>%
  arrange(outcome_family, year, treat_tercile_n)

five_year_totals_df <- panel_df %>%
  group_by(outcome_family, outcome_label, five_year_bin_start, five_year_bin_end, five_year_bin) %>%
  summarise(city_outcome_total = sum(outcome_value, na.rm = TRUE), .groups = "drop")

five_year_share_df <- panel_df %>%
  group_by(outcome_family, outcome_label, five_year_bin_start, five_year_bin_end, five_year_bin, treat_tercile_n, treat_tercile) %>%
  summarise(tercile_outcome_total = sum(outcome_value, na.rm = TRUE), .groups = "drop") %>%
  left_join(
    five_year_totals_df,
    by = c("outcome_family", "outcome_label", "five_year_bin_start", "five_year_bin_end", "five_year_bin")
  ) %>%
  mutate(tercile_share = ifelse(city_outcome_total == 0, NA_real_, tercile_outcome_total / city_outcome_total)) %>%
  arrange(outcome_family, five_year_bin_start, treat_tercile_n)

qc_df <- bind_rows(
  tibble(
    metric = "membership_district_count",
    value = nrow(membership_df),
    note = "Community districts assigned to within-borough treatment terciles."
  ),
  membership_df %>%
    count(borough_name, treat_tercile, name = "value") %>%
    mutate(
      metric = str_c("tercile_membership_", borough_name, "_", treat_tercile),
      note = "Community districts in each within-borough treatment tercile."
    ) %>%
    select(metric, value, note),
  year_share_df %>%
    group_by(outcome_family, outcome_label) %>%
    summarise(
      value = n_distinct(year),
      .groups = "drop"
    ) %>%
    mutate(
      metric = str_c("year_count_", outcome_family),
      note = str_c("Distinct years represented in the tercile-share summary for ", outcome_label, ".")
    ) %>%
    select(metric, value, note),
  five_year_share_df %>%
    group_by(outcome_family, outcome_label) %>%
    summarise(
      value = n_distinct(five_year_bin),
      .groups = "drop"
    ) %>%
    mutate(
      metric = str_c("five_year_bin_count_", outcome_family),
      note = str_c("Distinct five-year bins represented in the tercile-share summary for ", outcome_label, ".")
    ) %>%
    select(metric, value, note),
  year_share_df %>%
    filter(city_outcome_total == 0) %>%
    group_by(outcome_family, outcome_label) %>%
    summarise(
      value = n_distinct(year),
      .groups = "drop"
    ) %>%
    mutate(
      metric = str_c("zero_city_total_year_count_", outcome_family),
      note = str_c("Years with zero citywide outcome totals, so tercile shares are undefined, for ", outcome_label, ".")
    ) %>%
    select(metric, value, note),
  tibble(
    metric = "status",
    value = ifelse(
      nrow(membership_df) == 59 &&
        all(!is.na(year_share_df$tercile_share[year_share_df$city_outcome_total > 0])) &&
        all(!is.na(five_year_share_df$tercile_share[five_year_share_df$city_outcome_total > 0])),
      1,
      0
    ),
    note = "One means the tercile assignments and aggregated shares are complete wherever the citywide outcome denominator is positive."
  )
)

year_plot <- year_share_df %>%
  filter(city_outcome_total > 0) %>%
  ggplot(aes(x = year, y = tercile_share, color = treat_tercile)) +
  geom_line(linewidth = 0.8) +
  facet_wrap(~ outcome_label, ncol = 1, scales = "free_x") +
  scale_color_manual(values = c(Low = "#2c7bb6", Middle = "#fdae61", High = "#d7191c")) +
  scale_y_continuous(labels = function(x) paste0(round(100 * x), "%")) +
  labs(
    title = "Outcome Shares by Within-Borough Homeownership Tercile and Year",
    x = "Year",
    y = "Share of citywide outcome total",
    color = NULL
  ) +
  theme_minimal(base_size = 12)

five_year_plot <- five_year_share_df %>%
  filter(city_outcome_total > 0) %>%
  ggplot(aes(x = factor(five_year_bin, levels = unique(five_year_bin)), y = tercile_share, fill = treat_tercile)) +
  geom_col(position = "dodge") +
  facet_wrap(~ outcome_label, ncol = 1, scales = "free_x") +
  scale_fill_manual(values = c(Low = "#2c7bb6", Middle = "#fdae61", High = "#d7191c")) +
  scale_y_continuous(labels = function(x) paste0(round(100 * x), "%")) +
  labs(
    title = "Outcome Shares by Within-Borough Homeownership Tercile and Five-Year Bin",
    x = "Five-year bin",
    y = "Share of citywide outcome total",
    fill = NULL
  ) +
  theme_minimal(base_size = 12) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

pdf(out_figures_pdf, width = 11, height = 8.5)
print(year_plot)
print(five_year_plot)
dev.off()

write_csv_if_changed(membership_df, out_membership_csv)
write_csv_if_changed(year_share_df, out_year_csv)
write_csv_if_changed(five_year_share_df, out_five_year_csv)
write_csv_if_changed(qc_df, out_qc_csv)

cat("Wrote homeownership outcome summary outputs to", dirname(out_membership_csv), "\n")
