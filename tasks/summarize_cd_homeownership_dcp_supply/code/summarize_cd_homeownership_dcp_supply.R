# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_cd_homeownership_dcp_supply/code")
# cd_homeownership_dcp_supply_panel_csv <- "../input/cd_homeownership_dcp_supply_panel.csv"
# out_tercile_year_csv <- "../output/cd_homeownership_dcp_supply_tercile_year.csv"
# out_tercile_five_year_csv <- "../output/cd_homeownership_dcp_supply_tercile_five_year.csv"
# out_proxy_split_five_year_csv <- "../output/cd_homeownership_dcp_supply_proxy_split_five_year.csv"
# out_qc_csv <- "../output/cd_homeownership_dcp_supply_summary_qc.csv"
# out_plots_pdf <- "../output/cd_homeownership_dcp_supply_plots.pdf"

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

if (length(args) != 6) {
  stop("Expected 6 arguments: cd_homeownership_dcp_supply_panel_csv out_tercile_year_csv out_tercile_five_year_csv out_proxy_split_five_year_csv out_qc_csv out_plots_pdf")
}

cd_homeownership_dcp_supply_panel_csv <- args[1]
out_tercile_year_csv <- args[2]
out_tercile_five_year_csv <- args[3]
out_proxy_split_five_year_csv <- args[4]
out_qc_csv <- args[5]
out_plots_pdf <- args[6]

supply_panel <- read_csv(cd_homeownership_dcp_supply_panel_csv, show_col_types = FALSE, na = c("", "NA"))

plot_outcomes <- tribble(
  ~outcome_family, ~plot_family,
  "gross_add_units", "Gross additions",
  "gross_loss_units", "Gross losses",
  "net_units", "Net units",
  "nb_gross_units_1_2", "NB gross units: 1-2",
  "nb_gross_units_3_4", "NB gross units: 3-4",
  "nb_gross_units_5_plus", "NB gross units: 5+"
)

proxy_outcomes <- tribble(
  ~outcome_family, ~plot_family,
  "gross_add_units", "Gross additions",
  "nb_gross_units_10_49", "NB gross units: 10-49",
  "nb_gross_units_50_plus", "NB gross units: 50+"
)

district_lookup <- supply_panel %>%
  distinct(
    borocd,
    borough_code,
    borough_name,
    treat_pp,
    subway_commute_share_1990_exact,
    mean_commute_time_1990_minutes_exact,
    median_household_income_1990_1999_dollars_exact,
    median_housing_value_1990_2000_dollars_exact_filled
  ) %>%
  group_by(borough_code, borough_name) %>%
  mutate(
    treat_tercile = ntile(treat_pp, 3),
    treat_tercile_label = case_when(
      treat_tercile == 1 ~ "Low",
      treat_tercile == 2 ~ "Middle",
      TRUE ~ "High"
    ),
    high_subway_share_flag = subway_commute_share_1990_exact >= median(subway_commute_share_1990_exact, na.rm = TRUE),
    short_commute_flag = mean_commute_time_1990_minutes_exact <= median(mean_commute_time_1990_minutes_exact, na.rm = TRUE),
    high_income_flag = median_household_income_1990_1999_dollars_exact >= median(median_household_income_1990_1999_dollars_exact, na.rm = TRUE),
    high_housing_value_flag = median_housing_value_1990_2000_dollars_exact_filled >= median(median_housing_value_1990_2000_dollars_exact_filled, na.rm = TRUE)
  ) %>%
  ungroup()

plot_panel <- supply_panel %>%
  inner_join(plot_outcomes, by = "outcome_family") %>%
  left_join(
    district_lookup %>%
      select(borocd, treat_tercile, treat_tercile_label),
    by = "borocd"
  ) %>%
  mutate(
    five_year_bin = case_when(
      year >= 2000 & year <= 2004 ~ "2000-2004",
      year >= 2005 & year <= 2009 ~ "2005-2009",
      year >= 2010 & year <= 2014 ~ "2010-2014",
      year >= 2015 & year <= 2019 ~ "2015-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    )
  )

tercile_year_df <- plot_panel %>%
  group_by(outcome_family, plot_family, year, borough_code, borough_name, treat_tercile, treat_tercile_label) %>%
  summarise(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = first(borough_outcome_total),
    .groups = "drop"
  ) %>%
  group_by(outcome_family, plot_family, year, treat_tercile, treat_tercile_label) %>%
  summarise(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = sum(borough_outcome_total, na.rm = TRUE),
    borough_outcome_share = ifelse(borough_outcome_total > 0, outcome_value / borough_outcome_total, NA_real_),
    borough_count = n_distinct(borough_code),
    .groups = "drop"
  ) %>%
  arrange(outcome_family, year, treat_tercile)

tercile_five_year_df <- plot_panel %>%
  filter(!is.na(five_year_bin)) %>%
  group_by(outcome_family, plot_family, five_year_bin, borough_code, borough_name, treat_tercile, treat_tercile_label) %>%
  summarise(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = sum(distinct(data.frame(year, borough_outcome_total))$borough_outcome_total, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  group_by(outcome_family, plot_family, five_year_bin, treat_tercile, treat_tercile_label) %>%
  summarise(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = sum(borough_outcome_total, na.rm = TRUE),
    borough_outcome_share = ifelse(borough_outcome_total > 0, outcome_value / borough_outcome_total, NA_real_),
    borough_count = n_distinct(borough_code),
    .groups = "drop"
  ) %>%
  arrange(outcome_family, five_year_bin, treat_tercile)

proxy_lookup_long <- district_lookup %>%
  transmute(
    borocd,
    high_subway_share = high_subway_share_flag,
    low_subway_share = !high_subway_share_flag,
    short_commute = short_commute_flag,
    long_commute = !short_commute_flag,
    high_income = high_income_flag,
    low_income = !high_income_flag,
    high_housing_value = high_housing_value_flag,
    low_housing_value = !high_housing_value_flag
  ) %>%
  pivot_longer(
    cols = -borocd,
    names_to = "proxy_split",
    values_to = "proxy_flag"
  ) %>%
  filter(proxy_flag) %>%
  mutate(
    proxy_name = case_when(
      str_detect(proxy_split, "subway") ~ "subway_share",
      str_detect(proxy_split, "commute") ~ "commute_time",
      str_detect(proxy_split, "income") ~ "income",
      TRUE ~ "housing_value"
    ),
    split_label = case_when(
      str_starts(proxy_split, "high_") ~ "High",
      str_starts(proxy_split, "low_") ~ "Low",
      str_starts(proxy_split, "short_") ~ "High",
      TRUE ~ "Low"
    )
  ) %>%
  select(borocd, proxy_name, split_label)

proxy_split_five_year_df <- supply_panel %>%
  inner_join(proxy_outcomes, by = "outcome_family") %>%
  left_join(proxy_lookup_long, by = "borocd", relationship = "many-to-many") %>%
  mutate(
    five_year_bin = case_when(
      year >= 2000 & year <= 2004 ~ "2000-2004",
      year >= 2005 & year <= 2009 ~ "2005-2009",
      year >= 2010 & year <= 2014 ~ "2010-2014",
      year >= 2015 & year <= 2019 ~ "2015-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(five_year_bin)) %>%
  group_by(outcome_family, plot_family, proxy_name, split_label, five_year_bin, borough_code, borough_name) %>%
  summarise(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = sum(distinct(data.frame(year, borough_outcome_total))$borough_outcome_total, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  group_by(outcome_family, plot_family, proxy_name, split_label, five_year_bin) %>%
  summarise(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_outcome_total = sum(borough_outcome_total, na.rm = TRUE),
    borough_outcome_share = ifelse(borough_outcome_total > 0, outcome_value / borough_outcome_total, NA_real_),
    borough_count = n_distinct(borough_code),
    .groups = "drop"
  ) %>%
  arrange(proxy_name, outcome_family, five_year_bin, split_label)

qc_df <- bind_rows(
  tibble(
    metric = "district_count",
    value = nrow(district_lookup),
    note = "Community districts available for tercile and proxy split assignment."
  ),
  tibble(
    metric = "borough_min_tercile_size",
    value = district_lookup %>%
      count(borough_code, treat_tercile) %>%
      summarise(value = min(n), .groups = "drop") %>%
      pull(value),
    note = "Minimum number of CDs in any within-borough treatment tercile."
  ),
  tibble(
    metric = "tercile_year_row_count",
    value = nrow(tercile_year_df),
    note = "Rows in the annual tercile borough-share summary."
  ),
  tibble(
    metric = "tercile_five_year_row_count",
    value = nrow(tercile_five_year_df),
    note = "Rows in the five-year tercile borough-share summary."
  ),
  tibble(
    metric = "proxy_split_five_year_row_count",
    value = nrow(proxy_split_five_year_df),
    note = "Rows in the five-year proxy-split borough-share summary."
  ),
  tibble(
    metric = "status",
    value = ifelse(
      nrow(district_lookup) == 59 &&
        nrow(tercile_year_df) > 0 &&
        nrow(proxy_split_five_year_df) > 0,
      1,
      0
    ),
    note = "One means the descriptive DCP housing-supply summaries were built successfully."
  )
)

tercile_plot_df <- tercile_five_year_df %>%
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    five_year_bin = factor(five_year_bin, levels = c("2000-2004", "2005-2009", "2010-2014", "2015-2019", "2020-2025"))
  )

proxy_plot_df <- proxy_split_five_year_df %>%
  filter(proxy_name == "subway_share") %>%
  mutate(
    split_label = factor(split_label, levels = c("Low", "High")),
    five_year_bin = factor(five_year_bin, levels = c("2000-2004", "2005-2009", "2010-2014", "2015-2019", "2020-2025"))
  )

tercile_plot <- ggplot(tercile_plot_df, aes(x = five_year_bin, y = borough_outcome_share, color = treat_tercile_label, group = treat_tercile_label)) +
  geom_line(linewidth = 0.7) +
  geom_point(size = 1.8) +
  facet_wrap(~ plot_family, scales = "free_y", ncol = 3) +
  scale_color_manual(values = c("Low" = "#3B6FB6", "Middle" = "#8A8A8A", "High" = "#B55239")) +
  labs(
    x = NULL,
    y = "Within-borough share",
    color = "1990 homeownership tercile"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    panel.grid.minor = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.position = "bottom"
  )

proxy_plot <- ggplot(proxy_plot_df, aes(x = five_year_bin, y = borough_outcome_share, color = split_label, group = split_label)) +
  geom_line(linewidth = 0.7) +
  geom_point(size = 1.8) +
  facet_wrap(~ plot_family, scales = "free_y", ncol = 3) +
  scale_color_manual(values = c("Low" = "#8A8A8A", "High" = "#2C7A4B")) +
  labs(
    x = NULL,
    y = "Within-borough share",
    color = "1990 subway-share split"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    panel.grid.minor = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.position = "bottom"
  )

pdf_tmp <- tempfile(fileext = ".pdf")
pdf(pdf_tmp, width = 11, height = 8.5)
print(tercile_plot)
print(proxy_plot)
dev.off()

write_csv_if_changed(tercile_year_df, out_tercile_year_csv)
write_csv_if_changed(tercile_five_year_df, out_tercile_five_year_csv)
write_csv_if_changed(proxy_split_five_year_df, out_proxy_split_five_year_csv)
write_csv_if_changed(qc_df, out_qc_csv)
copy_if_changed(pdf_tmp, out_plots_pdf)

cat("Wrote DCP housing-supply descriptive summaries to", dirname(out_tercile_year_csv), "\n")
