suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tidyr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

static_control_cols <- c(
  "vacancy_rate_1990_exact",
  "structure_share_1_2_units_1990_exact",
  "structure_share_3_4_units_1990_exact",
  "structure_share_5_plus_units_1990_exact",
  "median_household_income_1990_1999_dollars_exact",
  "poverty_share_1990_exact",
  "median_housing_value_1990_2000_dollars_exact_filled",
  "median_housing_value_1990_missing_exact",
  "subway_commute_share_1990_exact",
  "mean_commute_time_1990_minutes_exact",
  "foreign_born_share_1990_exact",
  "college_graduate_share_1990_exact",
  "unemployment_rate_1990_exact"
)

application_era_from_year <- function(x) {
  case_when(
    x >= 1976 & x <= 1979 ~ "1976-1979",
    x >= 1980 & x <= 1984 ~ "1980-1984",
    x >= 1985 & x <= 1989 ~ "1985-1989",
    x >= 1990 & x <= 1999 ~ "1990-1999",
    x >= 2000 & x <= 2009 ~ "2000-2009",
    x >= 2010 & x <= 2019 ~ "2010-2019",
    x >= 2020 & x <= 2025 ~ "2020-2025",
    TRUE ~ NA_character_
  )
}

mature_era_from_year <- function(x) {
  case_when(
    x >= 1976 & x <= 1979 ~ "1976-1979",
    x >= 1980 & x <= 1984 ~ "1980-1984",
    x >= 1985 & x <= 1989 ~ "1985-1989",
    x >= 1990 & x <= 1999 ~ "1990-1999",
    x >= 2000 & x <= 2009 ~ "2000-2009",
    x >= 2010 & x <= 2015 ~ "2010-2015",
    TRUE ~ NA_character_
  )
}

assert_unique_keys <- function(df, keys, label) {
  duplicate_keys <- df %>%
    count(across(all_of(keys)), name = "n") %>%
    filter(n > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(label, " is not unique by ", paste(keys, collapse = ", "), ".")
  }
}

base_df <- read_csv("../input/zap_housing_cohort_base.csv", show_col_types = FALSE, na = c("", "NA"))

district_lookup <- base_df %>%
  distinct(
    borocd,
    borough_name,
    treat_pp,
    treat_z_boro,
    across(all_of(static_control_cols))
  ) %>%
  group_by(borough_name) %>%
  mutate(
    treat_tercile = ntile(treat_pp, 3),
    treat_tercile_label = case_when(
      treat_tercile == 1 ~ "Low",
      treat_tercile == 2 ~ "Middle",
      TRUE ~ "High"
    )
  ) %>%
  ungroup() %>%
  arrange(borocd)

assert_unique_keys(district_lookup, "borocd", "ZAP housing district lookup")

if (n_distinct(district_lookup$borocd) != 59) {
  stop("Expected the ZAP housing district lookup to cover 59 community districts.")
}

initial_counts <- base_df %>%
  filter(cert_year >= 1976, cert_year <= 2025) %>%
  count(borocd, cert_year, name = "initial_apps")

assert_unique_keys(initial_counts, c("borocd", "cert_year"), "ZAP housing initial counts")

initial_panel <- crossing(
  borocd = district_lookup$borocd,
  cert_year = 1976:2025
) %>%
  left_join(district_lookup, by = "borocd", relationship = "many-to-one") %>%
  left_join(initial_counts, by = c("borocd", "cert_year"), relationship = "many-to-one") %>%
  mutate(
    initial_apps = coalesce(initial_apps, 0L),
    cert_era_summary = application_era_from_year(cert_year)
  ) %>%
  group_by(borough_name, cert_year) %>%
  mutate(
    borough_initial_apps_total = sum(initial_apps, na.rm = TRUE),
    borough_initial_apps_share = ifelse(borough_initial_apps_total > 0, initial_apps / borough_initial_apps_total, NA_real_)
  ) %>%
  ungroup() %>%
  arrange(cert_year, borocd)

mature_counts <- base_df %>%
  filter(cert_year >= 1976, cert_year <= 2015) %>%
  group_by(borocd, cert_year) %>%
  summarise(
    initial_apps = n(),
    complete_apps = sum(is_complete, na.rm = TRUE),
    failed_apps = sum(is_fail, na.rm = TRUE),
    unresolved_apps = sum(is_unresolved, na.rm = TRUE),
    .groups = "drop"
  )

assert_unique_keys(mature_counts, c("borocd", "cert_year"), "ZAP housing mature counts")

mature_panel <- crossing(
  borocd = district_lookup$borocd,
  cert_year = 1976:2015
) %>%
  left_join(district_lookup, by = "borocd", relationship = "many-to-one") %>%
  left_join(mature_counts, by = c("borocd", "cert_year"), relationship = "many-to-one") %>%
  mutate(
    initial_apps = coalesce(initial_apps, 0L),
    complete_apps = coalesce(complete_apps, 0L),
    failed_apps = coalesce(failed_apps, 0L),
    unresolved_apps = coalesce(unresolved_apps, 0L),
    completion_share = ifelse(initial_apps > 0, complete_apps / initial_apps, NA_real_),
    failure_share = ifelse(initial_apps > 0, failed_apps / initial_apps, NA_real_),
    unresolved_share = ifelse(initial_apps > 0, unresolved_apps / initial_apps, NA_real_),
    cert_era_summary = mature_era_from_year(cert_year)
  ) %>%
  arrange(cert_year, borocd)

if (any(mature_panel$cert_year > 2015, na.rm = TRUE)) {
  stop("Mature ZAP housing cohort panel includes certification years after 2015.")
}

if (any(mature_panel$cert_era_summary %in% c("2010-2019", "2020-2025", "2016-2025"), na.rm = TRUE)) {
  stop("Mature ZAP housing cohort panel includes immature post-2015 era labels.")
}

initial_year_mean_df <- initial_panel %>%
  group_by(cert_year, treat_tercile, treat_tercile_label) %>%
  summarise(
    outcome_family = "initial_apps_mean",
    outcome_label = "Mean initial housing ULURP applications per CD",
    numerator = sum(initial_apps, na.rm = TRUE),
    denominator = n(),
    value = mean(initial_apps, na.rm = TRUE),
    .groups = "drop"
  )

initial_year_share_df <- initial_panel %>%
  group_by(cert_year, borough_name, treat_tercile, treat_tercile_label) %>%
  summarise(
    tercile_initial_apps = sum(initial_apps, na.rm = TRUE),
    borough_initial_apps_total = first(borough_initial_apps_total),
    .groups = "drop"
  ) %>%
  group_by(cert_year, treat_tercile, treat_tercile_label) %>%
  summarise(
    outcome_family = "initial_apps_borough_share",
    outcome_label = "Within-borough share of initial housing ULURP applications",
    numerator = sum(tercile_initial_apps, na.rm = TRUE),
    denominator = sum(borough_initial_apps_total, na.rm = TRUE),
    value = ifelse(denominator > 0, numerator / denominator, NA_real_),
    .groups = "drop"
  )

mature_year_completion_df <- mature_panel %>%
  group_by(cert_year, treat_tercile, treat_tercile_label) %>%
  summarise(
    outcome_family = "completion_share",
    outcome_label = "Completion share among mature housing ULURP cohorts",
    numerator = sum(complete_apps, na.rm = TRUE),
    denominator = sum(initial_apps, na.rm = TRUE),
    value = ifelse(denominator > 0, numerator / denominator, NA_real_),
    .groups = "drop"
  )

mature_year_failure_df <- mature_panel %>%
  group_by(cert_year, treat_tercile, treat_tercile_label) %>%
  summarise(
    outcome_family = "failure_share",
    outcome_label = "Failure share among mature housing ULURP cohorts",
    numerator = sum(failed_apps, na.rm = TRUE),
    denominator = sum(initial_apps, na.rm = TRUE),
    value = ifelse(denominator > 0, numerator / denominator, NA_real_),
    .groups = "drop"
  )

year_summary <- bind_rows(
  initial_year_mean_df,
  initial_year_share_df,
  mature_year_completion_df,
  mature_year_failure_df
) %>%
  arrange(outcome_family, cert_year, treat_tercile)

initial_era_mean_df <- initial_panel %>%
  group_by(cert_era_summary, treat_tercile, treat_tercile_label) %>%
  summarise(
    outcome_family = "initial_apps_mean",
    outcome_label = "Mean initial housing ULURP applications per CD",
    numerator = sum(initial_apps, na.rm = TRUE),
    denominator = n(),
    value = mean(initial_apps, na.rm = TRUE),
    .groups = "drop"
  )

initial_era_share_df <- initial_panel %>%
  group_by(cert_era_summary, borough_name, treat_tercile, treat_tercile_label) %>%
  summarise(
    tercile_initial_apps = sum(initial_apps, na.rm = TRUE),
    borough_initial_apps_total = sum(unique(data.frame(cert_year, borough_initial_apps_total))$borough_initial_apps_total, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  group_by(cert_era_summary, treat_tercile, treat_tercile_label) %>%
  summarise(
    outcome_family = "initial_apps_borough_share",
    outcome_label = "Within-borough share of initial housing ULURP applications",
    numerator = sum(tercile_initial_apps, na.rm = TRUE),
    denominator = sum(borough_initial_apps_total, na.rm = TRUE),
    value = ifelse(denominator > 0, numerator / denominator, NA_real_),
    .groups = "drop"
  )

mature_era_completion_df <- mature_panel %>%
  group_by(cert_era_summary, treat_tercile, treat_tercile_label) %>%
  summarise(
    outcome_family = "completion_share",
    outcome_label = "Completion share among mature housing ULURP cohorts",
    numerator = sum(complete_apps, na.rm = TRUE),
    denominator = sum(initial_apps, na.rm = TRUE),
    value = ifelse(denominator > 0, numerator / denominator, NA_real_),
    .groups = "drop"
  )

mature_era_failure_df <- mature_panel %>%
  group_by(cert_era_summary, treat_tercile, treat_tercile_label) %>%
  summarise(
    outcome_family = "failure_share",
    outcome_label = "Failure share among mature housing ULURP cohorts",
    numerator = sum(failed_apps, na.rm = TRUE),
    denominator = sum(initial_apps, na.rm = TRUE),
    value = ifelse(denominator > 0, numerator / denominator, NA_real_),
    .groups = "drop"
  )

era_summary <- bind_rows(
  initial_era_mean_df,
  initial_era_share_df,
  mature_era_completion_df,
  mature_era_failure_df
) %>%
  arrange(outcome_family, cert_era_summary, treat_tercile)

plot_year_df <- year_summary %>%
  filter(outcome_family %in% c("initial_apps_borough_share", "completion_share", "failure_share")) %>%
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High"))
  )

plot_era_df <- era_summary %>%
  filter(outcome_family %in% c("initial_apps_borough_share", "completion_share", "failure_share")) %>%
  mutate(
    cert_era_summary = factor(
      cert_era_summary,
      levels = c("1976-1979", "1980-1984", "1985-1989", "1990-1999", "2000-2009", "2010-2015", "2010-2019", "2020-2025")
    ),
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High"))
  )

year_plot <- ggplot(
  plot_year_df,
  aes(x = cert_year, y = value, color = treat_tercile_label)
) +
  geom_line(linewidth = 0.7, na.rm = TRUE) +
  facet_wrap(~ outcome_label, scales = "free_y", ncol = 1) +
  scale_color_manual(values = c("Low" = "#1b9e77", "Middle" = "#7570b3", "High" = "#d95f02")) +
  labs(
    x = "Certification year",
    y = NULL,
    color = "Treatment tercile",
    title = "Housing-oriented ULURP outcomes by treatment tercile"
  ) +
  theme_minimal(base_size = 11) +
  theme(legend.position = "bottom")

era_plot <- ggplot(
  plot_era_df,
  aes(x = cert_era_summary, y = value, fill = treat_tercile_label)
) +
  geom_col(position = "dodge", na.rm = TRUE) +
  facet_wrap(~ outcome_label, scales = "free_y", ncol = 1) +
  scale_fill_manual(values = c("Low" = "#1b9e77", "Middle" = "#7570b3", "High" = "#d95f02")) +
  labs(
    x = "Certification era",
    y = NULL,
    fill = "Treatment tercile",
    title = "Housing-oriented ULURP outcomes by treatment tercile and era"
  ) +
  theme_minimal(base_size = 11) +
  theme(legend.position = "bottom")

temp_pdf <- tempfile(fileext = ".pdf")
pdf(temp_pdf, width = 11, height = 8.5)
print(year_plot)
print(era_plot)
dev.off()
copy_if_changed(temp_pdf, "../output/zap_housing_plots.pdf")

write_csv_if_changed(initial_panel, "../output/zap_housing_initial_panel.csv")
write_csv_if_changed(mature_panel, "../output/zap_housing_mature_cohort_panel.csv")
write_csv_if_changed(year_summary, "../output/zap_housing_tercile_year_summary.csv")
write_csv_if_changed(era_summary, "../output/zap_housing_tercile_era_summary.csv")

cat("Wrote ZAP housing cohort summary outputs to ../output\n")
