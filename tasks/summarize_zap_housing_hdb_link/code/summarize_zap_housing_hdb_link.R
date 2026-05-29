# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_zap_housing_hdb_link/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tidyr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

summary_era_from_year <- function(x) {
  case_when(
    x >= 1976 & x <= 1979 ~ "1976-1979",
    x >= 1980 & x <= 1984 ~ "1980-1984",
    x >= 1985 & x <= 1989 ~ "1985-1989",
    x >= 1990 & x <= 1999 ~ "1990-1999",
    x >= 2000 & x <= 2009 ~ "2000-2009",
    x >= 2010 & x <= 2015 ~ "2010-2015",
    x >= 2016 & x <= 2025 ~ "2016-2025",
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

project_df <- read_csv("../input/zap_housing_hdb_project_summary.csv", show_col_types = FALSE, na = c("", "NA"))

if (!"bbl_linkable" %in% names(project_df)) {
  project_df <- project_df %>%
    mutate(bbl_linkable = has_bbl %in% TRUE)
}

linked_0_10_cols <- c(
  "linked_housing_projects_0_10",
  "linked_addition_projects_0_10",
  "linked_nb_projects_0_10",
  "linked_nb_50_plus_projects_0_10",
  "linked_nb_gross_units_0_10",
  "linked_gross_add_units_0_10",
  "linked_gross_loss_units_0_10",
  "linked_net_units_0_10"
)

district_lookup <- project_df %>%
  distinct(borocd, borough_name, treat_pp) %>%
  group_by(borough_name) %>%
  mutate(
    treat_tercile = ntile(treat_pp, 3),
    treat_tercile_label = case_when(
      treat_tercile == 1 ~ "Low",
      treat_tercile == 2 ~ "Middle",
      TRUE ~ "High"
    )
  ) %>%
  ungroup()

assert_unique_keys(district_lookup, "borocd", "ZAP-HDB district lookup")

if (n_distinct(district_lookup$borocd) != 59) {
  stop("Expected the ZAP-HDB district lookup to cover 59 community districts.")
}

cohort_counts <- project_df %>%
  filter(cert_year >= 1976, cert_year <= 2025) %>%
  group_by(borocd, cert_year) %>%
  summarise(
    initial_apps = n(),
    linkable_apps = sum(bbl_linkable %in% TRUE),
    no_valid_bbl_apps = sum(!(bbl_linkable %in% TRUE)),
    linked_housing_projects_0_10 = sum(has_any_housing_job_0_10[bbl_linkable %in% TRUE], na.rm = TRUE),
    linked_addition_projects_0_10 = sum(has_any_addition_job_0_10[bbl_linkable %in% TRUE], na.rm = TRUE),
    linked_nb_projects_0_10 = sum(has_any_nb_job_0_10[bbl_linkable %in% TRUE], na.rm = TRUE),
    linked_nb_50_plus_projects_0_10 = sum(has_any_nb_50_plus_job_0_10[bbl_linkable %in% TRUE], na.rm = TRUE),
    linked_nb_gross_units_0_10 = sum(linked_nb_gross_units_0_10[bbl_linkable %in% TRUE], na.rm = TRUE),
    linked_gross_add_units_0_10 = sum(linked_gross_add_units_0_10[bbl_linkable %in% TRUE], na.rm = TRUE),
    linked_gross_loss_units_0_10 = sum(linked_gross_loss_units_0_10[bbl_linkable %in% TRUE], na.rm = TRUE),
    linked_net_units_0_10 = sum(linked_net_units_0_10[bbl_linkable %in% TRUE], na.rm = TRUE),
    .groups = "drop"
  )

assert_unique_keys(cohort_counts, c("borocd", "cert_year"), "ZAP-HDB cohort counts")

cohort_panel <- crossing(
  borocd = district_lookup$borocd,
  cert_year = 1976:2025
) %>%
  left_join(district_lookup, by = "borocd", relationship = "many-to-one") %>%
  left_join(cohort_counts, by = c("borocd", "cert_year"), relationship = "many-to-one") %>%
  mutate(
    initial_apps = coalesce(initial_apps, 0L),
    linkable_apps = coalesce(linkable_apps, 0L),
    no_valid_bbl_apps = coalesce(no_valid_bbl_apps, 0L),
    linked_housing_projects_0_10 = coalesce(linked_housing_projects_0_10, 0L),
    linked_addition_projects_0_10 = coalesce(linked_addition_projects_0_10, 0L),
    linked_nb_projects_0_10 = coalesce(linked_nb_projects_0_10, 0L),
    linked_nb_50_plus_projects_0_10 = coalesce(linked_nb_50_plus_projects_0_10, 0L),
    linked_nb_gross_units_0_10 = coalesce(linked_nb_gross_units_0_10, 0),
    linked_gross_add_units_0_10 = coalesce(linked_gross_add_units_0_10, 0),
    linked_gross_loss_units_0_10 = coalesce(linked_gross_loss_units_0_10, 0),
    linked_net_units_0_10 = coalesce(linked_net_units_0_10, 0),
    observable_0_10_window = cert_year >= 2010 & cert_year <= 2015,
    across(all_of(linked_0_10_cols), ~ ifelse(observable_0_10_window, .x, NA_real_)),
    linked_addition_rate_0_10 = ifelse(observable_0_10_window & linkable_apps > 0, linked_addition_projects_0_10 / linkable_apps, NA_real_),
    linked_nb_50_plus_rate_0_10 = ifelse(observable_0_10_window & linkable_apps > 0, linked_nb_50_plus_projects_0_10 / linkable_apps, NA_real_),
    linked_gross_add_units_per_app_0_10 = ifelse(observable_0_10_window & linkable_apps > 0, linked_gross_add_units_0_10 / linkable_apps, NA_real_),
    cert_era_summary = summary_era_from_year(cert_year)
  ) %>%
  arrange(cert_year, borocd)

cohort_panel_mature_0_10 <- cohort_panel %>%
  filter(observable_0_10_window)

era_summary <- bind_rows(
  cohort_panel_mature_0_10 %>%
    group_by(cert_era_summary, treat_tercile, treat_tercile_label) %>%
    summarise(
      outcome_family = "linked_addition_rate_0_10",
      outcome_label = "Share of linkable ZAP housing projects linking to an addition job within 0-10 years",
      numerator = sum(linked_addition_projects_0_10, na.rm = TRUE),
      denominator = sum(linkable_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    ),
  cohort_panel_mature_0_10 %>%
    group_by(cert_era_summary, treat_tercile, treat_tercile_label) %>%
    summarise(
      outcome_family = "linked_nb_50_plus_rate_0_10",
      outcome_label = "Share of linkable ZAP housing projects linking to a 50+ unit NB job within 0-10 years",
      numerator = sum(linked_nb_50_plus_projects_0_10, na.rm = TRUE),
      denominator = sum(linkable_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    ),
  cohort_panel_mature_0_10 %>%
    group_by(cert_era_summary, treat_tercile, treat_tercile_label) %>%
    summarise(
      outcome_family = "linked_gross_add_units_per_app_0_10",
      outcome_label = "Linked gross addition units per linkable ZAP housing project within 0-10 years",
      numerator = sum(linked_gross_add_units_0_10, na.rm = TRUE),
      denominator = sum(linkable_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    )
) %>%
  arrange(outcome_family, cert_era_summary, treat_tercile)

plot_df <- era_summary %>%
  mutate(
    cert_era_summary = factor(
      cert_era_summary,
      levels = c("2010-2015")
    ),
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High"))
  )

plot_obj <- ggplot(
  plot_df,
  aes(x = cert_era_summary, y = value, fill = treat_tercile_label)
) +
  geom_col(position = "dodge", na.rm = TRUE) +
  facet_wrap(~ outcome_label, scales = "free_y", ncol = 1) +
  scale_fill_manual(values = c("Low" = "#1b9e77", "Middle" = "#7570b3", "High" = "#d95f02")) +
  labs(
    x = "Certification era",
    y = NULL,
    fill = "Treatment tercile",
    title = "Linked housing outcomes for ZAP housing projects",
    subtitle = "0-10 year outcomes are shown only for 2010-2015 certification cohorts with valid BBL support"
  ) +
  theme_minimal(base_size = 11) +
  theme(legend.position = "bottom")

temp_pdf <- tempfile(fileext = ".pdf")
pdf(temp_pdf, width = 11, height = 8.5)
print(plot_obj)
dev.off()

copy_if_changed(temp_pdf, "../output/zap_housing_hdb_plots.pdf")

write_csv_if_changed(cohort_panel, "../output/zap_housing_hdb_cohort_panel.csv")
write_csv_if_changed(era_summary, "../output/zap_housing_hdb_tercile_era_summary.csv")

cat("Wrote ZAP-HDB summary outputs to ../output\n")
