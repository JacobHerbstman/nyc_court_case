# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_zap_housing_hdb_link/code")
# zap_housing_hdb_project_summary_csv <- "../input/zap_housing_hdb_project_summary.csv"
# out_panel_csv <- "../output/zap_housing_hdb_cohort_panel.csv"
# out_era_summary_csv <- "../output/zap_housing_hdb_tercile_era_summary.csv"
# out_qc_csv <- "../output/zap_housing_hdb_summary_qc.csv"
# out_plots_pdf <- "../output/zap_housing_hdb_plots.pdf"

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tidyr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 5) {
  stop("Expected 5 arguments: zap_housing_hdb_project_summary_csv out_panel_csv out_era_summary_csv out_qc_csv out_plots_pdf")
}

zap_housing_hdb_project_summary_csv <- args[1]
out_panel_csv <- args[2]
out_era_summary_csv <- args[3]
out_qc_csv <- args[4]
out_plots_pdf <- args[5]

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

project_df <- read_csv(zap_housing_hdb_project_summary_csv, show_col_types = FALSE, na = c("", "NA"))

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

cohort_panel <- crossing(
  borocd = district_lookup$borocd,
  cert_year = 1976:2025
) %>%
  left_join(district_lookup, by = "borocd") %>%
  left_join(
    project_df %>%
      filter(cert_year >= 1976, cert_year <= 2025) %>%
      group_by(borocd, cert_year) %>%
      summarise(
        initial_apps = n(),
        linked_housing_projects_0_10 = sum(has_any_housing_job_0_10, na.rm = TRUE),
        linked_addition_projects_0_10 = sum(has_any_addition_job_0_10, na.rm = TRUE),
        linked_nb_projects_0_10 = sum(has_any_nb_job_0_10, na.rm = TRUE),
        linked_nb_50_plus_projects_0_10 = sum(has_any_nb_50_plus_job_0_10, na.rm = TRUE),
        linked_nb_gross_units_0_10 = sum(linked_nb_gross_units_0_10, na.rm = TRUE),
        linked_gross_add_units_0_10 = sum(linked_gross_add_units_0_10, na.rm = TRUE),
        linked_gross_loss_units_0_10 = sum(linked_gross_loss_units_0_10, na.rm = TRUE),
        linked_net_units_0_10 = sum(linked_net_units_0_10, na.rm = TRUE),
        .groups = "drop"
      ),
    by = c("borocd", "cert_year")
  ) %>%
  mutate(
    initial_apps = coalesce(initial_apps, 0L),
    linked_housing_projects_0_10 = coalesce(linked_housing_projects_0_10, 0L),
    linked_addition_projects_0_10 = coalesce(linked_addition_projects_0_10, 0L),
    linked_nb_projects_0_10 = coalesce(linked_nb_projects_0_10, 0L),
    linked_nb_50_plus_projects_0_10 = coalesce(linked_nb_50_plus_projects_0_10, 0L),
    linked_nb_gross_units_0_10 = coalesce(linked_nb_gross_units_0_10, 0),
    linked_gross_add_units_0_10 = coalesce(linked_gross_add_units_0_10, 0),
    linked_gross_loss_units_0_10 = coalesce(linked_gross_loss_units_0_10, 0),
    linked_net_units_0_10 = coalesce(linked_net_units_0_10, 0),
    linked_addition_rate_0_10 = ifelse(initial_apps > 0, linked_addition_projects_0_10 / initial_apps, NA_real_),
    linked_nb_50_plus_rate_0_10 = ifelse(initial_apps > 0, linked_nb_50_plus_projects_0_10 / initial_apps, NA_real_),
    linked_gross_add_units_per_app_0_10 = ifelse(initial_apps > 0, linked_gross_add_units_0_10 / initial_apps, NA_real_),
    cert_era_summary = summary_era_from_year(cert_year)
  ) %>%
  arrange(cert_year, borocd)

era_summary <- bind_rows(
  cohort_panel %>%
    group_by(cert_era_summary, treat_tercile, treat_tercile_label) %>%
    summarise(
      outcome_family = "linked_addition_rate_0_10",
      outcome_label = "Share of ZAP housing projects linking to an addition job within 0-10 years",
      numerator = sum(linked_addition_projects_0_10, na.rm = TRUE),
      denominator = sum(initial_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    ),
  cohort_panel %>%
    group_by(cert_era_summary, treat_tercile, treat_tercile_label) %>%
    summarise(
      outcome_family = "linked_nb_50_plus_rate_0_10",
      outcome_label = "Share of ZAP housing projects linking to a 50+ unit NB job within 0-10 years",
      numerator = sum(linked_nb_50_plus_projects_0_10, na.rm = TRUE),
      denominator = sum(initial_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    ),
  cohort_panel %>%
    group_by(cert_era_summary, treat_tercile, treat_tercile_label) %>%
    summarise(
      outcome_family = "linked_gross_add_units_per_app_0_10",
      outcome_label = "Linked gross addition units per ZAP housing project within 0-10 years",
      numerator = sum(linked_gross_add_units_0_10, na.rm = TRUE),
      denominator = sum(initial_apps, na.rm = TRUE),
      value = ifelse(denominator > 0, numerator / denominator, NA_real_),
      .groups = "drop"
    )
) %>%
  arrange(outcome_family, cert_era_summary, treat_tercile)

qc_df <- bind_rows(
  tibble(
    metric = "cohort_panel_row_count",
    value = nrow(cohort_panel),
    note = "Balanced 59 x 50 CD-year cohort panel."
  ),
  tibble(
    metric = "cohort_panel_expected_row_count",
    value = n_distinct(cohort_panel$borocd) * length(1976:2025),
    note = "Expected balanced row count for the linked cohort panel."
  ),
  tibble(
    metric = "linked_addition_projects_share_0_10",
    value = sum(cohort_panel$linked_addition_projects_0_10, na.rm = TRUE) / sum(cohort_panel$initial_apps, na.rm = TRUE),
    note = "Share of ZAP housing projects that link to any addition-producing housing job within 0-10 years."
  ),
  tibble(
    metric = "linked_nb_50_plus_projects_share_0_10",
    value = sum(cohort_panel$linked_nb_50_plus_projects_0_10, na.rm = TRUE) / sum(cohort_panel$initial_apps, na.rm = TRUE),
    note = "Share of ZAP housing projects that link to any 50+ unit new-building housing job within 0-10 years."
  ),
  tibble(
    metric = "mean_linked_gross_add_units_per_app_0_10",
    value = sum(cohort_panel$linked_gross_add_units_0_10, na.rm = TRUE) / sum(cohort_panel$initial_apps, na.rm = TRUE),
    note = "Average linked gross addition units per ZAP housing project within the 0-10 year window."
  )
)

plot_df <- era_summary %>%
  mutate(
    cert_era_summary = factor(
      cert_era_summary,
      levels = c("1976-1979", "1980-1984", "1985-1989", "1990-1999", "2000-2009", "2010-2015", "2016-2025")
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
    title = "Linked housing outcomes for ZAP housing projects"
  ) +
  theme_minimal(base_size = 11) +
  theme(legend.position = "bottom")

temp_pdf <- tempfile(fileext = ".pdf")
pdf(temp_pdf, width = 11, height = 8.5)
print(plot_obj)
dev.off()

source("../../_lib/source_pipeline_utils.R")
copy_if_changed(temp_pdf, out_plots_pdf)

write_csv_if_changed(cohort_panel, out_panel_csv)
write_csv_if_changed(era_summary, out_era_summary_csv)
write_csv_if_changed(qc_df, out_qc_csv)

cat("Wrote ZAP-HDB summary outputs to", dirname(out_panel_csv), "\n")
