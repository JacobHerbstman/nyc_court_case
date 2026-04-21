# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_cd_homeownership_proxy_overlap_validation/code")
# cd_homeownership_1990_measure_csv <- "../input/cd_homeownership_1990_measure.csv"
# mappluto_construction_proxy_cd_year_csv <- "../input/mappluto_construction_proxy_cd_year.csv"
# dcp_housing_database_project_level_parquet <- "../input/dcp_housing_database_project_level_25q4.parquet"
# out_tercile_year_csv <- "../output/cd_homeownership_proxy_overlap_tercile_year.csv"
# out_borough_year_csv <- "../output/cd_homeownership_proxy_overlap_borough_year.csv"
# out_residual_csv <- "../output/cd_homeownership_proxy_overlap_cd_year_residual.csv"
# out_metrics_csv <- "../output/cd_homeownership_proxy_overlap_metrics.csv"
# out_qc_csv <- "../output/cd_homeownership_proxy_overlap_qc.csv"
# out_plots_pdf <- "../output/cd_homeownership_proxy_overlap_plots.pdf"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tidyr)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 9) {
  stop("Expected 9 arguments: cd_homeownership_1990_measure_csv mappluto_construction_proxy_cd_year_csv dcp_housing_database_project_level_parquet out_tercile_year_csv out_borough_year_csv out_residual_csv out_metrics_csv out_qc_csv out_plots_pdf")
}

cd_homeownership_1990_measure_csv <- args[1]
mappluto_construction_proxy_cd_year_csv <- args[2]
dcp_housing_database_project_level_parquet <- args[3]
out_tercile_year_csv <- args[4]
out_borough_year_csv <- args[5]
out_residual_csv <- args[6]
out_metrics_csv <- args[7]
out_qc_csv <- args[8]
out_plots_pdf <- args[9]

metric_value <- function(df, family, metric_name) {
  value <- df |>
    filter(outcome_family == family, metric == metric_name) |>
    pull(value)

  if (length(value) == 0) {
    return(NA_real_)
  }

  value[[1]]
}

treatment_df <- read_csv(cd_homeownership_1990_measure_csv, show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    treat_pp = suppressWarnings(as.numeric(treat_pp))
  ) |>
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

proxy_long <- read_csv(mappluto_construction_proxy_cd_year_csv, show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(yearbuilt)),
    total_nb_units = suppressWarnings(as.numeric(residential_units_proxy)),
    units_50_plus = suppressWarnings(as.numeric(units_50_plus_proxy)),
    projects_50_plus = suppressWarnings(as.numeric(lots_50_plus_proxy))
  ) |>
  filter(year >= 2010, year <= 2025) |>
  pivot_longer(
    cols = c(total_nb_units, units_50_plus, projects_50_plus),
    names_to = "outcome_family",
    values_to = "outcome_value"
  ) |>
  mutate(source = "proxy")

observed_source <- read_parquet(
  dcp_housing_database_project_level_parquet,
  col_select = c("completion_year", "community_district", "borough_code", "borough_name", "job_type", "classa_prop")
) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(community_district))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(completion_year)),
    job_type = as.character(job_type),
    classa_prop = suppressWarnings(as.numeric(classa_prop))
  ) |>
  filter(
    borocd %in% treatment_df$borocd,
    year >= 2010,
    year <= 2025,
    job_type == "New Building",
    !is.na(classa_prop),
    classa_prop > 0
  ) |>
  group_by(borocd, borough_code, borough_name, year) |>
  summarize(
    total_nb_units = sum(classa_prop, na.rm = TRUE),
    units_50_plus = sum(classa_prop[classa_prop >= 50], na.rm = TRUE),
    projects_50_plus = sum(classa_prop >= 50, na.rm = TRUE),
    .groups = "drop"
  ) |>
  pivot_longer(
    cols = c(total_nb_units, units_50_plus, projects_50_plus),
    names_to = "outcome_family",
    values_to = "outcome_value"
  ) |>
  mutate(source = "observed")

balanced_df <- expand_grid(
  treatment_df |>
    select(borocd, borough_code, borough_name, treat_tercile, treat_tercile_label),
  year = 2010:2025,
  outcome_family = c("total_nb_units", "units_50_plus", "projects_50_plus"),
  source = c("proxy", "observed")
) |>
  left_join(bind_rows(proxy_long, observed_source), by = c("borocd", "borough_code", "borough_name", "year", "outcome_family", "source")) |>
  mutate(outcome_value = coalesce(outcome_value, 0))

tercile_borough_source <- balanced_df |>
  group_by(source, outcome_family, borough_code, borough_name, year, treat_tercile, treat_tercile_label) |>
  summarize(outcome_value = sum(outcome_value, na.rm = TRUE), .groups = "drop") |>
  group_by(source, outcome_family, borough_code, borough_name, year) |>
  mutate(
    borough_total = sum(outcome_value, na.rm = TRUE),
    borough_share = if_else(borough_total > 0, outcome_value / borough_total, NA_real_)
  ) |>
  ungroup()

tercile_year_df <- tercile_borough_source |>
  group_by(source, outcome_family, year, treat_tercile, treat_tercile_label) |>
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_total = sum(distinct(data.frame(borough_code, borough_name, borough_total))$borough_total, na.rm = TRUE),
    borough_share = if_else(borough_total > 0, outcome_value / borough_total, NA_real_),
    .groups = "drop"
  ) |>
  arrange(outcome_family, year, source, treat_tercile)

borough_year_compare_df <- tercile_borough_source |>
  select(source, outcome_family, borough_code, borough_name, year, treat_tercile, treat_tercile_label, outcome_value, borough_total, borough_share) |>
  pivot_wider(
    names_from = source,
    values_from = c(outcome_value, borough_total, borough_share),
    names_sep = "_"
  ) |>
  rename(
    proxy_value = outcome_value_proxy,
    observed_value = outcome_value_observed,
    proxy_borough_total = borough_total_proxy,
    observed_borough_total = borough_total_observed,
    proxy_share = borough_share_proxy,
    observed_share = borough_share_observed
  ) |>
  arrange(outcome_family, borough_code, year, treat_tercile)

cd_year_residual_df <- balanced_df |>
  group_by(source, outcome_family, borough_code, borough_name, year) |>
  mutate(
    borough_mean = mean(outcome_value, na.rm = TRUE),
    outcome_residual = outcome_value - borough_mean
  ) |>
  ungroup() |>
  select(source, outcome_family, borocd, borough_code, borough_name, year, treat_tercile, treat_tercile_label, outcome_value, outcome_residual) |>
  pivot_wider(
    names_from = source,
    values_from = c(outcome_value, outcome_residual),
    names_sep = "_"
  ) |>
  rename(
    proxy_value = outcome_value_proxy,
    observed_value = outcome_value_observed,
    proxy_residual = outcome_residual_proxy,
    observed_residual = outcome_residual_observed
  ) |>
  arrange(outcome_family, borocd, year)

write_csv(tercile_year_df, out_tercile_year_csv, na = "")
write_csv(borough_year_compare_df, out_borough_year_csv, na = "")
write_csv(cd_year_residual_df, out_residual_csv, na = "")

base_metrics_df <- bind_rows(
  tercile_year_df |>
    select(source, outcome_family, year, treat_tercile_label, borough_share) |>
    pivot_wider(names_from = source, values_from = borough_share) |>
    group_by(outcome_family) |>
    summarize(
      metric = "city_year_tercile_share_corr",
      value = cor(proxy, observed, use = "complete.obs"),
      .groups = "drop"
    ),
  tercile_year_df |>
    select(source, outcome_family, year, treat_tercile_label, borough_share) |>
    pivot_wider(names_from = source, values_from = borough_share) |>
    group_by(outcome_family) |>
    summarize(
      metric = "city_year_tercile_share_mae",
      value = mean(abs(proxy - observed), na.rm = TRUE),
      .groups = "drop"
    ),
  borough_year_compare_df |>
    group_by(outcome_family) |>
    summarize(
      metric = "borough_year_tercile_share_corr",
      value = cor(proxy_share, observed_share, use = "complete.obs"),
      .groups = "drop"
    ),
  borough_year_compare_df |>
    group_by(outcome_family) |>
    summarize(
      metric = "borough_year_tercile_share_mae",
      value = mean(abs(proxy_share - observed_share), na.rm = TRUE),
      .groups = "drop"
    ),
  cd_year_residual_df |>
    group_by(outcome_family) |>
    summarize(
      metric = "cd_year_residual_corr",
      value = cor(proxy_residual, observed_residual, use = "complete.obs"),
      .groups = "drop"
    ),
  cd_year_residual_df |>
    group_by(outcome_family) |>
    summarize(
      metric = "cd_year_residual_mae",
      value = mean(abs(proxy_residual - observed_residual), na.rm = TRUE),
      .groups = "drop"
    )
) 

metrics_df <- bind_rows(
  base_metrics_df,
  tibble(
    outcome_family = c("total_nb_units", "units_50_plus", "projects_50_plus"),
    metric = "comfort_threshold_pass",
    value = c(
      as.numeric(metric_value(base_metrics_df, "total_nb_units", "borough_year_tercile_share_corr") >= 0.75),
      as.numeric(metric_value(base_metrics_df, "units_50_plus", "borough_year_tercile_share_corr") >= 0.65),
      NA_real_
    )
  )
) |>
  arrange(outcome_family, metric)

write_csv(metrics_df, out_metrics_csv, na = "")

qc_df <- bind_rows(
  tibble(metric = "district_count", value = n_distinct(balanced_df$borocd), note = "Standard CDs in the overlap validation sample."),
  tibble(metric = "year_min", value = min(balanced_df$year, na.rm = TRUE), note = "Minimum overlap year."),
  tibble(metric = "year_max", value = max(balanced_df$year, na.rm = TRUE), note = "Maximum overlap year."),
  tibble(metric = "tercile_share_sum_min", value = min(tercile_borough_source |>
    group_by(source, outcome_family, borough_code, borough_name, year) |>
    summarize(total_share = sum(borough_share, na.rm = TRUE), .groups = "drop") |>
    pull(total_share), na.rm = TRUE), note = "Minimum borough-year tercile-share sum; should equal 1 when totals are positive."),
  tibble(metric = "tercile_share_sum_max", value = max(tercile_borough_source |>
    group_by(source, outcome_family, borough_code, borough_name, year) |>
    summarize(total_share = sum(borough_share, na.rm = TRUE), .groups = "drop") |>
    pull(total_share), na.rm = TRUE), note = "Maximum borough-year tercile-share sum; should equal 1 when totals are positive.")
)

write_csv(qc_df, out_qc_csv, na = "")

share_plot_df <- tercile_year_df |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    source_label = case_when(
      source == "proxy" ~ "MapPLUTO proxy",
      TRUE ~ "Observed DCP HDB"
    ),
    outcome_label = case_when(
      outcome_family == "total_nb_units" ~ "Total new-building units",
      outcome_family == "units_50_plus" ~ "50+ new-building units",
      TRUE ~ "50+ new-building project counts"
    )
  )

share_scatter_df <- borough_year_compare_df |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    outcome_label = case_when(
      outcome_family == "total_nb_units" ~ "Total new-building units",
      outcome_family == "units_50_plus" ~ "50+ new-building units",
      TRUE ~ "50+ new-building project counts"
    )
  )

residual_plot_df <- cd_year_residual_df |>
  mutate(
    outcome_label = case_when(
      outcome_family == "total_nb_units" ~ "Total new-building units",
      outcome_family == "units_50_plus" ~ "50+ new-building units",
      TRUE ~ "50+ new-building project counts"
    )
  )

pdf(out_plots_pdf, width = 11, height = 8.5)
print(
  ggplot(share_plot_df, aes(x = year, y = borough_share, color = treat_tercile_label, linetype = source_label)) +
    geom_line(linewidth = 0.75) +
    facet_wrap(~outcome_label, ncol = 1, scales = "free_y") +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = "Within-borough share", color = "Treat tercile", linetype = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(share_scatter_df, aes(x = observed_share, y = proxy_share, color = treat_tercile_label)) +
    geom_point(alpha = 0.7, size = 1.8) +
    geom_abline(intercept = 0, slope = 1, linetype = "dashed", color = "#666666") +
    facet_grid(treat_tercile_label ~ outcome_label, scales = "free") +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = "Observed share", y = "Proxy share", color = "Treat tercile") +
    theme_minimal(base_size = 10) +
    theme(legend.position = "none")
)
print(
  ggplot(residual_plot_df, aes(x = observed_residual, y = proxy_residual)) +
    geom_point(alpha = 0.3, size = 1.1, color = "#1b6ca8") +
    geom_abline(intercept = 0, slope = 1, linetype = "dashed", color = "#666666") +
    facet_wrap(~outcome_label, scales = "free") +
    labs(x = "Observed within-borough residual", y = "Proxy within-borough residual") +
    theme_minimal(base_size = 11)
)
dev.off()

cat("Wrote proxy overlap validation outputs to", dirname(out_tercile_year_csv), "\n")
