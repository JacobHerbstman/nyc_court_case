# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_cd_homeownership_long_units_sensitivity/code")
# cd_homeownership_long_units_series_csv <- "../input/cd_homeownership_long_units_series.csv"
# mappluto_construction_proxy_cd_year_csv <- "../input/mappluto_construction_proxy_cd_year.csv"
# dcp_housing_database_project_level_parquet <- "../input/dcp_housing_database_project_level_25q4.parquet"
# out_pooled_csv <- "../output/cd_homeownership_long_units_sensitivity_pooled.csv"
# out_borough_csv <- "../output/cd_homeownership_long_units_sensitivity_borough.csv"
# out_leave_one_out_csv <- "../output/cd_homeownership_long_units_sensitivity_leave_one_out.csv"
# out_top5_out_csv <- "../output/cd_homeownership_long_units_sensitivity_top5_out.csv"
# out_levels_csv <- "../output/cd_homeownership_long_units_sensitivity_levels.csv"
# out_extensive_csv <- "../output/cd_homeownership_long_units_sensitivity_extensive.csv"
# out_top5_csv <- "../output/cd_homeownership_long_units_sensitivity_top5_cd_year.csv"
# out_summary_csv <- "../output/cd_homeownership_long_units_sensitivity_summary.csv"
# out_qc_csv <- "../output/cd_homeownership_long_units_sensitivity_qc.csv"
# out_plots_pdf <- "../output/cd_homeownership_long_units_sensitivity_plots.pdf"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tidyr)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 13) {
  stop("Expected 13 arguments: cd_homeownership_long_units_series_csv mappluto_construction_proxy_cd_year_csv dcp_housing_database_project_level_parquet out_pooled_csv out_borough_csv out_leave_one_out_csv out_top5_out_csv out_levels_csv out_extensive_csv out_top5_csv out_summary_csv out_qc_csv out_plots_pdf")
}

cd_homeownership_long_units_series_csv <- args[1]
mappluto_construction_proxy_cd_year_csv <- args[2]
dcp_housing_database_project_level_parquet <- args[3]
out_pooled_csv <- args[4]
out_borough_csv <- args[5]
out_leave_one_out_csv <- args[6]
out_top5_out_csv <- args[7]
out_levels_csv <- args[8]
out_extensive_csv <- args[9]
out_top5_csv <- args[10]
out_summary_csv <- args[11]
out_qc_csv <- args[12]
out_plots_pdf <- args[13]

compute_share_scenario <- function(df, scenario_type, scenario_name) {
  borough_level <- df |>
    group_by(series_family, year, borough_code, borough_name, treat_tercile, treat_tercile_label) |>
    summarize(outcome_value = sum(outcome_value, na.rm = TRUE), .groups = "drop") |>
    group_by(series_family, year, borough_code, borough_name) |>
    mutate(
      borough_total = sum(outcome_value, na.rm = TRUE),
      borough_share = if_else(borough_total > 0, outcome_value / borough_total, NA_real_)
    ) |>
    ungroup()

  borough_level |>
    group_by(series_family, year, treat_tercile, treat_tercile_label) |>
    summarize(
      outcome_value = sum(outcome_value, na.rm = TRUE),
      borough_total = sum(distinct(data.frame(borough_code, borough_name, borough_total))$borough_total, na.rm = TRUE),
      borough_share = if_else(borough_total > 0, outcome_value / borough_total, NA_real_),
      scenario_type = scenario_type,
      scenario_name = scenario_name,
      .groups = "drop"
    )
}

compute_level_scenario <- function(units_df, project_df, scenario_type, scenario_name) {
  units_rates <- units_df |>
    group_by(series_family, year, treat_tercile, treat_tercile_label) |>
    summarize(
      outcome_value = sum(outcome_value, na.rm = TRUE),
      occupied_units_1990 = sum(distinct(data.frame(borocd, occupied_units_1990))$occupied_units_1990, na.rm = TRUE),
      cd_count = n_distinct(borocd),
      .groups = "drop"
    ) |>
    mutate(
      metric = case_when(
        series_family == "units_built_total" ~ "units_built_total_per_10000_occupied_1990",
        TRUE ~ "units_built_50_plus_per_10000_occupied_1990"
      ),
      metric_value = if_else(occupied_units_1990 > 0, 10000 * outcome_value / occupied_units_1990, NA_real_),
      scenario_type = scenario_type,
      scenario_name = scenario_name
    ) |>
    select(scenario_type, scenario_name, year, treat_tercile, treat_tercile_label, metric, metric_value)

  project_levels <- project_df |>
    group_by(year, treat_tercile, treat_tercile_label) |>
    summarize(
      cd_count = n_distinct(borocd),
      project_count = sum(project_count_50_plus, na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(
      metric = "projects_50_plus_per_cd_year",
      metric_value = if_else(cd_count > 0, project_count / cd_count, NA_real_),
      scenario_type = scenario_type,
      scenario_name = scenario_name
    ) |>
    select(scenario_type, scenario_name, year, treat_tercile, treat_tercile_label, metric, metric_value)

  bind_rows(units_rates, project_levels)
}

series_df <- read_csv(cd_homeownership_long_units_series_csv, show_col_types = FALSE, na = c("", "NA")) |>
  filter(
    series_kind == "preferred_long_series",
    series_family %in% c("units_built_total", "units_built_50_plus")
  ) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(year)),
    series_family = series_family,
    outcome_value = suppressWarnings(as.numeric(outcome_value)),
    treat_tercile = suppressWarnings(as.integer(ntile(treat_pp, 3))),
    treat_tercile_label = case_when(
      ntile(treat_pp, 3) == 1 ~ "Low",
      ntile(treat_pp, 3) == 2 ~ "Middle",
      TRUE ~ "High"
    ),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990))
  )

district_lookup <- read_csv(cd_homeownership_long_units_series_csv, show_col_types = FALSE, na = c("", "NA")) |>
  distinct(borocd, borough_code, borough_name, treat_pp, occupied_units_1990) |>
  mutate(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code))
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

series_df <- series_df |>
  select(-treat_tercile, -treat_tercile_label) |>
  left_join(
    district_lookup |>
      select(borocd, borough_code, borough_name, treat_tercile, treat_tercile_label, occupied_units_1990),
    by = c("borocd", "borough_code", "borough_name", "occupied_units_1990")
  )

top5_cd_year <- series_df |>
  filter(series_family == "units_built_50_plus") |>
  distinct(borocd, borough_code, borough_name, year, outcome_value) |>
  arrange(desc(outcome_value), borocd, year) |>
  slice_head(n = 5)

write_csv(top5_cd_year, out_top5_csv, na = "")

project_proxy <- read_csv(mappluto_construction_proxy_cd_year_csv, show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(yearbuilt)),
    project_count_50_plus = suppressWarnings(as.numeric(lots_50_plus_proxy)),
    units_50_plus = suppressWarnings(as.numeric(units_50_plus_proxy)),
    total_nb_projects = suppressWarnings(as.numeric(residential_lot_count_proxy)),
    total_nb_units = suppressWarnings(as.numeric(residential_units_proxy))
  ) |>
  filter(year >= 1980, year <= 2009)

project_observed <- read_parquet(
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
    borocd %in% district_lookup$borocd,
    year >= 2010,
    year <= 2025,
    job_type == "New Building",
    !is.na(classa_prop),
    classa_prop > 0
  ) |>
  group_by(borocd, borough_code, borough_name, year) |>
  summarize(
    project_count_50_plus = sum(classa_prop >= 50, na.rm = TRUE),
    units_50_plus = sum(classa_prop[classa_prop >= 50], na.rm = TRUE),
    total_nb_projects = n(),
    total_nb_units = sum(classa_prop, na.rm = TRUE),
    .groups = "drop"
  )

project_panel <- bind_rows(project_proxy, project_observed) |>
  right_join(
    expand_grid(
      district_lookup |>
        select(borocd, borough_code, borough_name, treat_tercile, treat_tercile_label),
      year = 1980:2025
    ),
    by = c("borocd", "borough_code", "borough_name", "year")
  ) |>
  mutate(
    project_count_50_plus = coalesce(project_count_50_plus, 0),
    units_50_plus = coalesce(units_50_plus, 0),
    total_nb_projects = coalesce(total_nb_projects, 0),
    total_nb_units = coalesce(total_nb_units, 0)
  )

pooled_share_df <- compute_share_scenario(series_df, "pooled", "all_boroughs")

borough_share_df <- bind_rows(lapply(
  sort(unique(series_df$borough_name)),
  function(this_borough) {
    compute_share_scenario(
      filter(series_df, borough_name == this_borough),
      "borough_specific",
      this_borough
    )
  }
))

leave_one_out_share_df <- bind_rows(lapply(
  sort(unique(series_df$borough_name)),
  function(excluded_borough) {
    compute_share_scenario(
      filter(series_df, borough_name != excluded_borough),
      "leave_one_borough_out",
      paste0("drop_", excluded_borough)
    )
  }
))

top5_out_share_df <- compute_share_scenario(
  anti_join(series_df, top5_cd_year, by = c("borocd", "borough_code", "borough_name", "year")),
  "top5_out",
  "drop_top5_cd_year_50_plus"
)

write_csv(pooled_share_df, out_pooled_csv, na = "")
write_csv(borough_share_df, out_borough_csv, na = "")
write_csv(leave_one_out_share_df, out_leave_one_out_csv, na = "")
write_csv(top5_out_share_df, out_top5_out_csv, na = "")

levels_df <- bind_rows(
  compute_level_scenario(series_df, project_panel, "pooled", "all_boroughs"),
  compute_level_scenario(
    anti_join(series_df, top5_cd_year, by = c("borocd", "borough_code", "borough_name", "year")),
    anti_join(project_panel, top5_cd_year, by = c("borocd", "borough_code", "borough_name", "year")),
    "top5_out",
    "drop_top5_cd_year_50_plus"
  )
)

write_csv(levels_df, out_levels_csv, na = "")

extensive_df <- project_panel |>
  group_by(year, treat_tercile, treat_tercile_label) |>
  summarize(
    any_50_plus_project_rate = mean(project_count_50_plus > 0, na.rm = TRUE),
    projects_50_plus_per_cd_year = mean(project_count_50_plus, na.rm = TRUE),
    units_50_plus_per_cd_year = mean(units_50_plus, na.rm = TRUE),
    avg_units_per_nb_project = if_else(sum(total_nb_projects, na.rm = TRUE) > 0, sum(total_nb_units, na.rm = TRUE) / sum(total_nb_projects, na.rm = TRUE), NA_real_),
    .groups = "drop"
  ) |>
  pivot_longer(
    cols = c(any_50_plus_project_rate, projects_50_plus_per_cd_year, units_50_plus_per_cd_year, avg_units_per_nb_project),
    names_to = "metric",
    values_to = "metric_value"
  ) |>
  mutate(
    metric_value = if_else(metric == "avg_units_per_nb_project" & year < 2010, NA_real_, metric_value)
  )

write_csv(extensive_df, out_extensive_csv, na = "")

all_share_df <- bind_rows(pooled_share_df, borough_share_df, leave_one_out_share_df, top5_out_share_df) |>
  mutate(
    era = case_when(
      year >= 1980 & year <= 1984 ~ "1980-1984",
      year >= 1985 & year <= 1989 ~ "1985-1989",
      year >= 1990 & year <= 1999 ~ "1990-1999",
      year >= 2000 & year <= 2009 ~ "2000-2009",
      year >= 2010 & year <= 2019 ~ "2010-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    )
  )

summary_df <- all_share_df |>
  filter(treat_tercile_label == "High", !is.na(era)) |>
  group_by(scenario_type, scenario_name, series_family, era) |>
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_total = sum(borough_total, na.rm = TRUE),
    borough_share = if_else(borough_total > 0, outcome_value / borough_total, NA_real_),
    .groups = "drop"
  ) |>
  select(scenario_type, scenario_name, series_family, era, borough_share) |>
  pivot_wider(names_from = era, values_from = borough_share) |>
  mutate(
    change_1990s_vs_1985_1989 = `1990-1999` - `1985-1989`,
    change_2000s_vs_1985_1989 = `2000-2009` - `1985-1989`,
    change_2010s_vs_1985_1989 = `2010-2019` - `1985-1989`,
    change_2020s_vs_1985_1989 = `2020-2025` - `1985-1989`,
    decline_survives_1990s = change_1990s_vs_1985_1989 < 0,
    decline_survives_2000s = change_2000s_vs_1985_1989 < 0,
    decline_survives_2010s = change_2010s_vs_1985_1989 < 0,
    decline_survives_2020s = change_2020s_vs_1985_1989 < 0
  ) |>
  arrange(series_family, scenario_type, scenario_name)

write_csv(summary_df, out_summary_csv, na = "")

qc_df <- bind_rows(
  tibble(metric = "district_count", value = n_distinct(series_df$borocd), note = "Standard CDs in the preferred long sensitivity series."),
  tibble(metric = "top5_cd_year_count", value = nrow(top5_cd_year), note = "CD-year observations removed in the top-5-out sensitivity."),
  tibble(metric = "pooled_total_high_2020s_decline", value = summary_df$decline_survives_2020s[summary_df$scenario_type == "pooled" & summary_df$scenario_name == "all_boroughs" & summary_df$series_family == "units_built_total"], note = "One means the high-homeownership tercile still has a lower 2020-2025 share than in 1985-1989 for total units."),
  tibble(metric = "pooled_50_plus_high_2020s_decline", value = summary_df$decline_survives_2020s[summary_df$scenario_type == "pooled" & summary_df$scenario_name == "all_boroughs" & summary_df$series_family == "units_built_50_plus"], note = "One means the high-homeownership tercile still has a lower 2020-2025 share than in 1985-1989 for 50+ units.")
)

write_csv(qc_df, out_qc_csv, na = "")

pooled_plot_df <- pooled_share_df |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = if_else(series_family == "units_built_total", "Total units", "50+ units")
  )

borough_plot_df <- borough_share_df |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    series_label = if_else(series_family == "units_built_total", "Total units", "50+ units")
  )

leave_one_out_plot_df <- leave_one_out_share_df |>
  filter(treat_tercile_label == "High") |>
  mutate(
    series_label = if_else(series_family == "units_built_total", "Total units", "50+ units")
  )

top5_plot_df <- bind_rows(
  pooled_share_df |>
    filter(treat_tercile_label == "High") |>
    mutate(version = "Pooled"),
  top5_out_share_df |>
    filter(treat_tercile_label == "High") |>
    mutate(version = "Top 5 out")
) |>
  mutate(series_label = if_else(series_family == "units_built_total", "Total units", "50+ units"))

levels_plot_df <- levels_df |>
  filter(scenario_type == "pooled") |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    metric_label = case_when(
      metric == "units_built_total_per_10000_occupied_1990" ~ "Total units per 10,000 occupied homes",
      metric == "units_built_50_plus_per_10000_occupied_1990" ~ "50+ units per 10,000 occupied homes",
      TRUE ~ "50+ projects per CD-year"
    )
  )

extensive_plot_df <- extensive_df |>
  filter(!(metric == "avg_units_per_nb_project" & year < 2010)) |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    metric_label = case_when(
      metric == "any_50_plus_project_rate" ~ "Probability of any 50+ project",
      metric == "projects_50_plus_per_cd_year" ~ "50+ projects per CD-year",
      metric == "units_50_plus_per_cd_year" ~ "50+ units per CD-year",
      TRUE ~ "Average units per new-building project"
    )
  )

pdf(out_plots_pdf, width = 11, height = 8.5)
print(
  ggplot(pooled_plot_df, aes(x = year, y = borough_share, color = treat_tercile_label)) +
    geom_line(linewidth = 0.8) +
    facet_wrap(~series_label, ncol = 1, scales = "free_y") +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = "Within-borough share", color = "Treat tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(borough_plot_df, aes(x = year, y = borough_share, color = treat_tercile_label)) +
    geom_line(linewidth = 0.65) +
    facet_grid(scenario_name ~ series_label, scales = "free_y") +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = "Within-borough share", color = "Treat tercile") +
    theme_minimal(base_size = 9) +
    theme(legend.position = "bottom")
)
print(
  ggplot(leave_one_out_plot_df, aes(x = year, y = borough_share, color = scenario_name)) +
    geom_line(linewidth = 0.75) +
    facet_wrap(~series_label, ncol = 1, scales = "free_y") +
    labs(x = NULL, y = "High-tercile share", color = "Scenario") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(top5_plot_df, aes(x = year, y = borough_share, color = version)) +
    geom_line(linewidth = 0.85) +
    facet_wrap(~series_label, ncol = 1, scales = "free_y") +
    scale_color_manual(values = c("Pooled" = "#1b6ca8", "Top 5 out" = "#d65f0e")) +
    labs(x = NULL, y = "High-tercile share", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(levels_plot_df, aes(x = year, y = metric_value, color = treat_tercile_label)) +
    geom_line(linewidth = 0.75) +
    facet_wrap(~metric_label, ncol = 1, scales = "free_y") +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = NULL, color = "Treat tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(extensive_plot_df, aes(x = year, y = metric_value, color = treat_tercile_label)) +
    geom_line(linewidth = 0.75) +
    facet_wrap(~metric_label, ncol = 2, scales = "free_y") +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = NULL, color = "Treat tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
dev.off()

cat("Wrote long-units sensitivity outputs to", dirname(out_pooled_csv), "\n")
