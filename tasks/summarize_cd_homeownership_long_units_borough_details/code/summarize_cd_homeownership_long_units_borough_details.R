# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_cd_homeownership_long_units_borough_details/code")
# cd_homeownership_long_units_series_csv <- "../input/cd_homeownership_long_units_series.csv"
# mappluto_construction_proxy_cd_year_csv <- "../input/mappluto_construction_proxy_cd_year.csv"
# dcp_housing_database_project_level_parquet <- "../input/dcp_housing_database_project_level_25q4.parquet"
# cd_homeownership_exact_decadal_validation_tercile_csv <- "../input/cd_homeownership_exact_decadal_validation_tercile.csv"
# cd_homeownership_proxy_overlap_borough_year_csv <- "../input/cd_homeownership_proxy_overlap_borough_year.csv"
# out_borough_era_csv <- "../output/cd_homeownership_long_units_borough_era_shares.csv"
# out_overlap_error_csv <- "../output/cd_homeownership_long_units_borough_overlap_error.csv"
# out_exact_splice_csv <- "../output/cd_homeownership_long_units_exact_era_splice_total_units.csv"
# out_plots_pdf <- "../output/cd_homeownership_long_units_borough_plots.pdf"
# out_qc_csv <- "../output/cd_homeownership_long_units_borough_details_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tidyr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 10) {
  stop("Expected 10 arguments: cd_homeownership_long_units_series_csv mappluto_construction_proxy_cd_year_csv dcp_housing_database_project_level_parquet cd_homeownership_exact_decadal_validation_tercile_csv cd_homeownership_proxy_overlap_borough_year_csv out_borough_era_csv out_overlap_error_csv out_exact_splice_csv out_plots_pdf out_qc_csv")
}

cd_homeownership_long_units_series_csv <- args[1]
mappluto_construction_proxy_cd_year_csv <- args[2]
dcp_housing_database_project_level_parquet <- args[3]
cd_homeownership_exact_decadal_validation_tercile_csv <- args[4]
cd_homeownership_proxy_overlap_borough_year_csv <- args[5]
out_borough_era_csv <- args[6]
out_overlap_error_csv <- args[7]
out_exact_splice_csv <- args[8]
out_plots_pdf <- args[9]
out_qc_csv <- args[10]

district_lookup <- read_csv(cd_homeownership_long_units_series_csv, show_col_types = FALSE, na = c("", "NA")) |>
  distinct(borocd, borough_code, borough_name, treat_pp) |>
  mutate(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
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

units_series_df <- read_csv(cd_homeownership_long_units_series_csv, show_col_types = FALSE, na = c("", "NA")) |>
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
    series_label = series_label,
    outcome_value = suppressWarnings(as.numeric(outcome_value))
  )

projects_proxy_df <- read_csv(mappluto_construction_proxy_cd_year_csv, show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(yearbuilt)),
    series_family = "projects_built_50_plus",
    series_label = "Projects built: 50+",
    outcome_value = suppressWarnings(as.numeric(lots_50_plus_proxy))
  ) |>
  filter(year >= 1980, year <= 2009)

projects_observed_df <- read_parquet(
  dcp_housing_database_project_level_parquet,
  col_select = c("completion_year", "community_district", "borough_code", "borough_name", "job_type", "classa_prop")
) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(community_district))),
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
  left_join(
    district_lookup |>
      distinct(borocd, borough_code, borough_name),
    by = "borocd"
  ) |>
  group_by(borocd, borough_code, borough_name, year) |>
  summarize(
    series_family = "projects_built_50_plus",
    series_label = "Projects built: 50+",
    outcome_value = sum(classa_prop >= 50, na.rm = TRUE),
    .groups = "drop"
  )

all_series_df <- bind_rows(units_series_df, projects_proxy_df, projects_observed_df) |>
  left_join(
    district_lookup |>
      select(borocd, borough_code, borough_name, treat_tercile, treat_tercile_label),
    by = c("borocd", "borough_code", "borough_name")
  ) |>
  mutate(
    era = case_when(
      year >= 1985 & year <= 1989 ~ "1985-1989",
      year >= 1990 & year <= 1999 ~ "1990-1999",
      year >= 2000 & year <= 2009 ~ "2000-2009",
      year >= 2010 & year <= 2019 ~ "2010-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    )
  ) |>
  filter(!is.na(era))

borough_year_shares_df <- all_series_df |>
  group_by(series_family, series_label, borough_code, borough_name, year, treat_tercile, treat_tercile_label) |>
  summarize(outcome_value = sum(outcome_value, na.rm = TRUE), .groups = "drop") |>
  group_by(series_family, series_label, borough_code, borough_name, year) |>
  mutate(
    borough_total = sum(outcome_value, na.rm = TRUE),
    borough_share = if_else(borough_total > 0, outcome_value / borough_total, NA_real_)
  ) |>
  ungroup()

borough_era_df <- all_series_df |>
  group_by(series_family, series_label, borough_code, borough_name, era, treat_tercile, treat_tercile_label) |>
  summarize(outcome_value = sum(outcome_value, na.rm = TRUE), .groups = "drop") |>
  group_by(series_family, series_label, borough_code, borough_name, era) |>
  mutate(
    borough_total = sum(outcome_value, na.rm = TRUE),
    borough_share = if_else(borough_total > 0, outcome_value / borough_total, NA_real_)
  ) |>
  ungroup() |>
  arrange(series_family, borough_code, era, treat_tercile)

write_csv(borough_era_df, out_borough_era_csv, na = "")

overlap_error_df <- read_csv(cd_homeownership_proxy_overlap_borough_year_csv, show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    outcome_family = outcome_family,
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(year)),
    treat_tercile = suppressWarnings(as.integer(treat_tercile)),
    treat_tercile_label = treat_tercile_label,
    proxy_value = suppressWarnings(as.numeric(proxy_value)),
    observed_value = suppressWarnings(as.numeric(observed_value)),
    proxy_borough_total = suppressWarnings(as.numeric(proxy_borough_total)),
    observed_borough_total = suppressWarnings(as.numeric(observed_borough_total))
  ) |>
  mutate(
    series_family = case_when(
      outcome_family == "total_nb_units" ~ "units_built_total",
      outcome_family == "units_50_plus" ~ "units_built_50_plus",
      TRUE ~ "projects_built_50_plus"
    ),
    era = case_when(
      year >= 2010 & year <= 2019 ~ "2010-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    )
  ) |>
  filter(!is.na(era)) |>
  group_by(series_family, borough_code, borough_name, era, treat_tercile, treat_tercile_label) |>
  summarize(
    proxy_share = if_else(sum(proxy_borough_total, na.rm = TRUE) > 0, sum(proxy_value, na.rm = TRUE) / sum(proxy_borough_total, na.rm = TRUE), NA_real_),
    observed_share = if_else(sum(observed_borough_total, na.rm = TRUE) > 0, sum(observed_value, na.rm = TRUE) / sum(observed_borough_total, na.rm = TRUE), NA_real_),
    signed_error = proxy_share - observed_share,
    abs_error = abs(signed_error),
    .groups = "drop"
  ) |>
  arrange(series_family, borough_code, era, treat_tercile)

write_csv(overlap_error_df, out_overlap_error_csv, na = "")

exact_tercile_df <- read_csv(cd_homeownership_exact_decadal_validation_tercile_csv, show_col_types = FALSE, na = c("", "NA")) |>
  filter(
    geography_scope == "all_boroughs",
    source == "Exact DCP 2000 structure-built counts"
  ) |>
  transmute(
    era = if_else(grepl("^1980", decade), "1980s exact", "1990s exact"),
    treat_tercile = suppressWarnings(as.integer(treat_tercile)),
    treat_tercile_label = treat_tercile_label,
    borough_share = suppressWarnings(as.numeric(borough_share))
  )

observed_era_df <- read_csv(cd_homeownership_long_units_series_csv, show_col_types = FALSE, na = c("", "NA")) |>
  filter(
    source_family == "dcp_hdb_completion",
    series_kind == "preferred_long_series",
    series_family == "units_built_total",
    year >= 2010,
    year <= 2025
  ) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(year)),
    outcome_value = suppressWarnings(as.numeric(outcome_value))
  ) |>
  left_join(
    district_lookup |>
      select(borocd, borough_code, borough_name, treat_tercile, treat_tercile_label),
    by = c("borocd", "borough_code", "borough_name")
  ) |>
  mutate(
    era = case_when(
      year >= 2010 & year <= 2019 ~ "2010s observed",
      year >= 2020 & year <= 2025 ~ "2020s observed",
      TRUE ~ NA_character_
    )
  ) |>
  filter(!is.na(era)) |>
  group_by(era, borough_code, borough_name, treat_tercile, treat_tercile_label) |>
  summarize(outcome_value = sum(outcome_value, na.rm = TRUE), .groups = "drop") |>
  group_by(era, borough_code, borough_name) |>
  mutate(
    borough_total = sum(outcome_value, na.rm = TRUE),
    borough_share = if_else(borough_total > 0, outcome_value / borough_total, NA_real_)
  ) |>
  ungroup() |>
  group_by(era, treat_tercile, treat_tercile_label) |>
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_total = sum(distinct(data.frame(borough_code, borough_name, borough_total))$borough_total, na.rm = TRUE),
    borough_share = if_else(borough_total > 0, outcome_value / borough_total, NA_real_),
    .groups = "drop"
  ) |>
  select(era, treat_tercile, treat_tercile_label, borough_share)

exact_splice_df <- bind_rows(exact_tercile_df, observed_era_df) |>
  mutate(treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High"))) |>
  arrange(era, treat_tercile)

write_csv(exact_splice_df, out_exact_splice_csv, na = "")

plot_df <- borough_year_shares_df |>
  filter(series_family %in% c("units_built_total", "units_built_50_plus", "projects_built_50_plus")) |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    borough_name = factor(borough_name, levels = c("Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"))
  )

pdf(out_plots_pdf, width = 11, height = 8.5)

for (family_value in c("units_built_total", "units_built_50_plus", "projects_built_50_plus")) {
  family_plot_df <- plot_df |>
    filter(series_family == family_value)

  print(
    ggplot(family_plot_df, aes(x = year, y = borough_share, color = treat_tercile_label)) +
      geom_line(linewidth = 0.8) +
      facet_wrap(~borough_name, ncol = 2, scales = "free_y") +
      scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
      labs(
        x = NULL,
        y = "Within-borough share",
        color = "Treat tercile",
        title = unique(family_plot_df$series_label)
      ) +
      theme_minimal(base_size = 11) +
      theme(legend.position = "bottom")
  )
}

print(
  ggplot(
    exact_splice_df |>
      mutate(era = factor(era, levels = c("1980s exact", "1990s exact", "2010s observed", "2020s observed"))),
    aes(x = era, y = borough_share, color = treat_tercile_label, group = treat_tercile_label)
  ) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2.2) +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = "Within-borough share", color = "Treat tercile", title = "Exact DCP 1980s/1990s splice to observed post-2010 total units") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)

dev.off()

qc_df <- bind_rows(
  tibble(metric = "borough_count", value = n_distinct(borough_era_df$borough_name), note = "Boroughs represented in the borough-era share table."),
  tibble(metric = "series_family_count", value = n_distinct(borough_era_df$series_family), note = "Series represented in the borough-era share table."),
  tibble(metric = "overlap_error_row_count", value = nrow(overlap_error_df), note = "Rows in the overlap error table by borough, era, and tercile."),
  tibble(metric = "exact_splice_row_count", value = nrow(exact_splice_df), note = "Rows in the exact-to-observed total-units era splice table."),
  tibble(metric = "max_borough_era_share_sum_gap", value = max(abs(borough_era_df |>
    group_by(series_family, borough_code, borough_name, era) |>
    summarize(total_share = sum(borough_share, na.rm = TRUE), .groups = "drop") |>
    pull(total_share) - 1), na.rm = TRUE), note = "Maximum absolute gap from 1 in borough-era tercile-share sums.")
)

write_csv(qc_df, out_qc_csv, na = "")

cat("Wrote borough detail outputs to", dirname(out_borough_era_csv), "\n")
