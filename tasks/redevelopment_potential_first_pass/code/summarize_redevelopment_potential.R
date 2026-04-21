# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/redevelopment_potential_first_pass/code")
# treatment_csv <- "../input/cd_homeownership_1990_measure.csv"
# baseline_controls_csv <- "../input/cd_baseline_1990_controls.csv"
# redev_baseline_csv <- "../output/cd_redevelopment_potential_baseline.csv"
# long_units_series_csv <- "../input/cd_homeownership_long_units_series.csv"
# dcp_supply_panel_csv <- "../input/cd_homeownership_dcp_supply_panel.csv"
# dob_nb_panel_csv <- "../input/cd_homeownership_permit_nb_panel.csv"
# cd_boundary_parquet <- "../input/dcp_boundary_community_districts_20260412.parquet"
# out_redev_by_treat_csv <- "../output/tables/redev_potential_by_treatment_tercile.csv"
# out_treat_by_redev_csv <- "../output/tables/treatment_by_redev_tercile.csv"
# out_cell_summary_csv <- "../output/tables/two_by_two_cell_summary.csv"
# out_era_outcomes_csv <- "../output/tables/two_by_two_era_outcomes.csv"
# out_scatter_pdf <- "../output/figures/homeownership_vs_redev_potential_scatter.pdf"
# out_maps_pdf <- "../output/figures/redev_potential_maps.pdf"
# out_total_units_pdf <- "../output/figures/two_by_two_total_units_paths.pdf"
# out_50plus_units_pdf <- "../output/figures/two_by_two_50plus_units_paths.pdf"
# out_gross_add_pdf <- "../output/figures/two_by_two_gross_additions_paths.pdf"
# out_project_count_pdf <- "../output/figures/two_by_two_50plus_project_count_paths.pdf"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(sf)
  library(stringr)
  library(tidyr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 17) {
  stop("Expected 17 arguments: treatment_csv baseline_controls_csv redev_baseline_csv long_units_series_csv dcp_supply_panel_csv dob_nb_panel_csv cd_boundary_parquet out_redev_by_treat_csv out_treat_by_redev_csv out_cell_summary_csv out_era_outcomes_csv out_scatter_pdf out_maps_pdf out_total_units_pdf out_50plus_units_pdf out_gross_add_pdf out_project_count_pdf")
}

treatment_csv <- args[1]
baseline_controls_csv <- args[2]
redev_baseline_csv <- args[3]
long_units_series_csv <- args[4]
dcp_supply_panel_csv <- args[5]
dob_nb_panel_csv <- args[6]
cd_boundary_parquet <- args[7]
out_redev_by_treat_csv <- args[8]
out_treat_by_redev_csv <- args[9]
out_cell_summary_csv <- args[10]
out_era_outcomes_csv <- args[11]
out_scatter_pdf <- args[12]
out_maps_pdf <- args[13]
out_total_units_pdf <- args[14]
out_50plus_units_pdf <- args[15]
out_gross_add_pdf <- args[16]
out_project_count_pdf <- args[17]

era_levels <- c("1980-1984", "1985-1989", "1990-1999", "2000-2009", "2010-2019", "2020-2025")
era_observed_levels <- c("2010-2014", "2015-2019", "2020-2025")

base_df <- read_csv(redev_baseline_csv, show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name
  ) |>
  group_by(borough_code, borough_name) |>
  mutate(
    treat_tercile = ntile(treat_pp, 3),
    redev_tercile_A = ntile(redev_potential_A_z_boro, 3),
    redev_tercile_B = ntile(redev_potential_B_z_boro, 3),
    redev_tercile_C = ntile(redev_potential_C_z_boro, 3),
    treat_boro_median = median(treat_z_boro, na.rm = TRUE),
    redev_A_boro_median = median(redev_potential_A_z_boro, na.rm = TRUE),
    high_homeowner = treat_z_boro >= treat_boro_median,
    high_redev_A = redev_potential_A_z_boro >= redev_A_boro_median
  ) |>
  ungroup() |>
  mutate(
    treat_tercile_label = case_when(
      treat_tercile == 1 ~ "Low",
      treat_tercile == 2 ~ "Middle",
      TRUE ~ "High"
    ),
    redev_tercile_A_label = case_when(
      redev_tercile_A == 1 ~ "Low",
      redev_tercile_A == 2 ~ "Middle",
      TRUE ~ "High"
    ),
    two_by_two_cell = case_when(
      !high_homeowner & !high_redev_A ~ "LL",
      !high_homeowner & high_redev_A ~ "LH",
      high_homeowner & !high_redev_A ~ "HL",
      TRUE ~ "HH"
    ),
    two_by_two_label = case_when(
      two_by_two_cell == "LL" ~ "Low homeowner / Low redev",
      two_by_two_cell == "LH" ~ "Low homeowner / High redev",
      two_by_two_cell == "HL" ~ "High homeowner / Low redev",
      TRUE ~ "High homeowner / High redev"
    )
  )

redev_by_treat_df <- bind_rows(
  base_df |>
    group_by(treat_tercile_label) |>
    summarize(
      scope = "city",
      redev_A_mean = mean(redev_potential_A_z_boro, na.rm = TRUE),
      redev_B_mean = mean(redev_potential_B_z_boro, na.rm = TRUE),
      redev_C_mean = mean(redev_potential_C_z_boro, na.rm = TRUE),
      n_cd = n(),
      .groups = "drop"
    ) |>
    rename(group_label = treat_tercile_label),
  base_df |>
    group_by(borough_name, treat_tercile_label) |>
    summarize(
      scope = borough_name[1],
      redev_A_mean = mean(redev_potential_A_z_boro, na.rm = TRUE),
      redev_B_mean = mean(redev_potential_B_z_boro, na.rm = TRUE),
      redev_C_mean = mean(redev_potential_C_z_boro, na.rm = TRUE),
      n_cd = n(),
      .groups = "drop"
    ) |>
    rename(group_label = treat_tercile_label)
)

treat_by_redev_df <- bind_rows(
  base_df |>
    group_by(redev_tercile_A_label) |>
    summarize(
      scope = "city",
      treat_pp_mean = mean(treat_pp, na.rm = TRUE),
      treat_z_boro_mean = mean(treat_z_boro, na.rm = TRUE),
      n_cd = n(),
      .groups = "drop"
    ) |>
    rename(group_label = redev_tercile_A_label),
  base_df |>
    group_by(borough_name, redev_tercile_A_label) |>
    summarize(
      scope = borough_name[1],
      treat_pp_mean = mean(treat_pp, na.rm = TRUE),
      treat_z_boro_mean = mean(treat_z_boro, na.rm = TRUE),
      n_cd = n(),
      .groups = "drop"
    ) |>
    rename(group_label = redev_tercile_A_label)
)

cell_summary_df <- base_df |>
  group_by(two_by_two_cell, two_by_two_label) |>
  summarize(
    n_cd = n(),
    bronx_cd_share = mean(borough_name == "Bronx"),
    brooklyn_cd_share = mean(borough_name == "Brooklyn"),
    manhattan_cd_share = mean(borough_name == "Manhattan"),
    queens_cd_share = mean(borough_name == "Queens"),
    staten_island_cd_share = mean(borough_name == "Staten Island"),
    mean_treat_pp = mean(treat_pp, na.rm = TRUE),
    mean_treat_z_boro = mean(treat_z_boro, na.rm = TRUE),
    mean_redev_A_z_boro = mean(redev_potential_A_z_boro, na.rm = TRUE),
    mean_redev_C_z_boro = mean(redev_potential_C_z_boro, na.rm = TRUE),
    mean_income = mean(median_household_income_1990_1999_dollars_exact, na.rm = TRUE),
    mean_poverty = mean(poverty_share_1990_exact, na.rm = TRUE),
    mean_vacancy = mean(vacancy_rate_1990_exact, na.rm = TRUE),
    mean_structure_1_2 = mean(structure_share_1_2_units_1990_exact, na.rm = TRUE),
    mean_built_far = mean(cd_mean_built_far_lot_weighted, na.rm = TRUE),
    mean_max_resid_far = mean(cd_mean_max_resid_far_lot_weighted, na.rm = TRUE),
    mean_unused_res_far = mean(cd_mean_unused_res_far_lot_weighted, na.rm = TRUE),
    mean_unused_res_floor_area_per_res_acre = mean(cd_unused_res_floor_area_per_res_acre, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(two_by_two_cell)

write_csv(redev_by_treat_df, out_redev_by_treat_csv, na = "")
write_csv(treat_by_redev_df, out_treat_by_redev_csv, na = "")
write_csv(cell_summary_df, out_cell_summary_csv, na = "")

scatter_label_df <- bind_rows(
  base_df |>
    arrange(desc(treat_z_boro)) |>
    slice_head(n = 3),
  base_df |>
    arrange(treat_z_boro) |>
    slice_head(n = 3),
  base_df |>
    arrange(desc(redev_potential_A_z_boro)) |>
    slice_head(n = 3),
  base_df |>
    arrange(redev_potential_A_z_boro) |>
    slice_head(n = 3),
  base_df |>
    filter(borocd %in% c("101", "105", "106", "108"))
) |>
  distinct(borocd, .keep_all = TRUE)

pdf(out_scatter_pdf, width = 8.5, height = 7)
for (x_name in c("treat_z_boro", "treat_pp")) {
  for (y_name in c("redev_potential_A_z_boro", "redev_potential_B_z_boro", "redev_potential_C_z_boro")) {
    print(
      ggplot(base_df, aes(x = .data[[x_name]], y = .data[[y_name]], color = borough_name)) +
        geom_point(size = 2, alpha = 0.85) +
        geom_text(
          data = scatter_label_df,
          aes(label = borocd),
          color = "black",
          nudge_y = 0.05,
          check_overlap = TRUE,
          size = 3
        ) +
        labs(
          title = paste0(x_name, " vs ", y_name),
          subtitle = "Borough-colored CDs with extreme and Manhattan labels.",
          x = x_name,
          y = y_name,
          color = "Borough"
        ) +
        theme_minimal(base_size = 11)
    )
  }
}
dev.off()

boundary_sf <- read_parquet(cd_boundary_parquet) |>
  transmute(
    borocd = sprintf(
      "%03d",
      coalesce(
        suppressWarnings(as.integer(district_id)),
        suppressWarnings(as.integer(boro_cd))
      )
    ),
    geometry = st_as_sfc(geometry_wkt, crs = suppressWarnings(as.integer(crs_epsg[1])))
  ) |>
  st_as_sf() |>
  filter(borocd %in% base_df$borocd) |>
  left_join(
    base_df |>
      select(borocd, treat_z_boro, redev_potential_A_z_boro, borough_name),
    by = "borocd"
  )

pdf(out_maps_pdf, width = 10, height = 7.5)
print(
  ggplot(boundary_sf) +
    geom_sf(aes(fill = treat_z_boro), color = "white", linewidth = 0.1) +
    scale_fill_viridis_c(option = "C") +
    labs(title = "1990 homeownership exposure", subtitle = "Canonical within-borough z-score treatment.", fill = "Treat z") +
    theme_void(base_size = 11)
)
print(
  ggplot(boundary_sf) +
    geom_sf(aes(fill = redev_potential_A_z_boro), color = "white", linewidth = 0.1) +
    scale_fill_viridis_c(option = "C") +
    labs(title = "Redevelopment potential A", subtitle = "Within-borough z-score of log unused residential floor area.", fill = "A z") +
    theme_void(base_size = 11)
)
dev.off()

long_df <- read_csv(long_units_series_csv, show_col_types = FALSE, na = c("", "NA")) |>
  filter(series_kind == "preferred_long_series", series_family %in% c("units_built_total", "units_built_50_plus")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(year)),
    outcome_family = series_family,
    outcome_value = suppressWarnings(as.numeric(outcome_value)),
    borough_outcome_total = suppressWarnings(as.numeric(borough_outcome_total))
  ) |>
  left_join(
    base_df |>
      select(borocd, borough_code, borough_name, two_by_two_cell, two_by_two_label, occupied_units_1990, residential_acres),
    by = c("borocd", "borough_code", "borough_name")
  ) |>
  mutate(
    era = case_when(
      year >= 1980 & year <= 1984 ~ "1980-1984",
      year >= 1985 & year <= 1989 ~ "1985-1989",
      year >= 1990 & year <= 1999 ~ "1990-1999",
      year >= 2000 & year <= 2009 ~ "2000-2009",
      year >= 2010 & year <= 2019 ~ "2010-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    ),
    era = factor(era, levels = era_levels)
  ) |>
  filter(!is.na(era))

long_era_df <- bind_rows(
  long_df |>
    group_by(outcome_family, era, two_by_two_cell, two_by_two_label) |>
    summarize(
      metric = "per_10000_occupied_1990",
      value = 10000 * sum(outcome_value, na.rm = TRUE) / sum(occupied_units_1990, na.rm = TRUE),
      .groups = "drop"
    ),
  long_df |>
    group_by(outcome_family, era, two_by_two_cell, two_by_two_label) |>
    summarize(
      metric = "per_residential_acre",
      value = sum(outcome_value, na.rm = TRUE) / sum(residential_acres, na.rm = TRUE),
      .groups = "drop"
    ),
  long_df |>
    group_by(outcome_family, borough_name, era, two_by_two_cell, two_by_two_label) |>
    summarize(
      borough_share = sum(outcome_value, na.rm = TRUE) / sum(distinct(data.frame(year, borough_outcome_total))$borough_outcome_total, na.rm = TRUE),
      .groups = "drop"
    ) |>
    group_by(outcome_family, era, two_by_two_cell, two_by_two_label) |>
    summarize(
      metric = "within_borough_share",
      value = mean(borough_share, na.rm = TRUE),
      .groups = "drop"
    )
)

dcp_df <- read_csv(dcp_supply_panel_csv, show_col_types = FALSE, na = c("", "NA")) |>
  filter(
    year >= 2010,
    outcome_family %in% c("gross_add_units", "nb_gross_units", "nb_gross_units_50_plus", "nb_project_count", "nb_project_count_50_plus")
  ) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(year)),
    outcome_family = outcome_family,
    outcome_value = suppressWarnings(as.numeric(outcome_value)),
    borough_outcome_total = suppressWarnings(as.numeric(borough_outcome_total))
  ) |>
  left_join(
    base_df |>
      select(borocd, borough_code, borough_name, two_by_two_cell, two_by_two_label, occupied_units_1990, residential_acres),
    by = c("borocd", "borough_code", "borough_name")
  ) |>
  mutate(
    era = case_when(
      year >= 2010 & year <= 2014 ~ "2010-2014",
      year >= 2015 & year <= 2019 ~ "2015-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    ),
    era = factor(era, levels = era_observed_levels)
  ) |>
  filter(!is.na(era))

nb_units_df <- dcp_df |>
  filter(outcome_family %in% c("nb_gross_units", "nb_project_count")) |>
  select(borocd, borough_code, borough_name, year, outcome_family, outcome_value) |>
  pivot_wider(names_from = outcome_family, values_from = outcome_value) |>
  mutate(mean_units_per_nb_project = if_else(nb_project_count > 0, nb_gross_units / nb_project_count, NA_real_)) |>
  left_join(
    base_df |>
      select(borocd, borough_code, borough_name, two_by_two_cell, two_by_two_label, occupied_units_1990, residential_acres),
    by = c("borocd", "borough_code", "borough_name")
  ) |>
  mutate(
    era = case_when(
      year >= 2010 & year <= 2014 ~ "2010-2014",
      year >= 2015 & year <= 2019 ~ "2015-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    ),
    era = factor(era, levels = era_observed_levels),
    any_50plus_project = NA_real_
  )

project_count_df <- dcp_df |>
  filter(outcome_family == "nb_project_count_50_plus") |>
  mutate(any_50plus_project = outcome_value > 0)

observed_era_df <- bind_rows(
  dcp_df |>
    filter(outcome_family %in% c("gross_add_units", "nb_gross_units", "nb_gross_units_50_plus")) |>
    group_by(outcome_family, era, two_by_two_cell, two_by_two_label) |>
    summarize(
      metric = "per_10000_occupied_1990",
      value = 10000 * sum(outcome_value, na.rm = TRUE) / sum(occupied_units_1990, na.rm = TRUE),
      .groups = "drop"
    ),
  dcp_df |>
    filter(outcome_family %in% c("gross_add_units", "nb_gross_units", "nb_gross_units_50_plus")) |>
    group_by(outcome_family, era, two_by_two_cell, two_by_two_label) |>
    summarize(
      metric = "per_residential_acre",
      value = sum(outcome_value, na.rm = TRUE) / sum(residential_acres, na.rm = TRUE),
      .groups = "drop"
    ),
  dcp_df |>
    filter(outcome_family %in% c("gross_add_units", "nb_gross_units", "nb_gross_units_50_plus")) |>
    group_by(outcome_family, borough_name, era, two_by_two_cell, two_by_two_label) |>
    summarize(
      borough_share = sum(outcome_value, na.rm = TRUE) / sum(distinct(data.frame(year, borough_outcome_total))$borough_outcome_total, na.rm = TRUE),
      .groups = "drop"
    ) |>
    group_by(outcome_family, era, two_by_two_cell, two_by_two_label) |>
    summarize(metric = "within_borough_share", value = mean(borough_share, na.rm = TRUE), .groups = "drop"),
  project_count_df |>
    group_by(era, two_by_two_cell, two_by_two_label) |>
    summarize(
      outcome_family = "nb_project_count_50_plus",
      metric = "projects_per_cd_year",
      value = mean(outcome_value, na.rm = TRUE),
      .groups = "drop"
    ),
  project_count_df |>
    group_by(era, two_by_two_cell, two_by_two_label) |>
    summarize(
      outcome_family = "any_50plus_project",
      metric = "probability",
      value = mean(any_50plus_project, na.rm = TRUE),
      .groups = "drop"
    ),
  nb_units_df |>
    group_by(era, two_by_two_cell, two_by_two_label) |>
    summarize(
      outcome_family = "mean_units_per_nb_project",
      metric = "mean_units",
      value = sum(nb_gross_units, na.rm = TRUE) / sum(nb_project_count, na.rm = TRUE),
      .groups = "drop"
    )
)

dob_df <- read_csv(dob_nb_panel_csv, show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(year)),
    outcome_family = "dob_nb_jobs",
    outcome_value = suppressWarnings(as.numeric(outcome_value)),
    borough_outcome_total = suppressWarnings(as.numeric(borough_outcome_total))
  ) |>
  filter(year >= 1980, year <= 2025) |>
  left_join(
    base_df |>
      select(borocd, borough_code, borough_name, two_by_two_cell, two_by_two_label, occupied_units_1990),
    by = c("borocd", "borough_code", "borough_name")
  ) |>
  mutate(
    era = case_when(
      year >= 1980 & year <= 1984 ~ "1980-1984",
      year >= 1985 & year <= 1989 ~ "1985-1989",
      year >= 1990 & year <= 1999 ~ "1990-1999",
      year >= 2000 & year <= 2009 ~ "2000-2009",
      year >= 2010 & year <= 2019 ~ "2010-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    ),
    era = factor(era, levels = era_levels)
  ) |>
  filter(!is.na(era)) |>
  group_by(era, two_by_two_cell, two_by_two_label) |>
  summarize(
    metric = "per_10000_occupied_1990",
    value = 10000 * sum(outcome_value, na.rm = TRUE) / sum(occupied_units_1990, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(outcome_family = "dob_nb_jobs")

era_outcomes_df <- bind_rows(long_era_df, observed_era_df, dob_df) |>
  arrange(outcome_family, era, two_by_two_cell, metric)

write_csv(era_outcomes_df, out_era_outcomes_csv, na = "")

plot_era_path <- function(df, family_name, x_levels, title_text, subtitle_text, y_labels, out_pdf_path) {
  pdf(out_pdf_path, width = 8.5, height = 7)
  metrics <- unique(df$metric)
  for (metric_name in metrics) {
    metric_df <- df |>
      filter(metric == metric_name) |>
      mutate(
        era = factor(era, levels = x_levels),
        two_by_two_label = factor(two_by_two_label, levels = c("Low homeowner / Low redev", "Low homeowner / High redev", "High homeowner / Low redev", "High homeowner / High redev"))
      )

    print(
      ggplot(metric_df, aes(x = era, y = value, color = two_by_two_label, group = two_by_two_label)) +
        geom_line(linewidth = 0.9) +
        geom_point(size = 2) +
        labs(
          title = title_text,
          subtitle = paste0(subtitle_text, " Metric: ", y_labels[[metric_name]]),
          x = NULL,
          y = y_labels[[metric_name]],
          color = "2x2 cell"
        ) +
        theme_minimal(base_size = 11)
    )
  }
  dev.off()
}

plot_era_path(
  df = filter(era_outcomes_df, outcome_family == "units_built_total"),
  family_name = "units_built_total",
  x_levels = era_levels,
  title_text = "2x2 paths: total new-building units",
  subtitle_text = "Preferred long series. Source switches from PLUTO proxy to observed DCP HDB in 2010.",
  y_labels = list(per_10000_occupied_1990 = "Units per 10,000 occupied units", per_residential_acre = "Units per residential acre", within_borough_share = "Within-borough share"),
  out_pdf_path = out_total_units_pdf
)

plot_era_path(
  df = filter(era_outcomes_df, outcome_family == "units_built_50_plus"),
  family_name = "units_built_50_plus",
  x_levels = era_levels,
  title_text = "2x2 paths: 50+ new-building units",
  subtitle_text = "Preferred long series. Source switches from PLUTO proxy to observed DCP HDB in 2010.",
  y_labels = list(per_10000_occupied_1990 = "50+ units per 10,000 occupied units", per_residential_acre = "50+ units per residential acre", within_borough_share = "Within-borough share"),
  out_pdf_path = out_50plus_units_pdf
)

plot_era_path(
  df = filter(era_outcomes_df, outcome_family == "gross_add_units"),
  family_name = "gross_add_units",
  x_levels = era_observed_levels,
  title_text = "2x2 paths: gross additions",
  subtitle_text = "Observed DCP permit-year outcomes only.",
  y_labels = list(per_10000_occupied_1990 = "Gross additions per 10,000 occupied units", per_residential_acre = "Gross additions per residential acre", within_borough_share = "Within-borough share"),
  out_pdf_path = out_gross_add_pdf
)

plot_era_path(
  df = filter(era_outcomes_df, outcome_family %in% c("nb_project_count_50_plus", "any_50plus_project", "mean_units_per_nb_project")),
  family_name = "project_margins",
  x_levels = era_observed_levels,
  title_text = "2x2 paths: 50+ projects and project scale",
  subtitle_text = "Observed DCP permit-year outcomes only.",
  y_labels = list(projects_per_cd_year = "50+ projects per CD-year", probability = "Probability of any 50+ project", mean_units = "Mean units per NB project"),
  out_pdf_path = out_project_count_pdf
)

cat("Wrote redevelopment descriptive outputs to", dirname(out_redev_by_treat_csv), "\n")
