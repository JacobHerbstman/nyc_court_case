# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/summarize_cd_homeownership_long_units_levels_developability/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tidyr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

assert_unique_keys <- function(df, keys, label) {
  duplicate_keys <- df |>
    count(across(all_of(keys)), name = "n") |>
    filter(n > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(label, " is not unique by ", paste(keys, collapse = ", "), ".")
  }
}

district_lookup <- read_csv("../input/cd_homeownership_long_units_series.csv", show_col_types = FALSE, na = c("", "NA")) |>
  distinct(borocd, borough_code, borough_name, treat_pp, occupied_units_1990) |>
  mutate(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    treat_pp = suppressWarnings(as.numeric(treat_pp)),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990))
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

assert_unique_keys(district_lookup, "borocd", "levels/developability district lookup")

if (n_distinct(district_lookup$borocd) != 59) {
  stop("Expected the levels/developability district lookup to cover 59 community districts.")
}

hdb_file <- read_csv("../input/dcp_housing_database_files.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(parquet_path = paste0("../../../stage_dcp_housing_database/output/", basename(parquet_path))) |>
  filter(source_id == "dcp_housing_database_project_level", !is.na(parquet_path), file.exists(parquet_path)) |>
  mutate(
    vintage = as.character(vintage),
    vintage_year = suppressWarnings(as.integer(str_extract(vintage, "^[0-9]{2}"))),
    vintage_quarter = suppressWarnings(as.integer(str_extract(str_to_lower(vintage), "(?<=q)[1-4]$"))),
    vintage_order = 4L * vintage_year + vintage_quarter
  ) |>
  arrange(desc(vintage_order), desc(vintage), parquet_path) |>
  slice_head(n = 1)

if (nrow(hdb_file) == 0) {
  stop("Could not find a staged DCP Housing Database project-level parquet in ../input/dcp_housing_database_files.csv")
}

mappluto_file <- read_csv("../input/mappluto_lot_files.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(parquet_path = paste0("../../../stage_mappluto_lots/output/", basename(parquet_path))) |>
  filter(!is.na(parquet_path), file.exists(parquet_path)) |>
  mutate(
    source_priority = if_else(source_id == "dcp_mappluto_current", 1L, 0L),
    vintage = as.character(vintage),
    vintage_order = release_order_key(vintage)
  ) |>
  arrange(desc(source_priority), desc(vintage_order), desc(vintage), parquet_path) |>
  slice_head(n = 1)

if (nrow(mappluto_file) == 0) {
  stop("Could not find a staged MapPLUTO lot parquet in ../input/mappluto_lot_files.csv")
}

units_year_df <- read_csv("../input/cd_homeownership_long_units_series.csv", show_col_types = FALSE, na = c("", "NA")) |>
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
    outcome_value = suppressWarnings(as.numeric(outcome_value))
  ) |>
  left_join(
    district_lookup |>
      select(borocd, borough_code, borough_name, treat_tercile, treat_tercile_label, occupied_units_1990),
    by = c("borocd", "borough_code", "borough_name"),
    relationship = "many-to-one"
  ) |>
  group_by(series_family, year, treat_tercile, treat_tercile_label) |>
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    occupied_units_1990 = sum(distinct(data.frame(borocd, occupied_units_1990))$occupied_units_1990, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    metric = case_when(
      series_family == "units_built_total" ~ "units_built_total_per_10000_occupied_1990",
      TRUE ~ "units_built_50_plus_per_10000_occupied_1990"
    ),
    metric_value = if_else(occupied_units_1990 > 0, 10000 * outcome_value / occupied_units_1990, NA_real_)
  ) |>
  select(year, treat_tercile, treat_tercile_label, metric, metric_value)

projects_proxy_df <- read_csv("../input/mappluto_construction_proxy_cd_year.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(yearbuilt)),
    project_count_50_plus = suppressWarnings(as.numeric(lots_50_plus_proxy))
  ) |>
  filter(year >= 1980, year <= 2009)

projects_observed_df <- read_parquet(
  hdb_file$parquet_path[[1]],
  col_select = c("completion_year", "community_district", "borough_code", "borough_name", "job_type", "classa_prop", "classa_net")
) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(community_district))),
    year = suppressWarnings(as.integer(completion_year)),
    job_type = as.character(job_type),
    classa_prop = suppressWarnings(as.numeric(classa_prop)),
    classa_net = suppressWarnings(as.numeric(classa_net))
  ) |>
  filter(
    borocd %in% district_lookup$borocd,
    year >= 2010,
    year <= 2025
  ) |>
  left_join(
    district_lookup |>
      distinct(borocd, borough_code, borough_name),
    by = "borocd",
    relationship = "many-to-one"
  ) |>
  group_by(borocd, borough_code, borough_name, year) |>
  summarize(
    project_count_50_plus = sum(job_type == "New Building" & !is.na(classa_prop) & classa_prop >= 50, na.rm = TRUE),
    gross_add_units = sum(case_when(
      job_type == "New Building" ~ pmax(coalesce(classa_prop, 0), 0),
      job_type == "Alteration" ~ pmax(coalesce(classa_net, 0), 0),
      TRUE ~ 0
    ), na.rm = TRUE),
    units_50_plus_observed = sum(if_else(job_type == "New Building" & !is.na(classa_prop) & classa_prop >= 50, classa_prop, 0), na.rm = TRUE),
    .groups = "drop"
  )

project_panel_df <- bind_rows(
  projects_proxy_df |>
    mutate(gross_add_units = NA_real_, units_50_plus_observed = NA_real_),
  projects_observed_df
) |>
  right_join(
    expand_grid(
      district_lookup |>
        select(borocd, borough_code, borough_name, treat_tercile, treat_tercile_label),
      year = 1980:2025
    ),
    by = c("borocd", "borough_code", "borough_name", "year"),
    relationship = "one-to-one"
  ) |>
  mutate(
    project_count_50_plus = coalesce(project_count_50_plus, 0),
    gross_add_units = coalesce(gross_add_units, 0),
    units_50_plus_observed = coalesce(units_50_plus_observed, 0)
  )

project_year_df <- project_panel_df |>
  group_by(year, treat_tercile, treat_tercile_label) |>
  summarize(
    cd_count = n_distinct(borocd),
    project_count_50_plus = sum(project_count_50_plus, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    metric = "projects_50_plus_per_cd_year",
    metric_value = if_else(cd_count > 0, project_count_50_plus / cd_count, NA_real_)
  ) |>
  select(year, treat_tercile, treat_tercile_label, metric, metric_value)

built_form_df <- read_parquet(
  mappluto_file$parquet_path[[1]],
  col_select = c("cd", "unitsres", "lotarea", "builtfar", "residfar", "landuse")
) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(cd))),
    unitsres = suppressWarnings(as.numeric(unitsres)),
    lotarea = suppressWarnings(as.numeric(lotarea)),
    builtfar = suppressWarnings(as.numeric(builtfar)),
    residfar = suppressWarnings(as.numeric(residfar)),
    landuse = as.character(landuse)
  ) |>
  filter(borocd %in% district_lookup$borocd) |>
  mutate(
    is_residential_lot = coalesce(unitsres, 0) > 0,
    is_one_two_family = is_residential_lot & unitsres <= 2,
    is_five_plus = is_residential_lot & unitsres >= 5,
    is_vacant_land = landuse == "11",
    underbuilt_gap = if_else(is_residential_lot & residfar > 0, pmax(residfar - builtfar, 0), NA_real_),
    is_underbuilt_res = if_else(is_residential_lot & residfar > 0, underbuilt_gap > 0.01, FALSE)
  ) |>
  group_by(borocd) |>
  summarize(
    residential_acres_current = sum(lotarea[is_residential_lot], na.rm = TRUE) / 43560,
    residential_lot_count_current = sum(is_residential_lot, na.rm = TRUE),
    one_two_family_lot_share_current = mean(is_one_two_family[is_residential_lot], na.rm = TRUE),
    five_plus_lot_share_current = mean(is_five_plus[is_residential_lot], na.rm = TRUE),
    vacant_land_share_current = mean(is_vacant_land, na.rm = TRUE),
    mean_res_lot_area_current = mean(lotarea[is_residential_lot], na.rm = TRUE),
    mean_builtfar_res_current = mean(builtfar[is_residential_lot], na.rm = TRUE),
    mean_residfar_res_current = mean(residfar[is_residential_lot], na.rm = TRUE),
    underbuilt_res_lot_share_current = mean(is_underbuilt_res[is_residential_lot], na.rm = TRUE),
    mean_underbuilt_far_gap_current = mean(underbuilt_gap[is_residential_lot], na.rm = TRUE),
    .groups = "drop"
  ) |>
  left_join(
    district_lookup |>
      select(borocd, borough_code, borough_name, treat_pp, treat_tercile, treat_tercile_label),
    by = "borocd",
    relationship = "many-to-one"
  )

write_csv(built_form_df, "../output/cd_homeownership_long_units_built_form_controls.csv", na = "")

gross_add_year_df <- project_panel_df |>
  filter(year >= 2010) |>
  left_join(
    built_form_df |>
      select(borocd, residential_acres_current),
    by = "borocd",
    relationship = "many-to-one"
  ) |>
  group_by(year, treat_tercile, treat_tercile_label) |>
  summarize(
    gross_add_units = sum(gross_add_units, na.rm = TRUE),
    residential_acres_current = sum(distinct(data.frame(borocd, residential_acres_current))$residential_acres_current, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    metric = "gross_add_units_per_res_acre",
    metric_value = if_else(residential_acres_current > 0, gross_add_units / residential_acres_current, NA_real_)
  ) |>
  select(year, treat_tercile, treat_tercile_label, metric, metric_value)

level_year_df <- bind_rows(units_year_df, project_year_df, gross_add_year_df) |>
  mutate(source_period = if_else(year < 2010, "pre_2010_proxy", "post_2010_observed")) |>
  arrange(metric, year, treat_tercile)

write_csv(level_year_df, "../output/cd_homeownership_long_units_level_year.csv", na = "")

level_era_df <- level_year_df |>
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
  filter(!is.na(era)) |>
  group_by(metric, era, treat_tercile, treat_tercile_label) |>
  summarize(metric_value = mean(metric_value, na.rm = TRUE), .groups = "drop") |>
  arrange(metric, era, treat_tercile)

write_csv(level_era_df, "../output/cd_homeownership_long_units_level_era.csv", na = "")

baseline_controls_df <- read_csv("../input/cd_baseline_1990_controls.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    vacancy_rate_1990_exact = suppressWarnings(as.numeric(vacancy_rate_1990_exact)),
    structure_share_1_2_units_1990_exact = suppressWarnings(as.numeric(structure_share_1_2_units_1990_exact)),
    structure_share_5_plus_units_1990_exact = suppressWarnings(as.numeric(structure_share_5_plus_units_1990_exact)),
    subway_commute_share_1990_exact = suppressWarnings(as.numeric(subway_commute_share_1990_exact))
  )

residual_base_df <- expand_grid(
  district_lookup |>
    select(borocd, borough_code, borough_name, treat_pp),
  year = 2010:2025
) |>
  left_join(
    projects_observed_df |>
      select(borocd, borough_code, borough_name, year, units_50_plus_observed, gross_add_units),
    by = c("borocd", "borough_code", "borough_name", "year"),
    relationship = "many-to-one"
  ) |>
  mutate(
    units_50_plus_observed = coalesce(units_50_plus_observed, 0),
    gross_add_units = coalesce(gross_add_units, 0)
  ) |>
  group_by(borocd, borough_code, borough_name, treat_pp) |>
  summarize(
    avg_annual_50_plus_units_2010_2025 = mean(units_50_plus_observed, na.rm = TRUE),
    avg_annual_gross_add_units_2010_2025 = mean(gross_add_units, na.rm = TRUE),
    .groups = "drop"
  ) |>
  left_join(baseline_controls_df, by = c("borocd", "borough_code", "borough_name"), relationship = "many-to-one") |>
  left_join(
    built_form_df |>
      select(
        borocd,
        residential_acres_current,
        one_two_family_lot_share_current,
        vacant_land_share_current,
        underbuilt_res_lot_share_current,
        mean_residfar_res_current
      ),
    by = "borocd",
    relationship = "many-to-one"
  )

residual_control_cols <- c(
  "vacancy_rate_1990_exact",
  "structure_share_1_2_units_1990_exact",
  "structure_share_5_plus_units_1990_exact",
  "subway_commute_share_1990_exact",
  "residential_acres_current",
  "one_two_family_lot_share_current",
  "vacant_land_share_current",
  "underbuilt_res_lot_share_current",
  "mean_residfar_res_current"
)

fit_residual_table <- function(df, outcome_name) {
  model_df <- df |>
    select(
      borocd,
      borough_code,
      borough_name,
      treat_pp,
      all_of(outcome_name),
      all_of(residual_control_cols)
    ) |>
    filter(if_all(all_of(c("treat_pp", outcome_name, residual_control_cols)), ~ !is.na(.x)))

  if (nrow(model_df) < 10) {
    stop("Too few complete cases to residualize ", outcome_name, ".")
  }

  outcome_formula <- as.formula(paste0(
    outcome_name,
    " ~ factor(borough_name) + vacancy_rate_1990_exact + structure_share_1_2_units_1990_exact + ",
    "structure_share_5_plus_units_1990_exact + subway_commute_share_1990_exact + residential_acres_current + ",
    "one_two_family_lot_share_current + vacant_land_share_current + underbuilt_res_lot_share_current + mean_residfar_res_current"
  ))
  treat_formula <- as.formula(paste0(
    "treat_pp ~ factor(borough_name) + vacancy_rate_1990_exact + structure_share_1_2_units_1990_exact + ",
    "structure_share_5_plus_units_1990_exact + subway_commute_share_1990_exact + residential_acres_current + ",
    "one_two_family_lot_share_current + vacant_land_share_current + underbuilt_res_lot_share_current + mean_residfar_res_current"
  ))

  outcome_fit <- lm(outcome_formula, data = model_df)
  treat_fit <- lm(treat_formula, data = model_df)
  outcome_values <- model_df[[outcome_name]]

  tibble(
    outcome_name = as.character(outcome_name),
    borocd = model_df$borocd,
    borough_code = model_df$borough_code,
    borough_name = model_df$borough_name,
    treat_pp = model_df$treat_pp,
    outcome_value = outcome_values,
    treat_residual = resid(treat_fit),
    outcome_residual = resid(outcome_fit)
  )
}

residuals_df <- bind_rows(
  fit_residual_table(residual_base_df, "avg_annual_50_plus_units_2010_2025"),
  fit_residual_table(residual_base_df, "avg_annual_gross_add_units_2010_2025")
) |>
  mutate(
    outcome_label = case_when(
      outcome_name == "avg_annual_50_plus_units_2010_2025" ~ "Avg annual 50+ units, 2010-2025",
      TRUE ~ "Avg annual gross additions, 2010-2025"
    )
  ) |>
  left_join(
    district_lookup |>
      select(borocd, treat_tercile, treat_tercile_label),
    by = "borocd",
    relationship = "many-to-one"
  )

write_csv(residuals_df, "../output/cd_homeownership_long_units_developability_residuals.csv", na = "")

pdf("../output/cd_homeownership_long_units_levels_developability_plots.pdf", width = 11, height = 8.5)

plot_year_df <- level_year_df |>
  filter(metric %in% c(
    "units_built_total_per_10000_occupied_1990",
    "units_built_50_plus_per_10000_occupied_1990",
    "projects_50_plus_per_cd_year"
  )) |>
  mutate(
    metric_label = case_when(
      metric == "units_built_total_per_10000_occupied_1990" ~ "Total units per 10,000 occupied units (1990)",
      metric == "units_built_50_plus_per_10000_occupied_1990" ~ "50+ units per 10,000 occupied units (1990)",
      TRUE ~ "50+ projects per CD-year"
    ),
    source_period = factor(source_period, levels = c("pre_2010_proxy", "post_2010_observed")),
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High"))
  )

print(
  ggplot(
    plot_year_df,
    aes(x = year, y = metric_value, color = treat_tercile_label, group = interaction(treat_tercile_label, source_period))
  ) +
    geom_line(linewidth = 0.8) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666", linewidth = 0.4) +
    facet_wrap(~metric_label, ncol = 1, scales = "free_y") +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = NULL, color = "Treat tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)

gross_plot_df <- level_year_df |>
  filter(metric == "gross_add_units_per_res_acre") |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High"))
  )

print(
  ggplot(gross_plot_df, aes(x = year, y = metric_value, color = treat_tercile_label)) +
    geom_line(linewidth = 0.8) +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = "Gross additions per residential acre", color = "Treat tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)

residual_plot_df <- residuals_df |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High"))
  )

print(
  ggplot(residual_plot_df, aes(x = treat_residual, y = outcome_residual, color = treat_tercile_label)) +
    geom_point(alpha = 0.75, size = 2) +
    geom_smooth(method = "lm", se = FALSE, color = "#222222", linewidth = 0.7) +
    facet_wrap(~outcome_label, scales = "free_y") +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = "Treatment residual", y = "Outcome residual", color = "Treat tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)

dev.off()

qc_df <- bind_rows(
  tibble(metric = "district_count", value = n_distinct(district_lookup$borocd), note = "Districts in the levels and developability diagnostics."),
  tibble(metric = "hdb_vintage_order", value = as.numeric(hdb_file$vintage_order[[1]]), note = paste0("Selected DCP Housing Database vintage ", hdb_file$vintage[[1]], ".")),
  tibble(metric = "mappluto_vintage_order", value = as.numeric(mappluto_file$vintage_order[[1]]), note = paste0("Selected MapPLUTO source ", mappluto_file$source_id[[1]], " vintage ", mappluto_file$vintage[[1]], ".")),
  tibble(metric = "built_form_row_count", value = nrow(built_form_df), note = "Rows in the current MapPLUTO built-form control table."),
  tibble(metric = "post_2010_project_panel_gap_count", value = 59L * length(2010:2025) - nrow(filter(project_panel_df, year >= 2010)), note = "Missing standard CD-years after balancing observed HDB project counts to zero."),
  tibble(metric = "residual_row_count", value = nrow(residuals_df), note = "Rows in the residualized cross-section output."),
  tibble(metric = "avg_annual_50_plus_units_residual_corr", value = cor(
    residuals_df$treat_residual[residuals_df$outcome_name == "avg_annual_50_plus_units_2010_2025"],
    residuals_df$outcome_residual[residuals_df$outcome_name == "avg_annual_50_plus_units_2010_2025"],
    use = "complete.obs"
  ), note = "Residual correlation for avg annual 50+ units, 2010-2025."),
  tibble(metric = "avg_annual_gross_add_units_residual_corr", value = cor(
    residuals_df$treat_residual[residuals_df$outcome_name == "avg_annual_gross_add_units_2010_2025"],
    residuals_df$outcome_residual[residuals_df$outcome_name == "avg_annual_gross_add_units_2010_2025"],
    use = "complete.obs"
  ), note = "Residual correlation for avg annual gross additions, 2010-2025.")
)

write_csv(qc_df, "../output/cd_homeownership_long_units_levels_developability_qc.csv", na = "")

cat("Wrote levels and developability outputs to ../output\n")
