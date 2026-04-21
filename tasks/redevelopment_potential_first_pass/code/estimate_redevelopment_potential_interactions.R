# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/redevelopment_potential_first_pass/code")
# redev_baseline_csv <- "../output/cd_redevelopment_potential_baseline.csv"
# long_units_series_csv <- "../input/cd_homeownership_long_units_series.csv"
# dcp_supply_panel_csv <- "../input/cd_homeownership_dcp_supply_panel.csv"
# dob_nb_panel_csv <- "../input/cd_homeownership_permit_nb_panel.csv"
# proxy_lot_level_parquet <- "../input/mappluto_construction_proxy_lot_level.parquet"
# out_model_summary_csv <- "../output/tables/redev_interaction_model_summary.csv"
# out_nested_diag_csv <- "../output/tables/nested_controls_coefficient_diagnostics.csv"
# out_borough_sensitivity_csv <- "../output/tables/borough_sensitivity_redev.csv"
# out_manhattan_anatomy_csv <- "../output/tables/manhattan_baseline_anatomy.csv"
# out_nested_pdf <- "../output/figures/nested_controls_coefficients.pdf"
# out_event_pdf <- "../output/figures/redev_interaction_event_coefficients.pdf"
# out_borough_paths_pdf <- "../output/figures/borough_specific_two_by_two_paths.pdf"
# out_non_manhattan_pdf <- "../output/figures/non_manhattan_redev_paths.pdf"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tidyr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 13) {
  stop("Expected 13 arguments: redev_baseline_csv long_units_series_csv dcp_supply_panel_csv dob_nb_panel_csv proxy_lot_level_parquet out_model_summary_csv out_nested_diag_csv out_borough_sensitivity_csv out_manhattan_anatomy_csv out_nested_pdf out_event_pdf out_borough_paths_pdf out_non_manhattan_pdf")
}

redev_baseline_csv <- args[1]
long_units_series_csv <- args[2]
dcp_supply_panel_csv <- args[3]
dob_nb_panel_csv <- args[4]
proxy_lot_level_parquet <- args[5]
out_model_summary_csv <- args[6]
out_nested_diag_csv <- args[7]
out_borough_sensitivity_csv <- args[8]
out_manhattan_anatomy_csv <- args[9]
out_nested_pdf <- args[10]
out_event_pdf <- args[11]
out_borough_paths_pdf <- args[12]
out_non_manhattan_pdf <- args[13]

sanitize_era <- function(x) {
  str_replace_all(x, "-", "_")
}

add_terms <- function(df, variable_names, eras) {
  out_df <- df
  for (variable_name in variable_names) {
    for (era_value in eras) {
      out_df[[paste0(variable_name, "_x_", sanitize_era(era_value))]] <- out_df[[variable_name]] * as.integer(out_df$era == era_value)
    }
  }
  out_df
}

extract_term_rows <- function(model, requested_terms, analysis_family, sample_label, outcome_family, functional_form, control_layer, index_name, reference_era, year_min, year_max) {
  coef_df <- as.data.frame(coeftable(model))
  coef_df$term <- rownames(coef_df)
  rownames(coef_df) <- NULL
  p_value_col <- names(coef_df)[str_detect(names(coef_df), "^Pr\\(")][1]
  conf_df <- as.data.frame(confint(model))
  conf_df$term <- rownames(conf_df)
  rownames(conf_df) <- NULL
  names(conf_df)[1:2] <- c("conf_low", "conf_high")

  tibble(term = requested_terms) |>
    left_join(coef_df, by = "term") |>
    left_join(conf_df, by = "term") |>
    mutate(
      analysis_family = analysis_family,
      sample_label = sample_label,
      outcome_family = outcome_family,
      functional_form = functional_form,
      control_layer = control_layer,
      index_name = index_name,
      reference_era = reference_era,
      year_min = year_min,
      year_max = year_max,
      estimate = Estimate,
      std_error = `Std. Error`,
      p_value = .data[[p_value_col]],
      n_obs = nobs(model)
    ) |>
    select(analysis_family, sample_label, outcome_family, functional_form, control_layer, index_name, reference_era, year_min, year_max, term, estimate, std_error, p_value, conf_low, conf_high, n_obs)
}

classify_attenuation <- function(base_estimate, full_estimate, base_se, full_se) {
  if (is.na(base_estimate) || is.na(full_estimate) || is.na(base_se) || is.na(full_se)) {
    return("missing")
  }
  if (abs(base_estimate) > 0 && abs(full_estimate) < 0.75 * abs(base_estimate)) {
    return("coefficient_movement")
  }
  if (full_se > 1.25 * base_se) {
    return("precision_loss")
  }
  "mixed_or_stable"
}

base_df <- read_csv(redev_baseline_csv, show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name
  ) |>
  group_by(borough_code, borough_name) |>
  mutate(
    high_homeowner = treat_z_boro >= median(treat_z_boro, na.rm = TRUE),
    high_redev_A = redev_potential_A_z_boro >= median(redev_potential_A_z_boro, na.rm = TRUE),
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
  ) |>
  ungroup()

long_df <- read_csv(long_units_series_csv, show_col_types = FALSE, na = c("", "NA")) |>
  filter(series_kind == "preferred_long_series", series_family %in% c("units_built_total", "units_built_50_plus")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(year)),
    outcome_family = series_family,
    outcome_value = suppressWarnings(as.numeric(outcome_value))
  ) |>
  left_join(
    base_df |>
      select(
        borocd, borough_code, borough_name, treat_z_boro, redev_potential_A_z_boro, redev_potential_C_z_boro,
        occupied_units_1990, residential_acres, borough_name,
        total_housing_units_growth_1980_1990_approx, occupied_units_growth_1980_1990_approx,
        vacancy_rate_change_1980_1990_pp_approx, homeowner_share_change_1980_1990_pp_approx,
        vacancy_rate_1990_exact, structure_share_1_2_units_1990_exact, structure_share_3_4_units_1990_exact,
        structure_share_5_plus_units_1990_exact, median_household_income_1990_1999_dollars_exact,
        poverty_share_1990_exact, median_housing_value_1990_2000_dollars_exact_filled,
        foreign_born_share_1990_exact, college_graduate_share_1990_exact, unemployment_rate_1990_exact,
        subway_commute_share_1990_exact, mean_commute_time_1990_minutes_exact,
        cd_mean_built_far_lot_weighted, cd_mean_max_resid_far_lot_weighted,
        cd_share_lot_area_one_two_family, cd_share_lot_area_vacant,
        cd_share_lot_area_old_building, cd_share_lot_area_protected,
        cd_share_lot_area_parking_or_low_intensity, two_by_two_cell, two_by_two_label
      ),
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
    borough_year = interaction(borough_name, year, drop = TRUE),
    outcome_occ = 10000 * outcome_value / occupied_units_1990,
    outcome_acre = outcome_value / residential_acres,
    triple_A = treat_z_boro * redev_potential_A_z_boro,
    triple_C = treat_z_boro * redev_potential_C_z_boro
  ) |>
  filter(!is.na(era))

dcp_df <- read_csv(dcp_supply_panel_csv, show_col_types = FALSE, na = c("", "NA")) |>
  filter(year >= 2010, outcome_family %in% c("gross_add_units", "nb_gross_units", "nb_gross_units_50_plus", "nb_project_count", "nb_project_count_50_plus")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(year)),
    outcome_family = outcome_family,
    outcome_value = suppressWarnings(as.numeric(outcome_value))
  ) |>
  left_join(
    base_df |>
      select(
        borocd, borough_code, borough_name, treat_z_boro, redev_potential_A_z_boro, redev_potential_C_z_boro,
        occupied_units_1990, residential_acres,
        total_housing_units_growth_1980_1990_approx, occupied_units_growth_1980_1990_approx,
        vacancy_rate_change_1980_1990_pp_approx, homeowner_share_change_1980_1990_pp_approx,
        vacancy_rate_1990_exact, structure_share_1_2_units_1990_exact, structure_share_3_4_units_1990_exact,
        structure_share_5_plus_units_1990_exact, median_household_income_1990_1999_dollars_exact,
        poverty_share_1990_exact, median_housing_value_1990_2000_dollars_exact_filled,
        foreign_born_share_1990_exact, college_graduate_share_1990_exact, unemployment_rate_1990_exact,
        subway_commute_share_1990_exact, mean_commute_time_1990_minutes_exact,
        cd_mean_built_far_lot_weighted, cd_mean_max_resid_far_lot_weighted,
        cd_share_lot_area_one_two_family, cd_share_lot_area_vacant,
        cd_share_lot_area_old_building, cd_share_lot_area_protected,
        cd_share_lot_area_parking_or_low_intensity, two_by_two_cell, two_by_two_label
      ),
    by = c("borocd", "borough_code", "borough_name")
  ) |>
  mutate(
    era = case_when(
      year >= 2010 & year <= 2014 ~ "2010-2014",
      year >= 2015 & year <= 2019 ~ "2015-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    ),
    borough_year = interaction(borough_name, year, drop = TRUE),
    outcome_occ = 10000 * outcome_value / occupied_units_1990,
    outcome_acre = outcome_value / residential_acres,
    triple_A = treat_z_boro * redev_potential_A_z_boro,
    triple_C = treat_z_boro * redev_potential_C_z_boro
  ) |>
  filter(!is.na(era))

observed_aux_df <- dcp_df |>
  select(borocd, borough_code, borough_name, year, outcome_family, outcome_value) |>
  pivot_wider(names_from = outcome_family, values_from = outcome_value, values_fill = 0) |>
  mutate(
    any_50plus_project = as.numeric(nb_project_count_50_plus > 0),
    mean_units_per_nb_project = if_else(nb_project_count > 0, nb_gross_units / nb_project_count, NA_real_)
  ) |>
  left_join(
    base_df |>
      select(
        borocd, borough_code, borough_name, treat_z_boro, redev_potential_A_z_boro, redev_potential_C_z_boro,
        occupied_units_1990, residential_acres, total_housing_units_growth_1980_1990_approx,
        occupied_units_growth_1980_1990_approx, vacancy_rate_change_1980_1990_pp_approx,
        homeowner_share_change_1980_1990_pp_approx, vacancy_rate_1990_exact,
        structure_share_1_2_units_1990_exact, structure_share_3_4_units_1990_exact,
        structure_share_5_plus_units_1990_exact, median_household_income_1990_1999_dollars_exact,
        poverty_share_1990_exact, median_housing_value_1990_2000_dollars_exact_filled,
        foreign_born_share_1990_exact, college_graduate_share_1990_exact, unemployment_rate_1990_exact,
        subway_commute_share_1990_exact, mean_commute_time_1990_minutes_exact,
        cd_mean_built_far_lot_weighted, cd_mean_max_resid_far_lot_weighted,
        cd_share_lot_area_one_two_family, cd_share_lot_area_vacant,
        cd_share_lot_area_old_building, cd_share_lot_area_protected,
        cd_share_lot_area_parking_or_low_intensity, two_by_two_cell, two_by_two_label
      ),
    by = c("borocd", "borough_code", "borough_name")
  ) |>
  mutate(
    era = case_when(
      year >= 2010 & year <= 2014 ~ "2010-2014",
      year >= 2015 & year <= 2019 ~ "2015-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    ),
    borough_year = interaction(borough_name, year, drop = TRUE),
    triple_A = treat_z_boro * redev_potential_A_z_boro,
    triple_C = treat_z_boro * redev_potential_C_z_boro
  ) |>
  filter(!is.na(era))

top5_exclusion_df <- long_df |>
  filter(outcome_family == "units_built_50_plus") |>
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
  ) |>
  group_by(borocd, era) |>
  summarize(outcome_value = sum(outcome_value, na.rm = TRUE), .groups = "drop") |>
  arrange(desc(outcome_value)) |>
  slice_head(n = 5)

top5_year_filter_df <- top5_exclusion_df |>
  rowwise() |>
  mutate(
    years = list(
      if (era == "1980-1984") {
        1980:1984
      } else if (era == "1985-1989") {
        1985:1989
      } else if (era == "1990-1999") {
        1990:1999
      } else if (era == "2000-2009") {
        2000:2009
      } else if (era == "2010-2019") {
        2010:2019
      } else {
        2020:2025
      }
    )
  ) |>
  ungroup() |>
  unnest(years) |>
  rename(year = years)

control_blocks <- list(
  `0_fe_only` = character(),
  `1_pretrends` = c(
    "total_housing_units_growth_1980_1990_approx",
    "occupied_units_growth_1980_1990_approx",
    "vacancy_rate_change_1980_1990_pp_approx",
    "homeowner_share_change_1980_1990_pp_approx"
  ),
  `2_exact_1990` = c(
    "vacancy_rate_1990_exact",
    "structure_share_1_2_units_1990_exact",
    "structure_share_3_4_units_1990_exact",
    "structure_share_5_plus_units_1990_exact",
    "median_household_income_1990_1999_dollars_exact",
    "poverty_share_1990_exact",
    "median_housing_value_1990_2000_dollars_exact_filled",
    "foreign_born_share_1990_exact",
    "college_graduate_share_1990_exact",
    "unemployment_rate_1990_exact",
    "subway_commute_share_1990_exact",
    "mean_commute_time_1990_minutes_exact"
  ),
  `3_built_form` = c(
    "cd_mean_built_far_lot_weighted",
    "cd_mean_max_resid_far_lot_weighted",
    "cd_share_lot_area_one_two_family",
    "cd_share_lot_area_vacant",
    "cd_share_lot_area_old_building",
    "cd_share_lot_area_protected",
    "cd_share_lot_area_parking_or_low_intensity"
  ),
  `4_all_blocks` = c(
    "total_housing_units_growth_1980_1990_approx",
    "occupied_units_growth_1980_1990_approx",
    "vacancy_rate_change_1980_1990_pp_approx",
    "homeowner_share_change_1980_1990_pp_approx",
    "vacancy_rate_1990_exact",
    "structure_share_1_2_units_1990_exact",
    "structure_share_3_4_units_1990_exact",
    "structure_share_5_plus_units_1990_exact",
    "median_household_income_1990_1999_dollars_exact",
    "poverty_share_1990_exact",
    "median_housing_value_1990_2000_dollars_exact_filled",
    "foreign_born_share_1990_exact",
    "college_graduate_share_1990_exact",
    "unemployment_rate_1990_exact",
    "subway_commute_share_1990_exact",
    "mean_commute_time_1990_minutes_exact",
    "cd_mean_built_far_lot_weighted",
    "cd_mean_max_resid_far_lot_weighted",
    "cd_share_lot_area_one_two_family",
    "cd_share_lot_area_vacant",
    "cd_share_lot_area_old_building",
    "cd_share_lot_area_protected",
    "cd_share_lot_area_parking_or_low_intensity"
  )
)

sample_filters <- list(
  all_nyc = function(df) df,
  non_manhattan_only = function(df) filter(df, borough_name != "Manhattan"),
  manhattan_only = function(df) filter(df, borough_name == "Manhattan"),
  bronx_brooklyn_queens_only = function(df) filter(df, borough_name %in% c("Bronx", "Brooklyn", "Queens")),
  leave_out_bronx = function(df) filter(df, borough_name != "Bronx"),
  leave_out_brooklyn = function(df) filter(df, borough_name != "Brooklyn"),
  leave_out_manhattan = function(df) filter(df, borough_name != "Manhattan"),
  leave_out_queens = function(df) filter(df, borough_name != "Queens"),
  leave_out_staten_island = function(df) filter(df, borough_name != "Staten Island"),
  drop_top5_era_contributors = function(df) anti_join(df, top5_year_filter_df, by = c("borocd", "year")),
  drop_101_105_106_108 = function(df) filter(df, !borocd %in% c("101", "105", "106", "108"))
)

results_rows <- list()
result_index <- 1L

run_model_block <- function(work_df, outcome_var, raw_outcome_var, exposure_var, eras, reference_era, analysis_family, sample_label, outcome_family, functional_form, control_layer, index_name, allow_ppml = FALSE) {
  control_vars <- control_blocks[[control_layer]]
  index_var <- paste0("redev_potential_", index_name, "_z_boro")
  triple_var <- paste0("triple_", index_name)

  work_df <- add_terms(work_df, c("treat_z_boro", index_var, triple_var, control_vars), eras)
  treat_terms <- paste0("treat_z_boro_x_", sanitize_era(eras))
  redev_terms <- paste0(index_var, "_x_", sanitize_era(eras))
  triple_terms <- paste0(triple_var, "_x_", sanitize_era(eras))
  control_terms <- unlist(lapply(control_vars, function(x) paste0(x, "_x_", sanitize_era(eras))))

  if (!allow_ppml) {
    formula_terms <- c(treat_terms, redev_terms, triple_terms, control_terms)
    model <- tryCatch(
      feols(as.formula(paste0(outcome_var, " ~ ", paste(formula_terms, collapse = " + "), " | borocd + borough_year")), data = work_df, cluster = ~borocd),
      error = function(e) NULL
    )
  } else {
    formula_terms <- c(treat_terms, redev_terms, triple_terms, control_terms)
    model <- tryCatch(
      fepois(as.formula(paste0(raw_outcome_var, " ~ ", paste(formula_terms, collapse = " + "), " | borocd + borough_year")), data = work_df, cluster = ~borocd, offset = log(work_df[[exposure_var]])),
      error = function(e) NULL
    )
  }

  if (is.null(model)) {
    return(NULL)
  }

  term_rows <- bind_rows(
    extract_term_rows(model, treat_terms, analysis_family, sample_label, outcome_family, functional_form, control_layer, index_name, reference_era, min(work_df$year), max(work_df$year)) |>
      mutate(term_group = "homeowner"),
    extract_term_rows(model, triple_terms, analysis_family, sample_label, outcome_family, functional_form, control_layer, index_name, reference_era, min(work_df$year), max(work_df$year)) |>
      mutate(term_group = "homeowner_x_redev")
  ) |>
    mutate(
      era = case_when(
        str_detect(term, "1980_1984") ~ "1980-1984",
        str_detect(term, "1990_1999") ~ "1990-1999",
        str_detect(term, "2000_2009") ~ "2000-2009",
        str_detect(term, "2010_2019") ~ "2010-2019",
        str_detect(term, "2020_2025") ~ "2020-2025",
        str_detect(term, "2015_2019") ~ "2015-2019",
        TRUE ~ "2020-2025"
      ),
      converged = TRUE
    )

  term_rows
}

for (outcome_family in c("units_built_total", "units_built_50_plus")) {
  outcome_df <- filter(long_df, outcome_family == !!outcome_family)
  long_eras <- c("1980-1984", "1990-1999", "2000-2009", "2010-2019", "2020-2025")

  for (control_layer in names(control_blocks)) {
    for (functional_form in c("linear_occ", "linear_acre", "ppml_occ")) {
      if (functional_form == "ppml_occ") {
        model_rows <- run_model_block(outcome_df, "outcome_occ", "outcome_value", "occupied_units_1990", long_eras, "1985-1989", "long_series", "all_nyc", outcome_family, functional_form, control_layer, "A", allow_ppml = TRUE)
      } else {
        model_rows <- run_model_block(outcome_df, if_else(functional_form == "linear_occ", "outcome_occ", "outcome_acre"), "outcome_value", "occupied_units_1990", long_eras, "1985-1989", "long_series", "all_nyc", outcome_family, functional_form, control_layer, "A", allow_ppml = FALSE)
      }
      results_rows[[result_index]] <- model_rows
      result_index <- result_index + 1L
    }
  }
}

observed_families <- c("gross_add_units", "nb_gross_units", "nb_gross_units_50_plus", "nb_project_count_50_plus")
for (outcome_family in observed_families) {
  outcome_df <- filter(dcp_df, outcome_family == !!outcome_family)
  obs_eras <- c("2015-2019", "2020-2025")

  for (control_layer in names(control_blocks)) {
    for (functional_form in c("linear_occ", "linear_acre", "ppml_occ")) {
      if (functional_form == "ppml_occ") {
        model_rows <- run_model_block(outcome_df, "outcome_occ", "outcome_value", "occupied_units_1990", obs_eras, "2010-2014", "observed_supply", "all_nyc", outcome_family, functional_form, control_layer, "A", allow_ppml = TRUE)
      } else {
        model_rows <- run_model_block(outcome_df, if_else(functional_form == "linear_occ", "outcome_occ", "outcome_acre"), "outcome_value", "occupied_units_1990", obs_eras, "2010-2014", "observed_supply", "all_nyc", outcome_family, functional_form, control_layer, "A", allow_ppml = FALSE)
      }
      results_rows[[result_index]] <- model_rows
      result_index <- result_index + 1L
    }
  }
}

for (outcome_family in c("any_50plus_project", "mean_units_per_nb_project")) {
  outcome_df <- observed_aux_df |>
    transmute(
      borocd, borough_code, borough_name, year, era, borough_year,
      treat_z_boro, redev_potential_A_z_boro, redev_potential_C_z_boro, triple_A, triple_C,
      occupied_units_1990, residential_acres,
      total_housing_units_growth_1980_1990_approx, occupied_units_growth_1980_1990_approx,
      vacancy_rate_change_1980_1990_pp_approx, homeowner_share_change_1980_1990_pp_approx,
      vacancy_rate_1990_exact, structure_share_1_2_units_1990_exact, structure_share_3_4_units_1990_exact,
      structure_share_5_plus_units_1990_exact, median_household_income_1990_1999_dollars_exact,
      poverty_share_1990_exact, median_housing_value_1990_2000_dollars_exact_filled,
      foreign_born_share_1990_exact, college_graduate_share_1990_exact, unemployment_rate_1990_exact,
      subway_commute_share_1990_exact, mean_commute_time_1990_minutes_exact,
      cd_mean_built_far_lot_weighted, cd_mean_max_resid_far_lot_weighted,
      cd_share_lot_area_one_two_family, cd_share_lot_area_vacant,
      cd_share_lot_area_old_building, cd_share_lot_area_protected,
      cd_share_lot_area_parking_or_low_intensity,
      outcome_value = .data[[outcome_family]],
      outcome_occ = .data[[outcome_family]],
      outcome_acre = .data[[outcome_family]]
    ) |>
    filter(!is.na(outcome_value))

  obs_eras <- c("2015-2019", "2020-2025")
  for (control_layer in names(control_blocks)) {
    model_rows <- run_model_block(outcome_df, "outcome_value", "outcome_value", "occupied_units_1990", obs_eras, "2010-2014", "observed_supply", "all_nyc", outcome_family, if_else(outcome_family == "any_50plus_project", "linear_probability", "linear_level"), control_layer, "A", allow_ppml = FALSE)
    results_rows[[result_index]] <- model_rows
    result_index <- result_index + 1L
  }
}

for (control_layer in c("0_fe_only", "4_all_blocks")) {
  for (outcome_family in c("units_built_total", "gross_add_units", "nb_gross_units_50_plus")) {
    if (outcome_family %in% c("units_built_total")) {
      outcome_df <- filter(long_df, outcome_family == !!outcome_family)
      eras <- c("1980-1984", "1990-1999", "2000-2009", "2010-2019", "2020-2025")
      reference_era <- "1985-1989"
      analysis_family <- "long_series"
      outcome_var <- "outcome_occ"
    } else {
      outcome_df <- filter(dcp_df, outcome_family == !!outcome_family)
      eras <- c("2015-2019", "2020-2025")
      reference_era <- "2010-2014"
      analysis_family <- "observed_supply"
      outcome_var <- "outcome_occ"
    }
    model_rows <- run_model_block(outcome_df, outcome_var, "outcome_value", "occupied_units_1990", eras, reference_era, analysis_family, "all_nyc", outcome_family, "linear_occ", control_layer, "C", allow_ppml = FALSE)
    results_rows[[result_index]] <- model_rows
    result_index <- result_index + 1L
  }
}

for (sample_label in names(sample_filters)) {
  if (sample_label == "all_nyc") {
    next
  }
  for (outcome_family in c("units_built_total", "gross_add_units", "nb_gross_units_50_plus")) {
    if (outcome_family == "units_built_total") {
      outcome_df <- sample_filters[[sample_label]](filter(long_df, outcome_family == !!outcome_family))
      eras <- c("1980-1984", "1990-1999", "2000-2009", "2010-2019", "2020-2025")
      reference_era <- "1985-1989"
      analysis_family <- "long_series"
    } else {
      outcome_df <- sample_filters[[sample_label]](filter(dcp_df, outcome_family == !!outcome_family))
      eras <- c("2015-2019", "2020-2025")
      reference_era <- "2010-2014"
      analysis_family <- "observed_supply"
    }
    model_rows <- run_model_block(outcome_df, "outcome_occ", "outcome_value", "occupied_units_1990", eras, reference_era, analysis_family, sample_label, outcome_family, "linear_occ", "4_all_blocks", "A", allow_ppml = FALSE)
    results_rows[[result_index]] <- model_rows
    result_index <- result_index + 1L
  }
}

dob_df <- read_csv(dob_nb_panel_csv, show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(year)),
    outcome_value = suppressWarnings(as.numeric(outcome_value))
  ) |>
  left_join(
    base_df |>
      select(
        borocd, borough_code, borough_name, treat_z_boro, redev_potential_A_z_boro, occupied_units_1990,
        total_housing_units_growth_1980_1990_approx, occupied_units_growth_1980_1990_approx,
        vacancy_rate_change_1980_1990_pp_approx, homeowner_share_change_1980_1990_pp_approx,
        vacancy_rate_1990_exact, structure_share_1_2_units_1990_exact, structure_share_3_4_units_1990_exact,
        structure_share_5_plus_units_1990_exact, median_household_income_1990_1999_dollars_exact,
        poverty_share_1990_exact, median_housing_value_1990_2000_dollars_exact_filled,
        foreign_born_share_1990_exact, college_graduate_share_1990_exact, unemployment_rate_1990_exact,
        subway_commute_share_1990_exact, mean_commute_time_1990_minutes_exact,
        cd_mean_built_far_lot_weighted, cd_mean_max_resid_far_lot_weighted,
        cd_share_lot_area_one_two_family, cd_share_lot_area_vacant,
        cd_share_lot_area_old_building, cd_share_lot_area_protected,
        cd_share_lot_area_parking_or_low_intensity
      ),
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
    borough_year = interaction(borough_name, year, drop = TRUE),
    outcome_occ = 10000 * outcome_value / occupied_units_1990,
    triple_A = treat_z_boro * redev_potential_A_z_boro
  ) |>
  filter(!is.na(era))

results_rows[[result_index]] <- run_model_block(dob_df, "outcome_occ", "outcome_value", "occupied_units_1990", c("1980-1984", "1990-1999", "2000-2009", "2010-2019", "2020-2025"), "1985-1989", "dob_placebo", "all_nyc", "dob_nb_jobs", "linear_occ", "4_all_blocks", "A", allow_ppml = FALSE)

model_summary_df <- bind_rows(results_rows) |>
  mutate(
    era = factor(era, levels = c("1980-1984", "1990-1999", "2000-2009", "2010-2019", "2020-2025", "2015-2019")),
    control_layer = factor(control_layer, levels = names(control_blocks))
  ) |>
  arrange(analysis_family, sample_label, outcome_family, functional_form, control_layer, index_name, term_group, era)

write_csv(model_summary_df, out_model_summary_csv, na = "")

nested_diag_df <- model_summary_df |>
  filter(
    sample_label == "all_nyc",
    index_name == "A",
    functional_form == "linear_occ",
    outcome_family %in% c("units_built_total", "gross_add_units", "nb_gross_units_50_plus"),
    control_layer %in% c("0_fe_only", "4_all_blocks")
  ) |>
  group_by(outcome_family, term_group, era, control_layer) |>
  summarize(
    estimate = first(estimate),
    std_error = first(std_error),
    .groups = "drop"
  ) |>
  select(outcome_family, term_group, era, control_layer, estimate, std_error) |>
  pivot_wider(
    names_from = control_layer,
    values_from = c(estimate, std_error)
  ) |>
  mutate(
    attenuation_class = mapply(classify_attenuation, estimate_0_fe_only, estimate_4_all_blocks, std_error_0_fe_only, std_error_4_all_blocks)
  )

write_csv(nested_diag_df, out_nested_diag_csv, na = "")

borough_share_df <- bind_rows(
  long_df |>
    filter(outcome_family %in% c("units_built_total", "units_built_50_plus")) |>
    mutate(
      high_homeowner = base_df$high_homeowner[match(borocd, base_df$borocd)],
      era = factor(era, levels = c("1980-1984", "1985-1989", "1990-1999", "2000-2009", "2010-2019", "2020-2025"))
    ) |>
    group_by(borough_name, outcome_family, era, high_homeowner) |>
    summarize(units = sum(outcome_value, na.rm = TRUE), .groups = "drop") |>
    group_by(borough_name, outcome_family, era) |>
    mutate(high_homeowner_share = units / sum(units, na.rm = TRUE)) |>
    filter(high_homeowner) |>
    ungroup(),
  dcp_df |>
    filter(outcome_family == "nb_project_count_50_plus") |>
    mutate(
      high_homeowner = base_df$high_homeowner[match(borocd, base_df$borocd)],
      era = factor(case_when(
        year >= 2010 & year <= 2014 ~ "2010-2014",
        year >= 2015 & year <= 2019 ~ "2015-2019",
        TRUE ~ "2020-2025"
      ), levels = c("2010-2014", "2015-2019", "2020-2025"))
    ) |>
    group_by(borough_name, outcome_family, era, high_homeowner) |>
    summarize(units = sum(outcome_value, na.rm = TRUE), .groups = "drop") |>
    group_by(borough_name, outcome_family, era) |>
    mutate(high_homeowner_share = units / sum(units, na.rm = TRUE)) |>
    filter(high_homeowner) |>
    ungroup()
) |>
  transmute(
    row_type = "borough_high_homeowner_share",
    sample_label = NA_character_,
    borough_name,
    outcome_family,
    era = as.character(era),
    estimate = high_homeowner_share,
    std_error = NA_real_,
    p_value = NA_real_
  )

sensitivity_rows_df <- model_summary_df |>
  filter(
    sample_label %in% names(sample_filters),
    index_name == "A",
    functional_form == "linear_occ",
    control_layer == "4_all_blocks",
    term_group == "homeowner_x_redev",
    outcome_family %in% c("units_built_total", "gross_add_units", "nb_gross_units_50_plus")
  ) |>
  transmute(
    row_type = "interaction_regression",
    sample_label,
    borough_name = NA_character_,
    outcome_family,
    era = as.character(era),
    estimate,
    std_error,
    p_value
  )

borough_sensitivity_df <- bind_rows(borough_share_df, sensitivity_rows_df)
write_csv(borough_sensitivity_df, out_borough_sensitivity_csv, na = "")

manhattan_cd_df <- base_df |>
  filter(borocd %in% c("101", "105", "106", "108")) |>
  select(
    borocd, borough_name, treat_pp, treat_z_boro, redev_potential_A_z_boro, redev_potential_C_z_boro,
    cd_mean_built_far_lot_weighted, cd_mean_max_resid_far_lot_weighted, cd_mean_unused_res_far_lot_weighted
  )

manhattan_units_df <- long_df |>
  filter(borocd %in% c("101", "105", "106", "108"), outcome_family %in% c("units_built_total", "units_built_50_plus")) |>
  group_by(borocd, outcome_family, era) |>
  summarize(units = sum(outcome_value, na.rm = TRUE), .groups = "drop") |>
  filter(era %in% c("1985-1989", "1990-1999", "2010-2019", "2020-2025")) |>
  pivot_wider(names_from = c(outcome_family, era), values_from = units, values_fill = 0)
manhattan_units_df <- manhattan_units_df |>
  rename_with(~str_replace_all(., "-", "_"))

manhattan_top_lots_df <- read_parquet(proxy_lot_level_parquet) |>
  transmute(
    bbl = as.character(bbl),
    address = address,
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    yearbuilt = suppressWarnings(as.integer(yearbuilt)),
    unitsres = suppressWarnings(as.numeric(unitsres))
  ) |>
  filter(borocd %in% c("101", "105", "106", "108"), yearbuilt >= 1985, yearbuilt <= 1989) |>
  arrange(borocd, desc(unitsres)) |>
  group_by(borocd) |>
  slice_head(n = 10) |>
  mutate(rank = row_number()) |>
  ungroup()

manhattan_anatomy_df <- bind_rows(
  manhattan_cd_df |>
    left_join(manhattan_units_df, by = "borocd") |>
    mutate(row_type = "cd_summary", rank = NA_integer_, bbl = NA_character_, address = NA_character_, yearbuilt = NA_integer_, unitsres = NA_real_),
  manhattan_top_lots_df |>
    mutate(
      borough_name = "Manhattan",
      treat_pp = NA_real_,
      treat_z_boro = NA_real_,
      redev_potential_A_z_boro = NA_real_,
      redev_potential_C_z_boro = NA_real_,
      cd_mean_built_far_lot_weighted = NA_real_,
      cd_mean_max_resid_far_lot_weighted = NA_real_,
      cd_mean_unused_res_far_lot_weighted = NA_real_,
      row_type = "top_lot",
      units_built_total_1985_1989 = NA_real_,
      units_built_50_plus_1985_1989 = NA_real_,
      units_built_total_1990_1999 = NA_real_,
      units_built_total_2010_2019 = NA_real_,
      units_built_total_2020_2025 = NA_real_
    )
)

write_csv(manhattan_anatomy_df, out_manhattan_anatomy_csv, na = "")

pdf(out_nested_pdf, width = 9, height = 7)
print(
  ggplot(
    filter(model_summary_df, sample_label == "all_nyc", index_name == "A", functional_form == "linear_occ", term_group == "homeowner_x_redev", outcome_family %in% c("units_built_total", "gross_add_units", "nb_gross_units_50_plus")),
    aes(x = control_layer, y = estimate, color = era, group = era)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed") +
    geom_point(size = 2) +
    geom_line() +
    facet_wrap(~ outcome_family, scales = "free_y") +
    labs(title = "Nested-controls path for homeowner × redevelopment interaction", x = "Control layer", y = "Estimate", color = "Era") +
    theme_minimal(base_size = 11)
)
dev.off()

pdf(out_event_pdf, width = 9, height = 7)
print(
  ggplot(
    filter(model_summary_df, sample_label == "all_nyc", index_name == "A", functional_form == "linear_occ", control_layer %in% c("0_fe_only", "4_all_blocks"), term_group == "homeowner_x_redev", outcome_family %in% c("units_built_total", "gross_add_units", "nb_gross_units_50_plus")),
    aes(x = era, y = estimate, color = control_layer, group = control_layer)
  ) +
    geom_hline(yintercept = 0, linetype = "dashed") +
    geom_point(size = 2) +
    geom_line() +
    facet_wrap(~ outcome_family, scales = "free_y") +
    labs(title = "Event-style interaction coefficients", x = NULL, y = "Estimate", color = "Control layer") +
    theme_minimal(base_size = 11)
)
dev.off()

borough_path_df <- bind_rows(
  long_df |>
    filter(outcome_family %in% c("units_built_total", "units_built_50_plus")) |>
    group_by(borough_name, outcome_family, era, two_by_two_label) |>
    summarize(value = 10000 * sum(outcome_value, na.rm = TRUE) / sum(occupied_units_1990, na.rm = TRUE), .groups = "drop"),
  dcp_df |>
    filter(outcome_family == "nb_project_count_50_plus") |>
    mutate(era = case_when(
      year >= 2010 & year <= 2014 ~ "2010-2014",
      year >= 2015 & year <= 2019 ~ "2015-2019",
      TRUE ~ "2020-2025"
    )) |>
    group_by(borough_name, outcome_family, era, two_by_two_label) |>
    summarize(value = mean(outcome_value, na.rm = TRUE), .groups = "drop")
)

pdf(out_borough_paths_pdf, width = 10, height = 8)
print(
  ggplot(filter(borough_path_df, outcome_family == "units_built_total"), aes(x = era, y = value, color = two_by_two_label, group = two_by_two_label)) +
    geom_line() +
    geom_point(size = 1.8) +
    facet_wrap(~ borough_name, scales = "free_y") +
    labs(title = "Borough-specific 2x2 paths: total units", x = NULL, y = "Units per 10,000 occupied units", color = "2x2 cell") +
    theme_minimal(base_size = 10)
)
print(
  ggplot(filter(borough_path_df, outcome_family == "units_built_50_plus"), aes(x = era, y = value, color = two_by_two_label, group = two_by_two_label)) +
    geom_line() +
    geom_point(size = 1.8) +
    facet_wrap(~ borough_name, scales = "free_y") +
    labs(title = "Borough-specific 2x2 paths: 50+ units", x = NULL, y = "50+ units per 10,000 occupied units", color = "2x2 cell") +
    theme_minimal(base_size = 10)
)
print(
  ggplot(filter(borough_path_df, outcome_family == "nb_project_count_50_plus"), aes(x = era, y = value, color = two_by_two_label, group = two_by_two_label)) +
    geom_line() +
    geom_point(size = 1.8) +
    facet_wrap(~ borough_name, scales = "free_y") +
    labs(title = "Borough-specific 2x2 paths: 50+ project counts", x = NULL, y = "50+ projects per CD-year", color = "2x2 cell") +
    theme_minimal(base_size = 10)
)
dev.off()

non_manhattan_path_df <- bind_rows(
  long_df |>
    mutate(sample_label = case_when(
      borough_name == "Manhattan" ~ "Manhattan only",
      borough_name %in% c("Bronx", "Brooklyn", "Queens") ~ "Bronx/Brooklyn/Queens",
      borough_name != "Manhattan" ~ "Non-Manhattan",
      TRUE ~ NA_character_
    )) |>
    filter(sample_label %in% c("Non-Manhattan", "Manhattan only", "Bronx/Brooklyn/Queens"), outcome_family %in% c("units_built_total", "units_built_50_plus")) |>
    group_by(sample_label, outcome_family, era, two_by_two_label) |>
    summarize(value = 10000 * sum(outcome_value, na.rm = TRUE) / sum(occupied_units_1990, na.rm = TRUE), .groups = "drop")
)

pdf(out_non_manhattan_pdf, width = 9, height = 7)
print(
  ggplot(filter(non_manhattan_path_df, outcome_family == "units_built_total"), aes(x = era, y = value, color = two_by_two_label, group = two_by_two_label)) +
    geom_line() +
    geom_point(size = 2) +
    facet_wrap(~ sample_label, scales = "free_y") +
    labs(title = "Sample-split 2x2 paths: total units", x = NULL, y = "Units per 10,000 occupied units", color = "2x2 cell") +
    theme_minimal(base_size = 10)
)
print(
  ggplot(filter(non_manhattan_path_df, outcome_family == "units_built_50_plus"), aes(x = era, y = value, color = two_by_two_label, group = two_by_two_label)) +
    geom_line() +
    geom_point(size = 2) +
    facet_wrap(~ sample_label, scales = "free_y") +
    labs(title = "Sample-split 2x2 paths: 50+ units", x = NULL, y = "50+ units per 10,000 occupied units", color = "2x2 cell") +
    theme_minimal(base_size = 10)
)
dev.off()

cat("Wrote redevelopment interaction and Manhattan sensitivity outputs to", dirname(out_model_summary_csv), "\n")
