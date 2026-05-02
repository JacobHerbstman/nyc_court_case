#!/usr/bin/env Rscript

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_brooklyn_homeownership_case_study/code")
# controls_path <- "../input/brooklyn_homeownership_case_study_controls.csv"
# long_units_path <- "../input/cd_homeownership_long_units_series.csv"
# dcp_supply_path <- "../input/cd_homeownership_dcp_supply_panel.csv"
# permit_panel_path <- "../input/cd_homeownership_permit_nb_panel.csv"
# boundary_index_path <- "../input/dcp_boundary_index.csv"
# zap_cd_year_path <- "../input/zap_ulurp_redev_cd_year_panel.csv"
# zap_mature_path <- "../input/zap_ulurp_redev_mature_cohort_panel.csv"
# zap_yield_path <- "../input/zap_ulurp_redev_yield_panel.csv"
# cd_summary_out <- "../output/brooklyn_homeownership_case_study_cd_summary.csv"
# era_outcomes_out <- "../output/brooklyn_homeownership_case_study_era_outcomes.csv"
# regressions_out <- "../output/brooklyn_homeownership_case_study_regressions.csv"
# block_regressions_out <- "../output/brooklyn_homeownership_case_study_block_regressions.csv"
# block_diagnostics_out <- "../output/brooklyn_homeownership_case_study_block_diagnostics.csv"
# leave_one_cd_out <- "../output/brooklyn_homeownership_case_study_leave_one_cd_out.csv"
# size_bin_summary_out <- "../output/brooklyn_homeownership_case_study_size_bin_summary.csv"
# zap_summary_out <- "../output/brooklyn_homeownership_case_study_zap_summary.csv"
# zap_block_regressions_out <- "../output/brooklyn_homeownership_case_study_zap_block_regressions.csv"
# qc_out <- "../output/brooklyn_homeownership_case_study_qc.csv"
# plots_out <- "../output/brooklyn_homeownership_case_study_plots.pdf"
# control_flip_plots_out <- "../output/brooklyn_homeownership_case_study_control_flip_plots.pdf"

suppressPackageStartupMessages({
  library(arrow)
  library(broom)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(scales)
  library(sf)
  library(stringr)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 20) {
  stop(
    "Expected 20 arguments: controls_path long_units_path dcp_supply_path permit_panel_path ",
    "boundary_index_path zap_cd_year_path zap_mature_path zap_yield_path cd_summary_out ",
    "era_outcomes_out regressions_out block_regressions_out block_diagnostics_out ",
    "leave_one_cd_out size_bin_summary_out zap_summary_out zap_block_regressions_out ",
    "qc_out plots_out control_flip_plots_out"
  )
}

controls_path <- args[1]
long_units_path <- args[2]
dcp_supply_path <- args[3]
permit_panel_path <- args[4]
boundary_index_path <- args[5]
zap_cd_year_path <- args[6]
zap_mature_path <- args[7]
zap_yield_path <- args[8]
cd_summary_out <- args[9]
era_outcomes_out <- args[10]
regressions_out <- args[11]
block_regressions_out <- args[12]
block_diagnostics_out <- args[13]
leave_one_cd_out <- args[14]
size_bin_summary_out <- args[15]
zap_summary_out <- args[16]
zap_block_regressions_out <- args[17]
qc_out <- args[18]
plots_out <- args[19]
control_flip_plots_out <- args[20]

theme_set(
  theme_minimal(base_size = 11) +
    theme(
      panel.grid.minor = element_blank(),
      strip.text = element_text(face = "bold"),
      plot.title = element_text(face = "bold"),
      legend.position = "bottom"
    )
)

boundary_file <- read_csv(boundary_index_path, show_col_types = FALSE, na = c("", "NA")) %>%
  filter(source_id == "dcp_boundary_community_districts", !is.na(parquet_path), file.exists(parquet_path)) %>%
  mutate(
    pull_date = as.character(pull_date),
    pull_date_order = suppressWarnings(as.integer(pull_date))
  ) %>%
  arrange(desc(pull_date_order), desc(pull_date), parquet_path) %>%
  slice_head(n = 1)

if (nrow(boundary_file) == 0) {
  stop("Could not find a staged community-district boundary parquet in ", boundary_index_path)
}

safe_standardize <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  x_mean <- mean(x, na.rm = TRUE)
  x_sd <- stats::sd(x, na.rm = TRUE)

  if (is.na(x_sd) || x_sd == 0) {
    return(rep(0, length(x)))
  }

  (x - x_mean) / x_sd
}

safe_lm_fit <- function(df, predictors, spec_name, spec_family, spec_order) {
  model_df <- df %>%
    select(outcome_rate, treat_z_boro, all_of(predictors)) %>%
    filter(if_all(everything(), ~ !is.na(.x)))

  if (nrow(model_df) < 6 || stats::sd(model_df$treat_z_boro) == 0 || stats::sd(model_df$outcome_rate) == 0) {
    return(
      tibble(
        spec = spec_name,
        spec_family = spec_family,
        spec_order = spec_order,
        control_count = length(predictors),
        beta_treat_z = NA_real_,
        beta_treat_z_se = NA_real_,
        beta_treat_z_p = NA_real_,
        r_squared = NA_real_,
        n = nrow(model_df),
        fit_status = "insufficient_variation"
      )
    )
  }

  fit <- tryCatch(
    lm(reformulate(c("treat_z_boro", predictors), response = "outcome_rate"), data = model_df),
    error = function(e) NULL
  )

  if (is.null(fit)) {
    return(
      tibble(
        spec = spec_name,
        spec_family = spec_family,
        spec_order = spec_order,
        control_count = length(predictors),
        beta_treat_z = NA_real_,
        beta_treat_z_se = NA_real_,
        beta_treat_z_p = NA_real_,
        r_squared = NA_real_,
        n = nrow(model_df),
        fit_status = "fit_error"
      )
    )
  }

  tidy_df <- broom::tidy(fit) %>%
    filter(term == "treat_z_boro") %>%
    transmute(
      beta_treat_z = estimate,
      beta_treat_z_se = std.error,
      beta_treat_z_p = p.value
    )

  if (nrow(tidy_df) == 0) {
    tidy_df <- tibble(beta_treat_z = NA_real_, beta_treat_z_se = NA_real_, beta_treat_z_p = NA_real_)
  }

  fit_status <- ifelse(
    is.na(tidy_df$beta_treat_z[[1]]) || is.na(tidy_df$beta_treat_z_se[[1]]),
    "treat_term_dropped",
    "ok"
  )

  bind_cols(
    tibble(
      spec = spec_name,
      spec_family = spec_family,
      spec_order = spec_order,
      control_count = length(predictors),
      r_squared = summary(fit)$r.squared,
      n = nobs(fit),
      fit_status = fit_status
    ),
    tidy_df
  )
}

classify_movement <- function(raw_beta, spec_beta, raw_se, spec_se) {
  if (is.na(raw_beta) || is.na(spec_beta)) {
    return("unavailable")
  }

  if (abs(raw_beta) < 1e-9 && abs(spec_beta) < 1e-9) {
    return("no_change")
  }

  if (sign(raw_beta) != sign(spec_beta) && abs(spec_beta) > 1e-9) {
    return("sign_change")
  }

  if (abs(spec_beta) + 1e-9 < abs(raw_beta)) {
    return("shrink_toward_zero")
  }

  if (!is.na(raw_se) && !is.na(spec_se) && spec_se > raw_se) {
    return("precision_loss")
  }

  "no_attenuation"
}

run_block_suite <- function(df, spec_defs) {
  bind_rows(lapply(spec_defs, function(x) {
    safe_lm_fit(
      df = df,
      predictors = x$predictors,
      spec_name = x$spec,
      spec_family = x$spec_family,
      spec_order = x$spec_order
    )
  }))
}

make_long_era <- function(year) {
  case_when(
    year >= 1980 & year <= 1984 ~ "1980-1984",
    year >= 1985 & year <= 1989 ~ "1985-1989",
    year >= 1990 & year <= 1999 ~ "1990-1999",
    year >= 2000 & year <= 2009 ~ "2000-2009",
    year >= 2010 & year <= 2019 ~ "2010-2019",
    year >= 2020 & year <= 2025 ~ "2020-2025",
    TRUE ~ NA_character_
  )
}

make_post2010_era <- function(year) {
  case_when(
    year >= 2010 & year <= 2019 ~ "2010-2019",
    year >= 2020 & year <= 2025 ~ "2020-2025",
    TRUE ~ NA_character_
  )
}

make_zap_application_era <- function(year) {
  case_when(
    year >= 1990 & year <= 1999 ~ "1990-1999",
    year >= 2000 & year <= 2009 ~ "2000-2009",
    year >= 2010 & year <= 2019 ~ "2010-2019",
    year >= 2020 & year <= 2025 ~ "2020-2025",
    TRUE ~ NA_character_
  )
}

make_zap_mature_era <- function(year) {
  case_when(
    year >= 1990 & year <= 1999 ~ "1990-1999",
    year >= 2000 & year <= 2009 ~ "2000-2009",
    year >= 2010 & year <= 2015 ~ "2010-2015",
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

controls_raw <- read_csv(controls_path, show_col_types = FALSE) %>%
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    brooklyn_short_label = as.character(brooklyn_short_label),
    brooklyn_neighborhood_label = as.character(brooklyn_neighborhood_label),
    cd_label = as.character(cd_label)
  ) %>%
  arrange(borocd)

if (nrow(controls_raw) != 18) {
  stop("Expected exactly 18 Brooklyn CDs in the helper controls file.")
}

block_components <- list(
  density = c(
    "occupied_units_1990_exact",
    "density_1990_occ_per_res_acre"
  ),
  socio_race = c(
    "median_household_income_1990_1999_dollars_exact",
    "poverty_share_1990_exact",
    "college_graduate_share_1990_exact",
    "black_share_1990_nhgis",
    "hispanic_share_1990_nhgis"
  ),
  structure = c(
    "structure_share_1_2_units_1990_exact",
    "structure_share_3_4_units_1990_exact",
    "structure_share_50_plus_units_1990_exact"
  ),
  access = c(
    "subway_commute_share_1990_exact",
    "mean_commute_time_1990_minutes_exact",
    "distance_to_city_hall_miles"
  ),
  pretrend = c(
    "occupied_units_growth_1980_1990_approx",
    "vacancy_rate_change_1980_1990_pp_approx",
    "homeowner_share_change_1980_1990_pp_approx"
  ),
  redevelopment = c(
    "redev_potential_A_z_boro",
    "cd_mean_built_far_lot_weighted",
    "cd_mean_max_resid_far_lot_weighted",
    "cd_share_lot_area_one_two_family",
    "cd_share_lot_area_vacant",
    "cd_share_lot_area_old_building",
    "cd_share_lot_area_protected"
  )
)

controls_with_blocks <- controls_raw

for (block_name in names(block_components)) {
  std_cols <- paste0(block_components[[block_name]], "_bk_z")

  controls_with_blocks[std_cols] <- lapply(controls_raw[block_components[[block_name]]], safe_standardize)
  controls_with_blocks[[paste0("block_", block_name)]] <- rowMeans(controls_with_blocks[std_cols], na.rm = TRUE)
}

homeowner_cut <- median(controls_with_blocks$treat_z_boro, na.rm = TRUE)

brooklyn_base <- controls_with_blocks %>%
  mutate(
    homeowner_half = if_else(treat_z_boro >= homeowner_cut, "high_homeowner", "low_homeowner"),
    homeowner_half_label = if_else(treat_z_boro >= homeowner_cut, "High homeowner", "Low homeowner")
  )

lookup_cols <- c(
  "borocd", "brooklyn_short_label", "brooklyn_neighborhood_label", "cd_label",
  "borough_name", "treat_pp", "treat_z_boro", "homeowner_half", "homeowner_half_label",
  "occupied_units_1990_exact", "residential_acres", "density_1990_occ_per_res_acre",
  "redev_potential_A_z_boro", "redev_potential_C_z_boro",
  "cd_mean_built_far_lot_weighted", "cd_mean_max_resid_far_lot_weighted", "cd_mean_unused_res_far_lot_weighted",
  "cd_share_lot_area_one_two_family", "cd_share_lot_area_vacant", "cd_share_lot_area_old_building",
  "cd_share_lot_area_protected", "cd_share_lot_area_parking_or_low_intensity",
  "median_household_income_1990_1999_dollars_exact", "poverty_share_1990_exact", "college_graduate_share_1990_exact",
  "black_share_1990_nhgis", "hispanic_share_1990_nhgis", "white_share_1990_nhgis",
  "structure_share_1_2_units_1990_exact", "structure_share_3_4_units_1990_exact", "structure_share_50_plus_units_1990_exact",
  "subway_commute_share_1990_exact", "mean_commute_time_1990_minutes_exact", "distance_to_city_hall_miles",
  "occupied_units_growth_1980_1990_approx", "vacancy_rate_change_1980_1990_pp_approx", "homeowner_share_change_1980_1990_pp_approx",
  "block_density", "block_socio_race", "block_structure", "block_access", "block_pretrend", "block_redevelopment"
)

lookup_df <- brooklyn_base %>% select(all_of(lookup_cols))

assert_unique_keys(lookup_df, "borocd", "Brooklyn helper lookup")

long_outcomes <- read_csv(long_units_path, show_col_types = FALSE) %>%
  filter(
    borough_name == "Brooklyn",
    series_kind == "preferred_long_series",
    series_family %in% c("units_built_total", "units_built_50_plus")
  ) %>%
  mutate(era = make_long_era(year)) %>%
  filter(!is.na(era)) %>%
  group_by(borocd, era, series_family) %>%
  summarise(
    total_outcome = sum(outcome_value, na.rm = TRUE),
    years_n = n_distinct(year),
    .groups = "drop"
  ) %>%
  inner_join(lookup_df, by = "borocd", relationship = "many-to-one") %>%
  mutate(
    outcome_id = series_family,
    outcome_label = case_when(
      outcome_id == "units_built_total" ~ "Total new-building units",
      outcome_id == "units_built_50_plus" ~ "50+ new-building units",
      TRUE ~ outcome_id
    ),
    rate_type = "per_10k_occupied",
    outcome_rate = total_outcome / years_n * 10000 / occupied_units_1990_exact
  )

dcp_outcomes <- read_csv(dcp_supply_path, show_col_types = FALSE) %>%
  filter(
    borough_name == "Brooklyn",
    outcome_family %in% c(
      "gross_add_units",
      "nb_gross_units_1_2",
      "nb_gross_units_3_4",
      "nb_gross_units_5_9",
      "nb_gross_units_10_49",
      "nb_gross_units_50_plus",
      "nb_project_count_50_plus"
    )
  ) %>%
  mutate(era = make_post2010_era(year)) %>%
  filter(!is.na(era)) %>%
  group_by(borocd, era, outcome_family) %>%
  summarise(
    total_outcome = sum(outcome_value, na.rm = TRUE),
    years_n = n_distinct(year),
    .groups = "drop"
  ) %>%
  inner_join(lookup_df, by = "borocd", relationship = "many-to-one") %>%
  mutate(
    outcome_id = outcome_family,
    outcome_label = case_when(
      outcome_id == "gross_add_units" ~ "Gross additions",
      outcome_id == "nb_gross_units_1_2" ~ "1-2 unit new-building units",
      outcome_id == "nb_gross_units_3_4" ~ "3-4 unit new-building units",
      outcome_id == "nb_gross_units_5_9" ~ "5-9 unit new-building units",
      outcome_id == "nb_gross_units_10_49" ~ "10-49 unit new-building units",
      outcome_id == "nb_gross_units_50_plus" ~ "50+ new-building units (observed)",
      outcome_id == "nb_project_count_50_plus" ~ "50+ new-building projects",
      TRUE ~ outcome_id
    ),
    rate_type = case_when(
      outcome_id == "gross_add_units" ~ "per_res_acre",
      outcome_id == "nb_project_count_50_plus" ~ "per_cd_year",
      TRUE ~ "per_10k_occupied"
    ),
    outcome_rate = case_when(
      outcome_id == "gross_add_units" ~ total_outcome / years_n / residential_acres,
      outcome_id == "nb_project_count_50_plus" ~ total_outcome / years_n,
      TRUE ~ total_outcome / years_n * 10000 / occupied_units_1990_exact
    )
  )

permit_panel_raw <- read_csv(permit_panel_path, show_col_types = FALSE)

permit_outcome_families <- permit_panel_raw %>%
  filter(!is.na(outcome_family)) %>%
  distinct(outcome_family) %>%
  pull(outcome_family)

if (!identical(sort(permit_outcome_families), "permit_nb_jobs")) {
  stop("Expected permit panel to contain exactly one outcome_family: permit_nb_jobs.")
}

permit_outcomes <- permit_panel_raw %>%
  filter(borough_name == "Brooklyn") %>%
  mutate(era = make_post2010_era(year)) %>%
  filter(!is.na(era)) %>%
  group_by(borocd, era) %>%
  summarise(
    total_outcome = sum(outcome_value, na.rm = TRUE),
    years_n = n_distinct(year),
    .groups = "drop"
  ) %>%
  inner_join(lookup_df, by = "borocd", relationship = "many-to-one") %>%
  mutate(
    outcome_id = "permit_nb_jobs",
    outcome_label = "DOB new-building jobs",
    rate_type = "per_cd_year",
    outcome_rate = total_outcome / years_n
  )

era_outcomes <- bind_rows(long_outcomes, dcp_outcomes, permit_outcomes) %>%
  mutate(
    era = factor(
      era,
      levels = c("1980-1984", "1985-1989", "1990-1999", "2000-2009", "2010-2019", "2020-2025"),
      ordered = TRUE
    )
  ) %>%
  arrange(outcome_id, era, borocd)

wide_value_names <- era_outcomes %>%
  filter(era %in% c("1985-1989", "2010-2019", "2020-2025")) %>%
  mutate(
    value_name = case_when(
      rate_type == "per_10k_occupied" ~ paste0(outcome_id, "_", gsub("-", "_", era), "_per_10k_occupied"),
      rate_type == "per_res_acre" ~ paste0(outcome_id, "_", gsub("-", "_", era), "_per_res_acre"),
      TRUE ~ paste0(outcome_id, "_", gsub("-", "_", era), "_per_cd_year")
    )
  ) %>%
  select(borocd, value_name, outcome_rate) %>%
  pivot_wider(names_from = value_name, values_from = outcome_rate)

cd_summary <- brooklyn_base %>%
  left_join(wide_value_names, by = "borocd", relationship = "one-to-one") %>%
  mutate(
    treat_rank_brooklyn = min_rank(desc(treat_z_boro))
  ) %>%
  arrange(treat_rank_brooklyn, desc(treat_z_boro))

spec_definitions <- list(
  list(spec = "raw", spec_family = "raw", spec_order = 0L, predictors = character()),
  list(spec = "one_block_density", spec_family = "one_block", spec_order = 1L, predictors = c("block_density")),
  list(spec = "one_block_socio_race", spec_family = "one_block", spec_order = 2L, predictors = c("block_socio_race")),
  list(spec = "one_block_structure", spec_family = "one_block", spec_order = 3L, predictors = c("block_structure")),
  list(spec = "one_block_access", spec_family = "one_block", spec_order = 4L, predictors = c("block_access")),
  list(spec = "one_block_pretrend", spec_family = "one_block", spec_order = 5L, predictors = c("block_pretrend")),
  list(spec = "one_block_redevelopment", spec_family = "one_block", spec_order = 6L, predictors = c("block_redevelopment")),
  list(spec = "cumulative_density", spec_family = "cumulative", spec_order = 7L, predictors = c("block_density")),
  list(spec = "cumulative_density_socio_race", spec_family = "cumulative", spec_order = 8L, predictors = c("block_density", "block_socio_race")),
  list(spec = "cumulative_density_socio_race_structure", spec_family = "cumulative", spec_order = 9L, predictors = c("block_density", "block_socio_race", "block_structure")),
  list(spec = "cumulative_density_socio_race_structure_access", spec_family = "cumulative", spec_order = 10L, predictors = c("block_density", "block_socio_race", "block_structure", "block_access")),
  list(spec = "cumulative_density_socio_race_structure_access_pretrend", spec_family = "cumulative", spec_order = 11L, predictors = c("block_density", "block_socio_race", "block_structure", "block_access", "block_pretrend")),
  list(spec = "all_blocks", spec_family = "cumulative", spec_order = 12L, predictors = c("block_density", "block_socio_race", "block_structure", "block_access", "block_pretrend", "block_redevelopment"))
)

block_regressions <- era_outcomes %>%
  group_by(outcome_id, outcome_label, era, rate_type) %>%
  group_modify(~ run_block_suite(.x, spec_definitions)) %>%
  ungroup() %>%
  arrange(outcome_id, era, spec_order)

block_diagnostics <- block_regressions %>%
  group_by(outcome_id, era) %>%
  mutate(
    raw_beta = beta_treat_z[spec == "raw"][1],
    raw_se = beta_treat_z_se[spec == "raw"][1],
    raw_p = beta_treat_z_p[spec == "raw"][1],
    beta_change_from_raw = beta_treat_z - raw_beta,
    abs_beta_ratio_to_raw = ifelse(!is.na(raw_beta) & abs(raw_beta) > 1e-9, abs(beta_treat_z) / abs(raw_beta), NA_real_),
    movement_category = mapply(classify_movement, raw_beta, beta_treat_z, raw_se, beta_treat_z_se)
  ) %>%
  ungroup() %>%
  select(
    outcome_id,
    outcome_label,
    era,
    rate_type,
    spec,
    spec_family,
    spec_order,
    beta_treat_z,
    beta_treat_z_se,
    beta_treat_z_p,
    r_squared,
    raw_beta,
    raw_se,
    raw_p,
    beta_change_from_raw,
    abs_beta_ratio_to_raw,
    movement_category,
    fit_status,
    n
  )

headline_regressions <- block_regressions %>%
  filter(spec %in% c("raw", "all_blocks")) %>%
  arrange(outcome_id, era, spec_order)

size_bin_outcomes <- c(
  "nb_gross_units_1_2",
  "nb_gross_units_3_4",
  "nb_gross_units_5_9",
  "nb_gross_units_10_49",
  "nb_gross_units_50_plus"
)

size_bin_labels <- c(
  nb_gross_units_1_2 = "1-2",
  nb_gross_units_3_4 = "3-4",
  nb_gross_units_5_9 = "5-9",
  nb_gross_units_10_49 = "10-49",
  nb_gross_units_50_plus = "50+"
)

size_bin_summary <- era_outcomes %>%
  filter(outcome_id %in% size_bin_outcomes, era %in% c("2010-2019", "2020-2025")) %>%
  mutate(size_bin = recode(outcome_id, !!!size_bin_labels)) %>%
  group_by(era, size_bin, homeowner_half, homeowner_half_label) %>%
  summarise(
    mean_outcome_rate = mean(outcome_rate, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(
    block_regressions %>%
      filter(spec == "raw", outcome_id %in% size_bin_outcomes, era %in% c("2010-2019", "2020-2025")) %>%
      mutate(size_bin = recode(outcome_id, !!!size_bin_labels)) %>%
      select(era, size_bin, raw_beta_treat_z = beta_treat_z, raw_beta_treat_z_se = beta_treat_z_se),
    by = c("era", "size_bin"),
    relationship = "many-to-one"
  ) %>%
  arrange(era, factor(size_bin, levels = c("1-2", "3-4", "5-9", "10-49", "50+")), homeowner_half)

leave_targets <- tribble(
  ~outcome_id, ~era,
  "units_built_total", "2020-2025",
  "units_built_50_plus", "2020-2025",
  "nb_project_count_50_plus", "2020-2025",
  "nb_gross_units_1_2", "2020-2025"
)

exclusion_map <- bind_rows(
  brooklyn_base %>%
    transmute(
      exclusion_id = paste0("drop_", brooklyn_short_label),
      excluded_borocd = borocd,
      exclusion_label = paste0("Drop ", brooklyn_short_label)
    ),
  tibble(
    exclusion_id = "drop_BK01_BK16",
    excluded_borocd = NA_integer_,
    exclusion_label = "Drop BK01 and BK16"
  )
)

leave_one_out_results <- leave_targets %>%
  rowwise() %>%
  do({
    target_outcome <- .$outcome_id
    target_era <- .$era
    target_df <- era_outcomes %>%
      filter(outcome_id == target_outcome, era == target_era)

    full_raw_beta <- block_regressions %>%
      filter(outcome_id == target_outcome, era == target_era, spec == "raw") %>%
      pull(beta_treat_z)

    bind_rows(lapply(seq_len(nrow(exclusion_map)), function(i) {
      excluded_ids <- if (exclusion_map$exclusion_id[i] == "drop_BK01_BK16") c(301L, 316L) else exclusion_map$excluded_borocd[i]
      sample_df <- target_df %>% filter(!borocd %in% excluded_ids)

      bind_rows(
        safe_lm_fit(sample_df, character(), "raw", "leave_one_out", 0L),
        safe_lm_fit(sample_df, c("block_density", "block_socio_race", "block_structure", "block_access", "block_pretrend", "block_redevelopment"), "all_blocks", "leave_one_out", 12L)
      ) %>%
        mutate(
          outcome_id = target_outcome,
          era = target_era,
          exclusion_id = exclusion_map$exclusion_id[i],
          exclusion_label = exclusion_map$exclusion_label[i],
          excluded_borocd = ifelse(length(excluded_ids) == 1, excluded_ids, NA_integer_),
          beta_change_from_full_sample_raw = beta_treat_z - full_raw_beta
        )
    }))
  }) %>%
  ungroup() %>%
  group_by(outcome_id, spec) %>%
  mutate(
    influence_rank_abs_change = min_rank(desc(abs(beta_change_from_full_sample_raw)))
  ) %>%
  ungroup() %>%
  arrange(outcome_id, spec, influence_rank_abs_change)

zap_cd_year <- read_csv(zap_cd_year_path, show_col_types = FALSE) %>%
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    cert_year = suppressWarnings(as.integer(cert_year))
  ) %>%
  filter(borough_name == "Brooklyn")

zap_mature <- read_csv(zap_mature_path, show_col_types = FALSE) %>%
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    cert_year = suppressWarnings(as.integer(cert_year))
  ) %>%
  filter(borough_name == "Brooklyn")

zap_yield <- read_csv(zap_yield_path, show_col_types = FALSE) %>%
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    cert_year = suppressWarnings(as.integer(cert_year))
  ) %>%
  filter(borough_name == "Brooklyn")

zap_applications_era <- zap_cd_year %>%
  mutate(zap_era = make_zap_application_era(cert_year)) %>%
  filter(!is.na(zap_era)) %>%
  group_by(borocd, zap_era) %>%
  summarise(
    years_n = n_distinct(cert_year),
    initial_apps = sum(initial_apps, na.rm = TRUE),
    private_initial_apps = sum(private_initial_apps, na.rm = TRUE),
    mixed_private_rezoning_apps = sum(mixed_private_rezoning_apps, na.rm = TRUE),
    public_hpd_apps = sum(public_hpd_apps, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  inner_join(lookup_df, by = "borocd", relationship = "many-to-one") %>%
  mutate(
    initial_apps_per_10k = initial_apps / years_n * 10000 / occupied_units_1990_exact,
    private_initial_apps_per_10k = private_initial_apps / years_n * 10000 / occupied_units_1990_exact,
    mixed_private_rezoning_apps_per_10k = mixed_private_rezoning_apps / years_n * 10000 / occupied_units_1990_exact,
    public_hpd_apps_per_10k = public_hpd_apps / years_n * 10000 / occupied_units_1990_exact
  )

zap_mature_era <- zap_mature %>%
  mutate(zap_era = make_zap_mature_era(cert_year)) %>%
  filter(!is.na(zap_era)) %>%
  group_by(borocd, zap_era) %>%
  summarise(
    initial_apps = sum(initial_apps, na.rm = TRUE),
    complete_apps = sum(complete_apps, na.rm = TRUE),
    failed_apps = sum(failed_apps, na.rm = TRUE),
    unresolved_apps = sum(unresolved_apps, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  inner_join(lookup_df, by = "borocd", relationship = "many-to-one") %>%
  mutate(
    completion_share = ifelse(initial_apps > 0, complete_apps / initial_apps, NA_real_),
    failure_share = ifelse(initial_apps > 0, failed_apps / initial_apps, NA_real_)
  )

zap_yield_descriptive <- zap_yield %>%
  filter(yield_era == "2010-2015") %>%
  group_by(borocd, yield_era) %>%
  summarise(
    initial_apps = sum(initial_apps, na.rm = TRUE),
    linked_nb_50_plus_projects_0_10 = sum(linked_nb_50_plus_projects_0_10, na.rm = TRUE),
    linked_gross_add_units_0_10 = sum(linked_gross_add_units_0_10, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  inner_join(lookup_df, by = "borocd", relationship = "many-to-one") %>%
  mutate(
    linked_nb_50_plus_rate_0_10 = ifelse(initial_apps > 0, linked_nb_50_plus_projects_0_10 / initial_apps, NA_real_),
    linked_gross_add_units_per_app_0_10 = ifelse(initial_apps > 0, linked_gross_add_units_0_10 / initial_apps, NA_real_)
  )

zap_yield_regression <- zap_yield %>%
  filter(yield_era == "2016-2020") %>%
  group_by(borocd, yield_era) %>%
  summarise(
    initial_apps = sum(initial_apps, na.rm = TRUE),
    linked_nb_50_plus_projects_0_5 = sum(linked_nb_50_plus_projects_0_5, na.rm = TRUE),
    linked_gross_add_units_0_5 = sum(linked_gross_add_units_0_5, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  inner_join(lookup_df, by = "borocd", relationship = "many-to-one") %>%
  mutate(
    linked_nb_50_plus_rate_0_5 = ifelse(initial_apps > 0, linked_nb_50_plus_projects_0_5 / initial_apps, NA_real_),
    linked_gross_add_units_per_app_0_5 = ifelse(initial_apps > 0, linked_gross_add_units_0_5 / initial_apps, NA_real_)
  )

zap_summary <- bind_rows(
  zap_applications_era %>%
    select(
      homeowner_half, homeowner_half_label, era = zap_era,
      initial_apps_per_10k, private_initial_apps_per_10k, mixed_private_rezoning_apps_per_10k, public_hpd_apps_per_10k
    ) %>%
    pivot_longer(
      cols = c(initial_apps_per_10k, private_initial_apps_per_10k, mixed_private_rezoning_apps_per_10k, public_hpd_apps_per_10k),
      names_to = "outcome_id",
      values_to = "outcome_rate"
    ) %>%
    mutate(panel_family = "applications"),
  zap_mature_era %>%
    select(homeowner_half, homeowner_half_label, era = zap_era, completion_share, failure_share) %>%
    pivot_longer(
      cols = c(completion_share, failure_share),
      names_to = "outcome_id",
      values_to = "outcome_rate"
    ) %>%
    mutate(panel_family = "mature_status"),
  zap_yield_descriptive %>%
    select(homeowner_half, homeowner_half_label, era = yield_era, linked_nb_50_plus_rate_0_10, linked_gross_add_units_per_app_0_10) %>%
    pivot_longer(
      cols = c(linked_nb_50_plus_rate_0_10, linked_gross_add_units_per_app_0_10),
      names_to = "outcome_id",
      values_to = "outcome_rate"
    ) %>%
    mutate(panel_family = "yield_0_10")
) %>%
  group_by(panel_family, era, outcome_id, homeowner_half, homeowner_half_label) %>%
  summarise(
    mean_outcome_rate = mean(outcome_rate, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(panel_family, era, outcome_id, homeowner_half)

zap_applications_long <- zap_applications_era %>%
  select(
    borocd, zap_era, treat_z_boro,
    block_density, block_socio_race, block_structure, block_access, block_pretrend, block_redevelopment,
    initial_apps_per_10k, private_initial_apps_per_10k, mixed_private_rezoning_apps_per_10k, public_hpd_apps_per_10k
  ) %>%
  pivot_longer(
    cols = c(initial_apps_per_10k, private_initial_apps_per_10k, mixed_private_rezoning_apps_per_10k, public_hpd_apps_per_10k),
    names_to = "outcome_id",
    values_to = "outcome_rate"
  ) %>%
  rename(era = zap_era) %>%
  mutate(panel_family = "applications")

zap_mature_long <- zap_mature_era %>%
  select(
    borocd, zap_era, treat_z_boro,
    block_density, block_socio_race, block_structure, block_access, block_pretrend, block_redevelopment,
    completion_share, failure_share
  ) %>%
  pivot_longer(
    cols = c(completion_share, failure_share),
    names_to = "outcome_id",
    values_to = "outcome_rate"
  ) %>%
  rename(era = zap_era) %>%
  mutate(panel_family = "mature_status")

zap_yield_long <- zap_yield_regression %>%
  select(
    borocd, era = yield_era, treat_z_boro,
    block_density, block_socio_race, block_structure, block_access, block_pretrend, block_redevelopment,
    linked_nb_50_plus_rate_0_5, linked_gross_add_units_per_app_0_5
  ) %>%
  pivot_longer(
    cols = c(linked_nb_50_plus_rate_0_5, linked_gross_add_units_per_app_0_5),
    names_to = "outcome_id",
    values_to = "outcome_rate"
  ) %>%
  mutate(panel_family = "yield_0_5_appendix") %>%
  filter(!is.na(outcome_rate))

zap_regression_data <- bind_rows(zap_applications_long, zap_mature_long, zap_yield_long)

zap_block_regressions <- zap_regression_data %>%
  group_by(panel_family, outcome_id, era) %>%
  group_modify(~ run_block_suite(.x, spec_definitions)) %>%
  ungroup() %>%
  arrange(panel_family, outcome_id, era, spec_order)

qc <- bind_rows(
  tibble(
    check_name = "brooklyn_cd_count",
    check_value = as.character(nrow(brooklyn_base))
  ),
  tibble(
    check_name = "missing_neighborhood_label_count",
    check_value = as.character(sum(is.na(brooklyn_base$brooklyn_neighborhood_label)))
  ),
  tibble(
    check_name = "missing_distance_to_city_hall_count",
    check_value = as.character(sum(is.na(brooklyn_base$distance_to_city_hall_miles)))
  ),
  tibble(
    check_name = "missing_black_share_1990_nhgis_count",
    check_value = as.character(sum(is.na(brooklyn_base$black_share_1990_nhgis)))
  ),
  tibble(
    check_name = "missing_hispanic_share_1990_nhgis_count",
    check_value = as.character(sum(is.na(brooklyn_base$hispanic_share_1990_nhgis)))
  ),
  tibble(
    check_name = "size_bin_summary_expected_eras",
    check_value = "2010-2019,2020-2025"
  ),
  tibble(
    check_name = "size_bin_summary_row_count",
    check_value = as.character(nrow(size_bin_summary))
  ),
  tibble(
    check_name = "leave_one_cd_out_expected_exclusions",
    check_value = as.character(19)
  ),
  tibble(
    check_name = "leave_one_cd_out_actual_exclusions",
    check_value = as.character(n_distinct(leave_one_out_results$exclusion_id))
  ),
  tibble(
    check_name = "zap_cd_year_brooklyn_rows",
    check_value = as.character(nrow(zap_cd_year))
  ),
  tibble(
    check_name = "zap_mature_brooklyn_rows",
    check_value = as.character(nrow(zap_mature))
  ),
  tibble(
    check_name = "zap_yield_brooklyn_rows",
    check_value = as.character(nrow(zap_yield))
  ),
  tibble(
    check_name = "boundary_pull_date",
    check_value = as.character(boundary_file$pull_date[[1]])
  ),
  tibble(
    check_name = "notes",
    check_value = paste(
      "Block-decomposition regressions use within-Brooklyn standardized block scores",
      "to keep the cumulative specs identified with 18 CDs."
    )
  )
)

write_csv_if_changed(cd_summary, cd_summary_out)
write_csv_if_changed(era_outcomes, era_outcomes_out)
write_csv_if_changed(headline_regressions, regressions_out)
write_csv_if_changed(block_regressions, block_regressions_out)
write_csv_if_changed(block_diagnostics, block_diagnostics_out)
write_csv_if_changed(leave_one_out_results, leave_one_cd_out)
write_csv_if_changed(size_bin_summary, size_bin_summary_out)
write_csv_if_changed(zap_summary, zap_summary_out)
write_csv_if_changed(zap_block_regressions, zap_block_regressions_out)
write_csv_if_changed(qc, qc_out)

boundary_raw <- read_parquet(boundary_file$parquet_path[[1]], col_select = c("district_id", "geometry_wkt", "crs_epsg"))

boundary_crs <- unique(boundary_raw$crs_epsg[!is.na(boundary_raw$crs_epsg)])
if (length(boundary_crs) != 1) {
  stop("Expected exactly one non-missing CRS in the staged community-district boundary file.")
}

boundaries_sf <- boundary_raw %>%
  transmute(
    borocd = suppressWarnings(as.integer(district_id)),
    geometry = st_as_sfc(geometry_wkt, crs = boundary_crs[[1]])
  ) %>%
  st_as_sf() %>%
  st_transform(2263) %>%
  inner_join(
    brooklyn_base %>% select(borocd, brooklyn_short_label, cd_label, treat_z_boro),
    by = "borocd",
    relationship = "many-to-one"
  )

map_data <- boundaries_sf %>%
  left_join(
    cd_summary %>%
      select(
        borocd,
        units_built_total_2020_2025_per_10k_occupied,
        units_built_50_plus_2020_2025_per_10k_occupied
      ),
    by = "borocd",
    relationship = "many-to-one"
  )

rank_plot_data <- cd_summary %>%
  select(
    cd_label,
    treat_z_boro,
    units_built_total_2020_2025_per_10k_occupied,
    nb_gross_units_1_2_2020_2025_per_10k_occupied,
    units_built_50_plus_2020_2025_per_10k_occupied
  ) %>%
  pivot_longer(
    cols = -cd_label,
    names_to = "metric",
    values_to = "metric_value"
  ) %>%
  mutate(
    metric = factor(
      metric,
      levels = c(
        "treat_z_boro",
        "units_built_total_2020_2025_per_10k_occupied",
        "nb_gross_units_1_2_2020_2025_per_10k_occupied",
        "units_built_50_plus_2020_2025_per_10k_occupied"
      ),
      labels = c(
        "1990 homeowner exposure",
        "Total units, 2020-2025",
        "1-2 unit building units, 2020-2025",
        "50+ units, 2020-2025"
      )
    ),
    cd_label = factor(cd_label, levels = rev(cd_summary$cd_label))
  )

scatter_plot_data <- era_outcomes %>%
  filter(
    outcome_id %in% c("units_built_total", "units_built_50_plus", "nb_gross_units_1_2", "nb_project_count_50_plus", "gross_add_units"),
    era %in% c("2010-2019", "2020-2025")
  ) %>%
  mutate(
    outcome_label = factor(
      outcome_label,
      levels = c(
        "Total new-building units",
        "50+ new-building units",
        "1-2 unit new-building units",
        "50+ new-building projects",
        "Gross additions"
      )
    )
  )

coef_plot_data <- block_regressions %>%
  filter(
    spec %in% c("raw", "all_blocks"),
    outcome_id %in% c("units_built_total", "units_built_50_plus", "nb_project_count_50_plus", "nb_gross_units_1_2", "gross_add_units", "permit_nb_jobs")
  ) %>%
  mutate(
    spec = factor(spec, levels = c("raw", "all_blocks"), labels = c("Raw", "All blocks")),
    era = factor(
      era,
      levels = c("1980-1984", "1985-1989", "1990-1999", "2000-2009", "2010-2019", "2020-2025"),
      ordered = TRUE
    )
  )

size_bin_plot_data <- size_bin_summary %>%
  distinct(era, size_bin, raw_beta_treat_z, raw_beta_treat_z_se) %>%
  mutate(
    size_bin = factor(size_bin, levels = c("1-2", "3-4", "5-9", "10-49", "50+")),
    era = factor(era, levels = c("2010-2019", "2020-2025"))
  )

control_flip_plot_data <- block_regressions %>%
  filter(
    spec %in% c(
      "raw",
      "one_block_density",
      "one_block_socio_race",
      "one_block_structure",
      "one_block_access",
      "one_block_pretrend",
      "one_block_redevelopment",
      "all_blocks"
    ),
    outcome_id %in% c("units_built_total", "units_built_50_plus", "nb_project_count_50_plus"),
    era == "2020-2025"
  ) %>%
  mutate(
    spec = factor(
      spec,
      levels = c(
        "raw",
        "one_block_density",
        "one_block_socio_race",
        "one_block_structure",
        "one_block_access",
        "one_block_pretrend",
        "one_block_redevelopment",
        "all_blocks"
      ),
      labels = c("Raw", "Density", "Socio-race", "Structure", "Access", "Pretrend", "Redevelopment", "All blocks")
    )
  )

leave_plot_data <- leave_one_out_results %>%
  filter(outcome_id %in% c("units_built_total", "units_built_50_plus", "nb_project_count_50_plus"), spec == "raw") %>%
  mutate(
    exclusion_label = factor(exclusion_label, levels = unique(exclusion_label[order(beta_change_from_full_sample_raw)]))
  )

zap_plot_data <- zap_block_regressions %>%
  filter(
    spec %in% c("raw", "all_blocks"),
    outcome_id %in% c("initial_apps_per_10k", "private_initial_apps_per_10k", "mixed_private_rezoning_apps_per_10k", "public_hpd_apps_per_10k", "completion_share", "failure_share"),
    !is.na(beta_treat_z)
  ) %>%
  mutate(
    spec = factor(spec, levels = c("raw", "all_blocks"), labels = c("Raw", "All blocks")),
    era = factor(era, levels = c("1990-1999", "2000-2009", "2010-2019", "2020-2025", "2010-2015", "2016-2020"))
  )

pdf(plots_out, width = 14, height = 10)

for (metric_name in c(
  "treat_z_boro",
  "units_built_total_2020_2025_per_10k_occupied",
  "units_built_50_plus_2020_2025_per_10k_occupied"
)) {
  metric_title <- case_when(
    metric_name == "treat_z_boro" ~ "1990 homeowner exposure (z within borough)",
    metric_name == "units_built_total_2020_2025_per_10k_occupied" ~ "Total units, 2020-2025 per 10,000 occupied units",
    metric_name == "units_built_50_plus_2020_2025_per_10k_occupied" ~ "50+ units, 2020-2025 per 10,000 occupied units"
  )

  print(
    ggplot(map_data) +
      geom_sf(aes(fill = .data[[metric_name]]), color = "white", linewidth = 0.3) +
      geom_sf_text(aes(label = brooklyn_short_label), size = 2.8, color = "black") +
      scale_fill_distiller(palette = "YlOrRd", direction = 1, labels = label_number(accuracy = 0.1)) +
      coord_sf(datum = NA) +
      labs(
        title = "Brooklyn CD map",
        subtitle = metric_title,
        fill = NULL
      )
  )
}

print(
  ggplot(rank_plot_data, aes(x = cd_label, y = metric_value)) +
    geom_col(fill = "#c44e52") +
    coord_flip() +
    facet_wrap(~metric, scales = "free_x", ncol = 2) +
    labs(
      title = "Brooklyn rank plot",
      subtitle = "Community districts ordered by homeowner exposure, with later construction margins alongside",
      x = NULL,
      y = NULL
    )
)

print(
  ggplot(scatter_plot_data, aes(x = treat_z_boro, y = outcome_rate)) +
    geom_hline(yintercept = 0, color = "grey85", linewidth = 0.3) +
    geom_smooth(aes(group = 1), method = "lm", se = FALSE, color = "#4c78a8", linewidth = 0.6) +
    geom_point(color = "#4c78a8", size = 2) +
    geom_text(aes(label = brooklyn_short_label), size = 2.3, check_overlap = TRUE, nudge_y = 0) +
    facet_grid(era ~ outcome_label, scales = "free_y") +
    labs(
      title = "Within-Brooklyn scatterplots",
      subtitle = "Era-average outcomes against 1990 homeowner exposure",
      x = "1990 homeowner exposure (z within borough)",
      y = "Era-average outcome"
    )
)

print(
  ggplot(coef_plot_data, aes(x = era, y = beta_treat_z, color = spec, group = spec)) +
    geom_hline(yintercept = 0, color = "grey80", linewidth = 0.3) +
    geom_line(linewidth = 0.6) +
    geom_point(size = 2) +
    geom_errorbar(aes(ymin = beta_treat_z - 1.96 * beta_treat_z_se, ymax = beta_treat_z + 1.96 * beta_treat_z_se), width = 0.15) +
    facet_wrap(~outcome_label, scales = "free_y", ncol = 2) +
    labs(
      title = "Brooklyn homeowner slopes by era",
      subtitle = "Raw versus all-block slopes for the main later-period housing and permitting outcomes",
      x = NULL,
      y = "Coefficient on homeowner exposure",
      color = NULL
    )
)

dev.off()

pdf(control_flip_plots_out, width = 13, height = 9)

print(
  ggplot(size_bin_plot_data, aes(x = size_bin, y = raw_beta_treat_z, group = 1)) +
    geom_hline(yintercept = 0, color = "grey80", linewidth = 0.3) +
    geom_line(color = "#2a6f97", linewidth = 0.7) +
    geom_point(color = "#2a6f97", size = 2.5) +
    geom_errorbar(aes(ymin = raw_beta_treat_z - 1.96 * raw_beta_treat_z_se, ymax = raw_beta_treat_z + 1.96 * raw_beta_treat_z_se), width = 0.12, color = "#2a6f97") +
    facet_wrap(~era) +
    labs(
      title = "Brooklyn size-bin slopes",
      subtitle = "Raw homeowner slopes by size bin and era",
      x = "Size bin",
      y = "Raw coefficient on homeowner exposure"
    )
)

print(
  ggplot(control_flip_plot_data, aes(x = spec, y = beta_treat_z, group = 1)) +
    geom_hline(yintercept = 0, color = "grey80", linewidth = 0.3) +
    geom_line(color = "#8c2d04", linewidth = 0.6) +
    geom_point(color = "#8c2d04", size = 2) +
    coord_flip() +
    facet_wrap(~outcome_label, scales = "free_x", ncol = 1) +
    labs(
      title = "Which block attenuates the Brooklyn homeowner slope?",
      subtitle = "2020-2025 homeowner slope after adding one standardized control block at a time, then all blocks together",
      x = NULL,
      y = "Coefficient on homeowner exposure"
    )
)

print(
  ggplot(leave_plot_data, aes(x = exclusion_label, y = beta_change_from_full_sample_raw)) +
    geom_hline(yintercept = 0, color = "grey80", linewidth = 0.3) +
    geom_col(fill = "#6a994e") +
    coord_flip() +
    facet_wrap(~outcome_id, scales = "free_y", ncol = 1) +
    labs(
      title = "Brooklyn leave-one-out sensitivity",
      subtitle = "Change in the raw 2020-2025 homeowner slope relative to the full-sample raw slope",
      x = NULL,
      y = "Change relative to full-sample raw slope"
    )
)

print(
  ggplot(zap_plot_data, aes(x = era, y = beta_treat_z, color = spec, group = spec)) +
    geom_hline(yintercept = 0, color = "grey80", linewidth = 0.3) +
    geom_line(linewidth = 0.6) +
    geom_point(size = 2) +
    facet_wrap(~outcome_id, scales = "free_y", ncol = 2) +
    labs(
      title = "Brooklyn ZAP anatomy",
      subtitle = "Raw versus all-block homeowner slopes for applications, private rezoning entry, public HPD entry, and status margins",
      x = NULL,
      y = "Coefficient on homeowner exposure",
      color = NULL
    )
)

dev.off()
