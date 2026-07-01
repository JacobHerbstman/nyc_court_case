suppressPackageStartupMessages({
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tidyr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

sanitize_era <- function(x) {
  str_replace_all(x, "-", "_")
}

era_from_term <- function(term) {
  case_when(
    str_detect(term, "1976_1979") ~ "1976-1979",
    str_detect(term, "1980_1984") ~ "1980-1984",
    str_detect(term, "1985_1989") ~ "1985-1989",
    str_detect(term, "1990_1999") ~ "1990-1999",
    str_detect(term, "2000_2009") ~ "2000-2009",
    str_detect(term, "2010_2015") ~ "2010-2015",
    str_detect(term, "2010_2019") ~ "2010-2019",
    str_detect(term, "2016_2020") ~ "2016-2020",
    str_detect(term, "2020_2025") ~ "2020-2025",
    TRUE ~ NA_character_
  )
}

add_terms <- function(df, variable_names, eras) {
  out_df <- df
  missing_variables <- setdiff(variable_names, names(df))

  if (length(missing_variables) > 0) {
    stop("Missing variables needed for era interactions: ", paste(missing_variables, collapse = ", "))
  }

  for (variable_name in variable_names) {
    for (era_value in eras) {
      out_df[[paste0(variable_name, "_x_", sanitize_era(era_value))]] <- out_df[[variable_name]] * as.integer(out_df$era == era_value)
    }
  }

  out_df
}

extract_term_rows <- function(model, requested_terms, analysis_family, sample_label, outcome_family, functional_form, control_layer, index_name, reference_era, year_min, year_max, weighted_model) {
  coef_df <- as.data.frame(coeftable(model))
  coef_df$term <- rownames(coef_df)
  rownames(coef_df) <- NULL
  p_value_col <- names(coef_df)[str_detect(names(coef_df), "^Pr\\(")][1]

  if (is.na(p_value_col)) {
    stop("Could not identify p-value column for ", analysis_family, " / ", sample_label, " / ", outcome_family, " / ", functional_form, " / ", control_layer)
  }

  conf_df <- as.data.frame(confint(model))
  conf_df$term <- rownames(conf_df)
  rownames(conf_df) <- NULL
  names(conf_df)[1:2] <- c("conf_low", "conf_high")
  missing_terms <- setdiff(requested_terms, coef_df$term)

  tibble(term = requested_terms) %>%
    left_join(coef_df, by = "term", relationship = "many-to-one") %>%
    left_join(conf_df, by = "term", relationship = "many-to-one") %>%
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
      weighted_model = weighted_model,
      estimate = Estimate,
      std_error = `Std. Error`,
      p_value = .data[[p_value_col]],
      n_obs = nobs(model),
      model_status = if_else(term %in% missing_terms, "requested_term_dropped", "estimated"),
      model_message = if_else(
        term %in% missing_terms,
        paste0("Requested term was dropped by fixest: ", term),
        NA_character_
      )
    ) %>%
    select(
      analysis_family, sample_label, outcome_family, functional_form, control_layer,
      index_name, reference_era, year_min, year_max, weighted_model,
      term, estimate, std_error, p_value, conf_low, conf_high, n_obs,
      model_status, model_message
    )
}

failed_term_rows <- function(requested_terms, analysis_family, sample_label, outcome_family, functional_form, control_layer, index_name, reference_era, year_min, year_max, weighted_model, model_status, model_message) {
  tibble(
    analysis_family = analysis_family,
    sample_label = sample_label,
    outcome_family = outcome_family,
    functional_form = functional_form,
    control_layer = control_layer,
    index_name = index_name,
    reference_era = reference_era,
    year_min = year_min,
    year_max = year_max,
    weighted_model = weighted_model,
    term = requested_terms,
    estimate = NA_real_,
    std_error = NA_real_,
    p_value = NA_real_,
    conf_low = NA_real_,
    conf_high = NA_real_,
    n_obs = NA_integer_,
    model_status = model_status,
    model_message = model_message
  )
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

assert_required_columns <- function(df, required_cols, df_name) {
  missing_cols <- setdiff(required_cols, names(df))

  if (length(missing_cols) > 0) {
    stop(df_name, " is missing required columns: ", paste(missing_cols, collapse = ", "))
  }
}

assert_unique_keys <- function(df, key_cols, df_name) {
  duplicate_keys <- df %>%
    count(across(all_of(key_cols)), name = "source_row_count") %>%
    filter(source_row_count > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(df_name, " is not unique by ", paste(key_cols, collapse = ", "), ".")
  }
}

sample_filters <- list(
  all_nyc = function(df) df,
  non_manhattan_only = function(df) filter(df, borough_name != "Manhattan"),
  manhattan_only = function(df) filter(df, borough_name == "Manhattan"),
  leave_out_bronx = function(df) filter(df, borough_name != "Bronx"),
  leave_out_brooklyn = function(df) filter(df, borough_name != "Brooklyn"),
  leave_out_manhattan = function(df) filter(df, borough_name != "Manhattan"),
  leave_out_queens = function(df) filter(df, borough_name != "Queens"),
  leave_out_staten_island = function(df) filter(df, borough_name != "Staten Island"),
  drop_101_105_106_108 = function(df) filter(df, !borocd %in% c(101L, 105L, 106L, 108L))
)

pretrend_controls <- c(
  "total_housing_units_growth_1980_1990_approx",
  "occupied_units_growth_1980_1990_approx",
  "vacancy_rate_change_1980_1990_pp_approx",
  "homeowner_share_change_1980_1990_pp_approx"
)

exact_controls <- c(
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
)

built_form_controls <- c(
  "cd_mean_built_far_lot_weighted",
  "cd_mean_max_resid_far_lot_weighted",
  "cd_share_lot_area_one_two_family",
  "cd_share_lot_area_vacant",
  "cd_share_lot_area_old_building",
  "cd_share_lot_area_protected",
  "cd_share_lot_area_parking_or_low_intensity"
)

control_blocks <- list(
  `0_fe_only` = character(),
  `1_pretrends` = pretrend_controls,
  `2_exact_1990` = exact_controls,
  `3_built_form_plus_redev_C` = built_form_controls,
  `4_all_blocks` = c(pretrend_controls, exact_controls, built_form_controls)
)

all_control_cols <- unique(c(pretrend_controls, exact_controls, built_form_controls))

cd_year_panel <- read_csv("../input/zap_ulurp_redev_cd_year_panel.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    cert_year = suppressWarnings(as.integer(cert_year)),
    borough_name = as.character(borough_name),
    era = as.character(era),
    borough_year = interaction(borough_name, cert_year, drop = TRUE),
    triple_A = treat_z_boro * redev_potential_A_z_boro,
    triple_C = treat_z_boro * redev_potential_C_z_boro
  )

mature_panel <- read_csv("../input/zap_ulurp_redev_mature_cohort_panel.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    cert_year = suppressWarnings(as.integer(cert_year)),
    borough_name = as.character(borough_name),
    era = case_when(
      cert_year >= 2010 & cert_year <= 2015 ~ "2010-2015",
      TRUE ~ as.character(era)
    ),
    borough_year = interaction(borough_name, cert_year, drop = TRUE),
    triple_A = treat_z_boro * redev_potential_A_z_boro,
    triple_C = treat_z_boro * redev_potential_C_z_boro
  )

yield_panel <- read_csv("../input/zap_ulurp_redev_yield_panel.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    cert_year = suppressWarnings(as.integer(cert_year)),
    borough_name = as.character(borough_name),
    era = as.character(yield_era),
    borough_year = interaction(borough_name, cert_year, drop = TRUE),
    triple_A = treat_z_boro * redev_potential_A_z_boro,
    triple_C = treat_z_boro * redev_potential_C_z_boro
  )

assert_required_columns(
  cd_year_panel,
  c(
    "borocd", "cert_year", "borough_name", "era", "treat_z_boro",
    "redev_potential_A_z_boro", "redev_potential_C_z_boro", "occupied_units_1990",
    "initial_apps", "private_initial_apps", "public_hpd_apps",
    "initial_apps_per_10k", "private_initial_apps_per_10k", "public_hpd_apps_per_10k",
    "initial_apps_per_res_acre", "private_initial_apps_per_res_acre", "public_hpd_apps_per_res_acre",
    all_control_cols
  ),
  "ZAP ULURP CD-year panel"
)
assert_required_columns(
  mature_panel,
  c(
    "borocd", "cert_year", "borough_name", "era", "treat_z_boro",
    "redev_potential_A_z_boro", "redev_potential_C_z_boro", "initial_apps",
    "completion_share", "failure_share", all_control_cols
  ),
  "ZAP ULURP mature cohort panel"
)
assert_required_columns(
  yield_panel,
  c(
    "borocd", "cert_year", "borough_name", "yield_era", "era", "treat_z_boro",
    "redev_potential_A_z_boro", "redev_potential_C_z_boro", "initial_apps",
    "linked_nb_50_plus_rate_0_5", "linked_gross_add_units_per_app_0_5",
    all_control_cols
  ),
  "ZAP ULURP yield panel"
)
assert_unique_keys(cd_year_panel, c("borocd", "cert_year"), "ZAP ULURP CD-year panel")
assert_unique_keys(mature_panel, c("borocd", "cert_year"), "ZAP ULURP mature cohort panel")
assert_unique_keys(yield_panel, c("borocd", "cert_year", "yield_era"), "ZAP ULURP yield panel")

if (any(mature_panel$cert_year > 2015, na.rm = TRUE)) {
  stop("Mature-status estimation currently expects certification years through 2015 only; update mature status eras before including later cohorts.")
}

run_model_block <- function(df, analysis_family, sample_label, outcome_family, outcome_var, raw_outcome_var, eras, reference_era, control_layer, functional_form, weighted_model = FALSE, allow_ppml = FALSE) {
  spec_label <- paste(analysis_family, sample_label, outcome_family, functional_form, control_layer, sep = " / ")

  if (nrow(df) == 0) {
    stop("No input rows for model spec: ", spec_label)
  }

  allowed_eras <- unique(c(reference_era, eras))

  work_df <- df %>%
    filter(
      !is.na(.data[[outcome_var]]),
      !is.na(treat_z_boro),
      !is.na(redev_potential_A_z_boro),
      !is.na(era),
      era %in% allowed_eras
    )

  if (weighted_model) {
    work_df <- work_df %>% filter(initial_apps > 0)
  }

  if (allow_ppml) {
    work_df <- work_df %>% filter(!is.na(.data[[raw_outcome_var]]), !is.na(occupied_units_1990), occupied_units_1990 > 0)
  }

  if (nrow(work_df) == 0) {
    stop("No estimation rows after restrictions for model spec: ", spec_label)
  }

  observed_eras <- unique(work_df$era[!is.na(work_df$era)])
  if (!reference_era %in% observed_eras) {
    stop("Reference era ", reference_era, " is absent after restrictions for model spec: ", spec_label)
  }

  missing_requested_eras <- setdiff(eras, observed_eras)
  if (length(missing_requested_eras) > 0) {
    stop("Requested eras absent after restrictions for model spec ", spec_label, ": ", paste(missing_requested_eras, collapse = ", "))
  }

  control_vars <- control_blocks[[control_layer]]
  missing_control_vars <- setdiff(control_vars, names(work_df))

  if (length(missing_control_vars) > 0) {
    stop("Missing controls for ", spec_label, ": ", paste(missing_control_vars, collapse = ", "))
  }

  work_df <- add_terms(work_df, c("treat_z_boro", "redev_potential_A_z_boro", "triple_A", "redev_potential_C_z_boro", control_vars), eras)

  treat_terms <- paste0("treat_z_boro_x_", sanitize_era(eras))
  redev_terms <- paste0("redev_potential_A_z_boro_x_", sanitize_era(eras))
  triple_terms <- paste0("triple_A_x_", sanitize_era(eras))
  control_terms <- unlist(lapply(control_vars, function(x) paste0(x, "_x_", sanitize_era(eras))))
  if (control_layer %in% c("3_built_form_plus_redev_C", "4_all_blocks")) {
    control_terms <- c(control_terms, paste0("redev_potential_C_z_boro_x_", sanitize_era(eras)))
  }

  formula_terms <- c(treat_terms, redev_terms, triple_terms, control_terms)
  if (length(formula_terms) == 0) {
    stop("No formula terms for model spec: ", spec_label)
  }

  if (allow_ppml) {
    model_error_message <- NA_character_
    model <- tryCatch(
      fepois(
        as.formula(paste0(raw_outcome_var, " ~ ", paste(formula_terms, collapse = " + "), " | borocd + borough_year")),
        data = work_df,
        cluster = ~borocd,
        offset = log(work_df$occupied_units_1990),
        glm.iter = 1000
      ),
      error = function(e) {
        model_error_message <<- conditionMessage(e)
        NULL
      }
    )
  } else if (weighted_model) {
    model_error_message <- NA_character_
    model <- tryCatch(
      feols(
        as.formula(paste0(outcome_var, " ~ ", paste(formula_terms, collapse = " + "), " | borocd + borough_year")),
        data = work_df,
        weights = ~initial_apps,
        cluster = ~borocd
      ),
      error = function(e) {
        model_error_message <<- conditionMessage(e)
        NULL
      }
    )
  } else {
    model_error_message <- NA_character_
    model <- tryCatch(
      feols(
        as.formula(paste0(outcome_var, " ~ ", paste(formula_terms, collapse = " + "), " | borocd + borough_year")),
        data = work_df,
        cluster = ~borocd
      ),
      error = function(e) {
        model_error_message <<- conditionMessage(e)
        NULL
      }
    )
  }

  if (is.null(model)) {
    return(
      bind_rows(
        failed_term_rows(treat_terms, analysis_family, sample_label, outcome_family, functional_form, control_layer, "A", reference_era, min(work_df$cert_year), max(work_df$cert_year), weighted_model, "model_failed", model_error_message) %>%
          mutate(term_group = "homeowner"),
        failed_term_rows(triple_terms, analysis_family, sample_label, outcome_family, functional_form, control_layer, "A", reference_era, min(work_df$cert_year), max(work_df$cert_year), weighted_model, "model_failed", model_error_message) %>%
          mutate(term_group = "homeowner_x_redev")
      ) %>%
        mutate(
          era = era_from_term(term),
          converged = FALSE
        )
    )
  }

  if (allow_ppml && isFALSE(model$convStatus)) {
    return(
      bind_rows(
        failed_term_rows(treat_terms, analysis_family, sample_label, outcome_family, functional_form, control_layer, "A", reference_era, min(work_df$cert_year), max(work_df$cert_year), weighted_model, "ppml_not_converged", paste0("PPML did not converge for ", spec_label, ".")) %>%
          mutate(term_group = "homeowner"),
        failed_term_rows(triple_terms, analysis_family, sample_label, outcome_family, functional_form, control_layer, "A", reference_era, min(work_df$cert_year), max(work_df$cert_year), weighted_model, "ppml_not_converged", paste0("PPML did not converge for ", spec_label, ".")) %>%
          mutate(term_group = "homeowner_x_redev")
      ) %>%
        mutate(
          era = era_from_term(term),
          converged = FALSE
        )
    )
  }

  bind_rows(
    extract_term_rows(model, treat_terms, analysis_family, sample_label, outcome_family, functional_form, control_layer, "A", reference_era, min(work_df$cert_year), max(work_df$cert_year), weighted_model) %>%
      mutate(term_group = "homeowner"),
    extract_term_rows(model, triple_terms, analysis_family, sample_label, outcome_family, functional_form, control_layer, "A", reference_era, min(work_df$cert_year), max(work_df$cert_year), weighted_model) %>%
      mutate(term_group = "homeowner_x_redev")
  ) %>%
    mutate(
      era = era_from_term(term),
      converged = TRUE
    )
}

results_rows <- list()
result_index <- 1L

apps_eras <- c("1980-1984", "1985-1989", "1990-1999", "2000-2009", "2010-2019", "2020-2025")
for (outcome_family in c("initial_apps", "private_initial_apps", "public_hpd_apps")) {
  for (control_layer in names(control_blocks)) {
    for (functional_form in c("linear_occ", "linear_acre")) {
      outcome_var <- if (functional_form == "linear_occ") {
        paste0(outcome_family, "_per_10k")
      } else {
        paste0(outcome_family, "_per_res_acre")
      }

      model_rows <- run_model_block(
        df = cd_year_panel,
        analysis_family = "applications",
        sample_label = "all_nyc",
        outcome_family = outcome_family,
        outcome_var = outcome_var,
        raw_outcome_var = outcome_family,
        eras = apps_eras,
        reference_era = "1976-1979",
        control_layer = control_layer,
        functional_form = functional_form,
        weighted_model = FALSE,
        allow_ppml = FALSE
      )
      results_rows[[result_index]] <- model_rows
      result_index <- result_index + 1L
    }

    model_rows <- run_model_block(
      df = cd_year_panel,
      analysis_family = "applications",
      sample_label = "all_nyc",
      outcome_family = outcome_family,
      outcome_var = paste0(outcome_family, "_per_10k"),
      raw_outcome_var = outcome_family,
      eras = apps_eras,
      reference_era = "1976-1979",
      control_layer = control_layer,
      functional_form = "ppml_occ",
      weighted_model = FALSE,
      allow_ppml = TRUE
    )
    results_rows[[result_index]] <- model_rows
    result_index <- result_index + 1L
  }
}

status_eras <- c("1985-1989", "1990-1999", "2000-2009", "2010-2015")
for (outcome_family in c("completion_share", "failure_share")) {
  for (control_layer in names(control_blocks)) {
    model_rows <- run_model_block(
      df = mature_panel %>% filter(cert_year >= 1980),
      analysis_family = "mature_status",
      sample_label = "all_nyc",
      outcome_family = outcome_family,
      outcome_var = outcome_family,
      raw_outcome_var = outcome_family,
      eras = status_eras,
      reference_era = "1980-1984",
      control_layer = control_layer,
      functional_form = "linear_share",
      weighted_model = TRUE,
      allow_ppml = FALSE
    )
    results_rows[[result_index]] <- model_rows
    result_index <- result_index + 1L
  }
}

yield_regression_specs <- tibble(
  outcome_family = c("linked_nb_50_plus_rate", "linked_gross_add_units_per_app"),
  outcome_var = c("linked_nb_50_plus_rate_0_5", "linked_gross_add_units_per_app_0_5")
) 

yield_regression_df <- yield_panel %>% filter(cert_year >= 2010, cert_year <= 2020, !is.na(era))
for (spec_idx in seq_len(nrow(yield_regression_specs))) {
  for (control_layer in names(control_blocks)) {
    model_rows <- run_model_block(
      df = yield_regression_df,
      analysis_family = "build_yield",
      sample_label = "all_nyc",
      outcome_family = yield_regression_specs$outcome_family[[spec_idx]],
      outcome_var = yield_regression_specs$outcome_var[[spec_idx]],
      raw_outcome_var = yield_regression_specs$outcome_var[[spec_idx]],
      eras = c("2016-2020"),
      reference_era = "2010-2015",
      control_layer = control_layer,
      functional_form = "linear_yield",
      weighted_model = TRUE,
      allow_ppml = FALSE
    )
    results_rows[[result_index]] <- model_rows
    result_index <- result_index + 1L
  }
}

for (sample_label in names(sample_filters)) {
  if (sample_label == "all_nyc") {
    next
  }

  for (outcome_family in c("initial_apps", "private_initial_apps", "public_hpd_apps")) {
    outcome_var <- paste0(outcome_family, "_per_10k")
    model_rows <- run_model_block(
      df = sample_filters[[sample_label]](cd_year_panel),
      analysis_family = "applications",
      sample_label = sample_label,
      outcome_family = outcome_family,
      outcome_var = outcome_var,
      raw_outcome_var = outcome_family,
      eras = apps_eras,
      reference_era = "1976-1979",
      control_layer = "4_all_blocks",
      functional_form = "linear_occ",
      weighted_model = FALSE,
      allow_ppml = FALSE
    )
    results_rows[[result_index]] <- model_rows
    result_index <- result_index + 1L
  }

  for (outcome_family in c("completion_share", "failure_share")) {
    model_rows <- run_model_block(
      df = sample_filters[[sample_label]](mature_panel %>% filter(cert_year >= 1980)),
      analysis_family = "mature_status",
      sample_label = sample_label,
      outcome_family = outcome_family,
      outcome_var = outcome_family,
      raw_outcome_var = outcome_family,
      eras = status_eras,
      reference_era = "1980-1984",
      control_layer = "4_all_blocks",
      functional_form = "linear_share",
      weighted_model = TRUE,
      allow_ppml = FALSE
    )
    results_rows[[result_index]] <- model_rows
    result_index <- result_index + 1L
  }

  for (spec_idx in seq_len(nrow(yield_regression_specs))) {
    model_rows <- run_model_block(
      df = sample_filters[[sample_label]](yield_regression_df),
      analysis_family = "build_yield",
      sample_label = sample_label,
      outcome_family = yield_regression_specs$outcome_family[[spec_idx]],
      outcome_var = yield_regression_specs$outcome_var[[spec_idx]],
      raw_outcome_var = yield_regression_specs$outcome_var[[spec_idx]],
      eras = c("2016-2020"),
      reference_era = "2010-2015",
      control_layer = "4_all_blocks",
      functional_form = "linear_yield",
      weighted_model = TRUE,
      allow_ppml = FALSE
    )
    results_rows[[result_index]] <- model_rows
    result_index <- result_index + 1L
  }
}

model_summary_df <- bind_rows(results_rows) %>%
  filter(!is.na(term)) %>%
  mutate(
    era = factor(era, levels = c("1976-1979", "1980-1984", "1985-1989", "1990-1999", "2000-2009", "2010-2015", "2010-2019", "2016-2020", "2020-2025")),
    control_layer = factor(control_layer, levels = names(control_blocks))
  ) %>%
  arrange(analysis_family, sample_label, outcome_family, functional_form, control_layer, term_group, era)

if (any(model_summary_df$model_status == "estimated" & (is.na(model_summary_df$estimate) | is.na(model_summary_df$std_error)))) {
  stop("Model summary contains estimated rows with missing estimates or standard errors.")
}

nested_diag_df <- model_summary_df %>%
  filter(
    sample_label == "all_nyc",
    functional_form %in% c("linear_occ", "linear_share", "linear_yield"),
    control_layer %in% c("0_fe_only", "4_all_blocks"),
    outcome_family %in% c("initial_apps", "private_initial_apps", "public_hpd_apps", "completion_share", "failure_share", "linked_nb_50_plus_rate", "linked_gross_add_units_per_app")
  ) %>%
  group_by(analysis_family, outcome_family, term_group, era, control_layer) %>%
  summarise(
    estimate = first(estimate),
    std_error = first(std_error),
    .groups = "drop"
  ) %>%
  pivot_wider(
    names_from = control_layer,
    values_from = c(estimate, std_error)
  ) %>%
  mutate(
    attenuation_class = mapply(classify_attenuation, estimate_0_fe_only, estimate_4_all_blocks, std_error_0_fe_only, std_error_4_all_blocks)
  ) %>%
  arrange(analysis_family, outcome_family, term_group, era)

write_csv_if_changed(model_summary_df, "../output/zap_ulurp_redev_model_summary.csv")
write_csv_if_changed(nested_diag_df, "../output/zap_ulurp_redev_nested_diagnostics.csv")

headline_plot_df <- model_summary_df %>%
  filter(
    sample_label == "all_nyc",
    model_status == "estimated",
    control_layer %in% c("0_fe_only", "4_all_blocks"),
    term_group == "homeowner_x_redev",
    functional_form %in% c("linear_occ", "linear_share", "linear_yield"),
    outcome_family %in% c("initial_apps", "private_initial_apps", "public_hpd_apps", "completion_share", "failure_share", "linked_nb_50_plus_rate", "linked_gross_add_units_per_app")
  )

sensitivity_plot_df <- model_summary_df %>%
  filter(
    sample_label != "all_nyc",
    model_status == "estimated",
    control_layer == "4_all_blocks",
    term_group == "homeowner_x_redev",
    functional_form %in% c("linear_occ", "linear_share", "linear_yield"),
    outcome_family %in% c("private_initial_apps", "completion_share", "linked_nb_50_plus_rate", "linked_gross_add_units_per_app")
  )

temp_pdf <- tempfile(fileext = ".pdf")
pdf(temp_pdf, width = 10, height = 8)

print(
  ggplot(headline_plot_df, aes(x = era, y = estimate, color = control_layer, group = control_layer)) +
    geom_hline(yintercept = 0, linetype = "dashed") +
    geom_point(size = 2, na.rm = TRUE) +
    geom_line(na.rm = TRUE) +
    facet_wrap(~ outcome_family, scales = "free_y") +
    labs(
      title = "Homeowner × redevelopment interaction coefficients",
      subtitle = "All NYC, FE only versus all controls",
      x = NULL,
      y = "Estimate",
      color = "Control layer"
    ) +
    theme_minimal(base_size = 11)
)

print(
  ggplot(sensitivity_plot_df, aes(x = sample_label, y = estimate, color = era, group = era)) +
    geom_hline(yintercept = 0, linetype = "dashed") +
    geom_point(size = 1.8, position = position_dodge(width = 0.3), na.rm = TRUE) +
    geom_line(aes(group = interaction(era, outcome_family)), linewidth = 0.5, alpha = 0.6, na.rm = TRUE) +
    facet_wrap(~ outcome_family, scales = "free_y") +
    labs(
      title = "Sample-split sensitivity for homeowner × redevelopment interaction",
      subtitle = "All-controls specification",
      x = NULL,
      y = "Estimate",
      color = "Era"
    ) +
    theme_minimal(base_size = 10) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)

dev.off()

copy_if_changed(temp_pdf, "../output/zap_ulurp_redev_coefficients.pdf")

cat("Wrote ZAP ULURP redevelopment regression outputs to ../output\n")
