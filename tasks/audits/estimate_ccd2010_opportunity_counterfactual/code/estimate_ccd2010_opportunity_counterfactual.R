suppressPackageStartupMessages({
  library(dplyr)
  library(fixest)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

sanitize_period <- function(x) {
  str_replace_all(x, "-", "_")
}

safe_vcov <- function(model) {
  out <- tryCatch(vcov(model), error = function(e) NULL)
  if (is.null(out)) {
    coef_names <- names(coef(model))
    out <- matrix(NA_real_, nrow = length(coef_names), ncol = length(coef_names), dimnames = list(coef_names, coef_names))
  }
  out
}

make_design_vector <- function(period_value, delta_treat, redev_value, include_interaction, coef_names) {
  out <- setNames(rep(0, length(coef_names)), coef_names)
  treat_term <- paste0("treat_x_", sanitize_period(period_value))
  triple_term <- paste0("triple_x_", sanitize_period(period_value))

  if (treat_term %in% coef_names) {
    out[[treat_term]] <- delta_treat
  }

  if (include_interaction && triple_term %in% coef_names) {
    out[[triple_term]] <- delta_treat * redev_value
  }

  out
}

vector_quadratic_se <- function(gradient, vcov_matrix) {
  if (length(gradient) == 0 || any(is.na(vcov_matrix))) {
    return(NA_real_)
  }

  out <- as.numeric(t(gradient) %*% vcov_matrix %*% gradient)
  if (is.na(out) || out < 0) {
    NA_real_
  } else {
    sqrt(out)
  }
}

sum_gradients <- function(gradient_list, coef_names) {
  if (length(gradient_list) == 0) {
    return(setNames(rep(0, length(coef_names)), coef_names))
  }

  Reduce(`+`, gradient_list)
}

period_definitions <- tribble(
  ~period, ~period_start, ~period_end, ~period_role,
  "1980-1989", 1980L, 1989L, "pre/member-deference placebo and historical diagnostic",
  "1990-1999", 1990L, 1999L, "pre-hardening or early-transition diagnostic",
  "2000-2009", 2000L, 2009L, "pre/post transition diagnostic",
  "2010-2019", 2010L, 2019L, "main post-deference period",
  "2020-2025", 2020L, 2025L, "secondary post-deference/current period"
) |>
  mutate(period_years = period_end - period_start + 1L)

model_periods <- period_definitions$period
post_periods <- c("2010-2019", "2020-2025")

redev_df <- read_csv("../input/ccdist2010_redevelopment_potential.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    council_district = suppressWarnings(as.integer(council_district)),
    borough_code = as.character(borough_code),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro)),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990)),
    redev_A_z_boro = suppressWarnings(as.numeric(redev_A_z_boro)),
    redev_A_all_lots_z_boro = suppressWarnings(as.numeric(redev_A_all_lots_z_boro)),
    redev_A2002_allowed_all_lots_z_boro = suppressWarnings(as.numeric(redev_A2002_allowed_all_lots_z_boro)),
    redev_C_z_boro = suppressWarnings(as.numeric(redev_C_z_boro)),
    redev_A_25v4_z_boro = suppressWarnings(as.numeric(redev_A_25v4_z_boro)),
    redev_A2010approx_z_boro = suppressWarnings(as.numeric(redev_A2010approx_z_boro)),
    high_redev_A = as.logical(high_redev_A),
    high_redev_A_all_lots = as.logical(high_redev_A_all_lots),
    high_redev_A2002_allowed_all_lots = as.logical(high_redev_A2002_allowed_all_lots),
    high_redev_C = as.logical(high_redev_C),
    high_redev_A_25v4 = as.logical(high_redev_A_25v4),
    high_redev_A2010approx = as.logical(high_redev_A2010approx)
  ) |>
  group_by(borough_code, borough_name) |>
  mutate(
    homeowner_split_median_boro = stats::median(treat_z_boro, na.rm = TRUE),
    high_homeowner = treat_z_boro >= homeowner_split_median_boro,
    treat_cf_low_homeowner_boro = stats::median(treat_z_boro[!high_homeowner], na.rm = TRUE),
    two_by_two_cell_A = case_when(
      !high_homeowner & !high_redev_A ~ "LL",
      !high_homeowner & high_redev_A ~ "LH",
      high_homeowner & !high_redev_A ~ "HL",
      high_homeowner & high_redev_A ~ "HH",
      TRUE ~ NA_character_
    ),
    two_by_two_label_A = case_when(
      two_by_two_cell_A == "LL" ~ "Low homeowner / Low redev",
      two_by_two_cell_A == "LH" ~ "Low homeowner / High redev",
      two_by_two_cell_A == "HL" ~ "High homeowner / Low redev",
      two_by_two_cell_A == "HH" ~ "High homeowner / High redev",
      TRUE ~ NA_character_
    )
  ) |>
  ungroup()

if (nrow(redev_df) != 51 || anyDuplicated(redev_df$district_id)) {
  stop("Redevelopment-potential input must be unique across exactly 51 Council districts.")
}

period_panel <- bind_rows(lapply(seq_len(nrow(period_definitions)), function(i) {
  period_row <- period_definitions[i, ]

  read_csv("../input/ccdist2010_homeownership_long_units_series.csv", show_col_types = FALSE, na = c("", "NA")) |>
    filter(
      source_family == "mappluto_proxy_25v4",
      series_kind == "preferred_long_series",
      series_family %in% c("units_built_total", "units_built_1_4", "units_built_5_plus", "units_built_50_plus", "projects_built_50_plus"),
      year >= period_row$period_start,
      year <= period_row$period_end
    ) |>
    mutate(
      district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
      council_district = suppressWarnings(as.integer(council_district)),
      borough_code = as.character(borough_code),
      outcome_value = suppressWarnings(as.numeric(outcome_value))
    ) |>
    group_by(source_family, series_family, series_label, district_id, council_district, borough_code, borough_name) |>
    summarize(observed_units = sum(outcome_value, na.rm = TRUE), .groups = "drop") |>
    mutate(
      period = period_row$period,
      period_start = period_row$period_start,
      period_end = period_row$period_end,
      period_years = period_row$period_years,
      period_role = period_row$period_role
    )
}))

if (anyDuplicated(period_panel[c("series_family", "district_id", "period")])) {
  stop("Period panel is not unique by outcome, district, and period.")
}

analysis_df <- period_panel |>
  left_join(
    redev_df |>
      select(
        district_id, council_district, borough_code, borough_name,
        treat_z_boro, high_homeowner, homeowner_split_median_boro, treat_cf_low_homeowner_boro,
        occupied_units_1990, total_housing_units_1990, h_ccd_1990, h_b_1990, treat_pp,
        vacancy_rate_1990, total_population_1990, median_household_income_1990,
        redev_A_z_boro, redev_A_all_lots_z_boro, redev_A2002_allowed_all_lots_z_boro,
        redev_C_z_boro, redev_A_25v4_z_boro, redev_A2010approx_z_boro,
        high_redev_A, high_redev_A_all_lots, high_redev_A2002_allowed_all_lots,
        high_redev_C, high_redev_A_25v4, high_redev_A2010approx,
        two_by_two_cell_A, two_by_two_label_A
      ),
    by = c("district_id", "council_district", "borough_code", "borough_name"),
    relationship = "many-to-one"
  ) |>
  mutate(
    outcome_margin = series_family,
    outcome_label = series_label,
    exposure_occ_period = occupied_units_1990 / 10000 * period_years,
    log_exposure = log(exposure_occ_period),
    rate_annualized_per_10k_occ1990 = if_else(exposure_occ_period > 0, observed_units / exposure_occ_period, NA_real_),
    borough_period = interaction(borough_name, period, drop = TRUE)
  )

if (any(is.na(analysis_df$treat_z_boro)) || any(is.na(analysis_df$redev_A_z_boro))) {
  stop("Analysis panel has missing treatment or main redevelopment potential fields.")
}

spec_df <- tribble(
  ~spec_id, ~estimator, ~estimation_sample, ~redev_var, ~high_redev_var, ~include_interaction, ~spec_label,
  "main_A_PLUTO_WLS", "wls", "high_redev", "redev_A_z_boro", "high_redev_A", FALSE, "Main WLS on high-redevelopment-potential districts using 18v1.1 index A",
  "main_A2002_allowed_all_lots_WLS", "wls", "high_redev", "redev_A2002_allowed_all_lots_z_boro", "high_redev_A2002_allowed_all_lots", FALSE, "Timing-clean WLS using 2002 all-lots allowed-envelope residual-capacity index A",
  "main_A_PLUTO_PPML", "ppml", "high_redev", "redev_A_z_boro", "high_redev_A", FALSE, "PPML robustness on high-redevelopment-potential districts using 18v1.1 index A",
  "full_interaction_A_PLUTO_WLS", "wls", "full", "redev_A_z_boro", "high_redev_A", TRUE, "Full-sample WLS interaction between homeownership and 18v1.1 index A",
  "A_all_lots_PLUTO_WLS", "wls", "high_redev", "redev_A_all_lots_z_boro", "high_redev_A_all_lots", FALSE, "WLS robustness using all-lots unused residential-capacity index A",
  "C_PLUTO_WLS", "wls", "high_redev", "redev_C_z_boro", "high_redev_C", FALSE, "WLS robustness using composite index C",
  "A25_PLUTO_WLS", "wls", "high_redev", "redev_A_25v4_z_boro", "high_redev_A_25v4", FALSE, "WLS robustness using 25v4 index A",
  "A2010approx_PLUTO_WLS", "wls", "high_redev", "redev_A2010approx_z_boro", "high_redev_A2010approx", FALSE, "WLS robustness using approximate-2010 opportunity index A"
)

make_counterfactual_for_spec <- function(spec_row, outcome_value) {
  work_df <- analysis_df |>
    filter(outcome_margin == outcome_value) |>
    mutate(
      redev_for_spec = .data[[spec_row$redev_var]],
      high_redev_for_spec = .data[[spec_row$high_redev_var]],
      target_counterfactual = high_homeowner & high_redev_for_spec,
      in_estimation_sample = if (spec_row$estimation_sample == "high_redev") high_redev_for_spec else TRUE
    )

  reference_df <- work_df |>
    distinct(borough_code, borough_name, district_id, treat_z_boro, high_homeowner, high_redev_for_spec) |>
    group_by(borough_code, borough_name) |>
    summarize(
      reference_low_homeowner_high_redev_count = sum(!high_homeowner & high_redev_for_spec, na.rm = TRUE),
      reference_low_homeowner_count = sum(!high_homeowner, na.rm = TRUE),
      treat_cf_low_homeowner_high_redev_boro = stats::median(treat_z_boro[!high_homeowner & high_redev_for_spec], na.rm = TRUE),
      treat_cf_low_homeowner_all_boro = stats::median(treat_z_boro[!high_homeowner], na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(
      reference_fallback_all_low_homeowner = !is.finite(treat_cf_low_homeowner_high_redev_boro),
      treat_cf_for_spec_boro = if_else(reference_fallback_all_low_homeowner, treat_cf_low_homeowner_all_boro, treat_cf_low_homeowner_high_redev_boro)
    )

  if (any(!is.finite(reference_df$treat_cf_for_spec_boro))) {
    stop("Could not construct a finite low-homeowner counterfactual reference for ", spec_row$spec_id, ".")
  }

  work_df <- work_df |>
    left_join(reference_df, by = c("borough_code", "borough_name"), relationship = "many-to-one")

  for (period_value in model_periods) {
    suffix <- sanitize_period(period_value)
    work_df[[paste0("treat_x_", suffix)]] <- if_else(work_df$period == period_value, work_df$treat_z_boro, 0)
    work_df[[paste0("redev_x_", suffix)]] <- if_else(work_df$period == period_value, work_df$redev_for_spec, 0)
    work_df[[paste0("triple_x_", suffix)]] <- if_else(work_df$period == period_value, work_df$treat_z_boro * work_df$redev_for_spec, 0)
  }

  fit_df <- work_df |>
    filter(in_estimation_sample, is.finite(rate_annualized_per_10k_occ1990), is.finite(redev_for_spec), is.finite(log_exposure))

  treat_terms <- paste0("treat_x_", sanitize_period(model_periods))
  redev_terms <- paste0("redev_x_", sanitize_period(model_periods))
  triple_terms <- paste0("triple_x_", sanitize_period(model_periods))
  rhs_terms <- c(treat_terms, redev_terms, if (spec_row$include_interaction) triple_terms else character())

  if (spec_row$estimator == "wls") {
    model_formula <- as.formula(paste0("rate_annualized_per_10k_occ1990 ~ ", paste(rhs_terms, collapse = " + "), " | borough_period"))
    model <- feols(model_formula, data = fit_df, weights = ~ occupied_units_1990, cluster = ~ district_id, warn = FALSE, notes = FALSE)
  } else {
    model_formula <- as.formula(paste0("observed_units ~ ", paste(c(rhs_terms, "offset(log_exposure)"), collapse = " + "), " | borough_period"))
    model <- fepois(model_formula, data = fit_df, cluster = ~ district_id, warn = FALSE, notes = FALSE)
  }

  coef_vector <- coef(model)
  vcov_matrix <- safe_vcov(model)
  coef_names <- names(coef_vector)

  work_df <- work_df |>
    mutate(
      delta_treat_to_low_homeowner = treat_cf_for_spec_boro - treat_z_boro,
      spec_id = spec_row$spec_id,
      estimator = spec_row$estimator,
      estimation_sample = spec_row$estimation_sample,
      opportunity_index = spec_row$redev_var,
      high_redev_definition = spec_row$high_redev_var,
      include_interaction = spec_row$include_interaction,
      spec_label = spec_row$spec_label,
      interpretation_label = "descriptive opportunity-set counterfactual / welfare bridge",
      uncertainty_label = "model-based coefficient uncertainty only"
    )

  design_list <- lapply(seq_len(nrow(work_df)), function(i) {
    make_design_vector(
      work_df$period[[i]],
      work_df$delta_treat_to_low_homeowner[[i]],
      work_df$redev_for_spec[[i]],
      spec_row$include_interaction,
      coef_names
    )
  })

  linear_delta <- vapply(design_list, function(x) sum(x * coef_vector[names(x)], na.rm = TRUE), numeric(1))

  if (spec_row$estimator == "wls") {
    missing_units <- linear_delta * work_df$exposure_occ_period
    gradient_list <- lapply(seq_along(design_list), function(i) work_df$exposure_occ_period[[i]] * design_list[[i]])
    rate_delta <- linear_delta
  } else {
    missing_units <- work_df$observed_units * (exp(linear_delta) - 1)
    gradient_list <- lapply(seq_along(design_list), function(i) work_df$observed_units[[i]] * exp(linear_delta[[i]]) * design_list[[i]])
    rate_delta <- if_else(work_df$exposure_occ_period > 0, missing_units / work_df$exposure_occ_period, NA_real_)
  }

  missing_units <- if_else(work_df$target_counterfactual, missing_units, NA_real_)
  rate_delta <- if_else(work_df$target_counterfactual, rate_delta, NA_real_)

  gradient_list <- lapply(seq_along(gradient_list), function(i) {
    if (work_df$target_counterfactual[[i]]) {
      gradient_list[[i]]
    } else {
      setNames(rep(0, length(coef_names)), coef_names)
    }
  })

  missing_se <- vapply(seq_along(gradient_list), function(i) {
    if (work_df$target_counterfactual[[i]]) {
      vector_quadratic_se(gradient_list[[i]], vcov_matrix)
    } else {
      NA_real_
    }
  }, numeric(1))

  period_rows <- work_df |>
    mutate(
      rate_delta_per_10k_occ1990 = rate_delta,
      missing_units_signed = missing_units,
      missing_units_positive = if_else(!is.na(missing_units_signed), pmax(missing_units_signed, 0), NA_real_),
      missing_units_se = missing_se,
      missing_units_conf_low = missing_units_signed - 1.96 * missing_units_se,
      missing_units_conf_high = missing_units_signed + 1.96 * missing_units_se,
      units_counterfactual = observed_units + missing_units_signed,
      gradient = I(gradient_list)
    )

  pooled_rows <- bind_rows(lapply(group_split(group_by(period_rows, spec_id, outcome_margin, district_id)), function(group_df) {
    post_df <- group_df |>
      filter(period %in% post_periods)

    if (nrow(post_df) == 0) {
      return(tibble())
    }

    first_row <- post_df[1, ]
    target_flag <- first_row$target_counterfactual[[1]]
    gradient_sum <- sum_gradients(post_df$gradient, coef_names)
    pooled_missing <- if (target_flag) sum(post_df$missing_units_signed, na.rm = TRUE) else NA_real_
    pooled_se <- if (target_flag) vector_quadratic_se(gradient_sum, vcov_matrix) else NA_real_
    pooled_observed <- sum(post_df$observed_units, na.rm = TRUE)
    pooled_exposure <- sum(post_df$exposure_occ_period, na.rm = TRUE)

    first_row |>
      mutate(
        period = "2010-2025",
        period_start = 2010L,
        period_end = 2025L,
        period_years = 16L,
        period_role = "pooled welfare-bridge period",
        observed_units = pooled_observed,
        exposure_occ_period = pooled_exposure,
        log_exposure = log(pooled_exposure),
        rate_annualized_per_10k_occ1990 = if_else(pooled_exposure > 0, pooled_observed / pooled_exposure, NA_real_),
        rate_delta_per_10k_occ1990 = if_else(pooled_exposure > 0 & target_flag, pooled_missing / pooled_exposure, NA_real_),
        missing_units_signed = pooled_missing,
        missing_units_positive = if_else(!is.na(pooled_missing), pmax(pooled_missing, 0), NA_real_),
        missing_units_se = pooled_se,
        missing_units_conf_low = pooled_missing - 1.96 * pooled_se,
        missing_units_conf_high = pooled_missing + 1.96 * pooled_se,
        units_counterfactual = pooled_observed + pooled_missing,
        gradient = I(list(gradient_sum))
      )
  }))

  panel_rows <- bind_rows(period_rows, pooled_rows)

  summarize_with_uncertainty <- function(input_df, group_vars, geography_level_value) {
    bind_rows(lapply(group_split(group_by(input_df |> filter(target_counterfactual), across(all_of(group_vars)))), function(group_df) {
      if (nrow(group_df) == 0) {
        return(tibble())
      }

      gradient_sum <- sum_gradients(group_df$gradient, coef_names)
      missing_sum <- sum(group_df$missing_units_signed, na.rm = TRUE)
      missing_se_sum <- vector_quadratic_se(gradient_sum, vcov_matrix)
      observed_sum <- sum(group_df$observed_units, na.rm = TRUE)

      group_df[1, group_vars, drop = FALSE] |>
        mutate(
          geography_level = geography_level_value,
          target_district_count = n_distinct(group_df$district_id),
          observed_units = observed_sum,
          counterfactual_units = observed_sum + missing_sum,
          missing_units_signed = missing_sum,
          missing_units_positive = sum(pmax(group_df$missing_units_signed, 0), na.rm = TRUE),
          missing_units_se = missing_se_sum,
          missing_units_conf_low = missing_sum - 1.96 * missing_se_sum,
          missing_units_conf_high = missing_sum + 1.96 * missing_se_sum
        )
    }))
  }

  summary_borough <- summarize_with_uncertainty(
    panel_rows,
    c("spec_id", "estimator", "outcome_margin", "outcome_label", "period", "period_start", "period_end", "period_years", "period_role", "borough_code", "borough_name"),
    "borough"
  )

  summary_city <- panel_rows |>
    mutate(borough_code = "city", borough_name = "All NYC") |>
    summarize_with_uncertainty(
      c("spec_id", "estimator", "outcome_margin", "outcome_label", "period", "period_start", "period_end", "period_years", "period_role", "borough_code", "borough_name"),
      "city"
    )

  coef_df <- as.data.frame(coeftable(model))
  coef_df$term <- rownames(coef_df)
  rownames(coef_df) <- NULL
  p_value_col <- names(coef_df)[str_detect(names(coef_df), "^Pr\\(")][1]
  if (is.na(p_value_col)) {
    coef_df$p_value <- NA_real_
  } else {
    coef_df$p_value <- coef_df[[p_value_col]]
  }

  missing_requested_terms <- setdiff(rhs_terms, coef_names)
  model_summary <- coef_df |>
    as_tibble() |>
    transmute(
      spec_id = spec_row$spec_id,
      estimator = spec_row$estimator,
      outcome_margin = outcome_value,
      term = term,
      estimate = Estimate,
      std_error = `Std. Error`,
      p_value = p_value,
      n_obs = nobs(model),
      dropped_requested_terms = paste(missing_requested_terms, collapse = ";")
    )

  list(
    panel = panel_rows,
    summary = bind_rows(summary_borough, summary_city),
    model_summary = model_summary
  )
}

fit_results <- list()
result_index <- 1L
for (spec_index in seq_len(nrow(spec_df))) {
  for (outcome_value in c("units_built_total", "units_built_5_plus", "units_built_50_plus", "units_built_1_4", "projects_built_50_plus")) {
    fit_results[[result_index]] <- make_counterfactual_for_spec(spec_df[spec_index, ], outcome_value)
    result_index <- result_index + 1L
  }
}

counterfactual_panel <- bind_rows(lapply(fit_results, `[[`, "panel")) |>
  select(
    spec_id, spec_label, estimator, estimation_sample, opportunity_index, high_redev_definition,
    include_interaction, interpretation_label, uncertainty_label, source_family, outcome_margin, outcome_label,
    district_id, council_district, borough_code, borough_name, period, period_start, period_end,
    period_years, period_role, observed_units, units_counterfactual, missing_units_signed,
    missing_units_positive, missing_units_se, missing_units_conf_low, missing_units_conf_high,
    rate_annualized_per_10k_occ1990, rate_delta_per_10k_occ1990, exposure_occ_period,
    treat_z_boro, treat_cf_low_homeowner_boro, treat_cf_for_spec_boro,
    reference_low_homeowner_high_redev_count, reference_low_homeowner_count,
    reference_fallback_all_low_homeowner, delta_treat_to_low_homeowner,
    high_homeowner, redev_for_spec, high_redev_for_spec, target_counterfactual,
    in_estimation_sample, two_by_two_cell_A, two_by_two_label_A
  ) |>
  arrange(spec_id, outcome_margin, council_district, period)

counterfactual_summary <- bind_rows(lapply(fit_results, `[[`, "summary")) |>
  mutate(
    interpretation_label = "descriptive opportunity-set counterfactual / welfare bridge",
    uncertainty_label = "model-based coefficient uncertainty only"
  ) |>
  select(
    spec_id, estimator, interpretation_label, uncertainty_label, geography_level, borough_code, borough_name,
    outcome_margin, outcome_label, period, period_start, period_end, period_years, period_role,
    target_district_count, observed_units, counterfactual_units, missing_units_signed,
    missing_units_positive, missing_units_se, missing_units_conf_low, missing_units_conf_high
  ) |>
  arrange(spec_id, outcome_margin, period, geography_level, borough_code)

model_summary <- bind_rows(lapply(fit_results, `[[`, "model_summary")) |>
  arrange(spec_id, outcome_margin, term)

main_treatment_coefficients <- model_summary |>
  filter(spec_id == "main_A_PLUTO_WLS", str_detect(term, "^treat_x_")) |>
  mutate(period = str_replace_all(str_remove(term, "^treat_x_"), "_", "-")) |>
  select(spec_id, estimator, outcome_margin, period, term, estimate, std_error, p_value, n_obs, dropped_requested_terms) |>
  arrange(outcome_margin, period)

main_dropped_terms <- model_summary |>
  filter(spec_id == "main_A_PLUTO_WLS") |>
  distinct(outcome_margin, dropped_requested_terms) |>
  mutate(
    has_dropped_terms = !is.na(dropped_requested_terms) & dropped_requested_terms != "",
    has_dropped_post_treatment_terms = str_detect(coalesce(dropped_requested_terms, ""), "treat_x_2010_2019|treat_x_2020_2025")
  )

if (any(main_dropped_terms$has_dropped_post_treatment_terms)) {
  stop("Main post-period treatment terms were dropped in main_A_PLUTO_WLS.")
}

two_by_two_df <- analysis_df |>
  mutate(
    two_by_two_cell = two_by_two_cell_A,
    two_by_two_label = two_by_two_label_A
  ) |>
  group_by(outcome_margin, outcome_label, period, period_start, period_end, period_years, two_by_two_cell, two_by_two_label) |>
  summarize(
    district_count = n_distinct(district_id),
    observed_units = sum(observed_units, na.rm = TRUE),
    exposure_occ_period = sum(exposure_occ_period, na.rm = TRUE),
    annualized_rate_per_10k_occ1990 = observed_units / exposure_occ_period,
    .groups = "drop"
  ) |>
  arrange(outcome_margin, period, two_by_two_cell)

write_csv_if_changed(counterfactual_panel, "../output/ccdist2010_opportunity_counterfactual_panel.csv")
write_csv_if_changed(counterfactual_summary, "../output/ccdist2010_opportunity_counterfactual_summary.csv")
write_csv_if_changed(model_summary, "../output/ccdist2010_opportunity_counterfactual_model_summary.csv")
write_csv_if_changed(main_treatment_coefficients, "../output/ccdist2010_opportunity_counterfactual_main_treatment_coefficients.csv")
write_csv_if_changed(two_by_two_df, "../output/ccdist2010_opportunity_counterfactual_two_by_two.csv")

cat("Wrote 2010 Council district opportunity-set counterfactual outputs to ../output\n")
