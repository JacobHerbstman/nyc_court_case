# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_zap_housing_buildout_after_certification/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(readr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

assert_unique_keys <- function(df, key_cols, df_name) {
  duplicate_keys <- df %>%
    count(across(all_of(key_cols)), name = "row_count") %>%
    filter(row_count > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(df_name, " is not unique by ", paste(key_cols, collapse = ", "), ".")
  }
}

safe_min_int <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) {
    return(NA_integer_)
  }
  as.integer(min(x))
}

mask_logical_outcome <- function(value, mature, linkable) {
  case_when(
    !mature ~ NA,
    !linkable ~ NA,
    TRUE ~ coalesce(value, FALSE)
  )
}

mask_numeric_outcome <- function(value, mature, linkable) {
  case_when(
    !mature ~ NA_real_,
    !linkable ~ NA_real_,
    TRUE ~ coalesce(value, 0)
  )
}

mask_lag_outcome <- function(value, mature, linkable) {
  case_when(
    !mature ~ NA_integer_,
    !linkable ~ NA_integer_,
    TRUE ~ value
  )
}

coef_row <- function(df, outcome_name, window_name) {
  model_df <- df %>%
    filter(window == window_name, outcome == outcome_name, !is.na(value), !is.na(treat_z_boro), !is.na(borough_name))

  if (nrow(model_df) < 20 || n_distinct(model_df$borough_name) < 2 || n_distinct(model_df$value) < 2) {
    return(tibble(window = window_name, outcome = outcome_name, estimate = NA_real_, std_error = NA_real_, p_value = NA_real_, projects = nrow(model_df)))
  }

  fit <- feols(value ~ treat_z_boro | borough_name + cert_year, data = model_df, vcov = "hetero")
  ct <- coeftable(fit)
  tibble(
    window = window_name,
    outcome = outcome_name,
    estimate = unname(ct["treat_z_boro", "Estimate"]),
    std_error = unname(ct["treat_z_boro", "Std. Error"]),
    p_value = unname(ct["treat_z_boro", "Pr(>|t|)"]),
    projects = nrow(model_df)
  )
}

project_summary <- read_csv("../input/zap_housing_hdb_project_summary.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    project_id = as.character(project_id),
    cert_year = as.integer(cert_year),
    borocd = as.integer(borocd),
    treat_z_boro = as.numeric(treat_z_boro),
    treat_pp = as.numeric(treat_pp),
    is_complete = as.logical(is_complete),
    is_fail = as.logical(is_fail),
    is_unresolved = as.logical(is_unresolved),
    has_bbl = as.logical(has_bbl),
    bbl_linkable = as.logical(bbl_linkable),
    bbl_count = as.integer(bbl_count),
    valid_bbl_count = as.integer(valid_bbl_count),
    invalid_bbl_row_count = as.integer(invalid_bbl_row_count),
    blank_bbl_row_count = as.integer(blank_bbl_row_count)
  ) %>%
  select(
    project_id,
    project_name,
    project_brief,
    borocd,
    borough_name,
    cert_year,
    cert_era,
    treat_pp,
    treat_z_boro,
    is_complete,
    is_fail,
    is_unresolved,
    has_bbl,
    bbl_count,
    bbl_linkable,
    valid_bbl_count,
    invalid_bbl_row_count,
    blank_bbl_row_count
  ) %>%
  mutate(
    valid_bbl_count = coalesce(valid_bbl_count, 0L),
    invalid_bbl_row_count = coalesce(invalid_bbl_row_count, 0L),
    blank_bbl_row_count = coalesce(blank_bbl_row_count, 0L),
    bbl_linkable = coalesce(bbl_linkable, valid_bbl_count > 0),
    has_bbl = coalesce(has_bbl, bbl_linkable),
    bbl_count = coalesce(bbl_count, valid_bbl_count)
  )

if (n_distinct(project_summary$borocd[!is.na(project_summary$borocd)]) != 59) {
  stop("Expected ZAP-HDB project summary to cover 59 CDs.")
}

assert_unique_keys(project_summary, "project_id", "ZAP-HDB project summary")

district_lookup <- project_summary %>%
  distinct(borocd, borough_name, treat_pp) %>%
  filter(!is.na(borocd)) %>%
  group_by(borough_name) %>%
  mutate(
    treat_tercile = ntile(treat_pp, 3),
    treat_tercile_label = case_when(
      treat_tercile == 1 ~ "Low",
      treat_tercile == 2 ~ "Middle",
      TRUE ~ "High"
    )
  ) %>%
  ungroup() %>%
  select(borocd, treat_tercile, treat_tercile_label)

candidates <- read_csv("../input/zap_housing_hdb_link_candidates.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    project_id = as.character(project_id),
    job_number = as.character(job_number),
    permit_lag = suppressWarnings(as.integer(permit_lag)),
    completion_lag = suppressWarnings(as.integer(completion_lag)),
    is_housing_active_job = as.logical(is_housing_active_job),
    is_nb_50_plus_job = as.logical(is_nb_50_plus_job),
    gross_add_units = as.numeric(gross_add_units),
    nb_gross_units = as.numeric(nb_gross_units),
    within_0_5 = as.logical(within_0_5),
    within_0_10 = as.logical(within_0_10),
    completion_0_5 = !is.na(completion_lag) & completion_lag >= 0 & completion_lag <= 5,
    completion_0_10 = !is.na(completion_lag) & completion_lag >= 0 & completion_lag <= 10
  ) %>%
  filter(!is.na(job_number))

assert_unique_keys(candidates, "job_number", "Assigned ZAP-HDB candidate links")

candidate_project <- candidates %>%
  group_by(project_id) %>%
  summarise(
    any_housing_permit_link_0_5 = any(is_housing_active_job %in% TRUE & within_0_5, na.rm = TRUE),
    any_housing_completion_link_0_5 = any(is_housing_active_job %in% TRUE & completion_0_5, na.rm = TRUE),
    any_nb_50plus_permit_link_0_5 = any(is_nb_50_plus_job %in% TRUE & within_0_5, na.rm = TRUE),
    any_nb_50plus_completion_link_0_5 = any(is_nb_50_plus_job %in% TRUE & completion_0_5, na.rm = TRUE),
    linked_gross_add_units_permitted_0_5 = sum(gross_add_units[is_housing_active_job %in% TRUE & within_0_5 %in% TRUE], na.rm = TRUE),
    linked_gross_add_units_completed_0_5 = sum(gross_add_units[is_housing_active_job %in% TRUE & completion_0_5 %in% TRUE], na.rm = TRUE),
    linked_nb_50plus_gross_units_permitted_0_5 = sum(nb_gross_units[is_nb_50_plus_job %in% TRUE & within_0_5 %in% TRUE], na.rm = TRUE),
    linked_nb_50plus_gross_units_completed_0_5 = sum(nb_gross_units[is_nb_50_plus_job %in% TRUE & completion_0_5 %in% TRUE], na.rm = TRUE),
    first_housing_permit_lag_0_5 = safe_min_int(permit_lag[is_housing_active_job %in% TRUE & within_0_5]),
    first_housing_completion_lag_0_5 = safe_min_int(completion_lag[is_housing_active_job %in% TRUE & completion_0_5]),
    any_housing_permit_link_0_10 = any(is_housing_active_job %in% TRUE & within_0_10, na.rm = TRUE),
    any_housing_completion_link_0_10 = any(is_housing_active_job %in% TRUE & completion_0_10, na.rm = TRUE),
    any_nb_50plus_permit_link_0_10 = any(is_nb_50_plus_job %in% TRUE & within_0_10, na.rm = TRUE),
    any_nb_50plus_completion_link_0_10 = any(is_nb_50_plus_job %in% TRUE & completion_0_10, na.rm = TRUE),
    linked_gross_add_units_permitted_0_10 = sum(gross_add_units[is_housing_active_job %in% TRUE & within_0_10 %in% TRUE], na.rm = TRUE),
    linked_gross_add_units_completed_0_10 = sum(gross_add_units[is_housing_active_job %in% TRUE & completion_0_10 %in% TRUE], na.rm = TRUE),
    linked_nb_50plus_gross_units_permitted_0_10 = sum(nb_gross_units[is_nb_50_plus_job %in% TRUE & within_0_10 %in% TRUE], na.rm = TRUE),
    linked_nb_50plus_gross_units_completed_0_10 = sum(nb_gross_units[is_nb_50_plus_job %in% TRUE & completion_0_10 %in% TRUE], na.rm = TRUE),
    first_housing_permit_lag_0_10 = safe_min_int(permit_lag[is_housing_active_job %in% TRUE & within_0_10]),
    first_housing_completion_lag_0_10 = safe_min_int(completion_lag[is_housing_active_job %in% TRUE & completion_0_10]),
    .groups = "drop"
  )

project_out <- project_summary %>%
  left_join(district_lookup, by = "borocd", relationship = "many-to-one") %>%
  left_join(candidate_project, by = "project_id", relationship = "one-to-one") %>%
  mutate(
    mature_0_5 = cert_year >= 2010 & cert_year <= 2020,
    mature_0_10 = cert_year >= 2010 & cert_year <= 2015,
    any_housing_permit_link_0_5 = mask_logical_outcome(any_housing_permit_link_0_5, mature_0_5, bbl_linkable),
    any_housing_completion_link_0_5 = mask_logical_outcome(any_housing_completion_link_0_5, mature_0_5, bbl_linkable),
    any_nb_50plus_permit_link_0_5 = mask_logical_outcome(any_nb_50plus_permit_link_0_5, mature_0_5, bbl_linkable),
    any_nb_50plus_completion_link_0_5 = mask_logical_outcome(any_nb_50plus_completion_link_0_5, mature_0_5, bbl_linkable),
    linked_gross_add_units_permitted_0_5 = mask_numeric_outcome(linked_gross_add_units_permitted_0_5, mature_0_5, bbl_linkable),
    linked_gross_add_units_completed_0_5 = mask_numeric_outcome(linked_gross_add_units_completed_0_5, mature_0_5, bbl_linkable),
    linked_nb_50plus_gross_units_permitted_0_5 = mask_numeric_outcome(linked_nb_50plus_gross_units_permitted_0_5, mature_0_5, bbl_linkable),
    linked_nb_50plus_gross_units_completed_0_5 = mask_numeric_outcome(linked_nb_50plus_gross_units_completed_0_5, mature_0_5, bbl_linkable),
    first_housing_permit_lag_0_5 = mask_lag_outcome(first_housing_permit_lag_0_5, mature_0_5, bbl_linkable),
    first_housing_completion_lag_0_5 = mask_lag_outcome(first_housing_completion_lag_0_5, mature_0_5, bbl_linkable),
    any_housing_permit_link_0_10 = mask_logical_outcome(any_housing_permit_link_0_10, mature_0_10, bbl_linkable),
    any_housing_completion_link_0_10 = mask_logical_outcome(any_housing_completion_link_0_10, mature_0_10, bbl_linkable),
    any_nb_50plus_permit_link_0_10 = mask_logical_outcome(any_nb_50plus_permit_link_0_10, mature_0_10, bbl_linkable),
    any_nb_50plus_completion_link_0_10 = mask_logical_outcome(any_nb_50plus_completion_link_0_10, mature_0_10, bbl_linkable),
    linked_gross_add_units_permitted_0_10 = mask_numeric_outcome(linked_gross_add_units_permitted_0_10, mature_0_10, bbl_linkable),
    linked_gross_add_units_completed_0_10 = mask_numeric_outcome(linked_gross_add_units_completed_0_10, mature_0_10, bbl_linkable),
    linked_nb_50plus_gross_units_permitted_0_10 = mask_numeric_outcome(linked_nb_50plus_gross_units_permitted_0_10, mature_0_10, bbl_linkable),
    linked_nb_50plus_gross_units_completed_0_10 = mask_numeric_outcome(linked_nb_50plus_gross_units_completed_0_10, mature_0_10, bbl_linkable),
    first_housing_permit_lag_0_10 = mask_lag_outcome(first_housing_permit_lag_0_10, mature_0_10, bbl_linkable),
    first_housing_completion_lag_0_10 = mask_lag_outcome(first_housing_completion_lag_0_10, mature_0_10, bbl_linkable),
    any_housing_job_0_5 = any_housing_permit_link_0_5,
    any_nb_50_plus_job_0_5 = any_nb_50plus_permit_link_0_5,
    linked_gross_add_units_0_5 = linked_gross_add_units_permitted_0_5,
    any_housing_job_0_10 = any_housing_permit_link_0_10,
    any_nb_50_plus_job_0_10 = any_nb_50plus_permit_link_0_10,
    linked_gross_add_units_0_10 = linked_gross_add_units_permitted_0_10,
    cert_period = case_when(
      cert_year >= 2010 & cert_year <= 2015 ~ "2010-2015",
      cert_year >= 2016 & cert_year <= 2020 ~ "2016-2020",
      TRUE ~ NA_character_
    )
  ) %>%
  arrange(cert_year, borocd, project_id)

project_long <- bind_rows(
  project_out %>%
    filter(mature_0_5) %>%
    transmute(
      project_id, borocd, borough_name, cert_year, cert_period, treat_z_boro, treat_tercile_label, bbl_linkable,
      window = "0-5 years",
      any_housing_permit_link = as.numeric(any_housing_permit_link_0_5),
      any_housing_completion_link = as.numeric(any_housing_completion_link_0_5),
      any_nb_50plus_permit_link = as.numeric(any_nb_50plus_permit_link_0_5),
      any_nb_50plus_completion_link = as.numeric(any_nb_50plus_completion_link_0_5),
      linked_gross_add_units_permitted_per_project = linked_gross_add_units_permitted_0_5,
      linked_gross_add_units_completed_per_project = linked_gross_add_units_completed_0_5,
      linked_nb_50plus_units_permitted_per_project = linked_nb_50plus_gross_units_permitted_0_5,
      linked_nb_50plus_units_completed_per_project = linked_nb_50plus_gross_units_completed_0_5,
      first_housing_permit_lag = first_housing_permit_lag_0_5,
      first_housing_completion_lag = first_housing_completion_lag_0_5
    ),
  project_out %>%
    filter(mature_0_10) %>%
    transmute(
      project_id, borocd, borough_name, cert_year, cert_period = "2010-2015", treat_z_boro, treat_tercile_label, bbl_linkable,
      window = "0-10 years",
      any_housing_permit_link = as.numeric(any_housing_permit_link_0_10),
      any_housing_completion_link = as.numeric(any_housing_completion_link_0_10),
      any_nb_50plus_permit_link = as.numeric(any_nb_50plus_permit_link_0_10),
      any_nb_50plus_completion_link = as.numeric(any_nb_50plus_completion_link_0_10),
      linked_gross_add_units_permitted_per_project = linked_gross_add_units_permitted_0_10,
      linked_gross_add_units_completed_per_project = linked_gross_add_units_completed_0_10,
      linked_nb_50plus_units_permitted_per_project = linked_nb_50plus_gross_units_permitted_0_10,
      linked_nb_50plus_units_completed_per_project = linked_nb_50plus_gross_units_completed_0_10,
      first_housing_permit_lag = first_housing_permit_lag_0_10,
      first_housing_completion_lag = first_housing_completion_lag_0_10
    )
) %>%
  pivot_longer(
    cols = c(
      any_housing_permit_link,
      any_housing_completion_link,
      any_nb_50plus_permit_link,
      any_nb_50plus_completion_link,
      linked_gross_add_units_permitted_per_project,
      linked_gross_add_units_completed_per_project,
      linked_nb_50plus_units_permitted_per_project,
      linked_nb_50plus_units_completed_per_project,
      first_housing_permit_lag,
      first_housing_completion_lag
    ),
    names_to = "outcome",
    values_to = "value"
  )

summary_df <- project_long %>%
  group_by(window, cert_period, treat_tercile_label, outcome) %>%
  summarise(
    total_projects = n_distinct(project_id),
    linkable_projects = n_distinct(project_id[!is.na(value)]),
    excluded_no_valid_bbl_projects = n_distinct(project_id[bbl_linkable %in% FALSE]),
    nonmissing_outcome_projects = sum(!is.na(value)),
    value = mean(value, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(window, cert_period, outcome, treat_tercile_label)

coefficients <- bind_rows(lapply(unique(project_long$window), function(window_name) {
  bind_rows(lapply(unique(project_long$outcome), function(outcome_name) {
    coef_row(project_long, outcome_name, window_name)
  }))
}))

plot_outcomes <- c(
  "any_housing_permit_link",
  "any_housing_completion_link",
  "any_nb_50plus_permit_link",
  "any_nb_50plus_completion_link",
  "linked_gross_add_units_completed_per_project",
  "linked_nb_50plus_units_completed_per_project"
)

plot_df <- summary_df %>%
  filter(outcome %in% plot_outcomes) %>%
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    outcome_label = recode(
      outcome,
      any_housing_permit_link = "Any housing\npermit",
      any_housing_completion_link = "Any housing\ncompletion",
      any_nb_50plus_permit_link = "Any 50+ NB\npermit",
      any_nb_50plus_completion_link = "Any 50+ NB\ncompletion",
      linked_gross_add_units_completed_per_project = "Completed gross\nunits/project",
      linked_nb_50plus_units_completed_per_project = "Completed 50+ NB\nunits/project"
    )
  )

plot_obj <- ggplot(plot_df, aes(x = treat_tercile_label, y = value, fill = treat_tercile_label)) +
  geom_col(width = 0.7, na.rm = TRUE) +
  facet_grid(outcome_label ~ window + cert_period, scales = "free_y", switch = "y") +
  scale_fill_manual(values = c("Low" = "#2166ac", "Middle" = "#8c8c8c", "High" = "#d6604d")) +
  labs(
    x = "1990 homeowner tercile",
    y = NULL,
    fill = NULL,
    title = "HDB links after ZAP certification",
    subtitle = "Project-level means among ZAP housing projects with valid BBLs; permit and completion windows are labeled separately"
  ) +
  theme_minimal(base_size = 10) +
  theme(
    legend.position = "none",
    strip.placement = "outside",
    strip.text.y.left = element_text(angle = 0, hjust = 1),
    plot.margin = margin(5.5, 5.5, 5.5, 5.5)
  )

temp_pdf <- tempfile(fileext = ".pdf")
pdf(temp_pdf, width = 12, height = 10.5)
print(plot_obj)
dev.off()
copy_if_changed(temp_pdf, "../output/zap_housing_buildout_plots.pdf")

write_csv_if_changed(project_out, "../output/zap_housing_buildout_project_audited.csv")
write_csv_if_changed(summary_df, "../output/zap_housing_buildout_cohort_summary.csv")
write_csv_if_changed(coefficients, "../output/zap_housing_buildout_delay_coefficients.csv")

cat("Wrote ZAP build-out outputs to ../output\n")
