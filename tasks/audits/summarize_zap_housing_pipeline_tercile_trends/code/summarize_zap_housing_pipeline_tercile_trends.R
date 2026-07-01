suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

assert_unique_keys <- function(df, keys, label) {
  duplicate_keys <- df |>
    count(across(all_of(keys)), name = "n") |>
    filter(n > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(label, " is not unique by ", paste(keys, collapse = ", "), ".")
  }
}

plot_outcomes <- tribble(
  ~outcome_name, ~outcome_plot_label, ~plot_order,
  "all_ulurp_apps", "All ULURP", 1,
  "housing_any_candidate_apps", "Housing-oriented", 2,
  "housing_any_rezoning_special_apps", "Housing rezoning/special permit", 3,
  "housing_any_public_land_disposition_apps", "Housing public land/disposition", 4
)

assign_period <- function(year_value) {
  case_when(
    is.na(year_value) ~ "missing_year",
    year_value < 1976 ~ "pre_1976",
    year_value <= 1979 ~ "1976-1979",
    year_value <= 1984 ~ "1980-1984",
    year_value <= 1989 ~ "1985-1989",
    year_value <= 1999 ~ "1990-1999",
    year_value <= 2009 ~ "2000-2009",
    year_value <= 2019 ~ "2010-2019",
    year_value <= 2025 ~ "2020-2025",
    TRUE ~ "2026_plus"
  )
}

outcome_dictionary <- tribble(
  ~outcome_name, ~outcome_label, ~requires_action_split,
  "all_ulurp_apps", "All ULURP applications", FALSE,
  "housing_any_candidate_apps", "Housing-oriented ULURP applications", FALSE,
  "housing_strict_text_apps", "Strict-text housing applications", FALSE,
  "housing_broad_text_apps", "Broad-text housing applications", FALSE,
  "housing_mih_apps", "MIH-flagged housing applications", FALSE,
  "housing_action_code_apps", "Housing-action proxy applications", TRUE,
  "housing_any_private_apps", "Private housing-oriented applications", FALSE,
  "housing_any_public_apps", "Public housing-oriented applications", FALSE,
  "housing_any_rezoning_special_apps", "Housing-oriented rezoning/special-permit applications", TRUE,
  "housing_any_public_land_disposition_apps", "Housing-oriented public-land/disposition applications", TRUE,
  "housing_any_hpd_public_housing_apps", "Housing-oriented HPD/public-housing proxy applications", TRUE
)

standard_cd <- read_csv("../input/cd_homeownership_1990_measure.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = suppressWarnings(as.integer(borocd)),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name,
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro))
  ) |>
  arrange(borocd)

assert_unique_keys(standard_cd, "borocd", "CD homeownership measure")

cd_denoms <- standard_cd |>
  left_join(
    read_csv("../input/cd_redevelopment_potential_baseline.csv", show_col_types = FALSE, na = c("", "NA")) |>
      transmute(
        borocd = suppressWarnings(as.integer(borocd)),
        residential_acres = suppressWarnings(as.numeric(residential_acres))
      ),
    by = "borocd",
    relationship = "one-to-one"
  )

if (any(is.na(cd_denoms$occupied_units_1990)) || any(is.na(cd_denoms$residential_acres))) {
  stop("Missing occupied-unit or residential-acre denominators.")
}

outcome_usability <- read_csv("../input/zap_outcome_usability_by_period.csv", show_col_types = FALSE, na = c("", "NA")) |>
  select(period, outcome_type, usability) |>
  pivot_wider(names_from = outcome_type, values_from = usability, names_prefix = "usability_")

mappluto_cd <- read_parquet("../input/dcp_mappluto_current_25v4.parquet", col_select = c("bbl", "cd")) |>
  transmute(
    bbl_standardized = as.character(bbl),
    borocd = suppressWarnings(as.integer(cd))
  ) |>
  filter(!is.na(bbl_standardized), bbl_standardized != "") |>
  distinct(bbl_standardized, .keep_all = TRUE)

assert_unique_keys(mappluto_cd, "bbl_standardized", "Current MapPLUTO BBL-CD crosswalk")

zap_bbl <- read_parquet("../input/zap_project_bbl.parquet", col_select = c("project_id", "bbl_standardized")) |>
  transmute(
    project_id = as.character(project_id),
    bbl_standardized = as.character(bbl_standardized)
  ) |>
  filter(!is.na(project_id), project_id != "", !is.na(bbl_standardized), bbl_standardized != "") |>
  distinct(project_id, bbl_standardized)

assert_unique_keys(zap_bbl, c("project_id", "bbl_standardized"), "Staged ZAP BBL links")

project_base <- read_csv("../input/zap_housing_project_base_audited.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    project_id = as.character(project_id),
    cert_year = suppressWarnings(as.integer(cert_year)),
    borocd_primary = suppressWarnings(as.integer(borocd_primary)),
    primary_standard_cd_flag = str_to_lower(as.character(primary_standard_cd_flag)) %in% c("true", "t", "1", "yes"),
    across(all_of(outcome_dictionary$outcome_name), ~ str_to_lower(as.character(.x)) %in% c("true", "t", "1", "yes"))
  )

assert_unique_keys(project_base, "project_id", "Audited ZAP project base")

project_outcome_rows <- function(df, assignment_type) {
  df |>
    pivot_longer(
      cols = all_of(outcome_dictionary$outcome_name),
      names_to = "outcome_name",
      values_to = "outcome_included"
    ) |>
    filter(outcome_included) |>
    left_join(outcome_dictionary, by = "outcome_name", relationship = "many-to-one") |>
    mutate(assignment_type = assignment_type) |>
    select(
      assignment_type,
      project_id,
      outcome_name,
      outcome_label,
      requires_action_split,
      borocd,
      assignment_weight,
      cert_year,
      period,
      project_status,
      status_simple,
      primary_applicant,
      applicant_type,
      private_applicant_flag,
      public_applicant_flag,
      project_name,
      actions,
      ulurp_numbers
    )
}

primary_project_cd <- project_base |>
  filter(primary_standard_cd_flag) |>
  mutate(
    borocd = borocd_primary,
    assignment_weight = 1
  ) |>
  project_outcome_rows("primary_zap_cd")

bbl_cd_weights <- zap_bbl |>
  left_join(mappluto_cd, by = "bbl_standardized", relationship = "many-to-one") |>
  filter(borocd %in% standard_cd$borocd) |>
  count(project_id, borocd, name = "matched_bbl_count_in_cd") |>
  group_by(project_id) |>
  mutate(
    matched_bbl_count_total = sum(matched_bbl_count_in_cd),
    assignment_weight = matched_bbl_count_in_cd / matched_bbl_count_total
  ) |>
  ungroup()

assert_unique_keys(bbl_cd_weights, c("project_id", "borocd"), "BBL-CD weights")

bbl_project_cd <- bbl_cd_weights |>
  left_join(project_base, by = "project_id", relationship = "many-to-one") |>
  filter(!is.na(cert_year)) |>
  project_outcome_rows("bbl_fractional_current_mappluto")

make_cd_year_panel <- function(project_cd_df, assignment_type_value) {
  observed_counts <- project_cd_df |>
    group_by(borocd, year = cert_year, outcome_name) |>
    summarise(
      project_count_observed = sum(assignment_weight),
      distinct_project_count_observed = n_distinct(project_id),
      .groups = "drop"
    )

  expand_grid(
    cd_denoms,
    year = 1976:2025,
    outcome_dictionary
  ) |>
    mutate(
      period = assign_period(year),
      assignment_type = assignment_type_value
    ) |>
    left_join(outcome_usability, by = "period", relationship = "many-to-one") |>
    left_join(observed_counts, by = c("borocd", "year", "outcome_name"), relationship = "one-to-one") |>
    mutate(
      project_count_observed = coalesce(project_count_observed, 0),
      distinct_project_count_observed = coalesce(distinct_project_count_observed, 0L),
      support_problem = case_when(
        usability_application_count == "not_recommended" ~ "not_recommended_application_count",
        requires_action_split & usability_action_category_split == "not_recommended" ~ "not_recommended_action_category_split",
        assignment_type == "bbl_fractional_current_mappluto" & usability_bbl_fractional_geography == "not_recommended" ~ "not_recommended_bbl_fractional_geography",
        TRUE ~ NA_character_
      ),
      analysis_usability = case_when(
        !is.na(support_problem) ~ "not_recommended",
        usability_application_count == "limited" ~ "limited",
        requires_action_split & usability_action_category_split == "limited" ~ "limited",
        assignment_type == "bbl_fractional_current_mappluto" & usability_bbl_fractional_geography == "limited" ~ "limited",
        TRUE ~ "usable"
      ),
      project_count = if_else(analysis_usability == "not_recommended", NA_real_, project_count_observed),
      rate_per_10000_occupied_units_1990 = 10000 * project_count / occupied_units_1990,
      rate_per_residential_acre = project_count / residential_acres
    ) |>
    arrange(outcome_name, year, borocd)
}

primary_df <- make_cd_year_panel(primary_project_cd, "primary_zap_cd")
bbl_df <- make_cd_year_panel(bbl_project_cd, "bbl_fractional_current_mappluto")

panel_df <- bind_rows(primary_df, bbl_df) |>
  filter(outcome_name %in% plot_outcomes$outcome_name) |>
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    borough_code = suppressWarnings(as.integer(borough_code)),
    year = suppressWarnings(as.integer(year)),
    project_count_observed = suppressWarnings(as.numeric(project_count_observed)),
    project_count = suppressWarnings(as.numeric(project_count)),
    rate_per_10000_occupied_units_1990 = suppressWarnings(as.numeric(rate_per_10000_occupied_units_1990)),
    rate_per_residential_acre = suppressWarnings(as.numeric(rate_per_residential_acre)),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990)),
    residential_acres = suppressWarnings(as.numeric(residential_acres)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro))
  ) |>
  left_join(plot_outcomes, by = "outcome_name", relationship = "many-to-one")

required_cols <- c(
  "assignment_type",
  "borocd",
  "borough_code",
  "borough_name",
  "year",
  "outcome_name",
  "analysis_usability",
  "project_count",
  "rate_per_10000_occupied_units_1990",
  "rate_per_residential_acre",
  "treat_z_boro"
)

missing_cols <- setdiff(required_cols, names(panel_df))
if (length(missing_cols) > 0) {
  stop("Missing required panel columns: ", paste(missing_cols, collapse = ", "))
}

assert_unique_keys(
  panel_df,
  c("assignment_type", "borocd", "year", "outcome_name"),
  "ZAP housing CD-year panel"
)

district_lookup <- panel_df |>
  filter(assignment_type == "primary_zap_cd") |>
  distinct(borocd, borough_code, borough_name, treat_z_boro) |>
  group_by(borough_code, borough_name) |>
  mutate(
    treat_tercile = ntile(treat_z_boro, 3),
    treat_tercile_label = case_when(
      treat_tercile == 1 ~ "Low",
      treat_tercile == 2 ~ "Middle",
      TRUE ~ "High"
    )
  ) |>
  ungroup()

assert_unique_keys(district_lookup, "borocd", "ZAP tercile district lookup")

if (n_distinct(district_lookup$borocd) != 59) {
  stop("Expected 59 CDs in the ZAP tercile district lookup.")
}

tercile_year_df <- panel_df |>
  select(-treat_z_boro) |>
  left_join(
    district_lookup |>
      select(borocd, treat_tercile, treat_tercile_label),
    by = "borocd",
    relationship = "many-to-one"
  ) |>
  mutate(
    plotted_count = if_else(analysis_usability == "not_recommended", NA_real_, project_count),
    plotted_rate_per_10000 = if_else(analysis_usability == "not_recommended", NA_real_, rate_per_10000_occupied_units_1990),
    plotted_rate_per_residential_acre = if_else(analysis_usability == "not_recommended", NA_real_, rate_per_residential_acre),
    support_label = case_when(
      analysis_usability == "usable" ~ "Usable",
      analysis_usability == "limited" ~ "Limited support",
      TRUE ~ "Not recommended"
    ),
    assignment_label = case_when(
      assignment_type == "primary_zap_cd" ~ "Primary ZAP CD",
      assignment_type == "bbl_fractional_current_mappluto" ~ "BBL fractional",
      TRUE ~ assignment_type
    )
  ) |>
  group_by(
    assignment_type,
    assignment_label,
    outcome_name,
    outcome_plot_label,
    plot_order,
    year,
    period,
    treat_tercile,
    treat_tercile_label,
    analysis_usability,
    support_label
  ) |>
  summarise(
    project_count_observed = sum(project_count_observed, na.rm = TRUE),
    project_count = if_else(all(is.na(plotted_count)), NA_real_, sum(plotted_count, na.rm = TRUE)),
    rate_per_10000_occupied_units_1990 = if_else(all(is.na(plotted_rate_per_10000)), NA_real_, sum(plotted_rate_per_10000, na.rm = TRUE)),
    rate_per_residential_acre = if_else(all(is.na(plotted_rate_per_residential_acre)), NA_real_, sum(plotted_rate_per_residential_acre, na.rm = TRUE)),
    cd_count = n_distinct(borocd),
    .groups = "drop"
  ) |>
  arrange(assignment_type, outcome_name, year, treat_tercile)

expected_key_count <- 2 * nrow(plot_outcomes) * 50 * 3
if (nrow(tercile_year_df) != expected_key_count) {
  stop("Unexpected tercile-year row count.")
}

make_plot <- function(df, y_var, y_label, out_pdf) {
  plot_df <- df |>
    mutate(
      outcome_plot_label = factor(outcome_plot_label, levels = plot_outcomes$outcome_plot_label[order(plot_outcomes$plot_order)]),
      treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
      assignment_label = factor(assignment_label, levels = c("Primary ZAP CD", "BBL fractional"))
    )

  p <- ggplot(
    plot_df,
    aes(x = year, y = .data[[y_var]], color = treat_tercile_label, group = treat_tercile_label)
  ) +
    geom_hline(yintercept = 0, linewidth = 0.25, color = "grey75") +
    geom_line(linewidth = 0.45, na.rm = TRUE) +
    geom_point(size = 0.75, na.rm = TRUE) +
    geom_vline(xintercept = 1989, linetype = "dashed", linewidth = 0.25, color = "grey50") +
    facet_grid(outcome_plot_label ~ assignment_label, scales = "free_y") +
    scale_color_manual(values = c("Low" = "#2f6fdd", "Middle" = "#8f8f8f", "High" = "#d84a2b")) +
    scale_x_continuous(breaks = c(1976, 1985, 1990, 2000, 2010, 2020, 2025)) +
    labs(
      x = NULL,
      y = y_label,
      color = "1990 homeownership tercile",
      title = "Raw ZAP/ULURP pipeline trends by homeownership tercile",
      subtitle = "Unsupported BBL-fractional periods are omitted rather than plotted as zeros"
    ) +
    theme_minimal(base_size = 10) +
    theme(
      legend.position = "bottom",
      panel.grid.minor = element_blank(),
      strip.text = element_text(face = "bold", size = 8),
      plot.title = element_text(face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )

  ggsave(out_pdf, p, width = 11, height = 8.5)
}

make_plot(
  tercile_year_df,
  "rate_per_10000_occupied_units_1990",
  "Applications per 10,000 occupied 1990 units",
  "../output/zap_housing_pipeline_tercile_trends_per_10000.pdf"
)

make_plot(
  tercile_year_df,
  "rate_per_residential_acre",
  "Applications per residential acre",
  "../output/zap_housing_pipeline_tercile_trends_per_residential_acre.pdf"
)

write_csv(tercile_year_df, "../output/zap_housing_pipeline_tercile_year.csv", na = "")
