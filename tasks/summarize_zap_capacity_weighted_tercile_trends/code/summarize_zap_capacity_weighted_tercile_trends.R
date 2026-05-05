# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_zap_capacity_weighted_tercile_trends/code")
# zap_housing_project_base_audited_csv <- "../input/zap_housing_project_base_audited.csv"
# zap_project_bbl_parquet <- "../input/zap_project_bbl.parquet"
# mappluto_current_parquet <- "../input/dcp_mappluto_current_25v4.parquet"
# cd_homeownership_1990_measure_csv <- "../input/cd_homeownership_1990_measure.csv"
# cd_redevelopment_potential_baseline_csv <- "../input/cd_redevelopment_potential_baseline.csv"
# zap_outcome_usability_csv <- "../input/zap_outcome_usability_by_period.csv"
# out_project_bbl_csv <- "../output/zap_capacity_weighted_project_bbl.csv"
# out_cd_year_csv <- "../output/zap_capacity_weighted_cd_year.csv"
# out_tercile_year_csv <- "../output/zap_capacity_weighted_tercile_year.csv"
# out_per_10000_pdf <- "../output/zap_capacity_weighted_tercile_trends_per_10000.pdf"
# out_per_residential_acre_pdf <- "../output/zap_capacity_weighted_tercile_trends_per_residential_acre.pdf"
# out_qc_csv <- "../output/zap_capacity_weighted_tercile_trends_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

cli_args <- commandArgs(trailingOnly = TRUE)
if (length(cli_args) == 0) {
  cli_args <- c(
    zap_housing_project_base_audited_csv,
    zap_project_bbl_parquet,
    mappluto_current_parquet,
    cd_homeownership_1990_measure_csv,
    cd_redevelopment_potential_baseline_csv,
    zap_outcome_usability_csv,
    out_project_bbl_csv,
    out_cd_year_csv,
    out_tercile_year_csv,
    out_per_10000_pdf,
    out_per_residential_acre_pdf,
    out_qc_csv
  )
}
stopifnot(length(cli_args) == 12)

zap_housing_project_base_audited_csv <- cli_args[1]
zap_project_bbl_parquet <- cli_args[2]
mappluto_current_parquet <- cli_args[3]
cd_homeownership_1990_measure_csv <- cli_args[4]
cd_redevelopment_potential_baseline_csv <- cli_args[5]
zap_outcome_usability_csv <- cli_args[6]
out_project_bbl_csv <- cli_args[7]
out_cd_year_csv <- cli_args[8]
out_tercile_year_csv <- cli_args[9]
out_per_10000_pdf <- cli_args[10]
out_per_residential_acre_pdf <- cli_args[11]
out_qc_csv <- cli_args[12]

qc_rows <- tibble(
  check_name = character(),
  status = character(),
  value = double(),
  detail = character()
)

add_qc <- function(check_name, condition, value = NA_real_, detail = "") {
  qc_rows <<- bind_rows(
    qc_rows,
    tibble(
      check_name = check_name,
      status = if_else(isTRUE(condition), "pass", "fail"),
      value = value,
      detail = detail
    )
  )
}

as_bool <- function(x) {
  if (is.logical(x)) {
    return(replace_na(x, FALSE))
  }
  if (is.numeric(x)) {
    return(replace_na(x != 0, FALSE))
  }
  str_to_lower(replace_na(as.character(x), "")) %in% c("true", "t", "1", "yes", "y")
}

period_from_year <- function(year) {
  case_when(
    year >= 1985 & year <= 1989 ~ "1985-1989",
    year >= 1990 & year <= 1999 ~ "1990-1999",
    year >= 2000 & year <= 2009 ~ "2000-2009",
    year >= 2010 & year <= 2019 ~ "2010-2019",
    year >= 2020 & year <= 2025 ~ "2020-2025",
    TRUE ~ NA_character_
  )
}

usability_rank <- function(x) {
  case_when(
    x == "usable" ~ 3L,
    x == "limited" ~ 2L,
    x == "not_recommended" ~ 1L,
    TRUE ~ 0L
  )
}

usability_from_rank <- function(x) {
  case_when(
    x >= 3L ~ "usable",
    x == 2L ~ "limited",
    TRUE ~ "not_recommended"
  )
}

combine_usability <- function(application_usability, action_usability, bbl_usability, capacity_usability, requires_action_split) {
  source_rank <- pmin(
    usability_rank(application_usability),
    if_else(requires_action_split, usability_rank(action_usability), 3L),
    usability_rank(bbl_usability),
    usability_rank(capacity_usability)
  )
  usability_from_rank(source_rank)
}

sum_or_na <- function(x) {
  if (all(is.na(x))) {
    return(NA_real_)
  }
  sum(x, na.rm = TRUE)
}

plot_outcomes <- tribble(
  ~outcome_name, ~outcome_label, ~outcome_order, ~requires_action_split,
  "all_ulurp_apps", "All ULURP applications", 1L, FALSE,
  "housing_any_candidate_apps", "Housing-oriented applications", 2L, FALSE,
  "housing_any_rezoning_special_apps", "Housing rezoning/special permit", 3L, TRUE,
  "housing_any_public_land_disposition_apps", "Housing public land/disposition", 4L, TRUE
)

capacity_metrics <- tribble(
  ~capacity_metric, ~capacity_label, ~capacity_order,
  "affected_bbl_count", "Affected BBL count", 1L,
  "affected_lot_acres", "Affected current MapPLUTO lot acres", 2L,
  "affected_current_residential_lot_acres", "Affected current residential lot acres", 3L
)

standard_cd <- read_csv(cd_homeownership_1990_measure_csv, show_col_types = FALSE) |>
  transmute(
    borocd = as.integer(borocd),
    borough_code = as.integer(borough_code),
    borough_name = borough_name,
    occupied_units_1990 = as.numeric(occupied_units_1990),
    treat_z_boro = as.numeric(treat_z_boro)
  )

add_qc(
  "standard_cd_count",
  n_distinct(standard_cd$borocd) == 59L,
  n_distinct(standard_cd$borocd),
  "Expected 59 community districts."
)
add_qc(
  "standard_cd_unique_borocd",
  nrow(standard_cd) == n_distinct(standard_cd$borocd),
  nrow(standard_cd) - n_distinct(standard_cd$borocd),
  "Homeownership denominator rows must be unique by borocd."
)

redevelopment_denoms <- read_csv(cd_redevelopment_potential_baseline_csv, show_col_types = FALSE) |>
  transmute(
    borocd = as.integer(borocd),
    residential_acres = as.numeric(residential_acres)
  )
add_qc(
  "redevelopment_denominator_unique_borocd",
  nrow(redevelopment_denoms) == n_distinct(redevelopment_denoms$borocd),
  nrow(redevelopment_denoms) - n_distinct(redevelopment_denoms$borocd),
  "Residential-acre denominator rows must be unique by borocd."
)

cd_denoms <- standard_cd |>
  left_join(redevelopment_denoms, by = "borocd", relationship = "one-to-one") |>
  group_by(borough_code) |>
  mutate(
    homeownership_tercile = ntile(treat_z_boro, 3L),
    homeownership_tercile = factor(
      homeownership_tercile,
      levels = 1:3,
      labels = c("Low", "Middle", "High")
    )
  ) |>
  ungroup()
add_qc(
  "nonmissing_denominators",
  all(!is.na(cd_denoms$treat_z_boro)) &&
    all(!is.na(cd_denoms$occupied_units_1990)) &&
    all(!is.na(cd_denoms$residential_acres)) &&
    all(cd_denoms$occupied_units_1990 > 0) &&
    all(cd_denoms$residential_acres > 0),
  sum(is.na(cd_denoms$treat_z_boro) | is.na(cd_denoms$occupied_units_1990) | is.na(cd_denoms$residential_acres)),
  "Treatment, 1990 occupied-unit denominators, and residential-acre denominators must be nonmissing."
)

source_usability <- read_csv(zap_outcome_usability_csv, show_col_types = FALSE) |>
  filter(period %in% c("1985-1989", "1990-1999", "2000-2009", "2010-2019", "2020-2025")) |>
  select(period, outcome_type, usability) |>
  pivot_wider(names_from = outcome_type, values_from = usability, names_prefix = "source_")

project_base <- read_csv(zap_housing_project_base_audited_csv, show_col_types = FALSE) |>
  mutate(
    cert_year = as.integer(cert_year),
    period = period_from_year(cert_year),
    borocd_primary = as.integer(borocd_primary),
    primary_standard_cd_flag = as_bool(primary_standard_cd_flag),
    across(all_of(plot_outcomes$outcome_name), as_bool)
  ) |>
  filter(cert_year >= 1985, cert_year <= 2025, !is.na(period))

add_qc(
  "project_base_unique_project_id",
  nrow(project_base) == n_distinct(project_base$project_id),
  nrow(project_base) - n_distinct(project_base$project_id),
  "Audited project base must be unique by project_id."
)
add_qc(
  "project_base_year_support",
  min(project_base$cert_year, na.rm = TRUE) == 1985 && max(project_base$cert_year, na.rm = TRUE) == 2025,
  max(project_base$cert_year, na.rm = TRUE) - min(project_base$cert_year, na.rm = TRUE),
  "Capacity plots are restricted to certification years 1985-2025."
)

zap_project_bbl <- read_parquet(zap_project_bbl_parquet) |>
  transmute(
    project_id = as.character(project_id),
    bbl_standardized = as.character(bbl_standardized)
  ) |>
  filter(!is.na(project_id), !is.na(bbl_standardized), bbl_standardized != "") |>
  distinct(project_id, bbl_standardized)
add_qc(
  "project_bbl_unique_project_bbl",
  nrow(zap_project_bbl) == n_distinct(paste(zap_project_bbl$project_id, zap_project_bbl$bbl_standardized, sep = "___")),
  nrow(zap_project_bbl) - n_distinct(paste(zap_project_bbl$project_id, zap_project_bbl$bbl_standardized, sep = "___")),
  "Project-BBL links must be unique after distincting exact duplicates."
)

mappluto_lot <- read_parquet(mappluto_current_parquet) |>
  transmute(
    bbl_standardized = as.character(bbl),
    mappluto_borocd = as.integer(cd),
    lotarea = as.numeric(lotarea),
    landuse = str_pad(as.character(landuse), width = 2, side = "left", pad = "0"),
    unitsres = as.numeric(unitsres),
    resarea = as.numeric(resarea)
  ) |>
  filter(!is.na(bbl_standardized), bbl_standardized != "") |>
  distinct(bbl_standardized, .keep_all = TRUE) |>
  mutate(
    lot_acres = pmax(replace_na(lotarea, 0), 0) / 43560,
    current_residential_lot_flag = landuse %in% c("01", "02", "03", "04") |
      replace_na(unitsres, 0) > 0 |
      replace_na(resarea, 0) > 0,
    current_residential_lot_acres = if_else(current_residential_lot_flag, lot_acres, 0),
    mappluto_standard_cd_flag = mappluto_borocd %in% standard_cd$borocd
  )
add_qc(
  "mappluto_unique_bbl",
  nrow(mappluto_lot) == n_distinct(mappluto_lot$bbl_standardized),
  nrow(mappluto_lot) - n_distinct(mappluto_lot$bbl_standardized),
  "MapPLUTO lot rows must be unique by BBL."
)

project_bbl <- project_base |>
  left_join(zap_project_bbl, by = "project_id", relationship = "one-to-many") |>
  left_join(mappluto_lot, by = "bbl_standardized", relationship = "many-to-one") |>
  mutate(
    bbl_matched_current_mappluto = !is.na(mappluto_borocd),
    bbl_standard_cd_flag = bbl_matched_current_mappluto & mappluto_standard_cd_flag,
    affected_bbl_count = if_else(bbl_standard_cd_flag, 1, 0),
    affected_lot_acres = if_else(bbl_standard_cd_flag, lot_acres, 0),
    affected_current_residential_lot_acres = if_else(bbl_standard_cd_flag, current_residential_lot_acres, 0)
  )

write_csv(project_bbl, out_project_bbl_csv, na = "")

project_capacity <- project_bbl |>
  group_by(project_id) |>
  summarise(
    linked_bbl_count = sum(!is.na(bbl_standardized)),
    matched_standard_bbl_count = sum(bbl_standard_cd_flag, na.rm = TRUE),
    has_standard_matched_bbl = any(bbl_standard_cd_flag, na.rm = TRUE),
    project_affected_bbl_count = sum(affected_bbl_count, na.rm = TRUE),
    project_affected_lot_acres = sum(affected_lot_acres, na.rm = TRUE),
    project_affected_current_residential_lot_acres = sum(affected_current_residential_lot_acres, na.rm = TRUE),
    .groups = "drop"
  )

project_outcome_long <- project_base |>
  select(
    project_id,
    cert_year,
    period,
    borocd_primary,
    primary_standard_cd_flag,
    all_of(plot_outcomes$outcome_name)
  ) |>
  pivot_longer(
    cols = all_of(plot_outcomes$outcome_name),
    names_to = "outcome_name",
    values_to = "outcome_included"
  ) |>
  filter(outcome_included) |>
  left_join(project_capacity, by = "project_id", relationship = "many-to-one") |>
  left_join(plot_outcomes, by = "outcome_name", relationship = "many-to-one")

capacity_support <- project_outcome_long |>
  group_by(period, outcome_name) |>
  summarise(
    source_project_count = n_distinct(project_id),
    matched_project_count = n_distinct(project_id[has_standard_matched_bbl]),
    bbl_match_share = if_else(source_project_count > 0, matched_project_count / source_project_count, NA_real_),
    .groups = "drop"
  ) |>
  complete(
    period = c("1985-1989", "1990-1999", "2000-2009", "2010-2019", "2020-2025"),
    outcome_name = plot_outcomes$outcome_name,
    fill = list(source_project_count = 0L, matched_project_count = 0L)
  ) |>
  mutate(
    bbl_match_share = if_else(source_project_count > 0, bbl_match_share, NA_real_),
    capacity_usability = case_when(
      source_project_count == 0L ~ "usable",
      bbl_match_share >= 0.80 ~ "usable",
      bbl_match_share >= 0.50 ~ "limited",
      TRUE ~ "not_recommended"
    )
  ) |>
  left_join(plot_outcomes, by = "outcome_name", relationship = "many-to-one") |>
  left_join(source_usability, by = "period", relationship = "many-to-one") |>
  mutate(
    analysis_usability = combine_usability(
      source_application_count,
      source_action_category_split,
      source_bbl_fractional_geography,
      capacity_usability,
      requires_action_split
    ),
    support_note = paste0(
      "source_projects=", source_project_count,
      "; matched_projects=", matched_project_count,
      "; bbl_match_share=", if_else(is.na(bbl_match_share), "NA", format(round(bbl_match_share, 3), nsmall = 3)),
      "; source_application_count=", source_application_count,
      "; source_action_category_split=", source_action_category_split,
      "; source_bbl_fractional_geography=", source_bbl_fractional_geography,
      "; capacity_usability=", capacity_usability
    )
  ) |>
  select(
    period,
    outcome_name,
    outcome_label,
    outcome_order,
    source_project_count,
    matched_project_count,
    bbl_match_share,
    capacity_usability,
    analysis_usability,
    support_note
  )

observed_primary_cd_year <- project_outcome_long |>
  filter(primary_standard_cd_flag, borocd_primary %in% standard_cd$borocd) |>
  group_by(
    assignment_type = "primary_zap_cd",
    borocd = borocd_primary,
    year = cert_year,
    period,
    outcome_name
  ) |>
  summarise(
    project_count_observed = n_distinct(project_id),
    affected_bbl_count_observed = sum(project_affected_bbl_count, na.rm = TRUE),
    affected_lot_acres_observed = sum(project_affected_lot_acres, na.rm = TRUE),
    affected_current_residential_lot_acres_observed = sum(project_affected_current_residential_lot_acres, na.rm = TRUE),
    .groups = "drop"
  )

observed_bbl_cd_year <- project_bbl |>
  filter(bbl_standard_cd_flag, mappluto_borocd %in% standard_cd$borocd) |>
  select(
    project_id,
    cert_year,
    period,
    borocd = mappluto_borocd,
    affected_bbl_count,
    affected_lot_acres,
    affected_current_residential_lot_acres,
    all_of(plot_outcomes$outcome_name)
  ) |>
  pivot_longer(
    cols = all_of(plot_outcomes$outcome_name),
    names_to = "outcome_name",
    values_to = "outcome_included"
  ) |>
  filter(as_bool(outcome_included)) |>
  group_by(
    assignment_type = "bbl_current_mappluto_cd",
    borocd,
    year = cert_year,
    period,
    outcome_name
  ) |>
  summarise(
    project_count_observed = n_distinct(project_id),
    affected_bbl_count_observed = sum(affected_bbl_count, na.rm = TRUE),
    affected_lot_acres_observed = sum(affected_lot_acres, na.rm = TRUE),
    affected_current_residential_lot_acres_observed = sum(affected_current_residential_lot_acres, na.rm = TRUE),
    .groups = "drop"
  )

cd_year_grid <- expand_grid(
  tibble(assignment_type = c("primary_zap_cd", "bbl_current_mappluto_cd")),
  cd_denoms,
  tibble(year = 1985:2025),
  plot_outcomes |> select(outcome_name, outcome_label, outcome_order, requires_action_split)
) |>
  mutate(period = period_from_year(year))

cd_year <- cd_year_grid |>
  left_join(
    bind_rows(observed_primary_cd_year, observed_bbl_cd_year),
    by = c("assignment_type", "borocd", "year", "period", "outcome_name"),
    relationship = "one-to-one"
  ) |>
  mutate(
    across(ends_with("_observed"), ~ replace_na(.x, 0)),
    project_count_observed = replace_na(project_count_observed, 0)
  ) |>
  left_join(capacity_support, by = c("period", "outcome_name", "outcome_label", "outcome_order"), relationship = "many-to-one") |>
  mutate(
    affected_bbl_count = if_else(analysis_usability == "not_recommended", NA_real_, affected_bbl_count_observed),
    affected_lot_acres = if_else(analysis_usability == "not_recommended", NA_real_, affected_lot_acres_observed),
    affected_current_residential_lot_acres = if_else(
      analysis_usability == "not_recommended",
      NA_real_,
      affected_current_residential_lot_acres_observed
    ),
    affected_bbl_count_per_10000 = 10000 * affected_bbl_count / occupied_units_1990,
    affected_lot_acres_per_10000 = 10000 * affected_lot_acres / occupied_units_1990,
    affected_current_residential_lot_acres_per_10000 = 10000 * affected_current_residential_lot_acres / occupied_units_1990,
    affected_bbl_count_per_residential_acre = affected_bbl_count / residential_acres,
    affected_lot_acres_per_residential_acre = affected_lot_acres / residential_acres,
    affected_current_residential_lot_acres_per_residential_acre =
      affected_current_residential_lot_acres / residential_acres,
    assignment_label = recode(
      assignment_type,
      primary_zap_cd = "Primary ZAP CD",
      bbl_current_mappluto_cd = "BBL/current MapPLUTO CD"
    )
  )

write_csv(cd_year, out_cd_year_csv, na = "")

tercile_year <- cd_year |>
  select(
    assignment_type,
    assignment_label,
    borocd,
    year,
    period,
    outcome_name,
    outcome_label,
    outcome_order,
    homeownership_tercile,
    occupied_units_1990,
    residential_acres,
    analysis_usability,
    source_project_count,
    matched_project_count,
    bbl_match_share,
    capacity_usability,
    support_note,
    affected_bbl_count,
    affected_lot_acres,
    affected_current_residential_lot_acres
  ) |>
  pivot_longer(
    cols = all_of(capacity_metrics$capacity_metric),
    names_to = "capacity_metric",
    values_to = "capacity_value"
  ) |>
  left_join(capacity_metrics, by = "capacity_metric", relationship = "many-to-one") |>
  group_by(
    assignment_type,
    assignment_label,
    year,
    period,
    outcome_name,
    outcome_label,
    outcome_order,
    capacity_metric,
    capacity_label,
    capacity_order,
    homeownership_tercile,
    analysis_usability,
    source_project_count,
    matched_project_count,
    bbl_match_share,
    capacity_usability,
    support_note
  ) |>
  summarise(
    cd_count = n_distinct(borocd),
    occupied_units_1990 = sum(occupied_units_1990, na.rm = TRUE),
    residential_acres = sum(residential_acres, na.rm = TRUE),
    capacity_value = sum_or_na(capacity_value),
    value_per_10000 = 10000 * capacity_value / occupied_units_1990,
    value_per_residential_acre = capacity_value / residential_acres,
    .groups = "drop"
  ) |>
  arrange(assignment_type, capacity_order, outcome_order, year, homeownership_tercile)

write_csv(tercile_year, out_tercile_year_csv, na = "")

make_capacity_plot <- function(data, capacity_metric_name, y_var, y_label) {
  data |>
    filter(capacity_metric == capacity_metric_name, analysis_usability != "not_recommended") |>
    mutate(
      outcome_label = factor(outcome_label, levels = plot_outcomes$outcome_label[order(plot_outcomes$outcome_order)]),
      assignment_label = factor(assignment_label, levels = c("Primary ZAP CD", "BBL/current MapPLUTO CD"))
    ) |>
    ggplot(aes(x = year, y = .data[[y_var]], color = homeownership_tercile, group = homeownership_tercile)) +
    geom_vline(xintercept = 1989, linetype = "dashed", linewidth = 0.35, color = "gray55") +
    geom_line(linewidth = 0.55, na.rm = TRUE) +
    facet_grid(outcome_label ~ assignment_label, scales = "free_y") +
    scale_color_manual(values = c(Low = "#2769D8", Middle = "#8A8A8A", High = "#D9482B")) +
    scale_x_continuous(breaks = seq(1985, 2025, by = 10), minor_breaks = seq(1985, 2025, by = 5)) +
    labs(
      title = "Capacity-weighted ZAP/ULURP trends by homeownership tercile",
      subtitle = paste0(
        capacity_metrics$capacity_label[capacity_metrics$capacity_metric == capacity_metric_name],
        ". Current MapPLUTO BBL capacity; unsupported cells omitted."
      ),
      x = NULL,
      y = y_label,
      color = "Within-borough homeownership tercile"
    ) +
    theme_minimal(base_size = 10) +
    theme(
      legend.position = "bottom",
      panel.grid.minor.y = element_blank(),
      strip.text = element_text(face = "bold"),
      plot.title = element_text(face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )
}

pdf(out_per_10000_pdf, width = 11, height = 8.5)
for (metric_name in capacity_metrics$capacity_metric) {
  print(make_capacity_plot(
    tercile_year,
    metric_name,
    "value_per_10000",
    "Capacity per 10,000 occupied units in 1990"
  ))
}
dev.off()

pdf(out_per_residential_acre_pdf, width = 11, height = 8.5)
for (metric_name in capacity_metrics$capacity_metric) {
  print(make_capacity_plot(
    tercile_year,
    metric_name,
    "value_per_residential_acre",
    "Capacity per baseline residential acre"
  ))
}
dev.off()

add_qc(
  "cd_year_unique_keys",
  nrow(cd_year) == n_distinct(paste(cd_year$assignment_type, cd_year$borocd, cd_year$year, cd_year$outcome_name, sep = "___")),
  nrow(cd_year) - n_distinct(paste(cd_year$assignment_type, cd_year$borocd, cd_year$year, cd_year$outcome_name, sep = "___")),
  "CD-year capacity panel must be unique by assignment, CD, year, and outcome."
)
add_qc(
  "tercile_year_expected_rows",
  nrow(tercile_year) == 2L * 4L * 3L * 41L * 3L,
  nrow(tercile_year),
  "Expected two geography assignments, four outcomes, three capacity metrics, 41 years, and three terciles."
)
add_qc(
  "plotted_years_restricted",
  min(tercile_year$year) == 1985 && max(tercile_year$year) == 2025,
  max(tercile_year$year) - min(tercile_year$year),
  "Plotted years must be restricted to 1985-2025."
)
add_qc(
  "three_terciles_per_year",
  all(tercile_year |>
    group_by(assignment_type, outcome_name, capacity_metric, year) |>
    summarise(n_terciles = n_distinct(homeownership_tercile), .groups = "drop") |>
    pull(n_terciles) == 3L),
  min(tercile_year |>
    group_by(assignment_type, outcome_name, capacity_metric, year) |>
    summarise(n_terciles = n_distinct(homeownership_tercile), .groups = "drop") |>
    pull(n_terciles)),
  "Every plotted assignment/outcome/capacity/year cell must have low, middle, and high terciles."
)
add_qc(
  "not_recommended_cells_masked",
  sum(cd_year$analysis_usability == "not_recommended" & (
    !is.na(cd_year$affected_bbl_count) |
      !is.na(cd_year$affected_lot_acres) |
      !is.na(cd_year$affected_current_residential_lot_acres)
  )) == 0L,
  sum(cd_year$analysis_usability == "not_recommended" & (
    !is.na(cd_year$affected_bbl_count) |
      !is.na(cd_year$affected_lot_acres) |
      !is.na(cd_year$affected_current_residential_lot_acres)
  )),
  "Unsupported cells must be masked, not filled as zero."
)
add_qc(
  "nonnegative_capacity_and_rates",
  all(cd_year$affected_bbl_count >= 0, na.rm = TRUE) &&
    all(cd_year$affected_lot_acres >= 0, na.rm = TRUE) &&
    all(cd_year$affected_current_residential_lot_acres >= 0, na.rm = TRUE) &&
    all(cd_year$affected_bbl_count_per_10000 >= 0, na.rm = TRUE) &&
    all(cd_year$affected_lot_acres_per_10000 >= 0, na.rm = TRUE) &&
    all(cd_year$affected_current_residential_lot_acres_per_10000 >= 0, na.rm = TRUE) &&
    all(cd_year$affected_bbl_count_per_residential_acre >= 0, na.rm = TRUE) &&
    all(cd_year$affected_lot_acres_per_residential_acre >= 0, na.rm = TRUE) &&
    all(cd_year$affected_current_residential_lot_acres_per_residential_acre >= 0, na.rm = TRUE),
  NA_real_,
  "Capacity counts, acres, and scaled rates must be nonnegative."
)
add_qc(
  "bbl_support_reported",
  all(!is.na(capacity_support$bbl_match_share[capacity_support$source_project_count > 0])),
  sum(is.na(capacity_support$bbl_match_share[capacity_support$source_project_count > 0])),
  "BBL-match support must be reported for nonempty period/outcome cells."
)
add_qc(
  "project_bbl_output_nonempty",
  file.exists(out_project_bbl_csv) && file.info(out_project_bbl_csv)$size > 0,
  file.info(out_project_bbl_csv)$size,
  "Project-BBL capacity output must be nonempty."
)
add_qc(
  "cd_year_output_nonempty",
  file.exists(out_cd_year_csv) && file.info(out_cd_year_csv)$size > 0,
  file.info(out_cd_year_csv)$size,
  "CD-year capacity output must be nonempty."
)
add_qc(
  "tercile_year_output_nonempty",
  file.exists(out_tercile_year_csv) && file.info(out_tercile_year_csv)$size > 0,
  file.info(out_tercile_year_csv)$size,
  "Tercile-year capacity output must be nonempty."
)
add_qc(
  "per_10000_pdf_nonempty",
  file.exists(out_per_10000_pdf) && file.info(out_per_10000_pdf)$size > 0,
  file.info(out_per_10000_pdf)$size,
  "Per-10,000 occupied-unit PDF must be nonempty."
)
add_qc(
  "per_residential_acre_pdf_nonempty",
  file.exists(out_per_residential_acre_pdf) && file.info(out_per_residential_acre_pdf)$size > 0,
  file.info(out_per_residential_acre_pdf)$size,
  "Per-residential-acre PDF must be nonempty."
)

write_csv(qc_rows, out_qc_csv, na = "")

if (any(qc_rows$status == "fail")) {
  failed_checks <- paste(qc_rows$check_name[qc_rows$status == "fail"], collapse = ", ")
  stop("Capacity-weighted ZAP tercile trend QC failed: ", failed_checks)
}
