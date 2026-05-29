# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/build_cd_homeownership_exact_decadal_validation/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tidyr)
})

treatment_df <- read_csv("../input/cd_homeownership_1990_measure.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
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

if (anyDuplicated(treatment_df$borocd)) {
  stop("Homeownership treatment file is not unique by borocd.")
}

dcp_profile_file <- read_csv("../input/dcp_cd_profiles_1990_2000_files.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(pull_date = as.character(pull_date)) |>
  filter(!is.na(parquet_path)) |>
  mutate(parquet_path = paste0("../../../stage_dcp_cd_profiles_1990_2000/output/", basename(parquet_path))) |>
  filter(file.exists(parquet_path)) |>
  arrange(desc(pull_date), parquet_path) |>
  slice_head(n = 1)

if (nrow(dcp_profile_file) == 0) {
  stop("Could not find a staged DCP CD profiles parquet in ../input/dcp_cd_profiles_1990_2000_files.csv")
}

exact_raw <- read_parquet(dcp_profile_file$parquet_path[[1]]) |>
  filter(
    profile_page_type == "housing",
    section_name == "year_structure_built",
    metric_label %in% c("1980 to 1989", "1990 to 1994", "1995 to 1998", "1999 to March 2000")
  ) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(district_id))),
    borough_name = borough_name,
    metric_label = metric_label,
    value_2000_number = suppressWarnings(as.numeric(value_2000_number))
  ) |>
  left_join(
    treatment_df |>
      select(borocd, borough_code),
    by = "borocd",
    relationship = "many-to-one"
  )

duplicate_exact_cells <- exact_raw |>
  count(borocd, metric_label, name = "source_row_count") |>
  filter(source_row_count > 1)

if (nrow(duplicate_exact_cells) > 0) {
  stop("Exact DCP year-structure-built cells are not unique by borocd and metric_label.")
}

exact_cd <- exact_raw |>
  group_by(borocd, borough_code, borough_name) |>
  summarize(
    units_built_1980s_exact = sum(value_2000_number[metric_label == "1980 to 1989"], na.rm = TRUE),
    units_built_1990s_exact = sum(value_2000_number[metric_label %in% c("1990 to 1994", "1995 to 1998", "1999 to March 2000")], na.rm = TRUE),
    .groups = "drop"
  ) |>
  pivot_longer(
    cols = c(units_built_1980s_exact, units_built_1990s_exact),
    names_to = "decade_source",
    values_to = "exact_units"
  ) |>
  mutate(
    decade = if_else(decade_source == "units_built_1980s_exact", "1980s", "1990s")
  ) |>
  select(-decade_source)

mappluto_cd_year <- read_csv("../input/mappluto_construction_proxy_cd_year.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    yearbuilt = suppressWarnings(as.integer(yearbuilt)),
    residential_units_proxy = suppressWarnings(as.numeric(residential_units_proxy))
  )

mappluto_2000_excluded_units <- mappluto_cd_year |>
  filter(yearbuilt == 2000) |>
  summarize(value = sum(residential_units_proxy, na.rm = TRUE)) |>
  pull(value)

proxy_cd <- mappluto_cd_year |>
  filter(yearbuilt >= 1980, yearbuilt <= 1999) |>
  mutate(
    decade = if_else(yearbuilt <= 1989, "1980s", "1990s")
  ) |>
  group_by(borocd, borough_code, borough_name, decade) |>
  summarize(proxy_units = sum(residential_units_proxy, na.rm = TRUE), .groups = "drop")

cd_validation_joined <- exact_cd |>
  full_join(proxy_cd, by = c("borocd", "borough_code", "borough_name", "decade"), relationship = "many-to-one")

missing_exact_before_fill_count <- sum(is.na(cd_validation_joined$exact_units))
missing_proxy_before_fill_count <- sum(is.na(cd_validation_joined$proxy_units))

cd_validation_df <- cd_validation_joined |>
  left_join(
    treatment_df |>
      select(borocd, borough_code, borough_name, treat_pp, treat_tercile, treat_tercile_label),
    by = c("borocd", "borough_code", "borough_name"),
    relationship = "many-to-one"
  ) |>
  mutate(
    exact_units = coalesce(exact_units, 0),
    proxy_units = coalesce(proxy_units, 0)
  ) |>
  arrange(decade, borough_code, borocd)

tercile_source_df <- bind_rows(
  cd_validation_df |>
    transmute(
      source = "Exact DCP 2000 structure-built counts",
      borocd,
      borough_code,
      borough_name,
      decade,
      treat_tercile,
      treat_tercile_label,
      outcome_value = exact_units
    ),
  cd_validation_df |>
    transmute(
      source = "MapPLUTO proxy",
      borocd,
      borough_code,
      borough_name,
      decade,
      treat_tercile,
      treat_tercile_label,
      outcome_value = proxy_units
    )
) |>
  group_by(source, decade, borough_code, borough_name, treat_tercile, treat_tercile_label) |>
  summarize(outcome_value = sum(outcome_value, na.rm = TRUE), .groups = "drop") |>
  group_by(source, decade, borough_code, borough_name) |>
  mutate(
    borough_total = sum(outcome_value, na.rm = TRUE),
    borough_share = if_else(borough_total > 0, outcome_value / borough_total, NA_real_)
  ) |>
  ungroup()

overall_tercile_df <- tercile_source_df |>
  group_by(source, decade, treat_tercile, treat_tercile_label) |>
  summarize(
    outcome_value = sum(outcome_value, na.rm = TRUE),
    borough_total = sum(distinct(data.frame(borough_code, borough_name, borough_total))$borough_total, na.rm = TRUE),
    borough_share = if_else(borough_total > 0, outcome_value / borough_total, NA_real_),
    geography_scope = "all_boroughs",
    .groups = "drop"
  ) |>
  select(geography_scope, everything())

write_csv(cd_validation_df, "../output/cd_homeownership_exact_decadal_validation_cd.csv", na = "")
write_csv(overall_tercile_df, "../output/cd_homeownership_exact_decadal_validation_tercile.csv", na = "")
write_csv(
  tercile_source_df |>
    filter(source == "Exact DCP 2000 structure-built counts") |>
    select(borough_code, borough_name, decade, treat_tercile, treat_tercile_label, outcome_value, borough_total, borough_share),
  "../output/cd_homeownership_exact_decadal_validation_borough.csv",
  na = ""
)

comparison_df <- bind_rows(
  cd_validation_df |>
    group_by(decade) |>
    summarize(
      metric = "cd_corr_exact_vs_proxy",
      value = cor(exact_units, proxy_units, use = "complete.obs"),
      .groups = "drop"
    ),
  cd_validation_df |>
    group_by(decade) |>
    summarize(
      metric = "cd_mae_exact_vs_proxy",
      value = mean(abs(exact_units - proxy_units), na.rm = TRUE),
      .groups = "drop"
    ),
  overall_tercile_df |>
    filter(source == "Exact DCP 2000 structure-built counts", treat_tercile_label == "High") |>
    transmute(decade, metric = "exact_high_tercile_share", value = borough_share),
  overall_tercile_df |>
    filter(source == "MapPLUTO proxy", treat_tercile_label == "High") |>
    transmute(decade, metric = "proxy_high_tercile_share", value = borough_share),
  tibble(
    decade = "1980s_to_1990s",
    metric = "exact_high_share_change_1990s_minus_1980s",
    value = overall_tercile_df$borough_share[
      overall_tercile_df$source == "Exact DCP 2000 structure-built counts" &
        overall_tercile_df$treat_tercile_label == "High" &
        overall_tercile_df$decade == "1990s"
    ] -
      overall_tercile_df$borough_share[
        overall_tercile_df$source == "Exact DCP 2000 structure-built counts" &
          overall_tercile_df$treat_tercile_label == "High" &
          overall_tercile_df$decade == "1980s"
      ]
  ),
  tibble(
    decade = "1980s_to_1990s",
    metric = "proxy_high_share_change_1990s_minus_1980s",
    value = overall_tercile_df$borough_share[
      overall_tercile_df$source == "MapPLUTO proxy" &
        overall_tercile_df$treat_tercile_label == "High" &
        overall_tercile_df$decade == "1990s"
    ] -
      overall_tercile_df$borough_share[
        overall_tercile_df$source == "MapPLUTO proxy" &
          overall_tercile_df$treat_tercile_label == "High" &
          overall_tercile_df$decade == "1980s"
      ]
  )
) |>
  arrange(decade, metric)

write_csv(comparison_df, "../output/cd_homeownership_exact_decadal_validation_comparison.csv", na = "")

qc_df <- bind_rows(
  tibble(metric = "district_count_exact_1980s", value = n_distinct(cd_validation_df$borocd[cd_validation_df$decade == "1980s" & !is.na(cd_validation_df$exact_units)]), note = "CDs in the exact 1980s decade table."),
  tibble(metric = "district_count_exact_1990s", value = n_distinct(cd_validation_df$borocd[cd_validation_df$decade == "1990s" & !is.na(cd_validation_df$exact_units)]), note = "CDs in the exact 1990s decade table."),
  tibble(metric = "exact_1980s_total_units", value = sum(cd_validation_df$exact_units[cd_validation_df$decade == "1980s"], na.rm = TRUE), note = "Total exact 1980s units from DCP year_structure_built."),
  tibble(metric = "exact_1990s_total_units", value = sum(cd_validation_df$exact_units[cd_validation_df$decade == "1990s"], na.rm = TRUE), note = "Total exact 1990s units from DCP year_structure_built."),
  tibble(metric = "exact_share_sum_check_min", value = min(overall_tercile_df |>
    group_by(source, decade) |>
    summarize(total_share = sum(borough_share, na.rm = TRUE), .groups = "drop") |>
    pull(total_share), na.rm = TRUE), note = "Minimum sum of tercile shares across source-decade cells; should equal 1."),
  tibble(metric = "exact_share_sum_check_max", value = max(overall_tercile_df |>
    group_by(source, decade) |>
    summarize(total_share = sum(borough_share, na.rm = TRUE), .groups = "drop") |>
    pull(total_share), na.rm = TRUE), note = "Maximum sum of tercile shares across source-decade cells; should equal 1."),
  tibble(metric = "missing_exact_before_fill_count", value = missing_exact_before_fill_count, note = "Rows missing exact DCP units before zero-fill after the exact/proxy join."),
  tibble(metric = "missing_proxy_before_fill_count", value = missing_proxy_before_fill_count, note = "Rows missing MapPLUTO proxy units before zero-fill after the exact/proxy join."),
  tibble(metric = "mappluto_year_2000_units_excluded", value = mappluto_2000_excluded_units, note = "MapPLUTO units with yearbuilt 2000 excluded because the exact 1990s DCP bucket ends at March 2000."),
  tibble(metric = "exact_high_share_change_1990s_minus_1980s", value = comparison_df$value[comparison_df$metric == "exact_high_share_change_1990s_minus_1980s"], note = "Negative values mean the high-homeownership tercile loses exact share from the 1980s to the 1990s.")
)

write_csv(qc_df, "../output/cd_homeownership_exact_decadal_validation_qc.csv", na = "")

plot_df <- overall_tercile_df |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    source = factor(source, levels = c("Exact DCP 2000 structure-built counts", "MapPLUTO proxy"))
  )

pdf("../output/cd_homeownership_exact_decadal_validation_plot.pdf", width = 10, height = 6.5)
print(
  ggplot(plot_df, aes(x = treat_tercile_label, y = borough_share, fill = source)) +
    geom_col(position = "dodge", width = 0.72) +
    facet_wrap(~decade, nrow = 1) +
    scale_fill_manual(values = c("Exact DCP 2000 structure-built counts" = "#1b6ca8", "MapPLUTO proxy" = "#d65f0e")) +
    labs(x = NULL, y = "Citywide share", fill = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
dev.off()

cat("Wrote exact decadal validation outputs to ../output\n")
