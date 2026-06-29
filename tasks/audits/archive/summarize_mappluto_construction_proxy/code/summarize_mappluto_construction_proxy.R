# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_mappluto_construction_proxy/code")
# proxy_panel_csv <- "../input/mappluto_construction_proxy_cd_year.csv"
# treatment_csv <- "../input/cd_homeownership_1990_measure.csv"
# hdb_parquet <- "../input/dcp_housing_database_project_level_25q4.parquet"
# dcp_profiles_parquet <- "../input/dcp_cd_profiles_1990_2000_20260415.parquet"
# baseline_controls_csv <- "../input/cd_baseline_1990_controls.csv"
# out_city_csv <- "../output/mappluto_construction_proxy_city_year.csv"
# out_borough_csv <- "../output/mappluto_construction_proxy_borough_year.csv"
# out_hdb_city_csv <- "../output/mappluto_construction_proxy_vs_hdb_city_year.csv"
# out_hdb_borough_csv <- "../output/mappluto_construction_proxy_vs_hdb_borough_year.csv"
# out_decadal_csv <- "../output/mappluto_construction_proxy_vs_decadal.csv"
# out_summary_csv <- "../output/mappluto_construction_proxy_validation_summary.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tidyr)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 11) {
  stop("Expected 11 arguments: proxy_panel_csv treatment_csv hdb_parquet dcp_profiles_parquet baseline_controls_csv out_city_csv out_borough_csv out_hdb_city_csv out_hdb_borough_csv out_decadal_csv out_summary_csv")
}

proxy_panel_csv <- args[1]
treatment_csv <- args[2]
hdb_parquet <- args[3]
dcp_profiles_parquet <- args[4]
baseline_controls_csv <- args[5]
out_city_csv <- args[6]
out_borough_csv <- args[7]
out_hdb_city_csv <- args[8]
out_hdb_borough_csv <- args[9]
out_decadal_csv <- args[10]
out_summary_csv <- args[11]

standard_cd <- read_csv(treatment_csv, show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name
  ) |>
  distinct()

proxy_panel <- read_csv(proxy_panel_csv, show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    yearbuilt = suppressWarnings(as.integer(yearbuilt))
  )

proxy_city <- proxy_panel |>
  group_by(yearbuilt) |>
  summarize(
    residential_lot_count_proxy = sum(residential_lot_count_proxy, na.rm = TRUE),
    residential_units_proxy = sum(residential_units_proxy, na.rm = TRUE),
    units_5_plus_proxy = sum(units_5_plus_proxy, na.rm = TRUE),
    units_50_plus_proxy = sum(units_50_plus_proxy, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(yearbuilt)

proxy_borough <- proxy_panel |>
  group_by(borough_code, borough_name, yearbuilt) |>
  summarize(
    residential_lot_count_proxy = sum(residential_lot_count_proxy, na.rm = TRUE),
    residential_units_proxy = sum(residential_units_proxy, na.rm = TRUE),
    units_5_plus_proxy = sum(units_5_plus_proxy, na.rm = TRUE),
    units_50_plus_proxy = sum(units_50_plus_proxy, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(borough_code, yearbuilt)

write_csv(proxy_city, out_city_csv, na = "")
write_csv(proxy_borough, out_borough_csv, na = "")

hdb_nb <- read_parquet(
  hdb_parquet,
  col_select = c("community_district", "borough_code", "borough_name", "job_type", "completion_year", "classa_prop")
) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(community_district))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = as.character(borough_name),
    job_type = as.character(job_type),
    completion_year = suppressWarnings(as.integer(completion_year)),
    classa_prop = suppressWarnings(as.numeric(classa_prop))
  ) |>
  filter(
    job_type == "New Building",
    !is.na(completion_year),
    completion_year >= 2010,
    completion_year <= 2025,
    classa_prop > 0
  ) |>
  semi_join(standard_cd, by = "borocd")

hdb_city <- hdb_nb |>
  group_by(yearbuilt = completion_year) |>
  summarize(
    hdb_nb_units_completion = sum(classa_prop, na.rm = TRUE),
    hdb_nb_projects_completion = n(),
    hdb_nb_50_plus_units_completion = sum(classa_prop[classa_prop >= 50], na.rm = TRUE),
    hdb_nb_50_plus_projects_completion = sum(classa_prop >= 50, na.rm = TRUE),
    .groups = "drop"
  )

hdb_borough <- hdb_nb |>
  group_by(borough_code, borough_name, yearbuilt = completion_year) |>
  summarize(
    hdb_nb_units_completion = sum(classa_prop, na.rm = TRUE),
    hdb_nb_projects_completion = n(),
    hdb_nb_50_plus_units_completion = sum(classa_prop[classa_prop >= 50], na.rm = TRUE),
    hdb_nb_50_plus_projects_completion = sum(classa_prop >= 50, na.rm = TRUE),
    .groups = "drop"
  )

hdb_city_compare <- proxy_city |>
  full_join(hdb_city, by = "yearbuilt") |>
  mutate(
    residential_lot_count_proxy = coalesce(residential_lot_count_proxy, 0),
    residential_units_proxy = coalesce(residential_units_proxy, 0),
    units_5_plus_proxy = coalesce(units_5_plus_proxy, 0),
    units_50_plus_proxy = coalesce(units_50_plus_proxy, 0),
    hdb_nb_units_completion = coalesce(hdb_nb_units_completion, 0),
    hdb_nb_projects_completion = coalesce(hdb_nb_projects_completion, 0),
    hdb_nb_50_plus_units_completion = coalesce(hdb_nb_50_plus_units_completion, 0),
    hdb_nb_50_plus_projects_completion = coalesce(hdb_nb_50_plus_projects_completion, 0)
  ) |>
  arrange(yearbuilt)

hdb_borough_compare <- expand_grid(
  standard_cd |> distinct(borough_code, borough_name),
  yearbuilt = 2010:2025
) |>
  left_join(proxy_borough, by = c("borough_code", "borough_name", "yearbuilt")) |>
  left_join(hdb_borough, by = c("borough_code", "borough_name", "yearbuilt")) |>
  mutate(
    residential_lot_count_proxy = coalesce(residential_lot_count_proxy, 0),
    residential_units_proxy = coalesce(residential_units_proxy, 0),
    units_5_plus_proxy = coalesce(units_5_plus_proxy, 0),
    units_50_plus_proxy = coalesce(units_50_plus_proxy, 0),
    hdb_nb_units_completion = coalesce(hdb_nb_units_completion, 0),
    hdb_nb_projects_completion = coalesce(hdb_nb_projects_completion, 0),
    hdb_nb_50_plus_units_completion = coalesce(hdb_nb_50_plus_units_completion, 0),
    hdb_nb_50_plus_projects_completion = coalesce(hdb_nb_50_plus_projects_completion, 0)
  ) |>
  group_by(yearbuilt) |>
  mutate(
    proxy_city_share_units = if (sum(residential_units_proxy, na.rm = TRUE) > 0) residential_units_proxy / sum(residential_units_proxy, na.rm = TRUE) else NA_real_,
    hdb_city_share_units = if (sum(hdb_nb_units_completion, na.rm = TRUE) > 0) hdb_nb_units_completion / sum(hdb_nb_units_completion, na.rm = TRUE) else NA_real_,
    proxy_city_share_50_plus_units = if (sum(units_50_plus_proxy, na.rm = TRUE) > 0) units_50_plus_proxy / sum(units_50_plus_proxy, na.rm = TRUE) else NA_real_,
    hdb_city_share_50_plus_units = if (sum(hdb_nb_50_plus_units_completion, na.rm = TRUE) > 0) hdb_nb_50_plus_units_completion / sum(hdb_nb_50_plus_units_completion, na.rm = TRUE) else NA_real_
  ) |>
  ungroup() |>
  arrange(borough_code, yearbuilt)

write_csv(hdb_city_compare, out_hdb_city_csv, na = "")
write_csv(hdb_borough_compare, out_hdb_borough_csv, na = "")

exact_1990_2000_growth <- read_parquet(dcp_profiles_parquet) |>
  filter(
    profile_page_type == "housing",
    section_name == "housing_occupancy",
    metric_label == "Total housing units"
  ) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(district_id))),
    borough_name = borough_name,
    comparator_units = suppressWarnings(as.numeric(value_2000_number)) - suppressWarnings(as.numeric(value_1990_number)),
    decade = "1990s",
    comparator_source = "dcp_profiles_exact_1990_2000_stock_growth"
  )

exact_structure_built_2000 <- read_parquet(dcp_profiles_parquet) |>
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
  group_by(borocd, borough_name) |>
  summarize(
    comparator_units_1980s = sum(value_2000_number[metric_label == "1980 to 1989"], na.rm = TRUE),
    comparator_units_1990s = sum(value_2000_number[metric_label %in% c("1990 to 1994", "1995 to 1998", "1999 to March 2000")], na.rm = TRUE),
    .groups = "drop"
  )

approx_1980_1990_growth <- read_csv(baseline_controls_csv, show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_name = borough_name,
    comparator_units = suppressWarnings(as.numeric(total_housing_units_growth_1980_1990_approx)),
    decade = "1980s",
    comparator_source = "nhgis_overlay_approx_1980_1990_stock_growth"
  )

proxy_decadal_cd <- bind_rows(
  proxy_panel |>
    filter(yearbuilt >= 1980, yearbuilt <= 1989) |>
    group_by(borocd, borough_name) |>
    summarize(proxy_units = sum(residential_units_proxy, na.rm = TRUE), decade = "1980s", .groups = "drop"),
  proxy_panel |>
    filter(yearbuilt >= 1990, yearbuilt <= 1999) |>
    group_by(borocd, borough_name) |>
    summarize(proxy_units = sum(residential_units_proxy, na.rm = TRUE), decade = "1990s", .groups = "drop")
) |>
  distinct()

decadal_cd <- proxy_decadal_cd |>
  left_join(bind_rows(approx_1980_1990_growth, exact_1990_2000_growth), by = c("borocd", "borough_name", "decade")) |>
  mutate(
    geography_level = "cd",
    geography_id = borocd
  ) |>
  select(geography_level, geography_id, borocd, borough_name, decade, proxy_units, comparator_units, comparator_source)

decadal_cd_structure_built <- bind_rows(
  proxy_decadal_cd |>
    filter(decade == "1980s") |>
    left_join(
      exact_structure_built_2000 |>
        transmute(
          borocd,
          borough_name,
          decade = "1980s",
          comparator_units = comparator_units_1980s,
          comparator_source = "dcp_profiles_exact_2000_structure_built"
        ),
      by = c("borocd", "borough_name", "decade")
    ),
  proxy_decadal_cd |>
    filter(decade == "1990s") |>
    left_join(
      exact_structure_built_2000 |>
        transmute(
          borocd,
          borough_name,
          decade = "1990s",
          comparator_units = comparator_units_1990s,
          comparator_source = "dcp_profiles_exact_2000_structure_built"
        ),
      by = c("borocd", "borough_name", "decade")
    )
) |>
  mutate(
    geography_level = "cd",
    geography_id = borocd
  ) |>
  select(geography_level, geography_id, borocd, borough_name, decade, proxy_units, comparator_units, comparator_source)

decadal_borough <- bind_rows(decadal_cd, decadal_cd_structure_built) |>
  group_by(borough_name, decade, comparator_source) |>
  summarize(
    proxy_units = sum(proxy_units, na.rm = TRUE),
    comparator_units = sum(comparator_units, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    geography_level = "borough",
    geography_id = borough_name,
    borocd = NA_character_
  ) |>
  select(geography_level, geography_id, borocd, borough_name, decade, proxy_units, comparator_units, comparator_source)

decadal_city <- bind_rows(decadal_cd, decadal_cd_structure_built) |>
  group_by(decade, comparator_source) |>
  summarize(
    proxy_units = sum(proxy_units, na.rm = TRUE),
    comparator_units = sum(comparator_units, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    geography_level = "city",
    geography_id = "NYC",
    borocd = NA_character_,
    borough_name = "NYC"
  ) |>
  select(geography_level, geography_id, borocd, borough_name, decade, proxy_units, comparator_units, comparator_source)

decadal_compare <- bind_rows(decadal_cd, decadal_cd_structure_built, decadal_borough, decadal_city) |>
  arrange(decade, geography_level, geography_id)

write_csv(decadal_compare, out_decadal_csv, na = "")

city_year_corr_units <- cor(
  hdb_city_compare$residential_units_proxy[hdb_city_compare$yearbuilt >= 2010 & hdb_city_compare$yearbuilt <= 2025],
  hdb_city_compare$hdb_nb_units_completion[hdb_city_compare$yearbuilt >= 2010 & hdb_city_compare$yearbuilt <= 2025],
  use = "complete.obs"
)

city_year_corr_50_plus_units <- cor(
  hdb_city_compare$units_50_plus_proxy[hdb_city_compare$yearbuilt >= 2010 & hdb_city_compare$yearbuilt <= 2025],
  hdb_city_compare$hdb_nb_50_plus_units_completion[hdb_city_compare$yearbuilt >= 2010 & hdb_city_compare$yearbuilt <= 2025],
  use = "complete.obs"
)

borough_year_corr_units <- cor(
  hdb_borough_compare$residential_units_proxy,
  hdb_borough_compare$hdb_nb_units_completion,
  use = "complete.obs"
)

borough_year_corr_unit_shares <- cor(
  hdb_borough_compare$proxy_city_share_units,
  hdb_borough_compare$hdb_city_share_units,
  use = "complete.obs"
)

borough_year_corr_50_plus_unit_shares <- cor(
  hdb_borough_compare$proxy_city_share_50_plus_units,
  hdb_borough_compare$hdb_city_share_50_plus_units,
  use = "complete.obs"
)

cd_corr_1990s <- decadal_cd |>
  filter(decade == "1990s", !is.na(comparator_units)) |>
  summarize(value = cor(proxy_units, comparator_units, use = "complete.obs")) |>
  pull(value)

cd_corr_1980s <- decadal_cd |>
  filter(decade == "1980s", !is.na(comparator_units)) |>
  summarize(value = cor(proxy_units, comparator_units, use = "complete.obs")) |>
  pull(value)

cd_corr_1990s_structure_built <- decadal_cd_structure_built |>
  filter(decade == "1990s", !is.na(comparator_units)) |>
  summarize(value = cor(proxy_units, comparator_units, use = "complete.obs")) |>
  pull(value)

cd_corr_1980s_structure_built <- decadal_cd_structure_built |>
  filter(decade == "1980s", !is.na(comparator_units)) |>
  summarize(value = cor(proxy_units, comparator_units, use = "complete.obs")) |>
  pull(value)

write_csv(
  bind_rows(
    tibble(metric = "proxy_city_total_units_1980_2025", value = as.character(sum(proxy_city$residential_units_proxy, na.rm = TRUE)), note = "Current surviving residential units on proxy lots built between 1980 and 2025."),
    tibble(metric = "proxy_city_total_lots_1980_2025", value = as.character(sum(proxy_city$residential_lot_count_proxy, na.rm = TRUE)), note = "Current surviving residential lots on proxy lots built between 1980 and 2025."),
    tibble(metric = "city_year_corr_proxy_vs_hdb_nb_units_2010_2025", value = as.character(city_year_corr_units), note = "Correlation between PLUTO surviving-stock proxy units by yearbuilt and DCP Housing Database new-building units by completion year."),
    tibble(metric = "city_year_corr_proxy_vs_hdb_50_plus_units_2010_2025", value = as.character(city_year_corr_50_plus_units), note = "Correlation between PLUTO 50+ unit proxy units and DCP Housing Database 50+ unit new-building units by completion year."),
    tibble(metric = "borough_year_corr_proxy_vs_hdb_nb_units_2010_2025", value = as.character(borough_year_corr_units), note = "Correlation across borough-year observations between PLUTO proxy residential units and DCP Housing Database new-building units."),
    tibble(metric = "borough_year_corr_proxy_vs_hdb_unit_shares_2010_2025", value = as.character(borough_year_corr_unit_shares), note = "Correlation across borough-year observations between PLUTO and HDB borough shares of citywide new-building units."),
    tibble(metric = "borough_year_corr_proxy_vs_hdb_50_plus_unit_shares_2010_2025", value = as.character(borough_year_corr_50_plus_unit_shares), note = "Correlation across borough-year observations between PLUTO and HDB borough shares of 50+ unit new-building units."),
    tibble(metric = "city_total_ratio_proxy_to_hdb_nb_units_2010_2025", value = as.character(sum(hdb_city_compare$residential_units_proxy[hdb_city_compare$yearbuilt >= 2010 & hdb_city_compare$yearbuilt <= 2025], na.rm = TRUE) / sum(hdb_city_compare$hdb_nb_units_completion[hdb_city_compare$yearbuilt >= 2010 & hdb_city_compare$yearbuilt <= 2025], na.rm = TRUE)), note = "Ratio of PLUTO proxy residential units to DCP Housing Database new-building units over 2010-2025."),
    tibble(metric = "cd_corr_proxy_vs_exact_1990s_stock_growth", value = as.character(cd_corr_1990s), note = "Correlation across CDs between PLUTO 1990s proxy units and exact 1990-2000 CD housing-unit growth from DCP profiles."),
    tibble(metric = "cd_corr_proxy_vs_approx_1980s_stock_growth", value = as.character(cd_corr_1980s), note = "Correlation across CDs between PLUTO 1980s proxy units and approximate 1980-1990 CD housing-unit growth from NHGIS overlay controls."),
    tibble(metric = "cd_corr_proxy_vs_exact_1990s_structure_built_2000", value = as.character(cd_corr_1990s_structure_built), note = "Correlation across CDs between PLUTO 1990s proxy units and exact 2000 CD counts of units in structures built in the 1990s from DCP profiles."),
    tibble(metric = "cd_corr_proxy_vs_exact_1980s_structure_built_2000", value = as.character(cd_corr_1980s_structure_built), note = "Correlation across CDs between PLUTO 1980s proxy units and exact 2000 CD counts of units in structures built in the 1980s from DCP profiles."),
    tibble(metric = "city_total_ratio_proxy_to_exact_1990s_stock_growth", value = as.character(decadal_compare |> filter(geography_level == "city", decade == "1990s", comparator_source == "dcp_profiles_exact_1990_2000_stock_growth") |> summarize(value = first(proxy_units) / first(comparator_units)) |> pull(value)), note = "Ratio of PLUTO 1990s proxy units to exact NYC housing-unit growth from 1990 to 2000."),
    tibble(metric = "city_total_ratio_proxy_to_approx_1980s_stock_growth", value = NA_character_, note = "Not reported because the approximate citywide 1980-1990 comparator is near zero after overlay-based netting, so the ratio is not interpretable."),
    tibble(metric = "city_total_ratio_proxy_to_exact_1990s_structure_built_2000", value = as.character(decadal_compare |> filter(geography_level == "city", decade == "1990s", comparator_source == "dcp_profiles_exact_2000_structure_built") |> summarize(value = first(proxy_units) / first(comparator_units)) |> pull(value)), note = "Ratio of PLUTO 1990s proxy units to exact NYC 2000 counts of units in structures built in the 1990s from DCP profiles."),
    tibble(metric = "city_total_ratio_proxy_to_exact_1980s_structure_built_2000", value = as.character(decadal_compare |> filter(geography_level == "city", decade == "1980s", comparator_source == "dcp_profiles_exact_2000_structure_built") |> summarize(value = first(proxy_units) / first(comparator_units)) |> pull(value)), note = "Ratio of PLUTO 1980s proxy units to exact NYC 2000 counts of units in structures built in the 1980s from DCP profiles."),
    tibble(metric = "proxy_interpretation", value = "surviving_residential_stock_proxy", note = "Treat this as a cross-sectional proxy for historical new construction, not as an exact annual flow series.")
  ),
  out_summary_csv,
  na = ""
)

cat("Wrote MapPLUTO construction proxy summaries to", dirname(out_city_csv), "\n")
