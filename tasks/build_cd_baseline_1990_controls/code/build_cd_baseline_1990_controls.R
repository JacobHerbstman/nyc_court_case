# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_cd_baseline_1990_controls/code")
# cd_homeownership_1990_measure_csv <- "../input/cd_homeownership_1990_measure.csv"
# dcp_cd_profiles_parquet <- "../input/dcp_cd_profiles_1990_2000_20260415.parquet"
# nhgis_files_csv <- "../input/nhgis_files.csv"
# nhgis_1980_parquet <- "../input/nhgis_1980_tract_extract.parquet"
# nhgis_1990_parquet <- "../input/nhgis_1990_tract_extract.parquet"
# dcp_boundary_index_csv <- "../input/dcp_boundary_index.csv"
# out_controls_csv <- "../output/cd_baseline_1990_controls.csv"
# out_qc_csv <- "../output/cd_baseline_1990_controls_qc.csv"
# out_overlay_qc_csv <- "../output/cd_baseline_1990_controls_overlay_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(sf)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 9) {
  stop("Expected 9 arguments: cd_homeownership_1990_measure_csv dcp_cd_profiles_parquet nhgis_files_csv nhgis_1980_parquet nhgis_1990_parquet dcp_boundary_index_csv out_controls_csv out_qc_csv out_overlay_qc_csv")
}

cd_homeownership_1990_measure_csv <- args[1]
dcp_cd_profiles_parquet <- args[2]
nhgis_files_csv <- args[3]
nhgis_1980_parquet <- args[4]
nhgis_1990_parquet <- args[5]
dcp_boundary_index_csv <- args[6]
out_controls_csv <- args[7]
out_qc_csv <- args[8]
out_overlay_qc_csv <- args[9]

sf_use_s2(FALSE)

hex_to_raw <- function(x) {
  if (is.na(x) || x == "") {
    return(as.raw())
  }

  as.raw(strtoi(substring(x, seq(1, nchar(x), by = 2), seq(2, nchar(x), by = 2)), 16L))
}

read_nested_shape <- function(outer_zip_path) {
  outer_listing <- unzip(outer_zip_path, list = TRUE)
  inner_zip_rel <- outer_listing$Name[str_detect(tolower(outer_listing$Name), "shapefile.*\\.zip$")][1]

  if (is.na(inner_zip_rel)) {
    stop("Could not find nested NHGIS shapefile zip inside ", outer_zip_path)
  }

  outer_tmp_dir <- tempfile(pattern = "nhgis_outer_")
  inner_tmp_dir <- tempfile(pattern = "nhgis_inner_")
  dir.create(outer_tmp_dir)
  dir.create(inner_tmp_dir)

  unzip(outer_zip_path, files = inner_zip_rel, exdir = outer_tmp_dir)
  unzip(file.path(outer_tmp_dir, inner_zip_rel), exdir = inner_tmp_dir)

  shp_candidates <- list.files(inner_tmp_dir, pattern = "\\.shp$", recursive = TRUE, full.names = TRUE)
  tract_hits <- shp_candidates[str_detect(basename(shp_candidates), "^US_tract_[0-9]{4}\\.shp$")]
  shp_path <- if (length(tract_hits) > 0) tract_hits[1] else shp_candidates[1]

  if (is.na(shp_path)) {
    stop("Could not find shapefile after extracting ", outer_zip_path)
  }

  shape_df <- st_read(shp_path, quiet = TRUE)
  names(shape_df) <- normalize_names(names(shape_df))
  shape_df
}

pull_dcp_metric <- function(df, section_key, metric_key, value_col, out_name) {
  df %>%
    filter(section_name == section_key, metric_label == metric_key) %>%
    transmute(
      district_id,
      !!out_name := suppressWarnings(as.numeric(.data[[value_col]]))
    )
}

pull_dcp_metric_max <- function(df, section_key, metric_key, value_col, out_name) {
  df %>%
    filter(section_name == section_key, metric_label == metric_key) %>%
    transmute(
      district_id,
      value = suppressWarnings(as.numeric(.data[[value_col]]))
    ) %>%
    group_by(district_id) %>%
    summarise(
      !!out_name := max(value, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      !!out_name := ifelse(is.infinite(.data[[out_name]]), NA_real_, .data[[out_name]])
    )
}

build_cd_overlay <- function(nhgis_df, gis_zip_path, cd_sf, year_value) {
  tract_shape <- read_nested_shape(gis_zip_path) %>%
    transmute(gisjoin = as.character(gisjoin), geometry)

  tract_sf <- tract_shape %>%
    inner_join(nhgis_df, by = "gisjoin") %>%
    st_as_sf() %>%
    st_make_valid() %>%
    st_transform(2263) %>%
    mutate(tract_area = as.numeric(st_area(geometry)))

  intersection_sf <- suppressWarnings(
    st_intersection(
      tract_sf %>%
        select(gisjoin, total_housing_units, occupied_units, owner_occupied_units, vacant_units, tract_area),
      cd_sf %>%
        select(district_id, borough_code, borough_name)
    )
  ) %>%
    mutate(
      intersection_area = as.numeric(st_area(geometry)),
      area_share = ifelse(tract_area > 0, intersection_area / tract_area, NA_real_),
      total_housing_units_alloc = total_housing_units * area_share,
      occupied_units_alloc = occupied_units * area_share,
      owner_occupied_units_alloc = owner_occupied_units * area_share,
      vacant_units_alloc = vacant_units * area_share
    )

  assignment_qc <- tract_sf %>%
    st_drop_geometry() %>%
    select(gisjoin, total_housing_units, occupied_units, owner_occupied_units, vacant_units) %>%
    left_join(
      intersection_sf %>%
        st_drop_geometry() %>%
        group_by(gisjoin) %>%
        summarise(
          area_share_sum = sum(area_share, na.rm = TRUE),
          total_housing_units_alloc_sum = sum(total_housing_units_alloc, na.rm = TRUE),
          occupied_units_alloc_sum = sum(occupied_units_alloc, na.rm = TRUE),
          owner_occupied_units_alloc_sum = sum(owner_occupied_units_alloc, na.rm = TRUE),
          vacant_units_alloc_sum = sum(vacant_units_alloc, na.rm = TRUE),
          .groups = "drop"
        ),
      by = "gisjoin"
    ) %>%
    mutate(
      area_share_sum = coalesce(area_share_sum, 0),
      total_housing_units_assignment_share = ifelse(total_housing_units > 0, total_housing_units_alloc_sum / total_housing_units, NA_real_),
      occupied_units_assignment_share = ifelse(occupied_units > 0, occupied_units_alloc_sum / occupied_units, NA_real_),
      owner_occupied_units_assignment_share = ifelse(owner_occupied_units > 0, owner_occupied_units_alloc_sum / owner_occupied_units, NA_real_),
      vacant_units_assignment_share = ifelse(vacant_units > 0, vacant_units_alloc_sum / vacant_units, NA_real_)
    )

  cd_df <- intersection_sf %>%
    st_drop_geometry() %>%
    group_by(district_id, borough_code, borough_name) %>%
    summarise(
      total_housing_units = sum(total_housing_units_alloc, na.rm = TRUE),
      occupied_units = sum(occupied_units_alloc, na.rm = TRUE),
      owner_occupied_units = sum(owner_occupied_units_alloc, na.rm = TRUE),
      vacant_units = sum(vacant_units_alloc, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      homeowner_share = ifelse(occupied_units > 0, owner_occupied_units / occupied_units, NA_real_),
      vacancy_rate = ifelse(total_housing_units > 0, vacant_units / total_housing_units, NA_real_)
    ) %>%
    arrange(district_id)

  qc_df <- bind_rows(
    tibble(
      year = year_value,
      metric = "district_count",
      value = nrow(cd_df),
      note = "Standard current community districts receiving NHGIS tract allocations."
    ),
    tibble(
      year = year_value,
      metric = "total_housing_units_assigned_share",
      value = sum(assignment_qc$total_housing_units_alloc_sum, na.rm = TRUE) / sum(assignment_qc$total_housing_units, na.rm = TRUE),
      note = "Share of NHGIS tract total housing units assigned to the standard current community districts."
    ),
    tibble(
      year = year_value,
      metric = "occupied_units_assigned_share",
      value = sum(assignment_qc$occupied_units_alloc_sum, na.rm = TRUE) / sum(assignment_qc$occupied_units, na.rm = TRUE),
      note = "Share of NHGIS tract occupied units assigned to the standard current community districts."
    ),
    tibble(
      year = year_value,
      metric = "owner_occupied_units_assigned_share",
      value = sum(assignment_qc$owner_occupied_units_alloc_sum, na.rm = TRUE) / sum(assignment_qc$owner_occupied_units, na.rm = TRUE),
      note = "Share of NHGIS tract owner-occupied units assigned to the standard current community districts."
    ),
    tibble(
      year = year_value,
      metric = "vacant_units_assigned_share",
      value = sum(assignment_qc$vacant_units_alloc_sum, na.rm = TRUE) / sum(assignment_qc$vacant_units, na.rm = TRUE),
      note = "Share of NHGIS tract vacant units assigned to the standard current community districts."
    ),
    tibble(
      year = year_value,
      metric = "tract_area_share_mean",
      value = mean(assignment_qc$area_share_sum, na.rm = TRUE),
      note = "Mean tract polygon area share assigned to the standard current community districts."
    ),
    tibble(
      year = year_value,
      metric = "tract_area_share_min",
      value = min(assignment_qc$area_share_sum, na.rm = TRUE),
      note = "Minimum tract polygon area share assigned to the standard current community districts."
    )
  )

  list(cd_df = cd_df, qc_df = qc_df)
}

standard_cd_ids <- c(
  sprintf("1%02d", 1:12),
  sprintf("2%02d", 1:12),
  sprintf("3%02d", 1:18),
  sprintf("4%02d", 1:14),
  sprintf("5%02d", 1:3)
)

measure_df <- read_csv(cd_homeownership_1990_measure_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
    borocd = as.integer(borocd),
    borough_code = as.character(borough_code),
    borough_name = standardize_borough_name(borough_code)
  ) %>%
  arrange(district_id)

profiles_df <- read_parquet(dcp_cd_profiles_parquet) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0")) %>%
  filter(district_id %in% standard_cd_ids)

exact_df <- measure_df %>%
  select(source_id, pull_date, district_id, borocd, borough_code, borough_name, treat_pp, treat_z_boro) %>%
  left_join(pull_dcp_metric(profiles_df, "housing_occupancy", "Total housing units", "value_1990_number", "total_housing_units_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "housing_occupancy", "Occupied housing units", "value_1990_number", "occupied_units_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "housing_occupancy", "Vacant housing units", "value_1990_number", "vacant_housing_units_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "housing_tenure", "Owner-occupied housing units", "value_1990_number", "owner_occupied_units_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "housing_tenure", "Renter-occupied housing units", "value_1990_number", "renter_occupied_units_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "units_in_structure", "1 unit, detached", "value_1990_number", "structure_units_1_detached_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "units_in_structure", "1 unit, attached", "value_1990_number", "structure_units_1_attached_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "units_in_structure", "2 units", "value_1990_number", "structure_units_2_units_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "units_in_structure", "3 or 4 units", "value_1990_number", "structure_units_3_4_units_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "units_in_structure", "5 to 9 units", "value_1990_number", "structure_units_5_9_units_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "units_in_structure", "10 to 19 units", "value_1990_number", "structure_units_10_19_units_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "units_in_structure", "20 to 49 units", "value_1990_number", "structure_units_20_49_units_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "units_in_structure", "50 or more units", "value_1990_number", "structure_units_50_plus_units_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "units_in_structure", "Other", "value_1990_number", "structure_units_other_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "income_in_1989_and_1999", "Median household income (1999 constant dollars)", "value_1990_number", "median_household_income_1990_1999_dollars_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "poverty_status_in_1989_and_1999", "Persons below the poverty level", "value_1990_number", "persons_below_poverty_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "poverty_status_in_1989_and_1999", "Persons for whom poverty status was determined", "value_1990_number", "persons_poverty_universe_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "value", "Median housing value (2000 constant dollars)", "value_1990_number", "median_housing_value_1990_2000_dollars_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "nativity_and_place_of_birth", "Total population", "value_1990_number", "total_population_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "nativity_and_place_of_birth", "Foreign-born", "value_1990_number", "foreign_born_population_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "educational_attainment", "Population 25 years and over", "value_1990_number", "population_25_and_over_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "educational_attainment", "College graduate", "value_1990_number", "college_graduate_population_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "educational_attainment", "High school graduate or higher", "value_1990_number", "high_school_or_higher_population_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric_max(profiles_df, "employment_status", "In the labor force", "value_1990_number", "in_labor_force_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric_max(profiles_df, "employment_status", "Unemployed", "value_1990_number", "unemployed_population_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "commuting_to_work", "Workers 16 years and over", "value_1990_number", "workers_16_and_over_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "commuting_to_work", "Subway", "value_1990_number", "subway_commuters_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "commuting_to_work", "Public transportation", "value_1990_number", "public_transport_commuters_1990_exact"), by = "district_id") %>%
  left_join(pull_dcp_metric(profiles_df, "commuting_to_work", "Mean travel time to work (minutes)", "value_1990_number", "mean_commute_time_1990_minutes_exact"), by = "district_id") %>%
  mutate(
    vacancy_rate_1990_exact = ifelse(total_housing_units_1990_exact > 0, vacant_housing_units_1990_exact / total_housing_units_1990_exact, NA_real_),
    renter_share_1990_exact = ifelse(occupied_units_1990_exact > 0, renter_occupied_units_1990_exact / occupied_units_1990_exact, NA_real_),
    structure_share_1_detached_1990_exact = ifelse(total_housing_units_1990_exact > 0, structure_units_1_detached_1990_exact / total_housing_units_1990_exact, NA_real_),
    structure_share_1_attached_1990_exact = ifelse(total_housing_units_1990_exact > 0, structure_units_1_attached_1990_exact / total_housing_units_1990_exact, NA_real_),
    structure_share_2_units_1990_exact = ifelse(total_housing_units_1990_exact > 0, structure_units_2_units_1990_exact / total_housing_units_1990_exact, NA_real_),
    structure_share_3_4_units_1990_exact = ifelse(total_housing_units_1990_exact > 0, structure_units_3_4_units_1990_exact / total_housing_units_1990_exact, NA_real_),
    structure_share_5_9_units_1990_exact = ifelse(total_housing_units_1990_exact > 0, structure_units_5_9_units_1990_exact / total_housing_units_1990_exact, NA_real_),
    structure_share_10_19_units_1990_exact = ifelse(total_housing_units_1990_exact > 0, structure_units_10_19_units_1990_exact / total_housing_units_1990_exact, NA_real_),
    structure_share_20_49_units_1990_exact = ifelse(total_housing_units_1990_exact > 0, structure_units_20_49_units_1990_exact / total_housing_units_1990_exact, NA_real_),
    structure_share_50_plus_units_1990_exact = ifelse(total_housing_units_1990_exact > 0, structure_units_50_plus_units_1990_exact / total_housing_units_1990_exact, NA_real_),
    structure_share_other_1990_exact = ifelse(total_housing_units_1990_exact > 0, structure_units_other_1990_exact / total_housing_units_1990_exact, NA_real_),
    structure_share_1_2_units_1990_exact = ifelse(
      total_housing_units_1990_exact > 0,
      (structure_units_1_detached_1990_exact + structure_units_1_attached_1990_exact + structure_units_2_units_1990_exact) / total_housing_units_1990_exact,
      NA_real_
    ),
    structure_share_5_plus_units_1990_exact = ifelse(
      total_housing_units_1990_exact > 0,
      (structure_units_5_9_units_1990_exact + structure_units_10_19_units_1990_exact + structure_units_20_49_units_1990_exact + structure_units_50_plus_units_1990_exact) / total_housing_units_1990_exact,
      NA_real_
    ),
    poverty_share_1990_exact = ifelse(persons_poverty_universe_1990_exact > 0, persons_below_poverty_1990_exact / persons_poverty_universe_1990_exact, NA_real_),
    foreign_born_share_1990_exact = ifelse(total_population_1990_exact > 0, foreign_born_population_1990_exact / total_population_1990_exact, NA_real_),
    college_graduate_share_1990_exact = ifelse(population_25_and_over_1990_exact > 0, college_graduate_population_1990_exact / population_25_and_over_1990_exact, NA_real_),
    high_school_or_higher_share_1990_exact = ifelse(population_25_and_over_1990_exact > 0, high_school_or_higher_population_1990_exact / population_25_and_over_1990_exact, NA_real_),
    unemployment_rate_1990_exact = ifelse(in_labor_force_1990_exact > 0, unemployed_population_1990_exact / in_labor_force_1990_exact, NA_real_),
    subway_commute_share_1990_exact = ifelse(workers_16_and_over_1990_exact > 0, subway_commuters_1990_exact / workers_16_and_over_1990_exact, NA_real_),
    public_transit_commute_share_1990_exact = ifelse(workers_16_and_over_1990_exact > 0, public_transport_commuters_1990_exact / workers_16_and_over_1990_exact, NA_real_),
    median_housing_value_1990_missing_exact = is.na(median_housing_value_1990_2000_dollars_exact)
  ) %>%
  group_by(borough_code, borough_name) %>%
  mutate(
    median_housing_value_1990_2000_dollars_exact_filled = ifelse(
      is.na(median_housing_value_1990_2000_dollars_exact),
      median(median_housing_value_1990_2000_dollars_exact, na.rm = TRUE),
      median_housing_value_1990_2000_dollars_exact
    )
  ) %>%
  ungroup()

nhgis_files <- read_csv(nhgis_files_csv, show_col_types = FALSE, na = c("", "NA"))

nhgis_1980 <- read_parquet(nhgis_1980_parquet) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(gisjoin = as.character(gisjoin)) %>%
  select(gisjoin, total_housing_units, occupied_units, owner_occupied_units, vacant_units)

nhgis_1990 <- read_parquet(nhgis_1990_parquet) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(gisjoin = as.character(gisjoin)) %>%
  select(gisjoin, total_housing_units, occupied_units, owner_occupied_units, vacant_units)

nhgis_1980_gis_zip <- nhgis_files %>%
  filter(year == 1980, !is.na(gis_zip_path), file.exists(gis_zip_path)) %>%
  arrange(desc(status == "staged"), gis_zip_path) %>%
  slice_head(n = 1) %>%
  pull(gis_zip_path)

nhgis_1990_gis_zip <- nhgis_files %>%
  filter(year == 1990, !is.na(gis_zip_path), file.exists(gis_zip_path)) %>%
  arrange(desc(status == "staged"), gis_zip_path) %>%
  slice_head(n = 1) %>%
  pull(gis_zip_path)

if (length(nhgis_1980_gis_zip) == 0 || length(nhgis_1990_gis_zip) == 0) {
  stop("Could not find both 1980 and 1990 NHGIS GIS zip paths in ", nhgis_files_csv)
}

dcp_boundary_index <- read_csv(dcp_boundary_index_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(pull_date = as.Date(pull_date))

community_district_parquet <- dcp_boundary_index %>%
  filter(source_id == "dcp_boundary_community_districts", !is.na(parquet_path), file.exists(parquet_path)) %>%
  arrange(desc(pull_date), parquet_path) %>%
  slice_head(n = 1) %>%
  pull(parquet_path)

if (length(community_district_parquet) == 0) {
  stop("Could not find a staged community district parquet path in ", dcp_boundary_index_csv)
}

boundary_df <- read_parquet(community_district_parquet[[1]]) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0")) %>%
  filter(district_id %in% standard_cd_ids)

boundary_wkb <- lapply(boundary_df$geometry_wkb_hex, hex_to_raw)
class(boundary_wkb) <- c("WKB", class(boundary_wkb))

cd_sf <- st_sf(
  boundary_df %>%
    transmute(
      district_id,
      borough_code = substr(district_id, 1, 1),
      borough_name = standardize_borough_name(substr(district_id, 1, 1))
    ),
  geometry = st_as_sfc(boundary_wkb, EWKB = TRUE, crs = boundary_df$crs_epsg[1])
) %>%
  st_make_valid() %>%
  st_transform(2263)

overlay_1980 <- build_cd_overlay(nhgis_1980, nhgis_1980_gis_zip[[1]], cd_sf, 1980)
overlay_1990 <- build_cd_overlay(nhgis_1990, nhgis_1990_gis_zip[[1]], cd_sf, 1990)

controls_df <- exact_df %>%
  left_join(
    overlay_1980$cd_df %>%
      transmute(
        district_id,
        total_housing_units_1980_approx = total_housing_units,
        occupied_units_1980_approx = occupied_units,
        owner_occupied_units_1980_approx = owner_occupied_units,
        vacant_units_1980_approx = vacant_units,
        homeowner_share_1980_approx = homeowner_share,
        vacancy_rate_1980_approx = vacancy_rate
      ),
    by = "district_id"
  ) %>%
  left_join(
    overlay_1990$cd_df %>%
      transmute(
        district_id,
        total_housing_units_1990_approx = total_housing_units,
        occupied_units_1990_approx = occupied_units,
        owner_occupied_units_1990_approx = owner_occupied_units,
        vacant_units_1990_approx = vacant_units,
        homeowner_share_1990_approx = homeowner_share,
        vacancy_rate_1990_approx = vacancy_rate
      ),
    by = "district_id"
  ) %>%
  mutate(
    total_housing_units_growth_1980_1990_approx = ifelse(
      total_housing_units_1980_approx > 0,
      total_housing_units_1990_approx / total_housing_units_1980_approx - 1,
      NA_real_
    ),
    occupied_units_growth_1980_1990_approx = ifelse(
      occupied_units_1980_approx > 0,
      occupied_units_1990_approx / occupied_units_1980_approx - 1,
      NA_real_
    ),
    vacancy_rate_change_1980_1990_pp_approx = 100 * (vacancy_rate_1990_approx - vacancy_rate_1980_approx),
    homeowner_share_change_1980_1990_pp_approx = 100 * (homeowner_share_1990_approx - homeowner_share_1980_approx)
  ) %>%
  arrange(district_id)

overlay_qc_df <- bind_rows(
  overlay_1980$qc_df,
  overlay_1990$qc_df
) %>%
  arrange(year, metric)

primary_exact_control_vars <- c(
  "vacancy_rate_1990_exact",
  "renter_share_1990_exact",
  "structure_share_1_detached_1990_exact",
  "structure_share_1_attached_1990_exact",
  "structure_share_2_units_1990_exact",
  "structure_share_3_4_units_1990_exact",
  "structure_share_5_9_units_1990_exact",
  "structure_share_10_19_units_1990_exact",
  "structure_share_20_49_units_1990_exact",
  "structure_share_50_plus_units_1990_exact",
  "median_household_income_1990_1999_dollars_exact",
  "poverty_share_1990_exact",
  "subway_commute_share_1990_exact",
  "public_transit_commute_share_1990_exact",
  "mean_commute_time_1990_minutes_exact"
)

static_1990_demo_control_vars <- c(
  "foreign_born_share_1990_exact",
  "college_graduate_share_1990_exact",
  "high_school_or_higher_share_1990_exact",
  "unemployment_rate_1990_exact"
)

qc_df <- bind_rows(
  tibble(
    metric = "district_count",
    value = nrow(controls_df),
    note = "Standard community districts in the baseline controls file."
  ),
  tibble(
    metric = "missing_primary_exact_control_cd_count_excluding_housing_value",
    value = sum(!stats::complete.cases(controls_df[, primary_exact_control_vars])),
    note = "Community districts missing any chosen exact 1990 DCP baseline control other than median housing value."
  ),
  tibble(
    metric = "median_housing_value_missing_cd_count",
    value = sum(is.na(controls_df$median_housing_value_1990_2000_dollars_exact)),
    note = "Community districts where the DCP profile does not report the 1990 median housing value field."
  ),
  tibble(
    metric = "missing_static_1990_demo_control_cd_count",
    value = sum(!stats::complete.cases(controls_df[, static_1990_demo_control_vars])),
    note = "Community districts missing any chosen exact 1990 demographic baseline control from the DCP profiles."
  ),
  tibble(
    metric = "approx_pretrend_missing_cd_count",
    value = sum(!stats::complete.cases(controls_df[, c(
      "total_housing_units_growth_1980_1990_approx",
      "occupied_units_growth_1980_1990_approx",
      "vacancy_rate_change_1980_1990_pp_approx",
      "homeowner_share_change_1980_1990_pp_approx"
    )])),
    note = "Community districts missing any approximate 1980-1990 NHGIS overlay pretrend control."
  ),
  tibble(
    metric = "tenure_gap_max_abs",
    value = max(abs((controls_df$owner_occupied_units_1990_exact + controls_df$renter_occupied_units_1990_exact) - controls_df$occupied_units_1990_exact), na.rm = TRUE),
    note = "Maximum absolute gap between owner plus renter counts and occupied housing units in the exact DCP control source."
  ),
  tibble(
    metric = "structure_share_sum_gap_max_abs",
    value = max(abs((
      controls_df$structure_share_1_detached_1990_exact +
        controls_df$structure_share_1_attached_1990_exact +
        controls_df$structure_share_2_units_1990_exact +
        controls_df$structure_share_3_4_units_1990_exact +
        controls_df$structure_share_5_9_units_1990_exact +
        controls_df$structure_share_10_19_units_1990_exact +
        controls_df$structure_share_20_49_units_1990_exact +
        controls_df$structure_share_50_plus_units_1990_exact +
        controls_df$structure_share_other_1990_exact
    ) - 1), na.rm = TRUE),
    note = "Maximum absolute gap between the exact 1990 units-in-structure shares and one."
  ),
  tibble(
    metric = "year_structure_built_1980_1989_1990_available_count",
    value = profiles_df %>%
      filter(section_name == "year_structure_built", metric_label == "1980 to 1989") %>%
      summarise(value = sum(!is.na(value_1990_number) | !is.na(value_1990_percent)), .groups = "drop") %>%
      pull(value),
    note = "Available 1990 cells for the DCP profile year-structure-built 1980-1989 row; this source reports only 2000 values for that section."
  ),
  tibble(
    metric = "status",
    value = ifelse(
      nrow(controls_df) == 59 &&
        sum(!stats::complete.cases(controls_df[, primary_exact_control_vars])) == 0 &&
        all(overlay_qc_df %>% filter(metric == "district_count") %>% pull(value) == 59),
      1,
      0
    ),
    note = "One means the baseline controls file is complete for the chosen exact controls and approximate overlay pretrend coverage."
  )
)

write_csv_if_changed(controls_df, out_controls_csv)
write_csv_if_changed(qc_df, out_qc_csv)
write_csv_if_changed(overlay_qc_df, out_overlay_qc_csv)

cat("Wrote CD baseline control outputs to", dirname(out_controls_csv), "\n")
