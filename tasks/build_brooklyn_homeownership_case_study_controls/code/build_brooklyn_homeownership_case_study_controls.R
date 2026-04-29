#!/usr/bin/env Rscript

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_brooklyn_homeownership_case_study_controls/code")
# baseline_controls_path <- "../input/cd_baseline_1990_controls.csv"
# nhgis_files_path <- "../input/nhgis_files.csv"
# nhgis_1990_path <- "../input/nhgis_1990_tract_extract.parquet"
# boundary_index_path <- "../input/dcp_boundary_index.csv"
# boundary_parquet_path <- "../input/dcp_boundary_community_districts_20260412.parquet"
# redevelopment_path <- "../input/cd_redevelopment_potential_baseline.csv"
# controls_out <- "../output/brooklyn_homeownership_case_study_controls.csv"
# qc_out <- "../output/brooklyn_homeownership_case_study_controls_qc.csv"

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

if (length(args) != 8) {
  stop(
    "Expected 8 arguments: baseline_controls_path nhgis_files_path nhgis_1990_path ",
    "boundary_index_path boundary_parquet_path redevelopment_path controls_out qc_out"
  )
}

baseline_controls_path <- args[1]
nhgis_files_path <- args[2]
nhgis_1990_path <- args[3]
boundary_index_path <- args[4]
boundary_parquet_path <- args[5]
redevelopment_path <- args[6]
controls_out <- args[7]
qc_out <- args[8]

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

build_brooklyn_overlay <- function(nhgis_df, gis_zip_path, cd_sf) {
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
        select(gisjoin, total_population, white_population, black_population, hispanic_any_race, tract_area),
      cd_sf %>%
        select(district_id, borocd, borough_name)
    )
  ) %>%
    mutate(
      intersection_area = as.numeric(st_area(geometry)),
      area_share = ifelse(tract_area > 0, intersection_area / tract_area, NA_real_),
      total_population_alloc = total_population * area_share,
      white_population_alloc = white_population * area_share,
      black_population_alloc = black_population * area_share,
      hispanic_any_race_alloc = hispanic_any_race * area_share
    )

  assignment_qc <- tract_sf %>%
    st_drop_geometry() %>%
    select(gisjoin, total_population, white_population, black_population, hispanic_any_race) %>%
    left_join(
      intersection_sf %>%
        st_drop_geometry() %>%
        group_by(gisjoin) %>%
        summarise(
          area_share_sum = sum(area_share, na.rm = TRUE),
          total_population_alloc_sum = sum(total_population_alloc, na.rm = TRUE),
          white_population_alloc_sum = sum(white_population_alloc, na.rm = TRUE),
          black_population_alloc_sum = sum(black_population_alloc, na.rm = TRUE),
          hispanic_any_race_alloc_sum = sum(hispanic_any_race_alloc, na.rm = TRUE),
          .groups = "drop"
        ),
      by = "gisjoin"
    )

  cd_df <- intersection_sf %>%
    st_drop_geometry() %>%
    group_by(district_id, borocd, borough_name) %>%
    summarise(
      total_population_1990_nhgis = sum(total_population_alloc, na.rm = TRUE),
      white_population_1990_nhgis = sum(white_population_alloc, na.rm = TRUE),
      black_population_1990_nhgis = sum(black_population_alloc, na.rm = TRUE),
      hispanic_population_1990_nhgis = sum(hispanic_any_race_alloc, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      white_share_1990_nhgis = ifelse(total_population_1990_nhgis > 0, white_population_1990_nhgis / total_population_1990_nhgis, NA_real_),
      black_share_1990_nhgis = ifelse(total_population_1990_nhgis > 0, black_population_1990_nhgis / total_population_1990_nhgis, NA_real_),
      hispanic_share_1990_nhgis = ifelse(total_population_1990_nhgis > 0, hispanic_population_1990_nhgis / total_population_1990_nhgis, NA_real_)
    ) %>%
    arrange(borocd)

  qc_df <- bind_rows(
    tibble(
      metric = "overlay_district_count",
      value = nrow(cd_df),
      note = "Brooklyn community districts receiving NHGIS 1990 race/population allocations."
    ),
    tibble(
      metric = "overlay_total_population_assigned_share",
      value = sum(assignment_qc$total_population_alloc_sum, na.rm = TRUE) / sum(assignment_qc$total_population, na.rm = TRUE),
      note = "Share of NHGIS tract total population assigned to Brooklyn CDs."
    ),
    tibble(
      metric = "overlay_white_population_assigned_share",
      value = sum(assignment_qc$white_population_alloc_sum, na.rm = TRUE) / sum(assignment_qc$white_population, na.rm = TRUE),
      note = "Share of NHGIS tract white population assigned to Brooklyn CDs."
    ),
    tibble(
      metric = "overlay_black_population_assigned_share",
      value = sum(assignment_qc$black_population_alloc_sum, na.rm = TRUE) / sum(assignment_qc$black_population, na.rm = TRUE),
      note = "Share of NHGIS tract Black population assigned to Brooklyn CDs."
    ),
    tibble(
      metric = "overlay_hispanic_population_assigned_share",
      value = sum(assignment_qc$hispanic_any_race_alloc_sum, na.rm = TRUE) / sum(assignment_qc$hispanic_any_race, na.rm = TRUE),
      note = "Share of NHGIS tract Hispanic-any-race population assigned to Brooklyn CDs."
    ),
    tibble(
      metric = "overlay_tract_area_share_mean",
      value = mean(assignment_qc$area_share_sum, na.rm = TRUE),
      note = "Mean tract polygon area share assigned to Brooklyn CDs."
    ),
    tibble(
      metric = "overlay_tract_area_share_min",
      value = min(assignment_qc$area_share_sum, na.rm = TRUE),
      note = "Minimum tract polygon area share assigned to Brooklyn CDs."
    )
  )

  list(cd_df = cd_df, qc_df = qc_df)
}

brooklyn_label_map <- tribble(
  ~borocd, ~brooklyn_short_label, ~brooklyn_neighborhood_label,
  301L, "BK01", "Williamsburg/Greenpoint",
  302L, "BK02", "Fort Greene/Brooklyn Heights",
  303L, "BK03", "Bedford-Stuyvesant",
  304L, "BK04", "Bushwick",
  305L, "BK05", "East New York",
  306L, "BK06", "Park Slope/Carroll Gardens",
  307L, "BK07", "Sunset Park",
  308L, "BK08", "Crown Heights/Prospect Heights",
  309L, "BK09", "South Crown Heights/Lefferts Gardens",
  310L, "BK10", "Bay Ridge/Dyker Heights",
  311L, "BK11", "Bensonhurst",
  312L, "BK12", "Borough Park",
  313L, "BK13", "Coney Island/Brighton Beach",
  314L, "BK14", "Flatbush/Midwood",
  315L, "BK15", "Sheepshead Bay",
  316L, "BK16", "Brownsville",
  317L, "BK17", "East Flatbush",
  318L, "BK18", "Canarsie/Flatlands"
) %>%
  mutate(
    cd_label = paste(brooklyn_short_label, brooklyn_neighborhood_label)
  )

baseline_controls <- read_csv(baseline_controls_path, show_col_types = FALSE) %>%
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    district_id = str_pad(as.character(borocd), width = 3, side = "left", pad = "0"),
    borough_name = as.character(borough_name)
  ) %>%
  filter(borough_name == "Brooklyn") %>%
  select(
    borocd,
    district_id,
    borough_name,
    treat_pp,
    treat_z_boro,
    occupied_units_1990_exact,
    median_household_income_1990_1999_dollars_exact,
    poverty_share_1990_exact,
    college_graduate_share_1990_exact,
    structure_share_1_2_units_1990_exact,
    structure_share_3_4_units_1990_exact,
    structure_share_50_plus_units_1990_exact,
    subway_commute_share_1990_exact,
    public_transit_commute_share_1990_exact,
    mean_commute_time_1990_minutes_exact,
    occupied_units_growth_1980_1990_approx,
    vacancy_rate_change_1980_1990_pp_approx,
    homeowner_share_change_1980_1990_pp_approx
  )

redevelopment <- read_csv(redevelopment_path, show_col_types = FALSE) %>%
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    borough_name = as.character(borough_name)
  ) %>%
  filter(borough_name == "Brooklyn") %>%
  select(
    borocd,
    borough_name,
    residential_acres,
    redev_potential_A_z_boro,
    redev_potential_C_z_boro,
    cd_mean_built_far_lot_weighted,
    cd_mean_max_resid_far_lot_weighted,
    cd_mean_unused_res_far_lot_weighted,
    cd_share_lot_area_one_two_family,
    cd_share_lot_area_vacant,
    cd_share_lot_area_old_building,
    cd_share_lot_area_protected,
    cd_share_lot_area_parking_or_low_intensity
  )

nhgis_files <- read_csv(nhgis_files_path, show_col_types = FALSE) %>%
  mutate(year = suppressWarnings(as.integer(year)))

nhgis_1990 <- read_parquet(nhgis_1990_path) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(gisjoin = as.character(gisjoin)) %>%
  select(gisjoin, total_population, white_population, black_population, hispanic_any_race)

nhgis_1990_gis_zip <- nhgis_files %>%
  filter(year == 1990, !is.na(gis_zip_path), file.exists(gis_zip_path)) %>%
  arrange(desc(status == "staged"), gis_zip_path) %>%
  slice_head(n = 1) %>%
  pull(gis_zip_path)

if (length(nhgis_1990_gis_zip) == 0) {
  stop("Could not find the 1990 NHGIS GIS zip path in ", nhgis_files_path)
}

boundary_index <- read_csv(boundary_index_path, show_col_types = FALSE) %>%
  mutate(
    pull_date = as.Date(as.character(suppressWarnings(as.integer(pull_date))), format = "%Y%m%d")
  )

boundary_source_note <- boundary_index %>%
  filter(source_id == "dcp_boundary_community_districts") %>%
  arrange(desc(pull_date)) %>%
  slice_head(n = 1) %>%
  transmute(note = paste0("Boundary source pull date: ", pull_date, ".")) %>%
  pull(note)

boundary_df <- read_parquet(boundary_parquet_path) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(
    district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
    borocd = suppressWarnings(as.integer(district_id))
  ) %>%
  filter(borocd %in% brooklyn_label_map$borocd)

boundary_wkb <- lapply(boundary_df$geometry_wkb_hex, hex_to_raw)
class(boundary_wkb) <- c("WKB", class(boundary_wkb))

brooklyn_sf <- st_sf(
  boundary_df %>%
    transmute(
      district_id,
      borocd,
      borough_name = "Brooklyn"
    ),
  geometry = st_as_sfc(boundary_wkb, EWKB = TRUE, crs = boundary_df$crs_epsg[1])
) %>%
  st_make_valid() %>%
  st_transform(2263)

overlay_1990 <- build_brooklyn_overlay(nhgis_1990, nhgis_1990_gis_zip[[1]], brooklyn_sf)

city_hall_point <- st_sfc(st_point(c(-74.0060, 40.7128)), crs = 4326) %>%
  st_transform(2263)

distance_df <- brooklyn_sf %>%
  mutate(rep_point = st_point_on_surface(geometry)) %>%
  mutate(distance_to_city_hall_miles = as.numeric(st_distance(rep_point, city_hall_point)) / 5280) %>%
  st_drop_geometry() %>%
  select(borocd, distance_to_city_hall_miles)

controls_df <- baseline_controls %>%
  left_join(brooklyn_label_map, by = "borocd") %>%
  left_join(
    overlay_1990$cd_df %>%
      select(
        borocd,
        total_population_1990_nhgis,
        white_population_1990_nhgis,
        black_population_1990_nhgis,
        hispanic_population_1990_nhgis,
        white_share_1990_nhgis,
        black_share_1990_nhgis,
        hispanic_share_1990_nhgis
      ),
    by = "borocd"
  ) %>%
  left_join(distance_df, by = "borocd") %>%
  left_join(redevelopment, by = c("borocd", "borough_name")) %>%
  mutate(
    density_1990_occ_per_res_acre = occupied_units_1990_exact / residential_acres
  ) %>%
  arrange(borocd)

qc_df <- bind_rows(
  tibble(
    metric = "brooklyn_cd_count",
    value = nrow(controls_df),
    note = "Brooklyn CDs in the helper controls file."
  ),
  tibble(
    metric = "missing_neighborhood_label_count",
    value = sum(is.na(controls_df$brooklyn_neighborhood_label)),
    note = "Brooklyn CDs missing the fixed neighborhood label map."
  ),
  tibble(
    metric = "missing_distance_to_city_hall_count",
    value = sum(is.na(controls_df$distance_to_city_hall_miles)),
    note = "Brooklyn CDs missing the City Hall distance proxy."
  ),
  tibble(
    metric = "missing_black_share_1990_nhgis_count",
    value = sum(is.na(controls_df$black_share_1990_nhgis)),
    note = "Brooklyn CDs missing NHGIS 1990 Black share."
  ),
  tibble(
    metric = "missing_hispanic_share_1990_nhgis_count",
    value = sum(is.na(controls_df$hispanic_share_1990_nhgis)),
    note = "Brooklyn CDs missing NHGIS 1990 Hispanic share."
  ),
  tibble(
    metric = "missing_white_share_1990_nhgis_count",
    value = sum(is.na(controls_df$white_share_1990_nhgis)),
    note = "Brooklyn CDs missing NHGIS 1990 white share."
  ),
  overlay_1990$qc_df,
  tibble(
    metric = "boundary_source_note",
    value = NA_real_,
    note = ifelse(length(boundary_source_note) == 0, "Boundary source note unavailable.", boundary_source_note[[1]])
  )
) %>%
  mutate(value = as.character(value))

write_csv_if_changed(controls_df, controls_out)
write_csv_if_changed(qc_df, qc_out)
