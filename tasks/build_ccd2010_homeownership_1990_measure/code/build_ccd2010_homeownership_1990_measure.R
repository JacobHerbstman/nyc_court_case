# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_ccd2010_homeownership_1990_measure/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(sf)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

sf_use_s2(FALSE)

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
  on.exit(unlink(c(outer_tmp_dir, inner_tmp_dir), recursive = TRUE), add = TRUE)

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

read_council_shape <- function(zip_path) {
  tmp_dir <- tempfile(pattern = "nycc_10c_")
  dir.create(tmp_dir)
  on.exit(unlink(tmp_dir, recursive = TRUE), add = TRUE)

  unzip(zip_path, exdir = tmp_dir)
  shp_path <- list.files(tmp_dir, pattern = "\\.shp$", recursive = TRUE, full.names = TRUE)[1]

  if (is.na(shp_path)) {
    stop("Could not find a shapefile inside ", zip_path)
  }

  boundary_sf <- st_read(shp_path, quiet = TRUE)
  names(boundary_sf) <- normalize_names(names(boundary_sf))

  boundary_sf %>%
    mutate(
      council_district = standardize_council_district(pick_first_existing(., c("coundist", "coun_dist", "council", "district"))),
      district_id = sprintf("%02d", council_district)
    ) %>%
    filter(!is.na(council_district)) %>%
    transmute(district_id, council_district, geometry) %>%
    st_make_valid() %>%
    st_transform(2263) %>%
    arrange(council_district)
}

county_lookup <- tribble(
  ~countya, ~borough_code, ~borough_name,
  "061", "1", "Manhattan",
  "005", "2", "Bronx",
  "047", "3", "Brooklyn",
  "081", "4", "Queens",
  "085", "5", "Staten Island"
)

nhgis_files <- read_csv("../input/nhgis_raw_files.csv", show_col_types = FALSE, na = c("", "NA"))
nhgis_gis_zip <- nhgis_files %>%
  filter(year == 1990, !is.na(gis_zip_path), file.exists(gis_zip_path)) %>%
  arrange(desc(status == "loaded"), desc(extract_number), gis_zip_path) %>%
  slice_head(n = 1) %>%
  pull(gis_zip_path)

if (length(nhgis_gis_zip) == 0) {
  stop("Could not find a 1990 NHGIS GIS zip path in ../input/nhgis_raw_files.csv")
}

nhgis_1990 <- read_parquet("../input/nhgis_1990_tract_extract.parquet") %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(
    gisjoin = as.character(gisjoin),
    countya = str_pad(str_extract(as.character(countya), "[0-9]+"), width = 3, side = "left", pad = "0")
  ) %>%
  select(
    source_id,
    gisjoin,
    countya,
    total_housing_units,
    occupied_units,
    owner_occupied_units,
    renter_occupied_units,
    vacant_units,
    total_population,
    white_population,
    black_population,
    asian_pacific_islander_population,
    other_race_population,
    hispanic_any_race,
    median_household_income
  )

tract_sf <- read_nested_shape(nhgis_gis_zip[[1]]) %>%
  transmute(gisjoin = as.character(gisjoin), geometry) %>%
  inner_join(nhgis_1990, by = "gisjoin", relationship = "one-to-one") %>%
  left_join(county_lookup, by = "countya", relationship = "many-to-one") %>%
  st_as_sf() %>%
  st_make_valid() %>%
  st_transform(2263) %>%
  mutate(tract_area = as.numeric(st_area(geometry)))

council_sf <- read_council_shape("../input/nycc_10cav.zip")

if (nrow(council_sf) != 51 || n_distinct(council_sf$district_id) != 51) {
  stop("Expected the 2010 council boundary file to contain exactly 51 unique districts.")
}

intersection_sf <- suppressWarnings(
  st_intersection(
    tract_sf %>%
      select(
        gisjoin,
        countya,
        tract_borough_code = borough_code,
        tract_borough_name = borough_name,
        total_housing_units,
        occupied_units,
        owner_occupied_units,
        renter_occupied_units,
        vacant_units,
        total_population,
        white_population,
        black_population,
        asian_pacific_islander_population,
        other_race_population,
        hispanic_any_race,
        median_household_income,
        tract_area
      ),
    council_sf %>% select(district_id, council_district)
  )
) %>%
  mutate(
    intersection_area = as.numeric(st_area(geometry)),
    area_share = ifelse(tract_area > 0, intersection_area / tract_area, NA_real_),
    total_housing_units_alloc = total_housing_units * area_share,
    occupied_units_alloc = occupied_units * area_share,
    owner_occupied_units_alloc = owner_occupied_units * area_share,
    renter_occupied_units_alloc = renter_occupied_units * area_share,
    vacant_units_alloc = vacant_units * area_share,
    total_population_alloc = total_population * area_share,
    white_population_alloc = white_population * area_share,
    black_population_alloc = black_population * area_share,
    asian_pacific_islander_population_alloc = asian_pacific_islander_population * area_share,
    other_race_population_alloc = other_race_population * area_share,
    hispanic_any_race_alloc = hispanic_any_race * area_share,
    household_income_weight = ifelse(occupied_units > 0, occupied_units_alloc, 0),
    median_household_income_alloc = median_household_income * household_income_weight
  )

allocation_by_county <- intersection_sf %>%
  st_drop_geometry() %>%
  group_by(district_id, council_district, tract_borough_code, tract_borough_name) %>%
  summarise(
    occupied_units_alloc = sum(occupied_units_alloc, na.rm = TRUE),
    .groups = "drop"
  )

district_borough <- allocation_by_county %>%
  group_by(district_id, council_district) %>%
  mutate(district_occupied_units_alloc = sum(occupied_units_alloc, na.rm = TRUE)) %>%
  arrange(desc(occupied_units_alloc), tract_borough_code) %>%
  slice_head(n = 1) %>%
  ungroup() %>%
  transmute(
    district_id,
    council_district,
    borough_code = tract_borough_code,
    borough_name = tract_borough_name,
    majority_borough_occupied_share = ifelse(
      district_occupied_units_alloc > 0,
      occupied_units_alloc / district_occupied_units_alloc,
      NA_real_
    )
  )

district_alloc <- intersection_sf %>%
  st_drop_geometry() %>%
  group_by(district_id, council_district) %>%
  summarise(
    tract_overlap_count = n_distinct(gisjoin),
    total_housing_units_1990 = sum(total_housing_units_alloc, na.rm = TRUE),
    occupied_units_1990 = sum(occupied_units_alloc, na.rm = TRUE),
    owner_occupied_units_1990 = sum(owner_occupied_units_alloc, na.rm = TRUE),
    renter_occupied_units_1990 = sum(renter_occupied_units_alloc, na.rm = TRUE),
    vacant_units_1990 = sum(vacant_units_alloc, na.rm = TRUE),
    total_population_1990 = sum(total_population_alloc, na.rm = TRUE),
    white_population_1990 = sum(white_population_alloc, na.rm = TRUE),
    black_population_1990 = sum(black_population_alloc, na.rm = TRUE),
    asian_pacific_islander_population_1990 = sum(asian_pacific_islander_population_alloc, na.rm = TRUE),
    other_race_population_1990 = sum(other_race_population_alloc, na.rm = TRUE),
    hispanic_population_1990 = sum(hispanic_any_race_alloc, na.rm = TRUE),
    median_household_income_1990 = ifelse(
      sum(household_income_weight, na.rm = TRUE) > 0,
      sum(median_household_income_alloc, na.rm = TRUE) / sum(household_income_weight, na.rm = TRUE),
      NA_real_
    ),
    .groups = "drop"
  ) %>%
  left_join(district_borough, by = c("district_id", "council_district"), relationship = "one-to-one") %>%
  mutate(
    h_ccd_1990 = ifelse(occupied_units_1990 > 0, owner_occupied_units_1990 / occupied_units_1990, NA_real_),
    h_ccd_1990_pct = 100 * h_ccd_1990,
    vacancy_rate_1990 = ifelse(total_housing_units_1990 > 0, vacant_units_1990 / total_housing_units_1990, NA_real_),
    poverty_share_1990 = NA_real_
  )

borough_df <- nhgis_1990 %>%
  left_join(county_lookup, by = "countya", relationship = "many-to-one") %>%
  group_by(borough_code, borough_name) %>%
  summarise(
    borough_owner_occupied_units_1990 = sum(owner_occupied_units, na.rm = TRUE),
    borough_occupied_units_1990 = sum(occupied_units, na.rm = TRUE),
    h_b_1990 = borough_owner_occupied_units_1990 / borough_occupied_units_1990,
    h_b_1990_pct = 100 * h_b_1990,
    .groups = "drop"
  )

measure_attributes <- district_alloc %>%
  left_join(borough_df, by = c("borough_code", "borough_name"), relationship = "many-to-one") %>%
  mutate(
    ccd_minus_borough_1990 = h_ccd_1990 - h_b_1990,
    treat_pp = 100 * ccd_minus_borough_1990
  ) %>%
  group_by(borough_code, borough_name) %>%
  mutate(
    treat_pp_boro_mean = mean(treat_pp, na.rm = TRUE),
    treat_pp_boro_sd = sd(treat_pp, na.rm = TRUE),
    treat_z_boro = (treat_pp - treat_pp_boro_mean) / treat_pp_boro_sd,
    treat_z_boro = ifelse(is.finite(treat_z_boro), treat_z_boro, NA_real_)
  ) %>%
  ungroup() %>%
  arrange(council_district)

map_sf <- council_sf %>%
  left_join(measure_attributes, by = c("district_id", "council_district"), relationship = "one-to-one")

measure_df <- map_sf %>%
  mutate(
    geometry_wkb_hex = vapply(
      st_as_binary(st_geometry(.), EWKB = TRUE),
      function(x) paste(sprintf("%02X", as.integer(x)), collapse = ""),
      character(1)
    ),
    geometry_wkt = as.character(st_as_text(st_geometry(.))),
    source_id = "dcp_nycc_10cav_nhgis_1990_tract_area_overlay",
    council_boundary_source = "DCP archived city council districts 10C nycc_10cav.zip",
    tenure_source = unique(nhgis_1990$source_id)[1]
  ) %>%
  st_drop_geometry() %>%
  as_tibble() %>%
  select(
    source_id,
    council_boundary_source,
    tenure_source,
    district_id,
    council_district,
    borough_code,
    borough_name,
    majority_borough_occupied_share,
    owner_occupied_units_1990,
    occupied_units_1990,
    renter_occupied_units_1990,
    total_housing_units_1990,
    vacant_units_1990,
    borough_owner_occupied_units_1990,
    borough_occupied_units_1990,
    h_ccd_1990,
    h_ccd_1990_pct,
    h_b_1990,
    h_b_1990_pct,
    ccd_minus_borough_1990,
    treat_pp,
    treat_z_boro,
    vacancy_rate_1990,
    total_population_1990,
    white_population_1990,
    black_population_1990,
    asian_pacific_islander_population_1990,
    other_race_population_1990,
    hispanic_population_1990,
    median_household_income_1990,
    tract_overlap_count,
    geometry_wkb_hex,
    geometry_wkt
  )

write_csv_if_changed(measure_df, "../output/ccdist2010_homeownership_1990_measure.csv")

pdf("../output/ccdist2010_homeownership_1990_map.pdf", width = 10, height = 7.5)
print(
  ggplot() +
    geom_sf(data = map_sf, fill = "grey94", color = "white", linewidth = 0.08) +
    geom_sf(data = map_sf, aes(fill = treat_z_boro), color = "grey88", linewidth = 0.12) +
    scale_fill_gradient2(
      low = "#4C78A8",
      mid = "#F7F7F7",
      high = "#B64B4B",
      midpoint = 0,
      breaks = c(-1, 0, 1, 2),
      name = "1990 Homeownership\nExposure (z-score)",
      guide = guide_colorbar(
        title.position = "top",
        title.hjust = 0,
        barheight = grid::unit(45, "pt"),
        barwidth = grid::unit(9, "pt")
      )
    ) +
    coord_sf(datum = NA, expand = FALSE) +
    labs(
      title = "1990 homeownership exposure",
      subtitle = "Within-borough standardized 2010 council-district homeownership, 1990."
    ) +
    theme_void(base_size = 11) +
    theme(
      plot.title = element_text(face = "bold", size = 14, margin = margin(b = 4)),
      plot.subtitle = element_text(size = 10.5, color = "grey30", margin = margin(b = 8)),
      plot.margin = margin(12, 16, 12, 16),
      legend.title = element_text(size = 9.5, lineheight = 0.95),
      legend.text = element_text(size = 8.5),
      legend.margin = margin(l = 8)
    )
)
dev.off()

cat("Wrote 2010 Council district homeownership measure outputs to ../output\n")
