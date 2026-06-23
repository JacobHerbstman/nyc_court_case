# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_ccd2010_mappluto_construction_proxy/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(sf)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

council_measure <- read_csv("../input/ccdist2010_homeownership_1990_measure.csv", show_col_types = FALSE, na = c("", "NA"))

standard_ccd <- council_measure %>%
  transmute(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    council_district = suppressWarnings(as.integer(council_district)),
    borough_code = as.character(borough_code),
    borough_name = borough_name
  ) %>%
  distinct()

if (anyDuplicated(standard_ccd$district_id)) {
  stop("Council district treatment input is not unique by district_id.")
}

council_sf <- council_measure %>%
  transmute(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    council_district = suppressWarnings(as.integer(council_district)),
    borough_code = as.character(borough_code),
    borough_name = borough_name,
    geometry = st_as_sfc(geometry_wkt, crs = 2263)
  ) %>%
  st_as_sf() %>%
  arrange(council_district)

normalize_text_field <- function(x) {
  out <- trimws(as.character(x))
  out[out %in% c("", "NA", "N/A", "NULL")] <- NA_character_
  out
}

normalize_integer_field <- function(x) {
  suppressWarnings(as.integer(normalize_text_field(x)))
}

normalize_numeric_field <- function(x) {
  suppressWarnings(as.numeric(normalize_text_field(x)))
}

normalize_year_field <- function(x) {
  x_int <- normalize_integer_field(x)
  x_int[x_int == 0L] <- NA_integer_
  x_int[x_int < 1800L] <- NA_integer_
  x_int
}

read_mappluto_sf <- function(raw_path) {
  if (!str_detect(tolower(raw_path), "[.]zip$")) {
    return(st_read(raw_path, quiet = TRUE, stringsAsFactors = FALSE))
  }

  temp_dir <- tempfile(pattern = "mappluto_sf_")
  dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  unzip_status <- suppressWarnings(unzip(raw_path, exdir = temp_dir))
  shp_path <- list.files(temp_dir, pattern = "[.]shp$", recursive = TRUE, full.names = TRUE)[1]

  if (is.na(shp_path) || !nzchar(shp_path)) {
    stop("No shapefile found in ", raw_path)
  }

  st_read(shp_path, quiet = TRUE, stringsAsFactors = FALSE)
}

mappluto_row <- read_csv("../input/mappluto_files.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  filter(
    source_id == "dcp_mappluto_current",
    vintage == "25v4",
    file_role == "mappluto_shapefile_zip",
    status %in% c("downloaded", "already_present", "redownloaded_after_validation_failure"),
    !is.na(raw_path),
    file.exists(raw_path)
  ) %>%
  slice_head(n = 1)

if (nrow(mappluto_row) == 0) {
  stop("Could not find current 25v4 MapPLUTO shapefile zip in ../input/mappluto_files.csv")
}

mappluto_raw_sf <- read_mappluto_sf(mappluto_row$raw_path[[1]])

mappluto_attr <- mappluto_raw_sf %>%
  st_drop_geometry() %>%
  as_tibble()
names(mappluto_attr) <- normalize_names(names(mappluto_attr))

jia_codes <- c(164L, 226L, 227L, 228L, 355L, 356L, 480L, 481L, 482L, 483L, 484L, 595L)

mappluto_attr <- mappluto_attr %>%
  transmute(
    row_id = row_number(),
    bbl = coalesce_character(
      normalize_text_field(pick_first_existing(pick(everything()), c("bbl"))),
      build_bbl(
        pick_first_existing(pick(everything()), c("borough", "boro_code", "borocode")),
        pick_first_existing(pick(everything()), c("block")),
        pick_first_existing(pick(everything()), c("lot"))
      )
    ),
    address = normalize_text_field(pick_first_existing(pick(everything()), c("address"))),
    cd = standardize_community_district(
      pick_first_existing(pick(everything()), c("borough", "boro_code", "borocode")),
      pick_first_existing(pick(everything()), c("cd"))
    ),
    yearbuilt = normalize_year_field(pick_first_existing(pick(everything()), c("yearbuilt"))),
    unitsres = coalesce(normalize_numeric_field(pick_first_existing(pick(everything()), c("unitsres"))), 0),
    unitstotal = coalesce(normalize_numeric_field(pick_first_existing(pick(everything()), c("unitstotal"))), 0),
    resarea = coalesce(normalize_numeric_field(pick_first_existing(pick(everything()), c("resarea"))), 0),
    bldgarea = coalesce(normalize_numeric_field(pick_first_existing(pick(everything()), c("bldgarea"))), 0),
    lotarea = coalesce(normalize_numeric_field(pick_first_existing(pick(everything()), c("lotarea"))), 0),
    builtfar = normalize_numeric_field(pick_first_existing(pick(everything()), c("builtfar"))),
    numbldgs = normalize_integer_field(pick_first_existing(pick(everything()), c("numbldgs"))),
    numfloors = normalize_numeric_field(pick_first_existing(pick(everything()), c("numfloors"))),
    landuse = normalize_text_field(pick_first_existing(pick(everything()), c("landuse"))),
    bldgclass = normalize_text_field(pick_first_existing(pick(everything()), c("bldgclass"))),
    is_joint_interest_area = cd %in% jia_codes
  )

mappluto_points <- st_sf(
  row_id = seq_len(nrow(mappluto_raw_sf)),
  geometry = st_point_on_surface(st_geometry(mappluto_raw_sf)),
  crs = st_crs(mappluto_raw_sf)
)

if (is.na(st_crs(mappluto_points))) {
  st_crs(mappluto_points) <- st_crs(council_sf)
}

mappluto_points <- st_transform(mappluto_points, st_crs(council_sf))
district_hits <- st_intersects(mappluto_points, council_sf)
assigned_flag <- lengths(district_hits) > 0
assigned_row <- vapply(district_hits[assigned_flag], function(x) x[[1]], integer(1))

mappluto_assignment <- tibble(
  row_id = which(assigned_flag),
  council_row = assigned_row,
  council_match_count = lengths(district_hits)[assigned_flag]
) %>%
  bind_cols(
    council_sf %>%
      st_drop_geometry() %>%
      slice(assigned_row) %>%
      select(district_id, council_district, borough_code, borough_name)
  )

mappluto_df <- mappluto_attr %>%
  inner_join(mappluto_assignment, by = "row_id", relationship = "one-to-one") %>%
  select(-row_id, -council_row)

bbl_lookup <- mappluto_df %>%
  filter(!is_joint_interest_area, !is.na(bbl), !is.na(council_district)) %>%
  count(bbl, district_id, council_district, name = "mappluto_lot_rows") %>%
  group_by(bbl) %>%
  arrange(desc(mappluto_lot_rows), district_id) %>%
  slice_head(n = 1) %>%
  ungroup()

write_parquet_if_changed(bbl_lookup, "../output/ccdist2010_mappluto_bbl_lookup.parquet")

lot_level <- mappluto_df %>%
  filter(
    !is_joint_interest_area,
    !is.na(council_district),
    yearbuilt >= 1970,
    yearbuilt <= 2025,
    unitsres > 0
  ) %>%
  mutate(
    residential_only_flag = unitstotal == unitsres,
    mixed_use_flag = unitstotal > unitsres,
    size_bin = case_when(
      unitsres >= 1 & unitsres <= 2 ~ "1_2",
      unitsres >= 3 & unitsres <= 4 ~ "3_4",
      unitsres >= 5 & unitsres <= 9 ~ "5_9",
      unitsres >= 10 & unitsres <= 49 ~ "10_49",
      unitsres >= 50 ~ "50_plus",
      TRUE ~ NA_character_
    )
  ) %>%
  select(
    bbl, address, district_id, council_district, borough_code, borough_name, yearbuilt, unitsres, unitstotal,
    resarea, bldgarea, lotarea, builtfar, numbldgs, numfloors, landuse, bldgclass,
    residential_only_flag, mixed_use_flag, size_bin
  )

write_parquet_if_changed(lot_level, "../output/ccdist2010_mappluto_construction_proxy_lot_level.parquet")

panel_base <- lot_level %>%
  group_by(district_id, council_district, borough_code, borough_name, yearbuilt) %>%
  summarise(
    residential_lot_count_proxy = n(),
    residential_only_lot_count_proxy = sum(residential_only_flag, na.rm = TRUE),
    mixed_use_lot_count_proxy = sum(mixed_use_flag, na.rm = TRUE),
    residential_units_proxy = sum(unitsres, na.rm = TRUE),
    total_units_proxy = sum(unitstotal, na.rm = TRUE),
    resarea_proxy = sum(resarea, na.rm = TRUE),
    bldgarea_proxy = sum(bldgarea, na.rm = TRUE),
    lots_1_2_proxy = sum(size_bin == "1_2", na.rm = TRUE),
    lots_3_4_proxy = sum(size_bin == "3_4", na.rm = TRUE),
    lots_5_9_proxy = sum(size_bin == "5_9", na.rm = TRUE),
    lots_10_49_proxy = sum(size_bin == "10_49", na.rm = TRUE),
    lots_50_plus_proxy = sum(size_bin == "50_plus", na.rm = TRUE),
    units_1_2_proxy = sum(if_else(size_bin == "1_2", unitsres, 0), na.rm = TRUE),
    units_3_4_proxy = sum(if_else(size_bin == "3_4", unitsres, 0), na.rm = TRUE),
    units_5_9_proxy = sum(if_else(size_bin == "5_9", unitsres, 0), na.rm = TRUE),
    units_10_49_proxy = sum(if_else(size_bin == "10_49", unitsres, 0), na.rm = TRUE),
    units_50_plus_proxy = sum(if_else(size_bin == "50_plus", unitsres, 0), na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    lots_1_4_proxy = lots_1_2_proxy + lots_3_4_proxy,
    lots_5_plus_proxy = lots_5_9_proxy + lots_10_49_proxy + lots_50_plus_proxy,
    units_1_4_proxy = units_1_2_proxy + units_3_4_proxy,
    units_5_plus_proxy = units_5_9_proxy + units_10_49_proxy + units_50_plus_proxy
  )

panel <- expand_grid(
  standard_ccd,
  yearbuilt = 1970:2025
) %>%
  left_join(panel_base, by = c("district_id", "council_district", "borough_code", "borough_name", "yearbuilt"), relationship = "one-to-one") %>%
  mutate(
    residential_lot_count_proxy = coalesce(residential_lot_count_proxy, 0),
    residential_only_lot_count_proxy = coalesce(residential_only_lot_count_proxy, 0),
    mixed_use_lot_count_proxy = coalesce(mixed_use_lot_count_proxy, 0),
    residential_units_proxy = coalesce(residential_units_proxy, 0),
    total_units_proxy = coalesce(total_units_proxy, 0),
    resarea_proxy = coalesce(resarea_proxy, 0),
    bldgarea_proxy = coalesce(bldgarea_proxy, 0),
    lots_1_2_proxy = coalesce(lots_1_2_proxy, 0),
    lots_3_4_proxy = coalesce(lots_3_4_proxy, 0),
    lots_5_9_proxy = coalesce(lots_5_9_proxy, 0),
    lots_10_49_proxy = coalesce(lots_10_49_proxy, 0),
    lots_50_plus_proxy = coalesce(lots_50_plus_proxy, 0),
    units_1_2_proxy = coalesce(units_1_2_proxy, 0),
    units_3_4_proxy = coalesce(units_3_4_proxy, 0),
    units_5_9_proxy = coalesce(units_5_9_proxy, 0),
    units_10_49_proxy = coalesce(units_10_49_proxy, 0),
    units_50_plus_proxy = coalesce(units_50_plus_proxy, 0),
    lots_1_4_proxy = coalesce(lots_1_4_proxy, 0),
    lots_5_plus_proxy = coalesce(lots_5_plus_proxy, 0),
    units_1_4_proxy = coalesce(units_1_4_proxy, 0),
    units_5_plus_proxy = coalesce(units_5_plus_proxy, 0)
  ) %>%
  arrange(council_district, yearbuilt)

write_csv_if_changed(panel, "../output/ccdist2010_mappluto_construction_proxy_district_year.csv")

cat("Wrote 2010 Council district MapPLUTO construction proxy outputs to ../output\n")
