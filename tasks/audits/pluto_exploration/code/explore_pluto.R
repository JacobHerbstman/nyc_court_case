# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/pluto_exploration/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(janitor)
  library(readr)
  library(sf)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

options(scipen = 999)
sf_use_s2(FALSE)

jia_codes <- c(164L, 226L, 227L, 228L, 355L, 356L, 480L, 481L, 482L, 483L, 484L, 595L)
min_plausible_year_built <- 1800L
proxy_start_year <- 1980L
proxy_end_year <- 2025L

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

mappluto_temp_dir <- tempfile(pattern = "pluto_exploration_mappluto_")
dir.create(mappluto_temp_dir, recursive = TRUE, showWarnings = FALSE)
unzip("../input/nyc_mappluto_25v4_shp.zip", exdir = mappluto_temp_dir)
mappluto_shp_path <- list.files(mappluto_temp_dir, pattern = "[.]shp$", recursive = TRUE, full.names = TRUE)[1]

if (is.na(mappluto_shp_path) || !nzchar(mappluto_shp_path)) {
  stop("No shapefile found in ../input/nyc_mappluto_25v4_shp.zip")
}

raw_mappluto_sf <- st_read(mappluto_shp_path, quiet = TRUE, stringsAsFactors = FALSE)
raw_mappluto_attr <- raw_mappluto_sf %>%
  st_drop_geometry() %>%
  as_tibble() %>%
  clean_names()

mappluto_borough_raw <- pick_first_existing(raw_mappluto_attr, c("boro_code", "borocode", "borough"))
mappluto_block_raw <- pick_first_existing(raw_mappluto_attr, c("block"))
mappluto_lot_raw <- pick_first_existing(raw_mappluto_attr, c("lot"))
mappluto_bbl_raw <- str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("bbl"))))
mappluto_bbl_raw[mappluto_bbl_raw %in% c("", "NA", "N/A", "NULL", "0")] <- NA_character_
mappluto_bbl_numeric <- suppressWarnings(as.numeric(mappluto_bbl_raw))
mappluto_bbl_from_field <- rep(NA_character_, length(mappluto_bbl_raw))
mappluto_bbl_numeric_flag <- !is.na(mappluto_bbl_numeric) & mappluto_bbl_numeric > 0
mappluto_bbl_from_field[mappluto_bbl_numeric_flag] <- sprintf("%010.0f", mappluto_bbl_numeric[mappluto_bbl_numeric_flag])
mappluto_bbl_digit_flag <- is.na(mappluto_bbl_from_field) & !is.na(mappluto_bbl_raw)
mappluto_bbl_digits <- str_replace_all(mappluto_bbl_raw, "[^0-9]", "")
mappluto_bbl_from_field[mappluto_bbl_digit_flag & nchar(mappluto_bbl_digits) > 0] <- str_pad(mappluto_bbl_digits[mappluto_bbl_digit_flag & nchar(mappluto_bbl_digits) > 0], width = 10, side = "left", pad = "0")
mappluto_bbl_invalid <- !is.na(mappluto_bbl_from_field) & (nchar(mappluto_bbl_from_field) != 10 | mappluto_bbl_from_field == "0000000000")
mappluto_bbl_from_field[mappluto_bbl_invalid] <- NA_character_

mappluto_appbbl_raw <- str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("appbbl"))))
mappluto_appbbl_raw[mappluto_appbbl_raw %in% c("", "NA", "N/A", "NULL", "0")] <- NA_character_
mappluto_appbbl_numeric <- suppressWarnings(as.numeric(mappluto_appbbl_raw))
mappluto_appbbl <- rep(NA_character_, length(mappluto_appbbl_raw))
mappluto_appbbl_numeric_flag <- !is.na(mappluto_appbbl_numeric) & mappluto_appbbl_numeric > 0
mappluto_appbbl[mappluto_appbbl_numeric_flag] <- sprintf("%010.0f", mappluto_appbbl_numeric[mappluto_appbbl_numeric_flag])
mappluto_appbbl_digit_flag <- is.na(mappluto_appbbl) & !is.na(mappluto_appbbl_raw)
mappluto_appbbl_digits <- str_replace_all(mappluto_appbbl_raw, "[^0-9]", "")
mappluto_appbbl[mappluto_appbbl_digit_flag & nchar(mappluto_appbbl_digits) > 0] <- str_pad(mappluto_appbbl_digits[mappluto_appbbl_digit_flag & nchar(mappluto_appbbl_digits) > 0], width = 10, side = "left", pad = "0")
mappluto_appbbl_invalid <- !is.na(mappluto_appbbl) & (nchar(mappluto_appbbl) != 10 | mappluto_appbbl == "0000000000")
mappluto_appbbl[mappluto_appbbl_invalid] <- NA_character_

raw_mappluto <- raw_mappluto_attr %>%
  transmute(
    source_sample = "raw_mappluto_25v4_shapefile",
    source_row_id = row_number(),
    borough = standardize_borough_code(mappluto_borough_raw),
    block = suppressWarnings(as.integer(str_squish(as.character(mappluto_block_raw)))),
    lot = suppressWarnings(as.integer(str_squish(as.character(mappluto_lot_raw)))),
    bbl = coalesce_character(mappluto_bbl_from_field, build_bbl(mappluto_borough_raw, mappluto_block_raw, mappluto_lot_raw)),
    cd = standardize_community_district(mappluto_borough_raw, pick_first_existing(raw_mappluto_attr, c("cd"))),
    raw_current_council = standardize_council_district(pick_first_existing(raw_mappluto_attr, c("council"))),
    address = str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("address")))),
    year_built = suppressWarnings(as.integer(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("year_built")))))),
    year_alter1 = suppressWarnings(as.integer(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("year_alter1")))))),
    year_alter2 = suppressWarnings(as.integer(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("year_alter2")))))),
    units_res = coalesce(suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("units_res")))))), 0),
    units_total = coalesce(suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("units_total")))))), 0),
    lot_area = coalesce(suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("lot_area")))))), 0),
    bldg_area = coalesce(suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("bldg_area")))))), 0),
    res_area = coalesce(suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("res_area")))))), 0),
    built_far = suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("built_far")))))),
    num_bldgs = suppressWarnings(as.integer(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("num_bldgs")))))),
    num_floors = suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("num_floors")))))),
    land_use = str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("land_use")))),
    bldg_class = str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("bldg_class")))),
    condo_no = suppressWarnings(as.integer(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("condo_no")))))),
    appbbl = mappluto_appbbl,
    app_date = parse_mixed_date(pick_first_existing(raw_mappluto_attr, c("app_date"))),
    pluto_map_id = suppressWarnings(as.integer(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("pluto_map_id")))))),
    dcp_edited = str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("dcp_edited")))),
    latitude = suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("latitude")))))),
    longitude = suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("longitude")))))),
    x_coord = suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("x_coord")))))),
    y_coord = suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("y_coord")))))),
    version = str_squish(as.character(pick_first_existing(raw_mappluto_attr, c("version"))))
  ) %>%
  mutate(
    across(c(address, land_use, bldg_class, dcp_edited, version), ~ na_if(.x, "")),
    across(c(address, land_use, bldg_class, dcp_edited, version), ~ na_if(.x, "NA")),
    across(c(address, land_use, bldg_class, dcp_edited, version), ~ na_if(.x, "N/A")),
    across(c(address, land_use, bldg_class, dcp_edited, version), ~ na_if(.x, "NULL")),
    year_built = case_when(is.na(year_built) ~ NA_integer_, year_built == 0L ~ NA_integer_, year_built < min_plausible_year_built ~ NA_integer_, TRUE ~ year_built),
    year_alter1 = case_when(is.na(year_alter1) ~ NA_integer_, year_alter1 == 0L ~ NA_integer_, year_alter1 < min_plausible_year_built ~ NA_integer_, TRUE ~ year_alter1),
    year_alter2 = case_when(is.na(year_alter2) ~ NA_integer_, year_alter2 == 0L ~ NA_integer_, year_alter2 < min_plausible_year_built ~ NA_integer_, TRUE ~ year_alter2),
    condo_no = case_when(is.na(condo_no) ~ NA_integer_, condo_no == 0L ~ NA_integer_, TRUE ~ condo_no),
    is_joint_interest_area = cd %in% jia_codes,
    bbl_lot_number = suppressWarnings(as.integer(str_sub(bbl, -4))),
    condo_lot_range = case_when(
      !is.na(bbl_lot_number) & bbl_lot_number >= 7501L & bbl_lot_number <= 7599L ~ "7501_7599_billing_lot_range",
      !is.na(bbl_lot_number) & bbl_lot_number >= 1001L & bbl_lot_number <= 6999L ~ "1001_6999_unit_lot_range",
      !is.na(bbl_lot_number) ~ "other_lot_range",
      TRUE ~ NA_character_
    ),
    condo_no_present = !is.na(condo_no),
    bldg_class_prefix = str_sub(str_to_upper(bldg_class), 1, 1),
    residential_positive = units_res > 0,
    residential_proxy_candidate = year_built >= proxy_start_year & year_built <= proxy_end_year & units_res > 0,
    units_total_lt_units_res = units_total < units_res,
    residential_only_flag = units_total == units_res,
    mixed_use_flag = units_total > units_res,
    multi_building_flag = !is.na(num_bldgs) & num_bldgs > 1L,
    size_bin = case_when(
      units_res >= 1 & units_res <= 2 ~ "1_2",
      units_res >= 3 & units_res <= 4 ~ "3_4",
      units_res >= 5 & units_res <= 9 ~ "5_9",
      units_res >= 10 & units_res <= 49 ~ "10_49",
      units_res >= 50 ~ "50_plus",
      TRUE ~ NA_character_
    )
  )

pluto_zip_index <- unzip("../input/nyc_pluto_25v4_csv.zip", list = TRUE)
pluto_csv_name <- pluto_zip_index$Name[str_detect(tolower(pluto_zip_index$Name), "[.]csv$")][1]

if (is.na(pluto_csv_name) || !nzchar(pluto_csv_name)) {
  stop("No CSV found in ../input/nyc_pluto_25v4_csv.zip")
}

raw_pluto_attr <- read_csv(
  unz("../input/nyc_pluto_25v4_csv.zip", pluto_csv_name),
  col_types = cols(.default = col_character()),
  show_col_types = FALSE,
  na = c("", "NA", "N/A", "NULL")
)
raw_pluto_attr <- raw_pluto_attr %>%
  clean_names()

pluto_borough_raw <- pick_first_existing(raw_pluto_attr, c("boro_code", "borocode", "borough"))
pluto_block_raw <- pick_first_existing(raw_pluto_attr, c("block"))
pluto_lot_raw <- pick_first_existing(raw_pluto_attr, c("lot"))
pluto_bbl_raw <- str_squish(as.character(pick_first_existing(raw_pluto_attr, c("bbl"))))
pluto_bbl_raw[pluto_bbl_raw %in% c("", "NA", "N/A", "NULL", "0")] <- NA_character_
pluto_bbl_numeric <- suppressWarnings(as.numeric(pluto_bbl_raw))
pluto_bbl_from_field <- rep(NA_character_, length(pluto_bbl_raw))
pluto_bbl_numeric_flag <- !is.na(pluto_bbl_numeric) & pluto_bbl_numeric > 0
pluto_bbl_from_field[pluto_bbl_numeric_flag] <- sprintf("%010.0f", pluto_bbl_numeric[pluto_bbl_numeric_flag])
pluto_bbl_digit_flag <- is.na(pluto_bbl_from_field) & !is.na(pluto_bbl_raw)
pluto_bbl_digits <- str_replace_all(pluto_bbl_raw, "[^0-9]", "")
pluto_bbl_from_field[pluto_bbl_digit_flag & nchar(pluto_bbl_digits) > 0] <- str_pad(pluto_bbl_digits[pluto_bbl_digit_flag & nchar(pluto_bbl_digits) > 0], width = 10, side = "left", pad = "0")
pluto_bbl_invalid <- !is.na(pluto_bbl_from_field) & (nchar(pluto_bbl_from_field) != 10 | pluto_bbl_from_field == "0000000000")
pluto_bbl_from_field[pluto_bbl_invalid] <- NA_character_

pluto_appbbl_raw <- str_squish(as.character(pick_first_existing(raw_pluto_attr, c("appbbl"))))
pluto_appbbl_raw[pluto_appbbl_raw %in% c("", "NA", "N/A", "NULL", "0")] <- NA_character_
pluto_appbbl_numeric <- suppressWarnings(as.numeric(pluto_appbbl_raw))
pluto_appbbl <- rep(NA_character_, length(pluto_appbbl_raw))
pluto_appbbl_numeric_flag <- !is.na(pluto_appbbl_numeric) & pluto_appbbl_numeric > 0
pluto_appbbl[pluto_appbbl_numeric_flag] <- sprintf("%010.0f", pluto_appbbl_numeric[pluto_appbbl_numeric_flag])
pluto_appbbl_digit_flag <- is.na(pluto_appbbl) & !is.na(pluto_appbbl_raw)
pluto_appbbl_digits <- str_replace_all(pluto_appbbl_raw, "[^0-9]", "")
pluto_appbbl[pluto_appbbl_digit_flag & nchar(pluto_appbbl_digits) > 0] <- str_pad(pluto_appbbl_digits[pluto_appbbl_digit_flag & nchar(pluto_appbbl_digits) > 0], width = 10, side = "left", pad = "0")
pluto_appbbl_invalid <- !is.na(pluto_appbbl) & (nchar(pluto_appbbl) != 10 | pluto_appbbl == "0000000000")
pluto_appbbl[pluto_appbbl_invalid] <- NA_character_

raw_pluto <- raw_pluto_attr %>%
  transmute(
    source_sample = "raw_pluto_25v4_csv",
    source_row_id = row_number(),
    borough = standardize_borough_code(pluto_borough_raw),
    block = suppressWarnings(as.integer(str_squish(as.character(pluto_block_raw)))),
    lot = suppressWarnings(as.integer(str_squish(as.character(pluto_lot_raw)))),
    bbl = coalesce_character(pluto_bbl_from_field, build_bbl(pluto_borough_raw, pluto_block_raw, pluto_lot_raw)),
    cd = standardize_community_district(pluto_borough_raw, pick_first_existing(raw_pluto_attr, c("cd"))),
    raw_current_council = standardize_council_district(pick_first_existing(raw_pluto_attr, c("council"))),
    address = str_squish(as.character(pick_first_existing(raw_pluto_attr, c("address")))),
    year_built = suppressWarnings(as.integer(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("yearbuilt")))))),
    year_alter1 = suppressWarnings(as.integer(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("yearalter1")))))),
    year_alter2 = suppressWarnings(as.integer(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("yearalter2")))))),
    units_res = coalesce(suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("unitsres")))))), 0),
    units_total = coalesce(suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("unitstotal")))))), 0),
    lot_area = coalesce(suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("lotarea")))))), 0),
    bldg_area = coalesce(suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("bldgarea")))))), 0),
    res_area = coalesce(suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("resarea")))))), 0),
    built_far = suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("builtfar")))))),
    num_bldgs = suppressWarnings(as.integer(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("numbldgs")))))),
    num_floors = suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("numfloors")))))),
    land_use = str_squish(as.character(pick_first_existing(raw_pluto_attr, c("landuse")))),
    bldg_class = str_squish(as.character(pick_first_existing(raw_pluto_attr, c("bldgclass")))),
    condo_no = suppressWarnings(as.integer(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("condono")))))),
    appbbl = pluto_appbbl,
    app_date = parse_mixed_date(pick_first_existing(raw_pluto_attr, c("appdate"))),
    pluto_map_id = suppressWarnings(as.integer(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("plutomapid")))))),
    dcp_edited = str_squish(as.character(pick_first_existing(raw_pluto_attr, c("dcpedited")))),
    latitude = suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("latitude")))))),
    longitude = suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("longitude")))))),
    x_coord = suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("xcoord")))))),
    y_coord = suppressWarnings(as.numeric(str_squish(as.character(pick_first_existing(raw_pluto_attr, c("ycoord")))))),
    version = str_squish(as.character(pick_first_existing(raw_pluto_attr, c("version"))))
  ) %>%
  mutate(
    across(c(address, land_use, bldg_class, dcp_edited, version), ~ na_if(.x, "")),
    across(c(address, land_use, bldg_class, dcp_edited, version), ~ na_if(.x, "NA")),
    across(c(address, land_use, bldg_class, dcp_edited, version), ~ na_if(.x, "N/A")),
    across(c(address, land_use, bldg_class, dcp_edited, version), ~ na_if(.x, "NULL")),
    year_built = case_when(is.na(year_built) ~ NA_integer_, year_built == 0L ~ NA_integer_, year_built < min_plausible_year_built ~ NA_integer_, TRUE ~ year_built),
    year_alter1 = case_when(is.na(year_alter1) ~ NA_integer_, year_alter1 == 0L ~ NA_integer_, year_alter1 < min_plausible_year_built ~ NA_integer_, TRUE ~ year_alter1),
    year_alter2 = case_when(is.na(year_alter2) ~ NA_integer_, year_alter2 == 0L ~ NA_integer_, year_alter2 < min_plausible_year_built ~ NA_integer_, TRUE ~ year_alter2),
    condo_no = case_when(is.na(condo_no) ~ NA_integer_, condo_no == 0L ~ NA_integer_, TRUE ~ condo_no),
    is_joint_interest_area = cd %in% jia_codes,
    bbl_lot_number = suppressWarnings(as.integer(str_sub(bbl, -4))),
    condo_lot_range = case_when(
      !is.na(bbl_lot_number) & bbl_lot_number >= 7501L & bbl_lot_number <= 7599L ~ "7501_7599_billing_lot_range",
      !is.na(bbl_lot_number) & bbl_lot_number >= 1001L & bbl_lot_number <= 6999L ~ "1001_6999_unit_lot_range",
      !is.na(bbl_lot_number) ~ "other_lot_range",
      TRUE ~ NA_character_
    ),
    condo_no_present = !is.na(condo_no),
    bldg_class_prefix = str_sub(str_to_upper(bldg_class), 1, 1),
    residential_positive = units_res > 0,
    residential_proxy_candidate = year_built >= proxy_start_year & year_built <= proxy_end_year & units_res > 0,
    units_total_lt_units_res = units_total < units_res,
    residential_only_flag = units_total == units_res,
    mixed_use_flag = units_total > units_res,
    multi_building_flag = !is.na(num_bldgs) & num_bldgs > 1L,
    size_bin = case_when(
      units_res >= 1 & units_res <= 2 ~ "1_2",
      units_res >= 3 & units_res <= 4 ~ "3_4",
      units_res >= 5 & units_res <= 9 ~ "5_9",
      units_res >= 10 & units_res <= 49 ~ "10_49",
      units_res >= 50 ~ "50_plus",
      TRUE ~ NA_character_
    )
  )

mappluto_points <- st_sf(
  source_row_id = seq_len(nrow(raw_mappluto_sf)),
  geometry = st_point_on_surface(st_geometry(raw_mappluto_sf)),
  crs = st_crs(raw_mappluto_sf)
)

if (is.na(st_crs(mappluto_points))) {
  st_crs(mappluto_points) <- st_crs(council_sf)
}

mappluto_points <- st_transform(mappluto_points, st_crs(council_sf))
district_hits <- st_intersects(mappluto_points, council_sf)
council_match_count <- lengths(district_hits)
assigned_council_row <- rep(NA_integer_, length(district_hits))

for (i in seq_along(district_hits)) {
  if (length(district_hits[[i]]) > 0) {
    assigned_council_row[[i]] <- district_hits[[i]][[1]]
  }
}

council_lookup <- council_sf %>%
  st_drop_geometry() %>%
  mutate(council_row = row_number()) %>%
  select(council_row, district_id, council_district, borough_code, borough_name)

mappluto_assignment <- tibble(
  source_row_id = seq_along(district_hits),
  council_row = assigned_council_row,
  council_match_count = council_match_count
) %>%
  filter(!is.na(council_row)) %>%
  left_join(council_lookup, by = "council_row", relationship = "many-to-one")

mappluto_assigned <- raw_mappluto %>%
  inner_join(mappluto_assignment, by = "source_row_id", relationship = "one-to-one") %>%
  select(-council_row) %>%
  mutate(source_sample = "rebuilt_mappluto_assigned")

rebuilt_bbl_lookup <- mappluto_assigned %>%
  filter(!is_joint_interest_area, !is.na(bbl), !is.na(council_district)) %>%
  count(bbl, district_id, council_district, name = "mappluto_lot_rows") %>%
  group_by(bbl) %>%
  arrange(desc(mappluto_lot_rows), district_id) %>%
  slice_head(n = 1) %>%
  ungroup() %>%
  arrange(bbl)

rebuilt_lot_level <- mappluto_assigned %>%
  filter(
    !is_joint_interest_area,
    !is.na(council_district),
    year_built >= proxy_start_year,
    year_built <= proxy_end_year,
    units_res > 0
  ) %>%
  mutate(source_sample = "rebuilt_figure2_lot_level") %>%
  select(
    source_sample, bbl, address, district_id, council_district, borough_code, borough_name,
    borough, block, lot, cd, raw_current_council, council_match_count,
    year_built, year_alter1, year_alter2, units_res, units_total,
    res_area, bldg_area, lot_area, built_far, num_bldgs, num_floors,
    land_use, bldg_class, bldg_class_prefix, condo_no, condo_no_present, appbbl, app_date,
    pluto_map_id, dcp_edited, latitude, longitude, x_coord, y_coord, version,
    is_joint_interest_area, bbl_lot_number, condo_lot_range, residential_only_flag,
    mixed_use_flag, multi_building_flag, size_bin, units_total_lt_units_res
  ) %>%
  arrange(council_district, year_built, bbl)

panel_base <- rebuilt_lot_level %>%
  group_by(district_id, council_district, borough_code, borough_name, year_built) %>%
  summarise(
    residential_lot_count_proxy = n(),
    residential_only_lot_count_proxy = sum(residential_only_flag, na.rm = TRUE),
    mixed_use_lot_count_proxy = sum(mixed_use_flag, na.rm = TRUE),
    residential_units_proxy = sum(units_res, na.rm = TRUE),
    total_units_proxy = sum(units_total, na.rm = TRUE),
    res_area_proxy = sum(res_area, na.rm = TRUE),
    bldg_area_proxy = sum(bldg_area, na.rm = TRUE),
    lots_1_2_proxy = sum(size_bin == "1_2", na.rm = TRUE),
    lots_3_4_proxy = sum(size_bin == "3_4", na.rm = TRUE),
    lots_5_9_proxy = sum(size_bin == "5_9", na.rm = TRUE),
    lots_10_49_proxy = sum(size_bin == "10_49", na.rm = TRUE),
    lots_50_plus_proxy = sum(size_bin == "50_plus", na.rm = TRUE),
    units_1_2_proxy = sum(if_else(size_bin == "1_2", units_res, 0), na.rm = TRUE),
    units_3_4_proxy = sum(if_else(size_bin == "3_4", units_res, 0), na.rm = TRUE),
    units_5_9_proxy = sum(if_else(size_bin == "5_9", units_res, 0), na.rm = TRUE),
    units_10_49_proxy = sum(if_else(size_bin == "10_49", units_res, 0), na.rm = TRUE),
    units_50_plus_proxy = sum(if_else(size_bin == "50_plus", units_res, 0), na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    lots_1_4_proxy = lots_1_2_proxy + lots_3_4_proxy,
    lots_5_plus_proxy = lots_5_9_proxy + lots_10_49_proxy + lots_50_plus_proxy,
    units_1_4_proxy = units_1_2_proxy + units_3_4_proxy,
    units_5_plus_proxy = units_5_9_proxy + units_10_49_proxy + units_50_plus_proxy
  )

rebuilt_district_year <- expand_grid(
  standard_ccd,
  year_built = proxy_start_year:proxy_end_year
) %>%
  left_join(panel_base, by = c("district_id", "council_district", "borough_code", "borough_name", "year_built"), relationship = "one-to-one") %>%
  mutate(
    residential_lot_count_proxy = coalesce(residential_lot_count_proxy, 0),
    residential_only_lot_count_proxy = coalesce(residential_only_lot_count_proxy, 0),
    mixed_use_lot_count_proxy = coalesce(mixed_use_lot_count_proxy, 0),
    residential_units_proxy = coalesce(residential_units_proxy, 0),
    total_units_proxy = coalesce(total_units_proxy, 0),
    res_area_proxy = coalesce(res_area_proxy, 0),
    bldg_area_proxy = coalesce(bldg_area_proxy, 0),
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
  arrange(council_district, year_built)

mappluto_comparable <- raw_mappluto %>%
  filter(!is_joint_interest_area, year_built >= proxy_start_year, year_built <= proxy_end_year, units_res > 0, !is.na(bbl))

pluto_comparable <- raw_pluto %>%
  filter(!is_joint_interest_area, year_built >= proxy_start_year, year_built <= proxy_end_year, units_res > 0, !is.na(bbl))

pluto_only_residential_lots <- pluto_comparable %>%
  anti_join(raw_mappluto %>% filter(!is.na(bbl)) %>% distinct(bbl), by = "bbl") %>%
  select(
    source_sample, bbl, address, borough, block, lot, cd, raw_current_council,
    year_built, units_res, units_total, res_area, bldg_area, lot_area, built_far,
    num_bldgs, num_floors, land_use, bldg_class, bldg_class_prefix, condo_no,
    condo_no_present, appbbl, pluto_map_id, dcp_edited, latitude, longitude,
    x_coord, y_coord, version, bbl_lot_number, condo_lot_range, residential_only_flag,
    mixed_use_flag, multi_building_flag, size_bin, units_total_lt_units_res
  ) %>%
  arrange(desc(units_res), year_built, bbl)

pluto_mappluto_overlap <- bind_rows(
  tibble(metric = "raw_mappluto_rows", value = nrow(raw_mappluto), note = "Rows read directly from the raw MapPLUTO 25v4 shapefile zip."),
  tibble(metric = "raw_pluto_rows", value = nrow(raw_pluto), note = "Rows read directly from the raw PLUTO 25v4 CSV zip."),
  tibble(metric = "raw_mappluto_distinct_bbl", value = n_distinct(raw_mappluto$bbl, na.rm = TRUE), note = "Distinct nonmissing BBL values in raw MapPLUTO after canonical BBL construction."),
  tibble(metric = "raw_pluto_distinct_bbl", value = n_distinct(raw_pluto$bbl, na.rm = TRUE), note = "Distinct nonmissing BBL values in raw PLUTO after canonical BBL construction."),
  tibble(metric = "mappluto_residential_1980_2025_rows", value = nrow(mappluto_comparable), note = "Raw MapPLUTO rows with non-JIA CD, YearBuilt 1980-2025, positive UnitsRes, and BBL."),
  tibble(metric = "mappluto_residential_1980_2025_units", value = sum(mappluto_comparable$units_res, na.rm = TRUE), note = "UnitsRes on comparable raw MapPLUTO rows."),
  tibble(metric = "pluto_residential_1980_2025_rows", value = nrow(pluto_comparable), note = "Raw PLUTO CSV rows with non-JIA CD, YearBuilt 1980-2025, positive UnitsRes, and BBL."),
  tibble(metric = "pluto_residential_1980_2025_units", value = sum(pluto_comparable$units_res, na.rm = TRUE), note = "UnitsRes on comparable raw PLUTO CSV rows."),
  tibble(metric = "pluto_only_residential_1980_2025_rows", value = nrow(pluto_only_residential_lots), note = "Comparable raw PLUTO rows whose BBL is not present in raw MapPLUTO."),
  tibble(metric = "pluto_only_residential_1980_2025_units", value = sum(pluto_only_residential_lots$units_res, na.rm = TRUE), note = "UnitsRes on comparable raw PLUTO rows whose BBL is not present in raw MapPLUTO."),
  tibble(metric = "mappluto_only_residential_1980_2025_rows", value = nrow(anti_join(mappluto_comparable, raw_pluto %>% filter(!is.na(bbl)) %>% distinct(bbl), by = "bbl")), note = "Comparable raw MapPLUTO rows whose BBL is not present in raw PLUTO CSV."),
  tibble(metric = "mappluto_only_residential_1980_2025_units", value = sum(anti_join(mappluto_comparable, raw_pluto %>% filter(!is.na(bbl)) %>% distinct(bbl), by = "bbl")$units_res, na.rm = TRUE), note = "UnitsRes on comparable raw MapPLUTO rows whose BBL is not present in raw PLUTO CSV.")
)

key_columns_dictionary <- tribble(
  ~column, ~primary_source, ~used_for, ~assumption_under_review, ~diagnostic_outputs,
  "BBL", "MapPLUTO shapefile and PLUTO CSV", "Lot identifier and PLUTO/MapPLUTO overlap.", "BBL is a 10-digit tax-lot key; raw numeric/scientific formatting is safely normalized before use.", "column_profile; pluto_mappluto_overlap; outlier_lots",
  "Borough/Block/Lot", "MapPLUTO shapefile and PLUTO CSV", "Fallback BBL construction and lot-range classification.", "Borough, block, and lot identify the tax lot; lot number ranges signal possible condo billing or unit lots.", "column_profile; condo_profile; condo_suspect_groups",
  "CD", "MapPLUTO shapefile and PLUTO CSV", "Joint-interest-area exclusion.", "DCP community district codes in the JIA list are not ordinary district observations.", "proxy_filter_steps; assumption_checks",
  "Council", "MapPLUTO shapefile and PLUTO CSV", "Diagnostic only.", "Current PLUTO Council is not the analysis geography; Figure 2 uses archived 2010 districts from spatial assignment.", "column_profile",
  "geometry", "MapPLUTO shapefile", "Assign lots to 2010 Council districts by representative point.", "The representative point falls in the intended archived 2010 Council district; ties are rare and first district in district order is used.", "assumption_checks; district_year_sanity",
  "YearBuilt", "MapPLUTO shapefile and PLUTO CSV", "Proxy year for surviving residential stock built in year t.", "Zero or pre-1800 values are invalid; 1980-2025 are the Figure 2 years.", "year_built_profile; proxy_filter_steps; outlier_lots",
  "UnitsRes", "MapPLUTO shapefile and PLUTO CSV", "Residential-unit outcome and size-bin construction.", "Positive UnitsRes means the lot contributes to the residential construction proxy; very large values need review.", "numeric_quantiles; outlier_lots; district_year_sanity",
  "UnitsTotal", "MapPLUTO shapefile and PLUTO CSV", "Residential-only and mixed-use flags.", "UnitsTotal should usually be at least UnitsRes.", "assumption_checks; outlier_lots",
  "LotArea/BldgArea/ResArea/BuiltFAR", "MapPLUTO shapefile and PLUTO CSV", "Outlier and plausibility checks.", "Area and FAR variables should be nonnegative and plausible for lots in the proxy.", "numeric_quantiles; outlier_lots",
  "NumBldgs/NumFloors", "MapPLUTO shapefile and PLUTO CSV", "Building-level interpretation checks.", "A lot with multiple buildings weakens a literal building-level reading of size bins.", "condo_profile; assumption_checks; outlier_lots",
  "LandUse/BldgClass", "MapPLUTO shapefile and PLUTO CSV", "Residential and building-type plausibility checks.", "Residential-unit rows should have building classes and land uses consistent with housing or mixed use.", "categorical_values; column_profile",
  "CondoNo", "MapPLUTO shapefile and PLUTO CSV", "Condo identification.", "Nonmissing CondoNo identifies recognized condominium records, but condo representation may still vary by tax-lot convention.", "condo_profile; condo_suspect_groups",
  "APPBBL/APPDate/PLUTOMapID/DCPEdited", "MapPLUTO shapefile and PLUTO CSV", "Condo and DCP-edit diagnostics.", "Apartment/condo metadata and PLUTO map flags may reveal records that should not be read as independent buildings.", "condo_suspect_groups; outlier_lots",
  "Version", "MapPLUTO shapefile and PLUTO CSV", "Vintage validation.", "All current raw rows should identify the intended 25v4 PLUTO/MapPLUTO release.", "assumption_checks"
)

audit_columns <- c(
  "source_sample", "source_row_id", "borough", "block", "lot", "bbl", "cd", "raw_current_council",
  "district_id", "council_district", "borough_code", "borough_name", "council_match_count",
  "address", "year_built", "year_alter1", "year_alter2", "units_res", "units_total",
  "lot_area", "bldg_area", "res_area", "built_far", "num_bldgs", "num_floors",
  "land_use", "bldg_class", "bldg_class_prefix", "condo_no", "condo_no_present", "appbbl",
  "app_date", "pluto_map_id", "dcp_edited", "latitude", "longitude", "x_coord", "y_coord",
  "version", "is_joint_interest_area", "bbl_lot_number", "condo_lot_range",
  "residential_positive", "residential_proxy_candidate", "residential_only_flag",
  "mixed_use_flag", "multi_building_flag", "size_bin", "units_total_lt_units_res"
)

audit_datasets <- list(
  raw_mappluto_25v4_shapefile = raw_mappluto,
  raw_pluto_25v4_csv = raw_pluto,
  rebuilt_mappluto_assigned = mappluto_assigned,
  rebuilt_figure2_lot_level = rebuilt_lot_level
)

column_profile_rows <- list()
profile_row <- 1L

for (dataset_name in names(audit_datasets)) {
  dataset <- audit_datasets[[dataset_name]]
  dataset_columns <- intersect(audit_columns, names(dataset))

  for (column_name in dataset_columns) {
    column_character <- str_squish(as.character(dataset[[column_name]]))
    column_character[column_character == ""] <- NA_character_
    column_numeric <- suppressWarnings(as.numeric(column_character))
    nonmissing_flag <- !is.na(column_character)
    numeric_flag <- !is.na(column_numeric)
    numeric_parse_count <- sum(numeric_flag)
    zero_count <- sum(column_numeric == 0, na.rm = TRUE)
    negative_count <- sum(column_numeric < 0, na.rm = TRUE)
    top_table <- sort(table(column_character, useNA = "no"), decreasing = TRUE)

    if (numeric_parse_count == 0) {
      min_numeric <- NA_real_
      max_numeric <- NA_real_
      numeric_parse_share <- NA_real_
    } else {
      min_numeric <- min(column_numeric[numeric_flag], na.rm = TRUE)
      max_numeric <- max(column_numeric[numeric_flag], na.rm = TRUE)
      numeric_parse_share <- numeric_parse_count / nrow(dataset)
    }

    if (length(top_table) == 0) {
      top_values <- NA_character_
    } else {
      top_values <- paste0(names(head(top_table, 8)), "=", as.integer(head(top_table, 8)), collapse = "; ")
    }

    column_profile_rows[[profile_row]] <- tibble(
      source_sample = dataset_name,
      column = column_name,
      row_count = nrow(dataset),
      nonmissing_count = sum(nonmissing_flag),
      nonmissing_share = mean(nonmissing_flag),
      distinct_nonmissing_count = n_distinct(column_character[nonmissing_flag]),
      numeric_parse_count = numeric_parse_count,
      numeric_parse_share = numeric_parse_share,
      zero_count = zero_count,
      negative_count = negative_count,
      min_numeric = min_numeric,
      max_numeric = max_numeric,
      top_values = top_values
    )
    profile_row <- profile_row + 1L
  }
}

column_profile <- bind_rows(column_profile_rows) %>%
  arrange(source_sample, column)

numeric_columns <- c(
  "block", "lot", "cd", "raw_current_council", "council_match_count",
  "year_built", "year_alter1", "year_alter2", "units_res", "units_total",
  "lot_area", "bldg_area", "res_area", "built_far", "num_bldgs", "num_floors",
  "condo_no", "pluto_map_id", "latitude", "longitude", "x_coord", "y_coord", "bbl_lot_number"
)

numeric_quantile_rows <- list()
numeric_row <- 1L

for (dataset_name in names(audit_datasets)) {
  dataset <- audit_datasets[[dataset_name]]
  dataset_columns <- intersect(numeric_columns, names(dataset))

  for (column_name in dataset_columns) {
    numeric_values <- suppressWarnings(as.numeric(as.character(dataset[[column_name]])))
    numeric_values <- numeric_values[!is.na(numeric_values)]

    if (length(numeric_values) > 0) {
      quantile_values <- as.numeric(quantile(numeric_values, probs = c(0, 0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1), na.rm = TRUE, names = FALSE))
      numeric_quantile_rows[[numeric_row]] <- tibble(
        source_sample = dataset_name,
        column = column_name,
        nonmissing_count = length(numeric_values),
        mean = mean(numeric_values, na.rm = TRUE),
        sd = sd(numeric_values, na.rm = TRUE),
        min = quantile_values[[1]],
        p001 = quantile_values[[2]],
        p01 = quantile_values[[3]],
        p05 = quantile_values[[4]],
        p10 = quantile_values[[5]],
        p25 = quantile_values[[6]],
        p50 = quantile_values[[7]],
        p75 = quantile_values[[8]],
        p90 = quantile_values[[9]],
        p95 = quantile_values[[10]],
        p99 = quantile_values[[11]],
        p999 = quantile_values[[12]],
        max = quantile_values[[13]]
      )
      numeric_row <- numeric_row + 1L
    }
  }
}

numeric_quantiles <- bind_rows(numeric_quantile_rows) %>%
  arrange(source_sample, column)

categorical_columns <- c(
  "borough", "cd", "raw_current_council", "district_id", "council_district",
  "borough_code", "borough_name", "land_use", "bldg_class", "bldg_class_prefix",
  "condo_no_present", "dcp_edited", "version", "is_joint_interest_area",
  "condo_lot_range", "residential_positive", "residential_proxy_candidate",
  "residential_only_flag", "mixed_use_flag", "multi_building_flag", "size_bin",
  "units_total_lt_units_res"
)

categorical_rows <- list()
categorical_row <- 1L

for (dataset_name in names(audit_datasets)) {
  dataset <- audit_datasets[[dataset_name]]
  dataset_columns <- intersect(categorical_columns, names(dataset))

  for (column_name in dataset_columns) {
    categorical_values <- str_squish(as.character(dataset[[column_name]]))
    categorical_values[categorical_values == ""] <- NA_character_
    categorical_table <- sort(table(categorical_values, useNA = "ifany"), decreasing = TRUE)
    categorical_table <- head(categorical_table, 50)

    if (length(categorical_table) > 0) {
      categorical_rows[[categorical_row]] <- tibble(
        source_sample = dataset_name,
        column = column_name,
        value = names(categorical_table),
        row_count = as.integer(categorical_table),
        row_share = as.integer(categorical_table) / nrow(dataset)
      )
      categorical_row <- categorical_row + 1L
    }
  }
}

categorical_values <- bind_rows(categorical_rows) %>%
  arrange(source_sample, column, desc(row_count), value)

filter_step_1 <- raw_mappluto
filter_step_2 <- filter_step_1 %>%
  inner_join(mappluto_assignment, by = "source_row_id", relationship = "one-to-one")
filter_step_3 <- filter_step_2 %>%
  filter(!is_joint_interest_area)
filter_step_4 <- filter_step_3 %>%
  filter(!is.na(council_district))
filter_step_5 <- filter_step_4 %>%
  filter(year_built >= proxy_start_year, year_built <= proxy_end_year)
filter_step_6 <- filter_step_5 %>%
  filter(units_res > 0)

proxy_filter_steps <- bind_rows(
  tibble(step_order = 1L, step = "raw_mappluto_25v4_shapefile_rows", rows = nrow(filter_step_1), residential_units = sum(filter_step_1$units_res, na.rm = TRUE), note = "Rows read directly from raw MapPLUTO shapefile zip after canonical column parsing."),
  tibble(step_order = 2L, step = "assigned_to_2010_council_by_representative_point", rows = nrow(filter_step_2), residential_units = sum(filter_step_2$units_res, na.rm = TRUE), note = "Rows whose representative point intersects an archived 2010 Council district."),
  tibble(step_order = 3L, step = "exclude_joint_interest_area_cd_codes", rows = nrow(filter_step_3), residential_units = sum(filter_step_3$units_res, na.rm = TRUE), note = "Drops DCP joint-interest-area community district codes."),
  tibble(step_order = 4L, step = "require_nonmissing_2010_council", rows = nrow(filter_step_4), residential_units = sum(filter_step_4$units_res, na.rm = TRUE), note = "Kept for transparency; same as step 3 after spatial assignment."),
  tibble(step_order = 5L, step = "restrict_year_built_1980_2025", rows = nrow(filter_step_5), residential_units = sum(filter_step_5$units_res, na.rm = TRUE), note = "Figure 2 window for the surviving-stock YearBuilt proxy."),
  tibble(step_order = 6L, step = "require_positive_units_res", rows = nrow(filter_step_6), residential_units = sum(filter_step_6$units_res, na.rm = TRUE), note = "Residential lots contributing units to Figure 2.")
) %>%
  mutate(
    rows_removed_since_previous = lag(rows) - rows,
    residential_units_removed_since_previous = lag(residential_units) - residential_units
  )

condo_profile <- bind_rows(
  rebuilt_lot_level %>%
    group_by(size_bin, condo_lot_range, condo_no_present, residential_only_flag, mixed_use_flag, multi_building_flag) %>%
    summarise(
      rows = n(),
      units_res = sum(units_res, na.rm = TRUE),
      one_unit_rows = sum(units_res == 1, na.rm = TRUE),
      fifty_plus_rows = sum(units_res >= 50, na.rm = TRUE),
      median_units_res = median(units_res, na.rm = TRUE),
      max_units_res = max(units_res, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(profile = "rebuilt_figure2_by_condo_lot_range"),
  raw_pluto %>%
    filter(year_built >= proxy_start_year, year_built <= proxy_end_year, units_res > 0) %>%
    group_by(size_bin, condo_lot_range, condo_no_present, residential_only_flag, mixed_use_flag, multi_building_flag) %>%
    summarise(
      rows = n(),
      units_res = sum(units_res, na.rm = TRUE),
      one_unit_rows = sum(units_res == 1, na.rm = TRUE),
      fifty_plus_rows = sum(units_res >= 50, na.rm = TRUE),
      median_units_res = median(units_res, na.rm = TRUE),
      max_units_res = max(units_res, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(profile = "raw_pluto_1980_2025_positive_units_res_by_condo_lot_range")
) %>%
  select(profile, everything()) %>%
  arrange(profile, size_bin, condo_lot_range, desc(units_res))

condo_suspect_groups <- rebuilt_lot_level %>%
  filter(condo_no_present | condo_lot_range != "other_lot_range" | bbl_lot_number >= 1001L) %>%
  mutate(group_appbbl = coalesce(appbbl, "missing_appbbl"), group_condo_no = coalesce(as.character(condo_no), "missing_condo_no")) %>%
  group_by(condo_lot_range, group_appbbl, group_condo_no, year_built, council_district, district_id) %>%
  summarise(
    lot_rows = n(),
    residential_units = sum(units_res, na.rm = TRUE),
    one_unit_lot_rows = sum(units_res == 1, na.rm = TRUE),
    two_to_four_unit_lot_rows = sum(units_res >= 2 & units_res <= 4, na.rm = TRUE),
    fifty_plus_lot_rows = sum(units_res >= 50, na.rm = TRUE),
    distinct_bbl = n_distinct(bbl),
    min_units_res = min(units_res, na.rm = TRUE),
    max_units_res = max(units_res, na.rm = TRUE),
    sample_bbls = paste(head(bbl, 8), collapse = "; "),
    sample_addresses = paste(head(na.omit(address), 5), collapse = "; "),
    .groups = "drop"
  ) %>%
  arrange(condo_lot_range, desc(residential_units), desc(lot_rows), group_appbbl, group_condo_no)

year_built_profile <- bind_rows(
  raw_mappluto %>%
    filter(units_res > 0) %>%
    transmute(profile = "raw_mappluto_positive_units_res", year_built, units_res),
  mappluto_assigned %>%
    filter(!is_joint_interest_area, units_res > 0) %>%
    transmute(profile = "assigned_nonjia_mappluto_positive_units_res", year_built, units_res),
  rebuilt_lot_level %>%
    transmute(profile = "rebuilt_figure2_lot_level", year_built, units_res)
) %>%
  mutate(
    year_built_terminal_digit = year_built %% 10L,
    year_built_ends_0_or_5 = year_built_terminal_digit %in% c(0L, 5L)
  ) %>%
  group_by(profile, year_built, year_built_terminal_digit, year_built_ends_0_or_5) %>%
  summarise(
    rows = n(),
    residential_units = sum(units_res, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(profile, year_built)

district_year_sanity <- rebuilt_district_year %>%
  group_by(district_id, council_district, borough_code, borough_name) %>%
  summarise(
    years = n(),
    total_proxy_lots = sum(residential_lot_count_proxy, na.rm = TRUE),
    total_proxy_units = sum(residential_units_proxy, na.rm = TRUE),
    total_1_4_units = sum(units_1_4_proxy, na.rm = TRUE),
    total_5_plus_units = sum(units_5_plus_proxy, na.rm = TRUE),
    total_50_plus_units = sum(units_50_plus_proxy, na.rm = TRUE),
    max_annual_units = max(residential_units_proxy, na.rm = TRUE),
    max_annual_50_plus_units = max(units_50_plus_proxy, na.rm = TRUE),
    zero_unit_years = sum(residential_units_proxy == 0, na.rm = TRUE),
    first_positive_year = suppressWarnings(min(year_built[residential_units_proxy > 0], na.rm = TRUE)),
    last_positive_year = suppressWarnings(max(year_built[residential_units_proxy > 0], na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  mutate(
    first_positive_year = if_else(is.infinite(first_positive_year), NA_integer_, as.integer(first_positive_year)),
    last_positive_year = if_else(is.infinite(last_positive_year), NA_integer_, as.integer(last_positive_year))
  ) %>%
  arrange(council_district)

rebuilt_units_p999 <- as.numeric(quantile(rebuilt_lot_level$units_res, probs = 0.999, na.rm = TRUE, names = FALSE))

outlier_lots <- bind_rows(
  rebuilt_lot_level %>%
    filter(units_res >= rebuilt_units_p999) %>%
    mutate(flag = "rebuilt_proxy_top_0_1_percent_units_res"),
  rebuilt_lot_level %>%
    filter(units_res >= 1000) %>%
    mutate(flag = "rebuilt_proxy_units_res_ge_1000"),
  rebuilt_lot_level %>%
    filter(units_total_lt_units_res) %>%
    mutate(flag = "rebuilt_proxy_units_total_lt_units_res"),
  rebuilt_lot_level %>%
    filter(lot_area <= 0 | bldg_area <= 0 | res_area <= 0) %>%
    mutate(flag = "rebuilt_proxy_nonpositive_area"),
  rebuilt_lot_level %>%
    filter(!is.na(built_far), built_far > 30) %>%
    mutate(flag = "rebuilt_proxy_built_far_gt_30"),
  rebuilt_lot_level %>%
    filter(!is.na(num_bldgs), num_bldgs >= 10) %>%
    mutate(flag = "rebuilt_proxy_num_bldgs_ge_10"),
  rebuilt_lot_level %>%
    filter(condo_lot_range == "1001_6999_unit_lot_range") %>%
    mutate(flag = "rebuilt_proxy_condo_unit_lot_range"),
  rebuilt_lot_level %>%
    filter(condo_no_present & condo_lot_range != "7501_7599_billing_lot_range") %>%
    mutate(flag = "rebuilt_proxy_condo_no_present_not_billing_lot_range"),
  rebuilt_lot_level %>%
    filter(!condo_no_present & condo_lot_range == "7501_7599_billing_lot_range") %>%
    mutate(flag = "rebuilt_proxy_billing_lot_range_without_condo_no"),
  mappluto_assigned %>%
    filter(!is_joint_interest_area, units_res > 0, is.na(year_built)) %>%
    mutate(source_sample = "rebuilt_mappluto_assigned", flag = "assigned_positive_units_res_missing_year_built") %>%
    select(any_of(names(rebuilt_lot_level)), flag),
  raw_mappluto %>%
    filter(units_res > 0, !is.na(year_built), year_built > proxy_end_year) %>%
    mutate(source_sample = "raw_mappluto_25v4_shapefile", flag = "raw_mappluto_future_year_built_positive_units_res") %>%
    select(any_of(names(rebuilt_lot_level)), flag),
  pluto_only_residential_lots %>%
    mutate(source_sample = "raw_pluto_25v4_csv", flag = "pluto_only_residential_1980_2025") %>%
    select(any_of(names(rebuilt_lot_level)), flag)
) %>%
  select(
    flag, source_sample, bbl, address, district_id, council_district, borough_code, borough_name,
    borough, block, lot, cd, raw_current_council, council_match_count,
    year_built, units_res, units_total, res_area, bldg_area, lot_area, built_far,
    num_bldgs, num_floors, land_use, bldg_class, bldg_class_prefix, condo_no, condo_no_present,
    appbbl, app_date, pluto_map_id, dcp_edited, latitude, longitude, x_coord, y_coord, version,
    bbl_lot_number, condo_lot_range, residential_only_flag, mixed_use_flag,
    multi_building_flag, size_bin, units_total_lt_units_res
  ) %>%
  arrange(flag, desc(units_res), year_built, bbl)

missing_year_built_residential_units <- sum(mappluto_assigned$units_res[!mappluto_assigned$is_joint_interest_area & mappluto_assigned$units_res > 0 & is.na(mappluto_assigned$year_built)], na.rm = TRUE)
pluto_only_residential_units <- sum(pluto_only_residential_lots$units_res, na.rm = TRUE)
pluto_comparable_units <- sum(pluto_comparable$units_res, na.rm = TRUE)
unit_lot_range_rows <- sum(rebuilt_lot_level$condo_lot_range == "1001_6999_unit_lot_range", na.rm = TRUE)
unit_lot_range_units <- sum(rebuilt_lot_level$units_res[rebuilt_lot_level$condo_lot_range == "1001_6999_unit_lot_range"], na.rm = TRUE)
recognized_condo_units <- sum(rebuilt_lot_level$units_res[rebuilt_lot_level$condo_no_present], na.rm = TRUE)
recognized_condo_50_plus_units <- sum(rebuilt_lot_level$units_res[rebuilt_lot_level$condo_no_present & rebuilt_lot_level$size_bin == "50_plus"], na.rm = TRUE)
proxy_units <- sum(rebuilt_lot_level$units_res, na.rm = TRUE)
proxy_50_plus_units <- sum(rebuilt_lot_level$units_res[rebuilt_lot_level$size_bin == "50_plus"], na.rm = TRUE)
multi_building_50_plus_units <- sum(rebuilt_lot_level$units_res[rebuilt_lot_level$size_bin == "50_plus" & rebuilt_lot_level$multi_building_flag], na.rm = TRUE)
year_built_heaped_units_share <- sum(rebuilt_lot_level$units_res[rebuilt_lot_level$year_built %% 5L == 0L], na.rm = TRUE) / proxy_units
raw_mappluto_duplicate_bbl_rows <- sum(duplicated(raw_mappluto$bbl[!is.na(raw_mappluto$bbl)]))
rebuilt_proxy_duplicate_bbl_rows <- sum(duplicated(rebuilt_lot_level$bbl[!is.na(rebuilt_lot_level$bbl)]))
raw_mappluto_version_not_25v4 <- sum(!is.na(raw_mappluto$version) & !str_detect(str_to_lower(raw_mappluto$version), "25v4"))
unassigned_rows <- nrow(raw_mappluto) - nrow(mappluto_assigned)
boundary_tie_rows <- sum(mappluto_assignment$council_match_count > 1, na.rm = TRUE)
bad_year_built_rows <- sum(rebuilt_lot_level$year_built < proxy_start_year | rebuilt_lot_level$year_built > proxy_end_year | is.na(rebuilt_lot_level$year_built), na.rm = TRUE)
nonpositive_units_res_rows <- sum(rebuilt_lot_level$units_res <= 0 | is.na(rebuilt_lot_level$units_res), na.rm = TRUE)
units_total_lt_units_res_rows <- sum(rebuilt_lot_level$units_total_lt_units_res, na.rm = TRUE)
recognized_condo_not_billing_rows <- sum(rebuilt_lot_level$condo_no_present & rebuilt_lot_level$condo_lot_range != "7501_7599_billing_lot_range", na.rm = TRUE)

assumption_checks <- bind_rows(
  tibble(check = "raw_mappluto_bbl_unique", status = if_else(raw_mappluto_duplicate_bbl_rows == 0, "pass", "warn"), value = as.character(raw_mappluto_duplicate_bbl_rows), detail = "Duplicate nonmissing BBL rows in raw MapPLUTO after canonical BBL construction."),
  tibble(check = "rebuilt_proxy_bbl_unique", status = if_else(rebuilt_proxy_duplicate_bbl_rows == 0, "pass", "warn"), value = as.character(rebuilt_proxy_duplicate_bbl_rows), detail = "Duplicate nonmissing BBL rows in the rebuilt Figure 2 proxy."),
  tibble(check = "raw_mappluto_version_25v4", status = if_else(raw_mappluto_version_not_25v4 == 0, "pass", "fail"), value = as.character(raw_mappluto_version_not_25v4), detail = "Rows with a nonmissing Version value not containing 25v4."),
  tibble(check = "council_assignment_unassigned_rows", status = if_else(unassigned_rows == 0, "pass", "info"), value = as.character(unassigned_rows), detail = "Raw MapPLUTO rows whose representative point did not intersect an archived 2010 Council district."),
  tibble(check = "council_assignment_boundary_tie_rows", status = if_else(boundary_tie_rows == 0, "pass", "info"), value = as.character(boundary_tie_rows), detail = "Raw MapPLUTO representative points intersecting multiple Council polygons; first district in district order is used."),
  tibble(check = "rebuilt_proxy_year_built_range", status = if_else(bad_year_built_rows == 0, "pass", "fail"), value = as.character(bad_year_built_rows), detail = "Rows in rebuilt proxy outside YearBuilt 1980-2025."),
  tibble(check = "rebuilt_proxy_units_res_positive", status = if_else(nonpositive_units_res_rows == 0, "pass", "fail"), value = as.character(nonpositive_units_res_rows), detail = "Rows in rebuilt proxy with missing or nonpositive UnitsRes."),
  tibble(check = "rebuilt_proxy_units_total_ge_units_res", status = if_else(units_total_lt_units_res_rows == 0, "pass", "warn"), value = as.character(units_total_lt_units_res_rows), detail = "Rows in rebuilt proxy with UnitsTotal below UnitsRes."),
  tibble(check = "recognized_condos_are_billing_lot_range", status = if_else(recognized_condo_not_billing_rows == 0, "pass", "warn"), value = as.character(recognized_condo_not_billing_rows), detail = "Rows with CondoNo present but lot number outside the 7501-7599 billing-lot range."),
  tibble(check = "unit_lot_range_rows_in_rebuilt_proxy", status = if_else(unit_lot_range_rows == 0, "pass", "warn"), value = paste0(unit_lot_range_rows, " rows; ", unit_lot_range_units, " units"), detail = "Rows with BBL lot number 1001-6999 and positive UnitsRes; these can be condo unit lots."),
  tibble(check = "pluto_only_residential_1980_2025_units", status = if_else(pluto_only_residential_units == 0, "pass", "warn"), value = paste0(pluto_only_residential_units, " units; ", round(100 * pluto_only_residential_units / pluto_comparable_units, 2), "% of current PLUTO comparable units"), detail = "Raw PLUTO CSV residential 1980-2025 rows absent from raw MapPLUTO by BBL."),
  tibble(check = "multi_building_50_plus_units", status = "info", value = paste0(multi_building_50_plus_units, " units; ", round(100 * multi_building_50_plus_units / proxy_50_plus_units, 2), "% of proxy 50+ units"), detail = "50+ is a lot/complex-level size bin when a tax lot has multiple buildings."),
  tibble(check = "recognized_condo_units_in_proxy", status = "info", value = paste0(recognized_condo_units, " units; ", round(100 * recognized_condo_units / proxy_units, 2), "% of proxy units"), detail = "Rows with nonmissing CondoNo in the rebuilt Figure 2 proxy."),
  tibble(check = "recognized_condo_50_plus_units_in_proxy", status = "info", value = paste0(recognized_condo_50_plus_units, " units; ", round(100 * recognized_condo_50_plus_units / proxy_50_plus_units, 2), "% of proxy 50+ units"), detail = "Rows with nonmissing CondoNo in the rebuilt Figure 2 proxy 50+ margin."),
  tibble(check = "year_built_terminal_digit_heaping", status = "info", value = paste0(round(100 * year_built_heaped_units_share, 2), "%"), detail = "Share of rebuilt proxy units with YearBuilt ending in 0 or 5."),
  tibble(check = "missing_year_built_residential_units", status = if_else(missing_year_built_residential_units == 0, "pass", "warn"), value = as.character(missing_year_built_residential_units), detail = "Positive residential units in assigned non-JIA raw MapPLUTO with missing or invalid YearBuilt.")
) %>%
  arrange(factor(status, levels = c("fail", "warn", "info", "pass")), check)

write_csv_if_changed(rebuilt_lot_level, "../output/pluto_exploration_rebuilt_lot_level.csv")
write_csv_if_changed(rebuilt_district_year, "../output/pluto_exploration_rebuilt_district_year.csv")
write_csv_if_changed(rebuilt_bbl_lookup, "../output/pluto_exploration_rebuilt_bbl_lookup.csv")
write_csv_if_changed(key_columns_dictionary, "../output/pluto_exploration_key_columns_dictionary.csv")
write_csv_if_changed(column_profile, "../output/pluto_exploration_column_profile.csv")
write_csv_if_changed(numeric_quantiles, "../output/pluto_exploration_numeric_quantiles.csv")
write_csv_if_changed(categorical_values, "../output/pluto_exploration_categorical_values.csv")
write_csv_if_changed(proxy_filter_steps, "../output/pluto_exploration_proxy_filter_steps.csv")
write_csv_if_changed(assumption_checks, "../output/pluto_exploration_assumption_checks.csv")
write_csv_if_changed(condo_profile, "../output/pluto_exploration_condo_profile.csv")
write_csv_if_changed(condo_suspect_groups, "../output/pluto_exploration_condo_suspect_groups.csv")
write_csv_if_changed(year_built_profile, "../output/pluto_exploration_year_built_profile.csv")
write_csv_if_changed(pluto_mappluto_overlap, "../output/pluto_exploration_pluto_mappluto_overlap.csv")
write_csv_if_changed(pluto_only_residential_lots, "../output/pluto_exploration_pluto_only_residential_lots.csv")
write_csv_if_changed(district_year_sanity, "../output/pluto_exploration_district_year_sanity.csv")
write_csv_if_changed(outlier_lots, "../output/pluto_exploration_outlier_lots.csv")

warning_checks <- assumption_checks %>%
  filter(status == "warn")
failure_checks <- assumption_checks %>%
  filter(status == "fail")
info_checks <- assumption_checks %>%
  filter(status == "info")

report_lines <- c(
  "# PLUTO Exploration Audit",
  "",
  "Generated from the raw downloaded 25v4 PLUTO and MapPLUTO zip files in `../input/`.",
  "This task does not load staged MapPLUTO parquets or precomputed construction-proxy outputs; it rebuilds the Figure 2 proxy from the raw current files inside this script.",
  "",
  "## Raw Inputs",
  "",
  "- `../input/nyc_mappluto_25v4_shp.zip`: raw MapPLUTO shapefile zip.",
  "- `../input/nyc_pluto_25v4_csv.zip`: raw PLUTO CSV zip.",
  "- `../input/ccdist2010_homeownership_1990_measure.csv`: 2010 Council district geometry and baseline measures used only to assign lots to the archived analysis geography.",
  "",
  "## Rebuilt Process",
  "",
  "1. Read raw MapPLUTO geometry and attributes from the shapefile zip.",
  "2. Read raw PLUTO attributes from the CSV zip for overlap and missing-record checks.",
  "3. Canonicalize BBL, Borough/Block/Lot, YearBuilt, unit, area, building, condo, and DCP metadata columns.",
  "4. Place one representative point inside each MapPLUTO lot polygon and assign it to the archived 2010 Council district containing that point.",
  "5. Rebuild the Figure 2 lot proxy by excluding joint-interest-area CDs, requiring assigned 2010 Council district, requiring YearBuilt 1980-2025, and requiring positive UnitsRes.",
  "6. Aggregate rebuilt lot records to Council-district-by-YearBuilt cells.",
  "",
  "## Main Counts",
  "",
  paste0("- Raw MapPLUTO rows: ", format(nrow(raw_mappluto), big.mark = ","), "."),
  paste0("- Raw PLUTO CSV rows: ", format(nrow(raw_pluto), big.mark = ","), "."),
  paste0("- Raw MapPLUTO rows assigned to 2010 Council districts: ", format(nrow(mappluto_assigned), big.mark = ","), "."),
  paste0("- Rebuilt Figure 2 proxy rows: ", format(nrow(rebuilt_lot_level), big.mark = ","), "."),
  paste0("- Rebuilt Figure 2 proxy residential units: ", format(proxy_units, big.mark = ","), "."),
  paste0("- Rebuilt Figure 2 proxy 50+ units: ", format(proxy_50_plus_units, big.mark = ","), "."),
  paste0("- Recognized condo units in rebuilt proxy: ", format(recognized_condo_units, big.mark = ","), " (", round(100 * recognized_condo_units / proxy_units, 2), "%)."),
  paste0("- Recognized condo 50+ units in rebuilt proxy: ", format(recognized_condo_50_plus_units, big.mark = ","), " (", round(100 * recognized_condo_50_plus_units / proxy_50_plus_units, 2), "% of 50+ units)."),
  paste0("- PLUTO-only residential 1980-2025 units absent from MapPLUTO by BBL: ", format(pluto_only_residential_units, big.mark = ","), " (", round(100 * pluto_only_residential_units / pluto_comparable_units, 2), "% of current PLUTO comparable units)."),
  "",
  "## Warnings To Review",
  "",
  if (nrow(warning_checks) == 0) "- No warning checks triggered." else paste0("- `", warning_checks$check, "`: ", warning_checks$value, ". ", warning_checks$detail),
  "",
  "## Failures",
  "",
  if (nrow(failure_checks) == 0) "- No failure checks triggered." else paste0("- `", failure_checks$check, "`: ", failure_checks$value, ". ", failure_checks$detail),
  "",
  "## Interpretive Diagnostics",
  "",
  if (nrow(info_checks) == 0) "- No informational diagnostics." else paste0("- `", info_checks$check, "`: ", info_checks$value, ". ", info_checks$detail),
  "",
  "## Output Files",
  "",
  "- `pluto_exploration_rebuilt_lot_level.csv`: The lot-level Figure 2 proxy rebuilt directly from raw MapPLUTO.",
  "- `pluto_exploration_rebuilt_district_year.csv`: The Council-district-by-year Figure 2 proxy rebuilt from the lot-level file.",
  "- `pluto_exploration_rebuilt_bbl_lookup.csv`: Non-JIA BBL-to-2010-Council assignment rebuilt from raw MapPLUTO geometry.",
  "- `pluto_exploration_key_columns_dictionary.csv`: Explicit use and assumption for each key PLUTO/MapPLUTO column.",
  "- `pluto_exploration_column_profile.csv`: Nonmissing, distinct, numeric-parse, range, and top-value profile for key columns.",
  "- `pluto_exploration_numeric_quantiles.csv`: Detailed quantiles for numeric fields.",
  "- `pluto_exploration_categorical_values.csv`: Top categorical values by source sample.",
  "- `pluto_exploration_proxy_filter_steps.csv`: Row and unit funnel from raw MapPLUTO to the rebuilt Figure 2 proxy.",
  "- `pluto_exploration_assumption_checks.csv`: Pass/warn/fail diagnostics.",
  "- `pluto_exploration_condo_profile.csv`: Condo and lot-range summary by size bin.",
  "- `pluto_exploration_condo_suspect_groups.csv`: Condo/unit-lot groups that deserve manual review.",
  "- `pluto_exploration_year_built_profile.csv`: Year and terminal-digit distribution.",
  "- `pluto_exploration_pluto_mappluto_overlap.csv`: Raw PLUTO CSV versus raw MapPLUTO overlap.",
  "- `pluto_exploration_pluto_only_residential_lots.csv`: Residential 1980-2025 PLUTO rows missing from MapPLUTO.",
  "- `pluto_exploration_district_year_sanity.csv`: District-year aggregate sanity checks.",
  "- `pluto_exploration_outlier_lots.csv`: Lot-level records behind the most important flags."
)

report_path <- tempfile(fileext = ".md")
writeLines(report_lines, report_path)
copy_if_changed(report_path, "../output/pluto_exploration_report.md")

unlink(mappluto_temp_dir, recursive = TRUE)

cat("Wrote raw-source PLUTO exploration outputs to ../output\n")
