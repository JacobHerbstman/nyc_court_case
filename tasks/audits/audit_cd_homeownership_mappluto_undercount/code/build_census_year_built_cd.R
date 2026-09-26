# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_cd_homeownership_mappluto_undercount/code")

# Census year-structure-built counts by community district from three surveys:
# 1990 STF3 tracts (NHGIS), the DCP 2000 CD profiles, and ACS 2020-2024 tracts.
# Tract counts are split across CDs in proportion to the tract's 25v4 MapPLUTO
# residential units in each CD, so they use the same lot-to-CD assignment as
# the production outcome.

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(jsonlite)
  library(purrr)
  library(readr)
  library(sf)
  library(stringr)
  library(tidyr)
})

source("../../../_lib/data_reports.R")

sf_use_s2(FALSE)

standard_cds <- c(sprintf("1%02d", 1:12), sprintf("2%02d", 1:12), sprintf("3%02d", 1:18), sprintf("4%02d", 1:14), sprintf("5%02d", 1:3))
nyc_counties <- c("005", "047", "061", "081", "085")

# 25v4 residential lots as points (XCoord/YCoord are NY State Plane feet)
lot_dir <- tempfile(pattern = "mappluto_dbf_")
unzip("../input/nyc_mappluto_25v4_arc_shp.zip", files = "MapPLUTO.dbf", exdir = lot_dir)

residential_points <- foreign::read.dbf(file.path(lot_dir, "MapPLUTO.dbf"), as.is = TRUE) |>
  transmute(borocd = sprintf("%03d", as.integer(CD)), unitsres = coalesce(as.numeric(UnitsRes), 0), XCoord, YCoord) |>
  filter(borocd %in% standard_cds, unitsres > 0, XCoord > 0, YCoord > 0) |>
  st_as_sf(coords = c("XCoord", "YCoord"), crs = 2263)

unlink(lot_dir, recursive = TRUE)

allocate_to_cd <- function(tract_sf, value_cols) {
  weights <- st_join(residential_points, tract_sf |> select(tract_id), join = st_within) |>
    st_drop_geometry() |>
    filter(!is.na(tract_id)) |>
    group_by(tract_id, borocd) |>
    summarise(units = sum(unitsres), .groups = "drop") |>
    group_by(tract_id) |>
    mutate(unit_share = units / sum(units)) |>
    ungroup()

  tract_df <- st_drop_geometry(tract_sf)

  list(
    cd = weights |>
      inner_join(tract_df, by = "tract_id", relationship = "many-to-one") |>
      group_by(borocd) |>
      summarise(across(all_of(value_cols), ~ sum(.x * unit_share)), .groups = "drop"),
    tracts_without_lots = tract_df |> filter(!tract_id %in% weights$tract_id)
  )
}

# 1990 STF3: NH25 year built, NH27 tenure by year built, NH77 year-built imputation
nhgis_csv <- grep("ds123_1990_tract\\.csv$", unzip("../output/nhgis_1990_stf3_year_built_tract_csv.zip", list = TRUE)$Name, value = TRUE)

tracts_1990 <- read_csv(
  unz("../output/nhgis_1990_stf3_year_built_tract_csv.zip", nhgis_csv),
  col_types = cols(.default = col_character())
) |>
  filter(COUNTYA %in% nyc_counties) |>
  mutate(across(matches("^(EX7|EYA|EZZ)[0-9]{3}$"), as.numeric)) |>
  transmute(
    tract_id = GISJOIN,
    c1990_built_1989_mar1990 = EX7001,
    c1990_built_1985_1988 = EX7002,
    c1990_built_1980_1984 = EX7003,
    c1990_built_1970_1979 = EX7004,
    c1990_housing_units = EX7001 + EX7002 + EX7003 + EX7004 + EX7005 + EX7006 + EX7007 + EX7008,
    c1990_owner_built_1985_mar1990 = EYA001 + EYA002,
    c1990_renter_built_1985_mar1990 = EYA009 + EYA010,
    c1990_year_built_allocated = EZZ001
  )

stopifnot(!anyDuplicated(tracts_1990$tract_id), nrow(tracts_1990) > 2000)

shape_dir <- tempfile(pattern = "tract_1990_")
dir.create(shape_dir)
inner_zip <- grep("shapefile.*\\.zip$", unzip("../input/nhgis0005_shape.zip", list = TRUE)$Name, value = TRUE)
unzip("../input/nhgis0005_shape.zip", files = inner_zip, exdir = shape_dir, junkpaths = TRUE)
unzip(file.path(shape_dir, basename(inner_zip)), exdir = shape_dir)

tract_1990_sf <- st_read(
  file.path(shape_dir, "US_tract_1990.shp"),
  query = "SELECT GISJOIN FROM US_tract_1990 WHERE NHGISST = '360' AND NHGISCTY IN ('0050', '0470', '0610', '0810', '0850')",
  quiet = TRUE
) |>
  transmute(tract_id = GISJOIN) |>
  inner_join(tracts_1990, by = "tract_id", relationship = "one-to-one") |>
  st_make_valid() |>
  st_transform(2263)

unlink(shape_dir, recursive = TRUE)

alloc_1990 <- allocate_to_cd(tract_1990_sf, setdiff(names(tracts_1990), "tract_id"))

# ACS 2020-2024: B25034 year built (all units), B25127 tenure by year built by units in structure (occupied units)
read_acs <- function(table_id) {
  map_dfr(nyc_counties, function(county_code) {
    response <- fromJSON(paste0("../output/acs5_2024_", table_id, "_", county_code, ".json"))
    colnames(response) <- response[1, ]
    # group() repeats NAME and GEO_ID
    as_tibble(response[-1, !duplicated(colnames(response)), drop = FALSE])
  }) |>
    mutate(tract_id = str_remove(GEO_ID, "^1400000US")) |>
    select(tract_id, matches(paste0("^", table_id, "_[0-9]{3}E$"))) |>
    mutate(across(-tract_id, as.numeric))
}

b25034 <- read_acs("B25034")
b25127 <- read_acs("B25127")

sum_cols <- function(df, ids) rowSums(df[, sprintf("B25127_%03dE", ids)])

tracts_2024 <- b25034 |>
  transmute(
    tract_id,
    acs2024_housing_units = B25034_001E,
    acs2024_built_2020_later = B25034_002E,
    acs2024_built_2010_2019 = B25034_003E,
    acs2024_built_2000_2009 = B25034_004E,
    acs2024_built_1990_1999 = B25034_005E,
    acs2024_built_1980_1989 = B25034_006E,
    acs2024_built_1970_1979 = B25034_007E
  ) |>
  inner_join(
    b25127 |>
      transmute(
        tract_id,
        # Owner block rows 3-44, renter block rows 46-87; within each year group: 1 unit, 2-4, 5-19, 20-49, 50+, mobile.
        acs2024_occ_1_4_built_2020_later = sum_cols(b25127, c(4, 5, 47, 48)),
        acs2024_occ_5_plus_built_2020_later = sum_cols(b25127, c(6, 7, 8, 49, 50, 51)),
        acs2024_occ_1_4_built_2000_2019 = sum_cols(b25127, c(11, 12, 54, 55)),
        acs2024_occ_5_plus_built_2000_2019 = sum_cols(b25127, c(13, 14, 15, 56, 57, 58)),
        acs2024_occ_1_4_built_1980_1999 = sum_cols(b25127, c(18, 19, 61, 62)),
        acs2024_occ_5_plus_built_1980_1999 = sum_cols(b25127, c(20, 21, 22, 63, 64, 65)),
        acs2024_occ_1_4_built_1960_1979 = sum_cols(b25127, c(25, 26, 68, 69)),
        acs2024_occ_5_plus_built_1960_1979 = sum_cols(b25127, c(27, 28, 29, 70, 71, 72))
      ),
    by = "tract_id",
    relationship = "one-to-one"
  )

stopifnot(!anyDuplicated(tracts_2024$tract_id), nrow(tracts_2024) == 2327)

tiger_dir <- tempfile(pattern = "tiger_2020_")
unzip("../input/tl_2020_36_tract.zip", exdir = tiger_dir)

tract_2024_sf <- st_read(file.path(tiger_dir, "tl_2020_36_tract.shp"), quiet = TRUE) |>
  filter(COUNTYFP %in% nyc_counties) |>
  transmute(tract_id = GEOID) |>
  inner_join(tracts_2024, by = "tract_id", relationship = "one-to-one") |>
  st_make_valid() |>
  st_transform(2263)

unlink(tiger_dir, recursive = TRUE)

alloc_2024 <- allocate_to_cd(tract_2024_sf, setdiff(names(tracts_2024), "tract_id"))

# 2000 Census from the DCP CD profiles (published CD tabulations)
profiles <- read_parquet("../input/dcp_cd_profiles_1990_2000_20260501.parquet") |>
  mutate(borocd = sprintf("%03d", as.integer(district_id)))

census_2000 <- profiles |>
  filter(section_name == "year_structure_built") |>
  group_by(borocd) |>
  summarise(
    c2000_built_1970_1979 = sum(value_2000_number[metric_label == "1970 to 1979"]),
    c2000_built_1980_1989 = sum(value_2000_number[metric_label == "1980 to 1989"]),
    c2000_built_1990_mar2000 = sum(value_2000_number[metric_label %in% c("1990 to 1994", "1995 to 1998", "1999 to March 2000")]),
    .groups = "drop"
  )

housing_units_dcp <- profiles |>
  filter(section_name == "housing_occupancy", metric_label == "Total housing units") |>
  transmute(borocd, dcp_housing_units_1990 = value_1990_number, dcp_housing_units_2000 = value_2000_number)

census_cd <- tibble(borocd = standard_cds) |>
  left_join(alloc_1990$cd, by = "borocd", relationship = "one-to-one") |>
  left_join(census_2000, by = "borocd", relationship = "one-to-one") |>
  left_join(housing_units_dcp, by = "borocd", relationship = "one-to-one") |>
  left_join(alloc_2024$cd, by = "borocd", relationship = "one-to-one")

stopifnot(nrow(census_cd) == 59, !anyNA(census_cd))

# Allocation check: 1990 tract totals split to CDs against DCP's published CD totals
allocation_error <- census_cd$c1990_housing_units / census_cd$dcp_housing_units_1990 - 1
cat("1990 tracts without residential lots:", nrow(alloc_1990$tracts_without_lots), "holding", sum(alloc_1990$tracts_without_lots$c1990_housing_units), "housing units\n")
cat("2020 tracts without residential lots:", nrow(alloc_2024$tracts_without_lots), "holding", sum(alloc_2024$tracts_without_lots$acs2024_housing_units), "housing units\n")
cat("1990 allocated vs DCP CD housing units: median abs error", round(median(abs(allocation_error)), 3), " max", round(max(abs(allocation_error)), 3), "\n")

if (median(abs(allocation_error)) > 0.05) {
  stop("1990 tract-to-CD allocation departs from DCP CD housing-unit totals by more than 5% at the median.")
}

save_csv(census_cd |> mutate(across(where(is.numeric), ~ round(.x, 1))), "../output/census_year_built_cd.csv", "borocd")

cat("Wrote ../output/census_year_built_cd.csv\n")
