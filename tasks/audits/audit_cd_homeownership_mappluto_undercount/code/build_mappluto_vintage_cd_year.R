# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_cd_homeownership_mappluto_undercount/code")

# Community-district-by-year residential unit counts from the 25v4 MapPLUTO
# (the production vintage) and the 2002 MapPLUTO (02b), including units in
# buildings with a recorded alteration, city-owned units, and whether 02b lots
# keep the same BBL and year built in 25v4.

suppressPackageStartupMessages({
  library(dplyr)
  library(purrr)
  library(readr)
  library(tidyr)
})

source("../../../_lib/data_reports.R")

standard_cds <- c(sprintf("1%02d", 1:12), sprintf("2%02d", 1:12), sprintf("3%02d", 1:18), sprintf("4%02d", 1:14), sprintf("5%02d", 1:3))

read_zipped_dbf <- function(zip_path, dbf_names) {
  dbf_dir <- tempfile(pattern = "mappluto_dbf_")
  dir.create(dbf_dir)
  on.exit(unlink(dbf_dir, recursive = TRUE))
  unzip(zip_path, files = dbf_names, exdir = dbf_dir, junkpaths = TRUE)
  map_dfr(file.path(dbf_dir, basename(dbf_names)), ~ foreign::read.dbf(.x, as.is = TRUE))
}

# 25v4: the first DBF in the archive, the same file the production lookup reads
lots_25v4 <- read_zipped_dbf("../input/nyc_mappluto_25v4_arc_shp.zip", "MapPLUTO.dbf") |>
  transmute(
    vintage = "25v4",
    bbl = sprintf("%.0f", BBL),
    borocd = sprintf("%03d", as.integer(CD)),
    yearbuilt = as.integer(YearBuilt),
    yearalter1 = as.integer(YearAlter1),
    yearalter2 = as.integer(YearAlter2),
    unitsres = coalesce(as.numeric(UnitsRes), 0),
    city_owned = coalesce(OwnerType == "C", FALSE)
  )

# 02b: one DBF per borough
dbf_02b <- grep("mappluto\\.dbf$", unzip("../input/mappluto_02b.zip", list = TRUE)$Name, value = TRUE)
stopifnot(length(dbf_02b) == 5)

lots_02b <- read_zipped_dbf("../input/mappluto_02b.zip", dbf_02b) |>
  transmute(
    vintage = "02b",
    bbl = sprintf("%d%05d%04d", match(borough, c("MN", "BX", "BK", "QN", "SI")), as.integer(block), as.integer(lot)),
    borocd = sprintf("%03d", as.integer(cd2)),
    yearbuilt = as.integer(yearBuilt),
    yearalter1 = as.integer(yearAlter),
    yearalter2 = as.integer(yearAlter2),
    unitsres = coalesce(as.numeric(unitsRes), 0),
    city_owned = coalesce(ownerType == "C", FALSE)
  )

stopifnot(!anyDuplicated(lots_25v4$bbl), !anyDuplicated(lots_02b$bbl), !anyNA(lots_02b$bbl))

# Does the 02b lot survive in 25v4 with the same BBL and year built?
lots_02b <- lots_02b |>
  left_join(
    lots_25v4 |> transmute(bbl, yearbuilt_25v4 = yearbuilt, unitsres_25v4 = unitsres),
    by = "bbl",
    relationship = "one-to-one"
  ) |>
  mutate(same_lot_and_year_in_25v4 = coalesce(yearbuilt_25v4 == yearbuilt & unitsres_25v4 > 0, FALSE))

residential_lots <- bind_rows(lots_25v4, lots_02b |> select(-yearbuilt_25v4, -unitsres_25v4)) |>
  filter(borocd %in% standard_cds, unitsres > 0) |>
  mutate(
    size = if_else(unitsres >= 5, "5_plus", "1_4"),
    same_lot_and_year_in_25v4 = if_else(vintage == "25v4", NA, same_lot_and_year_in_25v4)
  )

built <- residential_lots |>
  filter(yearbuilt >= 1910, yearbuilt <= 2025) |>
  group_by(vintage, borocd, year = yearbuilt) |>
  summarise(
    units_built = sum(unitsres),
    units_built_1_4 = sum(unitsres[size == "1_4"]),
    units_built_5_plus = sum(unitsres[size == "5_plus"]),
    units_built_city_owned = sum(unitsres[city_owned]),
    units_built_same_lot_and_year_in_25v4 = sum(unitsres[same_lot_and_year_in_25v4]),
    .groups = "drop"
  )

# Alteration years after construction; a lot with both alterations in one year counts once
altered <- residential_lots |>
  filter(yearbuilt > 0) |>
  select(vintage, borocd, bbl, yearbuilt, unitsres, size, yearalter1, yearalter2) |>
  pivot_longer(c(yearalter1, yearalter2), values_to = "year") |>
  filter(year > yearbuilt, year >= 1910, year <= 2025) |>
  distinct(vintage, borocd, bbl, year, yearbuilt, unitsres, size) |>
  group_by(vintage, borocd, year) |>
  summarise(
    units_altered = sum(unitsres),
    units_altered_1_4 = sum(unitsres[size == "1_4"]),
    units_altered_5_plus = sum(unitsres[size == "5_plus"]),
    units_altered_pre1940_5_plus = sum(unitsres[size == "5_plus" & yearbuilt < 1940]),
    .groups = "drop"
  )

cd_year <- expand_grid(vintage = c("25v4", "02b"), borocd = standard_cds, year = 1910:2025) |>
  left_join(built, by = c("vintage", "borocd", "year"), relationship = "one-to-one") |>
  left_join(altered, by = c("vintage", "borocd", "year"), relationship = "one-to-one") |>
  mutate(across(starts_with("units_"), ~ coalesce(.x, 0))) |>
  mutate(units_built_same_lot_and_year_in_25v4 = if_else(vintage == "25v4", NA_real_, units_built_same_lot_and_year_in_25v4)) |>
  arrange(vintage, borocd, year)

# The 25v4 built counts must reproduce the production construction proxy exactly
production_proxy <- read_csv("../input/mappluto_construction_proxy_cd_year.csv", col_types = cols_only(borocd = "c", yearbuilt = "i", units_1_4_proxy = "d", units_5_plus_proxy = "d")) |>
  transmute(borocd, year = as.integer(yearbuilt), units_1_4_proxy, units_5_plus_proxy)

proxy_check <- cd_year |>
  filter(vintage == "25v4") |>
  inner_join(production_proxy, by = c("borocd", "year"), relationship = "one-to-one")

stopifnot(
  nrow(proxy_check) == 59 * 116,
  all(proxy_check$units_built_1_4 == proxy_check$units_1_4_proxy),
  all(proxy_check$units_built_5_plus == proxy_check$units_5_plus_proxy)
)

save_csv(cd_year, "../output/mappluto_vintage_cd_year.csv", c("vintage", "borocd", "year"))

cat("Wrote ../output/mappluto_vintage_cd_year.csv\n")
