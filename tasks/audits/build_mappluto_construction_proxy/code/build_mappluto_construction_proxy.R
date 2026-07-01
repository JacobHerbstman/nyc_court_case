suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

standard_cd <- read_csv("../input/cd_homeownership_1990_measure.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name
  ) |>
  distinct()

if (anyDuplicated(standard_cd$borocd)) {
  stop("Treatment input is not unique by borocd.")
}

current_df <- read_parquet("../input/mappluto_current_lot_lookup.parquet") |>
  transmute(
    bbl = as.character(bbl),
    address = as.character(address),
    borocd = sprintf("%03d", suppressWarnings(as.integer(cd))),
    yearbuilt = suppressWarnings(as.integer(yearbuilt)),
    unitsres = suppressWarnings(as.numeric(unitsres)),
    unitstotal = suppressWarnings(as.numeric(unitstotal)),
    resarea = suppressWarnings(as.numeric(resarea)),
    bldgarea = suppressWarnings(as.numeric(bldgarea)),
    lotarea = suppressWarnings(as.numeric(lotarea)),
    builtfar = suppressWarnings(as.numeric(builtfar)),
    numbldgs = suppressWarnings(as.integer(numbldgs)),
    numfloors = suppressWarnings(as.numeric(numfloors)),
    landuse = as.character(landuse),
    bldgclass = as.character(bldgclass),
    is_joint_interest_area = as.logical(is_joint_interest_area)
  ) |>
  mutate(
    unitsres = coalesce(unitsres, 0),
    unitstotal = coalesce(unitstotal, 0),
    resarea = coalesce(resarea, 0),
    bldgarea = coalesce(bldgarea, 0),
    lotarea = coalesce(lotarea, 0),
    is_joint_interest_area = coalesce(is_joint_interest_area, FALSE)
  )

lot_level <- current_df |>
  filter(
    !is_joint_interest_area,
    !is.na(borocd),
    yearbuilt >= 1980,
    yearbuilt <= 2025,
    unitsres > 0
  ) |>
  inner_join(standard_cd, by = "borocd", relationship = "many-to-one") |>
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
  ) |>
  select(
    bbl, address, borocd, borough_code, borough_name, yearbuilt, unitsres, unitstotal,
    resarea, bldgarea, lotarea, builtfar, numbldgs, numfloors, landuse, bldgclass,
    residential_only_flag, mixed_use_flag, size_bin
  )

write_parquet(lot_level, "../output/mappluto_construction_proxy_lot_level.parquet")

panel_base <- lot_level |>
  group_by(borocd, borough_code, borough_name, yearbuilt) |>
  summarize(
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
  ) |>
  mutate(
    lots_1_4_proxy = lots_1_2_proxy + lots_3_4_proxy,
    lots_5_plus_proxy = lots_5_9_proxy + lots_10_49_proxy + lots_50_plus_proxy,
    units_1_4_proxy = units_1_2_proxy + units_3_4_proxy,
    units_5_plus_proxy = units_5_9_proxy + units_10_49_proxy + units_50_plus_proxy
  )

panel <- expand_grid(
  standard_cd |> select(borocd, borough_code, borough_name),
  yearbuilt = 1980:2025
) |>
  left_join(panel_base, by = c("borocd", "borough_code", "borough_name", "yearbuilt"), relationship = "one-to-one") |>
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
  ) |>
  arrange(borocd, yearbuilt)

write_csv(panel, "../output/mappluto_construction_proxy_cd_year.csv", na = "")

cat("Wrote MapPLUTO construction proxy outputs to ../output\n")
