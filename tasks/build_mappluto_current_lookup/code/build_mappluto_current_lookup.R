# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_mappluto_current_lookup/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(foreign)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

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

read_mappluto_dbf <- function(raw_path) {
  zip_listing <- unzip(raw_path, list = TRUE) |>
    as_tibble()

  dbf_entry <- zip_listing |>
    filter(str_detect(tolower(Name), "[.]dbf$")) |>
    arrange(Name) |>
    pull(Name) |>
    first()

  if (is.na(dbf_entry) || !nzchar(dbf_entry)) {
    stop("No DBF found in ", raw_path)
  }

  temp_dir <- tempfile(pattern = "mappluto_current_lookup_")
  dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  suppressWarnings(unzip(raw_path, files = dbf_entry, exdir = temp_dir, junkpaths = TRUE))
  read_path <- file.path(temp_dir, basename(dbf_entry))

  if (!file.exists(read_path)) {
    stop("Could not extract DBF from ", raw_path)
  }

  read.dbf(read_path, as.is = TRUE) |>
    as_tibble()
}

mappluto_row <- read_csv("../input/mappluto_files.csv", show_col_types = FALSE, na = c("", "NA")) |>
  filter(
    source_id == "dcp_mappluto_current",
    vintage == "25v4",
    file_role == "mappluto_shapefile_zip",
    status %in% c("downloaded", "already_present", "redownloaded_after_validation_failure"),
    !is.na(raw_path),
    file.exists(raw_path)
  ) |>
  arrange(raw_path)

if (nrow(mappluto_row) != 1) {
  stop("Current 25v4 MapPLUTO shapefile zip lookup is not unique in ../input/mappluto_files.csv")
}

mappluto_attr <- read_mappluto_dbf(mappluto_row$raw_path[[1]])
names(mappluto_attr) <- normalize_names(names(mappluto_attr))

jia_codes <- c(164L, 226L, 227L, 228L, 355L, 356L, 480L, 481L, 482L, 483L, 484L, 595L)

lookup <- mappluto_attr |>
  transmute(
    source_id = "dcp_mappluto_current",
    source_vintage = "25v4",
    source_raw_path = mappluto_row$raw_path[[1]],
    bbl = coalesce_character(
      normalize_text_field(pick_first_existing(pick(everything()), c("bbl"))),
      build_bbl(
        pick_first_existing(pick(everything()), c("borough", "boro_code", "borocode")),
        pick_first_existing(pick(everything()), c("block")),
        pick_first_existing(pick(everything()), c("lot"))
      )
    ),
    borough = standardize_borough_code(pick_first_existing(pick(everything()), c("borough", "boro_code", "borocode"))),
    block = normalize_integer_field(pick_first_existing(pick(everything()), c("block"))),
    lot = normalize_integer_field(pick_first_existing(pick(everything()), c("lot"))),
    address = normalize_text_field(pick_first_existing(pick(everything()), c("address"))),
    cd = normalize_integer_field(pick_first_existing(pick(everything()), c("cd"))),
    council = normalize_integer_field(pick_first_existing(pick(everything()), c("council"))),
    yearbuilt = normalize_year_field(pick_first_existing(pick(everything()), c("yearbuilt"))),
    unitsres = normalize_numeric_field(pick_first_existing(pick(everything()), c("unitsres"))),
    unitstotal = normalize_numeric_field(pick_first_existing(pick(everything()), c("unitstotal"))),
    resarea = normalize_numeric_field(pick_first_existing(pick(everything()), c("resarea"))),
    bldgarea = normalize_numeric_field(pick_first_existing(pick(everything()), c("bldgarea"))),
    lotarea = normalize_numeric_field(pick_first_existing(pick(everything()), c("lotarea"))),
    builtfar = normalize_numeric_field(pick_first_existing(pick(everything()), c("builtfar"))),
    numbldgs = normalize_integer_field(pick_first_existing(pick(everything()), c("numbldgs"))),
    numfloors = normalize_numeric_field(pick_first_existing(pick(everything()), c("numfloors"))),
    landuse = normalize_text_field(pick_first_existing(pick(everything()), c("landuse"))),
    bldgclass = normalize_text_field(pick_first_existing(pick(everything()), c("bldgclass"))),
    is_joint_interest_area = cd %in% jia_codes
  ) |>
  arrange(bbl)

if (any(is.na(lookup$bbl))) {
  stop("Current MapPLUTO lookup has missing BBLs.")
}

write_parquet_if_changed(lookup, "../output/mappluto_current_lot_lookup.parquet")

cat("Wrote current MapPLUTO lot lookup to ../output\n")
