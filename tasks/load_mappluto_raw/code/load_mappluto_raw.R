# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_mappluto_raw/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(foreign)
  library(readr)
  library(sf)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

mappluto_files <- read_csv("../input/mappluto_files.csv", show_col_types = FALSE, na = c("", "NA"))

extract_mappluto_release_from_path <- function(path) {
  release <- str_match(
    tolower(basename(path)),
    "nyc_mappluto_([0-9]{2}v[0-9]+(?:_[0-9]+)?)(?:_arc)?_shp[.]zip$"
  )[, 2]

  str_replace_all(release, "_", ".")
}

mappluto_vintage_matches_file <- function(vintage, file_release) {
  vintage_clean <- str_replace_all(tolower(as.character(vintage)), "_", ".")
  file_clean <- str_replace_all(tolower(as.character(file_release)), "_", ".")

  !is.na(file_clean) & nzchar(file_clean) &
    (vintage_clean == file_clean | startsWith(vintage_clean, file_clean))
}

zip_has_valid_listing <- function(path) {
  if (!str_detect(tolower(path), "[.]zip$")) {
    return(TRUE)
  }

  listing <- suppressWarnings(system2("unzip", c("-Z1", path), stdout = TRUE, stderr = FALSE))
  status <- attr(listing, "status")

  if (is.null(status)) {
    status <- 0L
  }

  identical(status, 0L) && length(listing) > 0
}

extract_mappluto_table <- function(raw_path) {
  read_path <- raw_path
  read_mode <- if (str_detect(tolower(raw_path), "\\.gpkg$")) "gpkg" else "dbf"
  temp_dir <- NULL

  if (str_detect(raw_path, "\\.zip$")) {
    temp_dir <- tempfile(pattern = "mappluto_")
    dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
    zip_listing <- suppressWarnings(system2("unzip", c("-Z1", raw_path), stdout = TRUE, stderr = FALSE))
    dbf_entry <- zip_listing[str_detect(tolower(zip_listing), "\\.dbf$")][1]
    gpkg_entry <- zip_listing[str_detect(tolower(zip_listing), "\\.gpkg$")][1]

    if (!is.na(dbf_entry) && nzchar(dbf_entry)) {
      unzip_status <- suppressWarnings(system2("unzip", c("-oj", raw_path, dbf_entry, "-d", temp_dir), stdout = FALSE, stderr = FALSE))
      if (!identical(unzip_status, 0L)) {
        stop("System unzip failed for DBF in ", raw_path)
      }
      read_path <- file.path(temp_dir, basename(dbf_entry))
      read_mode <- "dbf"
    } else if (!is.na(gpkg_entry) && nzchar(gpkg_entry)) {
      unzip_status <- suppressWarnings(system2("unzip", c("-oj", raw_path, gpkg_entry, "-d", temp_dir), stdout = FALSE, stderr = FALSE))
      if (!identical(unzip_status, 0L)) {
        stop("System unzip failed for GPKG in ", raw_path)
      }
      read_path <- file.path(temp_dir, basename(gpkg_entry))
      read_mode <- "gpkg"
    } else {
      stop("No .dbf or .gpkg found in ", raw_path)
    }
  } else if (str_detect(tolower(raw_path), "\\.dbf$")) {
    read_mode <- "dbf"
  } else if (!str_detect(tolower(raw_path), "\\.gpkg$")) {
    dbf_candidates <- list.files(dirname(raw_path), pattern = "\\.dbf$", recursive = FALSE, full.names = TRUE)
    if (length(dbf_candidates) > 0) {
      read_path <- dbf_candidates[1]
      read_mode <- "dbf"
    } else {
      stop("Unsupported MapPLUTO raw path without .dbf or .gpkg: ", raw_path)
    }
  }

  pluto <- if (read_mode == "dbf") {
    read.dbf(read_path, as.is = TRUE) |>
      as_tibble()
  } else {
    st_read(read_path, quiet = TRUE, stringsAsFactors = FALSE) |>
      st_drop_geometry() |>
      as_tibble()
  }

  names(pluto) <- normalize_names(names(pluto))

  lot_table <- tibble(
    bbl = pick_first_existing(pluto, c("bbl")),
    borough = pick_first_existing(pluto, c("borough", "boro_code", "borocode")),
    block = pick_first_existing(pluto, c("block")),
    lot = pick_first_existing(pluto, c("lot")),
    address = pick_first_existing(pluto, c("address")),
    cd = pick_first_existing(pluto, c("cd")),
    zipcode = pick_first_existing(pluto, c("zipcode", "zip_code", "zip")),
    ct2010 = pick_first_existing(pluto, c("ct2010", "tract2010")),
    cb2010 = pick_first_existing(pluto, c("cb2010")),
    schooldist = pick_first_existing(pluto, c("schooldist", "school_dist")),
    council = pick_first_existing(pluto, c("council")),
    zonedist1 = pick_first_existing(pluto, c("zonedist1")),
    zonedist2 = pick_first_existing(pluto, c("zonedist2")),
    zonedist3 = pick_first_existing(pluto, c("zonedist3")),
    zonedist4 = pick_first_existing(pluto, c("zonedist4")),
    overlay1 = pick_first_existing(pluto, c("overlay1")),
    overlay2 = pick_first_existing(pluto, c("overlay2")),
    spdist1 = pick_first_existing(pluto, c("spdist1")),
    spdist2 = pick_first_existing(pluto, c("spdist2")),
    spdist3 = pick_first_existing(pluto, c("spdist3")),
    ltdheight = pick_first_existing(pluto, c("ltdheight")),
    splitzone = pick_first_existing(pluto, c("splitzone")),
    zonemap = pick_first_existing(pluto, c("zonemap")),
    zmcode = pick_first_existing(pluto, c("zmcode")),
    lotarea = pick_first_existing(pluto, c("lotarea")),
    unitsres = pick_first_existing(pluto, c("unitsres")),
    unitstotal = pick_first_existing(pluto, c("unitstotal")),
    comarea = pick_first_existing(pluto, c("comarea")),
    yearbuilt = pick_first_existing(pluto, c("yearbuilt")),
    yearalter1 = pick_first_existing(pluto, c("yearalter1")),
    yearalter2 = pick_first_existing(pluto, c("yearalter2")),
    bldgarea = pick_first_existing(pluto, c("bldgarea")),
    resarea = pick_first_existing(pluto, c("resarea")),
    officearea = pick_first_existing(pluto, c("officearea")),
    retailarea = pick_first_existing(pluto, c("retailarea")),
    garagearea = pick_first_existing(pluto, c("garagearea")),
    strgearea = pick_first_existing(pluto, c("strgearea")),
    factryarea = pick_first_existing(pluto, c("factryarea")),
    otherarea = pick_first_existing(pluto, c("otherarea")),
    areasource = pick_first_existing(pluto, c("areasource")),
    numbldgs = pick_first_existing(pluto, c("numbldgs")),
    numfloors = pick_first_existing(pluto, c("numfloors")),
    lotfront = pick_first_existing(pluto, c("lotfront")),
    lotdepth = pick_first_existing(pluto, c("lotdepth")),
    bldgfront = pick_first_existing(pluto, c("bldgfront")),
    bldgdepth = pick_first_existing(pluto, c("bldgdepth")),
    appdate = pick_first_existing(pluto, c("appdate")),
    assessland = pick_first_existing(pluto, c("assessland")),
    assesstot = pick_first_existing(pluto, c("assesstot")),
    exempttot = pick_first_existing(pluto, c("exempttot")),
    histdist = pick_first_existing(pluto, c("histdist")),
    landmark = pick_first_existing(pluto, c("landmark")),
    builtfar = pick_first_existing(pluto, c("builtfar")),
    residfar = pick_first_existing(pluto, c("residfar")),
    commfar = pick_first_existing(pluto, c("commfar")),
    facilfar = pick_first_existing(pluto, c("facilfar")),
    firm07_flag = pick_first_existing(pluto, c("firm07_flag")),
    pfirm15_flag = pick_first_existing(pluto, c("pfirm15_flag")),
    landuse = pick_first_existing(pluto, c("landuse")),
    bldgclass = pick_first_existing(pluto, c("bldgclass"))
  )

  missing_bbl <- is.na(lot_table$bbl)
  lot_table$bbl[missing_bbl] <- build_bbl(lot_table$borough, lot_table$block, lot_table$lot)[missing_bbl]
  lot_table
}

available_rows <- mappluto_files |>
  filter(file_role == "mappluto_shapefile_zip", file.exists(raw_path)) |>
  mutate(
    raw_path = as.character(raw_path),
    vintage = as.character(vintage),
    fetch_status = as.character(status),
    raw_file_release = extract_mappluto_release_from_path(raw_path),
    raw_zip_valid = vapply(raw_path, zip_has_valid_listing, logical(1)),
    status = case_when(
      !fetch_status %in% c("downloaded", "already_present", "redownloaded_after_validation_failure") ~ "upstream_fetch_not_valid",
      !raw_zip_valid ~ "raw_zip_validation_failed",
      is.na(raw_file_release) ~ "release_not_detected_from_filename",
      !mappluto_vintage_matches_file(vintage, raw_file_release) ~ "vintage_file_mismatch",
      TRUE ~ "loadable"
    )
  )

if (nrow(available_rows) == 0) {
  write_csv(tibble(), "../output/mappluto_raw_files.csv", na = "")
  quit(save = "no")
}

index_rows <- list()
row_id <- 1L

invalid_rows <- available_rows |>
  filter(status != "loadable")

if (nrow(invalid_rows) > 0) {
  for (i in seq_len(nrow(invalid_rows))) {
    row <- invalid_rows[i, ]

    index_rows[[row_id]] <- tibble(
      source_id = row$source_id,
      vintage = row$vintage,
      raw_path = row$raw_path,
      raw_parquet_path = NA_character_,
      file_role = row$file_role,
      raw_file_release = row$raw_file_release,
      fetch_status = row$fetch_status,
      raw_zip_valid = row$raw_zip_valid,
      status = row$status
    )

    row_id <- row_id + 1L
  }
}

available_rows <- available_rows |>
  filter(status == "loadable")

for (i in seq_len(nrow(available_rows))) {
  row <- available_rows[i, ]
  raw_stub <- paste(sanitize_file_stub(paste(row$source_id, row$vintage, sep = "_")), "raw", sep = "_")
  out_parquet_local <- file.path("..", "output", paste0(raw_stub, ".parquet"))
  out_parquet <- file.path("..", "..", "load_mappluto_raw", "output", paste0(raw_stub, ".parquet"))

  lot_table <- extract_mappluto_table(row$raw_path) |>
    mutate(
      source_id = row$source_id,
      source_vintage = row$vintage,
      source_raw_path = row$raw_path
    ) |>
    select(source_id, source_vintage, source_raw_path, everything())

  write_parquet_if_changed(lot_table, out_parquet_local)

  index_rows[[row_id]] <- tibble(
    source_id = row$source_id,
    vintage = row$vintage,
    raw_path = row$raw_path,
    raw_parquet_path = out_parquet,
    file_role = row$file_role,
    raw_file_release = row$raw_file_release,
    fetch_status = row$fetch_status,
    raw_zip_valid = row$raw_zip_valid,
    status = "loaded"
  )

  row_id <- row_id + 1L
}

write_csv(bind_rows(index_rows), "../output/mappluto_raw_files.csv", na = "")
cat("Wrote raw MapPLUTO load outputs to ../output\n")
