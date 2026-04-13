# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_mappluto_raw/code")
# mappluto_files_csv <- "../input/mappluto_files.csv"
# out_index_csv <- "../output/mappluto_raw_files.csv"
# out_qc_csv <- "../output/mappluto_raw_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(foreign)
  library(readr)
  library(sf)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: mappluto_files_csv out_index_csv out_qc_csv")
}

mappluto_files_csv <- args[1]
out_index_csv <- args[2]
out_qc_csv <- args[3]

mappluto_files <- read_csv(mappluto_files_csv, show_col_types = FALSE, na = c("", "NA"))

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
    council = pick_first_existing(pluto, c("council")),
    unitsres = pick_first_existing(pluto, c("unitsres")),
    unitstotal = pick_first_existing(pluto, c("unitstotal")),
    yearbuilt = pick_first_existing(pluto, c("yearbuilt")),
    yearalter1 = pick_first_existing(pluto, c("yearalter1")),
    yearalter2 = pick_first_existing(pluto, c("yearalter2")),
    bldgarea = pick_first_existing(pluto, c("bldgarea")),
    resarea = pick_first_existing(pluto, c("resarea")),
    landuse = pick_first_existing(pluto, c("landuse")),
    bldgclass = pick_first_existing(pluto, c("bldgclass"))
  )

  missing_bbl <- is.na(lot_table$bbl)
  lot_table$bbl[missing_bbl] <- build_bbl(lot_table$borough, lot_table$block, lot_table$lot)[missing_bbl]
  lot_table
}

safe_min_int <- function(x) {
  x <- suppressWarnings(as.integer(x))
  if (all(is.na(x))) {
    return(NA_integer_)
  }
  min(x, na.rm = TRUE)
}

safe_max_int <- function(x) {
  x <- suppressWarnings(as.integer(x))
  if (all(is.na(x))) {
    return(NA_integer_)
  }
  max(x, na.rm = TRUE)
}

available_rows <- mappluto_files |>
  filter(file_role == "mappluto_shapefile_zip", file.exists(raw_path)) |>
  mutate(raw_path = as.character(raw_path), vintage = as.character(vintage))

if (nrow(available_rows) == 0) {
  write_csv(tibble(), out_index_csv, na = "")
  write_csv(tibble(), out_qc_csv, na = "")
  quit(save = "no")
}

index_rows <- list()
qc_rows <- list()

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

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    vintage = row$vintage,
    raw_path = row$raw_path,
    raw_parquet_path = out_parquet,
    file_role = row$file_role
  )

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    vintage = row$vintage,
    row_count = nrow(lot_table),
    nonmissing_bbl_share = mean(!is.na(lot_table$bbl)),
    nonmissing_cd_share = mean(!is.na(lot_table$cd)),
    nonmissing_council_share = mean(!is.na(lot_table$council)),
    nonmissing_unitsres_share = mean(!is.na(lot_table$unitsres)),
    nonmissing_yearbuilt_share = mean(!is.na(lot_table$yearbuilt)),
    min_raw_yearbuilt = safe_min_int(lot_table$yearbuilt),
    max_raw_yearbuilt = safe_max_int(lot_table$yearbuilt)
  )
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote raw MapPLUTO load outputs to", dirname(out_index_csv), "\n")
