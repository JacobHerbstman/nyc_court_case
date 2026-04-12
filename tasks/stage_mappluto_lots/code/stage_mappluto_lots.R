# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_mappluto_lots/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# mappluto_files_csv <- "../input/mappluto_files.csv"
# out_index_csv <- "../output/mappluto_lot_files.csv"
# out_qc_csv <- "../output/mappluto_lot_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(sf)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4) {
  stop("Expected 4 arguments: source_catalog_csv mappluto_files_csv out_index_csv out_qc_csv")
}

source_catalog_csv <- args[1]
mappluto_files_csv <- args[2]
out_index_csv <- args[3]
out_qc_csv <- args[4]

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
mappluto_files <- read_csv(mappluto_files_csv, show_col_types = FALSE, na = c("", "NA"))

jia_codes <- c(164L, 226L, 227L, 228L, 355L, 356L, 480L, 481L, 482L, 483L, 484L, 595L)

extract_mappluto_table <- function(raw_path) {
  read_path <- raw_path
  temp_dir <- NULL

  if (str_detect(raw_path, "\\.zip$")) {
    temp_dir <- tempfile(pattern = "mappluto_")
    dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
    unzip_status <- suppressWarnings(system2("unzip", c("-oq", raw_path, "-d", temp_dir), stdout = FALSE, stderr = FALSE))
    if (!identical(unzip_status, 0L)) {
      stop("System unzip failed for ", raw_path)
    }
    shp_candidates <- list.files(temp_dir, pattern = "\\.(shp|gpkg)$", recursive = TRUE, full.names = TRUE)
    if (length(shp_candidates) == 0) {
      stop("No .shp or .gpkg found in ", raw_path)
    }
    read_path <- shp_candidates[1]
  }

  pluto <- st_read(read_path, quiet = TRUE, stringsAsFactors = FALSE) |>
    st_drop_geometry() |>
    as_tibble()

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

  lot_table$bbl[is.na(lot_table$bbl)] <- build_bbl(lot_table$borough, lot_table$block, lot_table$lot)[is.na(lot_table$bbl)]
  lot_table$is_joint_interest_area <- suppressWarnings(as.integer(lot_table$cd)) %in% jia_codes
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
  filter(file_role == "mappluto_shapefile_zip", file.exists(raw_path))

if (nrow(available_rows) == 0) {
  write_csv(tibble(), out_index_csv, na = "")
  write_csv(
    tibble(
      source_id = source_catalog$source_id[str_detect(source_catalog$source_id, "^dcp_mappluto_")],
      status = "no_raw_files_found"
    ),
    out_qc_csv,
    na = ""
  )
  quit(save = "no")
}

available_rows <- available_rows |> mutate(raw_path = as.character(raw_path), vintage = as.character(vintage))

index_rows <- list()
qc_rows <- list()

for (i in seq_len(nrow(available_rows))) {
  row <- available_rows[i, ]
  vintage_stub <- sanitize_file_stub(paste(row$source_id, row$vintage, sep = "_"))
  out_parquet_local <- file.path("..", "output", paste0(vintage_stub, ".parquet"))
  out_parquet <- file.path("..", "..", "stage_mappluto_lots", "output", paste0(vintage_stub, ".parquet"))

  lot_table <- extract_mappluto_table(row$raw_path)
  lot_table <- lot_table |>
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
    parquet_path = out_parquet,
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
    ordinary_cd_rows = sum(!lot_table$is_joint_interest_area, na.rm = TRUE),
    joint_interest_area_rows = sum(lot_table$is_joint_interest_area, na.rm = TRUE),
    min_yearbuilt = safe_min_int(lot_table$yearbuilt),
    max_yearbuilt = safe_max_int(lot_table$yearbuilt)
  )
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote MapPLUTO staging outputs to", dirname(out_index_csv), "\n")
