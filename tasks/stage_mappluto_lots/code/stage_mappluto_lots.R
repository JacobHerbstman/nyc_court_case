# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_mappluto_lots/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# download_audit_csv <- "../input/mappluto_download_audit.csv"
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
  stop("Expected 4 arguments: source_catalog_csv download_audit_csv out_index_csv out_qc_csv")
}

source_catalog_csv <- args[1]
download_audit_csv <- args[2]
out_index_csv <- args[3]
out_qc_csv <- args[4]

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
download_audit <- read_csv(download_audit_csv, show_col_types = FALSE, na = c("", "NA"))

jia_codes <- c(164L, 226L, 227L, 228L, 355L, 356L, 480L, 481L, 482L, 483L, 484L, 595L)

extract_mappluto_table <- function(raw_path) {
  read_path <- raw_path
  temp_dir <- NULL

  if (str_detect(raw_path, "\\.zip$")) {
    temp_dir <- tempfile(pattern = "mappluto_")
    dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
    unzip(raw_path, exdir = temp_dir)
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

available_rows <- download_audit |>
  filter(status == "present", !is.na(raw_path))

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

available_rows <- available_rows |>
  mutate(
    raw_path = as.character(raw_path),
    vintage_key = basename(dirname(raw_path))
  )

index_rows <- list()
qc_rows <- list()

for (i in seq_len(nrow(available_rows))) {
  row <- available_rows[i, ]
  vintage_stub <- sanitize_file_stub(paste(row$source_id, row$vintage_key, sep = "_"))
  out_parquet <- file.path("..", "output", paste0(vintage_stub, ".parquet"))

  lot_table <- extract_mappluto_table(row$raw_path)
  write_parquet_if_changed(lot_table, out_parquet)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    vintage = row$vintage_key,
    raw_path = row$raw_path,
    parquet_path = out_parquet
  )

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    vintage = row$vintage_key,
    row_count = nrow(lot_table),
    nonmissing_bbl_share = mean(!is.na(lot_table$bbl)),
    ordinary_cd_rows = sum(!lot_table$is_joint_interest_area, na.rm = TRUE),
    joint_interest_area_rows = sum(lot_table$is_joint_interest_area, na.rm = TRUE),
    min_yearbuilt = safe_min_int(lot_table$yearbuilt),
    max_yearbuilt = safe_max_int(lot_table$yearbuilt)
  )
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote MapPLUTO staging outputs to", dirname(out_index_csv), "\n")
