suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(sf)
  library(stringr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

council_measure <- read_csv("../input/ccdist2010_homeownership_1990_measure.csv", show_col_types = FALSE, na = c("", "NA"))

council_sf <- council_measure |>
  transmute(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    council_district = suppressWarnings(as.integer(council_district)),
    geometry = st_as_sfc(geometry_wkt, crs = 2263)
  ) |>
  st_as_sf() |>
  arrange(council_district)

if (anyDuplicated(council_sf$district_id)) {
  stop("Council district treatment input is not unique by district_id.")
}

normalize_text_field <- function(x) {
  out <- trimws(as.character(x))
  out[out %in% c("", "NA", "N/A", "NULL")] <- NA_character_
  out
}

normalize_integer_field <- function(x) {
  suppressWarnings(as.integer(normalize_text_field(x)))
}

read_mappluto_sf <- function(raw_path) {
  if (!str_detect(tolower(raw_path), "[.]zip$")) {
    return(st_read(raw_path, quiet = TRUE, stringsAsFactors = FALSE))
  }

  temp_dir <- tempfile(pattern = "mappluto_sf_")
  dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  suppressWarnings(unzip(raw_path, exdir = temp_dir))
  shp_path <- list.files(temp_dir, pattern = "[.]shp$", recursive = TRUE, full.names = TRUE)[1]

  if (is.na(shp_path) || !nzchar(shp_path)) {
    stop("No shapefile found in ", raw_path)
  }

  st_read(shp_path, quiet = TRUE, stringsAsFactors = FALSE)
}

mappluto_row <- read_csv("../input/mappluto_files.csv", show_col_types = FALSE, na = c("", "NA")) |>
  filter(
    source_id == "dcp_mappluto_current",
    vintage == "25v4",
    file_role == "mappluto_shapefile_zip",
    status %in% c("downloaded", "already_present", "redownloaded_after_validation_failure"),
    !is.na(raw_path)
  ) |>
  arrange(raw_path)

if (nrow(mappluto_row) != 1) {
  stop("Current 25v4 MapPLUTO shapefile zip lookup is not unique in ../input/mappluto_files.csv")
}

mappluto_raw_path <- mappluto_row$raw_path[[1]]
if (!file.exists(mappluto_raw_path) && file.exists(file.path("..", mappluto_raw_path))) {
  mappluto_raw_path <- file.path("..", mappluto_raw_path)
}
if (!file.exists(mappluto_raw_path)) {
  stop("Could not find current 25v4 MapPLUTO shapefile zip from ../input/mappluto_files.csv")
}

mappluto_raw_sf <- read_mappluto_sf(mappluto_raw_path)

mappluto_attr <- mappluto_raw_sf |>
  st_drop_geometry() |>
  as_tibble()
names(mappluto_attr) <- normalize_names(names(mappluto_attr))

jia_codes <- c(164L, 226L, 227L, 228L, 355L, 356L, 480L, 481L, 482L, 483L, 484L, 595L)

mappluto_attr <- mappluto_attr |>
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
    cd = normalize_integer_field(pick_first_existing(pick(everything()), c("cd"))),
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
) |>
  bind_cols(
    council_sf |>
      st_drop_geometry() |>
      slice(assigned_row) |>
      select(district_id, council_district)
  )

bbl_lookup <- mappluto_attr |>
  inner_join(mappluto_assignment, by = "row_id", relationship = "one-to-one") |>
  filter(!is_joint_interest_area, !is.na(bbl), !is.na(council_district)) |>
  count(bbl, district_id, council_district, name = "mappluto_lot_rows") |>
  group_by(bbl) |>
  arrange(desc(mappluto_lot_rows), district_id) |>
  slice_head(n = 1) |>
  ungroup() |>
  arrange(bbl)

if (nrow(bbl_lookup) != n_distinct(bbl_lookup$bbl)) {
  stop("2010 Council district MapPLUTO BBL lookup is not unique by BBL.")
}

write_parquet_if_changed(bbl_lookup, "../output/ccdist2010_mappluto_bbl_lookup.parquet")

cat("Wrote 2010 Council district MapPLUTO BBL lookup to ../output\n")
