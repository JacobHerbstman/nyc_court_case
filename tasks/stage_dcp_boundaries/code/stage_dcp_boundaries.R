# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_dcp_boundaries/code")
# dcp_boundary_files_csv <- "../input/dcp_boundary_files.csv"
# out_index_csv <- "../output/dcp_boundary_index.csv"
# out_qc_csv <- "../output/dcp_boundary_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(sf)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: dcp_boundary_files_csv out_index_csv out_qc_csv")
}

dcp_boundary_files_csv <- args[1]
out_index_csv <- args[2]
out_qc_csv <- args[3]

boundary_files <- read_csv(dcp_boundary_files_csv, show_col_types = FALSE, na = c("", "NA")) |>
  filter(file_role == "boundary_shapefile_zip", file.exists(raw_path))

if (nrow(boundary_files) == 0) {
  write_csv(tibble(), out_index_csv, na = "")
  write_csv(tibble(status = "no_boundary_raw_files"), out_qc_csv, na = "")
  quit(save = "no")
}

index_rows <- list()
qc_rows <- list()

for (i in seq_len(nrow(boundary_files))) {
  row <- boundary_files[i, ]
  temp_dir <- tempfile(pattern = "boundary_")
  dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
  unzip(row$raw_path, exdir = temp_dir)

  shp_path <- list.files(temp_dir, pattern = "\\.shp$", recursive = TRUE, full.names = TRUE)[1]

  if (is.na(shp_path)) {
    stop("No shapefile found inside ", row$raw_path)
  }

  boundary_sf <- st_read(shp_path, quiet = TRUE, stringsAsFactors = FALSE)
  names(boundary_sf) <- normalize_names(names(boundary_sf))

  district_id <- if (row$source_id == "dcp_boundary_community_districts") {
    pick_first_existing(boundary_sf, c("borocd", "boro_cd", "cd"))
  } else {
    pick_first_existing(boundary_sf, c("coundist", "coun_dist", "council_di", "district", "council", "coun_dist_1"))
  }

  district_name <- pick_first_existing(boundary_sf, c("name", "boro_name", "district_name"))
  district_id <- ifelse(trimws(as.character(district_id)) == "", NA_character_, trimws(as.character(district_id)))
  district_name <- ifelse(trimws(as.character(district_name)) == "", NA_character_, trimws(as.character(district_name)))
  district_name <- coalesce(district_name, district_id)
  boundary_repaired <- boundary_sf |>
    st_make_valid() |>
    st_transform(2263) |>
    st_buffer(0) |>
    st_transform(st_crs(boundary_sf))

  staged_df <- boundary_sf |>
    mutate(
      district_id = district_id,
      district_name = district_name,
      shape_length = suppressWarnings(as.numeric(pick_first_existing(boundary_sf, c("shape_length", "shape_len")))),
      shape_area = suppressWarnings(as.numeric(pick_first_existing(boundary_sf, c("shape_area", "shape__area")))),
      crs_epsg = st_crs(boundary_sf)$epsg,
      geometry_wkb_hex = vapply(
        st_as_binary(st_geometry(boundary_repaired), EWKB = TRUE),
        function(x) paste(sprintf("%02X", as.integer(x)), collapse = ""),
        character(1)
      ),
      geometry_wkt = as.character(st_as_text(st_geometry(boundary_repaired))),
      source_id = row$source_id,
      pull_date = row$pull_date,
      source_raw_path = row$raw_path
    ) |>
    st_drop_geometry() |>
    as_tibble() |>
    select(source_id, pull_date, source_raw_path, district_id, district_name, shape_length, shape_area, crs_epsg, geometry_wkb_hex, geometry_wkt, everything())

  out_parquet_local <- file.path("..", "output", paste0(sanitize_file_stub(paste(row$source_id, row$pull_date, sep = "_")), ".parquet"))
  out_parquet <- file.path("..", "..", "stage_dcp_boundaries", "output", basename(out_parquet_local))
  write_parquet_if_changed(staged_df, out_parquet_local)

  validity <- st_is_valid(boundary_sf)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    pull_date = row$pull_date,
    raw_path = row$raw_path,
    parquet_path = out_parquet
  )

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    pull_date = row$pull_date,
    district_count = nrow(staged_df),
    duplicated_district_id_count = sum(duplicated(staged_df$district_id) & !is.na(staged_df$district_id) & staged_df$district_id != ""),
    missing_district_id_count = sum(is.na(staged_df$district_id) | staged_df$district_id == ""),
    invalid_geometry_count = sum(!validity, na.rm = TRUE),
    total_area = as.numeric(sum(st_area(boundary_sf), na.rm = TRUE))
  )
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")

cat("Wrote DCP boundary staging outputs to", dirname(out_index_csv), "\n")
