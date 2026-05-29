# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/load_dcp_boundaries_raw/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(sf)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

boundary_files <- read_csv("../input/dcp_boundary_files.csv", show_col_types = FALSE, na = c("", "NA")) |>
  filter(file_role == "boundary_shapefile_zip", file.exists(raw_path))

if (nrow(boundary_files) == 0) {
  write_csv_if_changed(tibble(), "../output/dcp_boundary_raw_files.csv")
  quit(save = "no")
}

index_rows <- list()

for (i in seq_len(nrow(boundary_files))) {
  row <- boundary_files[i, ]
  temp_dir <- tempfile(pattern = "boundary_raw_")
  dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
  unzip(row$raw_path, exdir = temp_dir)

  shp_paths <- list.files(temp_dir, pattern = "\\.shp$", recursive = TRUE, full.names = TRUE)
  shp_path <- shp_paths[1]

  if (length(shp_paths) != 1) {
    index_rows[[i]] <- tibble(
      source_id = row$source_id,
      pull_date = row$pull_date,
      raw_path = row$raw_path,
      shapefile_inside_zip = if (length(shp_paths) == 0) NA_character_ else paste(basename(shp_paths), collapse = ";"),
      raw_parquet_path = NA_character_,
      status = if (length(shp_paths) == 0) "shapefile_not_found_in_zip" else "unexpected_shapefile_payload"
    )
    next
  }

  boundary_sf <- st_read(shp_path, quiet = TRUE, stringsAsFactors = FALSE)
  names(boundary_sf) <- normalize_names(names(boundary_sf))

  raw_df <- boundary_sf |>
    mutate(
      source_id = row$source_id,
      pull_date = row$pull_date,
      source_raw_path = row$raw_path,
      raw_crs_epsg = st_crs(boundary_sf)$epsg,
      raw_geometry_wkb_hex = vapply(
        st_as_binary(st_geometry(boundary_sf), EWKB = TRUE),
        function(x) paste(sprintf("%02X", as.integer(x)), collapse = ""),
        character(1)
      )
    ) |>
    st_drop_geometry() |>
    as_tibble() |>
    select(source_id, pull_date, source_raw_path, raw_crs_epsg, raw_geometry_wkb_hex, everything())

  out_parquet_local <- file.path("..", "output", paste0(sanitize_file_stub(paste(row$source_id, row$pull_date, "raw", sep = "_")), ".parquet"))
  out_parquet <- file.path("..", "..", "load_dcp_boundaries_raw", "output", basename(out_parquet_local))
  write_parquet_if_changed(raw_df, out_parquet_local)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    pull_date = row$pull_date,
    raw_path = row$raw_path,
    shapefile_inside_zip = basename(shp_path),
    raw_parquet_path = out_parquet,
    status = "loaded"
  )
}

write_csv_if_changed(bind_rows(index_rows), "../output/dcp_boundary_raw_files.csv")
cat("Wrote raw DCP boundary outputs to ../output\n")
