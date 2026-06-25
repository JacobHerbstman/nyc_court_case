# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_dcp_boundaries/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(sf)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

hex_to_raw <- function(x) {
  if (is.na(x) || x == "") {
    return(as.raw())
  }

  as.raw(strtoi(substring(x, seq(1, nchar(x), by = 2), seq(2, nchar(x), by = 2)), 16L))
}

boundary_files <- read_csv("../input/dcp_boundary_raw_files.csv", show_col_types = FALSE, na = c("", "NA"))
boundary_files <- boundary_files[!is.na(boundary_files$raw_parquet_path) & file.exists(boundary_files$raw_parquet_path), ]
boundary_files <- boundary_files[boundary_files$source_id == "dcp_boundary_community_districts", ]

if (nrow(boundary_files) == 0) {
  write_csv(tibble(), "../output/dcp_boundary_index.csv", na = "")
  quit(save = "no")
}

index_rows <- list()

for (i in seq_len(nrow(boundary_files))) {
  row <- boundary_files[i, ]
  boundary_df <- read_parquet(row$raw_parquet_path) |>
    as.data.frame() |>
    as_tibble()

  wkb_list <- lapply(boundary_df$raw_geometry_wkb_hex, hex_to_raw)
  class(wkb_list) <- c("WKB", class(wkb_list))
  boundary_geom <- st_as_sfc(wkb_list, EWKB = TRUE, crs = boundary_df$raw_crs_epsg[1])
  boundary_sf <- st_sf(boundary_df, geometry = boundary_geom, crs = boundary_df$raw_crs_epsg[1])

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

  boundary_df <- boundary_sf |>
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
      source_raw_path = row$raw_path,
      raw_parquet_path = row$raw_parquet_path
    ) |>
    st_drop_geometry() |>
    as_tibble() |>
    select(source_id, pull_date, source_raw_path, raw_parquet_path, district_id, district_name, shape_length, shape_area, crs_epsg, geometry_wkb_hex, geometry_wkt, everything())

  out_parquet_local <- file.path("..", "output", paste0(sanitize_file_stub(paste(row$source_id, row$pull_date, sep = "_")), ".parquet"))
  out_parquet <- file.path("..", "..", "build_dcp_boundaries", "output", basename(out_parquet_local))
  write_parquet_if_changed(boundary_df, out_parquet_local)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    pull_date = row$pull_date,
    raw_path = row$raw_path,
    raw_parquet_path = row$raw_parquet_path,
    parquet_path = out_parquet
  )
}

write_csv(bind_rows(index_rows), "../output/dcp_boundary_index.csv", na = "")

cat("Wrote DCP boundary outputs to ../output\n")
