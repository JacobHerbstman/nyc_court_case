# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_dcp_boundaries/code")
# dcp_boundary_index_csv <- "../input/dcp_boundary_index.csv"
# dcp_boundary_qc_csv <- "../input/dcp_boundary_qc.csv"
# out_summary_csv <- "../output/dcp_boundary_audit_summary.csv"
# out_variable_csv <- "../output/dcp_boundary_variable_quality.csv"
# out_anomaly_csv <- "../output/dcp_boundary_anomalies.csv"
# out_figures_pdf <- "../output/dcp_boundary_figures.pdf"
# out_maps_pdf <- "../output/dcp_boundary_maps.pdf"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(sf)
  library(tibble)
})

safe_union_area <- function(x) {
  tryCatch(
    as.numeric(st_area(st_union(x))),
    error = function(e) NA_real_
  )
}

safe_symdiff_area <- function(x, y) {
  tryCatch(
    as.numeric(st_area(st_sym_difference(st_union(x), st_union(y)))),
    error = function(e) NA_real_
  )
}

hex_to_raw <- function(x) {
  if (is.na(x) || x == "") {
    return(as.raw())
  }

  as.raw(strtoi(substring(x, seq(1, nchar(x), by = 2), seq(2, nchar(x), by = 2)), 16L))
}

read_boundary_geometry <- function(staged_df, crs_value) {
  if ("geometry_wkb_hex" %in% names(staged_df) && any(!is.na(staged_df$geometry_wkb_hex))) {
    wkb_list <- lapply(staged_df$geometry_wkb_hex, hex_to_raw)
    class(wkb_list) <- c("WKB", class(wkb_list))
    return(st_as_sfc(wkb_list, EWKB = TRUE, crs = crs_value))
  }

  st_as_sfc(staged_df$geometry_wkt, crs = crs_value)
}

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 7) {
  stop("Expected 7 arguments: dcp_boundary_index_csv dcp_boundary_qc_csv out_summary_csv out_variable_csv out_anomaly_csv out_figures_pdf out_maps_pdf")
}

dcp_boundary_index_csv <- args[1]
dcp_boundary_qc_csv <- args[2]
out_summary_csv <- args[3]
out_variable_csv <- args[4]
out_anomaly_csv <- args[5]
out_figures_pdf <- args[6]
out_maps_pdf <- args[7]

index_df <- read_csv(dcp_boundary_index_csv, show_col_types = FALSE, na = c("", "NA"))
qc_df <- read_csv(dcp_boundary_qc_csv, show_col_types = FALSE, na = c("", "NA"))

parquet_rows <- index_df[!is.na(index_df$parquet_path) & file.exists(index_df$parquet_path), ]

if (nrow(parquet_rows) == 0) {
  write_csv(tibble(status = "no_staged_boundary_files"), out_summary_csv, na = "")
  write_csv(tibble(), out_variable_csv, na = "")
  write_csv(tibble(), out_anomaly_csv, na = "")
  pdf(out_figures_pdf, width = 10, height = 7)
  plot.new()
  text(0.5, 0.5, "No staged boundary files available")
  dev.off()
  pdf(out_maps_pdf, width = 10, height = 7)
  plot.new()
  text(0.5, 0.5, "No staged boundary files available")
  dev.off()
  quit(save = "no")
}

boundary_list <- lapply(seq_len(nrow(parquet_rows)), function(i) {
  row <- parquet_rows[i, ]
  staged_df <- read_parquet(row$parquet_path) %>%
    as.data.frame() %>%
    as_tibble()
  crs_epsg <- unique(staged_df$crs_epsg)
  crs_epsg <- crs_epsg[!is.na(crs_epsg)][1]
  crs_value <- if (length(crs_epsg) == 0 || is.na(crs_epsg)) 4326 else crs_epsg

  boundary_sf <- staged_df %>%
    mutate(geometry = read_boundary_geometry(staged_df, crs_value)) %>%
    st_as_sf()

  list(
    source_id = row$source_id,
    pull_date = row$pull_date,
    data = boundary_sf
  )
})

summary_rows <- list()
variable_rows <- list()
anomaly_rows <- list()

for (i in seq_along(boundary_list)) {
  boundary_item <- boundary_list[[i]]
  boundary_raw <- boundary_item$data %>%
    mutate(
      district_id = ifelse(is.na(district_id) | trimws(as.character(district_id)) == "", NA_character_, trimws(as.character(district_id))),
      district_name = ifelse(is.na(district_name) | trimws(as.character(district_name)) == "", NA_character_, trimws(as.character(district_name)))
    )
  qc_row <- qc_df %>%
    filter(source_id == boundary_item$source_id, pull_date == boundary_item$pull_date) %>%
    slice_head(n = 1)
  invalid_geometry_count <- if (nrow(qc_row) == 0) NA_real_ else qc_row$invalid_geometry_count[1]
  boundary_sf <- boundary_raw %>%
    st_make_valid()
  boundary_proj <- suppressWarnings(st_transform(boundary_sf, 2263)) %>%
    st_buffer(0)
  area_values <- as.numeric(st_area(boundary_proj))
  union_area <- safe_union_area(boundary_proj)
  overlap_area <- if (is.na(union_area)) NA_real_ else max(0, sum(area_values, na.rm = TRUE) - union_area)

  summary_rows[[i]] <- tibble(
    source_id = boundary_item$source_id,
    pull_date = boundary_item$pull_date,
    district_count = nrow(boundary_sf),
    total_area = sum(area_values, na.rm = TRUE),
    union_area = union_area,
    overlap_area = overlap_area,
    invalid_geometry_count = invalid_geometry_count,
    missing_district_id_count = sum(is.na(boundary_sf$district_id)),
    duplicate_district_id_count = sum(duplicated(boundary_sf$district_id) & !is.na(boundary_sf$district_id))
  )

  variable_rows[[i]] <- bind_rows(
    tibble(
      source_id = boundary_item$source_id,
      pull_date = boundary_item$pull_date,
      variable = c("district_id", "district_name", "shape_length", "shape_area", "geometry_wkb_hex"),
      missing_share = c(
        mean(is.na(boundary_sf$district_id)),
        mean(is.na(boundary_sf$district_name)),
        mean(is.na(boundary_sf$shape_length)),
        mean(is.na(boundary_sf$shape_area)),
        mean(is.na(boundary_sf$geometry_wkb_hex))
      )
    )
  )

  anomaly_rows[[i]] <- bind_rows(
    boundary_sf %>%
      filter(is.na(district_id) | district_id == "") %>%
      st_drop_geometry() %>%
      transmute(source_id = boundary_item$source_id, pull_date = boundary_item$pull_date, district_id, issue = "missing_district_id"),
    boundary_sf %>%
      filter(duplicated(district_id) | duplicated(district_id, fromLast = TRUE), !is.na(district_id), district_id != "") %>%
      st_drop_geometry() %>%
      transmute(source_id = boundary_item$source_id, pull_date = boundary_item$pull_date, district_id, issue = "duplicate_district_id"),
    tibble(
      source_id = boundary_item$source_id,
      pull_date = boundary_item$pull_date,
      district_id = NA_character_,
      issue = if (!is.na(overlap_area) && overlap_area > 0) "overlap_area_positive" else if (is.na(union_area)) "union_area_failed" else NA_character_
    ) %>%
      filter(!is.na(issue))
  )
}

summary_df <- bind_rows(summary_rows)

if (all(c("dcp_boundary_community_districts", "dcp_boundary_city_council_districts") %in% summary_df$source_id)) {
  cd_sf <- boundary_list[[which(vapply(boundary_list, function(x) x$source_id == "dcp_boundary_community_districts", logical(1)))[1]]]$data %>%
    st_make_valid() %>%
    st_transform(2263) %>%
    st_buffer(0)
  council_sf <- boundary_list[[which(vapply(boundary_list, function(x) x$source_id == "dcp_boundary_city_council_districts", logical(1)))[1]]]$data %>%
    st_make_valid() %>%
    st_transform(2263) %>%
    st_buffer(0)
  coverage_diff_area <- safe_symdiff_area(cd_sf, council_sf)
  summary_df <- summary_df %>%
    mutate(coverage_difference_area_vs_other_layer = coverage_diff_area)
}

variable_df <- bind_rows(variable_rows)
anomaly_df <- bind_rows(anomaly_rows)

write_csv(summary_df, out_summary_csv, na = "")
write_csv(variable_df, out_variable_csv, na = "")
write_csv(anomaly_df, out_anomaly_csv, na = "")

summary_plot_df <- summary_df %>%
  mutate(source_label = if_else(source_id == "dcp_boundary_community_districts", "Community Districts", "City Council Districts"))

pdf(out_figures_pdf, width = 11, height = 8.5)

print(
  ggplot(summary_plot_df, aes(x = source_label, y = district_count, fill = source_label)) +
    geom_col(show.legend = FALSE) +
    labs(title = "Boundary District Counts", x = NULL, y = "District count") +
    theme_minimal(base_size = 12)
)

area_plot_df <- bind_rows(lapply(boundary_list, function(x) {
  boundary_sf <- x$data %>%
    st_make_valid() %>%
    st_transform(2263) %>%
    st_buffer(0)

  boundary_sf %>%
    mutate(source_id = x$source_id, area_sqft = as.numeric(st_area(boundary_sf))) %>%
    st_drop_geometry() %>%
    as_tibble()
}))

print(
  ggplot(area_plot_df, aes(x = area_sqft, fill = source_id)) +
    geom_histogram(bins = 40, alpha = 0.7, position = "identity") +
    labs(title = "Boundary Area Distribution", x = "Area in square feet", y = "District count", fill = NULL) +
    theme_minimal(base_size = 12)
)

dev.off()

pdf(out_maps_pdf, width = 11, height = 8.5)

for (boundary_item in boundary_list) {
  boundary_sf <- boundary_item$data %>%
    st_make_valid() %>%
    st_transform(2263) %>%
    st_buffer(0)
  label_points <- suppressWarnings(st_point_on_surface(boundary_sf))

  print(
    ggplot(boundary_sf) +
      geom_sf(fill = "#d9e6f2", color = "#3b4c63", linewidth = 0.15) +
      geom_sf_text(data = label_points, aes(label = district_id), size = 2) +
      labs(
        title = ifelse(
          boundary_item$source_id == "dcp_boundary_community_districts",
          "Community District Boundaries",
          "City Council District Boundaries"
        )
      ) +
      theme_void(base_size = 12)
  )
}

dev.off()

cat("Wrote DCP boundary summary outputs to", dirname(out_summary_csv), "\n")
