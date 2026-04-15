# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_mappluto_lots/code")
# mappluto_lot_files_csv <- "../input/mappluto_lot_files.csv"
# mappluto_lot_qc_csv <- "../input/mappluto_lot_qc.csv"
# mappluto_checksums_csv <- "../input/mappluto_checksums.csv"
# dcp_boundary_index_csv <- "../input/dcp_boundary_index.csv"
# out_summary_csv <- "../output/mappluto_audit_summary.csv"
# out_variable_csv <- "../output/mappluto_variable_quality.csv"
# out_anomaly_csv <- "../output/mappluto_anomalies.csv"
# out_cd_totals_csv <- "../output/mappluto_cd_totals.csv"
# out_borough_totals_csv <- "../output/mappluto_borough_totals.csv"
# out_figures_pdf <- "../output/mappluto_figures.pdf"
# out_maps_pdf <- "../output/mappluto_maps.pdf"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(sf)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

min_plausible_yearbuilt <- 1800L

hex_to_raw <- function(x) {
  if (is.na(x) || x == "") {
    return(as.raw())
  }

  as.raw(strtoi(substring(x, seq(1, nchar(x), by = 2), seq(2, nchar(x), by = 2)), 16L))
}

read_boundary_geometry <- function(boundary_parquet_df, crs_value) {
  if ("geometry_wkb_hex" %in% names(boundary_parquet_df) && any(!is.na(boundary_parquet_df$geometry_wkb_hex))) {
    wkb_list <- lapply(boundary_parquet_df$geometry_wkb_hex, hex_to_raw)
    class(wkb_list) <- c("WKB", class(wkb_list))
    return(st_as_sfc(wkb_list, EWKB = TRUE, crs = crs_value))
  }

  st_as_sfc(boundary_parquet_df$geometry_wkt, crs = crs_value)
}

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 11) {
  stop("Expected 11 arguments: mappluto_lot_files_csv mappluto_lot_qc_csv mappluto_checksums_csv dcp_boundary_index_csv out_summary_csv out_variable_csv out_anomaly_csv out_cd_totals_csv out_borough_totals_csv out_figures_pdf out_maps_pdf")
}

mappluto_lot_files_csv <- args[1]
mappluto_lot_qc_csv <- args[2]
mappluto_checksums_csv <- args[3]
dcp_boundary_index_csv <- args[4]
out_summary_csv <- args[5]
out_variable_csv <- args[6]
out_anomaly_csv <- args[7]
out_cd_totals_csv <- args[8]
out_borough_totals_csv <- args[9]
out_figures_pdf <- args[10]
out_maps_pdf <- args[11]

mappluto_index <- read_csv(mappluto_lot_files_csv, show_col_types = FALSE, na = c("", "NA"))
mappluto_qc <- read_csv(mappluto_lot_qc_csv, show_col_types = FALSE, na = c("", "NA"))
mappluto_checksums <- read_csv(mappluto_checksums_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  filter(file_role == "mappluto_shapefile_zip")
boundary_index <- read_csv(dcp_boundary_index_csv, show_col_types = FALSE, na = c("", "NA"))

available_rows <- mappluto_index[!is.na(mappluto_index$parquet_path) & file.exists(mappluto_index$parquet_path), ]

if (nrow(available_rows) == 0) {
  write_csv(tibble(status = "no_staged_mappluto_files"), out_summary_csv, na = "")
  write_csv(tibble(), out_variable_csv, na = "")
  write_csv(tibble(), out_anomaly_csv, na = "")
  write_csv(tibble(), out_cd_totals_csv, na = "")
  write_csv(tibble(), out_borough_totals_csv, na = "")
  pdf(out_figures_pdf, width = 10, height = 7)
  plot.new()
  text(0.5, 0.5, "No staged MapPLUTO files available")
  dev.off()
  pdf(out_maps_pdf, width = 10, height = 7)
  plot.new()
  text(0.5, 0.5, "No staged MapPLUTO files available")
  dev.off()
  quit(save = "no")
}

summary_df <- mappluto_qc %>%
  left_join(mappluto_index, by = c("source_id", "vintage")) %>%
  left_join(
    mappluto_checksums %>%
      transmute(source_id, vintage, raw_path, checksum_sha256),
    by = c("source_id", "vintage", "raw_path")
  ) %>%
  arrange(source_id, desc(release_order_key(vintage)), desc(vintage))

variable_df <- bind_rows(lapply(seq_len(nrow(mappluto_qc)), function(i) {
  row <- mappluto_qc[i, ]
  tibble(
    source_id = row$source_id,
    vintage = row$vintage,
    variable = c("bbl", "cd", "council", "unitsres", "yearbuilt"),
    missing_share = c(
      1 - row$nonmissing_bbl_share,
      1 - row$nonmissing_cd_share,
      1 - row$nonmissing_council_share,
      1 - row$nonmissing_unitsres_share,
      1 - row$nonmissing_yearbuilt_share
    )
  )
}))

anomaly_rows <- list()
for (i in seq_len(nrow(available_rows))) {
  row <- available_rows[i, ]
  lot_df <- read_parquet(row$parquet_path) %>%
    as.data.frame() %>%
    as_tibble()

  anomaly_rows[[i]] <- bind_rows(
    tibble(
      source_id = row$source_id,
      vintage = row$vintage,
      issue = "missing_bbl",
      affected_rows = sum(is.na(lot_df$bbl))
    ),
    tibble(
      source_id = row$source_id,
      vintage = row$vintage,
      issue = "missing_cd",
      affected_rows = sum(is.na(lot_df$cd))
    ),
    tibble(
      source_id = row$source_id,
      vintage = row$vintage,
      issue = "missing_council",
      affected_rows = sum(is.na(lot_df$council))
    ),
    tibble(
      source_id = row$source_id,
      vintage = row$vintage,
      issue = "missing_unitsres",
      affected_rows = sum(is.na(lot_df$unitsres))
    ),
    tibble(
      source_id = row$source_id,
      vintage = row$vintage,
      issue = paste0("yearbuilt_outside_", min_plausible_yearbuilt, "_2030"),
      affected_rows = sum(!is.na(suppressWarnings(as.integer(lot_df$yearbuilt))) & (suppressWarnings(as.integer(lot_df$yearbuilt)) < min_plausible_yearbuilt | suppressWarnings(as.integer(lot_df$yearbuilt)) > 2030))
    )
  )
}

anomaly_df <- bind_rows(anomaly_rows) %>%
  filter(affected_rows > 0)

current_row <- available_rows %>%
  filter(source_id == "dcp_mappluto_current") %>%
  mutate(release_rank = release_order_key(vintage)) %>%
  arrange(desc(release_rank), desc(vintage)) %>%
  slice_head(n = 1)

current_df <- if (nrow(current_row) == 0) {
  tibble()
} else {
  read_parquet(current_row$parquet_path[1]) %>%
    as.data.frame() %>%
    as_tibble()
}

cd_totals <- if (nrow(current_df) == 0) {
  tibble()
} else {
  current_df %>%
    mutate(
      cd = suppressWarnings(as.integer(cd)),
      borough = as.character(borough),
      unitsres = suppressWarnings(as.numeric(unitsres)),
      yearbuilt = suppressWarnings(as.integer(yearbuilt))
    ) %>%
    filter(!is.na(cd), !is_joint_interest_area) %>%
    group_by(cd) %>%
    summarise(
      residential_units = sum(unitsres, na.rm = TRUE),
      missing_yearbuilt_share = mean(is.na(yearbuilt)),
      lot_count = n(),
      .groups = "drop"
    ) %>%
    arrange(cd)
}

borough_totals <- if (nrow(current_df) == 0) {
  tibble()
} else {
  current_df %>%
    mutate(
      borough = case_when(
        as.character(borough) %in% c("1", "MN", "MANHATTAN") ~ "Manhattan",
        as.character(borough) %in% c("2", "BX", "BRONX") ~ "Bronx",
        as.character(borough) %in% c("3", "BK", "BROOKLYN") ~ "Brooklyn",
        as.character(borough) %in% c("4", "QN", "QUEENS") ~ "Queens",
        as.character(borough) %in% c("5", "SI", "STATEN ISLAND") ~ "Staten Island",
        TRUE ~ as.character(borough)
      ),
      unitsres = suppressWarnings(as.numeric(unitsres))
    ) %>%
    group_by(borough) %>%
    summarise(residential_units = sum(unitsres, na.rm = TRUE), lot_count = n(), .groups = "drop") %>%
    arrange(borough)
}

write_csv(summary_df, out_summary_csv, na = "")
write_csv(variable_df, out_variable_csv, na = "")
write_csv(anomaly_df, out_anomaly_csv, na = "")
write_csv(cd_totals, out_cd_totals_csv, na = "")
write_csv(borough_totals, out_borough_totals_csv, na = "")

pdf(out_figures_pdf, width = 11, height = 8.5)

print(
  ggplot(summary_df, aes(x = reorder(vintage, release_order_key(vintage)), y = row_count, fill = source_id)) +
    geom_col() +
    coord_flip() +
    labs(title = "MapPLUTO Staged Row Counts by Release", x = "Release", y = "Rows", fill = NULL) +
    theme_minimal(base_size = 12)
)

if (nrow(current_df) > 0) {
  current_plot_df <- current_df %>%
    mutate(
      yearbuilt = suppressWarnings(as.integer(yearbuilt)),
      unitsres = suppressWarnings(as.numeric(unitsres))
    )

  print(
    ggplot(current_plot_df %>% filter(!is.na(yearbuilt)), aes(x = yearbuilt)) +
      geom_histogram(bins = 60, fill = "#2a6f97", color = "white") +
      labs(title = "Current MapPLUTO YearBuilt Distribution", x = "YearBuilt", y = "Lot count") +
      theme_minimal(base_size = 12)
  )

  print(
    ggplot(current_plot_df %>% filter(!is.na(unitsres), unitsres > 0), aes(x = unitsres)) +
      geom_histogram(bins = 60, fill = "#c97c5d", color = "white") +
      scale_x_log10() +
      labs(title = "Current MapPLUTO Residential Units Distribution", x = "UnitsRes (log scale)", y = "Lot count") +
      theme_minimal(base_size = 12)
  )
}

dev.off()

pdf(out_maps_pdf, width = 11, height = 8.5)

if (nrow(cd_totals) == 0) {
  plot.new()
  text(0.5, 0.5, "No current MapPLUTO release available for CD maps")
} else {
  boundary_row <- boundary_index[boundary_index$source_id == "dcp_boundary_community_districts" &
    !is.na(boundary_index$parquet_path) &
    file.exists(boundary_index$parquet_path), ]
  boundary_row <- boundary_row %>%
    arrange(desc(pull_date)) %>%
    slice_head(n = 1)

  if (nrow(boundary_row) == 0) {
    plot.new()
    text(0.5, 0.5, "No current community district boundary layer available")
  } else {
    boundary_parquet_df <- read_parquet(boundary_row$parquet_path[1]) %>%
      as.data.frame() %>%
      as_tibble()
    crs_epsg <- boundary_parquet_df$crs_epsg[!is.na(boundary_parquet_df$crs_epsg)][1]
    crs_value <- if (length(crs_epsg) == 0 || is.na(crs_epsg)) 4326 else crs_epsg

    boundary_sf <- boundary_parquet_df %>%
      mutate(geometry = read_boundary_geometry(boundary_parquet_df, crs_value)) %>%
      st_as_sf() %>%
      mutate(cd = suppressWarnings(as.integer(district_id))) %>%
      left_join(cd_totals, by = "cd") %>%
      st_transform(2263)

    boundary_sf$unit_density <- boundary_sf$residential_units / as.numeric(st_area(boundary_sf))

    print(
      ggplot(boundary_sf) +
        geom_sf(aes(fill = missing_yearbuilt_share), color = "#4a4a4a", linewidth = 0.1) +
        scale_fill_gradient(low = "#f7fbff", high = "#08306b", na.value = "grey90") +
        labs(title = "Current MapPLUTO Missing YearBuilt Share by Community District", fill = "Missing share") +
        theme_void(base_size = 12)
    )

    print(
      ggplot(boundary_sf) +
        geom_sf(aes(fill = unit_density), color = "#4a4a4a", linewidth = 0.1) +
        scale_fill_gradient(low = "#fff5eb", high = "#7f2704", na.value = "grey90") +
        labs(title = "Current MapPLUTO Residential Unit Density by Community District", fill = "Units / sqft") +
        theme_void(base_size = 12)
    )
  }
}

dev.off()

cat("Wrote MapPLUTO summary outputs to", dirname(out_summary_csv), "\n")
