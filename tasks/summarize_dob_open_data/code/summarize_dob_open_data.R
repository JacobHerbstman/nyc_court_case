# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_dob_open_data/code")
# dob_open_data_files_csv <- "../input/dob_open_data_files.csv"
# dob_open_data_qc_csv <- "../input/dob_open_data_qc.csv"
# dob_field_dictionary_csv <- "../input/dob_field_dictionary.csv"
# dcp_boundary_index_csv <- "../input/dcp_boundary_index.csv"
# out_summary_csv <- "../output/dob_audit_summary.csv"
# out_variable_csv <- "../output/dob_variable_quality.csv"
# out_anomaly_csv <- "../output/dob_anomalies.csv"
# out_figures_pdf <- "../output/dob_figures.pdf"
# out_maps_pdf <- "../output/dob_maps.pdf"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(sf)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 9) {
  stop("Expected 9 arguments for summarize_dob_open_data.R")
}

dob_open_data_files_csv <- args[1]
dob_open_data_qc_csv <- args[2]
dob_field_dictionary_csv <- args[3]
dcp_boundary_index_csv <- args[4]
out_summary_csv <- args[5]
out_variable_csv <- args[6]
out_anomaly_csv <- args[7]
out_figures_pdf <- args[8]
out_maps_pdf <- args[9]

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

dob_index <- read_csv(dob_open_data_files_csv, show_col_types = FALSE, na = c("", "NA"))
dob_qc <- read_csv(dob_open_data_qc_csv, show_col_types = FALSE, na = c("", "NA"))
dob_field_dictionary <- read_csv(dob_field_dictionary_csv, show_col_types = FALSE, na = c("", "NA"))
boundary_index <- read_csv(dcp_boundary_index_csv, show_col_types = FALSE, na = c("", "NA"))

available_rows <- dob_index %>% filter(!is.na(parquet_path), file.exists(parquet_path))

if (nrow(available_rows) == 0) {
  write_csv(tibble(status = "no_staged_dob_files"), out_summary_csv, na = "")
  write_csv(tibble(), out_variable_csv, na = "")
  write_csv(tibble(), out_anomaly_csv, na = "")
  pdf(out_figures_pdf, width = 10, height = 7)
  plot.new()
  text(0.5, 0.5, "No staged DOB files available")
  dev.off()
  pdf(out_maps_pdf, width = 10, height = 7)
  plot.new()
  text(0.5, 0.5, "No staged DOB files available")
  dev.off()
  quit(save = "no")
}

summary_df <- dob_qc %>%
  left_join(
    dob_index %>% select(source_id, raw_path, parquet_path, pull_date),
    by = "source_id"
  ) %>%
  arrange(source_id)

variable_df <- bind_rows(
  dob_qc %>%
    transmute(source_id, variable = "bbl", missing_share = 1 - nonmissing_bbl_share),
  dob_qc %>%
    transmute(source_id, variable = "bin", missing_share = 1 - nonmissing_bin_share),
  dob_qc %>%
    transmute(source_id, variable = "address", missing_share = 1 - nonmissing_address_share),
  dob_qc %>%
    transmute(source_id, variable = "community_district", missing_share = 1 - nonmissing_cd_share),
  dob_qc %>%
    transmute(source_id, variable = "council_district", missing_share = 1 - nonmissing_council_share),
  dob_qc %>%
    transmute(source_id, variable = "record_year", missing_share = 1 - nonmissing_record_year_share),
  dob_qc %>%
    transmute(source_id, variable = "unresolved_reason", missing_share = unresolved_share)
) %>%
  arrange(source_id, variable)

anomaly_rows <- list()
map_rows <- list()

for (i in seq_len(nrow(available_rows))) {
  row <- available_rows[i, ]
  dob_df <- read_parquet(row$parquet_path) %>%
    as.data.frame() %>%
    as_tibble()

  anomaly_rows[[i]] <- bind_rows(
    tibble(source_id = row$source_id, issue = "missing_all_location_keys", affected_rows = sum(is.na(dob_df$bbl) & is.na(dob_df$bin) & is.na(dob_df$address))),
    tibble(source_id = row$source_id, issue = "missing_record_year", affected_rows = sum(is.na(dob_df$record_year))),
    tibble(source_id = row$source_id, issue = "missing_community_district", affected_rows = sum(is.na(dob_df$community_district))),
    tibble(source_id = row$source_id, issue = "missing_council_district", affected_rows = sum(is.na(dob_df$council_district))),
    tibble(source_id = row$source_id, issue = "negative_net_dwelling_units", affected_rows = sum(!is.na(dob_df$net_dwelling_units) & dob_df$net_dwelling_units < 0)),
    tibble(source_id = row$source_id, issue = "rows_with_unresolved_reason", affected_rows = sum(!is.na(dob_df$unresolved_reason)))
  )

  map_rows[[i]] <- dob_df %>%
    filter(!is.na(community_district)) %>%
    group_by(source_id, community_district) %>%
    summarise(
      record_count = n(),
      unresolved_share = mean(!is.na(unresolved_reason)),
      .groups = "drop"
    )
}

anomaly_df <- bind_rows(anomaly_rows) %>%
  filter(affected_rows > 0) %>%
  arrange(source_id, desc(affected_rows), issue)

write_csv(summary_df, out_summary_csv, na = "")
write_csv(variable_df, out_variable_csv, na = "")
write_csv(anomaly_df, out_anomaly_csv, na = "")

pdf(out_figures_pdf, width = 11, height = 8.5)

print(
  ggplot(summary_df, aes(x = reorder(source_id, row_count), y = row_count, fill = source_id)) +
    geom_col(show.legend = FALSE) +
    coord_flip() +
    labs(title = "DOB Staged Row Counts by Source", x = NULL, y = "Rows") +
    theme_minimal(base_size = 12)
)

print(
  ggplot(variable_df, aes(x = variable, y = missing_share, fill = source_id)) +
    geom_col(position = "dodge") +
    labs(title = "DOB Missingness by Canonical Field", x = NULL, y = "Missing share", fill = NULL) +
    theme_minimal(base_size = 12)
)

print(
  ggplot(dob_field_dictionary, aes(x = canonical_field, fill = source_id)) +
    geom_bar(position = "dodge") +
    coord_flip() +
    labs(title = "DOB Raw-to-Canonical Field Coverage", x = NULL, y = "Matched raw columns", fill = NULL) +
    theme_minimal(base_size = 12)
)

dev.off()

pdf(out_maps_pdf, width = 11, height = 8.5)

boundary_row <- boundary_index %>%
  filter(source_id == "dcp_boundary_community_districts", !is.na(parquet_path), file.exists(parquet_path)) %>%
  arrange(desc(pull_date)) %>%
  slice_head(n = 1)

map_df <- bind_rows(map_rows)

if (nrow(boundary_row) == 0 || nrow(map_df) == 0) {
  plot.new()
  text(0.5, 0.5, "No current community district boundaries or mappable DOB records")
} else {
  boundary_parquet_df <- read_parquet(boundary_row$parquet_path[1]) %>%
    as.data.frame() %>%
    as_tibble()
  crs_epsg <- boundary_parquet_df$crs_epsg[!is.na(boundary_parquet_df$crs_epsg)][1]
  crs_value <- if (length(crs_epsg) == 0 || is.na(crs_epsg)) 4326 else crs_epsg

  boundary_sf <- boundary_parquet_df %>%
    mutate(geometry = read_boundary_geometry(boundary_parquet_df, crs_value)) %>%
    st_as_sf() %>%
    mutate(cd = suppressWarnings(as.integer(district_id)))

  bis_map_df <- map_df %>%
    filter(source_id == "dob_bis_job_filings") %>%
    rename(cd = community_district)

  if (nrow(bis_map_df) == 0) {
    plot.new()
    text(0.5, 0.5, "No BIS job filing records with parsed community districts")
  } else {
    boundary_sf <- boundary_sf %>%
      left_join(bis_map_df, by = "cd")

    print(
      ggplot(boundary_sf) +
        geom_sf(aes(fill = record_count), color = "#4a4a4a", linewidth = 0.1) +
        scale_fill_gradient(low = "#f7fbff", high = "#08306b", na.value = "grey90") +
        labs(title = "DOB BIS Job Filings by Community District", fill = "Rows") +
        theme_void(base_size = 12)
    )

    print(
      ggplot(boundary_sf) +
        geom_sf(aes(fill = unresolved_share), color = "#4a4a4a", linewidth = 0.1) +
        scale_fill_gradient(low = "#fff5eb", high = "#7f2704", na.value = "grey90") +
        labs(title = "DOB BIS Job Filing Unresolved Share by Community District", fill = "Share") +
        theme_void(base_size = 12)
    )
  }
}

dev.off()

cat("Wrote DOB audit outputs to", dirname(out_summary_csv), "\n")
