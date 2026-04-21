# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_dob_permit_issuance/code")
# harmonized_parquet <- "../input/dob_permit_issuance_harmonized.parquet"
# harmonized_qc_csv <- "../input/dob_permit_issuance_harmonized_qc.csv"
# comparison_csv <- "../input/current_source_decision_1989_2013.csv"
# dcp_boundary_index_csv <- "../input/dcp_boundary_index.csv"
# census_bps_city_year_parquet <- "../input/census_bps_city_year.parquet"
# out_summary_csv <- "../output/dob_permit_issuance_harmonized_audit_summary.csv"
# out_anomaly_csv <- "../output/dob_permit_issuance_harmonized_anomalies.csv"
# out_figures_pdf <- "../output/dob_permit_issuance_harmonized_figures.pdf"
# out_maps_pdf <- "../output/dob_permit_issuance_harmonized_maps.pdf"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(sf)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 9) {
  stop("Expected 9 arguments for summarize_dob_permit_issuance.R")
}

harmonized_parquet <- args[1]
harmonized_qc_csv <- args[2]
comparison_csv <- args[3]
dcp_boundary_index_csv <- args[4]
census_bps_city_year_parquet <- args[5]
out_summary_csv <- args[6]
out_anomaly_csv <- args[7]
out_figures_pdf <- args[8]
out_maps_pdf <- args[9]

output_dir <- dirname(out_summary_csv)
out_city_year_csv <- file.path(output_dir, "dob_permit_issuance_harmonized_city_year.csv")
out_borough_year_csv <- file.path(output_dir, "dob_permit_issuance_harmonized_borough_year.csv")
out_cd_csv <- file.path(output_dir, "dob_permit_issuance_harmonized_community_district.csv")
out_bps_compare_csv <- file.path(output_dir, "dob_permit_issuance_harmonized_bps_city_year_compare.csv")

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

harmonized_df <- read_parquet(
  harmonized_parquet,
  col_select = c(
    "canonical_source_id",
    "permit_identifier",
    "issuance_date",
    "record_year",
    "job_type",
    "borough",
    "bbl",
    "bin",
    "address",
    "community_district",
    "council_district",
    "latitude",
    "longitude",
    "issuance_date_missing_flag"
  )
) %>%
  as.data.frame() %>%
  as_tibble()

harmonized_qc <- read_csv(harmonized_qc_csv, show_col_types = FALSE, na = c("", "NA"))
comparison_df <- read_csv(comparison_csv, show_col_types = FALSE, na = c("", "NA"))
boundary_index <- read_csv(dcp_boundary_index_csv, show_col_types = FALSE, na = c("", "NA"))

bps_city_year <- if (file.exists(census_bps_city_year_parquet)) {
  read_parquet(census_bps_city_year_parquet) %>%
    as.data.frame() %>%
    as_tibble()
} else {
  tibble()
}

city_year_df <- harmonized_df %>%
  filter(!is.na(record_year)) %>%
  group_by(record_year) %>%
  summarise(
    row_count = n(),
    nb_row_count = sum(job_type == "New Building", na.rm = TRUE),
    share_with_bbl = mean(!is.na(bbl) & bbl != "", na.rm = TRUE),
    share_with_bin = mean(!is.na(bin) & bin != "", na.rm = TRUE),
    share_with_address = mean(!is.na(address) & address != "", na.rm = TRUE),
    share_with_cd = mean(!is.na(community_district), na.rm = TRUE),
    share_with_council = mean(!is.na(council_district), na.rm = TRUE),
    share_with_latlon = mean(!is.na(latitude) & !is.na(longitude), na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(record_year)

borough_year_df <- harmonized_df %>%
  filter(!is.na(record_year), !is.na(borough)) %>%
  group_by(record_year, borough) %>%
  summarise(
    row_count = n(),
    nb_row_count = sum(job_type == "New Building", na.rm = TRUE),
    .groups = "drop_last"
  ) %>%
  mutate(share_of_city_rows = row_count / sum(row_count)) %>%
  ungroup() %>%
  arrange(record_year, borough)

community_district_df <- harmonized_df %>%
  filter(!is.na(community_district)) %>%
  group_by(community_district) %>%
  summarise(
    record_count = n(),
    nb_record_count = sum(job_type == "New Building", na.rm = TRUE),
    missing_issuance_date_count = sum(issuance_date_missing_flag, na.rm = TRUE),
    earliest_record_year = suppressWarnings(min(record_year, na.rm = TRUE)),
    latest_record_year = suppressWarnings(max(record_year, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  mutate(
    earliest_record_year = ifelse(is.infinite(earliest_record_year), NA_integer_, earliest_record_year),
    latest_record_year = ifelse(is.infinite(latest_record_year), NA_integer_, latest_record_year)
  ) %>%
  arrange(desc(record_count), community_district)

bps_compare_df <- if (nrow(bps_city_year) == 0) {
  tibble(record_year = integer(), nb_row_count = double(), city_total_units = double())
} else {
  city_year_df %>%
    select(record_year, nb_row_count) %>%
    inner_join(
      bps_city_year %>%
        transmute(record_year = as.integer(year), city_total_units = as.numeric(city_total_units)),
      by = "record_year"
    )
}

summary_df <- bind_rows(
  tibble(metric = "canonical_source_id", value = unique(harmonized_df$canonical_source_id)[1], note = "Canonical source stored in the unified permit dataset."),
  tibble(metric = "total_rows", value = as.character(nrow(harmonized_df)), note = "Total rows in the unified permit issuance file."),
  tibble(metric = "earliest_issuance_date", value = as.character(safe_min_date(harmonized_df$issuance_date)), note = "Earliest nonmissing issuance date in the unified file."),
  tibble(metric = "latest_issuance_date", value = as.character(safe_max_date(harmonized_df$issuance_date)), note = "Latest nonmissing issuance date in the unified file."),
  tibble(metric = "missing_issuance_date_rows", value = as.character(sum(harmonized_df$issuance_date_missing_flag, na.rm = TRUE)), note = "Rows retained with missing issuance dates."),
  tibble(metric = "missing_issuance_date_share", value = as.character(mean(harmonized_df$issuance_date_missing_flag, na.rm = TRUE)), note = "Share of rows retained with missing issuance dates."),
  tibble(metric = "share_with_bbl", value = as.character(mean(!is.na(harmonized_df$bbl) & harmonized_df$bbl != "", na.rm = TRUE)), note = "Share of unified rows with BBL."),
  tibble(metric = "share_with_cd", value = as.character(mean(!is.na(harmonized_df$community_district), na.rm = TRUE)), note = "Share of unified rows with community district."),
  tibble(metric = "share_with_council", value = as.character(mean(!is.na(harmonized_df$council_district), na.rm = TRUE)), note = "Share of unified rows with council district."),
  tibble(metric = "share_with_latlon", value = as.character(mean(!is.na(harmonized_df$latitude) & !is.na(harmonized_df$longitude), na.rm = TRUE)), note = "Share of unified rows with latitude and longitude."),
  tibble(metric = "nb_share", value = as.character(mean(harmonized_df$job_type == "New Building", na.rm = TRUE)), note = "Share of permit rows classified as New Building."),
  tibble(metric = "current_ge_historical_all_years_1989_2013", value = harmonized_qc %>% filter(metric == "current_ge_historical_all_years_1989_2013") %>% pull(value) %>% first(), note = "Current source row counts are at least as large as historical in every year from 1989 through 2013."),
  tibble(metric = "min_current_minus_historical_rows_1989_2013", value = as.character(min(comparison_df$current_minus_historical_rows, na.rm = TRUE)), note = "Smallest annual current minus historical row-count difference across 1989-2013."),
  tibble(metric = "min_current_minus_historical_nb_rows_1989_2013", value = as.character(min(comparison_df$current_minus_historical_nb_rows, na.rm = TRUE)), note = "Smallest annual current minus historical New Building row-count difference across 1989-2013."),
  tibble(metric = "comparison_years_checked", value = as.character(nrow(comparison_df)), note = "Number of years in the current-versus-historical comparison table.")
)

anomaly_df <- bind_rows(
  tibble(issue = "missing_issuance_date", affected_rows = sum(harmonized_df$issuance_date_missing_flag, na.rm = TRUE), note = "Rows retained without issuance date."),
  tibble(issue = "missing_community_district", affected_rows = sum(is.na(harmonized_df$community_district)), note = "Rows without community district."),
  tibble(issue = "missing_council_district", affected_rows = sum(is.na(harmonized_df$council_district)), note = "Rows without council district."),
  tibble(issue = "missing_latlon", affected_rows = sum(is.na(harmonized_df$latitude) | is.na(harmonized_df$longitude)), note = "Rows without latitude or longitude."),
  tibble(issue = "years_where_current_lt_historical", affected_rows = sum(!comparison_df$current_ge_historical_row_count_flag, na.rm = TRUE), note = "Should be zero for the current-primary decision.")
) %>%
  filter(affected_rows > 0)

write_csv(summary_df, out_summary_csv, na = "")
write_csv(anomaly_df, out_anomaly_csv, na = "")
write_csv(city_year_df, out_city_year_csv, na = "")
write_csv(borough_year_df, out_borough_year_csv, na = "")
write_csv(community_district_df, out_cd_csv, na = "")
write_csv(bps_compare_df, out_bps_compare_csv, na = "")

city_year_rows_plot <- ggplot(city_year_df, aes(x = record_year, y = row_count)) +
  geom_line(color = "#08519c", linewidth = 0.7) +
  labs(title = "Unified Permit Issuance Rows by Year", x = "Year", y = "Rows") +
  theme_minimal(base_size = 12)

city_year_nb_plot <- ggplot(city_year_df, aes(x = record_year, y = nb_row_count)) +
  geom_line(color = "#cb181d", linewidth = 0.7) +
  labs(title = "Unified New Building Permit Rows by Year", x = "Year", y = "Rows") +
  theme_minimal(base_size = 12)

coverage_plot <- city_year_df %>%
  select(record_year, share_with_cd, share_with_council, share_with_latlon) %>%
  pivot_longer(
    cols = c(share_with_cd, share_with_council, share_with_latlon),
    names_to = "metric",
    values_to = "share"
  ) %>%
  mutate(
    metric = dplyr::recode(
      metric,
      share_with_cd = "Community district coverage",
      share_with_council = "Council district coverage",
      share_with_latlon = "Latitude/longitude coverage"
    )
  ) %>%
  ggplot(aes(x = record_year, y = share, color = metric)) +
  geom_line(linewidth = 0.7) +
  labs(title = "Unified Permit Geography Coverage by Year", x = "Year", y = "Share", color = NULL) +
  theme_minimal(base_size = 12)

borough_total_plot <- borough_year_df %>%
  group_by(borough) %>%
  summarise(record_count = sum(row_count), .groups = "drop") %>%
  ggplot(aes(x = reorder(borough, record_count), y = record_count)) +
  geom_col(fill = "#6baed6") +
  coord_flip() +
  labs(title = "Unified Permit Rows by Borough, 1989-Present", x = NULL, y = "Rows") +
  theme_minimal(base_size = 12)

bps_compare_plot <- if (nrow(bps_compare_df) == 0) {
  ggplot() +
    annotate("text", x = 0.5, y = 0.5, label = "No BPS comparison available") +
    xlim(0, 1) +
    ylim(0, 1) +
    theme_void()
} else {
  bps_compare_df %>%
    mutate(
      permit_index = nb_row_count / nb_row_count[match(min(record_year), record_year)],
      bps_index = city_total_units / city_total_units[match(min(record_year), record_year)]
    ) %>%
    select(record_year, permit_index, bps_index) %>%
    pivot_longer(
      cols = c(permit_index, bps_index),
      names_to = "series",
      values_to = "index_value"
    ) %>%
    mutate(
      series = dplyr::recode(
        series,
        permit_index = "Unified NB permit rows",
        bps_index = "BPS city units"
      )
    ) %>%
    ggplot(aes(x = record_year, y = index_value, color = series)) +
    geom_line(linewidth = 0.7) +
    labs(title = "Descriptive Trend Check: Unified NB Permit Rows vs BPS Units", x = "Year", y = "Index (first year = 1)", color = NULL) +
    theme_minimal(base_size = 12)
}

pdf(out_figures_pdf, width = 11, height = 8.5)
print(city_year_rows_plot)
print(city_year_nb_plot)
print(coverage_plot)
print(borough_total_plot)
print(bps_compare_plot)
dev.off()

boundary_row <- boundary_index %>%
  filter(source_id == "dcp_boundary_community_districts", !is.na(parquet_path), file.exists(parquet_path)) %>%
  slice_head(n = 1)

pdf(out_maps_pdf, width = 11, height = 8.5)

if (nrow(boundary_row) == 0) {
  plot.new()
  text(0.5, 0.5, "No current community district boundary parquet available")
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

  cd_total_df <- community_district_df %>%
    transmute(cd = community_district, record_count = record_count, nb_record_count = nb_record_count)

  cd_early_df <- harmonized_df %>%
    filter(!is.na(record_year), record_year >= 1989, record_year <= 1995, !is.na(community_district)) %>%
    group_by(community_district) %>%
    summarise(record_count = n(), .groups = "drop") %>%
    transmute(cd = community_district, early_record_count = record_count)

  total_map_sf <- boundary_sf %>%
    left_join(cd_total_df, by = "cd")

  early_map_sf <- boundary_sf %>%
    left_join(cd_early_df, by = "cd")

  total_map_plot <- ggplot(total_map_sf) +
    geom_sf(aes(fill = record_count), color = "#4a4a4a", linewidth = 0.1) +
    scale_fill_gradient(low = "#f7fbff", high = "#08306b", na.value = "grey90") +
    labs(title = "Unified Permit Rows by Community District, 1989-Present", fill = "Rows") +
    theme_void(base_size = 12)

  nb_map_plot <- ggplot(total_map_sf) +
    geom_sf(aes(fill = nb_record_count), color = "#4a4a4a", linewidth = 0.1) +
    scale_fill_gradient(low = "#fff5f0", high = "#a50f15", na.value = "grey90") +
    labs(title = "Unified New Building Permit Rows by Community District, 1989-Present", fill = "Rows") +
    theme_void(base_size = 12)

  early_map_plot <- ggplot(early_map_sf) +
    geom_sf(aes(fill = early_record_count), color = "#4a4a4a", linewidth = 0.1) +
    scale_fill_gradient(low = "#f7fcf5", high = "#006d2c", na.value = "grey90") +
    labs(title = "Unified Permit Rows by Community District, 1989-1995", fill = "Rows") +
    theme_void(base_size = 12)

  print(total_map_plot)
  print(nb_map_plot)
  print(early_map_plot)
}

dev.off()

cat("Wrote unified DOB permit issuance audit outputs to", output_dir, "\n")
