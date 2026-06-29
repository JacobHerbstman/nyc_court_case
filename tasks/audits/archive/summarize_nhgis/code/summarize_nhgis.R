# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_nhgis/code")
# nhgis_files_csv <- "../input/nhgis_files.csv"
# nhgis_qc_csv <- "../input/nhgis_qc.csv"
# nhgis_1980_parquet <- "../input/nhgis_1980_tract_extract.parquet"
# nhgis_1990_parquet <- "../input/nhgis_1990_tract_extract.parquet"
# nhgis_1980_reconciliation_csv <- "../input/nhgis_1980_reconciliation.csv"
# out_summary_csv <- "../output/nhgis_audit_summary.csv"
# out_variable_csv <- "../output/nhgis_variable_quality.csv"
# out_extremes_csv <- "../output/nhgis_homeowner_share_extremes.csv"
# out_county_csv <- "../output/nhgis_county_counts.csv"
# out_suspicious_csv <- "../output/nhgis_suspicious_tracts.csv"
# out_unresolved_centroids_csv <- "../output/nhgis_1980_unresolved_centroids.csv"
# out_unresolved_1990_centroids_csv <- "../output/nhgis_1990_unresolved_centroids.csv"
# out_group_quarters_review_csv <- "../output/nhgis_1990_group_quarters_review.csv"
# out_figures_pdf <- "../output/nhgis_figures.pdf"
# out_maps_pdf <- "../output/nhgis_maps.pdf"

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

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 15) {
  stop("Expected 15 arguments for summarize_nhgis.R")
}

nhgis_files_csv <- args[1]
nhgis_qc_csv <- args[2]
nhgis_1980_parquet <- args[3]
nhgis_1990_parquet <- args[4]
nhgis_1980_reconciliation_csv <- args[5]
out_summary_csv <- args[6]
out_variable_csv <- args[7]
out_extremes_csv <- args[8]
out_county_csv <- args[9]
out_suspicious_csv <- args[10]
out_unresolved_centroids_csv <- args[11]
out_unresolved_1990_centroids_csv <- args[12]
out_group_quarters_review_csv <- args[13]
out_figures_pdf <- args[14]
out_maps_pdf <- args[15]

safe_quantile <- function(x, prob) {
  x <- suppressWarnings(as.numeric(x))
  x <- x[!is.na(x)]
  if (length(x) == 0) {
    return(NA_real_)
  }
  as.numeric(stats::quantile(x, probs = prob, names = FALSE, type = 7, na.rm = TRUE))
}

read_nested_shape <- function(outer_zip_path) {
  outer_listing <- unzip(outer_zip_path, list = TRUE)
  inner_zip_rel <- outer_listing$Name[str_detect(tolower(outer_listing$Name), "shapefile.*\\.zip$")][1]

  if (is.na(inner_zip_rel)) {
    stop("Could not find nested NHGIS shapefile zip inside ", outer_zip_path)
  }

  outer_tmp_dir <- tempfile(pattern = "nhgis_outer_")
  inner_tmp_dir <- tempfile(pattern = "nhgis_inner_")
  dir.create(outer_tmp_dir)
  dir.create(inner_tmp_dir)

  unzip(outer_zip_path, files = inner_zip_rel, exdir = outer_tmp_dir)
  inner_zip_path <- file.path(outer_tmp_dir, inner_zip_rel)
  unzip(inner_zip_path, exdir = inner_tmp_dir)

  shp_candidates <- list.files(inner_tmp_dir, pattern = "\\.shp$", recursive = TRUE, full.names = TRUE)
  tract_hits <- shp_candidates[str_detect(basename(shp_candidates), "^US_tract_[0-9]{4}\\.shp$")]
  shp_path <- if (length(tract_hits) > 0) tract_hits[1] else shp_candidates[1]

  if (is.na(shp_path)) {
    stop("Could not find shapefile after extracting ", outer_zip_path)
  }

  shape_df <- st_read(shp_path, quiet = TRUE)
  names(shape_df) <- normalize_names(names(shape_df))
  shape_df
}

county_lookup <- tibble(
  countya = c("005", "047", "061", "081", "085"),
  county_name = c("Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island")
)

nhgis_files <- read_csv(nhgis_files_csv, show_col_types = FALSE, na = c("", "NA"))
nhgis_qc <- read_csv(nhgis_qc_csv, show_col_types = FALSE, na = c("", "NA"))
reconciliation_1980 <- read_csv(nhgis_1980_reconciliation_csv, show_col_types = FALSE, na = c("", "NA"))

nhgis_1980 <- read_parquet(nhgis_1980_parquet) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(year = as.integer(year), countya = str_pad(as.character(countya), width = 3, side = "left", pad = "0"))

nhgis_1990 <- read_parquet(nhgis_1990_parquet) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(year = as.integer(year), countya = str_pad(as.character(countya), width = 3, side = "left", pad = "0"))

audit_df <- bind_rows(nhgis_1980, nhgis_1990) %>%
  left_join(county_lookup, by = "countya") %>%
  mutate(year = as.integer(year))

summary_table <- audit_df %>%
  group_by(year) %>%
  summarise(
    tract_count = n(),
    county_count = n_distinct(countya),
    total_population_sum = sum(total_population, na.rm = TRUE),
    total_housing_units_sum = sum(total_housing_units, na.rm = TRUE),
    occupied_units_sum = sum(occupied_units, na.rm = TRUE),
    vacant_units_sum = sum(vacant_units, na.rm = TRUE),
    owner_occupied_units_sum = sum(owner_occupied_units, na.rm = TRUE),
    renter_occupied_units_sum = sum(renter_occupied_units, na.rm = TRUE),
    vacancy_status_gap_sum = sum(vacancy_status_gap, na.rm = TRUE),
    reconciled_housing_balance_gap_sum = sum(reconciled_housing_balance_gap, na.rm = TRUE),
    unresolved_tract_count = sum(unresolved_flag, na.rm = TRUE),
    missing_income_share = mean(is.na(median_household_income)),
    zero_income_share = mean(median_household_income == 0, na.rm = TRUE),
    zero_population_share = mean(total_population == 0, na.rm = TRUE),
    zero_housing_share = mean(total_housing_units == 0, na.rm = TRUE),
    homeowner_share_mean = mean(homeowner_share, na.rm = TRUE),
    homeowner_share_p10 = safe_quantile(homeowner_share, 0.10),
    homeowner_share_p50 = safe_quantile(homeowner_share, 0.50),
    homeowner_share_p90 = safe_quantile(homeowner_share, 0.90),
    .groups = "drop"
  ) %>%
  left_join(
    nhgis_qc %>% select(year, status, missing_nhgis_codes),
    by = "year"
  )

numeric_fields <- c(
  "total_housing_units", "occupied_units", "vacant_units", "vacant_units_status_sum", "owner_occupied_units",
  "renter_occupied_units", "households_total", "total_population", "group_quarters_population",
  "non_group_quarters_population", "group_quarters_population_share", "household_count_gap",
  "median_household_income", "structure_1unit", "structure_2_4_unit", "structure_5plus_unit",
  "homeowner_share", "vacancy_status_gap"
)

variable_quality <- bind_rows(lapply(sort(unique(audit_df$year)), function(year_value) {
  year_df <- audit_df %>% filter(year == year_value)

  bind_rows(lapply(numeric_fields, function(field_name) {
    field_values <- suppressWarnings(as.numeric(year_df[[field_name]]))
    tibble(
      year = year_value,
      variable = field_name,
      nonmissing_count = sum(!is.na(field_values)),
      missing_share = mean(is.na(field_values)),
      zero_share = mean(field_values == 0, na.rm = TRUE),
      min_value = if (all(is.na(field_values))) NA_real_ else min(field_values, na.rm = TRUE),
      p01_value = safe_quantile(field_values, 0.01),
      p50_value = safe_quantile(field_values, 0.50),
      p99_value = safe_quantile(field_values, 0.99),
      max_value = if (all(is.na(field_values))) NA_real_ else max(field_values, na.rm = TRUE)
    )
  }))
}))

homeowner_share_extremes <- bind_rows(lapply(sort(unique(audit_df$year)), function(year_value) {
  year_df <- audit_df %>%
    filter(year == year_value, occupied_units > 0, !is.na(homeowner_share)) %>%
    select(year, county_name, gisjoin, tracta, homeowner_share, occupied_units, total_housing_units, total_population, median_household_income)

  bind_rows(
    year_df %>% arrange(homeowner_share, gisjoin) %>% slice_head(n = 10) %>% mutate(rank_group = "lowest"),
    year_df %>% arrange(desc(homeowner_share), gisjoin) %>% slice_head(n = 10) %>% mutate(rank_group = "highest")
  )
}))

county_counts <- audit_df %>%
  group_by(year, countya, county_name) %>%
  summarise(
    tract_count = n(),
    total_population_sum = sum(total_population, na.rm = TRUE),
    total_housing_units_sum = sum(total_housing_units, na.rm = TRUE),
    homeowner_share_mean = mean(homeowner_share, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(year, countya)

suspicious_tracts <- bind_rows(
  audit_df %>%
    filter(unresolved_flag | zero_income_flag | zero_population_flag | zero_housing_flag | vacancy_status_gap != 0) %>%
    transmute(
      year,
      county_name,
      gisjoin,
      tracta,
      suspicious_reason = paste(
        ifelse(vacancy_status_gap != 0, "vacancy_status_gap", NA_character_),
        ifelse(zero_income_flag, "zero_income", NA_character_),
        ifelse(zero_population_flag, "zero_population", NA_character_),
        ifelse(zero_housing_flag, "zero_housing", NA_character_),
        ifelse(unresolved_flag, "unresolved", NA_character_),
        sep = ";"
      ),
      housing_balance_classification,
      income_classification,
      vacancy_status_gap,
      homeowner_share,
      households_total,
      household_count_gap,
      occupied_units,
      total_housing_units,
      vacant_units_status_sum,
      vacant_units,
      total_population,
      group_quarters_population,
      non_group_quarters_population,
      group_quarters_population_share,
      median_household_income
    ),
  reconciliation_1980 %>%
    transmute(
      year,
      county_name = NA_character_,
      gisjoin,
      tracta,
      suspicious_reason = paste(housing_balance_classification, income_classification, sep = ";"),
      housing_balance_classification,
      income_classification,
      vacancy_status_gap,
      homeowner_share = NA_real_,
      households_total = NA_real_,
      household_count_gap = NA_real_,
      occupied_units,
      total_housing_units,
      vacant_units_status_sum,
      vacant_units = vacant_units_reconciled,
      total_population = NA_real_,
      group_quarters_population = NA_real_,
      non_group_quarters_population = NA_real_,
      group_quarters_population_share = NA_real_,
      median_household_income = NA_real_
    )
) %>%
  distinct() %>%
  mutate(suspicious_reason = str_replace_all(suspicious_reason, "NA;|;NA|NA", "")) %>%
  mutate(suspicious_reason = str_replace_all(suspicious_reason, "^;+|;+$", "")) %>%
  filter(suspicious_reason != "") %>%
  arrange(year, gisjoin)

audit_df_for_figures <- audit_df %>% mutate(year = factor(year))

pdf(out_figures_pdf, width = 11, height = 8.5)

print(
  ggplot(audit_df_for_figures %>% filter(!is.na(homeowner_share), occupied_units > 0), aes(x = homeowner_share)) +
    geom_histogram(bins = 40, fill = "#2a6f97", color = "white") +
    facet_wrap(~year, ncol = 2) +
    labs(title = "NHGIS Homeowner Share Distribution", x = "Homeowner share", y = "Tract count") +
    theme_minimal(base_size = 12)
)

print(
  ggplot(audit_df_for_figures %>% filter(!is.na(median_household_income), median_household_income > 0), aes(x = median_household_income)) +
    geom_histogram(bins = 40, fill = "#c97c5d", color = "white") +
    facet_wrap(~year, ncol = 2, scales = "free_y") +
    labs(title = "NHGIS Median Household Income Distribution", x = "Median household income", y = "Tract count") +
    theme_minimal(base_size = 12)
)

print(
  ggplot(county_counts %>% mutate(year = factor(year)), aes(x = county_name, y = tract_count, fill = year)) +
    geom_col(position = "dodge") +
    labs(title = "NHGIS Tract Counts by County", x = NULL, y = "Tract count", fill = "Year") +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 20, hjust = 1))
)

dev.off()

shape_1980_zip <- nhgis_files %>%
  filter(source_id == "nhgis_1980_tract_extract") %>%
  slice_head(n = 1) %>%
  pull(gis_zip_path)

shape_1990_zip <- nhgis_files %>%
  filter(source_id == "nhgis_1990_tract_extract") %>%
  slice_head(n = 1) %>%
  pull(gis_zip_path)

shape_1980 <- read_nested_shape(shape_1980_zip) %>%
  left_join(nhgis_1980, by = "gisjoin") %>%
  mutate(year = factor(1980))

shape_1990 <- read_nested_shape(shape_1990_zip) %>%
  left_join(nhgis_1990, by = "gisjoin") %>%
  mutate(year = factor(1990))

audit_sf <- bind_rows(shape_1980, shape_1990) %>% filter(!is.na(source_id))

centroid_sf <- audit_sf %>%
  st_transform(3857)

centroid_sf <- suppressWarnings(st_centroid(centroid_sf, of_largest_polygon = TRUE)) %>%
  st_transform(4326)

centroid_coords <- st_coordinates(centroid_sf)

centroid_df <- centroid_sf %>%
  st_drop_geometry() %>%
  mutate(
    year = as.integer(as.character(year)),
    centroid_lon = centroid_coords[, 1],
    centroid_lat = centroid_coords[, 2]
  ) %>%
  select(year, gisjoin, centroid_lon, centroid_lat)

suspicious_tracts <- suspicious_tracts %>%
  left_join(centroid_df, by = c("year", "gisjoin")) %>%
  select(
    year, county_name, gisjoin, tracta, centroid_lon, centroid_lat, suspicious_reason,
    housing_balance_classification, income_classification, vacancy_status_gap,
    homeowner_share, households_total, household_count_gap, occupied_units,
    total_housing_units, vacant_units_status_sum, vacant_units, total_population,
    group_quarters_population, non_group_quarters_population,
    group_quarters_population_share, median_household_income
  )

unresolved_centroids <- suspicious_tracts %>%
  filter(year == 1980, income_classification == "unresolved") %>%
  arrange(is.na(county_name), gisjoin) %>%
  distinct(year, gisjoin, .keep_all = TRUE) %>%
  arrange(county_name, gisjoin)

unresolved_1990_centroids <- suspicious_tracts %>%
  filter(year == 1990, income_classification == "unresolved") %>%
  arrange(is.na(county_name), gisjoin) %>%
  distinct(year, gisjoin, .keep_all = TRUE) %>%
  arrange(county_name, gisjoin)

group_quarters_review <- audit_df %>%
  filter(year == 1990, zero_income_flag) %>%
  transmute(
    year,
    county_name,
    gisjoin,
    tracta,
    households_total,
    occupied_units,
    household_count_gap,
    total_population,
    group_quarters_population,
    non_group_quarters_population,
    group_quarters_population_share,
    owner_occupied_units,
    renter_occupied_units,
    median_household_income,
    income_classification,
    income_override_reason
  ) %>%
  arrange(desc(group_quarters_population_share), gisjoin)

write_csv(summary_table, out_summary_csv, na = "")
write_csv(variable_quality, out_variable_csv, na = "")
write_csv(homeowner_share_extremes, out_extremes_csv, na = "")
write_csv(county_counts, out_county_csv, na = "")
write_csv(suspicious_tracts, out_suspicious_csv, na = "")
write_csv(unresolved_centroids, out_unresolved_centroids_csv, na = "")
write_csv(unresolved_1990_centroids, out_unresolved_1990_centroids_csv, na = "")
write_csv(group_quarters_review, out_group_quarters_review_csv, na = "")

pdf(out_maps_pdf, width = 12, height = 8.5)

print(
  ggplot(audit_sf) +
    geom_sf(aes(fill = homeowner_share), color = NA) +
    facet_wrap(~year, ncol = 2) +
    scale_fill_gradient(low = "#f7fbff", high = "#08306b", na.value = "grey90") +
    labs(title = "NHGIS Homeowner Share by Tract", fill = "Share") +
    theme_void(base_size = 12)
)

print(
  ggplot(audit_sf %>% filter(!is.na(median_household_income), median_household_income > 0)) +
    geom_sf(aes(fill = median_household_income), color = NA) +
    facet_wrap(~year, ncol = 2) +
    scale_fill_gradient(low = "#fff5eb", high = "#7f2704", na.value = "grey90") +
    labs(title = "NHGIS Median Household Income by Tract", fill = "Income") +
    theme_void(base_size = 12)
)

dev.off()

cat("Wrote NHGIS audit outputs to", dirname(out_summary_csv), "\n")
