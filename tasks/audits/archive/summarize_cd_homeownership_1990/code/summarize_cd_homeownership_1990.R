# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_cd_homeownership_1990/code")
# nhgis_files_csv <- "../input/nhgis_files.csv"
# nhgis_1990_parquet <- "../input/nhgis_1990_tract_extract.parquet"
# dcp_boundary_index_csv <- "../input/dcp_boundary_index.csv"
# out_cd_csv <- "../output/nhgis_cd_homeownership_1990.csv"
# out_borough_summary_csv <- "../output/nhgis_cd_homeownership_1990_borough_summary.csv"
# out_assignment_qc_csv <- "../output/nhgis_cd_homeownership_1990_assignment_qc.csv"
# out_figures_pdf <- "../output/nhgis_cd_homeownership_1990_figures.pdf"

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

if (length(args) != 7) {
  stop("Expected 7 arguments: nhgis_files_csv nhgis_1990_parquet dcp_boundary_index_csv out_cd_csv out_borough_summary_csv out_assignment_qc_csv out_figures_pdf")
}

nhgis_files_csv <- args[1]
nhgis_1990_parquet <- args[2]
dcp_boundary_index_csv <- args[3]
out_cd_csv <- args[4]
out_borough_summary_csv <- args[5]
out_assignment_qc_csv <- args[6]
out_figures_pdf <- args[7]

sf_use_s2(FALSE)

safe_quantile <- function(x, prob) {
  x <- suppressWarnings(as.numeric(x))
  x <- x[!is.na(x)]

  if (length(x) == 0) {
    return(NA_real_)
  }

  as.numeric(stats::quantile(x, probs = prob, names = FALSE, type = 7, na.rm = TRUE))
}

hex_to_raw <- function(x) {
  if (is.na(x) || x == "") {
    return(as.raw())
  }

  as.raw(strtoi(substring(x, seq(1, nchar(x), by = 2), seq(2, nchar(x), by = 2)), 16L))
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
  unzip(file.path(outer_tmp_dir, inner_zip_rel), exdir = inner_tmp_dir)

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

standard_cd_ids <- c(
  sprintf("1%02d", 1:12),
  sprintf("2%02d", 1:12),
  sprintf("3%02d", 1:18),
  sprintf("4%02d", 1:14),
  sprintf("5%02d", 1:3)
)

county_lookup <- tibble(
  countya = c("005", "047", "061", "081", "085"),
  borough_code = c("2", "3", "1", "4", "5"),
  borough_name = c("Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island")
)

nhgis_files <- read_csv(nhgis_files_csv, show_col_types = FALSE, na = c("", "NA"))
nhgis_1990 <- read_parquet(nhgis_1990_parquet) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(
    gisjoin = as.character(gisjoin),
    countya = str_pad(as.character(countya), width = 3, side = "left", pad = "0"),
    tracta = as.character(tracta)
  ) %>%
  select(gisjoin, countya, tracta, owner_occupied_units, occupied_units, total_housing_units, homeowner_share)

borough_homeownership <- nhgis_1990 %>%
  inner_join(county_lookup, by = "countya") %>%
  group_by(borough_code, borough_name) %>%
  summarise(
    borough_owner_occupied_units_1990 = sum(owner_occupied_units, na.rm = TRUE),
    borough_occupied_units_1990 = sum(occupied_units, na.rm = TRUE),
    borough_homeowner_share_1990 = borough_owner_occupied_units_1990 / borough_occupied_units_1990,
    .groups = "drop"
  )

nhgis_1990_gis_zip <- nhgis_files %>%
  filter(year == 1990, !is.na(gis_zip_path), file.exists(gis_zip_path)) %>%
  arrange(desc(status == "staged"), gis_zip_path) %>%
  slice_head(n = 1) %>%
  pull(gis_zip_path)

if (length(nhgis_1990_gis_zip) == 0) {
  stop("Could not find a 1990 NHGIS GIS zip path in ", nhgis_files_csv)
}

dcp_boundary_index <- read_csv(dcp_boundary_index_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(pull_date = as.Date(pull_date))

community_district_parquet <- dcp_boundary_index %>%
  filter(source_id == "dcp_boundary_community_districts", !is.na(parquet_path), file.exists(parquet_path)) %>%
  arrange(desc(pull_date), parquet_path) %>%
  slice_head(n = 1) %>%
  pull(parquet_path)

if (length(community_district_parquet) == 0) {
  stop("Could not find a staged community district parquet path in ", dcp_boundary_index_csv)
}

tract_shape <- read_nested_shape(nhgis_1990_gis_zip[[1]]) %>%
  transmute(gisjoin = as.character(gisjoin), geometry)

tract_sf <- tract_shape %>%
  inner_join(nhgis_1990, by = "gisjoin") %>%
  st_as_sf() %>%
  st_make_valid() %>%
  st_transform(2263) %>%
  mutate(tract_area = as.numeric(st_area(geometry)))

boundary_df <- read_parquet(community_district_parquet[[1]]) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0")) %>%
  filter(district_id %in% standard_cd_ids)

boundary_wkb <- lapply(boundary_df$geometry_wkb_hex, hex_to_raw)
class(boundary_wkb) <- c("WKB", class(boundary_wkb))

cd_sf <- st_sf(
  boundary_df %>%
    transmute(
      district_id,
      borough_code = substr(district_id, 1, 1),
      borough_name = standardize_borough_name(substr(district_id, 1, 1))
    ),
  geometry = st_as_sfc(boundary_wkb, EWKB = TRUE, crs = boundary_df$crs_epsg[1])
) %>%
  st_make_valid() %>%
  st_transform(2263)

intersection_sf <- suppressWarnings(
  st_intersection(
    tract_sf %>%
      select(gisjoin, countya, tracta, owner_occupied_units, occupied_units, total_housing_units, tract_area),
    cd_sf %>%
      select(district_id, borough_code, borough_name)
  )
) %>%
  mutate(
    intersection_area = as.numeric(st_area(geometry)),
    area_share = ifelse(tract_area > 0, intersection_area / tract_area, NA_real_),
    owner_occupied_units_alloc = owner_occupied_units * area_share,
    occupied_units_alloc = occupied_units * area_share,
    total_housing_units_alloc = total_housing_units * area_share
  )

assignment_qc <- tract_sf %>%
  st_drop_geometry() %>%
  select(gisjoin, occupied_units, owner_occupied_units, total_housing_units) %>%
  left_join(
    intersection_sf %>%
      st_drop_geometry() %>%
      group_by(gisjoin) %>%
      summarise(
        area_share_sum = sum(area_share, na.rm = TRUE),
        occupied_units_alloc_sum = sum(occupied_units_alloc, na.rm = TRUE),
        owner_occupied_units_alloc_sum = sum(owner_occupied_units_alloc, na.rm = TRUE),
        total_housing_units_alloc_sum = sum(total_housing_units_alloc, na.rm = TRUE),
        .groups = "drop"
      ),
    by = "gisjoin"
  ) %>%
  mutate(
    area_share_sum = coalesce(area_share_sum, 0),
    occupied_assignment_share = ifelse(occupied_units > 0, occupied_units_alloc_sum / occupied_units, NA_real_),
    owner_assignment_share = ifelse(owner_occupied_units > 0, owner_occupied_units_alloc_sum / owner_occupied_units, NA_real_),
    housing_assignment_share = ifelse(total_housing_units > 0, total_housing_units_alloc_sum / total_housing_units, NA_real_),
    assignment_gap = 1 - area_share_sum
  )

assignment_qc_summary <- bind_rows(
  tibble(
    metric = "tract_count",
    value = nrow(assignment_qc),
    note = "Number of 1990 NHGIS tracts included in the CD aggregation."
  ),
  tibble(
    metric = "occupied_units_total",
    value = sum(assignment_qc$occupied_units, na.rm = TRUE),
    note = "Total occupied units in the staged 1990 NHGIS tract file."
  ),
  tibble(
    metric = "occupied_units_assigned_share",
    value = sum(assignment_qc$occupied_units_alloc_sum, na.rm = TRUE) / sum(assignment_qc$occupied_units, na.rm = TRUE),
    note = "Share of tract occupied units assigned to standard current community districts."
  ),
  tibble(
    metric = "owner_occupied_units_assigned_share",
    value = sum(assignment_qc$owner_occupied_units_alloc_sum, na.rm = TRUE) / sum(assignment_qc$owner_occupied_units, na.rm = TRUE),
    note = "Share of tract owner-occupied units assigned to standard current community districts."
  ),
  tibble(
    metric = "tract_area_share_mean",
    value = mean(assignment_qc$area_share_sum, na.rm = TRUE),
    note = "Mean share of each tract polygon assigned to standard current community districts."
  ),
  tibble(
    metric = "tract_area_share_min",
    value = min(assignment_qc$area_share_sum, na.rm = TRUE),
    note = "Minimum tract area share assigned to standard current community districts."
  ),
  tibble(
    metric = "tract_area_share_p01",
    value = safe_quantile(assignment_qc$area_share_sum, 0.01),
    note = "First percentile of tract area share assigned to standard current community districts."
  ),
  tibble(
    metric = "tract_area_share_p99",
    value = safe_quantile(assignment_qc$area_share_sum, 0.99),
    note = "Ninety-ninth percentile of tract area share assigned to standard current community districts."
  ),
  tibble(
    metric = "tracts_below_0_99_area_share",
    value = sum(assignment_qc$area_share_sum < 0.99, na.rm = TRUE),
    note = "Number of tracts with less than 99 percent of tract area assigned to standard current community districts."
  )
)

cd_homeownership <- intersection_sf %>%
  st_drop_geometry() %>%
  group_by(district_id, borough_code, borough_name) %>%
  summarise(
    tract_count = n_distinct(gisjoin),
    tract_piece_count = n(),
    owner_occupied_units_1990 = sum(owner_occupied_units_alloc, na.rm = TRUE),
    occupied_units_1990 = sum(occupied_units_alloc, na.rm = TRUE),
    total_housing_units_1990 = sum(total_housing_units_alloc, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(homeowner_share_1990 = owner_occupied_units_1990 / occupied_units_1990)

cd_homeownership <- cd_homeownership %>%
  left_join(borough_homeownership, by = c("borough_code", "borough_name")) %>%
  mutate(
    homeowner_share_1990_pct = 100 * homeowner_share_1990,
    borough_homeowner_share_1990_pct = 100 * borough_homeowner_share_1990,
    homeowner_share_minus_borough = homeowner_share_1990 - borough_homeowner_share_1990,
    treat_pp = 100 * homeowner_share_minus_borough
  ) %>%
  arrange(borough_code, district_id)

borough_summary <- cd_homeownership %>%
  group_by(borough_code, borough_name) %>%
  summarise(
    cd_count = n(),
    homeowner_share_min_pct = min(homeowner_share_1990_pct, na.rm = TRUE),
    homeowner_share_p25_pct = safe_quantile(homeowner_share_1990_pct, 0.25),
    homeowner_share_median_pct = safe_quantile(homeowner_share_1990_pct, 0.50),
    homeowner_share_p75_pct = safe_quantile(homeowner_share_1990_pct, 0.75),
    homeowner_share_max_pct = max(homeowner_share_1990_pct, na.rm = TRUE),
    homeowner_share_sd_pp = stats::sd(homeowner_share_1990_pct, na.rm = TRUE),
    treat_pp_min = min(treat_pp, na.rm = TRUE),
    treat_pp_p25 = safe_quantile(treat_pp, 0.25),
    treat_pp_median = safe_quantile(treat_pp, 0.50),
    treat_pp_p75 = safe_quantile(treat_pp, 0.75),
    treat_pp_max = max(treat_pp, na.rm = TRUE),
    treat_pp_sd = stats::sd(treat_pp, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(borough_code)

write_csv(cd_homeownership, out_cd_csv, na = "")
write_csv(borough_summary, out_borough_summary_csv, na = "")
write_csv(assignment_qc_summary, out_assignment_qc_csv, na = "")

pdf(out_figures_pdf, width = 11, height = 8.5)

print(
  ggplot(cd_homeownership, aes(x = homeowner_share_1990_pct)) +
    geom_histogram(binwidth = 5, fill = "#1f78b4", color = "white") +
    facet_wrap(~ borough_name, scales = "free_y") +
    labs(
      title = "1990 Community-District Homeownership Share",
      subtitle = "NHGIS tract counts aggregated to current standard community districts",
      x = "Homeownership share (percent)",
      y = "Community districts"
    ) +
    theme_minimal(base_size = 12)
)

print(
  ggplot(cd_homeownership, aes(x = borough_name, y = treat_pp, fill = borough_name)) +
    geom_boxplot(alpha = 0.8, width = 0.6, outlier.shape = 21, outlier.fill = "white") +
    geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
    labs(
      title = "Community-District Homeownership Minus Borough Mean",
      subtitle = "Positive values indicate above-borough 1990 homeownership exposure",
      x = NULL,
      y = "Percentage points"
    ) +
    theme_minimal(base_size = 12) +
    theme(legend.position = "none")
)

print(
  ggplot(cd_homeownership, aes(x = reorder(district_id, homeowner_share_1990_pct), y = homeowner_share_1990_pct, fill = borough_name)) +
    geom_col() +
    coord_flip() +
    labs(
      title = "1990 Community-District Homeownership Rank",
      subtitle = "Current standard community districts",
      x = "Community district",
      y = "Homeownership share (percent)"
    ) +
    theme_minimal(base_size = 11)
)

dev.off()

cat("Wrote 1990 community-district homeownership outputs to", dirname(out_cd_csv), "\n")
