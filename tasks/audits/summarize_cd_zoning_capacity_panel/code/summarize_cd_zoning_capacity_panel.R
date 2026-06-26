# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/summarize_cd_zoning_capacity_panel/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

version_rank <- function(vintage) {
  year_part <- suppressWarnings(as.integer(str_extract(vintage, "^[0-9]{2}")))
  v_part <- suppressWarnings(as.numeric(str_extract(vintage, "(?<=v)[0-9]+(?:\\.[0-9]+)?")))
  2000 + year_part + v_part / 100
}

coef_row <- function(df, outcome_name, year_value) {
  model_df <- df %>%
    filter(year == year_value, outcome == outcome_name, is.finite(value), !is.na(treat_z_boro), !is.na(borough_name))

  if (nrow(model_df) < 20 || n_distinct(model_df$borough_name) < 2) {
    return(tibble(outcome = outcome_name, year = year_value, estimate = NA_real_, std_error = NA_real_, p_value = NA_real_, cds = n_distinct(model_df$borocd)))
  }

  fit <- feols(value ~ treat_z_boro | borough_name, data = model_df, vcov = "hetero")
  ct <- coeftable(fit)
  tibble(
    outcome = outcome_name,
    year = year_value,
    estimate = unname(ct["treat_z_boro", "Estimate"]),
    std_error = unname(ct["treat_z_boro", "Std. Error"]),
    p_value = unname(ct["treat_z_boro", "Pr(>|t|)"]),
    cds = n_distinct(model_df$borocd)
  )
}

cd_base <- read_csv("../input/cd_redevelopment_potential_baseline.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  transmute(
    borocd = as.integer(borocd),
    borough_name = as.character(borough_name),
    occupied_units_1990 = as.numeric(occupied_units_1990),
    residential_acres = as.numeric(residential_acres),
    treat_pp = as.numeric(treat_pp),
    treat_z_boro = as.numeric(treat_z_boro)
  ) %>%
  distinct(borocd, .keep_all = TRUE)

if (nrow(cd_base) != 59) {
  stop("Expected 59 CDs in denominator/treatment table.")
}

district_lookup <- cd_base %>%
  group_by(borough_name) %>%
  mutate(
    treat_tercile = ntile(treat_pp, 3),
    treat_tercile_label = case_when(treat_tercile == 1 ~ "Low", treat_tercile == 2 ~ "Middle", TRUE ~ "High")
  ) %>%
  ungroup()

mappluto_files <- read_csv("../input/mappluto_lot_files.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(parquet_path = paste0("../../build_mappluto_lots/output/", basename(parquet_path))) %>%
  filter(raw_status == "loaded", str_detect(vintage, "^[0-9]{2}v")) %>%
  mutate(
    year = 2000L + suppressWarnings(as.integer(str_extract(vintage, "^[0-9]{2}"))),
    rank = version_rank(vintage)
  ) %>%
  filter(year >= 2018, year <= 2025) %>%
  group_by(year) %>%
  slice_max(rank, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  arrange(year)

if (!identical(mappluto_files$year, 2018:2025)) {
  stop("Expected one selected MapPLUTO release for each year 2018-2025.")
}

lot_release <- bind_rows(lapply(seq_len(nrow(mappluto_files)), function(i) {
  row <- mappluto_files[i, ]
  lot_df <- read_parquet(
    row$parquet_path,
    col_select = c("bbl", "cd", "lotarea", "builtfar", "residfar", "landuse", "unitsres", "resarea", "is_joint_interest_area")
  ) %>%
    as.data.frame() %>%
    as_tibble()

  lot_df %>%
    transmute(
      year = row$year,
      source_vintage = row$vintage,
      bbl = as.character(bbl),
      borocd = suppressWarnings(as.integer(cd)),
      lotarea = suppressWarnings(as.numeric(lotarea)),
      builtfar = suppressWarnings(as.numeric(builtfar)),
      residfar = suppressWarnings(as.numeric(residfar)),
      landuse = as.character(landuse),
      unitsres = suppressWarnings(as.numeric(unitsres)),
      resarea = suppressWarnings(as.numeric(resarea)),
      is_joint_interest_area = as.logical(is_joint_interest_area)
    ) %>%
    filter(!is_joint_interest_area, !is.na(borocd), borocd >= 101, borocd <= 595, !is.na(bbl)) %>%
    mutate(
      unused_res_far = pmax(residfar - builtfar, 0),
      unused_res_floor_area = unused_res_far * lotarea,
      residential_lot_flag = landuse %in% c("1", "2", "3") | coalesce(unitsres, 0) > 0 | coalesce(resarea, 0) > 0,
      large_building_plausible_flag = unused_res_floor_area >= 42500
    )
}))

duplicate_lot_keys <- lot_release %>%
  count(year, bbl, name = "row_count") %>%
  filter(row_count > 1)

if (nrow(duplicate_lot_keys) > 0) {
  stop("MapPLUTO lot-release table is not unique by year and BBL.")
}

cd_year <- lot_release %>%
  group_by(year, source_vintage, borocd) %>%
  summarise(
    lot_count = n(),
    lot_area_total = sum(lotarea, na.rm = TRUE),
    current_residential_lot_area = sum(lotarea[residential_lot_flag], na.rm = TRUE),
    mean_builtfar_lotarea_weighted = weighted.mean(builtfar, w = lotarea, na.rm = TRUE),
    mean_residfar_lotarea_weighted = weighted.mean(residfar, w = lotarea, na.rm = TRUE),
    mean_unused_res_far_lotarea_weighted = weighted.mean(unused_res_far, w = lotarea, na.rm = TRUE),
    unused_res_floor_area = sum(unused_res_floor_area, na.rm = TRUE),
    large_building_plausible_lots = sum(large_building_plausible_flag, na.rm = TRUE),
    large_building_plausible_lot_share = mean(large_building_plausible_flag, na.rm = TRUE),
    large_building_plausible_lot_area_share = sum(lotarea[large_building_plausible_flag], na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  right_join(crossing(year = 2018:2025, borocd = district_lookup$borocd), by = c("year", "borocd"), relationship = "many-to-one") %>%
  left_join(district_lookup, by = "borocd", relationship = "many-to-one") %>%
  mutate(
    unused_res_floor_area_per_residential_acre = unused_res_floor_area / residential_acres,
    unused_res_floor_area_per_10000_occ_1990 = 10000 * unused_res_floor_area / occupied_units_1990
  ) %>%
  arrange(year, borocd)

long_df <- cd_year %>%
  select(
    borocd, borough_name, year, treat_z_boro,
    mean_unused_res_far_lotarea_weighted,
    unused_res_floor_area_per_residential_acre,
    large_building_plausible_lot_share,
    large_building_plausible_lot_area_share
  ) %>%
  pivot_longer(
    cols = c(mean_unused_res_far_lotarea_weighted, unused_res_floor_area_per_residential_acre, large_building_plausible_lot_share, large_building_plausible_lot_area_share),
    names_to = "outcome",
    values_to = "value"
  )

coefficients <- bind_rows(lapply(unique(long_df$outcome), function(outcome_name) {
  bind_rows(lapply(sort(unique(long_df$year)), function(year_value) coef_row(long_df, outcome_name, year_value)))
}))

plot_df <- cd_year %>%
  group_by(year, treat_tercile_label) %>%
  summarise(
    unused_res_floor_area_per_residential_acre = mean(unused_res_floor_area_per_residential_acre, na.rm = TRUE),
    large_building_plausible_lot_share = mean(large_building_plausible_lot_share, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  pivot_longer(
    cols = c(unused_res_floor_area_per_residential_acre, large_building_plausible_lot_share),
    names_to = "outcome",
    values_to = "value"
  ) %>%
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    outcome_label = recode(
      outcome,
      unused_res_floor_area_per_residential_acre = "Unused residential floor area per baseline residential acre",
      large_building_plausible_lot_share = "Share of lots with >=42,500 sq ft unused residential floor area"
    )
  )

plot_obj <- ggplot(plot_df, aes(x = year, y = value, color = treat_tercile_label)) +
  geom_line(linewidth = 0.6, na.rm = TRUE) +
  geom_point(size = 1, na.rm = TRUE) +
  facet_wrap(~ outcome_label, scales = "free_y", ncol = 1) +
  scale_color_manual(values = c("Low" = "#2166ac", "Middle" = "#8c8c8c", "High" = "#d6604d")) +
  labs(
    x = NULL,
    y = NULL,
    color = "1990 homeowner tercile",
    title = "CD zoning-capacity trends from staged MapPLUTO releases",
    subtitle = "One release per year, 2018-2025; no pre-2018 capacity claims"
  ) +
  theme_minimal(base_size = 10) +
  theme(legend.position = "bottom")

temp_pdf <- tempfile(fileext = ".pdf")
pdf(temp_pdf, width = 10.5, height = 8)
print(plot_obj)
dev.off()
copy_if_changed(temp_pdf, "../output/cd_zoning_capacity_tercile_trends.pdf")

qc_df <- bind_rows(
  tibble(metric = "selected_release_count", value = nrow(mappluto_files), status = if_else(nrow(mappluto_files) == 8, "pass", "fail"), note = "One MapPLUTO release per year, 2018-2025."),
  tibble(metric = "selected_year_min", value = min(mappluto_files$year), status = if_else(min(mappluto_files$year) == 2018, "pass", "fail"), note = "Zoning capacity starts in 2018."),
  tibble(metric = "selected_year_max", value = max(mappluto_files$year), status = if_else(max(mappluto_files$year) == 2025, "pass", "fail"), note = "Zoning capacity ends in 2025/current release."),
  tibble(metric = "lot_release_row_count", value = nrow(lot_release), status = if_else(nrow(lot_release) > 0, "pass", "fail"), note = "Lot-release rows."),
  tibble(metric = "duplicate_year_bbl_count", value = nrow(duplicate_lot_keys), status = if_else(nrow(duplicate_lot_keys) == 0, "pass", "fail"), note = "Lot-release rows should be unique by year and BBL."),
  tibble(metric = "cd_count", value = n_distinct(cd_year$borocd), status = if_else(n_distinct(cd_year$borocd) == 59, "pass", "fail"), note = "Expected 59 CDs."),
  tibble(metric = "negative_capacity_count", value = sum(cd_year$unused_res_floor_area < 0 | cd_year$unused_res_floor_area_per_residential_acre < 0, na.rm = TRUE), status = if_else(sum(cd_year$unused_res_floor_area < 0 | cd_year$unused_res_floor_area_per_residential_acre < 0, na.rm = TRUE) == 0, "pass", "fail"), note = "Capacity measures must be nonnegative.")
)

if (any(qc_df$status == "fail")) {
  write_csv_if_changed(qc_df, "../output/cd_zoning_capacity_qc.csv")
  stop("Zoning capacity QC failed.")
}

write_parquet_if_changed(lot_release, "../output/cd_zoning_capacity_lot_release.parquet")
write_csv_if_changed(cd_year, "../output/cd_zoning_capacity_cd_year.csv")
write_csv_if_changed(coefficients, "../output/cd_zoning_capacity_coefficients.csv")
write_csv_if_changed(qc_df, "../output/cd_zoning_capacity_qc.csv")

cat("Wrote CD zoning-capacity panel outputs to ../output\n")
