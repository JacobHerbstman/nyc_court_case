# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_cd_homeownership_1990_measure/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(sf)
  library(tibble)
  library(tidyr)
})

source("../../_lib/data_reports.R")

profile_metrics <- read_parquet("../input/dcp_cd_profiles_1990_2000_20260501.parquet") |>
  as.data.frame() |>
  as_tibble() |>
  mutate(
    metric_key = case_when(
      section_name == "housing_occupancy" & metric_label == "Total housing units" ~ "total_housing_units_1990",
      section_name == "housing_occupancy" & metric_label == "Vacant housing units" ~ "vacant_housing_units_1990",
      section_name == "housing_tenure" & metric_label == "Occupied housing units" ~ "occupied_units_1990",
      section_name == "housing_tenure" & metric_label == "Owner-occupied housing units" ~ "owner_occupied_units_1990",
      section_name == "income_in_1989_and_1999" & metric_label == "Median household income (1999 constant dollars)" ~ "median_household_income_1990",
      TRUE ~ NA_character_
    )
  ) |>
  filter(!is.na(metric_key)) |>
  transmute(
    district_id = sprintf("%03d", suppressWarnings(as.integer(district_id))),
    borough_code = suppressWarnings(as.integer(substr(district_id, 1, 1))),
    borough_name,
    metric_key,
    value_1990 = suppressWarnings(as.numeric(value_1990_number))
  )

if (
  nrow(profile_metrics) != nrow(distinct(profile_metrics, district_id, metric_key)) ||
  nrow(profile_metrics) != 59 * 5
) {
  stop("DCP profiles must contain five unique 1990 values for each of 59 community districts.")
}

measure_df <- profile_metrics |>
  pivot_wider(names_from = metric_key, values_from = value_1990) |>
  mutate(
    borocd = suppressWarnings(as.integer(district_id)),
    h_cd_1990 = owner_occupied_units_1990 / occupied_units_1990,
    h_cd_1990_pct = 100 * h_cd_1990,
    vacancy_rate_1990 = vacant_housing_units_1990 / total_housing_units_1990
  ) |>
  group_by(borough_code, borough_name) |>
  mutate(
    borough_owner_occupied_units_1990 = sum(owner_occupied_units_1990),
    borough_occupied_units_1990 = sum(occupied_units_1990),
    h_b_1990 = borough_owner_occupied_units_1990 / borough_occupied_units_1990,
    h_b_1990_pct = 100 * h_b_1990,
    cd_minus_borough_1990 = h_cd_1990 - h_b_1990,
    treat_pp = 100 * cd_minus_borough_1990,
    treat_z_boro = (treat_pp - mean(treat_pp)) / sd(treat_pp)
  ) |>
  ungroup() |>
  transmute(
    source_id = "dcp_cd_profiles_1990_2000",
    pull_date = 20260501,
    district_id,
    borocd,
    borough_code,
    borough_name,
    owner_occupied_units_1990,
    occupied_units_1990,
    total_housing_units_1990,
    vacant_housing_units_1990,
    borough_owner_occupied_units_1990,
    borough_occupied_units_1990,
    h_cd_1990,
    h_cd_1990_pct,
    h_b_1990,
    h_b_1990_pct,
    cd_minus_borough_1990,
    treat_pp,
    treat_z_boro,
    vacancy_rate_1990,
    median_household_income_1990
  ) |>
  arrange(borocd)

if (
  nrow(measure_df) != 59 ||
  anyDuplicated(measure_df$borocd) ||
  any(!is.finite(measure_df$treat_z_boro))
) {
  stop("Community-district treatment must cover 59 unique districts without missing values.")
}

save_csv(measure_df, "../output/cd_homeownership_1990_measure.csv", c("district_id"))

map_df <- read_parquet("../input/dcp_boundary_community_districts_20260501.parquet") |>
  transmute(
    borocd = suppressWarnings(as.integer(district_id)),
    geometry = st_as_sfc(geometry_wkt, crs = 4326)
  ) |>
  st_as_sf() |>
  inner_join(
    measure_df |> select(borocd, treat_z_boro),
    by = "borocd",
    relationship = "one-to-one"
  )

if (nrow(map_df) != 59 || anyDuplicated(map_df$borocd)) {
  stop("Community-district map must contain the same 59 districts as the treatment measure.")
}

pdf("../output/cd_homeownership_1990_map.pdf", width = 8.5, height = 8.5)
print(
  ggplot(map_df) +
    geom_sf(aes(fill = treat_z_boro), color = "white", linewidth = 0.15) +
    scale_fill_gradient2(
      low = "#3366CC",
      mid = "#F5F5F5",
      high = "#CC3311",
      midpoint = 0,
      name = "Within-borough\nhomeowner exposure"
    ) +
    labs(title = "1990 homeownership exposure by community district") +
    theme_void(base_size = 11) +
    theme(legend.position = "bottom", plot.title = element_text(hjust = 0.5))
)
dev.off()

cat("Wrote 1990 community-district homeownership measure and map to ../output\n")
