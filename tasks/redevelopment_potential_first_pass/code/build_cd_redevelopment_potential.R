# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/redevelopment_potential_first_pass/code")
# treatment_csv <- "../input/cd_homeownership_1990_measure.csv"
# baseline_controls_csv <- "../input/cd_baseline_1990_controls.csv"
# mappluto_main_parquet <- "../input/dcp_mappluto_archive_18v1_1.parquet"
# mappluto_current_parquet <- "../input/dcp_mappluto_current_25v4.parquet"
# mappluto_lot_qc_csv <- "../input/mappluto_lot_qc.csv"
# out_baseline_csv <- "../output/cd_redevelopment_potential_baseline.csv"
# out_qc_csv <- "../output/cd_redevelopment_potential_qc.csv"
# out_index_corr_csv <- "../output/cd_redevelopment_potential_index_correlations.csv"
# out_sensitivity_csv <- "../output/cd_redevelopment_potential_sensitivity.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 9) {
  stop("Expected 9 arguments: treatment_csv baseline_controls_csv mappluto_main_parquet mappluto_current_parquet mappluto_lot_qc_csv out_baseline_csv out_qc_csv out_index_corr_csv out_sensitivity_csv")
}

treatment_csv <- args[1]
baseline_controls_csv <- args[2]
mappluto_main_parquet <- args[3]
mappluto_current_parquet <- args[4]
mappluto_lot_qc_csv <- args[5]
out_baseline_csv <- args[6]
out_qc_csv <- args[7]
out_index_corr_csv <- args[8]
out_sensitivity_csv <- args[9]

treatment_df <- read_csv(treatment_csv, show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    district_id = district_id,
    h_cd_1990 = suppressWarnings(as.numeric(h_cd_1990)),
    h_b_1990 = suppressWarnings(as.numeric(h_b_1990)),
    treat_pp = suppressWarnings(as.numeric(treat_pp)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro)),
    owner_occupied_units_1990 = suppressWarnings(as.numeric(owner_occupied_units_1990)),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990))
  )

controls_df <- read_csv(baseline_controls_csv, show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    vacancy_rate_1990_exact = suppressWarnings(as.numeric(vacancy_rate_1990_exact)),
    structure_share_1_2_units_1990_exact = suppressWarnings(as.numeric(structure_share_1_2_units_1990_exact)),
    structure_share_3_4_units_1990_exact = suppressWarnings(as.numeric(structure_share_3_4_units_1990_exact)),
    structure_share_5_plus_units_1990_exact = suppressWarnings(as.numeric(structure_share_5_plus_units_1990_exact)),
    median_household_income_1990_1999_dollars_exact = suppressWarnings(as.numeric(median_household_income_1990_1999_dollars_exact)),
    poverty_share_1990_exact = suppressWarnings(as.numeric(poverty_share_1990_exact)),
    median_housing_value_1990_2000_dollars_exact_filled = suppressWarnings(as.numeric(median_housing_value_1990_2000_dollars_exact_filled)),
    foreign_born_share_1990_exact = suppressWarnings(as.numeric(foreign_born_share_1990_exact)),
    college_graduate_share_1990_exact = suppressWarnings(as.numeric(college_graduate_share_1990_exact)),
    unemployment_rate_1990_exact = suppressWarnings(as.numeric(unemployment_rate_1990_exact)),
    subway_commute_share_1990_exact = suppressWarnings(as.numeric(subway_commute_share_1990_exact)),
    mean_commute_time_1990_minutes_exact = suppressWarnings(as.numeric(mean_commute_time_1990_minutes_exact)),
    total_housing_units_growth_1980_1990_approx = suppressWarnings(as.numeric(total_housing_units_growth_1980_1990_approx)),
    occupied_units_growth_1980_1990_approx = suppressWarnings(as.numeric(occupied_units_growth_1980_1990_approx)),
    vacancy_rate_change_1980_1990_pp_approx = suppressWarnings(as.numeric(vacancy_rate_change_1980_1990_pp_approx)),
    homeowner_share_change_1980_1990_pp_approx = suppressWarnings(as.numeric(homeowner_share_change_1980_1990_pp_approx))
  )

mappluto_qc_df <- read_csv(mappluto_lot_qc_csv, show_col_types = FALSE, na = c("", "NA"))

truthy_value <- function(x) {
  x_chr <- str_to_upper(str_trim(coalesce(as.character(x), "")))
  !x_chr %in% c("", "0", "N", "NO", "FALSE", "F", "NA", "NULL")
}

z_city <- function(x) {
  spread <- stats::sd(x, na.rm = TRUE)
  if (is.na(spread) || spread == 0) {
    rep(NA_real_, length(x))
  } else {
    (x - mean(x, na.rm = TRUE)) / spread
  }
}

z_boro <- function(x) {
  spread <- stats::sd(x, na.rm = TRUE)
  if (is.na(spread) || spread == 0) {
    rep(NA_real_, length(x))
  } else {
    (x - mean(x, na.rm = TRUE)) / spread
  }
}

build_cd_dataset <- function(pluto_path, release_label, weighted_means = TRUE, valid_far_only = FALSE) {
  lot_df <- read_parquet(
    pluto_path,
    col_select = c(
      "source_vintage", "bbl", "borough", "cd", "lotarea", "bldgarea", "resarea", "unitsres",
      "yearbuilt", "builtfar", "residfar", "commfar", "facilfar", "landuse", "bldgclass",
      "landmark", "histdist", "is_joint_interest_area"
    )
  ) |>
    transmute(
      source_vintage = source_vintage,
      bbl = as.character(bbl),
      borough = as.character(borough),
      borocd = sprintf("%03d", suppressWarnings(as.integer(cd))),
      lotarea = suppressWarnings(as.numeric(lotarea)),
      bldgarea = suppressWarnings(as.numeric(bldgarea)),
      resarea = suppressWarnings(as.numeric(resarea)),
      unitsres = suppressWarnings(as.numeric(unitsres)),
      yearbuilt = suppressWarnings(as.integer(yearbuilt)),
      builtfar = suppressWarnings(as.numeric(builtfar)),
      residfar = suppressWarnings(as.numeric(residfar)),
      commfar = suppressWarnings(as.numeric(commfar)),
      facilfar = suppressWarnings(as.numeric(facilfar)),
      landuse = str_pad(str_extract(as.character(landuse), "\\d+"), width = 2, pad = "0"),
      bldgclass = str_to_upper(str_trim(as.character(bldgclass))),
      landmark = as.character(landmark),
      histdist = as.character(histdist),
      is_joint_interest_area = as.logical(is_joint_interest_area)
    ) |>
    left_join(
      treatment_df |>
        select(borocd, borough_code, borough_name),
      by = "borocd"
    ) |>
    mutate(
      in_treatment_universe = !is.na(borough_code),
      valid_joint_interest = coalesce(is_joint_interest_area, FALSE),
      positive_lotarea = is.finite(lotarea) & lotarea > 0,
      built_far_calc = if_else(is.finite(bldgarea) & positive_lotarea, bldgarea / lotarea, NA_real_),
      built_far_use = case_when(
        is.finite(builtfar) & builtfar >= 0 ~ builtfar,
        is.finite(built_far_calc) & built_far_calc >= 0 ~ built_far_calc,
        TRUE ~ NA_real_
      ),
      max_resid_far = if_else(is.finite(residfar) & residfar > 0, residfar, NA_real_),
      max_comm_far = if_else(is.finite(commfar) & commfar > 0, commfar, NA_real_),
      max_facil_far = if_else(is.finite(facilfar) & facilfar > 0, facilfar, NA_real_),
      max_any_far = pmax(max_resid_far, max_comm_far, max_facil_far, na.rm = TRUE),
      max_any_far = if_else(is.infinite(max_any_far), NA_real_, max_any_far),
      is_residential_lot = coalesce(unitsres, 0) > 0 | landuse %in% c("01", "02", "03", "04"),
      unused_res_far = if_else(is.finite(max_resid_far) & is.finite(built_far_use), pmax(max_resid_far - built_far_use, 0), NA_real_),
      unused_any_far = if_else(is.finite(max_any_far) & is.finite(built_far_use), pmax(max_any_far - built_far_use, 0), NA_real_),
      unused_res_floor_area = if_else(is.finite(unused_res_far), unused_res_far * lotarea, NA_real_),
      unused_any_floor_area = if_else(is.finite(unused_any_far), unused_any_far * lotarea, NA_real_),
      near_res_cap_80 = is.finite(max_resid_far) & max_resid_far > 0 & is.finite(built_far_use) & built_far_use >= 0.8 * max_resid_far,
      near_res_cap_025 = is.finite(max_resid_far) & max_resid_far > 0 & is.finite(built_far_use) & (max_resid_far - built_far_use) <= 0.25,
      above_res_cap = is.finite(max_resid_far) & max_resid_far > 0 & is.finite(built_far_use) & built_far_use >= max_resid_far,
      old_building_proxy = !is.na(yearbuilt) & yearbuilt >= 1800 & yearbuilt <= 1940,
      one_two_family_proxy = landuse %in% c("01", "02") | str_detect(coalesce(bldgclass, ""), "^[AB]"),
      vacant_proxy = landuse == "11" | str_detect(coalesce(bldgclass, ""), "^V"),
      parking_or_low_intensity_proxy = landuse == "10" | str_detect(coalesce(bldgclass, ""), "^G"),
      protected_proxy = truthy_value(landmark) | truthy_value(histdist)
    )

  borough_pre_df <- lot_df |>
    filter(in_treatment_universe) |>
    group_by(borough_code, borough_name) |>
    summarize(
      lot_count_before = n(),
      lot_area_before = sum(lotarea[positive_lotarea], na.rm = TRUE),
      .groups = "drop"
    )

  lot_df <- lot_df |>
    filter(in_treatment_universe, !valid_joint_interest, positive_lotarea)

  if (valid_far_only) {
    lot_df <- lot_df |>
      filter(is.finite(built_far_use), is.finite(max_resid_far))
  }

  city_built_median <- lot_df |>
    filter(is_residential_lot, is.finite(built_far_use)) |>
    summarize(median_value = stats::median(built_far_use, na.rm = TRUE)) |>
    pull(median_value)

  city_units_median <- lot_df |>
    filter(is_residential_lot, is.finite(unitsres)) |>
    summarize(median_value = stats::median(unitsres, na.rm = TRUE)) |>
    pull(median_value)

  borough_medians_df <- lot_df |>
    filter(is_residential_lot) |>
    group_by(borough_code, borough_name) |>
    summarize(
      borough_built_far_median = stats::median(built_far_use[is.finite(built_far_use)], na.rm = TRUE),
      borough_unitsres_median = stats::median(unitsres[is.finite(unitsres)], na.rm = TRUE),
      .groups = "drop"
    )

  lot_df <- lot_df |>
    left_join(borough_medians_df, by = c("borough_code", "borough_name")) |>
    mutate(
      low_existing_far_boro = is_residential_lot & is.finite(built_far_use) & built_far_use < borough_built_far_median,
      low_existing_far_city = is_residential_lot & is.finite(built_far_use) & built_far_use < city_built_median,
      high_existing_far_boro = is_residential_lot & is.finite(built_far_use) & built_far_use > borough_built_far_median,
      high_units_existing_boro = is_residential_lot & is.finite(unitsres) & unitsres > borough_unitsres_median
    )

  borough_post_df <- lot_df |>
    group_by(borough_code, borough_name) |>
    summarize(
      lot_count_after = n(),
      lot_area_after = sum(lotarea, na.rm = TRUE),
      .groups = "drop"
    )

  cd_df <- lot_df |>
    group_by(borocd, borough_code, borough_name) |>
    summarize(
      source_release = release_label,
      aggregation_method = if_else(weighted_means, "lotarea_weighted", "unweighted_means"),
      valid_far_only = valid_far_only,
      lot_count_used = n(),
      cd_lot_area_total = sum(lotarea, na.rm = TRUE),
      cd_residential_lot_area = sum(if_else(is_residential_lot, lotarea, 0), na.rm = TRUE),
      cd_bldg_area_total = sum(coalesce(bldgarea, 0), na.rm = TRUE),
      cd_res_area_total = sum(coalesce(resarea, 0), na.rm = TRUE),
      cd_unitsres_total = sum(coalesce(unitsres, 0), na.rm = TRUE),
      cd_mean_built_far_lot_weighted = if (weighted_means) weighted.mean(built_far_use[is_residential_lot & is.finite(built_far_use)], lotarea[is_residential_lot & is.finite(built_far_use)], na.rm = TRUE) else mean(built_far_use[is_residential_lot & is.finite(built_far_use)], na.rm = TRUE),
      cd_median_built_far = stats::median(built_far_use[is_residential_lot & is.finite(built_far_use)], na.rm = TRUE),
      cd_mean_max_resid_far_lot_weighted = if (weighted_means) weighted.mean(max_resid_far[is_residential_lot & is.finite(max_resid_far)], lotarea[is_residential_lot & is.finite(max_resid_far)], na.rm = TRUE) else mean(max_resid_far[is_residential_lot & is.finite(max_resid_far)], na.rm = TRUE),
      cd_mean_unused_res_far_lot_weighted = if (weighted_means) weighted.mean(unused_res_far[is_residential_lot & is.finite(unused_res_far)], lotarea[is_residential_lot & is.finite(unused_res_far)], na.rm = TRUE) else mean(unused_res_far[is_residential_lot & is.finite(unused_res_far)], na.rm = TRUE),
      cd_sum_unused_res_floor_area = sum(if_else(is_residential_lot, coalesce(unused_res_floor_area, 0), 0), na.rm = TRUE),
      cd_sum_unused_any_floor_area = sum(coalesce(unused_any_floor_area, 0), na.rm = TRUE),
      cd_share_lot_area_near_res_cap_80 = sum(if_else(near_res_cap_80, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      cd_share_lot_area_above_res_cap = sum(if_else(above_res_cap, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      cd_share_lot_area_low_existing_far_boro = sum(if_else(low_existing_far_boro, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      cd_share_lot_area_low_existing_far_city = sum(if_else(low_existing_far_city, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      cd_share_lot_area_vacant = sum(if_else(vacant_proxy, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      cd_share_lot_area_one_two_family = sum(if_else(one_two_family_proxy, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      cd_share_lot_area_old_building = sum(if_else(old_building_proxy, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      cd_share_lot_area_protected = sum(if_else(protected_proxy, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      cd_share_lot_area_parking_or_low_intensity = sum(if_else(parking_or_low_intensity_proxy, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(
      cd_unused_res_floor_area_per_res_acre = if_else(cd_residential_lot_area > 0, cd_sum_unused_res_floor_area / (cd_residential_lot_area / 43560), NA_real_),
      residential_acres = cd_residential_lot_area / 43560
    ) |>
    left_join(treatment_df, by = c("borocd", "borough_code", "borough_name")) |>
    left_join(controls_df, by = c("borocd", "borough_code", "borough_name")) |>
    group_by(borough_code, borough_name) |>
    mutate(
      demand_proxy_ratio_boro = median_housing_value_1990_2000_dollars_exact_filled / mean(median_housing_value_1990_2000_dollars_exact_filled, na.rm = TRUE)
    ) |>
    ungroup()

  if (nrow(cd_df) != 59) {
    stop("Redevelopment baseline does not cover exactly 59 CDs for ", release_label, ".")
  }

  if (any(cd_df$cd_sum_unused_res_floor_area <= 0 | !is.finite(cd_df$cd_sum_unused_res_floor_area))) {
    stop("Found nonpositive unused residential floor area in ", release_label, ". Cannot log-transform index A.")
  }

  component_names <- c(
    "cd_mean_unused_res_far_lot_weighted",
    "cd_share_lot_area_low_existing_far_boro",
    "cd_share_lot_area_vacant",
    "cd_share_lot_area_parking_or_low_intensity",
    "cd_share_lot_area_near_res_cap_80",
    "cd_share_lot_area_above_res_cap",
    "cd_share_lot_area_protected",
    "cd_mean_built_far_lot_weighted"
  )

  cd_df <- cd_df |>
    mutate(
      redev_potential_A_raw = log(cd_sum_unused_res_floor_area),
      redev_potential_B_raw = cd_share_lot_area_low_existing_far_boro * cd_mean_unused_res_far_lot_weighted,
      redev_potential_D_raw = log(cd_sum_unused_res_floor_area) * demand_proxy_ratio_boro
    )

  for (component_name in component_names) {
    cd_df[[paste0(component_name, "_z_city")]] <- z_city(cd_df[[component_name]])
  }

  cd_df <- cd_df |>
    rowwise() |>
    mutate(
      index_c_component_count = sum(!is.na(c_across(ends_with("_z_city")))),
      index_c_components_used = str_c(component_names[!is.na(c_across(ends_with("_z_city")))], collapse = ";"),
      redev_potential_C_raw = sum(c(
        cd_mean_unused_res_far_lot_weighted_z_city,
        cd_share_lot_area_low_existing_far_boro_z_city,
        cd_share_lot_area_vacant_z_city,
        cd_share_lot_area_parking_or_low_intensity_z_city,
        -cd_share_lot_area_near_res_cap_80_z_city,
        -cd_share_lot_area_above_res_cap_z_city,
        -cd_share_lot_area_protected_z_city,
        -cd_mean_built_far_lot_weighted_z_city
      ), na.rm = TRUE)
    ) |>
    ungroup()

  for (index_name in c("A", "B", "C", "D")) {
    raw_name <- paste0("redev_potential_", index_name, "_raw")
    city_name <- paste0("redev_potential_", index_name, "_z_city")
    boro_name <- paste0("redev_potential_", index_name, "_z_boro")
    high_name <- paste0("high_redev_", index_name)
    low_name <- paste0("low_redev_", index_name)

    cd_df[[city_name]] <- z_city(cd_df[[raw_name]])
    cd_df[[boro_name]] <- cd_df |>
      group_by(borough_code, borough_name) |>
      mutate(z_value = z_boro(.data[[raw_name]])) |>
      pull(z_value)

    borough_medians <- cd_df |>
      group_by(borough_code, borough_name) |>
      summarize(median_value = stats::median(.data[[boro_name]], na.rm = TRUE), .groups = "drop")

    cd_df <- cd_df |>
      left_join(borough_medians, by = c("borough_code", "borough_name")) |>
      mutate(
        !!high_name := .data[[boro_name]] >= median_value,
        !!low_name := .data[[boro_name]] < median_value
      ) |>
      select(-median_value)
  }

  list(
    cd_df = cd_df,
    lot_df = lot_df,
    borough_pre_df = borough_pre_df,
    borough_post_df = borough_post_df
  )
}

main_build <- build_cd_dataset(mappluto_main_parquet, release_label = "18v1.1", weighted_means = TRUE, valid_far_only = FALSE)
current_build <- build_cd_dataset(mappluto_current_parquet, release_label = "25v4", weighted_means = TRUE, valid_far_only = FALSE)
main_unweighted_build <- build_cd_dataset(mappluto_main_parquet, release_label = "18v1.1", weighted_means = FALSE, valid_far_only = FALSE)
main_valid_far_build <- build_cd_dataset(mappluto_main_parquet, release_label = "18v1.1", weighted_means = TRUE, valid_far_only = TRUE)

baseline_df <- main_build$cd_df |>
  arrange(borocd)

index_corr_df <- expand_grid(
  scope = c("city", sort(unique(baseline_df$borough_name))),
  index_1 = c("A", "B", "C", "D"),
  index_2 = c("A", "B", "C", "D")
) |>
  filter(index_1 < index_2) |>
  rowwise() |>
  mutate(
    correlation = {
      work_df <- if (scope == "city") baseline_df else filter(baseline_df, borough_name == scope)
      stats::cor(work_df[[paste0("redev_potential_", index_1, "_z_boro")]], work_df[[paste0("redev_potential_", index_2, "_z_boro")]], use = "pairwise.complete.obs")
    },
    n_cd = if (scope == "city") nrow(baseline_df) else nrow(filter(baseline_df, borough_name == scope))
  ) |>
  ungroup() |>
  rename(scope_name = scope)

sensitivity_df <- bind_rows(
  tibble(
    comparison_family = "release",
    comparison_name = "18v1.1_vs_25v4",
    metric = c("redev_potential_A_z_boro", "redev_potential_C_z_boro"),
    value = c(
      stats::cor(main_build$cd_df$redev_potential_A_z_boro, current_build$cd_df$redev_potential_A_z_boro, use = "pairwise.complete.obs"),
      stats::cor(main_build$cd_df$redev_potential_C_z_boro, current_build$cd_df$redev_potential_C_z_boro, use = "pairwise.complete.obs")
    ),
    note = "Correlation between main baseline release and current release."
  ),
  tibble(
    comparison_family = "weighting",
    comparison_name = "weighted_vs_unweighted",
    metric = c("redev_potential_A_z_boro", "redev_potential_C_z_boro"),
    value = c(
      1,
      stats::cor(main_build$cd_df$redev_potential_C_z_boro, main_unweighted_build$cd_df$redev_potential_C_z_boro, use = "pairwise.complete.obs")
    ),
    note = c(
      "Index A is invariant to weighting because it uses the log of a summed unused residential floor-area measure.",
      "Correlation between weighted and unweighted Index C."
    )
  ),
  tibble(
    comparison_family = "valid_far_subset",
    comparison_name = "full_vs_valid_far_only",
    metric = c("redev_potential_A_z_boro", "redev_potential_C_z_boro"),
    value = c(
      stats::cor(main_build$cd_df$redev_potential_A_z_boro, main_valid_far_build$cd_df$redev_potential_A_z_boro, use = "pairwise.complete.obs"),
      stats::cor(main_build$cd_df$redev_potential_C_z_boro, main_valid_far_build$cd_df$redev_potential_C_z_boro, use = "pairwise.complete.obs")
    ),
    note = "Correlation between full-sample and valid-FAR-only versions."
  )
)

main_release_qc <- mappluto_qc_df |>
  filter(vintage == "18v1.1") |>
  slice_head(n = 1)

current_release_qc <- mappluto_qc_df |>
  filter(vintage == "25v4") |>
  slice_head(n = 1)

qc_df <- bind_rows(
  tibble(
    section = "coverage",
    item = c("district_count", "districts_all_59_present", "main_release", "current_release"),
    subgroup = NA_character_,
    borocd = NA_character_,
    borough_name = NA_character_,
    value = as.character(c(
      nrow(baseline_df),
      as.numeric(nrow(baseline_df) == 59),
      "18v1.1",
      "25v4"
    )),
    note = c(
      "Number of CDs in the main redevelopment baseline.",
      "Indicator for exact 59-CD coverage.",
      "Main redevelopment baseline release.",
      "Sensitivity release."
    )
  ),
  tibble(
    section = "field_nonmissing_main",
    item = c("nonmissing_cd_share", "nonmissing_council_share", "nonmissing_unitsres_share", "nonmissing_yearbuilt_share", "nonmissing_zonedist1_share", "nonmissing_lotarea_share", "nonmissing_builtfar_share", "nonmissing_assessland_share"),
    subgroup = NA_character_,
    borocd = NA_character_,
    borough_name = NA_character_,
    value = as.character(c(
      main_release_qc$nonmissing_cd_share,
      main_release_qc$nonmissing_council_share,
      main_release_qc$nonmissing_unitsres_share,
      main_release_qc$nonmissing_yearbuilt_share,
      main_release_qc$nonmissing_zonedist1_share,
      main_release_qc$nonmissing_lotarea_share,
      main_release_qc$nonmissing_builtfar_share,
      main_release_qc$nonmissing_assessland_share
    )),
    note = "Main release field nonmissing share from staged MapPLUTO QC."
  ),
  main_build$borough_pre_df |>
    left_join(main_build$borough_post_df, by = c("borough_code", "borough_name")) |>
    pivot_longer(cols = c(lot_count_before, lot_area_before, lot_count_after, lot_area_after), names_to = "item", values_to = "value") |>
    mutate(section = "borough_pre_post", subgroup = "18v1.1", borocd = NA_character_, value = as.character(value), note = "Borough totals before and after main redevelopment restrictions.") |>
    select(section, item, subgroup, borocd, borough_name, value, note),
  tibble(
    section = "drop_counts_main",
    item = c("raw_row_count", "treatment_universe_lots", "joint_interest_dropped", "invalid_lotarea_dropped", "far_subset_lots"),
    subgroup = NA_character_,
    borocd = NA_character_,
    borough_name = NA_character_,
    value = as.character(c(
      nrow(read_parquet(mappluto_main_parquet, col_select = "bbl")),
      sum(read_parquet(mappluto_main_parquet, col_select = c("cd")) |> transmute(borocd = sprintf("%03d", suppressWarnings(as.integer(cd)))) |> pull(borocd) %in% treatment_df$borocd),
      sum(coalesce(read_parquet(mappluto_main_parquet, col_select = "is_joint_interest_area") |> pull(is_joint_interest_area), FALSE), na.rm = TRUE),
      nrow(read_parquet(mappluto_main_parquet, col_select = c("cd", "lotarea")) |> transmute(borocd = sprintf("%03d", suppressWarnings(as.integer(cd))), lotarea = suppressWarnings(as.numeric(lotarea))) |> filter(borocd %in% treatment_df$borocd) |> filter(!(is.finite(lotarea) & lotarea > 0))),
      nrow(main_valid_far_build$lot_df)
    )),
    note = c(
      "Raw row count in the main release.",
      "Lots in the treatment universe before restrictions.",
      "Joint-interest areas removed in main baseline.",
      "Not reported directly after restriction because invalid lot area is handled before the retained lot table is stored.",
      "Lot count in valid-FAR-only sensitivity."
    )
  ),
  bind_rows(
    baseline_df |>
      arrange(desc(redev_potential_A_z_boro)) |>
      slice_head(n = 5) |>
      mutate(section = "index_extremes", item = "redev_potential_A_z_boro", subgroup = "top", value = as.character(redev_potential_A_z_boro), note = "Top 5 CDs by main redevelopment index.") |>
      select(section, item, subgroup, borocd, borough_name, value, note),
    baseline_df |>
      arrange(redev_potential_A_z_boro) |>
      slice_head(n = 5) |>
      mutate(section = "index_extremes", item = "redev_potential_A_z_boro", subgroup = "bottom", value = as.character(redev_potential_A_z_boro), note = "Bottom 5 CDs by main redevelopment index.") |>
      select(section, item, subgroup, borocd, borough_name, value, note),
    baseline_df |>
      arrange(desc(redev_potential_C_z_boro)) |>
      slice_head(n = 5) |>
      mutate(section = "index_extremes", item = "redev_potential_C_z_boro", subgroup = "top", value = as.character(redev_potential_C_z_boro), note = "Top 5 CDs by composite redevelopment index.") |>
      select(section, item, subgroup, borocd, borough_name, value, note),
    baseline_df |>
      arrange(redev_potential_C_z_boro) |>
      slice_head(n = 5) |>
      mutate(section = "index_extremes", item = "redev_potential_C_z_boro", subgroup = "bottom", value = as.character(redev_potential_C_z_boro), note = "Bottom 5 CDs by composite redevelopment index.") |>
      select(section, item, subgroup, borocd, borough_name, value, note)
  )
)

write_csv(baseline_df, out_baseline_csv, na = "")
write_csv(qc_df, out_qc_csv, na = "")
write_csv(index_corr_df, out_index_corr_csv, na = "")
write_csv(sensitivity_df, out_sensitivity_csv, na = "")

cat("Wrote redevelopment potential baseline outputs to", dirname(out_baseline_csv), "\n")
