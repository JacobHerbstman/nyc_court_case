# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_ccd2010_redevelopment_potential/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(sf)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

council_measure <- read_csv("../input/ccdist2010_homeownership_1990_measure.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    council_district = suppressWarnings(as.integer(council_district)),
    borough_code = as.character(borough_code),
    borough_name = borough_name,
    owner_occupied_units_1990 = suppressWarnings(as.numeric(owner_occupied_units_1990)),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990)),
    total_housing_units_1990 = suppressWarnings(as.numeric(total_housing_units_1990)),
    h_ccd_1990 = suppressWarnings(as.numeric(h_ccd_1990)),
    h_b_1990 = suppressWarnings(as.numeric(h_b_1990)),
    ccd_minus_borough_1990 = suppressWarnings(as.numeric(ccd_minus_borough_1990)),
    treat_pp = suppressWarnings(as.numeric(treat_pp)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro)),
    vacancy_rate_1990 = suppressWarnings(as.numeric(vacancy_rate_1990)),
    total_population_1990 = suppressWarnings(as.numeric(total_population_1990)),
    white_population_1990 = suppressWarnings(as.numeric(white_population_1990)),
    black_population_1990 = suppressWarnings(as.numeric(black_population_1990)),
    asian_pacific_islander_population_1990 = suppressWarnings(as.numeric(asian_pacific_islander_population_1990)),
    other_race_population_1990 = suppressWarnings(as.numeric(other_race_population_1990)),
    hispanic_population_1990 = suppressWarnings(as.numeric(hispanic_population_1990)),
    median_household_income_1990 = suppressWarnings(as.numeric(median_household_income_1990)),
    majority_borough_occupied_share = suppressWarnings(as.numeric(majority_borough_occupied_share)),
    geometry_wkt = geometry_wkt
  ) |>
  arrange(council_district)

if (anyDuplicated(council_measure$district_id)) {
  stop("Council district homeownership input is not unique by district_id.")
}

if (nrow(council_measure) != 51) {
  stop("Council district homeownership input does not cover exactly 51 districts.")
}

council_sf <- council_measure |>
  transmute(
    district_id = district_id,
    council_district = council_district,
    borough_code = borough_code,
    borough_name = borough_name,
    geometry = st_as_sfc(geometry_wkt, crs = 2263)
  ) |>
  st_as_sf() |>
  arrange(council_district)

mappluto_files <- read_csv("../input/mappluto_files.csv", show_col_types = FALSE, na = c("", "NA"))

normalize_text_field <- function(x) {
  out <- trimws(as.character(x))
  out[out %in% c("", "NA", "N/A", "NULL")] <- NA_character_
  out
}

normalize_integer_field <- function(x) {
  suppressWarnings(as.integer(normalize_text_field(x)))
}

normalize_numeric_field <- function(x) {
  suppressWarnings(as.numeric(normalize_text_field(x)))
}

normalize_year_field <- function(x) {
  x_int <- normalize_integer_field(x)
  x_int[x_int == 0L] <- NA_integer_
  x_int[x_int < 1800L] <- NA_integer_
  x_int
}

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

read_mappluto_sf <- function(raw_path) {
  if (!str_detect(tolower(raw_path), "[.]zip$")) {
    return(st_read(raw_path, quiet = TRUE, stringsAsFactors = FALSE))
  }

  temp_dir <- tempfile(pattern = "mappluto_sf_")
  dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  suppressWarnings(unzip(raw_path, exdir = temp_dir))
  shp_path <- list.files(temp_dir, pattern = "[.]shp$", recursive = TRUE, full.names = TRUE)[1]

  if (is.na(shp_path) || !nzchar(shp_path)) {
    stop("No shapefile found in ", raw_path)
  }

  st_read(shp_path, quiet = TRUE, stringsAsFactors = FALSE)
}

read_legacy_mappluto_02b_sf <- function(raw_path) {
  zip_listing <- unzip(raw_path, list = TRUE) |>
    as_tibble()

  shp_paths <- zip_listing |>
    filter(str_detect(tolower(Name), "[.]shp$")) |>
    arrange(Name) |>
    pull(Name)

  if (length(shp_paths) != 5) {
    stop("Expected five borough shapefiles in 2002 MapPLUTO archive; found ", length(shp_paths), ".")
  }

  shp_list <- lapply(shp_paths, function(shp_path) {
    st_read(paste0("/vsizip/", raw_path, "/", shp_path), quiet = TRUE, stringsAsFactors = FALSE)
  })

  all_names <- unique(unlist(lapply(shp_list, names)))
  shp_list <- lapply(shp_list, function(shp_df) {
    missing_names <- setdiff(all_names, names(shp_df))
    for (missing_name in missing_names) {
      shp_df[[missing_name]] <- NA
    }
    shp_df[, all_names]
  })

  do.call(rbind, shp_list)
}

get_mappluto_raw_path <- function(source_id_value, vintage_value) {
  row_df <- mappluto_files |>
    filter(
      source_id == source_id_value,
      vintage == vintage_value,
      file_role == "mappluto_shapefile_zip",
      status %in% c("downloaded", "already_present", "redownloaded_after_validation_failure"),
      !is.na(raw_path),
      file.exists(raw_path)
    ) |>
    arrange(raw_path)

  if (nrow(row_df) == 0) {
    stop("Could not find MapPLUTO shapefile zip for ", source_id_value, " ", vintage_value, ".")
  }

  if (nrow(row_df) > 1) {
    stop("MapPLUTO raw path lookup is not unique for ", source_id_value, " ", vintage_value, ".")
  }

  row_df$raw_path[[1]]
}

prepare_mappluto_lots <- function(raw_path, release_label, source_id_value, legacy_02b = FALSE) {
  mappluto_raw_sf <- if (legacy_02b) read_legacy_mappluto_02b_sf(raw_path) else read_mappluto_sf(raw_path)
  mappluto_attr <- mappluto_raw_sf |>
    st_drop_geometry() |>
    as_tibble()
  names(mappluto_attr) <- normalize_names(names(mappluto_attr))

  jia_codes <- c(164L, 226L, 227L, 228L, 355L, 356L, 480L, 481L, 482L, 483L, 484L, 595L)

  if (legacy_02b) {
    mappluto_attr <- mappluto_attr |>
      transmute(
        row_id = row_number(),
        source_id = source_id_value,
        source_vintage = release_label,
        bbl = build_bbl(
          pick_first_existing(pick(everything()), c("borough", "boro_code", "borocode", "borocode")),
          pick_first_existing(pick(everything()), c("block")),
          pick_first_existing(pick(everything()), c("lot"))
        ),
        native_borough = standardize_borough_code(pick_first_existing(pick(everything()), c("borough", "boro_code", "borocode"))),
        native_cd = standardize_community_district(
          pick_first_existing(pick(everything()), c("borough", "boro_code", "borocode")),
          pick_first_existing(pick(everything()), c("cd", "cd2"))
        ),
        lotarea = normalize_numeric_field(pick_first_existing(pick(everything()), c("lotarea"))),
        bldgarea = normalize_numeric_field(pick_first_existing(pick(everything()), c("bldgarea", "floorarea"))),
        resarea = normalize_numeric_field(pick_first_existing(pick(everything()), c("resarea"))),
        unitsres = normalize_numeric_field(pick_first_existing(pick(everything()), c("unitsres"))),
        unitstotal = normalize_numeric_field(pick_first_existing(pick(everything()), c("unitstotal"))),
        yearbuilt = normalize_year_field(pick_first_existing(pick(everything()), c("yearbuilt"))),
        builtfar = normalize_numeric_field(pick_first_existing(pick(everything()), c("builtfar", "far"))),
        residfar = normalize_numeric_field(pick_first_existing(pick(everything()), c("residfar", "maxallwfar"))),
        commfar = NA_real_,
        facilfar = NA_real_,
        landuse = str_pad(str_extract(as.character(pick_first_existing(pick(everything()), c("landuse", "landuse2"))), "\\d+"), width = 2, pad = "0"),
        bldgclass = str_to_upper(str_trim(as.character(pick_first_existing(pick(everything()), c("bldgclass"))))),
        landmark = as.character(pick_first_existing(pick(everything()), c("landmark"))),
        histdist = as.character(pick_first_existing(pick(everything()), c("histdist"))),
        is_joint_interest_area = native_cd %in% jia_codes
      )
  } else {
    mappluto_attr <- mappluto_attr |>
      transmute(
        row_id = row_number(),
        source_id = source_id_value,
        source_vintage = release_label,
        bbl = coalesce_character(
          normalize_text_field(pick_first_existing(pick(everything()), c("bbl"))),
          build_bbl(
            pick_first_existing(pick(everything()), c("borough", "boro_code", "borocode")),
            pick_first_existing(pick(everything()), c("block")),
            pick_first_existing(pick(everything()), c("lot"))
          )
        ),
        native_borough = standardize_borough_code(pick_first_existing(pick(everything()), c("borough", "boro_code", "borocode"))),
        native_cd = standardize_community_district(
          pick_first_existing(pick(everything()), c("borough", "boro_code", "borocode")),
          pick_first_existing(pick(everything()), c("cd"))
        ),
        lotarea = normalize_numeric_field(pick_first_existing(pick(everything()), c("lotarea"))),
        bldgarea = normalize_numeric_field(pick_first_existing(pick(everything()), c("bldgarea"))),
        resarea = normalize_numeric_field(pick_first_existing(pick(everything()), c("resarea"))),
        unitsres = normalize_numeric_field(pick_first_existing(pick(everything()), c("unitsres"))),
        unitstotal = normalize_numeric_field(pick_first_existing(pick(everything()), c("unitstotal"))),
        yearbuilt = normalize_year_field(pick_first_existing(pick(everything()), c("yearbuilt"))),
        builtfar = normalize_numeric_field(pick_first_existing(pick(everything()), c("builtfar"))),
        residfar = normalize_numeric_field(pick_first_existing(pick(everything()), c("residfar"))),
        commfar = normalize_numeric_field(pick_first_existing(pick(everything()), c("commfar"))),
        facilfar = normalize_numeric_field(pick_first_existing(pick(everything()), c("facilfar"))),
        landuse = str_pad(str_extract(as.character(pick_first_existing(pick(everything()), c("landuse"))), "\\d+"), width = 2, pad = "0"),
        bldgclass = str_to_upper(str_trim(as.character(pick_first_existing(pick(everything()), c("bldgclass"))))),
        landmark = as.character(pick_first_existing(pick(everything()), c("landmark"))),
        histdist = as.character(pick_first_existing(pick(everything()), c("histdist"))),
        is_joint_interest_area = native_cd %in% jia_codes
      )
  }

  mappluto_points <- st_sf(
    row_id = seq_len(nrow(mappluto_raw_sf)),
    geometry = st_point_on_surface(st_geometry(mappluto_raw_sf)),
    crs = st_crs(mappluto_raw_sf)
  )

  if (is.na(st_crs(mappluto_points))) {
    st_crs(mappluto_points) <- st_crs(council_sf)
  }

  mappluto_points <- st_transform(mappluto_points, st_crs(council_sf))
  district_hits <- st_intersects(mappluto_points, council_sf)
  assigned_flag <- lengths(district_hits) > 0
  assigned_row <- vapply(district_hits[assigned_flag], function(x) x[[1]], integer(1))

  assignment_df <- tibble(
    row_id = which(assigned_flag),
    council_row = assigned_row,
    council_match_count = lengths(district_hits)[assigned_flag]
  ) |>
    bind_cols(
      council_sf |>
        st_drop_geometry() |>
        slice(assigned_row) |>
        select(district_id, council_district, borough_code, borough_name)
    )

  list(
    lot_df = mappluto_attr |>
      inner_join(assignment_df, by = "row_id", relationship = "one-to-one") |>
      select(-row_id, -council_row),
    raw_row_count = nrow(mappluto_attr),
    assigned_row_count = nrow(assignment_df),
    unassigned_row_count = nrow(mappluto_attr) - nrow(assignment_df),
    boundary_tie_count = sum(assignment_df$council_match_count > 1, na.rm = TRUE)
  )
}

build_ccd_dataset <- function(raw_path, release_label, source_id_value, weighted_means = TRUE, valid_far_only = FALSE, legacy_02b = FALSE) {
  prepared <- prepare_mappluto_lots(raw_path, release_label, source_id_value, legacy_02b = legacy_02b)

  lot_df <- prepared$lot_df |>
    mutate(
      positive_lotarea = is.finite(lotarea) & lotarea > 0,
      built_far_calc = if_else(is.finite(bldgarea) & positive_lotarea, bldgarea / lotarea, NA_real_),
      built_far_use = case_when(
        legacy_02b & is.finite(built_far_calc) & built_far_calc >= 0 ~ built_far_calc,
        legacy_02b & is.finite(builtfar) & builtfar >= 0 ~ builtfar,
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
      above_res_cap = is.finite(max_resid_far) & max_resid_far > 0 & is.finite(built_far_use) & built_far_use >= max_resid_far,
      old_building_proxy = !is.na(yearbuilt) & yearbuilt >= 1800 & yearbuilt <= 1940,
      one_two_family_proxy = landuse %in% c("01", "02") | str_detect(coalesce(bldgclass, ""), "^[AB]"),
      vacant_proxy = landuse == "11" | str_detect(coalesce(bldgclass, ""), "^V"),
      parking_or_low_intensity_proxy = landuse == "10" | str_detect(coalesce(bldgclass, ""), "^G"),
      protected_proxy = truthy_value(landmark) | truthy_value(histdist)
    )

  borough_pre_df <- lot_df |>
    group_by(borough_code, borough_name) |>
    summarize(
      lot_count_before = n(),
      lot_area_before = sum(lotarea[positive_lotarea], na.rm = TRUE),
      .groups = "drop"
    )

  lot_df <- lot_df |>
    filter(!is_joint_interest_area, positive_lotarea)

  if (valid_far_only) {
    lot_df <- lot_df |>
      filter(is.finite(built_far_use), is.finite(max_resid_far))
  }

  city_built_median <- lot_df |>
    filter(is_residential_lot, is.finite(built_far_use)) |>
    summarize(median_value = stats::median(built_far_use, na.rm = TRUE)) |>
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
    left_join(borough_medians_df, by = c("borough_code", "borough_name"), relationship = "many-to-one") |>
    mutate(
      low_existing_far_boro = is_residential_lot & is.finite(built_far_use) & built_far_use < borough_built_far_median,
      low_existing_far_city = is_residential_lot & is.finite(built_far_use) & built_far_use < city_built_median
    )

  borough_post_df <- lot_df |>
    group_by(borough_code, borough_name) |>
    summarize(
      lot_count_after = n(),
      lot_area_after = sum(lotarea, na.rm = TRUE),
      .groups = "drop"
    )

  ccd_df <- lot_df |>
    group_by(district_id, council_district, borough_code, borough_name) |>
    summarize(
      source_release = release_label,
      aggregation_method = if_else(weighted_means, "lotarea_weighted", "unweighted_means"),
      valid_far_only = valid_far_only,
      lot_count_used = n(),
      ccd_lot_area_total = sum(lotarea, na.rm = TRUE),
      ccd_residential_lot_area = sum(if_else(is_residential_lot, lotarea, 0), na.rm = TRUE),
      ccd_bldg_area_total = sum(coalesce(bldgarea, 0), na.rm = TRUE),
      ccd_res_area_total = sum(coalesce(resarea, 0), na.rm = TRUE),
      ccd_unitsres_total = sum(coalesce(unitsres, 0), na.rm = TRUE),
      ccd_mean_built_far_lot_weighted = if (weighted_means) weighted.mean(built_far_use[is_residential_lot & is.finite(built_far_use)], lotarea[is_residential_lot & is.finite(built_far_use)], na.rm = TRUE) else mean(built_far_use[is_residential_lot & is.finite(built_far_use)], na.rm = TRUE),
      ccd_median_built_far = stats::median(built_far_use[is_residential_lot & is.finite(built_far_use)], na.rm = TRUE),
      ccd_mean_max_resid_far_lot_weighted = if (weighted_means) weighted.mean(max_resid_far[is_residential_lot & is.finite(max_resid_far)], lotarea[is_residential_lot & is.finite(max_resid_far)], na.rm = TRUE) else mean(max_resid_far[is_residential_lot & is.finite(max_resid_far)], na.rm = TRUE),
      ccd_mean_unused_res_far_lot_weighted = if (weighted_means) weighted.mean(unused_res_far[is_residential_lot & is.finite(unused_res_far)], lotarea[is_residential_lot & is.finite(unused_res_far)], na.rm = TRUE) else mean(unused_res_far[is_residential_lot & is.finite(unused_res_far)], na.rm = TRUE),
      ccd_sum_unused_res_floor_area = sum(if_else(is_residential_lot, coalesce(unused_res_floor_area, 0), 0), na.rm = TRUE),
      ccd_sum_unused_res_floor_area_all_lots = sum(coalesce(unused_res_floor_area, 0), na.rm = TRUE),
      ccd_sum_unused_any_floor_area = sum(coalesce(unused_any_floor_area, 0), na.rm = TRUE),
      ccd_share_lot_area_near_res_cap_80 = sum(if_else(near_res_cap_80, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      ccd_share_lot_area_above_res_cap = sum(if_else(above_res_cap, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      ccd_share_lot_area_low_existing_far_boro = sum(if_else(low_existing_far_boro, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      ccd_share_lot_area_low_existing_far_city = sum(if_else(low_existing_far_city, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      ccd_share_lot_area_vacant = sum(if_else(vacant_proxy, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      ccd_share_lot_area_one_two_family = sum(if_else(one_two_family_proxy, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      ccd_share_lot_area_old_building = sum(if_else(old_building_proxy, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      ccd_share_lot_area_protected = sum(if_else(protected_proxy, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      ccd_share_lot_area_parking_or_low_intensity = sum(if_else(parking_or_low_intensity_proxy, lotarea, 0), na.rm = TRUE) / sum(lotarea, na.rm = TRUE),
      ccd_built_res_floor_area_2010_2018 = sum(if_else(is_residential_lot & !is.na(yearbuilt) & yearbuilt >= 2010 & yearbuilt <= 2018, coalesce(resarea, 0), 0), na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(
      ccd_unused_res_floor_area_per_res_acre = if_else(ccd_residential_lot_area > 0, ccd_sum_unused_res_floor_area / (ccd_residential_lot_area / 43560), NA_real_),
      residential_acres = ccd_residential_lot_area / 43560
    ) |>
    left_join(
      council_measure |>
        select(-geometry_wkt),
      by = c("district_id", "council_district", "borough_code", "borough_name"),
      relationship = "one-to-one"
    ) |>
    group_by(borough_code, borough_name) |>
    mutate(
      demand_proxy_ratio_boro = median_household_income_1990 / mean(median_household_income_1990, na.rm = TRUE)
    ) |>
    ungroup()

  if (nrow(ccd_df) != 51) {
    stop("Redevelopment baseline does not cover exactly 51 Council districts for ", release_label, ".")
  }

  if (any(ccd_df$ccd_sum_unused_res_floor_area <= 0 | !is.finite(ccd_df$ccd_sum_unused_res_floor_area))) {
    stop("Found nonpositive unused residential floor area in ", release_label, ". Cannot log-transform index A.")
  }

  if (any(ccd_df$ccd_sum_unused_res_floor_area_all_lots <= 0 | !is.finite(ccd_df$ccd_sum_unused_res_floor_area_all_lots))) {
    stop("Found nonpositive all-lots unused residential floor area in ", release_label, ". Cannot log-transform all-lots index A.")
  }

  ccd_df <- ccd_df |>
    mutate(
      redev_A_raw = log(ccd_sum_unused_res_floor_area),
      redev_A_all_lots_raw = log(ccd_sum_unused_res_floor_area_all_lots),
      redev_B_raw = ccd_share_lot_area_low_existing_far_boro * ccd_mean_unused_res_far_lot_weighted,
      redev_D_raw = log(ccd_sum_unused_res_floor_area) * demand_proxy_ratio_boro,
      redev_A2010approx_raw = log(ccd_sum_unused_res_floor_area + ccd_built_res_floor_area_2010_2018)
    )

  component_names <- c(
    "ccd_mean_unused_res_far_lot_weighted",
    "ccd_share_lot_area_low_existing_far_boro",
    "ccd_share_lot_area_vacant",
    "ccd_share_lot_area_parking_or_low_intensity",
    "ccd_share_lot_area_near_res_cap_80",
    "ccd_share_lot_area_above_res_cap",
    "ccd_share_lot_area_protected",
    "ccd_mean_built_far_lot_weighted"
  )

  for (component_name in component_names) {
    ccd_df[[paste0(component_name, "_z_city")]] <- z_city(ccd_df[[component_name]])
  }

  ccd_df <- ccd_df |>
    mutate(
      redev_C_raw =
        ccd_mean_unused_res_far_lot_weighted_z_city +
        ccd_share_lot_area_low_existing_far_boro_z_city +
        ccd_share_lot_area_vacant_z_city +
        ccd_share_lot_area_parking_or_low_intensity_z_city -
        ccd_share_lot_area_near_res_cap_80_z_city -
        ccd_share_lot_area_above_res_cap_z_city -
        ccd_share_lot_area_protected_z_city -
        ccd_mean_built_far_lot_weighted_z_city,
      index_c_component_count = rowSums(!is.na(pick(ends_with("_z_city")))),
      index_c_components_used = paste(component_names, collapse = ";")
    )

  for (index_name in c("A", "A_all_lots", "B", "C", "D", "A2010approx")) {
    raw_name <- paste0("redev_", index_name, "_raw")
    city_name <- paste0("redev_", index_name, "_z_city")
    boro_name <- paste0("redev_", index_name, "_z_boro")
    high_name <- paste0("high_redev_", index_name)
    low_name <- paste0("low_redev_", index_name)

    ccd_df[[city_name]] <- z_city(ccd_df[[raw_name]])
    ccd_df[[boro_name]] <- ccd_df |>
      group_by(borough_code, borough_name) |>
      mutate(z_value = z_boro(.data[[raw_name]])) |>
      pull(z_value)

    borough_medians <- ccd_df |>
      group_by(borough_code, borough_name) |>
      summarize(median_value = stats::median(.data[[boro_name]], na.rm = TRUE), .groups = "drop")

    ccd_df <- ccd_df |>
      left_join(borough_medians, by = c("borough_code", "borough_name"), relationship = "many-to-one") |>
      mutate(
        !!high_name := .data[[boro_name]] >= median_value,
        !!low_name := .data[[boro_name]] < median_value
      ) |>
      select(-median_value)
  }

  ccd_df <- ccd_df |>
    mutate(
      redev_potential_A_raw = redev_A_raw,
      redev_potential_A_z_city = redev_A_z_city,
      redev_potential_A_z_boro = redev_A_z_boro,
      redev_potential_C_raw = redev_C_raw,
      redev_potential_C_z_city = redev_C_z_city,
      redev_potential_C_z_boro = redev_C_z_boro
    )

  list(
    ccd_df = ccd_df,
    lot_df = lot_df,
    borough_pre_df = borough_pre_df,
    borough_post_df = borough_post_df,
    raw_row_count = prepared$raw_row_count,
    assigned_row_count = prepared$assigned_row_count,
    unassigned_row_count = prepared$unassigned_row_count,
    boundary_tie_count = prepared$boundary_tie_count
  )
}

main_raw_path <- get_mappluto_raw_path("dcp_mappluto_archive", "18v1.1")
current_raw_path <- get_mappluto_raw_path("dcp_mappluto_current", "25v4")

main_build <- build_ccd_dataset(main_raw_path, "18v1.1", "dcp_mappluto_archive", weighted_means = TRUE, valid_far_only = FALSE)
current_build <- build_ccd_dataset(current_raw_path, "25v4", "dcp_mappluto_current", weighted_means = TRUE, valid_far_only = FALSE)
legacy_2002_build <- build_ccd_dataset("../input/mappluto_02b.zip", "02b", "dcp_mappluto_archive_legacy", weighted_means = TRUE, valid_far_only = FALSE, legacy_02b = TRUE)
main_unweighted_build <- build_ccd_dataset(main_raw_path, "18v1.1", "dcp_mappluto_archive", weighted_means = FALSE, valid_far_only = FALSE)
main_valid_far_build <- build_ccd_dataset(main_raw_path, "18v1.1", "dcp_mappluto_archive", weighted_means = TRUE, valid_far_only = TRUE)

baseline_df <- main_build$ccd_df |>
  left_join(
    legacy_2002_build$ccd_df |>
      select(
        district_id, council_district, borough_code, borough_name,
        source_release_2002 = source_release,
        lot_count_used_2002 = lot_count_used,
        ccd_lot_area_total_2002 = ccd_lot_area_total,
        ccd_residential_lot_area_2002 = ccd_residential_lot_area,
        ccd_sum_unused_allowed_floor_area_2002 = ccd_sum_unused_res_floor_area,
        ccd_sum_unused_allowed_floor_area_all_lots_2002 = ccd_sum_unused_res_floor_area_all_lots,
        redev_A2002_allowed_raw = redev_A_raw,
        redev_A2002_allowed_z_city = redev_A_z_city,
        redev_A2002_allowed_z_boro = redev_A_z_boro,
        high_redev_A2002_allowed = high_redev_A,
        low_redev_A2002_allowed = low_redev_A,
        redev_A2002_allowed_all_lots_raw = redev_A_all_lots_raw,
        redev_A2002_allowed_all_lots_z_city = redev_A_all_lots_z_city,
        redev_A2002_allowed_all_lots_z_boro = redev_A_all_lots_z_boro,
        high_redev_A2002_allowed_all_lots = high_redev_A_all_lots,
        low_redev_A2002_allowed_all_lots = low_redev_A_all_lots
      ),
    by = c("district_id", "council_district", "borough_code", "borough_name"),
    relationship = "one-to-one"
  ) |>
  left_join(
    current_build$ccd_df |>
      select(
        district_id, council_district, borough_code, borough_name,
        redev_A_25v4_z_boro = redev_A_z_boro,
        redev_A_all_lots_25v4_z_boro = redev_A_all_lots_z_boro,
        redev_C_25v4_z_boro = redev_C_z_boro,
        high_redev_A_25v4 = high_redev_A,
        high_redev_A_all_lots_25v4 = high_redev_A_all_lots,
        high_redev_C_25v4 = high_redev_C
      ),
    by = c("district_id", "council_district", "borough_code", "borough_name"),
    relationship = "one-to-one"
  ) |>
  left_join(
    main_valid_far_build$ccd_df |>
      select(
        district_id, council_district, borough_code, borough_name,
        redev_A_valid_far_z_boro = redev_A_z_boro,
        redev_A_all_lots_valid_far_z_boro = redev_A_all_lots_z_boro,
        redev_C_valid_far_z_boro = redev_C_z_boro,
        high_redev_A_valid_far = high_redev_A,
        high_redev_A_all_lots_valid_far = high_redev_A_all_lots,
        high_redev_C_valid_far = high_redev_C
      ),
    by = c("district_id", "council_district", "borough_code", "borough_name"),
    relationship = "one-to-one"
  ) |>
  left_join(
    main_unweighted_build$ccd_df |>
      select(
        district_id, council_district, borough_code, borough_name,
        redev_C_unweighted_z_boro = redev_C_z_boro
      ),
    by = c("district_id", "council_district", "borough_code", "borough_name"),
    relationship = "one-to-one"
  ) |>
  arrange(council_district)

write_csv_if_changed(baseline_df, "../output/ccdist2010_redevelopment_potential.csv")

cat("Wrote 2010 Council district redevelopment-potential output to ../output\n")
