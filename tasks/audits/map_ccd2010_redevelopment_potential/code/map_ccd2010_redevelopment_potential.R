suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(scales)
  library(sf)
  library(stringr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

theme_map <- function() {
  theme_void(base_size = 11) +
    theme(
      plot.background = element_rect(fill = "white", color = NA),
      panel.background = element_rect(fill = "white", color = NA),
      legend.position = "right",
      legend.title = element_text(size = 9),
      legend.text = element_text(size = 8),
      plot.title = element_text(face = "bold", size = 13, hjust = 0),
      plot.subtitle = element_text(size = 10, hjust = 0, margin = margin(b = 8)),
      plot.caption = element_text(size = 7, color = "grey35", hjust = 0)
    )
}

normalize_numeric_field <- function(x) {
  suppressWarnings(as.numeric(trimws(as.character(x))))
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

council_sf <- read_csv("../input/ccdist2010_homeownership_1990_measure.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    council_district = suppressWarnings(as.integer(council_district)),
    borough_code = as.character(borough_code),
    borough_name = borough_name,
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro)),
    geometry = st_as_sfc(geometry_wkt, crs = 2263)
  ) |>
  st_as_sf() |>
  arrange(council_district)

redev_df <- read_csv("../input/ccdist2010_redevelopment_potential.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    redev_A2002_allowed_all_lots_z_boro = suppressWarnings(as.numeric(redev_A2002_allowed_all_lots_z_boro)),
    redev_A2002_allowed_all_lots_raw = suppressWarnings(as.numeric(redev_A2002_allowed_all_lots_raw)),
    high_redev_A2002_allowed_all_lots = as.logical(high_redev_A2002_allowed_all_lots),
    ccd_sum_unused_allowed_floor_area_all_lots_2002 = suppressWarnings(as.numeric(ccd_sum_unused_allowed_floor_area_all_lots_2002))
  )

council_map_sf <- council_sf |>
  left_join(redev_df, by = "district_id", relationship = "one-to-one")

if (nrow(council_map_sf) != 51 || any(is.na(council_map_sf$redev_A2002_allowed_all_lots_z_boro))) {
  stop("Council district map input must cover exactly 51 districts with nonmissing 2002 opportunity fields.")
}

mappluto_raw_sf <- read_legacy_mappluto_02b_sf("../input/mappluto_02b.zip")
mappluto_attr <- mappluto_raw_sf |>
  st_drop_geometry() |>
  as_tibble()
names(mappluto_attr) <- normalize_names(names(mappluto_attr))

lot_attr <- mappluto_attr |>
  transmute(
    row_id = row_number(),
    lotarea = normalize_numeric_field(pick_first_existing(pick(everything()), c("lotarea"))),
    bldgarea = normalize_numeric_field(pick_first_existing(pick(everything()), c("bldgarea", "floorarea"))),
    reported_far = normalize_numeric_field(pick_first_existing(pick(everything()), c("far", "builtfar"))),
    max_all_far = normalize_numeric_field(pick_first_existing(pick(everything()), c("maxallwfar")))
  ) |>
  mutate(
    positive_lotarea = is.finite(lotarea) & lotarea > 0,
    built_far_calc = if_else(is.finite(bldgarea) & positive_lotarea, bldgarea / lotarea, NA_real_),
    built_far_use = case_when(
      is.finite(built_far_calc) & built_far_calc >= 0 ~ built_far_calc,
      is.finite(reported_far) & reported_far >= 0 ~ reported_far,
      TRUE ~ NA_real_
    ),
    unused_allowed_far = if_else(is.finite(max_all_far) & is.finite(built_far_use), pmax(max_all_far - built_far_use, 0), NA_real_),
    unused_allowed_floor_area = if_else(is.finite(unused_allowed_far) & positive_lotarea, unused_allowed_far * lotarea, NA_real_)
  )

lot_points <- st_sf(
  row_id = seq_len(nrow(mappluto_raw_sf)),
  geometry = st_point_on_surface(st_geometry(mappluto_raw_sf)),
  crs = st_crs(mappluto_raw_sf)
)

if (is.na(st_crs(lot_points))) {
  st_crs(lot_points) <- st_crs(council_sf)
}

lot_points <- st_transform(lot_points, st_crs(council_sf))

city_union <- st_union(st_geometry(council_sf))
hex_sf <- st_sf(
  hex_id = seq_along(st_make_grid(city_union, cellsize = 2500, square = FALSE)),
  geometry = st_make_grid(city_union, cellsize = 2500, square = FALSE),
  crs = st_crs(council_sf)
) |>
  mutate(in_city = lengths(st_intersects(st_point_on_surface(geometry), city_union)) > 0) |>
  filter(in_city) |>
  select(-in_city)

hex_hits <- st_intersects(lot_points, hex_sf)
assigned_flag <- lengths(hex_hits) > 0

hex_assignment <- tibble(
  row_id = which(assigned_flag),
  hex_row = vapply(hex_hits[assigned_flag], function(x) x[[1]], integer(1))
) |>
  mutate(hex_id = hex_sf$hex_id[hex_row]) |>
  select(row_id, hex_id)

hex_values <- lot_attr |>
  inner_join(hex_assignment, by = "row_id", relationship = "one-to-one") |>
  filter(positive_lotarea, is.finite(unused_allowed_floor_area)) |>
  group_by(hex_id) |>
  summarize(
    lot_count = n(),
    lot_area = sum(lotarea, na.rm = TRUE),
    unused_allowed_floor_area = sum(unused_allowed_floor_area, na.rm = TRUE),
    unused_allowed_far_lot_area_weighted = unused_allowed_floor_area / lot_area,
    .groups = "drop"
  ) |>
  mutate(
    log10_unused_allowed_floor_area = log10(unused_allowed_floor_area + 1),
    unused_allowed_floor_area_per_lot_acre = unused_allowed_floor_area / (lot_area / 43560)
  )

hex_map_sf <- hex_sf |>
  left_join(hex_values, by = "hex_id", relationship = "one-to-one") |>
  mutate(
    lot_count = coalesce(lot_count, 0L),
    lot_area = coalesce(lot_area, 0),
    unused_allowed_floor_area = coalesce(unused_allowed_floor_area, 0),
    log10_unused_allowed_floor_area = if_else(unused_allowed_floor_area > 0, log10_unused_allowed_floor_area, NA_real_),
    unused_allowed_floor_area_per_lot_acre = if_else(unused_allowed_floor_area > 0, unused_allowed_floor_area_per_lot_acre, NA_real_)
  )

st_write(hex_map_sf, "../output/ccdist2010_redev_2002_hex_grid.gpkg", delete_dsn = TRUE, quiet = TRUE)

city_outline <- st_as_sf(tibble(geometry = city_union))

hex_logsum_plot <- ggplot() +
  geom_sf(data = hex_map_sf, aes(fill = log10_unused_allowed_floor_area), color = NA, na.rm = TRUE) +
  geom_sf(data = council_sf, fill = NA, color = "white", linewidth = 0.15, alpha = 0.8) +
  geom_sf(data = city_outline, fill = NA, color = "grey20", linewidth = 0.25) +
  scale_fill_gradientn(
    colors = c("#fff7bc", "#fec44f", "#fe9929", "#ec7014", "#cc4c02", "#8c2d04"),
    name = "log10 residual\nfloor area",
    na.value = "white"
  ) +
  coord_sf(datum = NA) +
  labs(
    title = "2002 Redevelopment Opportunity, Aggregated to Hex Cells",
    subtitle = "All-lots residual allowed envelope: max(MaxAllwFAR - built FAR, 0) x lot area",
    caption = "2002 MapPLUTO 02b. Built FAR uses floor area / lot area first, with reported FAR fallback. Hex cells are 2,500 ft."
  ) +
  theme_map()

hex_density_plot <- ggplot() +
  geom_sf(data = hex_map_sf, aes(fill = unused_allowed_floor_area_per_lot_acre), color = NA, na.rm = TRUE) +
  geom_sf(data = council_sf, fill = NA, color = "white", linewidth = 0.15, alpha = 0.8) +
  geom_sf(data = city_outline, fill = NA, color = "grey20", linewidth = 0.25) +
  scale_fill_gradientn(
    colors = c("#f7fcf0", "#ccebc5", "#7bccc4", "#2b8cbe", "#084081"),
    trans = "log10",
    labels = label_number(big.mark = ","),
    name = "residual floor\narea per lot acre",
    na.value = "white"
  ) +
  coord_sf(datum = NA) +
  labs(
    title = "2002 Redevelopment Opportunity Density",
    subtitle = "Residual allowed envelope per acre of valid 2002 lot area within each hex cell",
    caption = "2002 MapPLUTO 02b. Built FAR uses floor area / lot area first, with reported FAR fallback. Hex cells are 2,500 ft."
  ) +
  theme_map()

council_plot <- ggplot(council_map_sf) +
  geom_sf(aes(fill = redev_A2002_allowed_all_lots_z_boro), color = "white", linewidth = 0.25) +
  geom_sf_text(aes(label = council_district), size = 2.2, color = "grey15", check_overlap = TRUE) +
  scale_fill_gradient2(
    low = "#2c7bb6",
    mid = "#f7f7f7",
    high = "#d7191c",
    midpoint = 0,
    name = "within-borough\nz-score",
    na.value = "grey90"
  ) +
  coord_sf(datum = NA) +
  labs(
    title = "2002 Redevelopment Opportunity by 2010 Council District",
    subtitle = "Within-borough z-score of log summed all-lots residual allowed envelope",
    caption = "Red districts have higher 2002 allowed-envelope residual capacity relative to other districts in the same borough."
  ) +
  theme_map()

ggsave("../output/ccdist2010_redev_2002_hex_logsum_map.png", hex_logsum_plot, width = 8.5, height = 8.5, dpi = 300, bg = "white")
ggsave("../output/ccdist2010_redev_2002_hex_logsum_map.pdf", hex_logsum_plot, width = 8.5, height = 8.5, bg = "white")
ggsave("../output/ccdist2010_redev_2002_hex_density_map.png", hex_density_plot, width = 8.5, height = 8.5, dpi = 300, bg = "white")
ggsave("../output/ccdist2010_redev_2002_hex_density_map.pdf", hex_density_plot, width = 8.5, height = 8.5, bg = "white")
ggsave("../output/ccdist2010_redev_2002_council_map.png", council_plot, width = 8.5, height = 8.5, dpi = 300, bg = "white")
ggsave("../output/ccdist2010_redev_2002_council_map.pdf", council_plot, width = 8.5, height = 8.5, bg = "white")

write_csv_if_changed(
  bind_rows(
    tibble(
      section = "coverage",
      item = c("raw_2002_lot_rows", "hex_count", "hex_count_with_positive_residual", "council_district_count"),
      value = as.character(c(
        nrow(mappluto_attr),
        nrow(hex_map_sf),
        sum(hex_map_sf$unused_allowed_floor_area > 0, na.rm = TRUE),
        nrow(council_map_sf)
      )),
      note = "Coverage of the aggregated 2002 redevelopment-potential maps."
    ),
    tibble(
      section = "measurement",
      item = c("built_far_calc_finite_share", "reported_far_fallback_share", "sum_unused_allowed_floor_area"),
      value = as.character(c(
        mean(is.finite(lot_attr$built_far_calc[lot_attr$positive_lotarea]), na.rm = TRUE),
        mean(!is.finite(lot_attr$built_far_calc[lot_attr$positive_lotarea]) & is.finite(lot_attr$reported_far[lot_attr$positive_lotarea]), na.rm = TRUE),
        sum(lot_attr$unused_allowed_floor_area, na.rm = TRUE)
      )),
      note = "Built-FAR and residual-capacity measurement checks for the 2002 map inputs."
    ),
    tibble(
      section = "output",
      item = c("hex_cell_size_feet", "aggregation_level", "opportunity_measure"),
      value = c("2500", "hex grid and 2010 Council districts", "max(MaxAllwFAR - built FAR, 0) * lot area"),
      note = "Map design choices."
    )
  ),
  "../output/ccdist2010_redev_2002_map_qc.csv"
)

cat("Wrote 2002 redevelopment-potential maps to ../output\n")
