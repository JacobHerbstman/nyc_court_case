# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_mappluto_yearbuilt/code")
# mappluto_lot_files_csv <- "../input/mappluto_lot_files.csv"
# out_city_csv <- "../output/mappluto_yearbuilt_city_summary.csv"
# out_borough_csv <- "../output/mappluto_yearbuilt_borough_summary.csv"
# out_cd_csv <- "../output/mappluto_yearbuilt_cd_summary.csv"
# out_adjacent_audit_csv <- "../output/mappluto_adjacent_release_audit.csv"
# out_support_csv <- "../output/mappluto_yearbuilt_support_summary.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

field_changed <- function(x, y) {
  xor(is.na(x), is.na(y)) | (!is.na(x) & !is.na(y) & x != y)
}

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 6) {
  stop("Expected 6 arguments: mappluto_lot_files_csv out_city_csv out_borough_csv out_cd_csv out_adjacent_audit_csv out_support_csv")
}

mappluto_lot_files_csv <- args[1]
out_city_csv <- args[2]
out_borough_csv <- args[3]
out_cd_csv <- args[4]
out_adjacent_audit_csv <- args[5]
out_support_csv <- args[6]

mappluto_index <- read_csv(mappluto_lot_files_csv, show_col_types = FALSE, na = c("", "NA")) |>
  filter(!is.na(parquet_path), file.exists(parquet_path)) |>
  mutate(
    source_id = as.character(source_id),
    vintage = as.character(vintage),
    parquet_path = as.character(parquet_path),
    release_rank = release_order_key(vintage),
    source_priority = if_else(source_id == "dcp_mappluto_current", 1L, 0L)
  ) |>
  arrange(release_rank, desc(source_priority), vintage) |>
  distinct(vintage, .keep_all = TRUE)

if (nrow(mappluto_index) == 0) {
  write_csv(tibble(), out_city_csv, na = "")
  write_csv(tibble(), out_borough_csv, na = "")
  write_csv(tibble(), out_cd_csv, na = "")
  write_csv(tibble(), out_adjacent_audit_csv, na = "")
  write_csv(tibble(metric = "status", value = "no_staged_mappluto_files", note = NA_character_), out_support_csv, na = "")
  quit(save = "no")
}

current_row <- mappluto_index |>
  arrange(desc(source_priority), desc(release_rank), desc(vintage)) |>
  slice_head(n = 1)

current_df <- read_parquet(current_row$parquet_path[[1]]) |>
  as.data.frame() |>
  as_tibble() |>
  mutate(
    borough_name = standardize_borough_name(borough),
    cd = suppressWarnings(as.integer(cd)),
    yearbuilt = suppressWarnings(as.integer(yearbuilt)),
    unitsres = suppressWarnings(as.numeric(unitsres)),
    unitstotal = suppressWarnings(as.numeric(unitstotal)),
    bldgarea = suppressWarnings(as.numeric(bldgarea)),
    resarea = suppressWarnings(as.numeric(resarea)),
    builtfar = suppressWarnings(as.numeric(builtfar))
  )

city_summary <- current_df |>
  group_by(yearbuilt) |>
  summarise(
    lot_count = n(),
    unitsres_total = sum(unitsres, na.rm = TRUE),
    unitstotal_total = sum(unitstotal, na.rm = TRUE),
    bldgarea_total = sum(bldgarea, na.rm = TRUE),
    resarea_total = sum(resarea, na.rm = TRUE),
    unitsres_per_lot = if_else(lot_count > 0, unitsres_total / lot_count, NA_real_),
    mean_builtfar = if_else(sum(!is.na(builtfar)) > 0, mean(builtfar, na.rm = TRUE), NA_real_),
    .groups = "drop"
  ) |>
  arrange(yearbuilt)

borough_summary <- current_df |>
  filter(!is.na(borough_name)) |>
  group_by(borough_name, yearbuilt) |>
  summarise(
    lot_count = n(),
    unitsres_total = sum(unitsres, na.rm = TRUE),
    unitstotal_total = sum(unitstotal, na.rm = TRUE),
    bldgarea_total = sum(bldgarea, na.rm = TRUE),
    resarea_total = sum(resarea, na.rm = TRUE),
    unitsres_per_lot = if_else(lot_count > 0, unitsres_total / lot_count, NA_real_),
    mean_builtfar = if_else(sum(!is.na(builtfar)) > 0, mean(builtfar, na.rm = TRUE), NA_real_),
    .groups = "drop"
  ) |>
  arrange(borough_name, yearbuilt)

cd_summary <- current_df |>
  filter(!is.na(cd), !is_joint_interest_area, cd >= 101L, cd <= 595L, !is.na(borough_name)) |>
  group_by(borough_name, cd, yearbuilt) |>
  summarise(
    lot_count = n(),
    unitsres_total = sum(unitsres, na.rm = TRUE),
    unitstotal_total = sum(unitstotal, na.rm = TRUE),
    bldgarea_total = sum(bldgarea, na.rm = TRUE),
    resarea_total = sum(resarea, na.rm = TRUE),
    unitsres_per_lot = if_else(lot_count > 0, unitsres_total / lot_count, NA_real_),
    mean_builtfar = if_else(sum(!is.na(builtfar)) > 0, mean(builtfar, na.rm = TRUE), NA_real_),
    .groups = "drop"
  ) |>
  arrange(cd, yearbuilt)

adjacent_rows <- list()

if (nrow(mappluto_index) >= 2) {
  for (i in 2:nrow(mappluto_index)) {
    prev_row <- mappluto_index[i - 1, ]
    next_row <- mappluto_index[i, ]

    prev_df <- read_parquet(prev_row$parquet_path[[1]]) |>
      as.data.frame() |>
      as_tibble() |>
      transmute(
        bbl = as.character(bbl),
        yearbuilt_prev = suppressWarnings(as.integer(yearbuilt)),
        unitsres_prev = suppressWarnings(as.numeric(unitsres)),
        unitstotal_prev = suppressWarnings(as.numeric(unitstotal)),
        bldgarea_prev = suppressWarnings(as.numeric(bldgarea)),
        resarea_prev = suppressWarnings(as.numeric(resarea))
      ) |>
      filter(!is.na(bbl))

    next_df <- read_parquet(next_row$parquet_path[[1]]) |>
      as.data.frame() |>
      as_tibble() |>
      transmute(
        bbl = as.character(bbl),
        yearbuilt_next = suppressWarnings(as.integer(yearbuilt)),
        unitsres_next = suppressWarnings(as.numeric(unitsres)),
        unitstotal_next = suppressWarnings(as.numeric(unitstotal)),
        bldgarea_next = suppressWarnings(as.numeric(bldgarea)),
        resarea_next = suppressWarnings(as.numeric(resarea))
      ) |>
      filter(!is.na(bbl))

    continuing_df <- inner_join(prev_df, next_df, by = "bbl")
    prev_only_bbl <- anti_join(prev_df |> distinct(bbl), next_df |> distinct(bbl), by = "bbl")
    next_only_bbl <- anti_join(next_df |> distinct(bbl), prev_df |> distinct(bbl), by = "bbl")
    continuing_count <- nrow(continuing_df)

    adjacent_rows[[i - 1]] <- tibble(
      prev_vintage = prev_row$vintage,
      next_vintage = next_row$vintage,
      prev_source_id = prev_row$source_id,
      next_source_id = next_row$source_id,
      prev_bbl_count = n_distinct(prev_df$bbl),
      next_bbl_count = n_distinct(next_df$bbl),
      continuing_bbl_count = continuing_count,
      entering_bbl_count = nrow(next_only_bbl),
      exiting_bbl_count = nrow(prev_only_bbl),
      yearbuilt_changed_count = sum(field_changed(continuing_df$yearbuilt_prev, continuing_df$yearbuilt_next)),
      unitsres_changed_count = sum(field_changed(continuing_df$unitsres_prev, continuing_df$unitsres_next)),
      unitstotal_changed_count = sum(field_changed(continuing_df$unitstotal_prev, continuing_df$unitstotal_next)),
      bldgarea_changed_count = sum(field_changed(continuing_df$bldgarea_prev, continuing_df$bldgarea_next)),
      resarea_changed_count = sum(field_changed(continuing_df$resarea_prev, continuing_df$resarea_next)),
      yearbuilt_changed_share = if_else(continuing_count > 0, yearbuilt_changed_count / continuing_count, NA_real_),
      unitsres_changed_share = if_else(continuing_count > 0, unitsres_changed_count / continuing_count, NA_real_),
      unitstotal_changed_share = if_else(continuing_count > 0, unitstotal_changed_count / continuing_count, NA_real_),
      bldgarea_changed_share = if_else(continuing_count > 0, bldgarea_changed_count / continuing_count, NA_real_),
      resarea_changed_share = if_else(continuing_count > 0, resarea_changed_count / continuing_count, NA_real_)
    )
  }
}

adjacent_audit <- bind_rows(adjacent_rows) |>
  arrange(release_order_key(prev_vintage), release_order_key(next_vintage), prev_vintage, next_vintage)

support_summary <- bind_rows(
  tibble(metric = "current_source_id", value = current_row$source_id[[1]], note = "Highest-ranked current staged MapPLUTO release used for the yearbuilt stock summaries."),
  tibble(metric = "current_vintage", value = current_row$vintage[[1]], note = "Current surviving-stock snapshot summarized by yearbuilt."),
  tibble(metric = "release_count", value = as.character(nrow(mappluto_index)), note = "Distinct staged MapPLUTO vintages available locally after deduplicating by release tag."),
  tibble(metric = "first_vintage", value = mappluto_index$vintage[[1]], note = "Earliest official archive release available locally."),
  tibble(metric = "last_vintage", value = mappluto_index$vintage[[nrow(mappluto_index)]], note = "Latest official release available locally."),
  tibble(metric = "current_missing_yearbuilt_share", value = as.character(mean(is.na(current_df$yearbuilt))), note = "Share of current staged lots without a plausible yearbuilt after standard normalization."),
  tibble(metric = "city_unitsres_reconcile_gap", value = as.character(sum(city_summary$unitsres_total, na.rm = TRUE) - sum(current_df$unitsres, na.rm = TRUE)), note = "Should equal zero because the city summary retains a missing-yearbuilt bucket."),
  tibble(metric = "city_unitstotal_reconcile_gap", value = as.character(sum(city_summary$unitstotal_total, na.rm = TRUE) - sum(current_df$unitstotal, na.rm = TRUE)), note = "Should equal zero because the city summary retains a missing-yearbuilt bucket."),
  tibble(metric = "city_bldgarea_reconcile_gap", value = as.character(sum(city_summary$bldgarea_total, na.rm = TRUE) - sum(current_df$bldgarea, na.rm = TRUE)), note = "Should equal zero because the city summary retains a missing-yearbuilt bucket."),
  tibble(metric = "city_resarea_reconcile_gap", value = as.character(sum(city_summary$resarea_total, na.rm = TRUE) - sum(current_df$resarea, na.rm = TRUE)), note = "Should equal zero because the city summary retains a missing-yearbuilt bucket."),
  tibble(metric = "median_adjacent_yearbuilt_changed_share", value = if_else(nrow(adjacent_audit) > 0, as.character(median(adjacent_audit$yearbuilt_changed_share, na.rm = TRUE)), NA_character_), note = "Across continuing BBLs in adjacent releases, this is the median share whose stored yearbuilt changes."),
  tibble(metric = "median_adjacent_unitsres_changed_share", value = if_else(nrow(adjacent_audit) > 0, as.character(median(adjacent_audit$unitsres_changed_share, na.rm = TRUE)), NA_character_), note = "Across continuing BBLs in adjacent releases, this is the median share whose residential-unit count changes."),
  tibble(metric = "supported_use", value = "surviving_stock_composition_and_recent_release_diff_diagnostics", note = "Use MapPLUTO for stock composition and recent release-diff diagnostics, including zoning/FAR/developability controls."),
  tibble(metric = "unsupported_use", value = "clean_annual_new_construction_flows_back_to_the_1970s", note = "Do not treat yearbuilt on surviving lots as a clean historical annual construction flow series.")
)

write_csv(city_summary, out_city_csv, na = "")
write_csv(borough_summary, out_borough_csv, na = "")
write_csv(cd_summary, out_cd_csv, na = "")
write_csv(adjacent_audit, out_adjacent_audit_csv, na = "")
write_csv(support_summary, out_support_csv, na = "")

cat("Wrote MapPLUTO yearbuilt summaries to", dirname(out_city_csv), "\n")
