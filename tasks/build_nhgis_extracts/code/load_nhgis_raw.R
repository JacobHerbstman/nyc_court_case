# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_nhgis_extracts/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

extract_number_from_path <- function(path) {
  suppressWarnings(as.integer(str_extract(basename(path), "(?<=nhgis)[0-9]{4}")))
}

nhgis_table_map <- read_csv("nhgis_table_map.csv", show_col_types = FALSE, na = c("", "NA"))
nhgis_extract_downloads <- read_csv("../temp/nhgis_extract_downloads.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(extract_number = coalesce(extract_number, extract_number_from_path(raw_path)))

expected_rows <- tibble(year = sort(unique(nhgis_table_map$year))) %>%
  mutate(source_id = paste0("nhgis_", year, "_tract_extract"))

if (nrow(expected_rows) != 1 || expected_rows$source_id[[1]] != "nhgis_1990_tract_extract") {
  stop("This task should load exactly the 1990 tract extract.")
}

nyc_counties <- c("005", "047", "061", "081", "085")

index_rows <- list()

for (i in seq_len(nrow(expected_rows))) {
  row <- expected_rows[i, ]
  source_files <- nhgis_extract_downloads %>%
    filter(source_id == row$source_id, !is.na(raw_path), file.exists(raw_path)) %>%
    mutate(
      extract_number = ifelse(is.na(extract_number), -1, extract_number),
      fetch_status = as.character(status),
      fetch_status_ok = is.na(fetch_status) | !str_detect(fetch_status, "failed")
    ) %>%
    arrange(desc(extract_number), desc(status == "downloaded"), raw_path)

  paired_extracts <- source_files %>%
    filter(fetch_status_ok, file_role %in% c("table_data", "gis_data")) %>%
    group_by(extract_number) %>%
    summarise(
      table_file_count = sum(file_role == "table_data"),
      gis_file_count = sum(file_role == "gis_data"),
      .groups = "drop"
    ) %>%
    filter(extract_number > 0, table_file_count > 0, gis_file_count > 0) %>%
    arrange(desc(extract_number))

  selected_extract_number <- if (nrow(paired_extracts) == 0) {
    NA_integer_
  } else {
    paired_extracts$extract_number[[1]]
  }

  table_zip <- source_files %>%
    filter(fetch_status_ok, file_role == "table_data", extract_number == selected_extract_number) %>%
    slice_head(n = 1) %>%
    pull(raw_path)

  gis_zip <- source_files %>%
    filter(fetch_status_ok, file_role == "gis_data", extract_number == selected_extract_number) %>%
    slice_head(n = 1) %>%
    pull(raw_path)

  missing_status <- if (nrow(source_files) == 0) {
    "fetch_required"
  } else if (any(str_detect(as.character(source_files$status), "failed"), na.rm = TRUE)) {
    "fetch_failed"
  } else {
    "bundle_incomplete"
  }

  if (length(table_zip) == 0 || length(gis_zip) == 0) {
    index_rows[[i]] <- tibble(
      source_id = row$source_id,
      year = row$year,
      extract_number = selected_extract_number,
      table_zip_path = if (length(table_zip) == 0) NA_character_ else table_zip[[1]],
      gis_zip_path = if (length(gis_zip) == 0) NA_character_ else gis_zip[[1]],
      table_file_inside_zip = NA_character_,
      shapefile_inside_zip = NA_character_,
      raw_parquet_path = NA_character_,
      status = missing_status
    )
    next
  }

  table_zip <- table_zip[[1]]
  gis_zip <- gis_zip[[1]]
  table_listing <- unzip(table_zip, list = TRUE)
  gis_listing <- unzip(gis_zip, list = TRUE)

  table_candidates <- table_listing$Name[
    str_detect(tolower(table_listing$Name), "\\.(csv|dat)$") &
      !str_detect(tolower(table_listing$Name), "(_datadict|_geog|_tables)\\.csv$")
  ]
  shapefile_candidates <- gis_listing$Name[
    str_detect(tolower(gis_listing$Name), "\\.shp$|shapefile.*\\.zip$")
  ]
  has_expected_shape <- any(str_detect(
    tolower(c(basename(gis_zip), shapefile_candidates)),
    paste0(
      "tract.*", row$year, ".*tl2000|",
      row$year, ".*tract.*tl2000|",
      "us_tract_", row$year, "_tl2000|",
      "shapefile.*tl2000.*tract.*", row$year
    )
  ))

  if (length(table_candidates) == 0 || length(shapefile_candidates) == 0 || !has_expected_shape) {
    index_rows[[i]] <- tibble(
      source_id = row$source_id,
      year = row$year,
      extract_number = selected_extract_number,
      table_zip_path = table_zip,
      gis_zip_path = gis_zip,
      table_file_inside_zip = if (length(table_candidates) == 0) NA_character_ else table_candidates[[1]],
      shapefile_inside_zip = if (length(shapefile_candidates) == 0) NA_character_ else shapefile_candidates[[1]],
      raw_parquet_path = NA_character_,
      status = "bundle_validation_failed"
    )
    next
  }

  table_dfs <- lapply(table_candidates, function(table_file) {
    out <- read_csv(unz(table_zip, table_file), show_col_types = FALSE, guess_max = 50000)
    names(out) <- normalize_names(names(out))
    out
  })

  nhgis_df <- table_dfs[[1]]

  if (length(table_dfs) > 1) {
    for (j in 2:length(table_dfs)) {
      join_keys <- intersect(
        c("gisjoin", "year", "state", "statea", "county", "countya", "tract", "tracta"),
        intersect(names(nhgis_df), names(table_dfs[[j]]))
      )

      if (length(join_keys) == 0) {
        stop("Could not identify NHGIS join keys across multiple dataset CSV files.")
      }

      nhgis_df <- nhgis_df %>%
        left_join(
          table_dfs[[j]] %>%
            select(any_of(join_keys), any_of(setdiff(names(table_dfs[[j]]), names(nhgis_df)))),
          by = join_keys,
          relationship = "many-to-one"
        )
    }
  }

  expected_codes <- nhgis_table_map %>%
    filter(year == row$year) %>%
    pull(nhgis_code) %>%
    normalize_names()
  missing_codes <- expected_codes[!expected_codes %in% names(nhgis_df)]

  if (length(missing_codes) > 0) {
    index_rows[[i]] <- tibble(
      source_id = row$source_id,
      year = row$year,
      extract_number = selected_extract_number,
      table_zip_path = table_zip,
      gis_zip_path = gis_zip,
      table_file_inside_zip = table_candidates[[1]],
      shapefile_inside_zip = shapefile_candidates[[1]],
      raw_parquet_path = NA_character_,
      status = "bundle_validation_failed"
    )
    next
  }

  nhgis_df$statea <- pick_first_existing(nhgis_df, c("statea"))
  nhgis_df$countya <- pick_first_existing(nhgis_df, c("countya"))
  nhgis_df$statea_std <- str_pad(str_extract(as.character(nhgis_df$statea), "[0-9]+"), width = 2, side = "left", pad = "0")
  nhgis_df$countya_std <- str_pad(str_extract(as.character(nhgis_df$countya), "[0-9]+"), width = 3, side = "left", pad = "0")
  nhgis_df <- nhgis_df %>%
    filter(statea_std == "36", countya_std %in% nyc_counties) %>%
    select(-statea_std, -countya_std) %>%
    mutate(source_id = row$source_id, source_year = row$year, table_zip_path = table_zip, gis_zip_path = gis_zip)

  write_parquet_if_changed(nhgis_df, "../temp/nhgis_1990_tract_extract_raw.parquet")

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    year = row$year,
    extract_number = selected_extract_number,
    table_zip_path = table_zip,
    gis_zip_path = gis_zip,
    table_file_inside_zip = table_candidates[[1]],
    shapefile_inside_zip = shapefile_candidates[[1]],
    raw_parquet_path = "../temp/nhgis_1990_tract_extract_raw.parquet",
    status = "loaded"
  )
}

write_csv_if_changed(bind_rows(index_rows), "../temp/nhgis_raw_files.csv")
cat("Wrote NHGIS raw outputs to ../temp\n")
