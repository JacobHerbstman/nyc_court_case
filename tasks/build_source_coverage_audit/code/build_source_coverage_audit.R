# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_source_coverage_audit/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# mappluto_qc_csv <- "../input/mappluto_lot_qc.csv"
# dob_qc_csv <- "../input/dob_open_data_qc.csv"
# nhgis_qc_csv <- "../input/nhgis_qc.csv"
# dcp_boundary_qc_csv <- "../input/dcp_boundary_qc.csv"
# census_bps_qc_csv <- "../input/census_bps_qc.csv"
# dcp_housing_database_qc_csv <- "../input/dcp_housing_database_qc.csv"
# census_acs_housing_qc_csv <- "../input/census_acs_housing_qc.csv"
# furman_files_csv <- "../input/furman_coredata_files.csv"
# archival_inventory_csv <- "../input/archival_record_inventory.csv"
# crosswalk_qc_csv <- "../input/lot_identifier_crosswalk_qc.csv"
# mappluto_release_diff_summary_csv <- "../input/mappluto_release_diff_summary.csv"
# benchmark_results_csv <- "../input/benchmark_results.csv"
# human_qa_spot_check_csv <- "human_qa_spot_check.csv"
# out_audit_csv <- "../output/source_coverage_audit.csv"
# out_human_qa_status_csv <- "../output/human_qa_gate_status.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 16) {
  stop("Expected 16 arguments for the coverage audit task.")
}

source_catalog_csv <- args[1]
mappluto_qc_csv <- args[2]
dob_qc_csv <- args[3]
nhgis_qc_csv <- args[4]
dcp_boundary_qc_csv <- args[5]
census_bps_qc_csv <- args[6]
dcp_housing_database_qc_csv <- args[7]
census_acs_housing_qc_csv <- args[8]
furman_files_csv <- args[9]
archival_inventory_csv <- args[10]
crosswalk_qc_csv <- args[11]
mappluto_release_diff_summary_csv <- args[12]
benchmark_results_csv <- args[13]
human_qa_spot_check_csv <- args[14]
out_audit_csv <- args[15]
out_human_qa_status_csv <- args[16]

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
mappluto_qc <- read_csv(mappluto_qc_csv, show_col_types = FALSE, na = c("", "NA"))
dob_qc <- read_csv(dob_qc_csv, show_col_types = FALSE, na = c("", "NA"))
nhgis_qc <- read_csv(nhgis_qc_csv, show_col_types = FALSE, na = c("", "NA"))
dcp_boundary_qc <- read_csv(dcp_boundary_qc_csv, show_col_types = FALSE, na = c("", "NA"))
census_bps_qc <- read_csv(census_bps_qc_csv, show_col_types = FALSE, na = c("", "NA"))
dcp_housing_database_qc <- read_csv(dcp_housing_database_qc_csv, show_col_types = FALSE, na = c("", "NA"))
census_acs_housing_qc <- read_csv(census_acs_housing_qc_csv, show_col_types = FALSE, na = c("", "NA"))
furman_files <- read_csv(furman_files_csv, show_col_types = FALSE, na = c("", "NA"))
archival_inventory <- read_csv(archival_inventory_csv, show_col_types = FALSE, na = c("", "NA"))
crosswalk_qc <- read_csv(crosswalk_qc_csv, show_col_types = FALSE, na = c("", "NA"))
mappluto_release_diff_summary <- read_csv(mappluto_release_diff_summary_csv, show_col_types = FALSE, na = c("", "NA"))
benchmark_results <- read_csv(benchmark_results_csv, show_col_types = FALSE, na = c("", "NA"))
human_qa_spot_check <- read_csv(
  human_qa_spot_check_csv,
  show_col_types = FALSE,
  na = c("", "NA"),
  col_types = cols(.default = col_character())
)

source_coverage <- source_catalog %>%
  transmute(
    source_id,
    source_name,
    access_mode,
    earliest_date = as.character(start_date),
    latest_date = as.character(end_date),
    row_count = NA_real_,
    share_with_bbl = NA_real_,
    share_with_bin = NA_real_,
    share_with_usable_address = NA_real_,
    share_mapped_current_cd = NA_real_,
    known_coverage_gaps = notes
  )

if (nrow(dob_qc) > 0) {
  source_coverage <- source_coverage %>%
    left_join(
      dob_qc %>%
        transmute(
          source_id,
          row_count_dob = row_count,
          earliest_date_dob = as.character(start_date),
          latest_date_dob = as.character(end_date),
          share_with_bbl_dob = nonmissing_bbl_share,
          share_with_bin_dob = nonmissing_bin_share,
          share_with_usable_address_dob = nonmissing_address_share,
          share_mapped_current_cd_dob = nonmissing_cd_share
        ),
      by = "source_id"
    ) %>%
    mutate(
      row_count = coalesce(row_count_dob, row_count),
      earliest_date = coalesce(earliest_date_dob, earliest_date),
      latest_date = coalesce(latest_date_dob, latest_date),
      share_with_bbl = coalesce(share_with_bbl_dob, share_with_bbl),
      share_with_bin = coalesce(share_with_bin_dob, share_with_bin),
      share_with_usable_address = coalesce(share_with_usable_address_dob, share_with_usable_address),
      share_mapped_current_cd = coalesce(share_mapped_current_cd_dob, share_mapped_current_cd)
    ) %>%
    select(-ends_with("_dob"))
}

if (nrow(mappluto_qc) > 0 && all(c("row_count", "min_yearbuilt", "max_yearbuilt", "nonmissing_bbl_share", "ordinary_cd_rows") %in% names(mappluto_qc))) {
  mappluto_qc_by_source <- mappluto_qc %>%
    group_by(source_id) %>%
    summarise(
      row_count_mappluto = sum(row_count, na.rm = TRUE),
      earliest_date_mappluto = as.character(min(min_yearbuilt, na.rm = TRUE)),
      latest_date_mappluto = as.character(max(max_yearbuilt, na.rm = TRUE)),
      share_with_bbl_mappluto = mean(nonmissing_bbl_share, na.rm = TRUE),
      share_mapped_current_cd_mappluto = sum(ordinary_cd_rows, na.rm = TRUE) / sum(row_count, na.rm = TRUE),
      .groups = "drop"
    )

  source_coverage <- source_coverage %>%
    left_join(mappluto_qc_by_source, by = "source_id") %>%
    mutate(
      row_count = coalesce(row_count_mappluto, row_count),
      earliest_date = coalesce(earliest_date_mappluto, earliest_date),
      latest_date = coalesce(latest_date_mappluto, latest_date),
      share_with_bbl = coalesce(share_with_bbl_mappluto, share_with_bbl),
      share_mapped_current_cd = coalesce(share_mapped_current_cd_mappluto, share_mapped_current_cd)
    ) %>%
    select(-ends_with("_mappluto"))
}

if (nrow(nhgis_qc) > 0) {
  source_coverage <- source_coverage %>%
    left_join(
      nhgis_qc %>%
        select(source_id, row_count_nhgis = row_count),
      by = "source_id"
    ) %>%
    mutate(row_count = coalesce(row_count_nhgis, row_count)) %>%
    select(-row_count_nhgis)
}

if (nrow(dcp_housing_database_qc) > 0) {
  dcp_housing_summary <- dcp_housing_database_qc %>%
    group_by(source_id) %>%
    summarise(
      row_count_dcp_hdb = max(row_count, na.rm = TRUE),
      earliest_date_dcp_hdb = as.character(min(permit_year_start, na.rm = TRUE)),
      latest_date_dcp_hdb = as.character(max(permit_year_end, na.rm = TRUE)),
      share_with_bbl_dcp_hdb = mean(nonmissing_bbl_share, na.rm = TRUE),
      share_with_bin_dcp_hdb = mean(nonmissing_bin_share, na.rm = TRUE),
      share_with_usable_address_dcp_hdb = mean(nonmissing_address_share, na.rm = TRUE),
      share_mapped_current_cd_dcp_hdb = mean(nonmissing_cd_share, na.rm = TRUE),
      .groups = "drop"
    )

  source_coverage <- source_coverage %>%
    left_join(dcp_housing_summary, by = "source_id") %>%
    mutate(
      row_count = coalesce(row_count_dcp_hdb, row_count),
      earliest_date = coalesce(earliest_date_dcp_hdb, earliest_date),
      latest_date = coalesce(latest_date_dcp_hdb, latest_date),
      share_with_bbl = coalesce(share_with_bbl_dcp_hdb, share_with_bbl),
      share_with_bin = coalesce(share_with_bin_dcp_hdb, share_with_bin),
      share_with_usable_address = coalesce(share_with_usable_address_dcp_hdb, share_with_usable_address),
      share_mapped_current_cd = coalesce(share_mapped_current_cd_dcp_hdb, share_mapped_current_cd)
    ) %>%
    select(-ends_with("_dcp_hdb"))
}

if (nrow(census_acs_housing_qc) > 0) {
  acs_summary <- census_acs_housing_qc %>%
    summarise(
      source_id = "census_acs_housing_tract_5yr",
      row_count_acs = sum(tract_count, na.rm = TRUE),
      earliest_date_acs = as.character(min(year, na.rm = TRUE)),
      latest_date_acs = as.character(max(year, na.rm = TRUE))
    )

  source_coverage <- source_coverage %>%
    left_join(acs_summary, by = "source_id") %>%
    mutate(
      row_count = coalesce(row_count_acs, row_count),
      earliest_date = coalesce(earliest_date_acs, earliest_date),
      latest_date = coalesce(latest_date_acs, latest_date)
    ) %>%
    select(-row_count_acs, -earliest_date_acs, -latest_date_acs)
}

inventory_sources <- bind_rows(
  mappluto_qc %>% transmute(source_id, status = paste0("vintage=", vintage, ";rows=", row_count)),
  dcp_boundary_qc %>% transmute(source_id, status = paste0("district_count=", district_count)),
  census_bps_qc %>% transmute(source_id = "census_bps_place_ascii", status = paste0("year=", year, ";status=", status)),
  dcp_housing_database_qc %>% transmute(source_id, status = paste0("release=", vintage, ";rows=", row_count)),
  census_acs_housing_qc %>% transmute(source_id = "census_acs_housing_tract_5yr", status = paste0("year=", year, ";status=", status)),
  furman_files %>% transmute(source_id, status),
  archival_inventory %>%
    count(source_id, name = "inventory_rows") %>%
    transmute(source_id, status = paste0("inventory_rows=", inventory_rows))
) %>%
  group_by(source_id) %>%
  summarise(status = paste(status, collapse = " | "), .groups = "drop")

if (nrow(inventory_sources) > 0) {
  source_coverage <- source_coverage %>%
    left_join(inventory_sources, by = "source_id") %>%
    mutate(
      known_coverage_gaps = if_else(
        !is.na(status),
        paste(known_coverage_gaps, "[status:", status, "]"),
        known_coverage_gaps
      )
    ) %>%
    select(-status)
}

if (nrow(mappluto_release_diff_summary) > 0) {
  yearbuilt_failures <- sum(!is.na(mappluto_release_diff_summary$yearbuilt_change_rate) & mappluto_release_diff_summary$yearbuilt_change_rate > 0.001)
  unitsres_positive_pairs <- sum(!is.na(mappluto_release_diff_summary$unitsres_change_rate) & mappluto_release_diff_summary$unitsres_change_rate > 0)

  source_coverage <- source_coverage %>%
    mutate(
      known_coverage_gaps = if_else(
        source_id %in% c("dcp_mappluto_current", "dcp_mappluto_archive"),
        paste(
          known_coverage_gaps,
          paste0("[release_diff: yearbuilt_failures=", yearbuilt_failures, ";unitsres_positive_pairs=", unitsres_positive_pairs, "]")
        ),
        known_coverage_gaps
      )
    )
}

if (nrow(benchmark_results) > 0) {
  benchmark_summary <- benchmark_results %>%
    summarise(
      blocking_failures = sum(blocking_flag %in% TRUE & result_status == "fail", na.rm = TRUE),
      blocking_deferred = sum(blocking_flag %in% TRUE & result_status == "deferred", na.rm = TRUE),
      supporting_flags = sum(!(blocking_flag %in% TRUE) & result_status %in% c("fail", "review"), na.rm = TRUE)
    )

  benchmark_note <- paste0(
    "[benchmarks: blocking_failures=", benchmark_summary$blocking_failures[1],
    ";blocking_deferred=", benchmark_summary$blocking_deferred[1],
    ";supporting_flags=", benchmark_summary$supporting_flags[1],
    "]"
  )

  source_coverage <- source_coverage %>%
    mutate(known_coverage_gaps = paste(known_coverage_gaps, benchmark_note))
}

write_csv(source_coverage, out_audit_csv, na = "")

completed_rows <- human_qa_spot_check %>%
  filter(
    !is.na(check_id),
    !is.na(raw_identifier),
    !is.na(checked_by),
    !is.na(checked_date),
    !is.na(result)
  )

group_summary <- human_qa_spot_check %>%
  count(qa_group, name = "planned_rows") %>%
  left_join(
    completed_rows %>%
      count(qa_group, name = "completed_rows"),
    by = "qa_group"
  ) %>%
  mutate(completed_rows = coalesce(completed_rows, 0L))

total_spot_check_rows <- nrow(human_qa_spot_check)
completed_spot_check_rows <- nrow(completed_rows)
group_pass <- nrow(group_summary) > 0 && all(group_summary$completed_rows >= group_summary$planned_rows)

human_qa_note <- if (nrow(crosswalk_qc) == 0) {
  "Populate all 30 stratified checked rows before any downstream aggregation task."
} else {
  paste(
    "Populate all 30 stratified checked rows before any downstream aggregation task.",
    "Crosswalk summary:",
    paste(names(crosswalk_qc), unlist(crosswalk_qc[1, ]), sep = "=", collapse = "; "),
    "Group status:",
    paste(paste(group_summary$qa_group, group_summary$completed_rows, group_summary$planned_rows, sep = ":"), collapse = " | ")
  )
}

human_qa_status <- tibble(
  total_rows = total_spot_check_rows,
  completed_rows = completed_spot_check_rows,
  pass = completed_spot_check_rows >= 30 & group_pass,
  note = human_qa_note
)

write_csv(human_qa_status, out_human_qa_status_csv, na = "")
cat("Wrote source coverage audit outputs to", dirname(out_audit_csv), "\n")
