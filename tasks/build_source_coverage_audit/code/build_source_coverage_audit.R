# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_source_coverage_audit/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# mappluto_qc_csv <- "../input/mappluto_lot_qc.csv"
# dob_qc_csv <- "../input/dob_open_data_qc.csv"
# nhgis_qc_csv <- "../input/nhgis_qc.csv"
# dcp_boundary_files_csv <- "../input/dcp_boundary_files.csv"
# census_bps_files_csv <- "../input/census_bps_files.csv"
# furman_files_csv <- "../input/furman_coredata_files.csv"
# archival_inventory_csv <- "../input/archival_record_inventory.csv"
# crosswalk_qc_csv <- "../input/lot_identifier_crosswalk_qc.csv"
# out_audit_csv <- "../output/source_coverage_audit.csv"
# out_spot_check_csv <- "../output/human_qa_spot_check.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 11) {
  stop("Expected 11 arguments for the coverage audit task.")
}

source_catalog_csv <- args[1]
mappluto_qc_csv <- args[2]
dob_qc_csv <- args[3]
nhgis_qc_csv <- args[4]
dcp_boundary_files_csv <- args[5]
census_bps_files_csv <- args[6]
furman_files_csv <- args[7]
archival_inventory_csv <- args[8]
crosswalk_qc_csv <- args[9]
out_audit_csv <- args[10]
out_spot_check_csv <- args[11]

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
mappluto_qc <- read_csv(mappluto_qc_csv, show_col_types = FALSE, na = c("", "NA"))
dob_qc <- read_csv(dob_qc_csv, show_col_types = FALSE, na = c("", "NA"))
nhgis_qc <- read_csv(nhgis_qc_csv, show_col_types = FALSE, na = c("", "NA"))
dcp_boundary_files <- read_csv(dcp_boundary_files_csv, show_col_types = FALSE, na = c("", "NA"))
census_bps_files <- read_csv(census_bps_files_csv, show_col_types = FALSE, na = c("", "NA"))
furman_files <- read_csv(furman_files_csv, show_col_types = FALSE, na = c("", "NA"))
archival_inventory <- read_csv(archival_inventory_csv, show_col_types = FALSE, na = c("", "NA"))
crosswalk_qc <- read_csv(crosswalk_qc_csv, show_col_types = FALSE, na = c("", "NA"))

source_coverage <- source_catalog |>
  transmute(
    source_id,
    source_name,
    access_mode,
    earliest_date = start_date,
    latest_date = end_date,
    row_count = NA_real_,
    share_with_bbl = NA_real_,
    share_with_bin = NA_real_,
    share_with_usable_address = NA_real_,
    share_mapped_current_cd = NA_real_,
    known_coverage_gaps = notes
  )

if (nrow(dob_qc) > 0) {
  source_coverage <- source_coverage |>
    left_join(
      dob_qc |>
        transmute(
          source_id,
          row_count = row_count,
          earliest_date = start_date,
          latest_date = end_date,
          share_with_bbl = nonmissing_bbl_share,
          share_with_bin = nonmissing_bin_share,
          share_with_usable_address = nonmissing_address_share,
          share_mapped_current_cd = nonmissing_cd_share
        ),
      by = "source_id",
      suffix = c("", "_dob")
    ) |>
    mutate(
      row_count = coalesce(row_count_dob, row_count),
      earliest_date = coalesce(earliest_date_dob, earliest_date),
      latest_date = coalesce(latest_date_dob, latest_date),
      share_with_bbl = coalesce(share_with_bbl_dob, share_with_bbl),
      share_with_bin = coalesce(share_with_bin_dob, share_with_bin),
      share_with_usable_address = coalesce(share_with_usable_address_dob, share_with_usable_address),
      share_mapped_current_cd = coalesce(share_mapped_current_cd_dob, share_mapped_current_cd)
    ) |>
    select(-ends_with("_dob"))
}

if (nrow(mappluto_qc) > 0) {
  source_coverage <- source_coverage |>
    left_join(
      mappluto_qc |>
        transmute(
          source_id,
          row_count = row_count,
          earliest_date = as.character(min_yearbuilt),
          latest_date = as.character(max_yearbuilt),
          share_with_bbl = nonmissing_bbl_share,
          share_mapped_current_cd = ordinary_cd_rows / row_count
        ),
      by = "source_id",
      suffix = c("", "_mappluto")
    ) |>
    mutate(
      row_count = coalesce(row_count_mappluto, row_count),
      earliest_date = coalesce(earliest_date_mappluto, earliest_date),
      latest_date = coalesce(latest_date_mappluto, latest_date),
      share_with_bbl = coalesce(share_with_bbl_mappluto, share_with_bbl),
      share_mapped_current_cd = coalesce(share_mapped_current_cd_mappluto, share_mapped_current_cd)
    ) |>
    select(-ends_with("_mappluto"))
}

if (nrow(nhgis_qc) > 0) {
  source_coverage <- source_coverage |>
    left_join(
      nhgis_qc |> select(source_id, row_count),
      by = "source_id",
      suffix = c("", "_nhgis")
    ) |>
    mutate(row_count = coalesce(row_count_nhgis, row_count)) |>
    select(-row_count_nhgis)
}

inventory_sources <- bind_rows(
  dcp_boundary_files |> transmute(source_id, status),
  census_bps_files |> transmute(source_id, status),
  furman_files |> transmute(source_id, status),
  archival_inventory |> count(source_id, name = "inventory_rows") |> transmute(source_id, status = paste0("inventory_rows=", inventory_rows))
)

if (nrow(inventory_sources) > 0) {
  source_coverage <- source_coverage |>
    left_join(inventory_sources, by = "source_id") |>
    mutate(
      known_coverage_gaps = if_else(
        !is.na(status),
        paste(known_coverage_gaps, "[status:", status, "]"),
        known_coverage_gaps
      )
    ) |>
    select(-status)
}

write_csv(source_coverage, out_audit_csv, na = "")

spot_check <- tibble(
  check_id = sprintf("qa_%02d", 1:10),
  source_id = NA_character_,
  raw_identifier = NA_character_,
  bbl = NA_character_,
  bin = NA_character_,
  address = NA_character_,
  checked_by = NA_character_,
  checked_date = NA_character_,
  result = NA_character_,
  notes = "Verify this row across PLUTO, DOB, and any returned archival source before any panel build."
)

if (nrow(crosswalk_qc) > 0) {
  spot_check$notes[1] <- paste(
    "Crosswalk summary:",
    paste(names(crosswalk_qc), unlist(crosswalk_qc[1, ]), sep = "=", collapse = "; ")
  )
}

write_csv(spot_check, out_spot_check_csv, na = "")
cat("Wrote source coverage audit outputs to", dirname(out_audit_csv), "\n")
