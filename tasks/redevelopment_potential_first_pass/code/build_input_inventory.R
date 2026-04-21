# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/redevelopment_potential_first_pass/code")
# treatment_csv <- "../input/cd_homeownership_1990_measure.csv"
# baseline_controls_csv <- "../input/cd_baseline_1990_controls.csv"
# dcp_supply_panel_csv <- "../input/cd_homeownership_dcp_supply_panel.csv"
# long_units_series_csv <- "../input/cd_homeownership_long_units_series.csv"
# mappluto_archive_parquet <- "../input/dcp_mappluto_archive_18v1_1.parquet"
# mappluto_current_parquet <- "../input/dcp_mappluto_current_25v4.parquet"
# mappluto_lot_qc_csv <- "../input/mappluto_lot_qc.csv"
# hdb_project_parquet <- "../input/dcp_housing_database_project_level_25q4.parquet"
# dob_nb_panel_csv <- "../input/cd_homeownership_permit_nb_panel.csv"
# cd_boundary_parquet <- "../input/dcp_boundary_community_districts_20260412.parquet"
# out_csv <- "../output/input_inventory.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(readr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 11) {
  stop("Expected 11 arguments: treatment_csv baseline_controls_csv dcp_supply_panel_csv long_units_series_csv mappluto_archive_parquet mappluto_current_parquet mappluto_lot_qc_csv hdb_project_parquet dob_nb_panel_csv cd_boundary_parquet out_csv")
}

treatment_csv <- args[1]
baseline_controls_csv <- args[2]
dcp_supply_panel_csv <- args[3]
long_units_series_csv <- args[4]
mappluto_archive_parquet <- args[5]
mappluto_current_parquet <- args[6]
mappluto_lot_qc_csv <- args[7]
hdb_project_parquet <- args[8]
dob_nb_panel_csv <- args[9]
cd_boundary_parquet <- args[10]
out_csv <- args[11]

inventory_df <- tribble(
  ~object, ~file_path, ~source_type, ~time_period, ~rows, ~key_fields, ~notes,
  "canonical_treatment", treatment_csv, "csv", "1990 CD treatment", nrow(read_csv(treatment_csv, show_col_types = FALSE, na = c("", "NA"))), "borocd; borough_name; h_cd_1990; h_b_1990; treat_pp; treat_z_boro", "Exact 1990 CD-level homeownership exposure. Canonical treatment file.",
  "baseline_controls", baseline_controls_csv, "csv", "1990 exact controls + 1980-1990 approximate pretrends", nrow(read_csv(baseline_controls_csv, show_col_types = FALSE, na = c("", "NA"))), "borocd; vacancy_rate_1990_exact; structure shares; income; poverty; housing value; NHGIS pretrend controls", "Exact 1990 DCP profile controls plus approximate NHGIS 1980-1990 overlay controls.",
  "dcp_supply_panel", dcp_supply_panel_csv, "csv", "2000-2025 permit-year CD outcomes", nrow(read_csv(dcp_supply_panel_csv, show_col_types = FALSE, na = c("", "NA"))), "borocd; year; outcome_family; outcome_value; borough_outcome_total; borough_outcome_share", "Decomposed DCP Housing Database supply panel. Permit-year based.",
  "preferred_long_units_series", long_units_series_csv, "csv", "1980-2025 stitched long series", nrow(read_csv(long_units_series_csv, show_col_types = FALSE, na = c("", "NA"))), "borocd; year; series_kind; series_family; outcome_value", "Preferred long series uses 1980-2009 MapPLUTO yearbuilt proxy plus 2010-2025 DCP HDB completion-year observed new-building units.",
  "mappluto_main_baseline", mappluto_archive_parquet, "parquet", "2018 archive release (18v1.1)", nrow(read_parquet(mappluto_archive_parquet, col_select = "bbl")), "bbl; cd; council; lotarea; builtfar; residfar; landuse; bldgclass", "Earliest available archived MapPLUTO release in repo. Post-treatment descriptive stock file, not a pre-1990 baseline.",
  "mappluto_sensitivity_current", mappluto_current_parquet, "parquet", "current 25v4 release", nrow(read_parquet(mappluto_current_parquet, col_select = "bbl")), "bbl; cd; council; lotarea; builtfar; residfar; landuse; bldgclass", "Current MapPLUTO release used only for sensitivity comparisons.",
  "mappluto_field_qc", mappluto_lot_qc_csv, "csv", "archive and current release QC", nrow(read_csv(mappluto_lot_qc_csv, show_col_types = FALSE, na = c("", "NA"))), "source_id; vintage; row_count; nonmissing shares", "Documents field coverage for archived and current MapPLUTO releases.",
  "hdb_project_level", hdb_project_parquet, "parquet", "1998-2026 permit years / 2010-2026 completion years", nrow(read_parquet(hdb_project_parquet, col_select = "job_number")), "job_number; job_type; permit_year; completion_year; classa_prop; classa_net; community_district; council_district", "Project-level DCP Housing Database file used only where project-level construction detail is needed.",
  "dob_nb_placebo_panel", dob_nb_panel_csv, "csv", "1989-2025 DOB new-building jobs", nrow(read_csv(dob_nb_panel_csv, show_col_types = FALSE, na = c("", "NA"))), "borocd; year; outcome_value; borough_outcome_share", "DOB new-building job panel used only as a comparison/placebo outcome.",
  "cd_boundary_geometry", cd_boundary_parquet, "parquet", "current CD boundaries", nrow(read_parquet(cd_boundary_parquet, col_select = "district_id")), "district_id; geometry_wkt", "Current DCP community-district geometry for maps."
)

write_csv(inventory_df, out_csv, na = "")

cat("Wrote input inventory to", out_csv, "\n")
