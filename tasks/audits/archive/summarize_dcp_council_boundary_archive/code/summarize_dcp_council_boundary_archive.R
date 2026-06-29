# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_dcp_council_boundary_archive/code")
# dcp_council_boundary_archive_files_csv <- "../input/dcp_council_boundary_archive_files.csv"
# dcp_council_boundary_archive_index_csv <- "../input/dcp_council_boundary_archive_index.csv"
# dcp_council_boundary_archive_qc_csv <- "../input/dcp_council_boundary_archive_qc.csv"
# dcp_boundary_qc_csv <- "../input/dcp_boundary_qc.csv"
# out_inventory_csv <- "../output/dcp_council_boundary_archive_inventory.csv"
# out_signature_csv <- "../output/dcp_council_boundary_archive_signature_groups.csv"
# out_canonical_csv <- "../output/dcp_council_boundary_archive_canonical_regimes.csv"
# out_summary_csv <- "../output/dcp_council_boundary_archive_audit_summary.csv"
# out_gate_csv <- "../output/dcp_council_boundary_archive_feasibility_gate.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 9) {
  stop("Expected 9 arguments: dcp_council_boundary_archive_files_csv dcp_council_boundary_archive_index_csv dcp_council_boundary_archive_qc_csv dcp_boundary_qc_csv out_inventory_csv out_signature_csv out_canonical_csv out_summary_csv out_gate_csv")
}

dcp_council_boundary_archive_files_csv <- args[1]
dcp_council_boundary_archive_index_csv <- args[2]
dcp_council_boundary_archive_qc_csv <- args[3]
dcp_boundary_qc_csv <- args[4]
out_inventory_csv <- args[5]
out_signature_csv <- args[6]
out_canonical_csv <- args[7]
out_summary_csv <- args[8]
out_gate_csv <- args[9]

archive_files <- read_csv(dcp_council_boundary_archive_files_csv, show_col_types = FALSE, na = c("", "NA"))
archive_index <- read_csv(dcp_council_boundary_archive_index_csv, show_col_types = FALSE, na = c("", "NA"))
archive_qc <- read_csv(dcp_council_boundary_archive_qc_csv, show_col_types = FALSE, na = c("", "NA"))
current_boundary_qc <- read_csv(dcp_boundary_qc_csv, show_col_types = FALSE, na = c("", "NA"))

archive_inventory <- archive_index |>
  left_join(
    archive_qc,
    by = c("source_id", "archive_year", "release", "raw_path"),
    relationship = "one-to-one"
  ) |>
  mutate(
    archive_year = suppressWarnings(as.integer(archive_year)),
    release = str_to_upper(as.character(release)),
    geometry_usable = district_count == 51 &
      duplicated_district_id_count == 0 &
      missing_district_id_count == 0 &
      invalid_geometry_count == 0,
    inferred_effective_regime = case_when(
      archive_year <= 2012 ~ "2003-2012 regime",
      archive_year < 2022 ~ "2013-2022 regime",
      archive_year == 2022 & release != "22C1" ~ "2013-2022 regime",
      archive_year >= 2023 | release == "22C1" ~ "2023+ regime",
      TRUE ~ NA_character_
    )
  ) |>
  arrange(archive_year, release)

signature_rows <- list()

for (i in seq_len(nrow(archive_inventory))) {
  row <- archive_inventory[i, ]
  staged_df <- read_parquet(row$parquet_path) |>
    as.data.frame() |>
    as_tibble() |>
    mutate(district_id = as.character(district_id)) |>
    arrange(district_id) |>
    select(district_id, geometry_wkb_hex)

  signature_path <- tempfile(fileext = ".csv")
  write_csv(staged_df, signature_path, na = "")

  signature_rows[[i]] <- tibble(
    raw_path = row$raw_path,
    release = row$release,
    geometry_signature_sha256 = compute_sha256(signature_path)
  )
}

signature_df <- bind_rows(signature_rows)

archive_inventory <- archive_inventory |>
  left_join(signature_df, by = c("raw_path", "release")) |>
  mutate(signature_group = dense_rank(geometry_signature_sha256))

signature_groups <- archive_inventory |>
  group_by(signature_group, geometry_signature_sha256) |>
  summarise(
    first_archive_year = min(archive_year, na.rm = TRUE),
    last_archive_year = max(archive_year, na.rm = TRUE),
    release_count = n(),
    releases = paste(release, collapse = ";"),
    inferred_regimes = paste(sort(unique(inferred_effective_regime)), collapse = ";"),
    .groups = "drop"
  ) |>
  arrange(first_archive_year, signature_group)

expected_regimes <- tibble(
  expected_regime = c(
    "pre-1991 council regime",
    "1991/1992-2002 regime",
    "2003-2012 regime",
    "2013-2022 regime",
    "2023+ regime"
  ),
  regime_order = 1:5
)

observed_regimes <- archive_inventory |>
  filter(!is.na(inferred_effective_regime)) |>
  group_by(inferred_effective_regime) |>
  arrange(archive_year, release, .by_group = TRUE) |>
  summarise(
    available_flag = any(geometry_usable, na.rm = TRUE),
    canonical_release = first(release[geometry_usable]),
    canonical_archive_year = first(archive_year[geometry_usable]),
    canonical_parquet_path = first(parquet_path[geometry_usable]),
    observed_release_count = n(),
    distinct_signature_count = n_distinct(geometry_signature_sha256),
    .groups = "drop"
  ) |>
  rename(expected_regime = inferred_effective_regime)

canonical_regimes <- expected_regimes |>
  left_join(observed_regimes, by = "expected_regime") |>
  mutate(
    available_flag = coalesce(available_flag, FALSE),
    note = case_when(
      expected_regime == "pre-1991 council regime" ~ "No official pre-1991 council boundary appears in the accessible DCP BYTES archive JSON.",
      expected_regime == "1991/1992-2002 regime" ~ "No official 1990s or early-2000s council boundary appears in the accessible DCP BYTES archive JSON.",
      available_flag ~ "Canonical release chosen as the earliest usable archive release observed for this regime.",
      TRUE ~ "Regime not observed in the accessible archive."
    )
  ) |>
  arrange(regime_order)

current_council_qc <- current_boundary_qc |>
  filter(source_id == "dcp_boundary_city_council_districts") |>
  arrange(desc(pull_date)) |>
  slice_head(n = 1)

current_council_stage_pass <- nrow(current_council_qc) == 1 &&
  current_council_qc$district_count[[1]] == 51 &&
  current_council_qc$missing_district_id_count[[1]] == 0 &&
  current_council_qc$invalid_geometry_count[[1]] == 0

pre_1991_available <- canonical_regimes |>
  filter(expected_regime == "pre-1991 council regime") |>
  pull(available_flag)

pre_1991_available <- if (length(pre_1991_available) == 0) FALSE else isTRUE(pre_1991_available[[1]])

gate_df <- tibble(
  current_council_stage_pass = current_council_stage_pass,
  archive_inventory_nonempty = nrow(archive_inventory) > 0,
  earliest_archive_year = min(archive_inventory$archive_year, na.rm = TRUE),
  latest_archive_year = max(archive_inventory$archive_year, na.rm = TRUE),
  pre_1991_official_available = pre_1991_available,
  phase_1_static_council_treatment_allowed = current_council_stage_pass && nrow(archive_inventory) > 0 && pre_1991_available,
  stop_reason = ifelse(
    current_council_stage_pass && nrow(archive_inventory) > 0 && !pre_1991_available,
    "Official DCP council-boundary archive JSON starts in 2006, so there is no accessible official pre-1991 council regime for a 1990 replacement treatment.",
    NA_character_
  )
)

summary_df <- bind_rows(
  tibble(metric = "current_council_district_count", value = if (nrow(current_council_qc) == 1) as.character(current_council_qc$district_count[[1]]) else NA_character_, note = "Current official council boundary layer staged through the existing DCP boundary pipeline."),
  tibble(metric = "current_council_stage_pass", value = as.character(current_council_stage_pass), note = "One means the current official council layer stages cleanly with 51 districts."),
  tibble(metric = "archive_json_release_count", value = as.character(nrow(archive_inventory)), note = "Number of shoreline-clipped archived council releases listed in the official DCP BYTES archive JSON."),
  tibble(metric = "archive_year_start", value = as.character(min(archive_inventory$archive_year, na.rm = TRUE)), note = "Earliest archive year listed in the accessible official DCP council archive."),
  tibble(metric = "archive_year_end", value = as.character(max(archive_inventory$archive_year, na.rm = TRUE)), note = "Latest archive year listed in the accessible official DCP council archive."),
  tibble(metric = "usable_release_count", value = as.character(sum(archive_inventory$geometry_usable, na.rm = TRUE)), note = "Count of archived releases with 51 districts, no missing IDs, no duplicated IDs, and no invalid geometries."),
  tibble(metric = "signature_group_count", value = as.character(n_distinct(archive_inventory$geometry_signature_sha256)), note = "Distinct geometry signatures observed across the accessible archive releases."),
  tibble(metric = "observed_regime_count", value = as.character(sum(canonical_regimes$available_flag, na.rm = TRUE)), note = "Number of expected council regimes observed in the accessible archive."),
  tibble(metric = "pre_1991_official_available", value = as.character(pre_1991_available), note = "One means an official pre-1991 council boundary is available in the accessible DCP archive."),
  tibble(metric = "phase_1_static_council_treatment_allowed", value = as.character(gate_df$phase_1_static_council_treatment_allowed[[1]]), note = "Hard gate for whether a 1990 replacement council treatment can be built without approximation.")
)

write_csv(archive_inventory, out_inventory_csv, na = "")
write_csv(signature_groups, out_signature_csv, na = "")
write_csv(canonical_regimes, out_canonical_csv, na = "")
write_csv(summary_df, out_summary_csv, na = "")
write_csv(gate_df, out_gate_csv, na = "")

cat("Wrote DCP council boundary archive summaries to", dirname(out_inventory_csv), "\n")
