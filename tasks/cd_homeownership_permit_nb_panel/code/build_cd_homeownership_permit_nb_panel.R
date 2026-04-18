# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/cd_homeownership_permit_nb_panel/code")
# cd_homeownership_1990_measure_csv <- "../input/cd_homeownership_1990_measure.csv"
# dob_permit_issuance_harmonized_parquet <- "../input/dob_permit_issuance_harmonized.parquet"
# out_panel_csv <- "../output/cd_homeownership_permit_nb_panel.csv"
# out_qc_csv <- "../output/cd_homeownership_permit_nb_panel_qc.csv"
# out_conflict_csv <- "../output/cd_homeownership_permit_nb_job_conflicts.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 5) {
  stop("Expected 5 arguments: cd_homeownership_1990_measure_csv dob_permit_issuance_harmonized_parquet out_panel_csv out_qc_csv out_conflict_csv")
}

cd_homeownership_1990_measure_csv <- args[1]
dob_permit_issuance_harmonized_parquet <- args[2]
out_panel_csv <- args[3]
out_qc_csv <- args[4]
out_conflict_csv <- args[5]

measure_df <- read_csv(cd_homeownership_1990_measure_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
    borocd = as.integer(borocd),
    borough_code = as.character(borough_code),
    borough_name = standardize_borough_name(borough_code)
  ) %>%
  arrange(borocd)

permit_df <- read_parquet(
  dob_permit_issuance_harmonized_parquet,
  col_select = c("permit_identifier", "job_number", "job_type", "issuance_date", "community_district")
) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(
    borocd = as.integer(community_district),
    standard_cd_flag = borocd %in% measure_df$borocd
  ) %>%
  filter(job_type == "New Building", !is.na(job_number))

job_cd_df <- permit_df %>%
  filter(standard_cd_flag) %>%
  group_by(job_number) %>%
  summarise(
    standard_cd_count = n_distinct(borocd),
    assigned_borocd = min(borocd),
    standard_cd_list = str_c(sort(unique(borocd)), collapse = ";"),
    standard_row_count = n(),
    .groups = "drop"
  )

conflict_jobs <- job_cd_df %>%
  filter(standard_cd_count > 1)

conflict_df <- conflict_jobs %>%
  select(job_number, standard_cd_count, standard_cd_list) %>%
  left_join(permit_df, by = "job_number") %>%
  group_by(job_number) %>%
  summarise(
    standard_cd_count = first(standard_cd_count),
    standard_cd_list = first(standard_cd_list),
    total_row_count = n(),
    nonmissing_issuance_date_row_count = sum(!is.na(issuance_date)),
    earliest_issuance_date = ifelse(all(is.na(issuance_date)), NA_character_, as.character(min(issuance_date, na.rm = TRUE))),
    latest_issuance_date = ifelse(all(is.na(issuance_date)), NA_character_, as.character(max(issuance_date, na.rm = TRUE))),
    .groups = "drop"
  ) %>%
  arrange(job_number)

first_issuance_df <- permit_df %>%
  arrange(job_number, is.na(issuance_date), issuance_date, permit_identifier) %>%
  group_by(job_number) %>%
  slice_head(n = 1) %>%
  ungroup() %>%
  transmute(
    job_number,
    first_issuance_permit_identifier = permit_identifier,
    first_issuance_date = issuance_date,
    first_issuance_year = as.integer(format(issuance_date, "%Y"))
  )

assigned_jobs_df <- job_cd_df %>%
  filter(standard_cd_count == 1) %>%
  select(job_number, assigned_borocd, standard_row_count) %>%
  left_join(first_issuance_df, by = "job_number")

job_counts_df <- assigned_jobs_df %>%
  filter(!is.na(first_issuance_year), first_issuance_year >= 1989, first_issuance_year <= 2025) %>%
  count(assigned_borocd, year = first_issuance_year, name = "outcome_value")

panel_df <- crossing(
  measure_df %>%
    select(
      source_id,
      pull_date,
      district_id,
      borocd,
      borough_code,
      borough_name,
      owner_occupied_units_1990,
      occupied_units_1990,
      borough_owner_occupied_units_1990,
      borough_occupied_units_1990,
      h_cd_1990,
      h_cd_1990_pct,
      h_b_1990,
      h_b_1990_pct,
      cd_minus_borough_1990,
      treat_pp,
      treat_z_boro,
      owner_occupied_share_reported_1990_pct,
      owner_share_reported_gap_pp
    ),
  year = 1989:2025
) %>%
  left_join(job_counts_df, by = c("borocd" = "assigned_borocd", "year")) %>%
  mutate(
    outcome_source_id = "dob_permit_issuance_harmonized",
    outcome_family = "permit_nb_jobs",
    outcome_value = coalesce(outcome_value, 0L)
  ) %>%
  group_by(borough_code, borough_name, year) %>%
  mutate(
    borough_outcome_total = sum(outcome_value, na.rm = TRUE),
    borough_outcome_share = ifelse(borough_outcome_total > 0, outcome_value / borough_outcome_total, NA_real_)
  ) %>%
  ungroup() %>%
  arrange(year, borocd)

qc_df <- bind_rows(
  tibble(
    metric = "panel_row_count",
    value = nrow(panel_df),
    note = "Rows in the balanced 59-CD by year panel for DOB new-building first-issuance jobs, 1989-2025."
  ),
  tibble(
    metric = "district_count",
    value = n_distinct(panel_df$borocd),
    note = "Standard community districts represented in the panel."
  ),
  tibble(
    metric = "year_count",
    value = n_distinct(panel_df$year),
    note = "Calendar years represented in the panel."
  ),
  tibble(
    metric = "nb_row_count_total",
    value = nrow(permit_df),
    note = "Total new-building permit issuance rows with nonmissing job numbers before standard-CD restrictions."
  ),
  tibble(
    metric = "nb_row_share_standard_59_cd",
    value = mean(permit_df$standard_cd_flag, na.rm = TRUE),
    note = "Share of new-building permit rows that carry one of the standard 59 community district codes."
  ),
  tibble(
    metric = "unique_nb_jobs_total",
    value = n_distinct(permit_df$job_number),
    note = "Unique new-building job numbers before standard-CD restrictions."
  ),
  tibble(
    metric = "unique_nb_jobs_with_one_standard_59_cd",
    value = nrow(job_cd_df %>% filter(standard_cd_count == 1)),
    note = "Unique new-building jobs with exactly one standard 59 community district across permit rows."
  ),
  tibble(
    metric = "unique_nb_jobs_missing_first_issuance_date_share",
    value = mean(is.na(first_issuance_df$first_issuance_date)),
    note = "Share of unique new-building jobs whose earliest observed permit row still has a missing issuance date."
  ),
  tibble(
    metric = "multi_standard_cd_job_conflict_count",
    value = nrow(conflict_jobs),
    note = "Unique new-building jobs dropped because permit rows span more than one standard community district."
  ),
  tibble(
    metric = "assigned_jobs_in_panel_year_range",
    value = nrow(assigned_jobs_df %>% filter(!is.na(first_issuance_year), first_issuance_year >= 1989, first_issuance_year <= 2025)),
    note = "Unique new-building jobs assigned to a single standard community district and placed in the 1989-2025 panel."
  ),
  tibble(
    metric = "dropped_partial_2026_jobs",
    value = nrow(assigned_jobs_df %>% filter(!is.na(first_issuance_year), first_issuance_year > 2025)),
    note = "Assigned new-building jobs excluded because their first issuance date falls after 2025."
  ),
  tibble(
    metric = "borough_year_zero_denominator_count",
    value = panel_df %>%
      distinct(borough_name, year, borough_outcome_total) %>%
      summarise(value = sum(borough_outcome_total <= 0), .groups = "drop") %>%
      pull(value),
    note = "Borough-year cells where the within-borough share denominator is nonpositive."
  ),
  tibble(
    metric = "status",
    value = ifelse(
      nrow(panel_df) == 59 * length(1989:2025) &&
        all(!is.na(panel_df$treat_pp)) &&
        all(!is.na(panel_df$treat_z_boro)),
      1,
      0
    ),
    note = "One means the DOB new-building panel is fully populated over the planned 59-CD by 1989-2025 support."
  )
)

write_csv_if_changed(panel_df, out_panel_csv)
write_csv_if_changed(qc_df, out_qc_csv)
write_csv_if_changed(conflict_df, out_conflict_csv)

cat("Wrote CD-year DOB new-building panel outputs to", dirname(out_panel_csv), "\n")
