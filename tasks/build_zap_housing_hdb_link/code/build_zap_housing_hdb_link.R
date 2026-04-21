# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_zap_housing_hdb_link/code")
# zap_housing_cohort_base_csv <- "../input/zap_housing_cohort_base.csv"
# zap_project_bbl_parquet <- "../input/zap_project_bbl.parquet"
# dcp_housing_database_project_level_parquet <- "../input/dcp_housing_database_project_level_25q4.parquet"
# out_candidates_csv <- "../output/zap_housing_hdb_link_candidates.csv"
# out_project_summary_csv <- "../output/zap_housing_hdb_project_summary.csv"
# out_qc_csv <- "../output/zap_housing_hdb_link_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 6) {
  stop("Expected 6 arguments: zap_housing_cohort_base_csv zap_project_bbl_parquet dcp_housing_database_project_level_parquet out_candidates_csv out_project_summary_csv out_qc_csv")
}

zap_housing_cohort_base_csv <- args[1]
zap_project_bbl_parquet <- args[2]
dcp_housing_database_project_level_parquet <- args[3]
out_candidates_csv <- args[4]
out_project_summary_csv <- args[5]
out_qc_csv <- args[6]

project_base <- read_csv(zap_housing_cohort_base_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    project_id = as.character(project_id),
    borocd = as.integer(borocd),
    cert_year = as.integer(cert_year),
    has_bbl = as.logical(has_bbl),
    bbl_count = as.integer(bbl_count)
  )

zap_bbl <- read_parquet(zap_project_bbl_parquet, col_select = c("project_id", "bbl_standardized")) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(
    project_id = as.character(project_id),
    bbl_standardized = as.character(bbl_standardized)
  ) %>%
  filter(!is.na(project_id), !is.na(bbl_standardized)) %>%
  distinct(project_id, bbl_standardized)

hdb_jobs <- read_parquet(
  dcp_housing_database_project_level_parquet,
  col_select = c("job_number", "job_type", "permit_year", "completion_year", "classa_prop", "classa_net", "borough_name", "community_district", "bbl")
) %>%
  as.data.frame() %>%
  as_tibble() %>%
  mutate(
    job_number = as.character(job_number),
    job_type = str_squish(as.character(job_type)),
    permit_year = suppressWarnings(as.integer(permit_year)),
    completion_year = suppressWarnings(as.integer(completion_year)),
    classa_prop = suppressWarnings(as.numeric(classa_prop)),
    classa_net = suppressWarnings(as.numeric(classa_net)),
    borough_name_hdb = as.character(borough_name),
    hdb_borocd = suppressWarnings(as.integer(community_district)),
    bbl_standardized = as.character(bbl),
    is_nb_job = job_type == "New Building" & coalesce(classa_prop, 0) > 0,
    is_addition_job = is_nb_job | (job_type == "Alteration" & coalesce(classa_net, 0) > 0),
    is_housing_active_job = is_nb_job | coalesce(classa_net, 0) != 0,
    is_nb_50_plus_job = is_nb_job & coalesce(classa_prop, 0) >= 50,
    nb_gross_units = ifelse(is_nb_job, coalesce(classa_prop, 0), 0),
    gross_add_units = case_when(
      is_nb_job ~ coalesce(classa_prop, 0),
      job_type == "Alteration" ~ pmax(coalesce(classa_net, 0), 0),
      TRUE ~ 0
    ),
    gross_loss_units = case_when(
      job_type %in% c("Alteration", "Demolition") ~ pmax(-coalesce(classa_net, 0), 0),
      TRUE ~ 0
    ),
    net_units = coalesce(classa_net, 0)
  ) %>%
  select(-borough_name, -community_district, -bbl) %>%
  filter(!is.na(job_number), !is.na(bbl_standardized))

candidate_links <- project_base %>%
  select(
    project_id,
    project_name,
    project_brief,
    borocd,
    borough_name,
    cert_year,
    cert_era,
    treat_pp,
    treat_z_boro,
    is_complete,
    is_fail,
    is_unresolved,
    has_bbl,
    bbl_count
  ) %>%
  left_join(zap_bbl, by = "project_id") %>%
  left_join(hdb_jobs, by = "bbl_standardized", relationship = "many-to-many") %>%
  mutate(
    permit_lag = ifelse(!is.na(permit_year), permit_year - cert_year, NA_integer_),
    completion_lag = ifelse(!is.na(completion_year), completion_year - cert_year, NA_integer_),
    within_0_5 = !is.na(permit_lag) & permit_lag >= 0 & permit_lag <= 5,
    within_0_10 = !is.na(permit_lag) & permit_lag >= 0 & permit_lag <= 10,
    within_neg2_10 = !is.na(permit_lag) & permit_lag >= -2 & permit_lag <= 10,
    within_neg5_15 = !is.na(permit_lag) & permit_lag >= -5 & permit_lag <= 15
  ) %>%
  arrange(project_id, bbl_standardized, permit_year, job_number)

project_summary <- candidate_links %>%
  group_by(project_id) %>%
  summarise(
    project_name = first(.data$project_name),
    project_brief = first(.data$project_brief),
    borocd = first(.data$borocd),
    borough_name = first(.data$borough_name),
    cert_year = first(.data$cert_year),
    cert_era = first(.data$cert_era),
    treat_pp = first(.data$treat_pp),
    treat_z_boro = first(.data$treat_z_boro),
    is_complete = first(.data$is_complete),
    is_fail = first(.data$is_fail),
    is_unresolved = first(.data$is_unresolved),
    has_bbl = first(.data$has_bbl),
    bbl_count = first(.data$bbl_count),
    matched_bbl_count = n_distinct(bbl_standardized[!is.na(job_number)]),
    has_any_hdb_match_exact_bbl = any(!is.na(job_number)),
    has_any_housing_job_exact_bbl = any(is_housing_active_job %in% TRUE, na.rm = TRUE),
    has_any_housing_job_0_5 = any(is_housing_active_job %in% TRUE & within_0_5, na.rm = TRUE),
    has_any_housing_job_0_10 = any(is_housing_active_job %in% TRUE & within_0_10, na.rm = TRUE),
    has_any_housing_job_neg2_10 = any(is_housing_active_job %in% TRUE & within_neg2_10, na.rm = TRUE),
    has_any_housing_job_neg5_15 = any(is_housing_active_job %in% TRUE & within_neg5_15, na.rm = TRUE),
    has_any_addition_job_0_10 = any(is_addition_job %in% TRUE & within_0_10, na.rm = TRUE),
    has_any_nb_job_0_10 = any(is_nb_job %in% TRUE & within_0_10, na.rm = TRUE),
    has_any_nb_50_plus_job_0_10 = any(is_nb_50_plus_job %in% TRUE & within_0_10, na.rm = TRUE),
    linked_housing_job_count_0_10 = n_distinct(job_number[is_housing_active_job %in% TRUE & within_0_10]),
    linked_addition_job_count_0_10 = n_distinct(job_number[is_addition_job %in% TRUE & within_0_10]),
    linked_nb_job_count_0_10 = n_distinct(job_number[is_nb_job %in% TRUE & within_0_10]),
    linked_nb_gross_units_0_10 = sum(nb_gross_units[within_0_10], na.rm = TRUE),
    linked_gross_add_units_0_10 = sum(gross_add_units[within_0_10], na.rm = TRUE),
    linked_gross_loss_units_0_10 = sum(gross_loss_units[within_0_10], na.rm = TRUE),
    linked_net_units_0_10 = sum(net_units[within_0_10], na.rm = TRUE),
    first_housing_permit_year_0_10 = suppressWarnings(min(permit_year[is_housing_active_job %in% TRUE & within_0_10], na.rm = TRUE)),
    first_housing_permit_lag_0_10 = suppressWarnings(min(permit_lag[is_housing_active_job %in% TRUE & within_0_10], na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  mutate(
    matched_bbl_count = coalesce(matched_bbl_count, 0L),
    linked_housing_job_count_0_10 = coalesce(linked_housing_job_count_0_10, 0L),
    linked_addition_job_count_0_10 = coalesce(linked_addition_job_count_0_10, 0L),
    linked_nb_job_count_0_10 = coalesce(linked_nb_job_count_0_10, 0L),
    linked_nb_gross_units_0_10 = coalesce(linked_nb_gross_units_0_10, 0),
    linked_gross_add_units_0_10 = coalesce(linked_gross_add_units_0_10, 0),
    linked_gross_loss_units_0_10 = coalesce(linked_gross_loss_units_0_10, 0),
    linked_net_units_0_10 = coalesce(linked_net_units_0_10, 0),
    first_housing_permit_year_0_10 = ifelse(is.infinite(first_housing_permit_year_0_10), NA_integer_, first_housing_permit_year_0_10),
    first_housing_permit_lag_0_10 = ifelse(is.infinite(first_housing_permit_lag_0_10), NA_integer_, first_housing_permit_lag_0_10)
  ) %>%
  arrange(cert_year, borocd, project_id)

qc_df <- bind_rows(
  tibble(
    metric = "project_count",
    value = nrow(project_summary),
    note = "Housing-oriented ULURP projects carried into the exact-BBL housing linkage."
  ),
  tibble(
    metric = "has_bbl_share",
    value = mean(project_summary$has_bbl, na.rm = TRUE),
    note = "Share of ZAP housing projects with at least one linked BBL."
  ),
  tibble(
    metric = "any_hdb_match_exact_bbl_share",
    value = mean(project_summary$has_any_hdb_match_exact_bbl, na.rm = TRUE),
    note = "Share with any exact-BBL match to a DCP housing-database job."
  ),
  tibble(
    metric = "any_housing_job_exact_bbl_share",
    value = mean(project_summary$has_any_housing_job_exact_bbl, na.rm = TRUE),
    note = "Share with any housing-active DCP job on an exact matched BBL, regardless of timing."
  ),
  tibble(
    metric = "any_housing_job_0_5_share",
    value = mean(project_summary$has_any_housing_job_0_5, na.rm = TRUE),
    note = "Share with any housing-active DCP job within 0-5 permit years after certification."
  ),
  tibble(
    metric = "any_housing_job_0_10_share",
    value = mean(project_summary$has_any_housing_job_0_10, na.rm = TRUE),
    note = "Share with any housing-active DCP job within 0-10 permit years after certification."
  ),
  tibble(
    metric = "any_housing_job_neg2_10_share",
    value = mean(project_summary$has_any_housing_job_neg2_10, na.rm = TRUE),
    note = "Share with any housing-active DCP job within -2 to +10 permit years around certification."
  ),
  tibble(
    metric = "any_housing_job_neg5_15_share",
    value = mean(project_summary$has_any_housing_job_neg5_15, na.rm = TRUE),
    note = "Share with any housing-active DCP job within -5 to +15 permit years around certification."
  ),
  tibble(
    metric = "any_addition_job_0_10_share",
    value = mean(project_summary$has_any_addition_job_0_10, na.rm = TRUE),
    note = "Share with any positive housing-addition DCP job within 0-10 permit years after certification."
  ),
  tibble(
    metric = "any_nb_job_0_10_share",
    value = mean(project_summary$has_any_nb_job_0_10, na.rm = TRUE),
    note = "Share with any new-building DCP housing job within 0-10 permit years after certification."
  ),
  tibble(
    metric = "any_nb_50_plus_job_0_10_share",
    value = mean(project_summary$has_any_nb_50_plus_job_0_10, na.rm = TRUE),
    note = "Share with any 50+ unit new-building DCP housing job within 0-10 permit years after certification."
  ),
  tibble(
    metric = "median_first_housing_permit_lag_0_10",
    value = median(project_summary$first_housing_permit_lag_0_10, na.rm = TRUE),
    note = "Median first-link permit lag, among projects with a 0-10 housing-active match."
  ),
  tibble(
    metric = "mean_linked_gross_add_units_0_10",
    value = mean(project_summary$linked_gross_add_units_0_10, na.rm = TRUE),
    note = "Average linked gross addition units per ZAP housing project in the 0-10 window."
  )
)

write_csv_if_changed(candidate_links, out_candidates_csv)
write_csv_if_changed(project_summary, out_project_summary_csv)
write_csv_if_changed(qc_df, out_qc_csv)

cat("Wrote ZAP-HDB linkage outputs to", dirname(out_candidates_csv), "\n")
