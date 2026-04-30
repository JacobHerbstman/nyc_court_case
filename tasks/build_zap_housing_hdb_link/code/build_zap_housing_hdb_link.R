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

assert_unique_keys <- function(df, key_cols, df_name) {
  duplicate_keys <- df %>%
    count(across(all_of(key_cols)), name = "source_row_count") %>%
    filter(source_row_count > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(df_name, " is not unique by ", paste(key_cols, collapse = ", "), ".")
  }
}

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

assert_unique_keys(project_base, "project_id", "ZAP housing cohort base")
assert_unique_keys(zap_bbl, c("project_id", "bbl_standardized"), "ZAP project BBL crosswalk")
assert_unique_keys(hdb_jobs, c("job_number", "bbl_standardized"), "HDB job-BBL input")

project_bbl <- project_base %>%
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
  left_join(zap_bbl, by = "project_id", relationship = "one-to-many") %>%
  arrange(project_id, bbl_standardized)

hdb_bbl_summary <- hdb_jobs %>%
  group_by(bbl_standardized) %>%
  summarise(
    hdb_job_count = n_distinct(job_number),
    hdb_housing_job_count = n_distinct(job_number[is_housing_active_job %in% TRUE]),
    .groups = "drop"
  )

project_bbl_status <- project_bbl %>%
  left_join(hdb_bbl_summary, by = "bbl_standardized", relationship = "many-to-one") %>%
  mutate(
    hdb_job_count = coalesce(hdb_job_count, 0L),
    hdb_housing_job_count = coalesce(hdb_housing_job_count, 0L)
  )

candidate_project_bbl <- project_bbl %>%
  filter(!is.na(bbl_standardized))

candidate_hdb_jobs <- hdb_jobs %>%
  filter(!is.na(permit_year))

candidate_bbls <- sort(intersect(
  unique(candidate_project_bbl$bbl_standardized),
  unique(candidate_hdb_jobs$bbl_standardized)
))

candidate_pair_list <- lapply(candidate_bbls, function(bbl_value) {
  project_rows <- candidate_project_bbl[candidate_project_bbl$bbl_standardized == bbl_value, , drop = FALSE]
  job_rows <- candidate_hdb_jobs[
    candidate_hdb_jobs$bbl_standardized == bbl_value,
    setdiff(names(candidate_hdb_jobs), "bbl_standardized"),
    drop = FALSE
  ]

  if (nrow(project_rows) == 0 || nrow(job_rows) == 0) {
    return(tibble())
  }

  bind_cols(
    as_tibble(project_rows[rep(seq_len(nrow(project_rows)), each = nrow(job_rows)), , drop = FALSE]),
    as_tibble(job_rows[rep(seq_len(nrow(job_rows)), times = nrow(project_rows)), , drop = FALSE])
  )
})

candidate_pairs <- bind_rows(candidate_pair_list) %>%
  mutate(
    permit_lag = ifelse(!is.na(permit_year), permit_year - cert_year, NA_integer_),
    completion_lag = ifelse(!is.na(completion_year), completion_year - cert_year, NA_integer_),
    within_0_5 = !is.na(permit_lag) & permit_lag >= 0 & permit_lag <= 5,
    within_0_10 = !is.na(permit_lag) & permit_lag >= 0 & permit_lag <= 10,
    within_neg2_10 = !is.na(permit_lag) & permit_lag >= -2 & permit_lag <= 10,
    within_neg5_15 = !is.na(permit_lag) & permit_lag >= -5 & permit_lag <= 15
  )

candidate_pairs_timed <- candidate_pairs %>%
  filter(within_neg5_15) %>%
  arrange(project_id, job_number, bbl_standardized, permit_year) %>%
  distinct(project_id, job_number, .keep_all = TRUE)

candidate_job_counts <- candidate_pairs_timed %>%
  count(job_number, name = "candidate_project_count")

candidate_links <- candidate_pairs_timed %>%
  left_join(candidate_job_counts, by = "job_number", relationship = "many-to-one") %>%
  mutate(
    assignment_window_rank = case_when(
      within_0_10 ~ 1L,
      within_neg2_10 ~ 2L,
      TRUE ~ 3L
    ),
    assignment_abs_lag = abs(permit_lag)
  ) %>%
  arrange(
    job_number,
    assignment_window_rank,
    assignment_abs_lag,
    desc(cert_year),
    project_id,
    bbl_standardized
  ) %>%
  group_by(job_number) %>%
  mutate(assigned_candidate_rank = row_number()) %>%
  ungroup() %>%
  filter(assigned_candidate_rank == 1L) %>%
  mutate(
    shared_job_candidate_count = candidate_project_count,
    assignment_rule = "Assign each HDB job to one ZAP project, preferring 0-10 links, closest permit lag, then latest certification year."
  ) %>%
  select(-candidate_project_count) %>%
  arrange(project_id, bbl_standardized, permit_year, job_number)

timed_project_summary <- candidate_links %>%
  group_by(project_id) %>%
  summarise(
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
  )

project_summary <- project_bbl_status %>%
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
    matched_bbl_count = n_distinct(bbl_standardized[hdb_job_count > 0]),
    has_any_hdb_match_exact_bbl = any(hdb_job_count > 0, na.rm = TRUE),
    has_any_housing_job_exact_bbl = any(hdb_housing_job_count > 0, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(timed_project_summary, by = "project_id", relationship = "one-to-one") %>%
  mutate(
    has_any_housing_job_0_5 = coalesce(has_any_housing_job_0_5, FALSE),
    has_any_housing_job_0_10 = coalesce(has_any_housing_job_0_10, FALSE),
    has_any_housing_job_neg2_10 = coalesce(has_any_housing_job_neg2_10, FALSE),
    has_any_housing_job_neg5_15 = coalesce(has_any_housing_job_neg5_15, FALSE),
    has_any_addition_job_0_10 = coalesce(has_any_addition_job_0_10, FALSE),
    has_any_nb_job_0_10 = coalesce(has_any_nb_job_0_10, FALSE),
    has_any_nb_50_plus_job_0_10 = coalesce(has_any_nb_50_plus_job_0_10, FALSE),
    linked_housing_job_count_0_10 = coalesce(linked_housing_job_count_0_10, 0L),
    linked_addition_job_count_0_10 = coalesce(linked_addition_job_count_0_10, 0L),
    linked_nb_job_count_0_10 = coalesce(linked_nb_job_count_0_10, 0L),
    linked_nb_gross_units_0_10 = coalesce(linked_nb_gross_units_0_10, 0),
    linked_gross_add_units_0_10 = coalesce(linked_gross_add_units_0_10, 0),
    linked_gross_loss_units_0_10 = coalesce(linked_gross_loss_units_0_10, 0),
    linked_net_units_0_10 = coalesce(linked_net_units_0_10, 0),
    first_housing_permit_year_0_10 = coalesce(first_housing_permit_year_0_10, NA_integer_),
    first_housing_permit_lag_0_10 = coalesce(first_housing_permit_lag_0_10, NA_integer_)
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

assigned_duplicate_job_count <- candidate_links %>%
  count(job_number, name = "assigned_row_count") %>%
  filter(assigned_row_count > 1) %>%
  nrow()

assigned_duplicate_project_job_count <- candidate_links %>%
  count(project_id, job_number, name = "assigned_project_job_row_count") %>%
  filter(assigned_project_job_row_count > 1) %>%
  nrow()

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
    metric = "timed_candidate_project_job_count_before_assignment",
    value = nrow(candidate_pairs_timed),
    note = "Project-job candidates on exact BBLs within the broad -5 to +15 timing window before one-job assignment."
  ),
  tibble(
    metric = "timed_candidate_shared_job_count",
    value = sum(candidate_job_counts$candidate_project_count > 1, na.rm = TRUE),
    note = "HDB jobs that had more than one eligible ZAP project candidate before assignment."
  ),
  tibble(
    metric = "timed_candidate_extra_project_job_count",
    value = sum(pmax(candidate_job_counts$candidate_project_count - 1, 0), na.rm = TRUE),
    note = "Extra project-job candidate attributions removed by the one-job-to-one-project assignment rule."
  ),
  tibble(
    metric = "assigned_candidate_row_count",
    value = nrow(candidate_links),
    note = "Assigned exact-BBL project-job links after resolving shared HDB jobs."
  ),
  tibble(
    metric = "assigned_duplicate_job_count",
    value = assigned_duplicate_job_count,
    note = "Should be zero because each HDB job is assigned to at most one ZAP project."
  ),
  tibble(
    metric = "assigned_duplicate_project_job_count",
    value = assigned_duplicate_project_job_count,
    note = "Should be zero because project-job candidate rows are deduplicated before assignment."
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
