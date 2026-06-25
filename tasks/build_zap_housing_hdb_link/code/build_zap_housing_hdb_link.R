# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_zap_housing_hdb_link/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

assert_unique_keys <- function(df, key_cols, df_name) {
  duplicate_keys <- df %>%
    count(across(all_of(key_cols)), name = "source_row_count") %>%
    filter(source_row_count > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(df_name, " is not unique by ", paste(key_cols, collapse = ", "), ".")
  }
}

clean_bbl_string <- function(x) {
  out <- str_squish(as.character(x))
  out[out == "" | out %in% c("NA", "NaN")] <- NA_character_
  out
}

valid_bbl_string <- function(x) {
  !is.na(x) & str_detect(x, "^[1-5][0-9]{9}$")
}

safe_min_int <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) {
    return(NA_integer_)
  }
  as.integer(min(x))
}

project_base <- read_csv("../input/zap_housing_cohort_base.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  mutate(
    project_id = as.character(project_id),
    borocd = as.integer(borocd),
    cert_year = as.integer(cert_year),
    source_has_bbl = as.logical(has_bbl),
    source_bbl_count = as.integer(bbl_count)
  )

zap_bbl_raw <- read_parquet("../input/zap_project_bbl.parquet", col_select = c("project_id", "bbl_standardized")) %>%
  as.data.frame() %>%
  as_tibble() %>%
  transmute(
    project_id = as.character(project_id),
    bbl_raw = as.character(bbl_standardized),
    bbl_standardized = clean_bbl_string(bbl_standardized),
    valid_bbl = valid_bbl_string(bbl_standardized)
  )

zap_bbl_quality <- zap_bbl_raw %>%
  filter(!is.na(project_id)) %>%
  group_by(project_id) %>%
  summarise(
    raw_bbl_row_count = n(),
    valid_bbl_count = n_distinct(bbl_standardized[valid_bbl]),
    invalid_bbl_row_count = sum(!valid_bbl, na.rm = TRUE),
    blank_bbl_row_count = sum(is.na(bbl_standardized)),
    .groups = "drop"
  )

zap_bbl <- zap_bbl_raw %>%
  filter(!is.na(project_id), valid_bbl) %>%
  distinct(project_id, bbl_standardized)

hdb_jobs_raw <- read_parquet(
  "../input/dcp_housing_database_project_level_25q4.parquet",
  col_select = c("job_number", "job_type", "permit_year", "completion_year", "classa_prop", "classa_net", "borough_name", "community_district", "bbl")
) %>%
  as.data.frame() %>%
  as_tibble() %>%
  transmute(
    job_number = as.character(job_number),
    job_type = str_squish(as.character(job_type)),
    permit_year = suppressWarnings(as.integer(permit_year)),
    completion_year = suppressWarnings(as.integer(completion_year)),
    classa_prop = suppressWarnings(as.numeric(classa_prop)),
    classa_net = suppressWarnings(as.numeric(classa_net)),
    borough_name_hdb = as.character(borough_name),
    hdb_borocd = suppressWarnings(as.integer(community_district)),
    hdb_bbl_raw = as.character(bbl),
    bbl_standardized = clean_bbl_string(bbl),
    hdb_valid_bbl = valid_bbl_string(bbl_standardized),
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
  )

hdb_jobs <- hdb_jobs_raw %>%
  filter(!is.na(job_number), hdb_valid_bbl)

assert_unique_keys(project_base, "project_id", "ZAP housing cohort base")
assert_unique_keys(zap_bbl, c("project_id", "bbl_standardized"), "ZAP project BBL crosswalk")
assert_unique_keys(hdb_jobs, "job_number", "HDB project-level job input")

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
    source_has_bbl,
    source_bbl_count
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
  ) %>%
  arrange(project_id, job_number, bbl_standardized, permit_year) %>%
  distinct(project_id, job_number, .keep_all = TRUE)

candidate_job_counts <- candidate_pairs %>%
  count(job_number, name = "candidate_project_count")

candidate_links <- candidate_pairs %>%
  left_join(candidate_job_counts, by = "job_number", relationship = "many-to-one") %>%
  mutate(
    assignment_window_rank = case_when(
      within_0_10 ~ 1L,
      within_neg2_10 ~ 2L,
      within_neg5_15 ~ 3L,
      TRUE ~ 4L
    ),
    assignment_abs_lag = abs(permit_lag),
    assignment_timing = case_when(
      within_0_10 ~ "preferred_0_10",
      within_neg2_10 ~ "early_neg2_to_neg1",
      within_neg5_15 ~ "broad_only_neg5_15",
      TRUE ~ "outside_neg5_15"
    )
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
    assignment_rule = "Assign each HDB job to one exact-BBL ZAP project, preferring 0-10 links, closest permit lag, then latest certification year; retain outside-window exact-BBL matches with timing flags."
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
    first_housing_permit_year_0_10 = safe_min_int(permit_year[is_housing_active_job %in% TRUE & within_0_10]),
    first_housing_permit_lag_0_10 = safe_min_int(permit_lag[is_housing_active_job %in% TRUE & within_0_10]),
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
    source_has_bbl = first(.data$source_has_bbl),
    source_bbl_count = first(.data$source_bbl_count),
    matched_bbl_count = n_distinct(bbl_standardized[hdb_job_count > 0]),
    has_any_hdb_match_exact_bbl = any(hdb_job_count > 0, na.rm = TRUE),
    has_any_housing_job_exact_bbl = any(hdb_housing_job_count > 0, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(zap_bbl_quality, by = "project_id", relationship = "one-to-one") %>%
  left_join(timed_project_summary, by = "project_id", relationship = "one-to-one") %>%
  mutate(
    raw_bbl_row_count = coalesce(raw_bbl_row_count, 0L),
    valid_bbl_count = coalesce(valid_bbl_count, 0L),
    invalid_bbl_row_count = coalesce(invalid_bbl_row_count, 0L),
    blank_bbl_row_count = coalesce(blank_bbl_row_count, 0L),
    has_bbl = valid_bbl_count > 0,
    bbl_count = valid_bbl_count,
    bbl_linkable = has_bbl,
    matched_bbl_count = coalesce(matched_bbl_count, 0L),
    has_any_hdb_match_exact_bbl = coalesce(has_any_hdb_match_exact_bbl, FALSE),
    has_any_housing_job_exact_bbl = coalesce(has_any_housing_job_exact_bbl, FALSE),
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
    linked_net_units_0_10 = coalesce(linked_net_units_0_10, 0)
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
  tibble(metric = "project_count", value = nrow(project_summary), status = if_else(nrow(project_summary) > 0, "pass", "fail"), note = "Housing-oriented ULURP projects carried into the exact-BBL housing linkage."),
  tibble(metric = "cd_count", value = n_distinct(project_summary$borocd[!is.na(project_summary$borocd)]), status = if_else(n_distinct(project_summary$borocd[!is.na(project_summary$borocd)]) == 59, "pass", "fail"), note = "Expected 59 CDs."),
  tibble(metric = "zap_project_bbl_raw_row_count", value = nrow(zap_bbl_raw), status = "pass", note = "Raw ZAP project-BBL rows before BBL validation."),
  tibble(metric = "zap_project_bbl_valid_row_count", value = nrow(zap_bbl), status = "pass", note = "ZAP project-BBL rows passing 10-digit NYC BBL validation."),
  tibble(metric = "zap_project_bbl_invalid_row_count", value = sum(!zap_bbl_raw$valid_bbl, na.rm = TRUE), status = "pass", note = "ZAP project-BBL rows excluded before exact-BBL matching because BBL is blank or malformed."),
  tibble(metric = "hdb_job_raw_row_count", value = nrow(hdb_jobs_raw), status = "pass", note = "Raw HDB project-level rows before BBL validation."),
  tibble(metric = "hdb_job_valid_bbl_row_count", value = nrow(hdb_jobs), status = "pass", note = "HDB project-level rows passing 10-digit NYC BBL validation."),
  tibble(metric = "hdb_job_invalid_bbl_row_count", value = sum(!hdb_jobs_raw$hdb_valid_bbl, na.rm = TRUE), status = "pass", note = "HDB rows excluded before exact-BBL matching because BBL is blank or malformed."),
  tibble(metric = "has_valid_bbl_share", value = mean(project_summary$bbl_linkable, na.rm = TRUE), status = "pass", note = "Share of ZAP housing projects with at least one valid linked BBL."),
  tibble(metric = "no_valid_bbl_project_count", value = sum(!project_summary$bbl_linkable, na.rm = TRUE), status = "pass", note = "ZAP housing projects not linkable by exact BBL after validation."),
  tibble(metric = "any_hdb_match_exact_bbl_share", value = mean(project_summary$has_any_hdb_match_exact_bbl, na.rm = TRUE), status = "pass", note = "Share with any exact-BBL match to a DCP housing-database job."),
  tibble(metric = "any_housing_job_exact_bbl_share", value = mean(project_summary$has_any_housing_job_exact_bbl, na.rm = TRUE), status = "pass", note = "Share with any housing-active DCP job on an exact matched BBL, regardless of timing."),
  tibble(metric = "any_housing_job_0_5_share", value = mean(project_summary$has_any_housing_job_0_5[project_summary$bbl_linkable], na.rm = TRUE), status = "pass", note = "Share of BBL-linkable projects with any housing-active DCP job within 0-5 permit years after certification."),
  tibble(metric = "any_housing_job_0_10_share", value = mean(project_summary$has_any_housing_job_0_10[project_summary$bbl_linkable], na.rm = TRUE), status = "pass", note = "Share of BBL-linkable projects with any housing-active DCP job within 0-10 permit years after certification."),
  tibble(metric = "any_housing_job_neg2_10_share", value = mean(project_summary$has_any_housing_job_neg2_10[project_summary$bbl_linkable], na.rm = TRUE), status = "pass", note = "Share of BBL-linkable projects with any housing-active DCP job within -2 to +10 permit years around certification."),
  tibble(metric = "any_housing_job_neg5_15_share", value = mean(project_summary$has_any_housing_job_neg5_15[project_summary$bbl_linkable], na.rm = TRUE), status = "pass", note = "Share of BBL-linkable projects with any housing-active DCP job within -5 to +15 permit years around certification."),
  tibble(metric = "any_addition_job_0_10_share", value = mean(project_summary$has_any_addition_job_0_10[project_summary$bbl_linkable], na.rm = TRUE), status = "pass", note = "Share of BBL-linkable projects with any positive housing-addition DCP job within 0-10 permit years after certification."),
  tibble(metric = "any_nb_job_0_10_share", value = mean(project_summary$has_any_nb_job_0_10[project_summary$bbl_linkable], na.rm = TRUE), status = "pass", note = "Share of BBL-linkable projects with any new-building DCP housing job within 0-10 permit years after certification."),
  tibble(metric = "any_nb_50_plus_job_0_10_share", value = mean(project_summary$has_any_nb_50_plus_job_0_10[project_summary$bbl_linkable], na.rm = TRUE), status = "pass", note = "Share of BBL-linkable projects with any 50+ unit new-building DCP housing job within 0-10 permit years after certification."),
  tibble(metric = "exact_bbl_candidate_project_job_count_before_assignment", value = nrow(candidate_pairs), status = "pass", note = "Project-job candidates on exact BBLs before one-job assignment, all permit lags retained."),
  tibble(metric = "broad_timing_candidate_project_job_count_before_assignment", value = sum(candidate_pairs$within_neg5_15, na.rm = TRUE), status = "pass", note = "Project-job candidates on exact BBLs within the broad -5 to +15 permit-year window before one-job assignment."),
  tibble(metric = "exact_bbl_candidate_shared_job_count", value = sum(candidate_job_counts$candidate_project_count > 1, na.rm = TRUE), status = "pass", note = "HDB jobs that had more than one exact-BBL ZAP project candidate before assignment."),
  tibble(metric = "exact_bbl_candidate_extra_project_job_count", value = sum(pmax(candidate_job_counts$candidate_project_count - 1, 0), na.rm = TRUE), status = "pass", note = "Extra project-job candidate attributions removed by the one-job-to-one-project assignment rule."),
  tibble(metric = "assigned_candidate_row_count", value = nrow(candidate_links), status = "pass", note = "Assigned exact-BBL project-job links after resolving shared HDB jobs."),
  tibble(metric = "assigned_candidate_outside_neg5_15_count", value = sum(candidate_links$assignment_timing == "outside_neg5_15", na.rm = TRUE), status = "pass", note = "Assigned exact-BBL project-job links outside the broad -5 to +15 permit-year timing window."),
  tibble(metric = "assigned_duplicate_job_count", value = assigned_duplicate_job_count, status = if_else(assigned_duplicate_job_count == 0, "pass", "fail"), note = "Should be zero because each HDB job is assigned to at most one ZAP project."),
  tibble(metric = "assigned_duplicate_project_job_count", value = assigned_duplicate_project_job_count, status = if_else(assigned_duplicate_project_job_count == 0, "pass", "fail"), note = "Should be zero because project-job candidate rows are deduplicated before assignment."),
  tibble(metric = "median_first_housing_permit_lag_0_10", value = median(project_summary$first_housing_permit_lag_0_10, na.rm = TRUE), status = "pass", note = "Median first-link permit lag, among projects with a 0-10 housing-active match."),
  tibble(metric = "mean_linked_gross_add_units_0_10_linkable_projects", value = mean(project_summary$linked_gross_add_units_0_10[project_summary$bbl_linkable], na.rm = TRUE), status = "pass", note = "Average linked gross addition units per BBL-linkable ZAP housing project in the 0-10 permit window.")
)

if (any(qc_df$status == "fail")) {
  stop("ZAP-HDB linkage checks failed.")
}

write_csv_if_changed(candidate_links, "../output/zap_housing_hdb_link_candidates.csv")
write_csv_if_changed(project_summary, "../output/zap_housing_hdb_project_summary.csv")

cat("Wrote ZAP-HDB linkage outputs to ../output\n")
