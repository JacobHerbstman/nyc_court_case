# start_year <- 2002
# end_year <- 2025
# sample_mode <- "core"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

cli_args <- commandArgs(trailingOnly = TRUE)
if (length(cli_args) != 3) {
  stop("Usage: Rscript build_ulurp_modification_spine.R <start_year> <end_year> <sample_mode>")
}

start_year <- suppressWarnings(as.integer(cli_args[1]))
end_year <- suppressWarnings(as.integer(cli_args[2]))
sample_mode <- as.character(cli_args[3])

if (is.na(start_year) || is.na(end_year) || start_year > end_year) {
  stop("Invalid start/end year arguments.")
}
if (!sample_mode %in% c("core")) {
  stop("Unsupported sample_mode: ", sample_mode)
}

collapse_values <- function(x) {
  values <- unique(str_squish(as.character(x)))
  values <- values[!is.na(values) & values != ""]
  if (length(values) == 0) {
    return(NA_character_)
  }

  paste(values, collapse = "; ")
}

collapse_numeric_sum <- function(x) {
  values <- suppressWarnings(as.numeric(x))
  values <- values[!is.na(values)]
  if (length(values) == 0) {
    return(0)
  }

  sum(values)
}

normalize_application_key <- function(x) {
  raw_value <- str_to_upper(str_replace_all(str_squish(as.character(x)), "[^A-Z0-9]", ""))
  raw_value[raw_value == ""] <- NA_character_
  str_replace(raw_value, "^[CNM](?=[0-9])", "")
}

application_pattern <- "\\b(?:[CNM]\\s*)?\\d{6,8}\\s*(?:\\([A-Z0-9]+\\)\\s*)?[A-Z]{2,4}\\b"

split_application_rows <- function(df, id_col, text_col, source_col) {
  df |>
    select({{ id_col }}, {{ text_col }}) |>
    mutate(raw_application_number = str_extract_all(coalesce({{ text_col }}, ""), regex(application_pattern, ignore_case = TRUE))) |>
    unnest(raw_application_number) |>
    mutate(
      raw_application_number = str_squish(str_to_upper(raw_application_number)),
      application_key = normalize_application_key(raw_application_number),
      application_prefix = str_extract(str_replace_all(raw_application_number, "\\s+", ""), "^[CNM]"),
      a_application_flag = str_detect(raw_application_number, "\\(A\\)"),
      source_field = source_col
    ) |>
    filter(!is.na(application_key), application_key != "") |>
    distinct({{ id_col }}, application_key, raw_application_number, .keep_all = TRUE)
}

assert_unique_keys <- function(df, key_cols, df_name) {
  duplicate_keys <- df |>
    count(across(all_of(key_cols)), name = "source_row_count") |>
    filter(source_row_count > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(df_name, " is not unique by ", paste(key_cols, collapse = ", "), ".")
  }
}

zap_project <- read_csv("../input/zap_ulurp_project_base.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    project_id = as.character(project_id),
    cert_year = suppressWarnings(as.integer(cert_year)),
    council_district_first = suppressWarnings(as.integer(council_district_first)),
    bbl_count = suppressWarnings(as.integer(bbl_count)),
    linked_gross_add_units_0_10 = suppressWarnings(as.numeric(linked_gross_add_units_0_10)),
    linked_net_units_0_10 = suppressWarnings(as.numeric(linked_net_units_0_10)),
    private_applicant = str_to_upper(as.character(private_applicant)) == "TRUE",
    public_applicant = str_to_upper(as.character(public_applicant)) == "TRUE",
    hpd_led_proxy = str_to_upper(as.character(hpd_led_proxy)) == "TRUE",
    rezoning_or_special_proxy = str_to_upper(as.character(rezoning_or_special_proxy)) == "TRUE",
    public_land_or_disposition_proxy = str_to_upper(as.character(public_land_or_disposition_proxy)) == "TRUE",
    mixed_private_rezoning_proxy = str_to_upper(as.character(mixed_private_rezoning_proxy)) == "TRUE",
    public_hpd_proxy = str_to_upper(as.character(public_hpd_proxy)) == "TRUE",
    has_bbl = str_to_upper(as.character(has_bbl)) == "TRUE",
    is_complete = str_to_upper(as.character(is_complete)) == "TRUE",
    is_fail = str_to_upper(as.character(is_fail)) == "TRUE",
    is_unresolved = str_to_upper(as.character(is_unresolved)) == "TRUE"
  )

decision_panel <- read_csv("../input/council_land_use_decision_panel.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    matter_id = as.character(matter_id),
    query_year = suppressWarnings(as.integer(query_year)),
    matter_file_year = suppressWarnings(as.integer(matter_file_year)),
    matter_in_main_vote_sample = str_to_upper(as.character(matter_in_main_vote_sample)) == "TRUE",
    has_affected_council_district = str_to_upper(as.character(has_affected_council_district)) == "TRUE",
    has_local_member_from_roster = str_to_upper(as.character(has_local_member_from_roster)) == "TRUE",
    has_local_member_vote_observed = str_to_upper(as.character(has_local_member_vote_observed)) == "TRUE"
  )

matter_universe <- read_csv("../input/member_deference_matter_universe.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    matter_id = as.character(matter_id),
    council_history_action_count = suppressWarnings(as.numeric(council_history_action_count)),
    council_approved_action_count = suppressWarnings(as.numeric(council_approved_action_count)),
    council_disapproved_action_count = suppressWarnings(as.numeric(council_disapproved_action_count)),
    council_filed_action_count = suppressWarnings(as.numeric(council_filed_action_count)),
    council_modified_action_count = suppressWarnings(as.numeric(council_modified_action_count))
  ) |>
  select(
    matter_id,
    council_history_action_count,
    council_approved_action_count,
    council_disapproved_action_count,
    council_filed_action_count,
    council_modified_action_count,
    matter_index_districts,
    legistar_text_districts,
    final_history_detail_url
  )

assert_unique_keys(zap_project, "project_id", "ZAP modification input")
assert_unique_keys(decision_panel, "matter_id", "Council decision input")
assert_unique_keys(matter_universe, "matter_id", "Member deference matter universe input")

council_matter <- decision_panel |>
  left_join(matter_universe, by = "matter_id", relationship = "one-to-one") |>
  mutate(
    council_modified_action_count = coalesce(council_modified_action_count, 0),
    searchable_matter_text = str_to_upper(str_squish(paste(
      coalesce(matter_file, ""),
      coalesce(title, ""),
      coalesce(application_keys, ""),
      coalesce(zap_matched_application_keys, "")
    )))
  )

zap_application_rows <- bind_rows(
  split_application_rows(zap_project, project_id, ulurp_numbers, "zap_ulurp_numbers"),
  split_application_rows(zap_project, project_id, project_brief, "zap_project_brief")
) |>
  distinct(project_id, application_key, raw_application_number, .keep_all = TRUE)

matter_application_rows <- bind_rows(
  split_application_rows(council_matter, matter_id, application_keys, "council_application_keys"),
  split_application_rows(council_matter, matter_id, title, "council_title")
) |>
  distinct(matter_id, application_key, raw_application_number, .keep_all = TRUE)

zap_application_key_summary <- zap_application_rows |>
  group_by(application_key) |>
  summarise(
    zap_project_count = n_distinct(project_id),
    zap_project_ids_for_key = collapse_values(project_id),
    zap_raw_application_numbers = collapse_values(raw_application_number),
    zap_source_fields = collapse_values(source_field),
    zap_a_application_key_flag = any(a_application_flag, na.rm = TRUE),
    .groups = "drop"
  )

matter_application_key_summary <- matter_application_rows |>
  group_by(application_key) |>
  summarise(
    council_matter_count = n_distinct(matter_id),
    council_matter_ids_for_key = collapse_values(matter_id),
    council_raw_application_numbers = collapse_values(raw_application_number),
    council_source_fields = collapse_values(source_field),
    council_a_application_key_flag = any(a_application_flag, na.rm = TRUE),
    council_m_application_key_flag = any(application_prefix == "M", na.rm = TRUE),
    .groups = "drop"
  )

ambiguous_application_keys <- full_join(
  zap_application_key_summary,
  matter_application_key_summary,
  by = "application_key",
  relationship = "one-to-one"
) |>
  filter(coalesce(zap_project_count, 0L) > 1L) |>
  mutate(
    ambiguity_reason = "application_key_matches_multiple_zap_projects",
    production_join_status = "excluded_from_spine_exact_join"
  ) |>
  arrange(application_key)

zap_application_key_unique <- zap_application_key_summary |>
  filter(zap_project_count == 1L) |>
  transmute(
    application_key,
    project_id = zap_project_ids_for_key,
    zap_raw_application_numbers,
    zap_source_fields,
    zap_a_application_key_flag
  )

matter_application_key_unique <- matter_application_rows |>
  distinct(matter_id, application_key) |>
  left_join(
    matter_application_key_summary |>
      select(application_key, council_raw_application_numbers, council_source_fields, council_a_application_key_flag, council_m_application_key_flag),
    by = "application_key",
    relationship = "many-to-one"
  )

project_matter_crosswalk <- zap_application_key_unique |>
  inner_join(matter_application_key_unique, by = "application_key", relationship = "one-to-many") |>
  left_join(
    council_matter,
    by = "matter_id",
    relationship = "many-to-one"
  ) |>
  mutate(
    exact_match_rule = "normalized_ulurp_or_application_key",
    council_modification_signal = council_modified_action_count > 0
  ) |>
  select(
    project_id,
    matter_id,
    application_key,
    exact_match_rule,
    zap_raw_application_numbers,
    council_raw_application_numbers,
    zap_source_fields,
    council_source_fields,
    zap_a_application_key_flag,
    council_a_application_key_flag,
    council_m_application_key_flag,
    matter_file,
    matter_type,
    matter_status,
    disposition_group,
    decision_date,
    decision_action,
    decision_result,
    vote_source,
    vote_date,
    vote_margin,
    affirmative_count,
    negative_count,
    abstain_count,
    affected_council_districts,
    affected_district_source,
    local_members_from_roster,
    local_member_final_action_vote_status,
    local_member_final_action_votes,
    member_deference_vote_signal,
    council_history_action_count,
    council_approved_action_count,
    council_disapproved_action_count,
    council_filed_action_count,
    council_modified_action_count,
    council_modification_signal,
    title,
    matter_url,
    history_detail_url
  ) |>
  distinct(project_id, matter_id, application_key, .keep_all = TRUE) |>
  arrange(project_id, matter_id, application_key)

project_matter_summary <- project_matter_crosswalk |>
  group_by(project_id) |>
  summarise(
    council_exact_match_flag = TRUE,
    council_matter_count = n_distinct(matter_id),
    council_application_key_count = n_distinct(application_key),
    council_matter_ids = collapse_values(matter_id),
    council_matter_files = collapse_values(matter_file),
    council_application_keys = collapse_values(application_key),
    council_disposition_groups = collapse_values(disposition_group),
    council_decision_dates = collapse_values(decision_date),
    council_vote_dates = collapse_values(vote_date),
    council_vote_sources = collapse_values(vote_source),
    council_affected_districts = collapse_values(affected_council_districts),
    council_affected_district_sources = collapse_values(affected_district_source),
    local_member_names = collapse_values(local_members_from_roster),
    local_member_vote_statuses = collapse_values(local_member_final_action_vote_status),
    local_member_final_action_votes = collapse_values(local_member_final_action_votes),
    member_deference_vote_signals = collapse_values(member_deference_vote_signal),
    council_modified_action_count = collapse_numeric_sum(council_modified_action_count),
    council_modification_signal = any(council_modification_signal, na.rm = TRUE),
    council_a_application_flag = any(council_a_application_key_flag, na.rm = TRUE),
    council_m_application_key_flag = any(council_m_application_key_flag, na.rm = TRUE),
    council_titles = collapse_values(title),
    council_matter_urls = collapse_values(matter_url),
    council_history_detail_urls = collapse_values(history_detail_url),
    .groups = "drop"
  )

project_spine <- zap_project |>
  left_join(project_matter_summary, by = "project_id", relationship = "one-to-one") |>
  mutate(
    council_exact_match_flag = coalesce(council_exact_match_flag, FALSE),
    council_matter_count = coalesce(council_matter_count, 0L),
    council_application_key_count = coalesce(council_application_key_count, 0L),
    council_modified_action_count = coalesce(council_modified_action_count, 0),
    council_modification_signal = coalesce(council_modification_signal, FALSE),
    council_a_application_flag = coalesce(council_a_application_flag, FALSE),
    council_m_application_key_flag = coalesce(council_m_application_key_flag, FALSE),
    searchable_project_text = str_to_upper(str_squish(paste(
      coalesce(project_name, ""),
      coalesce(project_brief, ""),
      coalesce(primary_applicant, ""),
      coalesce(ceqr_leadagency, ""),
      coalesce(actions, ""),
      coalesce(council_titles, "")
    ))),
    post_certification_withdrawal_flag = is_fail | str_detect(
      str_to_upper(str_squish(paste(coalesce(project_status, ""), coalesce(public_status, ""), coalesce(current_milestone, "")))),
      "WITHDRAW|TERMINAT"
    ),
    in_core_year_window = cert_year >= start_year & cert_year <= end_year,
    in_modification_spine = in_core_year_window & (council_exact_match_flag | post_certification_withdrawal_flag),
    spine_inclusion_reason = case_when(
      council_exact_match_flag & post_certification_withdrawal_flag ~ "council_exact_match_and_post_certification_withdrawal",
      council_exact_match_flag ~ "council_exact_match",
      post_certification_withdrawal_flag ~ "post_certification_withdrawal_or_termination",
      TRUE ~ "excluded"
    ),
    a_application_flag = str_detect(str_to_upper(coalesce(ulurp_numbers, "")), "\\(A\\)") | council_a_application_flag,
    m_report_or_m_application_flag = council_m_application_key_flag | str_detect(str_to_upper(coalesce(council_titles, "")), "\\bM\\s*\\d{6}|MODIFICATION"),
    council_outcome = case_when(
      council_exact_match_flag & council_modification_signal & str_detect(coalesce(council_disposition_groups, ""), "adopted") ~ "approve_w_mods",
      council_exact_match_flag & str_detect(coalesce(council_disposition_groups, ""), "adopted") ~ "approve",
      council_exact_match_flag & str_detect(coalesce(council_disposition_groups, ""), "disapproved") ~ "disapprove",
      council_exact_match_flag & str_detect(coalesce(council_disposition_groups, ""), "withdrawn|filed") ~ "withdrawn_or_filed",
      post_certification_withdrawal_flag ~ "withdrawn_post_cert",
      TRUE ~ "never_voted_or_unmatched"
    ),
    stratum = case_when(
      str_detect(searchable_project_text, "\\bCITYWIDE\\b") ~ "D",
      public_applicant & rezoning_or_special_proxy & (
        bbl_count >= 20L | str_detect(searchable_project_text, "NEIGHBORHOOD|AREA REZON|REZONING AREA|CORRIDOR|DISTRICT PLAN|SPECIAL DISTRICT|EAST NEW YORK|INWOOD|GOWANUS|SOHO|NOHO|WILLIAMSBURG|GREENPOINT|HUDSON YARDS")
      ) ~ "C",
      public_applicant | public_hpd_proxy | hpd_led_proxy ~ "B",
      private_applicant ~ "A",
      TRUE ~ "unknown"
    ),
    stratum_rule = case_when(
      stratum == "D" ~ "citywide_text_signal",
      stratum == "C" ~ "public_or_city_applicant_with_area_rezoning_signal",
      stratum == "B" ~ "public_or_hpd_project_level_signal",
      stratum == "A" ~ "private_applicant_signal",
      TRUE ~ "unclassified_applicant_signal"
    ),
    source_gap_flag = !council_exact_match_flag,
    source_gap_reason = case_when(
      council_exact_match_flag ~ NA_character_,
      post_certification_withdrawal_flag ~ "no_exact_council_match_retained_as_post_certification_withdrawal",
      TRUE ~ "excluded_no_exact_council_match"
    ),
    sample_start_year = start_year,
    sample_end_year = end_year,
    sample_mode = sample_mode
  ) |>
  filter(in_modification_spine) |>
  select(
    project_id,
    project_name,
    project_brief,
    borocd,
    borough_name,
    cert_year,
    cert_era,
    certified_referred_date,
    approval_date,
    completed_date,
    council_district_first,
    ulurp_numbers,
    current_milestone,
    current_milestone_date,
    project_status,
    public_status,
    applicant_type,
    primary_applicant,
    ceqr_leadagency,
    actions,
    private_applicant,
    public_applicant,
    hpd_led_proxy,
    rezoning_or_special_proxy,
    public_land_or_disposition_proxy,
    mixed_private_rezoning_proxy,
    public_hpd_proxy,
    has_bbl,
    bbl_count,
    linked_gross_add_units_0_10,
    linked_net_units_0_10,
    first_housing_permit_year_0_10,
    first_housing_permit_lag_0_10,
    council_exact_match_flag,
    council_matter_count,
    council_application_key_count,
    council_matter_ids,
    council_matter_files,
    council_application_keys,
    council_disposition_groups,
    council_decision_dates,
    council_vote_dates,
    council_vote_sources,
    council_affected_districts,
    council_affected_district_sources,
    local_member_names,
    local_member_vote_statuses,
    local_member_final_action_votes,
    member_deference_vote_signals,
    council_modified_action_count,
    council_modification_signal,
    council_a_application_flag,
    council_m_application_key_flag,
    council_titles,
    council_matter_urls,
    council_history_detail_urls,
    post_certification_withdrawal_flag,
    spine_inclusion_reason,
    a_application_flag,
    m_report_or_m_application_flag,
    council_outcome,
    stratum,
    stratum_rule,
    source_gap_flag,
    source_gap_reason,
    treat_pp,
    treat_z_boro,
    sample_start_year,
    sample_end_year,
    sample_mode
  ) |>
  arrange(cert_year, borough_name, project_id)

qc_df <- bind_rows(
  tibble(metric = "spine_row_count", value = as.character(nrow(project_spine)), status = if_else(nrow(project_spine) > 0, "pass", "fail"), note = "Rows in the modification project spine."),
  tibble(metric = "unique_project_id_count", value = as.character(n_distinct(project_spine$project_id)), status = if_else(nrow(project_spine) == n_distinct(project_spine$project_id), "pass", "fail"), note = "Spine should be unique by project_id."),
  tibble(metric = "min_cert_year", value = as.character(min(project_spine$cert_year, na.rm = TRUE)), status = if_else(min(project_spine$cert_year, na.rm = TRUE) >= start_year, "pass", "fail"), note = "Minimum certification year after sample filter."),
  tibble(metric = "max_cert_year", value = as.character(max(project_spine$cert_year, na.rm = TRUE)), status = if_else(max(project_spine$cert_year, na.rm = TRUE) <= end_year, "pass", "fail"), note = "Maximum certification year after sample filter."),
  tibble(metric = "council_exact_match_project_count", value = as.character(sum(project_spine$council_exact_match_flag)), status = if_else(sum(project_spine$council_exact_match_flag) > 0, "pass", "fail"), note = "Projects linked to Council matters by exact normalized ULURP/application keys."),
  tibble(metric = "post_certification_withdrawal_unmatched_count", value = as.character(sum(project_spine$post_certification_withdrawal_flag & !project_spine$council_exact_match_flag)), status = "pass", note = "Post-certification withdrawn/terminated projects retained without exact Council matter matches."),
  tibble(metric = "approve_with_mods_project_count", value = as.character(sum(project_spine$council_outcome == "approve_w_mods")), status = "pass", note = "Projects with a Council modification history signal and adopted disposition."),
  tibble(metric = "ambiguous_application_key_count", value = as.character(nrow(ambiguous_application_keys)), status = "pass", note = "Application keys excluded from exact production join because they match multiple ZAP projects.")
)

write_csv_if_changed(project_spine, "../output/ulurp_modification_project_spine.csv")
write_csv_if_changed(project_matter_crosswalk, "../output/ulurp_modification_project_matter_crosswalk.csv")

if (any(qc_df$status == "fail")) {
  stop("ULURP modification spine checks failed.")
}

cat("Wrote ULURP modification spine outputs to ../output\n")
