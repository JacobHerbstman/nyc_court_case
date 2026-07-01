suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

collapse_values <- function(x) {
  values <- unique(str_squish(as.character(x)))
  values <- values[!is.na(values) & values != ""]
  if (length(values) == 0) {
    return(NA_character_)
  }

  paste(values, collapse = "; ")
}

collapse_logical <- function(x) {
  values <- x[!is.na(x)]
  if (length(values) == 0) {
    return(NA)
  }

  any(values)
}

normalize_ulurp_key <- function(x) {
  raw_value <- str_to_upper(str_replace_all(str_squish(as.character(x)), "[^A-Z0-9]", ""))
  raw_value[raw_value == ""] <- NA_character_
  str_replace(raw_value, "^[CNM](?=[0-9])", "")
}

split_ulurp_rows <- function(df, id_col, ulurp_col) {
  df %>%
    select({{ id_col }}, {{ ulurp_col }}) %>%
    mutate(ulurp_number = str_split(coalesce({{ ulurp_col }}, ""), "\\s*;\\s*")) %>%
    unnest(ulurp_number) %>%
    mutate(
      ulurp_number = str_squish(ulurp_number),
      ulurp_key = normalize_ulurp_key(ulurp_number)
    ) %>%
    filter(!is.na(ulurp_key))
}

seed_cases <- read_csv(
  "charter_overrule_seed_cases.csv",
  col_types = cols(.default = col_character()),
  na = c("", "NA")
) %>%
  mutate(
    across(where(is.character), ~ na_if(str_squish(.x), "")),
    vote_year = as.integer(vote_year),
    vote_date = as.character(parse_mixed_date(vote_date)),
    residential_mixed_flag = str_to_upper(residential_mixed_flag) == "TRUE",
    udaap_flag = str_to_upper(udaap_flag) == "TRUE"
  )

zap_boolean_cols <- c(
  "private_applicant_flag",
  "public_applicant_flag",
  "housing_any_candidate_flag",
  "housing_strict_text_flag",
  "housing_broad_text_flag",
  "hpd_text_flag",
  "mih_flag_bool",
  "rezoning_special_action_flag",
  "public_land_disposition_action_flag",
  "hpd_public_housing_action_flag"
)

zap_projects <- read_csv(
  "../input/zap_housing_project_base_audited.csv",
  col_types = cols(.default = col_character()),
  na = c("", "NA")
) %>%
  mutate(
    across(where(is.character), ~ na_if(str_squish(.x), "")),
    across(any_of(zap_boolean_cols), ~ str_to_upper(.x) == "TRUE")
  )

council_matter <- read_csv(
  "../input/council_land_use_matter.csv",
  col_types = cols(.default = col_character()),
  na = c("", "NA")
) %>%
  mutate(
    across(where(is.character), ~ na_if(str_squish(.x), "")),
    vote_year = as.integer(vote_year)
  ) %>%
  transmute(
    seed_id,
    council_matter_id = matter_id,
    council_matter_guid = matter_guid,
    council_matter_file = matter_file,
    council_lu_numbers = lu_numbers,
    council_resolution_numbers = resolution_numbers,
    council_ulurp_numbers = ulurp_numbers,
    council_vote_date = as.character(vote_date),
    council_vote_year = vote_year,
    council_disposition_staged = council_disposition,
    council_vote_margin = vote_margin,
    council_source_roles = source_roles,
    council_source_urls = source_urls,
    council_source_raw_paths = source_raw_paths,
    council_source_coverage = source_coverage
  )

seed_ulurp <- split_ulurp_rows(seed_cases, seed_id, ulurp_numbers) %>%
  distinct(seed_id, ulurp_number, ulurp_key)

seed_ulurp_duplicates <- seed_ulurp %>%
  count(ulurp_key, name = "seed_key_rows") %>%
  filter(seed_key_rows > 1)

zap_ulurp <- split_ulurp_rows(zap_projects, project_id, ulurp_numbers) %>%
  left_join(
    zap_projects %>%
      select(
        project_id,
        zap_project_name = project_name,
        zap_project_brief = project_brief,
        zap_status_simple = status_simple,
        zap_project_status = project_status,
        zap_public_status = public_status,
        zap_approval_date = approval_date_parsed,
        zap_completed_date = completed_date_parsed,
        zap_cert_year = cert_year,
        zap_borough = borough_name_primary,
        zap_community_district = community_district,
        zap_borocd_primary = borocd_primary,
        zap_primary_applicant = primary_applicant,
        zap_applicant_type = applicant_type,
        zap_private_applicant_flag = private_applicant_flag,
        zap_public_applicant_flag = public_applicant_flag,
        zap_housing_any_candidate_flag = housing_any_candidate_flag,
        zap_housing_strict_text_flag = housing_strict_text_flag,
        zap_housing_broad_text_flag = housing_broad_text_flag,
        zap_hpd_text_flag = hpd_text_flag,
        zap_mih_flag = mih_flag_bool,
        zap_rezoning_special_action_flag = rezoning_special_action_flag,
        zap_public_land_disposition_action_flag = public_land_disposition_action_flag,
        zap_hpd_public_housing_action_flag = hpd_public_housing_action_flag
      ),
    by = "project_id",
    relationship = "many-to-one"
  ) %>%
  distinct(ulurp_key, project_id, .keep_all = TRUE)

zap_lookup <- zap_ulurp %>%
  group_by(ulurp_key) %>%
  summarise(
    zap_project_id = collapse_values(project_id),
    zap_project_name = collapse_values(zap_project_name),
    zap_project_brief = collapse_values(zap_project_brief),
    zap_status_simple = collapse_values(zap_status_simple),
    zap_project_status = collapse_values(zap_project_status),
    zap_public_status = collapse_values(zap_public_status),
    zap_approval_date = collapse_values(zap_approval_date),
    zap_completed_date = collapse_values(zap_completed_date),
    zap_cert_year = collapse_values(zap_cert_year),
    zap_borough = collapse_values(zap_borough),
    zap_community_district = collapse_values(zap_community_district),
    zap_borocd_primary = collapse_values(zap_borocd_primary),
    zap_primary_applicant = collapse_values(zap_primary_applicant),
    zap_applicant_type = collapse_values(zap_applicant_type),
    zap_private_applicant_flag = collapse_logical(zap_private_applicant_flag),
    zap_public_applicant_flag = collapse_logical(zap_public_applicant_flag),
    zap_housing_any_candidate_flag = collapse_logical(zap_housing_any_candidate_flag),
    zap_housing_strict_text_flag = collapse_logical(zap_housing_strict_text_flag),
    zap_housing_broad_text_flag = collapse_logical(zap_housing_broad_text_flag),
    zap_hpd_text_flag = collapse_logical(zap_hpd_text_flag),
    zap_mih_flag = collapse_logical(zap_mih_flag),
    zap_rezoning_special_action_flag = collapse_logical(zap_rezoning_special_action_flag),
    zap_public_land_disposition_action_flag = collapse_logical(zap_public_land_disposition_action_flag),
    zap_hpd_public_housing_action_flag = collapse_logical(zap_hpd_public_housing_action_flag),
    zap_project_count_for_ulurp = n_distinct(project_id),
    .groups = "drop"
  )

seed_matches <- seed_ulurp %>%
  left_join(zap_lookup, by = "ulurp_key", relationship = "many-to-one") %>%
  mutate(matched_zap_flag = !is.na(zap_project_id))

zap_seed_summary <- seed_matches %>%
  group_by(seed_id) %>%
  summarise(
    zap_project_ids = collapse_values(zap_project_id),
    zap_project_names = collapse_values(zap_project_name),
    zap_project_briefs = collapse_values(zap_project_brief),
    zap_status_simple = collapse_values(zap_status_simple),
    zap_project_status = collapse_values(zap_project_status),
    zap_public_status = collapse_values(zap_public_status),
    zap_approval_dates = collapse_values(zap_approval_date),
    zap_completed_dates = collapse_values(zap_completed_date),
    zap_cert_years = collapse_values(zap_cert_year),
    zap_boroughs = collapse_values(zap_borough),
    zap_community_districts = collapse_values(zap_community_district),
    zap_borocd_primary = collapse_values(zap_borocd_primary),
    zap_primary_applicants = collapse_values(zap_primary_applicant),
    zap_applicant_types = collapse_values(zap_applicant_type),
    matched_ulurp_numbers = collapse_values(ulurp_number[matched_zap_flag]),
    unmatched_ulurp_numbers = collapse_values(ulurp_number[!matched_zap_flag]),
    zap_match_count = sum(matched_zap_flag),
    zap_housing_any_candidate_flag = collapse_logical(zap_housing_any_candidate_flag[matched_zap_flag]),
    zap_housing_strict_text_flag = collapse_logical(zap_housing_strict_text_flag[matched_zap_flag]),
    zap_housing_broad_text_flag = collapse_logical(zap_housing_broad_text_flag[matched_zap_flag]),
    zap_hpd_text_flag = collapse_logical(zap_hpd_text_flag[matched_zap_flag]),
    zap_mih_flag = collapse_logical(zap_mih_flag[matched_zap_flag]),
    zap_private_applicant_flag = collapse_logical(zap_private_applicant_flag[matched_zap_flag]),
    zap_public_applicant_flag = collapse_logical(zap_public_applicant_flag[matched_zap_flag]),
    zap_rezoning_special_action_flag = collapse_logical(zap_rezoning_special_action_flag[matched_zap_flag]),
    zap_public_land_disposition_action_flag = collapse_logical(zap_public_land_disposition_action_flag[matched_zap_flag]),
    zap_hpd_public_housing_action_flag = collapse_logical(zap_hpd_public_housing_action_flag[matched_zap_flag]),
    max_zap_projects_per_ulurp = max(coalesce(zap_project_count_for_ulurp, 0L)),
    .groups = "drop"
  )

project_bundles <- seed_cases %>%
  left_join(zap_seed_summary, by = "seed_id", relationship = "one-to-one") %>%
  left_join(council_matter, by = "seed_id", relationship = "one-to-one") %>%
  rowwise() %>%
  mutate(
    project_bundle_id = seed_id,
    vote_date = coalesce(vote_date, council_vote_date),
    vote_year = coalesce(vote_year, council_vote_year),
    lu_numbers = coalesce(lu_numbers, council_lu_numbers),
    resolution_numbers = coalesce(resolution_numbers, council_resolution_numbers),
    ulurp_numbers = coalesce(ulurp_numbers, council_ulurp_numbers),
    council_disposition = coalesce(council_disposition, council_disposition_staged),
    vote_margin = coalesce(vote_margin, council_vote_margin),
    source_urls = collapse_values(c(source_urls, council_source_urls)),
    source_raw_paths = collapse_values(council_source_raw_paths)
  ) %>%
  ungroup() %>%
  mutate(
    zap_matched_flag = !is.na(zap_project_ids),
    evidence_tier_rank = case_when(
      evidence_tier == "official_transcript_local_member_no_vote" ~ 1L,
      evidence_tier == "official_minutes_local_member_no_vote" ~ 1L,
      evidence_tier == "official_action_detail_local_member_no_vote" ~ 1L,
      evidence_tier == "official_split_vote_charter_seed" ~ 2L,
      str_detect(evidence_tier, "charter_staff_table") ~ 3L,
      TRUE ~ 9L
    ),
    manual_audit_required = evidence_tier_rank > 2L,
    source_coverage_flag = case_when(
      vote_year < 1998 ~ "pre_legistar_archival_pending",
      !is.na(council_source_coverage) ~ council_source_coverage,
      TRUE ~ "charter_seed_pending_official_record_pull"
    ),
    zap_classifier_gap_flag = residential_mixed_flag & (is.na(zap_housing_any_candidate_flag) | !zap_housing_any_candidate_flag),
    classification_override_reason = case_when(
      residential_mixed_flag & zap_classifier_gap_flag ~ "Council/Charter text classifies this as residential/mixed-use even though the current ZAP housing candidate flag is missing or false.",
      residential_mixed_flag ~ "Council/Charter residential/mixed-use classification is retained as the analysis category.",
      TRUE ~ NA_character_
    ),
    candidate_universe = "charter_report_seed_validation",
    audit_version = "seed_2025_charter_plus_official_case_checks"
  ) %>%
  select(
    project_bundle_id, vote_year, vote_date, project_name, borough,
    affected_council_districts, local_members, lu_numbers, resolution_numbers,
    ulurp_numbers, charter_ulurp_number, zap_project_ids, zap_project_names,
    matched_ulurp_numbers, unmatched_ulurp_numbers, council_matter_id,
    council_matter_guid, council_matter_file, council_disposition,
    overrule_status, evidence_tier, evidence_tier_rank, evidence_summary,
    vote_margin, residential_mixed_flag, use_category, charter_category,
    applicant_type, action_code_families, udaap_flag, source_coverage_flag,
    manual_audit_required, candidate_generation_reason, non_confirmed_reason,
    zap_matched_flag, zap_match_count, zap_status_simple, zap_project_status,
    zap_public_status, zap_approval_dates, zap_completed_dates, zap_cert_years,
    zap_boroughs, zap_community_districts, zap_borocd_primary,
    zap_primary_applicants, zap_applicant_types,
    zap_housing_any_candidate_flag, zap_housing_strict_text_flag,
    zap_housing_broad_text_flag, zap_hpd_text_flag, zap_mih_flag,
    zap_classifier_gap_flag, classification_override_reason,
    zap_private_applicant_flag, zap_public_applicant_flag,
    zap_rezoning_special_action_flag, zap_public_land_disposition_action_flag,
    zap_hpd_public_housing_action_flag, source_urls, source_raw_paths,
    candidate_universe, audit_version
  ) %>%
  arrange(vote_year, project_name)

candidates <- project_bundles %>%
  mutate(candidate_status = "candidate") %>%
  select(candidate_status, everything())

action_crosswalk <- seed_matches %>%
  left_join(
    project_bundles %>%
      transmute(
        seed_id = project_bundle_id,
        project_bundle_id,
        project_name,
        lu_numbers, resolution_numbers, council_matter_id, vote_year,
        vote_date, overrule_status
      ),
    by = "seed_id",
    relationship = "many-to-one"
  ) %>%
  transmute(
    project_bundle_id,
    project_name,
    vote_year,
    vote_date,
    lu_numbers,
    resolution_numbers,
    ulurp_number,
    normalized_ulurp_key = ulurp_key,
    zap_project_id,
    zap_project_name,
    zap_status_simple,
    council_matter_id,
    overrule_status,
    matched_zap_flag,
    source_match_method = ifelse(matched_zap_flag, "parsed_ulurp_number", "unmatched_seed_identifier")
  ) %>%
  arrange(project_bundle_id, ulurp_number)

time_series <- tibble(vote_year = 1989:max(2025L, max(project_bundles$vote_year, na.rm = TRUE))) %>%
  left_join(
    project_bundles %>%
      group_by(vote_year) %>%
      summarise(
        confirmed_count = sum(overrule_status == "confirmed", na.rm = TRUE),
        candidate_count = n(),
        unresolved_count = sum(overrule_status == "unresolved", na.rm = TRUE),
        rejected_count = sum(overrule_status == "rejected", na.rm = TRUE),
        confirmed_direct_official_count = sum(overrule_status == "confirmed" & evidence_tier_rank <= 2L, na.rm = TRUE),
        confirmed_charter_seed_count = sum(overrule_status == "confirmed" & evidence_tier_rank > 2L, na.rm = TRUE),
        residential_mixed_confirmed_count = sum(overrule_status == "confirmed" & residential_mixed_flag, na.rm = TRUE),
        udaap_confirmed_count = sum(overrule_status == "confirmed" & udaap_flag, na.rm = TRUE),
        .groups = "drop"
      ),
    by = "vote_year",
    relationship = "one-to-one"
  ) %>%
  mutate(
    across(
      c(
        confirmed_count, candidate_count, unresolved_count, rejected_count,
        confirmed_direct_official_count, confirmed_charter_seed_count,
        residential_mixed_confirmed_count, udaap_confirmed_count
      ),
      ~ replace_na(.x, 0L)
    ),
    source_coverage = ifelse(vote_year < 1998, "pre_legistar_archival_pending", "legistar_seed_incomplete"),
    coverage_note = ifelse(
      vote_year < 1998,
      "Municipal Library and City Record archival records are not fully staged; zero counts are coverage gaps, not verified zeros.",
      "Post-1998 series currently uses the Charter seed table plus official Dock Street and Broadway Triangle case records; broad Legistar recall is not complete."
    )
  )

residential_mixed_case_anatomy <- project_bundles %>%
  filter(residential_mixed_flag) %>%
  transmute(
    project_bundle_id,
    vote_year,
    project_name,
    borough,
    affected_council_districts,
    local_members,
    lu_numbers,
    ulurp_numbers,
    zap_project_ids,
    zap_project_names,
    council_disposition,
    overrule_status,
    evidence_tier,
    vote_margin,
    use_category,
    applicant_type,
    action_code_families,
    udaap_flag,
    zap_housing_any_candidate_flag,
    zap_housing_strict_text_flag,
    zap_housing_broad_text_flag,
    zap_hpd_text_flag,
    zap_mih_flag,
    zap_classifier_gap_flag,
    classification_override_reason,
    evidence_summary,
    source_urls
  ) %>%
  arrange(vote_year, project_name)

required_seed_ids <- c(
  "ny_blood_center_2021",
  "police_academy_2009",
  "dock_street_2009",
  "broadway_triangle_2009",
  "college_point_2009",
  "jamaica_rezoning_2007",
  "maspeth_high_school_2007",
  "watchtower_2004",
  "harlem_park_hotel_2004",
  "nycem_headquarters_2003",
  "manhattan_parking_garage_2002",
  "laguardia_hotel_2001"
)

dock_row <- project_bundles %>% filter(project_bundle_id == "dock_street_2009")
broadway_row <- project_bundles %>% filter(project_bundle_id == "broadway_triangle_2009")

candidate_qc <- tibble(
  check_name = c(
    "all_charter_seed_examples_present",
    "non_confirmed_items_have_reason",
    "seed_ulurp_keys_are_unique",
    "zap_lookup_collapsed_to_unique_ulurp_keys",
    "dock_street_bundle_acceptance",
    "broadway_triangle_bundle_acceptance",
    "maspeth_scq_retained_without_zap_match",
    "residential_mixed_cases_have_council_text_classification",
    "time_series_has_pre_1998_coverage_flags",
    "time_series_is_annual_1989_to_2025"
  ),
  passed = c(
    all(required_seed_ids %in% project_bundles$project_bundle_id),
    all(project_bundles$overrule_status == "confirmed" | !is.na(project_bundles$non_confirmed_reason)),
    nrow(seed_ulurp_duplicates) == 0,
    !anyDuplicated(zap_lookup$ulurp_key),
    nrow(dock_row) == 1 &&
      str_detect(dock_row$lu_numbers, "1073") &&
      str_detect(dock_row$lu_numbers, "1074") &&
      str_detect(dock_row$lu_numbers, "1075") &&
      str_detect(dock_row$ulurp_numbers, "C090181ZMK") &&
      str_detect(dock_row$ulurp_numbers, "C090183ZSK") &&
      str_detect(dock_row$ulurp_numbers, "C090184ZSK") &&
      dock_row$local_members == "David Yassky" &&
      dock_row$council_disposition == "approved" &&
      dock_row$vote_margin == "41-10-0" &&
      dock_row$overrule_status == "confirmed",
    nrow(broadway_row) == 1 &&
      str_detect(broadway_row$lu_numbers, "1227") &&
      str_detect(broadway_row$lu_numbers, "1228") &&
      str_detect(broadway_row$lu_numbers, "1229") &&
      str_detect(broadway_row$lu_numbers, "1230") &&
      str_detect(broadway_row$ulurp_numbers, "C090413ZMK") &&
      str_detect(broadway_row$ulurp_numbers, "N090414ZRK") &&
      str_detect(broadway_row$ulurp_numbers, "C090415HUK") &&
      str_detect(broadway_row$ulurp_numbers, "C090416HAK") &&
      broadway_row$vote_margin == "36-10-4" &&
      broadway_row$overrule_status == "confirmed",
    any(project_bundles$project_bundle_id == "maspeth_high_school_2007" & !project_bundles$zap_matched_flag),
    all(
      project_bundles$project_bundle_id[project_bundles$project_bundle_id %in% c("dock_street_2009", "watchtower_2004", "jamaica_rezoning_2007")] %in%
        residential_mixed_case_anatomy$project_bundle_id
    ) &&
      all(!is.na(residential_mixed_case_anatomy$classification_override_reason)),
    all(time_series$source_coverage[time_series$vote_year < 1998] == "pre_legistar_archival_pending"),
    identical(time_series$vote_year, 1989:2025)
  ),
  detail = c(
    "All 12 post-2000 Charter-table examples appear as project-bundle candidates.",
    "Rejected or unresolved cases must carry an explicit reason.",
    "No ULURP identifier is assigned to multiple seed bundles.",
    "ZAP matches are collapsed to one row per normalized ULURP key before joining.",
    "Dock Street is one project event with LU 1073-1075, C090181/C090183/C090184, local-member opposition, and Council approval.",
    "Broadway Triangle is one project event with LU 1227-1230, C090413/N090414/C090415/C090416, and the 36-10-4 split vote.",
    "Maspeth High School remains in the candidate universe even though the SCQ identifier does not match current ZAP ULURP keys.",
    "Dock Street, Watchtower, and Jamaica are retained as residential/mixed-use using Council/Charter text rather than the ZAP-only housing flag.",
    "Pre-1998 years carry archival-pending coverage flags.",
    "Annual time series covers every year from the 1989 Charter reform through 2025."
  )
)

write_csv_if_changed(candidates, "../output/overrule_project_bundle_candidates.csv")
write_csv_if_changed(project_bundles, "../output/overrule_project_bundle_audited.csv")
write_csv_if_changed(action_crosswalk, "../output/overrule_action_crosswalk.csv")
write_csv_if_changed(time_series, "../output/overrule_time_series_year.csv")
write_csv_if_changed(residential_mixed_case_anatomy, "../output/overrule_residential_mixed_case_anatomy.csv")
write_csv_if_changed(candidate_qc, "../output/overrule_candidate_qc.csv")

if (any(!candidate_qc$passed)) {
  stop("Member-deference overrule candidate QC failed. See ../output/overrule_candidate_qc.csv")
}
