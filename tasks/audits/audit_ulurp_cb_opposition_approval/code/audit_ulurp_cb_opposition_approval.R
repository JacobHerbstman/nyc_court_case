# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_ulurp_cb_opposition_approval/code")
# break_year <- 2002
# transition_end_year <- 2009
# start_year <- 1975
# end_year <- 2025
# validation_cases_per_period <- 15

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

cli_args <- commandArgs(trailingOnly = TRUE)
if (length(cli_args) != 5) {
  stop("Expected BREAK_YEAR, TRANSITION_END_YEAR, START_YEAR, END_YEAR, and VALIDATION_CASES_PER_PERIOD.")
}

break_year <- suppressWarnings(as.integer(cli_args[[1]]))
transition_end_year <- suppressWarnings(as.integer(cli_args[[2]]))
start_year <- suppressWarnings(as.integer(cli_args[[3]]))
end_year <- suppressWarnings(as.integer(cli_args[[4]]))
validation_cases_per_period <- suppressWarnings(as.integer(cli_args[[5]]))

if (
  any(is.na(c(
    break_year,
    transition_end_year,
    start_year,
    end_year,
    validation_cases_per_period
  ))) ||
  start_year >= break_year || transition_end_year < break_year ||
  end_year <= transition_end_year || validation_cases_per_period < 1
) {
  stop("Year arguments and validation sample size must be valid integers.")
}

extract_context <- function(text, pattern, context_chars = 220) {
  location <- str_locate(text, regex(pattern, ignore_case = TRUE))
  if (is.na(location[[1]])) {
    return(NA_character_)
  }
  str_squish(str_sub(
    text,
    max(1, location[[1]] - context_chars),
    min(str_length(text), location[[2]] + context_chars)
  ))
}

extract_cpc_decision_block <- function(text, resolution_page) {
  if (!is.na(resolution_page) && resolution_page >= 1L) {
    pages <- str_split(text, fixed("\f"))[[1]]
    decision_text <- paste(
      pages[resolution_page:min(length(pages), resolution_page + 10L)],
      collapse = "\n"
    )
  } else {
    action_headings <- str_locate_all(
      text,
      regex(
        "(?m)^\\s*(?:RESOLUTION|CITY PLANNING COMMISSION ACTION)\\s*$",
        ignore_case = TRUE
      )
    )[[1]]
    resolved_headings <- str_locate_all(
      text,
      regex("(?m)^\\s*RESOLVED(?:,|\\s+by\\b)", ignore_case = TRUE)
    )[[1]]
    late_action_headings <- action_headings[
      action_headings[, "start"] > 0.35 * str_length(text),
      "start"
    ]
    late_resolved_headings <- resolved_headings[
      resolved_headings[, "start"] > 0.35 * str_length(text),
      "start"
    ]
    decision_text <- if (length(late_action_headings) > 0) {
      str_sub(text, max(late_action_headings))
    } else if (length(late_resolved_headings) > 0) {
      str_sub(text, min(late_resolved_headings))
    } else {
      str_sub(text, max(1, floor(0.65 * str_length(text))))
    }
  }

  adopted_line <- str_locate(
    decision_text,
    regex(
      "\\bthe above resolution.{0,180}duly adopted by the (?:city planning )?commission\\b",
      ignore_case = TRUE
    )
  )
  if (!is.na(adopted_line[[1]])) {
    decision_text <- str_sub(
      decision_text,
      1,
      min(str_length(decision_text), adopted_line[[2]] + 1000)
    )
  } else {
    decision_text <- str_sub(decision_text, 1, min(str_length(decision_text), 15000))
  }
  decision_text
}

classify_cpc_decision <- function(decision_text) {
  disapproved <- str_detect(
    decision_text,
    regex(
      paste0(
        "\\b(?:be(?: and)?(?: the same)?(?: hereby)?(?: is)?|is|are) (?:hereby )?(?:disapproved|denied)\\b|",
        "\\b(?:city planning commission|the commission) (?:therefore |hereby )?(?:disapproves|denies|rejects)\\b|",
        "\\b(?:application|proposal|project|amendment|special permit|disposition).{0,180}\\b(?:is|be) (?:hereby )?(?:disapproved|denied)\\b|",
        "\\b(?:application|proposal|project|amendment|special permit|disposition) does not warrant approval\\b"
      ),
      ignore_case = TRUE
    )
  )
  approved <- str_detect(
    decision_text,
    regex(
      paste0(
        "\\b(?:be(?: and)?(?: the same)?(?: hereby)?(?: is)?|is|are) (?:hereby )?approved\\b|",
        "\\b(?:city planning commission|the commission) (?:therefore |hereby )?approves\\b|",
        "\\b(?:application|proposal|project|amendment|special permit|disposition).{0,180}\\b(?:is|be) (?:hereby )?approved\\b"
      ),
      ignore_case = TRUE
    )
  )
  has_resolution <- str_detect(
    decision_text,
    regex(
      "(?m)^\\s*(?:RESOLUTION|RESOLVED(?:,|\\s+by\\b)|CITY PLANNING COMMISSION ACTION)",
      ignore_case = TRUE
    )
  )

  case_when(
    approved && disapproved ~ "mixed",
    disapproved ~ "disapproved",
    approved || has_resolution ~ "approved",
    TRUE ~ "unknown"
  )
}

text_labels <- read_csv(
  "../input/ulurp_cpc_text_labels.csv",
  col_types = cols(.default = col_character()),
  show_col_types = FALSE,
  na = c("", "NA")
) |>
  mutate(
    year = suppressWarnings(as.integer(year)),
    cb_opposition = suppressWarnings(as.integer(cb_opposition)),
    cb_support_votes = suppressWarnings(as.integer(cb_support_votes)),
    cb_opposition_votes = suppressWarnings(as.integer(cb_opposition_votes)),
    application_key = str_remove(str_remove_all(str_to_upper(application_number), "[^A-Z0-9]"), "^[CMN]")
  ) |>
  filter(year >= start_year, year <= end_year)

if (
  nrow(text_labels) == 0 || anyDuplicated(text_labels$document_id) ||
  any(is.na(text_labels$cb_opposition)) || any(!text_labels$cb_opposition %in% c(0L, 1L))
) {
  stop("Text labels must be unique by document and have complete binary CB opposition labels.")
}

manual_cb_opposition <- read_csv(
  "../input/ulurp_cpc_training_labels_jacob.csv",
  col_types = cols(.default = col_character()),
  show_col_types = FALSE,
  na = c("", "NA")
) |>
  transmute(
    document_id,
    coding_complete = suppressWarnings(as.integer(coding_complete)),
    manual_cb_support_votes = suppressWarnings(as.integer(cb_support_votes)),
    manual_cb_opposition_votes = suppressWarnings(as.integer(cb_opposition_votes))
  ) |>
  filter(
    coding_complete == 1L,
    !is.na(manual_cb_support_votes),
    !is.na(manual_cb_opposition_votes)
  ) |>
  mutate(
    manual_cb_opposition = as.integer(
      manual_cb_opposition_votes > manual_cb_support_votes
    )
  ) |>
  select(-coding_complete)

if (anyDuplicated(manual_cb_opposition$document_id)) {
  stop("Completed human CB labels must be unique by document_id.")
}

text_labels <- text_labels |>
  left_join(manual_cb_opposition, by = "document_id", relationship = "one-to-one") |>
  mutate(
    cb_coding_source = if_else(
      is.na(manual_cb_opposition),
      "text rule",
      "committed human vote coding"
    ),
    cb_opposition = coalesce(manual_cb_opposition, cb_opposition),
    cb_support_votes = coalesce(manual_cb_support_votes, cb_support_votes),
    cb_opposition_votes = coalesce(manual_cb_opposition_votes, cb_opposition_votes)
  ) |>
  select(
    -manual_cb_opposition,
    -manual_cb_support_votes,
    -manual_cb_opposition_votes
  )

manifest <- read_csv(
  "../input/ulurp_cpc_report_manifest.csv",
  col_types = cols(.default = col_character()),
  show_col_types = FALSE,
  na = c("", "NA")
) |>
  filter(source_usable == "TRUE", text_status == "text_extracted") |>
  transmute(
    application_number,
    official_pdf_url,
    local_text_path,
    main_report_resolution_page = suppressWarnings(as.integer(main_report_resolution_page))
  )

if (anyDuplicated(manifest$application_number)) {
  stop("Readable CPC manifest rows must be unique by application_number.")
}

documents <- text_labels |>
  left_join(manifest, by = "application_number", relationship = "many-to-one")

if (any(is.na(documents$local_text_path))) {
  stop("At least one analysis narrative is missing its full CPC source text.")
}

documents$full_text <- vapply(
  documents$local_text_path,
  function(path) read_file(file.path("../../../build_ulurp_cpc_report_corpus/code", path)),
  character(1)
)
documents$cpc_decision_block <- mapply(
  extract_cpc_decision_block,
  documents$full_text,
  documents$main_report_resolution_page,
  USE.NAMES = FALSE
)
documents$cpc_decision <- vapply(
  documents$cpc_decision_block,
  classify_cpc_decision,
  character(1)
)
documents$cpc_approved <- case_when(
  documents$cpc_decision == "approved" ~ 1L,
  documents$cpc_decision == "disapproved" ~ 0L,
  TRUE ~ NA_integer_
)
documents$cb_evidence <- vapply(
  documents$full_text,
  extract_context,
  character(1),
  pattern = "community board.{0,260}(?:recommend\\w* disapproval|disapprov\\w*|oppos\\w*)|(?:recommend\\w* disapproval|disapprov\\w*|oppos\\w*).{0,260}community board|(?:vote|voted|by a vote of).{0,120}(?:against|opposed|disapprov\\w*)"
)
documents$cpc_decision_evidence <- vapply(
  documents$cpc_decision_block,
  extract_context,
  character(1),
  pattern = "(?:be(?: and)?(?: the same)?(?: hereby)?(?: is)?|is|are) (?:hereby )?(?:approved|disapproved)|(?:city planning commission|the commission) (?:therefore |hereby )?(?:approves|disapproves|denies|rejects)|RESOLVED, by the City Planning Commission"
)
documents$full_text <- NULL
documents$cpc_decision_block <- NULL

community_district_corrections <- read_csv(
  "../input/ulurp_cpc_community_district_corrections.csv",
  col_types = cols(.default = col_character()),
  show_col_types = FALSE,
  na = c("", "NA")
)
if (anyDuplicated(community_district_corrections$application_number)) {
  stop("Community-district corrections must be unique by application_number.")
}

documents <- documents |>
  rename(source_official_community_district = community_district) |>
  left_join(
    community_district_corrections,
    by = "application_number",
    relationship = "many-to-one"
  ) |>
  mutate(official_community_district = coalesce(
    corrected_community_district,
    source_official_community_district
  ))

district_treatment <- read_csv(
  "../input/cd_homeownership_1990_measure.csv",
  col_types = cols(.default = col_character()),
  show_col_types = FALSE,
  na = c("", "NA")
) |>
  transmute(
    borocd = suppressWarnings(as.integer(borocd)),
    borough_code = suppressWarnings(as.integer(borough_code)),
    treat_pp = suppressWarnings(as.numeric(treat_pp))
  ) |>
  arrange(borough_code, treat_pp, borocd) |>
  group_by(borough_code) |>
  mutate(
    homeowner_tercile = ntile(treat_pp, 3),
    homeowner_tercile_label = c("Low homeowner", "Middle homeowner", "High homeowner")[homeowner_tercile],
    homeowner_median_group = ntile(treat_pp, 2),
    homeowner_median_label = c("Below-median homeowner", "Above-median homeowner")[homeowner_median_group]
  ) |>
  ungroup()

if (nrow(district_treatment) != 59 || anyDuplicated(district_treatment$borocd)) {
  stop("Homeowner treatment must contain 59 unique community districts.")
}

project_bbl <- read_parquet(
  "../input/zap_project_bbl.parquet",
  col_select = c("project_id", "bbl_standardized")
) |>
  as.data.frame() |>
  as_tibble() |>
  transmute(
    project_id = str_squish(as.character(project_id)),
    bbl_standardized = str_squish(as.character(bbl_standardized))
  ) |>
  filter(project_id != "", bbl_standardized != "") |>
  distinct()

mappluto_lots <- read_parquet(
  "../input/mappluto_current_lot_lookup.parquet",
  col_select = c("bbl", "cd", "is_joint_interest_area")
) |>
  as.data.frame() |>
  as_tibble() |>
  transmute(
    bbl_standardized = str_squish(as.character(bbl)),
    borocd = suppressWarnings(as.integer(cd)),
    is_joint_interest_area = coalesce(as.logical(is_joint_interest_area), FALSE)
  ) |>
  filter(
    !is_joint_interest_area,
    borocd %in% district_treatment$borocd,
    bbl_standardized != ""
  ) |>
  select(bbl_standardized, borocd)

if (anyDuplicated(mappluto_lots$bbl_standardized)) {
  stop("Current MapPLUTO input must be unique by BBL.")
}

report_projects <- documents |>
  select(document_id, zap_project_ids) |>
  filter(!is.na(zap_project_ids)) |>
  separate_rows(zap_project_ids, sep = ";\\s*") |>
  transmute(document_id, project_id = str_squish(zap_project_ids)) |>
  filter(project_id != "") |>
  distinct()

bbl_index <- split(project_bbl$bbl_standardized, project_bbl$project_id)
document_bbl <- bind_rows(lapply(seq_len(nrow(report_projects)), function(i) {
  matched_bbl <- bbl_index[[report_projects$project_id[[i]]]]
  if (is.null(matched_bbl)) {
    return(NULL)
  }
  tibble(
    document_id = report_projects$document_id[[i]],
    bbl_standardized = matched_bbl
  )
})) |>
  distinct(document_id, bbl_standardized)

bbl_assignment <- document_bbl |>
  inner_join(mappluto_lots, by = "bbl_standardized", relationship = "many-to-one") |>
  distinct(document_id, bbl_standardized, borocd) |>
  count(document_id, borocd, name = "assigned_bbl_count") |>
  group_by(document_id) |>
  mutate(assignment_weight = assigned_bbl_count / sum(assigned_bbl_count)) |>
  ungroup() |>
  select(document_id, borocd, assignment_weight)

fallback_assignment <- documents |>
  anti_join(distinct(bbl_assignment, document_id), by = "document_id") |>
  transmute(
    document_id,
    community_district_token = str_extract_all(
      str_to_upper(coalesce(official_community_district, "")),
      "(?:MN|BX|BK|QN|SI)\\s*[0-9]{1,2}"
    )
  ) |>
  unnest_longer(community_district_token) |>
  mutate(
    borough_code = case_when(
      str_starts(community_district_token, "MN") ~ 1L,
      str_starts(community_district_token, "BX") ~ 2L,
      str_starts(community_district_token, "BK") ~ 3L,
      str_starts(community_district_token, "QN") ~ 4L,
      str_starts(community_district_token, "SI") ~ 5L,
      TRUE ~ NA_integer_
    ),
    borocd = borough_code * 100L + suppressWarnings(as.integer(str_extract(
      community_district_token,
      "[0-9]{1,2}"
    )))
  ) |>
  filter(borocd %in% district_treatment$borocd) |>
  distinct(document_id, borocd) |>
  group_by(document_id) |>
  mutate(assignment_weight = 1 / n()) |>
  ungroup() |>
  select(document_id, borocd, assignment_weight)

assignment <- bind_rows(bbl_assignment, fallback_assignment) |>
  left_join(district_treatment, by = "borocd", relationship = "many-to-one")

if (
  n_distinct(assignment$document_id) / nrow(documents) < 0.99 ||
  any(is.na(assignment$homeowner_tercile))
) {
  stop("Community-district assignment coverage or homeowner-tercile assignment failed.")
}

weight_check <- assignment |>
  group_by(document_id) |>
  summarize(weight = sum(assignment_weight), .groups = "drop")
if (any(abs(weight_check$weight - 1) > 1e-8)) {
  stop("Community-district assignment weights must sum to one by narrative.")
}

council_panel <- read_csv(
  "../input/council_land_use_decision_panel.csv",
  col_types = cols(.default = col_character()),
  show_col_types = FALSE,
  na = c("", "NA")
)

withdrawal_application_keys <- council_panel |>
  filter(
    disposition_group == "filed_withdrawal_or_motion",
    !is.na(application_keys),
    !str_detect(application_keys, ";")
  ) |>
  pull(application_keys) |>
  unique()

council <- council_panel |>
  filter(
    str_detect(matter_file, "^LU\\s"),
    !is.na(application_keys),
    !str_detect(application_keys, ";")
  ) |>
  transmute(
    application_key = str_squish(application_keys),
    council_decision_date = suppressWarnings(as.Date(decision_date, format = "%m/%d/%Y")),
    council_decision_year = suppressWarnings(as.integer(format(council_decision_date, "%Y"))),
    council_matter_file = matter_file,
    council_disposition = if_else(
      application_key %in% withdrawal_application_keys &
        disposition_group %in% c(
          "filed_by_council_other",
          "filed_by_committee_or_subcommittee",
          "filed_other"
        ),
      "filed_withdrawal_or_motion",
      disposition_group
    ),
    council_outcome_priority = case_when(
      council_disposition %in% c("adopted", "disapproved") ~ 1L,
      council_disposition %in% c(
        "withdrawn",
        "filed_withdrawal_or_motion",
        "filed_by_council_other",
        "filed_by_committee_or_subcommittee"
      ) ~ 2L,
      council_disposition == "filed_end_of_session" ~ 4L,
      TRUE ~ 3L
    ),
    council_approved = case_when(
      council_disposition == "adopted" ~ 1L,
      council_disposition %in% c(
        "disapproved",
        "withdrawn",
        "filed_withdrawal_or_motion",
        "filed_by_council_other",
        "filed_by_committee_or_subcommittee"
      ) ~ 0L,
      TRUE ~ NA_integer_
    ),
    council_matter_url = matter_url
  ) |>
  filter(application_key != "") |>
  arrange(
    application_key,
    council_outcome_priority,
    desc(council_decision_date),
    council_matter_file
  ) |>
  group_by(application_key) |>
  slice_head(n = 1) |>
  ungroup() |>
  select(-council_outcome_priority)

documents <- documents |>
  left_join(council, by = "application_key", relationship = "many-to-one") |>
  mutate(
    cpc_period = if_else(year < break_year, "Pre-2002", "2002 onward"),
    council_period = if_else(
      council_decision_year < break_year,
      "Pre-2002",
      "2002 onward",
      missing = NA_character_
    ),
    all = TRUE,
    non_pp = analysis_non_pp_flag == "TRUE",
    zm_zr_zs = analysis_zm_zr_zs_flag == "TRUE"
  )

analysis_rows <- assignment |>
  select(document_id, homeowner_group_label = homeowner_tercile_label, assignment_weight) |>
  mutate(grouping = "Terciles") |>
  bind_rows(
    assignment |>
      select(document_id, homeowner_group_label = homeowner_median_label, assignment_weight) |>
      mutate(grouping = "Median split")
  ) |>
  bind_rows(
    tibble(
      document_id = documents$document_id,
      homeowner_group_label = "All community districts",
      assignment_weight = 1,
      grouping = "Citywide"
    )
  ) |>
  left_join(documents, by = "document_id", relationship = "many-to-one") |>
  pivot_longer(
    cols = c(all, non_pp, zm_zr_zs),
    names_to = "sample",
    values_to = "in_sample"
  ) |>
  filter(in_sample) |>
  mutate(sample = recode(
    sample,
    all = "All applications",
    non_pp = "Exclude PP",
    zm_zr_zs = "ZM/ZR/ZS only"
  ))

cpc_summary <- analysis_rows |>
  filter(cb_opposition == 1L) |>
  mutate(period = cpc_period) |>
  group_by(sample, grouping, homeowner_group_label, period) |>
  summarize(
    outcome_stage = "CPC decision",
    period_start_year = if_else(first(period) == "Pre-2002", start_year, break_year),
    period_end_year = if_else(first(period) == "Pre-2002", break_year - 1L, end_year),
    weighted_cb_opposed_applications = sum(assignment_weight * !is.na(cpc_approved)),
    weighted_approved_applications = sum(assignment_weight * coalesce(cpc_approved, 0L)),
    approval_share = weighted_approved_applications / weighted_cb_opposed_applications,
    .groups = "drop"
  )

council_summary <- analysis_rows |>
  filter(cb_opposition == 1L, council_decision_year >= 1998L) |>
  mutate(period = council_period) |>
  group_by(sample, grouping, homeowner_group_label, period) |>
  summarize(
    outcome_stage = "Final Council disposition",
    period_start_year = if_else(first(period) == "Pre-2002", 1998L, break_year),
    period_end_year = if_else(first(period) == "Pre-2002", break_year - 1L, end_year),
    weighted_cb_opposed_applications = sum(assignment_weight * !is.na(council_approved)),
    weighted_approved_applications = sum(assignment_weight * coalesce(council_approved, 0L)),
    approval_share = weighted_approved_applications / weighted_cb_opposed_applications,
    .groups = "drop"
  )

summary_table <- bind_rows(cpc_summary, council_summary) |>
  group_by(outcome_stage, sample, grouping, homeowner_group_label) |>
  mutate(
    pre_post_change = approval_share[period == "2002 onward"] - approval_share[period == "Pre-2002"]
  ) |>
  ungroup() |>
  arrange(outcome_stage, sample, grouping, homeowner_group_label, period_start_year)

write_csv(summary_table, "../output/ulurp_cb_opposition_approval_summary.csv", na = "")

transition_rows <- analysis_rows |>
  filter(
    cb_opposition == 1L,
    council_decision_year >= 1998L,
    grouping %in% c("Citywide", "Terciles")
  ) |>
  mutate(
    era = case_when(
      council_decision_year < break_year ~ paste0("Vallone (1998-", break_year - 1L, ")"),
      council_decision_year <= transition_end_year ~ paste0(
        "Transition (", break_year, "-", transition_end_year, ")"
      ),
      TRUE ~ paste0("Later (", transition_end_year + 1L, "-", end_year, ")")
    ),
    era_start_year = case_when(
      council_decision_year < break_year ~ 1998L,
      council_decision_year <= transition_end_year ~ break_year,
      TRUE ~ transition_end_year + 1L
    ),
    era_end_year = case_when(
      council_decision_year < break_year ~ break_year - 1L,
      council_decision_year <= transition_end_year ~ transition_end_year,
      TRUE ~ end_year
    )
  )

transition_annual <- transition_rows |>
  group_by(
    sample,
    grouping,
    homeowner_group_label,
    era,
    era_start_year,
    era_end_year,
    council_decision_year
  ) |>
  summarize(
    weighted_matched_applications = sum(assignment_weight),
    weighted_cb_opposed_applications = sum(
      assignment_weight * !is.na(council_approved)
    ),
    weighted_approved_applications = sum(
      assignment_weight * coalesce(council_approved, 0L)
    ),
    weighted_disapproved_applications = sum(
      assignment_weight * (council_disposition == "disapproved")
    ),
    weighted_filed_or_withdrawn_applications = sum(
      assignment_weight * (council_disposition %in% c(
        "withdrawn",
        "filed_withdrawal_or_motion",
        "filed_by_council_other",
        "filed_by_committee_or_subcommittee"
      ))
    ),
    weighted_end_session_unresolved_applications = sum(
      assignment_weight * (council_disposition == "filed_end_of_session")
    ),
    approval_share = weighted_approved_applications / weighted_cb_opposed_applications,
    disapproval_share = weighted_disapproved_applications / weighted_cb_opposed_applications,
    filed_or_withdrawn_share = weighted_filed_or_withdrawn_applications /
      weighted_cb_opposed_applications,
    .groups = "drop"
  ) |>
  arrange(sample, grouping, homeowner_group_label, council_decision_year) |>
  mutate(analysis_unit = "Application action", .before = sample)

project_transition <- documents |>
  filter(cb_opposition == 1L, council_decision_year >= 1998L) |>
  mutate(
    project_key = if_else(
      is.na(zap_project_ids) | str_squish(zap_project_ids) == "",
      paste0("application:", application_key),
      paste0("zap:", str_squish(zap_project_ids))
    ),
    `Exclude PP` = analysis_non_pp_flag == "TRUE",
    `ZM/ZR/ZS only` = analysis_zm_zr_zs_flag == "TRUE"
  ) |>
  pivot_longer(
    cols = c(`Exclude PP`, `ZM/ZR/ZS only`),
    names_to = "sample",
    values_to = "in_sample"
  ) |>
  filter(in_sample) |>
  group_by(sample, project_key) |>
  summarize(
    council_decision_year = if (all(is.na(council_decision_year))) {
      NA_integer_
    } else {
      max(council_decision_year, na.rm = TRUE)
    },
    project_approved = case_when(
      any(council_approved == 0L, na.rm = TRUE) ~ 0L,
      any(is.na(council_approved)) ~ NA_integer_,
      all(council_approved == 1L) ~ 1L,
      TRUE ~ NA_integer_
    ),
    project_disapproved = any(council_disposition == "disapproved", na.rm = TRUE),
    project_filed_or_withdrawn = any(council_disposition %in% c(
      "withdrawn",
      "filed_withdrawal_or_motion",
      "filed_by_council_other",
      "filed_by_committee_or_subcommittee"
    ), na.rm = TRUE),
    project_end_session_unresolved = any(
      council_disposition == "filed_end_of_session",
      na.rm = TRUE
    ) & is.na(project_approved),
    .groups = "drop"
  ) |>
  filter(!is.na(council_decision_year)) |>
  mutate(
    era = case_when(
      council_decision_year < break_year ~ paste0("Vallone (1998-", break_year - 1L, ")"),
      council_decision_year <= transition_end_year ~ paste0(
        "Transition (", break_year, "-", transition_end_year, ")"
      ),
      TRUE ~ paste0("Later (", transition_end_year + 1L, "-", end_year, ")")
    ),
    era_start_year = case_when(
      council_decision_year < break_year ~ 1998L,
      council_decision_year <= transition_end_year ~ break_year,
      TRUE ~ transition_end_year + 1L
    ),
    era_end_year = case_when(
      council_decision_year < break_year ~ break_year - 1L,
      council_decision_year <= transition_end_year ~ transition_end_year,
      TRUE ~ end_year
    )
  )

project_transition_annual <- project_transition |>
  group_by(sample, era, era_start_year, era_end_year, council_decision_year) |>
  summarize(
    weighted_matched_applications = n(),
    weighted_cb_opposed_applications = sum(!is.na(project_approved)),
    weighted_approved_applications = sum(coalesce(project_approved, 0L)),
    weighted_disapproved_applications = sum(project_disapproved),
    weighted_filed_or_withdrawn_applications = sum(project_filed_or_withdrawn),
    weighted_end_session_unresolved_applications = sum(
      project_end_session_unresolved
    ),
    approval_share = weighted_approved_applications / weighted_cb_opposed_applications,
    disapproval_share = weighted_disapproved_applications /
      weighted_cb_opposed_applications,
    filed_or_withdrawn_share = weighted_filed_or_withdrawn_applications /
      weighted_cb_opposed_applications,
    .groups = "drop"
  ) |>
  mutate(
    analysis_unit = "Project",
    grouping = "Citywide",
    homeowner_group_label = "All community districts",
    .before = sample
  )

transition_output <- bind_rows(transition_annual, project_transition_annual) |>
  arrange(analysis_unit, sample, grouping, homeowner_group_label, council_decision_year)

write_csv(transition_output, "../output/ulurp_cb_opposition_transition.csv", na = "")

transition_summary <- transition_annual |>
  group_by(
    sample,
    grouping,
    homeowner_group_label,
    era,
    era_start_year,
    era_end_year
  ) |>
  summarize(
    weighted_matched_applications = sum(weighted_matched_applications),
    weighted_cb_opposed_applications = sum(weighted_cb_opposed_applications),
    weighted_approved_applications = sum(weighted_approved_applications),
    weighted_disapproved_applications = sum(weighted_disapproved_applications),
    weighted_filed_or_withdrawn_applications = sum(
      weighted_filed_or_withdrawn_applications
    ),
    weighted_end_session_unresolved_applications = sum(
      weighted_end_session_unresolved_applications
    ),
    approval_share = weighted_approved_applications / weighted_cb_opposed_applications,
    disapproval_share = weighted_disapproved_applications /
      weighted_cb_opposed_applications,
    filed_or_withdrawn_share = weighted_filed_or_withdrawn_applications /
      weighted_cb_opposed_applications,
    .groups = "drop"
  )

annual_transition <- transition_annual |>
  filter(sample %in% c("All applications", "Exclude PP", "ZM/ZR/ZS only")) |>
  transmute(
    sample,
    grouping,
    homeowner_group_label,
    council_decision_year,
    weighted_applications = weighted_cb_opposed_applications,
    weighted_approved = weighted_approved_applications
  ) |>
  group_by(sample, grouping, homeowner_group_label) |>
  complete(
    council_decision_year = 1998L:end_year,
    fill = list(weighted_applications = 0, weighted_approved = 0)
  ) |>
  arrange(council_decision_year, .by_group = TRUE) |>
  mutate(
    annual_approval_share = if_else(
      weighted_applications > 0,
      weighted_approved / weighted_applications,
      NA_real_
    ),
    rolling_applications = weighted_applications +
      lag(weighted_applications, 1L, default = 0) +
      lag(weighted_applications, 2L, default = 0),
    rolling_approved = weighted_approved +
      lag(weighted_approved, 1L, default = 0) +
      lag(weighted_approved, 2L, default = 0),
    rolling_approval_share = if_else(
      rolling_applications > 0,
      rolling_approved / rolling_applications,
      NA_real_
    )
  ) |>
  ungroup()

pdf("../output/ulurp_cb_opposition_transition.pdf", width = 10, height = 7)
transition_zoom <- transition_output |>
  filter(
    analysis_unit == "Project",
    sample %in% c("Exclude PP", "ZM/ZR/ZS only"),
    grouping == "Citywide",
    council_decision_year >= 1998L,
    council_decision_year <= 2012L
  ) |>
  select(
    sample,
    council_decision_year,
    weighted_cb_opposed_applications,
    approval_share,
    disapproval_share,
    filed_or_withdrawn_share
  ) |>
  pivot_longer(
    cols = c(approval_share, disapproval_share, filed_or_withdrawn_share),
    names_to = "outcome",
    values_to = "share"
  ) |>
  mutate(
    sample = factor(sample, levels = c("Exclude PP", "ZM/ZR/ZS only")),
    outcome = recode(
      outcome,
      approval_share = "Adopted",
      disapproval_share = "Disapproved",
      filed_or_withdrawn_share = "Withdrawn or filed"
    )
  )

print(
  ggplot(
    transition_zoom,
    aes(x = council_decision_year, y = share, color = outcome)
  ) +
    annotate(
      "rect",
      xmin = 2002.5,
      xmax = 2005.5,
      ymin = -Inf,
      ymax = Inf,
      fill = "#DDDDDD",
      alpha = 0.35
    ) +
    geom_line(linewidth = 0.8) +
    geom_point(aes(size = weighted_cb_opposed_applications), alpha = 0.8) +
    geom_vline(xintercept = break_year, linetype = "dashed", color = "#555555") +
    facet_wrap(vars(sample), ncol = 1) +
    scale_x_continuous(breaks = seq(1998, 2012, 2)) +
    scale_y_continuous(
      limits = c(0, 1),
      breaks = seq(0, 1, 0.25),
      labels = scales::percent_format(accuracy = 1)
    ) +
    scale_color_manual(values = c(
      "Adopted" = "#2C6E9B",
      "Disapproved" = "#B33A3A",
      "Withdrawn or filed" = "#D07A20"
    )) +
    scale_size_continuous(range = c(2, 6)) +
    labs(
      title = "Council outcomes after Community Board opposition, 1998-2012",
      subtitle = "Shading marks the hypothesized 2003-2005 transition window",
      x = NULL,
      y = "Share of applications with verified outcomes",
      color = NULL,
      size = "Annual weighted N",
      caption = str_wrap(
        "End-of-session filings without another documented Council decision are unresolved and excluded from the rates.",
        width = 125
      )
    ) +
    theme_minimal(base_size = 11) +
    theme(
      legend.position = "bottom",
      panel.grid.minor = element_blank(),
      plot.margin = margin(8, 8, 18, 8)
    )
)

for (sample_name in c("All applications", "Exclude PP", "ZM/ZR/ZS only")) {
  transition_plot_data <- annual_transition |>
    filter(sample == sample_name) |>
    mutate(
      homeowner_group_label = factor(
        homeowner_group_label,
        levels = c(
          "All community districts",
          "Low homeowner",
          "Middle homeowner",
          "High homeowner"
        )
      )
    )
  print(
    ggplot(
      transition_plot_data,
      aes(x = council_decision_year, y = rolling_approval_share)
    ) +
      geom_point(
        aes(y = annual_approval_share, size = weighted_applications),
        color = "#999999",
        alpha = 0.55,
        na.rm = TRUE
      ) +
      geom_line(color = "#2C6E9B", linewidth = 0.8, na.rm = TRUE) +
      geom_vline(xintercept = break_year, linetype = "dashed", color = "#555555") +
      geom_vline(
        xintercept = transition_end_year + 1L,
        linetype = "dotted",
        color = "#555555"
      ) +
      facet_wrap(vars(homeowner_group_label), ncol = 2) +
      scale_x_continuous(breaks = seq(2000, end_year, 5)) +
      scale_y_continuous(
        limits = c(0, 1),
        breaks = seq(0, 1, 0.25),
        labels = scales::percent_format(accuracy = 1)
      ) +
      scale_size_continuous(range = c(1.5, 5)) +
      labs(
        title = "Final Council approval after Community Board opposition",
        subtitle = paste0(
          sample_name,
          "; blue line is a trailing three-year rate; dashed line marks ",
          break_year,
          " and dotted line marks ",
          transition_end_year + 1L
        ),
        x = NULL,
        y = "Share approved",
        size = "Annual weighted N",
        caption = str_wrap(
          "Gray points are annual rates. Homeowner terciles use 1990 community-district homeownership within borough; multi-district applications receive fractional weights.",
          width = 125
        )
      ) +
      theme_minimal(base_size = 11) +
      theme(
        legend.position = "bottom",
        panel.grid.minor = element_blank(),
        plot.margin = margin(8, 8, 18, 8)
      )
  )
}
dev.off()

plot_data <- summary_table |>
  filter(sample == "All applications")

pdf("../output/ulurp_cb_opposition_approval.pdf", width = 10, height = 7)
for (split in c("Terciles", "Median split")) {
for (stage in c("CPC decision", "Final Council disposition")) {
  group_levels <- if (split == "Terciles") {
    c("All community districts", "Low homeowner", "Middle homeowner", "High homeowner")
  } else {
    c("All community districts", "Below-median homeowner", "Above-median homeowner")
  }
  stage_data <- plot_data |>
    filter(outcome_stage == stage, grouping %in% c("Citywide", split)) |>
    mutate(
      homeowner_group_label = factor(homeowner_group_label, levels = group_levels),
      period = factor(period, levels = c("Pre-2002", "2002 onward"))
    )
  print(
    ggplot(
      stage_data,
      aes(x = homeowner_group_label, y = approval_share, fill = period)
    ) +
      geom_col(position = position_dodge(width = 0.75), width = 0.65) +
      geom_text(
        aes(label = paste0(
          scales::percent(approval_share, accuracy = 1),
          "\nN=", scales::number(weighted_cb_opposed_applications, accuracy = 0.1)
        )),
        position = position_dodge(width = 0.75),
        vjust = -0.25,
        size = 3.2
      ) +
      scale_fill_manual(values = c("Pre-2002" = "#4C78A8", "2002 onward" = "#E45756")) +
      scale_y_continuous(
        limits = c(0, 1.12),
        breaks = seq(0, 1, 0.2),
        labels = scales::percent_format(accuracy = 1)
      ) +
      labs(
        title = paste0(stage, ": approval after Community Board opposition"),
        subtitle = if_else(
          stage == "CPC decision",
          paste0(split, "; ", start_year, "-", break_year - 1L, " versus ", break_year, "-", end_year),
          paste0(split, "; matched Council records: 1998-", break_year - 1L, " versus ", break_year, "-", end_year)
        ),
        x = NULL,
        y = "Share approved",
        fill = NULL,
        caption = str_wrap(
          paste0(
            "N is weighted. ",
            if_else(split == "Terciles", "Terciles", "Median groups"),
            " use 1990 community-district homeownership within borough; multi-district applications receive fractional weights. Mixed or unknown CPC decisions are excluded."
          ),
          width = 125
        )
      ) +
      theme_minimal(base_size = 11) +
      theme(
        legend.position = "bottom",
        panel.grid.minor = element_blank(),
        axis.text.x = element_text(angle = 15, hjust = 1),
        plot.margin = margin(8, 8, 18, 8)
      )
  )
}
}
dev.off()

case_columns <- c(
  "document_id",
  "application_number",
  "project_name",
  "year",
  "action_code",
  "analysis_non_pp_flag",
  "analysis_zm_zr_zs_flag",
  "official_community_district",
  "cb_coding_source",
  "cb_support_votes",
  "cb_opposition_votes",
  "cpc_decision",
  "cpc_approved",
  "council_matter_file",
  "council_decision_year",
  "council_disposition",
  "council_approved",
  "official_pdf_url",
  "council_matter_url",
  "cb_evidence",
  "cpc_decision_evidence"
)

cpc_cases <- documents |>
  filter(cb_opposition == 1L) |>
  mutate(
    period = cpc_period,
    outcome_stage = "CPC decision",
    outcome_category = cpc_decision
  ) |>
  select(all_of(case_columns), period, outcome_stage, outcome_category)

council_cases <- documents |>
  filter(cb_opposition == 1L, !is.na(council_approved)) |>
  mutate(
    period = council_period,
    outcome_stage = "Final Council disposition",
    outcome_category = council_disposition
  ) |>
  select(all_of(case_columns), period, outcome_stage, outcome_category)

case_pool <- bind_rows(cpc_cases, council_cases) |>
  select(
    document_id,
    application_number,
    project_name,
    year,
    action_code,
    analysis_non_pp_flag,
    analysis_zm_zr_zs_flag,
    period,
    official_community_district,
    cb_coding_source,
    cb_support_votes,
    cb_opposition_votes,
    cpc_decision,
    cpc_approved,
    council_matter_file,
    council_decision_year,
    council_disposition,
    council_approved,
    official_pdf_url,
    council_matter_url,
    cb_evidence,
    cpc_decision_evidence,
    outcome_stage,
    outcome_category
  ) |>
  arrange(outcome_stage, period, outcome_category, year, document_id) |>
  group_by(outcome_stage, period, outcome_category) |>
  mutate(
    sample_rank = row_number(),
    keep = sample_rank %in% unique(round(seq(
      1,
      n(),
      length.out = min(validation_cases_per_period, n())
    )))
  ) |>
  ungroup() |>
  filter(keep) |>
  select(-sample_rank, -keep) |>
  mutate(review_reason = "period/outcome validation sample")

transition_cases <- documents |>
  filter(
    cb_opposition == 1L,
    analysis_non_pp_flag == "TRUE",
    council_decision_year >= 2003L,
    council_decision_year <= 2005L,
    !is.na(council_approved)
  ) |>
  mutate(
    period = "2003-2005",
    outcome_stage = "Final Council disposition",
    outcome_category = council_disposition,
    review_reason = "2003-2005 non-PP outcome census"
  ) |>
  select(all_of(case_columns), period, outcome_stage, outcome_category, review_reason)

bind_rows(transition_cases, case_pool) |>
  distinct(outcome_stage, document_id, .keep_all = TRUE) |>
  arrange(desc(review_reason), outcome_stage, council_decision_year, document_id) |>
  write_csv("../output/ulurp_cb_opposition_approval_cases.csv", na = "")
