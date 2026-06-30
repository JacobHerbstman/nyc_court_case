# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/investigate_council_land_use_split_votes/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tidyr)
})

theme_set(theme_minimal(base_size = 11))
recall_years <- 1998:2025
plot_year_breaks <- seq(1998, 2025, 3)

rolling_average_5 <- function(x) {
  vapply(
    seq_along(x),
    function(i) {
      if (i < 5L) {
        return(NA_real_)
      }
      mean(x[(i - 4L):i])
    },
    numeric(1)
  )
}

rolling_sum_5 <- function(x) {
  vapply(
    seq_along(x),
    function(i) {
      if (i < 5L) {
        return(NA_real_)
      }
      sum(x[(i - 4L):i])
    },
    numeric(1)
  )
}

rolling_sum_3 <- function(x) {
  vapply(
    seq_along(x),
    function(i) {
      if (i < 3L) {
        return(NA_real_)
      }
      sum(x[(i - 2L):i])
    },
    numeric(1)
  )
}

rolling_rate_5 <- function(numerator, denominator) {
  vapply(
    seq_along(numerator),
    function(i) {
      if (i < 5L) {
        return(NA_real_)
      }
      window_denominator <- sum(denominator[(i - 4L):i], na.rm = TRUE)
      if (window_denominator == 0L) {
        return(NA_real_)
      }
      sum(numerator[(i - 4L):i], na.rm = TRUE) / window_denominator
    },
    numeric(1)
  )
}

collapse_values <- function(x) {
  values <- unique(str_squish(as.character(x)))
  values <- values[!is.na(values) & values != ""]
  if (length(values) == 0L) {
    return(NA_character_)
  }
  str_c(values, collapse = "; ")
}

normalize_member_name <- function(x) {
  out <- iconv(coalesce(as.character(x), ""), to = "ASCII//TRANSLIT")
  out <- str_to_lower(coalesce(out, ""))
  out <- str_replace_all(out, "\\b(jr|sr|ii|iii|iv)\\b", " ")
  out <- str_replace_all(out, "[^a-z ]", " ")
  out <- str_replace_all(out, "\\b[a-z]\\b", " ")
  out <- str_squish(out)
  out[out == ""] <- NA_character_
  out
}

member_in_local_roster <- function(person_name, local_members_from_roster) {
  person_key <- normalize_member_name(person_name)
  vapply(
    seq_along(person_key),
    function(i) {
      if (is.na(person_key[i]) || is.na(local_members_from_roster[i]) ||
          str_squish(local_members_from_roster[i]) == "") {
        return(NA)
      }
      local_keys <- normalize_member_name(str_split(local_members_from_roster[i], "\\s*;\\s*")[[1]])
      local_keys <- local_keys[!is.na(local_keys)]
      if (length(local_keys) == 0L) {
        return(NA)
      }
      person_key[i] %in% local_keys
    },
    logical(1)
  )
}

count_local_member_vote_entries <- function(x) {
  vapply(
    str_split(coalesce(as.character(x), ""), "\\s*;\\s*"),
    function(values) {
      sum(str_detect(values, ":"))
    },
    integer(1)
  )
}

normalize_application_key <- function(x) {
  raw_value <- str_to_upper(str_replace_all(str_squish(as.character(x)), "[^A-Z0-9]", ""))
  raw_value[raw_value == ""] <- NA_character_
  str_replace(raw_value, "^[CNM](?=[0-9])", "")
}

classify_action_code <- function(action_code) {
  case_when(
    action_code %in% c("ZM", "ZR") ~ "Zoning map/text amendments",
    action_code == "ZS" ~ "Zoning special permits",
    action_code %in% c("ZA", "ZC", "ZJ") ~ "Zoning administrative actions",
    action_code %in% c("HA", "HD", "HG", "HC", "HL", "HM", "HO", "HP", "HU") ~
      "Housing/urban renewal",
    action_code %in% c("PP", "PQ", "PC", "PS", "PX") ~ "Public property/site selection",
    action_code == "MM" ~ "City map changes",
    action_code %in% c("HK", "HI") ~ "Landmarks/historic districts",
    action_code %in% c("TC", "EC") ~ "Sidewalk/enclosed cafes",
    action_code %in% c("RC", "RA") ~ "South Richmond actions",
    action_code %in% c("LD", "CM", "MD", "ME") ~ "Legal docs/modifications/renewals",
    is.na(action_code) ~ "No parsed application key",
    TRUE ~ "Other coded actions"
  )
}

period_from_query_year <- function(query_year) {
  case_when(
    query_year >= 1998L & query_year <= 2002L ~ "1998-2002",
    query_year >= 2003L & query_year <= 2009L ~ "2003-2009",
    query_year >= 2010L & query_year <= 2017L ~ "2010-2017",
    query_year >= 2018L & query_year <= 2025L ~ "2018-2025",
    TRUE ~ NA_character_
  )
}

action_category_priority <- c(
  "Zoning map/text amendments" = 1,
  "Zoning special permits" = 2,
  "City map changes" = 3,
  "Housing/urban renewal" = 4,
  "Public property/site selection" = 5,
  "Landmarks/historic districts" = 6,
  "Zoning administrative actions" = 7,
  "Sidewalk/enclosed cafes" = 8,
  "South Richmond actions" = 9,
  "Legal docs/modifications/renewals" = 10,
  "Other coded actions" = 11,
  "No parsed application key" = 12
)

count_council_districts <- function(x) {
  vapply(
    str_extract_all(coalesce(as.character(x), ""), "\\d{1,2}"),
    function(values) {
      districts <- suppressWarnings(as.integer(values))
      districts <- districts[!is.na(districts) & districts >= 1L & districts <= 51L]
      length(unique(districts))
    },
    integer(1)
  )
}

decision <- read_csv(
  "../input/council_land_use_decision_panel.csv",
  col_types = cols(.default = col_character()),
  na = character()
)
validation <- read_csv(
  "../input/council_land_use_decision_universe_validation_summary.csv",
  col_types = cols(.default = col_character()),
  na = character()
)
seed_cases <- read_csv(
  "../input/charter_overrule_seed_cases.csv",
  col_types = cols(.default = col_character()),
  na = character()
)

if (any(str_to_lower(validation$passed) != "true")) {
  stop("Council land-use decision universe validation did not pass.")
}

if (nrow(decision) != n_distinct(decision$matter_id)) {
  stop("Council land-use decision panel must be unique by matter_id.")
}

decision <- decision |>
  mutate(
    query_year = suppressWarnings(as.integer(query_year)),
    matter_in_main_vote_sample = str_to_lower(matter_in_main_vote_sample) == "true",
    has_affected_council_district = str_to_lower(has_affected_council_district) == "true",
    has_local_member_from_roster = str_to_lower(has_local_member_from_roster) == "true",
    has_local_member_vote_observed = str_to_lower(has_local_member_vote_observed) == "true",
    affirmative_count = suppressWarnings(as.integer(affirmative_count)),
    negative_count = suppressWarnings(as.integer(negative_count)),
    abstain_count = suppressWarnings(as.integer(abstain_count)),
    negative_count = coalesce(negative_count, 0L),
    abstain_count = coalesce(abstain_count, 0L),
    dissent_count = negative_count + abstain_count,
    split_final_action_vote = matter_in_main_vote_sample & dissent_count > 0L,
    vote_source_group = case_when(
      vote_source %in% c("approval_action_detail", "approval_action_detail_nonfinal_disposition") ~ "approval_action_detail",
      vote_source == "nonapproval_action_detail" ~ "nonapproval_action_detail",
      TRUE ~ "not_in_vote_detail_sample"
    ),
    total_voting_count = affirmative_count + negative_count + abstain_count,
    local_member_voting_count = count_local_member_vote_entries(local_member_final_action_votes),
    simple_majority_approval_margin = affirmative_count - 26L,
    dissent_type = case_when(
      negative_count > 0L & abstain_count > 0L ~ "negative_and_abstain",
      negative_count > 0L ~ "negative_only",
      abstain_count > 0L ~ "abstain_only",
      TRUE ~ "unanimous_or_unparsed"
    ),
    dissent_size_group = case_when(
      dissent_count == 0L ~ "0",
      dissent_count == 1L ~ "1",
      dissent_count <= 4L ~ "2-4",
      dissent_count <= 9L ~ "5-9",
      TRUE ~ "10+"
    ),
    local_member_final_action_vote_status = if_else(
      local_member_final_action_vote_status == "",
      "unresolved_missing_status",
      local_member_final_action_vote_status
    ),
    approval_local_member_negative_or_abstain = vote_source_group == "approval_action_detail" &
      local_member_final_action_vote_status == "local_member_negative_or_abstain",
    nonapproval_local_member_negative_or_abstain = vote_source_group == "nonapproval_action_detail" &
      local_member_final_action_vote_status == "local_member_negative_or_abstain"
  )

action_key_rows <- decision |>
  transmute(
    matter_id,
    application_key = str_split(coalesce(as.character(application_keys), ""), "\\s*;\\s*")
  ) |>
  unnest_longer(application_key, keep_empty = TRUE) |>
  mutate(
    application_key = normalize_application_key(application_key),
    action_code = if_else(
      str_detect(application_key, "^[0-9]{6,7}A?[A-Z]{3}$"),
      str_sub(application_key, -3, -2),
      NA_character_
    ),
    primary_action_category = classify_action_code(action_code),
    category_priority = unname(action_category_priority[primary_action_category])
  ) |>
  group_by(matter_id) |>
  arrange(category_priority, .by_group = TRUE) |>
  summarise(
    primary_action_category = first(primary_action_category),
    parsed_application_key_count = sum(!is.na(action_code)),
    distinct_action_category_count = n_distinct(primary_action_category),
    .groups = "drop"
  )

if (nrow(action_key_rows) != n_distinct(action_key_rows$matter_id)) {
  stop("Council action category rows must be unique by matter_id.")
}

decision <- decision |>
  left_join(action_key_rows, by = "matter_id", relationship = "one-to-one")

split_votes <- decision |>
  filter(split_final_action_vote)

all_roll_call_signatures <- decision |>
  filter(matter_in_main_vote_sample) |>
  mutate(
    roll_call_signature = str_c(
      query_year,
      vote_source_group,
      vote_date,
      vote_margin,
      affirmative_count,
      negative_count,
      abstain_count,
      sep = " | "
    )
  ) |>
  group_by(
    query_year,
    roll_call_signature,
    vote_source_group,
    vote_date,
    vote_margin,
    affirmative_count,
    negative_count,
    abstain_count,
    dissent_count
  ) |>
  summarise(
    matter_rows = n(),
    land_use_application_rows = sum(query_matter_type == "Land Use Application"),
    resolution_rows = sum(query_matter_type == "Resolution"),
    call_up_rows = sum(query_matter_type == "Land Use Call-Up"),
    affected_district_rows = sum(has_affected_council_district),
    local_member_vote_rows = sum(has_local_member_vote_observed),
    approval_local_member_negative_or_abstain_rows = sum(approval_local_member_negative_or_abstain),
    nonapproval_local_member_negative_or_abstain_rows = sum(nonapproval_local_member_negative_or_abstain),
    matter_files = collapse_values(matter_file),
    application_keys = collapse_values(application_keys),
    zap_project_ids = collapse_values(zap_project_ids),
    zap_project_names = collapse_values(zap_project_names),
    affected_council_districts = collapse_values(affected_council_districts),
    local_members_from_roster = collapse_values(local_members_from_roster),
    local_member_final_action_vote_statuses = collapse_values(local_member_final_action_vote_status),
    local_member_final_action_votes = collapse_values(local_member_final_action_votes),
    title_examples = collapse_values(head(unique(title), 3)),
    matter_urls = collapse_values(matter_url),
    history_detail_urls = collapse_values(history_detail_url),
    .groups = "drop"
  ) |>
  mutate(
    affected_council_district_count = count_council_districts(affected_council_districts)
  ) |>
  arrange(query_year, vote_source_group, vote_date, vote_margin)

missing_geography_roll_call_repair_queue <- all_roll_call_signatures |>
  filter(affected_district_rows == 0L) |>
  mutate(
    split_final_action_signature = dissent_count > 0L,
    approval_signature = vote_source_group == "approval_action_detail",
    probable_non_project_false_positive =
      str_detect(str_to_lower(coalesce(title_examples, "")), "rules, privileges and elections|appointment|reappointment") &
        coalesce(application_keys, "") == "" &
        coalesce(zap_project_ids, "") == "",
    repair_priority = case_when(
      probable_non_project_false_positive ~ 9L,
      split_final_action_signature & query_year <= 2009L ~ 1L,
      split_final_action_signature ~ 2L,
      matter_rows >= 5L & query_year <= 2009L ~ 3L,
      matter_rows >= 5L ~ 4L,
      TRUE ~ 5L
    ),
    repair_priority_reason = case_when(
      repair_priority == 1L ~ "pre_2010_split_vote",
      repair_priority == 2L ~ "split_vote",
      repair_priority == 3L ~ "pre_2010_large_bundle",
      repair_priority == 4L ~ "large_bundle",
      repair_priority == 9L ~ "probable_non_project_false_positive",
      TRUE ~ "other_missing_geography"
    )
  ) |>
  select(
    repair_priority,
    repair_priority_reason,
    probable_non_project_false_positive,
    query_year,
    vote_date,
    vote_source_group,
    vote_margin,
    affirmative_count,
    negative_count,
    abstain_count,
    dissent_count,
    split_final_action_signature,
    approval_signature,
    matter_rows,
    land_use_application_rows,
    resolution_rows,
    call_up_rows,
    local_member_vote_rows,
    matter_files,
    application_keys,
    zap_project_ids,
    zap_project_names,
    title_examples,
    matter_urls,
    history_detail_urls,
    roll_call_signature
  ) |>
  arrange(
    repair_priority,
    query_year,
    desc(dissent_count),
    desc(matter_rows),
    vote_date,
    vote_margin
  )

annual <- decision |>
  group_by(query_year) |>
  summarise(
    matter_rows = n(),
    main_vote_sample_rows = sum(matter_in_main_vote_sample),
    split_vote_rows = sum(split_final_action_vote),
    approval_split_vote_rows = sum(split_final_action_vote & vote_source_group == "approval_action_detail"),
    nonapproval_split_vote_rows = sum(split_final_action_vote & vote_source_group == "nonapproval_action_detail"),
    one_dissent_split_vote_rows = sum(split_final_action_vote & dissent_count == 1L),
    multi_dissent_split_vote_rows = sum(split_final_action_vote & dissent_count > 1L),
    negative_only_split_vote_rows = sum(split_final_action_vote & dissent_type == "negative_only"),
    abstain_only_split_vote_rows = sum(split_final_action_vote & dissent_type == "abstain_only"),
    negative_and_abstain_split_vote_rows = sum(split_final_action_vote & dissent_type == "negative_and_abstain"),
    split_vote_rows_with_affected_district = sum(split_final_action_vote & has_affected_council_district),
    split_vote_rows_with_local_member_vote = sum(split_final_action_vote & has_local_member_vote_observed),
    approval_split_local_member_negative_or_abstain_rows = sum(
      split_final_action_vote & approval_local_member_negative_or_abstain
    ),
    nonapproval_split_local_member_negative_or_abstain_rows = sum(
      split_final_action_vote & nonapproval_local_member_negative_or_abstain
    ),
    .groups = "drop"
  ) |>
  mutate(
    split_vote_share_of_vote_sample = split_vote_rows / main_vote_sample_rows,
    approval_split_share_of_split_votes = approval_split_vote_rows / split_vote_rows,
    multi_dissent_share_of_split_votes = multi_dissent_split_vote_rows / split_vote_rows,
    affected_district_share_of_split_votes = split_vote_rows_with_affected_district / split_vote_rows,
    local_member_vote_share_of_split_votes = split_vote_rows_with_local_member_vote / split_vote_rows
  ) |>
  arrange(query_year)

dissent_size_year <- split_votes |>
  group_by(query_year, vote_source_group, dissent_size_group, dissent_type) |>
  summarise(split_vote_rows = n(), .groups = "drop") |>
  group_by(query_year) |>
  mutate(split_vote_share = split_vote_rows / sum(split_vote_rows)) |>
  ungroup() |>
  arrange(query_year, vote_source_group, dissent_size_group, dissent_type)

local_member_alignment_year <- split_votes |>
  group_by(query_year, vote_source_group, local_member_final_action_vote_status) |>
  summarise(split_vote_rows = n(), .groups = "drop") |>
  left_join(
    split_votes |>
      group_by(query_year, vote_source_group) |>
      summarise(source_year_split_vote_rows = n(), .groups = "drop"),
    by = c("query_year", "vote_source_group"),
    relationship = "many-to-one"
  ) |>
  mutate(split_vote_source_year_share = split_vote_rows / source_year_split_vote_rows) |>
  arrange(query_year, vote_source_group, local_member_final_action_vote_status)

matter_type_year <- split_votes |>
  group_by(query_year, query_matter_type, matter_type, disposition_group, vote_source_group) |>
  summarise(
    split_vote_rows = n(),
    mean_dissent_count = mean(dissent_count),
    max_dissent_count = max(dissent_count),
    .groups = "drop"
  ) |>
  arrange(query_year, desc(split_vote_rows), query_matter_type, matter_type, disposition_group)

roll_call_signatures <- all_roll_call_signatures |>
  filter(dissent_count > 0L) |>
  arrange(query_year, desc(matter_rows), vote_source_group, vote_margin)

roll_call_signature_year <- roll_call_signatures |>
  group_by(query_year) |>
  summarise(
    split_matter_rows = sum(matter_rows),
    split_roll_call_signature_rows = n(),
    mean_matter_rows_per_signature = mean(matter_rows),
    max_matter_rows_per_signature = max(matter_rows),
    multi_matter_signature_rows = sum(matter_rows > 1L),
    matter_rows_in_multi_matter_signatures = sum(matter_rows[matter_rows > 1L]),
    .groups = "drop"
  ) |>
  mutate(
    matter_share_in_multi_matter_signatures =
      matter_rows_in_multi_matter_signatures / split_matter_rows
  ) |>
  arrange(query_year)

top_roll_call_signatures <- roll_call_signatures |>
  arrange(desc(matter_rows), query_year, vote_margin) |>
  slice_head(n = 100)

top_matter_examples <- split_votes |>
  arrange(desc(dissent_count), query_year, matter_file) |>
  select(
    query_year,
    matter_id,
    matter_file,
    query_matter_type,
    matter_type,
    disposition_group,
    vote_source_group,
    vote_date,
    decision_action,
    decision_result,
    vote_margin,
    affirmative_count,
    negative_count,
    abstain_count,
    dissent_count,
    dissent_type,
    affected_council_districts,
    affected_district_source,
    local_members_from_roster,
    local_member_final_action_vote_status,
    local_member_final_action_votes,
    zap_project_ids,
    zap_project_names,
    title,
    matter_url,
    history_detail_url
  ) |>
  slice_head(n = 150)

local_member_no_abstain_signature_year <- roll_call_signatures |>
  mutate(
    approval_local_member_negative_or_abstain_signature =
      approval_local_member_negative_or_abstain_rows > 0L,
    nonapproval_local_member_negative_or_abstain_signature =
      nonapproval_local_member_negative_or_abstain_rows > 0L
  ) |>
  group_by(query_year) |>
  summarise(
    split_roll_call_signature_rows = n(),
    approval_local_member_negative_or_abstain_signature_rows = sum(
      approval_local_member_negative_or_abstain_signature
    ),
    nonapproval_local_member_negative_or_abstain_signature_rows = sum(
      nonapproval_local_member_negative_or_abstain_signature
    ),
    approval_local_member_negative_or_abstain_matter_rows = sum(
      approval_local_member_negative_or_abstain_rows
    ),
    nonapproval_local_member_negative_or_abstain_matter_rows = sum(
      nonapproval_local_member_negative_or_abstain_rows
    ),
    .groups = "drop"
  ) |>
  arrange(query_year)

local_member_no_abstain_denominator_year <- all_roll_call_signatures |>
  mutate(
    local_member_observed_signature = local_member_vote_rows > 0L,
    approval_signature = vote_source_group == "approval_action_detail",
    nonapproval_signature = vote_source_group == "nonapproval_action_detail",
    approval_local_member_negative_or_abstain_signature =
      approval_local_member_negative_or_abstain_rows > 0L,
    nonapproval_local_member_negative_or_abstain_signature =
      nonapproval_local_member_negative_or_abstain_rows > 0L
  ) |>
  group_by(query_year) |>
  summarise(
    final_action_signature_rows = n(),
    local_member_observed_signature_rows = sum(local_member_observed_signature),
    approval_local_member_observed_signature_rows = sum(
      approval_signature & local_member_observed_signature
    ),
    nonapproval_local_member_observed_signature_rows = sum(
      nonapproval_signature & local_member_observed_signature
    ),
    approval_local_member_negative_or_abstain_signature_rows = sum(
      approval_local_member_negative_or_abstain_signature
    ),
    nonapproval_local_member_negative_or_abstain_signature_rows = sum(
      nonapproval_local_member_negative_or_abstain_signature
    ),
    .groups = "drop"
  ) |>
  mutate(
    approval_local_member_negative_or_abstain_signature_share = if_else(
      approval_local_member_observed_signature_rows > 0L,
      approval_local_member_negative_or_abstain_signature_rows /
        approval_local_member_observed_signature_rows,
      NA_real_
    ),
    nonapproval_local_member_negative_or_abstain_signature_share = if_else(
      nonapproval_local_member_observed_signature_rows > 0L,
      nonapproval_local_member_negative_or_abstain_signature_rows /
        nonapproval_local_member_observed_signature_rows,
      NA_real_
    )
  ) |>
  arrange(query_year)

multi_district_signature_year <- all_roll_call_signatures |>
  mutate(
    local_member_observed_signature = local_member_vote_rows > 0L,
    approval_signature = vote_source_group == "approval_action_detail",
    multi_district_signature = affected_council_district_count > 1L,
    single_district_signature = affected_council_district_count == 1L,
    approval_local_member_negative_or_abstain_signature =
      approval_local_member_negative_or_abstain_rows > 0L
  ) |>
  group_by(query_year) |>
  summarise(
    final_action_signature_rows = n(),
    local_member_observed_signature_rows = sum(local_member_observed_signature),
    multi_district_observed_signature_rows = sum(
      local_member_observed_signature & multi_district_signature
    ),
    approval_local_member_observed_signature_rows = sum(
      approval_signature & local_member_observed_signature
    ),
    approval_single_district_observed_signature_rows = sum(
      approval_signature & local_member_observed_signature & single_district_signature
    ),
    approval_multi_district_observed_signature_rows = sum(
      approval_signature & local_member_observed_signature & multi_district_signature
    ),
    approval_single_district_local_no_abstain_signature_rows = sum(
      approval_signature & single_district_signature &
        approval_local_member_negative_or_abstain_signature
    ),
    approval_multi_district_local_no_abstain_signature_rows = sum(
      approval_signature & multi_district_signature &
        approval_local_member_negative_or_abstain_signature
    ),
    .groups = "drop"
  ) |>
  mutate(
    multi_district_observed_signature_share = if_else(
      local_member_observed_signature_rows > 0L,
      multi_district_observed_signature_rows / local_member_observed_signature_rows,
      NA_real_
    ),
    approval_multi_district_observed_signature_share = if_else(
      approval_local_member_observed_signature_rows > 0L,
      approval_multi_district_observed_signature_rows /
        approval_local_member_observed_signature_rows,
      NA_real_
    ),
    approval_single_district_local_no_abstain_signature_share = if_else(
      approval_single_district_observed_signature_rows > 0L,
      approval_single_district_local_no_abstain_signature_rows /
        approval_single_district_observed_signature_rows,
      NA_real_
    ),
    approval_multi_district_local_no_abstain_signature_share = if_else(
      approval_multi_district_observed_signature_rows > 0L,
      approval_multi_district_local_no_abstain_signature_rows /
        approval_multi_district_observed_signature_rows,
      NA_real_
    )
  ) |>
  arrange(query_year)

local_member_no_abstain_rate_rolling_year <- local_member_no_abstain_denominator_year |>
  arrange(query_year) |>
  mutate(
    approval_local_member_negative_or_abstain_signature_rows_rolling_5 =
      rolling_sum_5(approval_local_member_negative_or_abstain_signature_rows),
    approval_local_member_observed_signature_rows_rolling_5 =
      rolling_sum_5(approval_local_member_observed_signature_rows),
    nonapproval_local_member_negative_or_abstain_signature_rows_rolling_5 =
      rolling_sum_5(nonapproval_local_member_negative_or_abstain_signature_rows),
    nonapproval_local_member_observed_signature_rows_rolling_5 =
      rolling_sum_5(nonapproval_local_member_observed_signature_rows),
    approval_local_member_negative_or_abstain_signature_share_rolling_5 =
      approval_local_member_negative_or_abstain_signature_rows_rolling_5 /
        approval_local_member_observed_signature_rows_rolling_5,
    nonapproval_local_member_negative_or_abstain_signature_share_rolling_5 =
      nonapproval_local_member_negative_or_abstain_signature_rows_rolling_5 /
        nonapproval_local_member_observed_signature_rows_rolling_5
  )

multi_district_signature_rolling_year <- multi_district_signature_year |>
  arrange(query_year) |>
  mutate(
    local_member_observed_signature_rows_rolling_5 =
      rolling_sum_5(local_member_observed_signature_rows),
    multi_district_observed_signature_rows_rolling_5 =
      rolling_sum_5(multi_district_observed_signature_rows),
    approval_local_member_observed_signature_rows_rolling_5 =
      rolling_sum_5(approval_local_member_observed_signature_rows),
    approval_multi_district_observed_signature_rows_rolling_5 =
      rolling_sum_5(approval_multi_district_observed_signature_rows),
    approval_single_district_observed_signature_rows_rolling_5 =
      rolling_sum_5(approval_single_district_observed_signature_rows),
    approval_single_district_local_no_abstain_signature_rows_rolling_5 =
      rolling_sum_5(approval_single_district_local_no_abstain_signature_rows),
    approval_multi_district_local_no_abstain_signature_rows_rolling_5 =
      rolling_sum_5(approval_multi_district_local_no_abstain_signature_rows),
    multi_district_observed_signature_share_rolling_5 = if_else(
      local_member_observed_signature_rows_rolling_5 > 0L,
      multi_district_observed_signature_rows_rolling_5 /
        local_member_observed_signature_rows_rolling_5,
      NA_real_
    ),
    approval_multi_district_observed_signature_share_rolling_5 = if_else(
      approval_local_member_observed_signature_rows_rolling_5 > 0L,
      approval_multi_district_observed_signature_rows_rolling_5 /
        approval_local_member_observed_signature_rows_rolling_5,
      NA_real_
    ),
    approval_single_district_local_no_abstain_signature_share_rolling_5 = if_else(
      approval_single_district_observed_signature_rows_rolling_5 > 0L,
      approval_single_district_local_no_abstain_signature_rows_rolling_5 /
        approval_single_district_observed_signature_rows_rolling_5,
      NA_real_
    ),
    approval_multi_district_local_no_abstain_signature_share_rolling_5 = if_else(
      approval_multi_district_observed_signature_rows_rolling_5 > 0L,
      approval_multi_district_local_no_abstain_signature_rows_rolling_5 /
        approval_multi_district_observed_signature_rows_rolling_5,
      NA_real_
    )
  )

local_member_no_abstain_rate_rolling_3_year <- local_member_no_abstain_denominator_year |>
  arrange(query_year) |>
  mutate(
    approval_local_member_negative_or_abstain_signature_rows_rolling_3 =
      rolling_sum_3(approval_local_member_negative_or_abstain_signature_rows),
    approval_local_member_observed_signature_rows_rolling_3 =
      rolling_sum_3(approval_local_member_observed_signature_rows),
    nonapproval_local_member_negative_or_abstain_signature_rows_rolling_3 =
      rolling_sum_3(nonapproval_local_member_negative_or_abstain_signature_rows),
    nonapproval_local_member_observed_signature_rows_rolling_3 =
      rolling_sum_3(nonapproval_local_member_observed_signature_rows),
    approval_local_member_negative_or_abstain_signature_share_rolling_3 =
      approval_local_member_negative_or_abstain_signature_rows_rolling_3 /
        approval_local_member_observed_signature_rows_rolling_3,
    nonapproval_local_member_negative_or_abstain_signature_share_rolling_3 =
      nonapproval_local_member_negative_or_abstain_signature_rows_rolling_3 /
        nonapproval_local_member_observed_signature_rows_rolling_3
  )

local_member_no_abstain_roll_call_events <- all_roll_call_signatures |>
  filter(
    approval_local_member_negative_or_abstain_rows > 0L |
      nonapproval_local_member_negative_or_abstain_rows > 0L
  ) |>
  mutate(
    event_period = case_when(
      query_year < 2009L ~ "pre_2009",
      query_year <= 2013L ~ "2009_2013",
      TRUE ~ "2014_2025"
    ),
    event_source = case_when(
      approval_local_member_negative_or_abstain_rows > 0L &
        nonapproval_local_member_negative_or_abstain_rows > 0L ~ "approval_and_nonapproval",
      approval_local_member_negative_or_abstain_rows > 0L ~ "approval",
      TRUE ~ "nonapproval"
    )
  ) |>
  select(
    query_year,
    event_period,
    event_source,
    roll_call_signature,
    vote_source_group,
    vote_date,
    vote_margin,
    affirmative_count,
    negative_count,
    abstain_count,
    matter_rows,
    land_use_application_rows,
    resolution_rows,
    call_up_rows,
    approval_local_member_negative_or_abstain_rows,
    nonapproval_local_member_negative_or_abstain_rows,
    local_member_vote_rows,
    affected_council_districts,
    local_members_from_roster,
    local_member_final_action_votes,
    matter_files,
    application_keys,
    zap_project_ids,
    zap_project_names,
    title_examples,
    matter_urls,
    history_detail_urls
  ) |>
  arrange(query_year, vote_date, vote_source_group, matter_files)

local_member_no_abstain_matter_events <- decision |>
  filter(approval_local_member_negative_or_abstain | nonapproval_local_member_negative_or_abstain) |>
  mutate(
    event_period = case_when(
      query_year < 2009L ~ "pre_2009",
      query_year <= 2013L ~ "2009_2013",
      TRUE ~ "2014_2025"
    ),
    event_source = if_else(approval_local_member_negative_or_abstain, "approval", "nonapproval")
  ) |>
  select(
    query_year,
    event_period,
    event_source,
    matter_id,
    matter_file,
    query_matter_type,
    matter_type,
    disposition_group,
    vote_source_group,
    vote_date,
    decision_action,
    decision_result,
    vote_margin,
    affirmative_count,
    negative_count,
    abstain_count,
    affected_council_districts,
    affected_district_source,
    local_members_from_roster,
    local_member_final_action_vote_status,
    local_member_final_action_votes,
    application_keys,
    zap_project_ids,
    zap_project_names,
    title,
    matter_url,
    history_detail_url
  ) |>
  arrange(query_year, vote_date, matter_file)

seed_keys <- seed_cases |>
  select(seed_id, seed_ulurp_numbers = ulurp_numbers) |>
  mutate(application_key = str_split(coalesce(seed_ulurp_numbers, ""), "\\s*;\\s*")) |>
  unnest(application_key) |>
  mutate(application_key = normalize_application_key(application_key)) |>
  filter(!is.na(application_key)) |>
  distinct(seed_id, application_key)

event_keys <- local_member_no_abstain_roll_call_events |>
  select(roll_call_signature, application_keys) |>
  mutate(application_key = str_split(coalesce(application_keys, ""), "\\s*;\\s*")) |>
  unnest(application_key) |>
  mutate(application_key = normalize_application_key(application_key)) |>
  filter(!is.na(application_key)) |>
  distinct(roll_call_signature, application_key)

panel_keys <- all_roll_call_signatures |>
  select(roll_call_signature, application_keys) |>
  mutate(application_key = str_split(coalesce(application_keys, ""), "\\s*;\\s*")) |>
  unnest(application_key) |>
  mutate(application_key = normalize_application_key(application_key)) |>
  filter(!is.na(application_key)) |>
  distinct(roll_call_signature, application_key)

panel_key_lookup <- panel_keys |>
  left_join(
    all_roll_call_signatures,
    by = "roll_call_signature",
    relationship = "many-to-one"
  ) |>
  group_by(application_key) |>
  summarise(
    matched_panel_roll_call_signatures = collapse_values(roll_call_signature),
    matched_panel_years = collapse_values(query_year),
    matched_panel_vote_dates = collapse_values(vote_date),
    matched_panel_vote_margins = collapse_values(vote_margin),
    matched_panel_matter_files = collapse_values(matter_files),
    matched_panel_local_member_statuses = collapse_values(local_member_final_action_vote_statuses),
    matched_panel_local_members = collapse_values(local_members_from_roster),
    matched_panel_local_member_votes = collapse_values(local_member_final_action_votes),
    matched_panel_title_examples = collapse_values(title_examples),
    matched_panel_matter_urls = collapse_values(matter_urls),
    .groups = "drop"
  )

if (anyDuplicated(panel_key_lookup$application_key)) {
  stop("Panel key lookup must be unique by normalized application key.")
}

event_key_lookup <- event_keys |>
  left_join(
    local_member_no_abstain_roll_call_events,
    by = "roll_call_signature",
    relationship = "many-to-one"
  ) |>
  group_by(application_key) |>
  summarise(
    matched_roll_call_signatures = collapse_values(roll_call_signature),
    matched_event_years = collapse_values(query_year),
    matched_event_sources = collapse_values(event_source),
    matched_vote_dates = collapse_values(vote_date),
    matched_vote_margins = collapse_values(vote_margin),
    matched_matter_files = collapse_values(matter_files),
    matched_local_members = collapse_values(local_members_from_roster),
    matched_local_member_votes = collapse_values(local_member_final_action_votes),
    matched_title_examples = collapse_values(title_examples),
    matched_matter_urls = collapse_values(matter_urls),
    .groups = "drop"
  )

if (anyDuplicated(event_key_lookup$application_key)) {
  stop("Event key lookup must be unique by normalized application key.")
}

seed_case_overlap <- seed_keys |>
  left_join(panel_key_lookup, by = "application_key", relationship = "many-to-one") |>
  left_join(event_key_lookup, by = "application_key", relationship = "many-to-one") |>
  group_by(seed_id) |>
  summarise(
    seed_application_keys = collapse_values(application_key),
    matched_panel_application_keys = collapse_values(application_key[!is.na(matched_panel_roll_call_signatures)]),
    matched_panel_roll_call_signatures = collapse_values(matched_panel_roll_call_signatures),
    matched_panel_years = collapse_values(matched_panel_years),
    matched_panel_vote_dates = collapse_values(matched_panel_vote_dates),
    matched_panel_vote_margins = collapse_values(matched_panel_vote_margins),
    matched_panel_matter_files = collapse_values(matched_panel_matter_files),
    matched_panel_local_member_statuses = collapse_values(matched_panel_local_member_statuses),
    matched_panel_local_members = collapse_values(matched_panel_local_members),
    matched_panel_local_member_votes = collapse_values(matched_panel_local_member_votes),
    matched_panel_title_examples = collapse_values(matched_panel_title_examples),
    matched_application_keys = collapse_values(application_key[!is.na(matched_roll_call_signatures)]),
    matched_roll_call_signatures = collapse_values(matched_roll_call_signatures),
    matched_event_years = collapse_values(matched_event_years),
    matched_event_sources = collapse_values(matched_event_sources),
    matched_vote_dates = collapse_values(matched_vote_dates),
    matched_vote_margins = collapse_values(matched_vote_margins),
    matched_matter_files = collapse_values(matched_matter_files),
    matched_local_members = collapse_values(matched_local_members),
    matched_local_member_votes = collapse_values(matched_local_member_votes),
    matched_title_examples = collapse_values(matched_title_examples),
    matched_matter_urls = collapse_values(matched_matter_urls),
    .groups = "drop"
  ) |>
  right_join(seed_cases, by = "seed_id", relationship = "one-to-one") |>
  mutate(
    matched_current_panel_signature = !is.na(matched_panel_roll_call_signatures),
    matched_current_roll_call_event = !is.na(matched_roll_call_signatures),
    pre_2009_seed = suppressWarnings(as.integer(vote_year)) < 2009L
  ) |>
  select(
    seed_id,
    vote_year,
    vote_date,
    project_name,
    borough,
    affected_council_districts,
    local_members,
    ulurp_numbers,
    seed_application_keys,
    matched_current_panel_signature,
    matched_panel_application_keys,
    matched_panel_years,
    matched_panel_vote_margins,
    matched_panel_matter_files,
    matched_panel_local_member_statuses,
    matched_panel_local_members,
    matched_panel_local_member_votes,
    matched_panel_title_examples,
    matched_current_roll_call_event,
    matched_application_keys,
    matched_event_years,
    matched_event_sources,
    matched_vote_dates,
    matched_vote_margins,
    matched_matter_files,
    matched_local_members,
    matched_local_member_votes,
    matched_title_examples,
    evidence_tier,
    evidence_summary,
    source_urls,
    matched_matter_urls
  ) |>
  arrange(vote_year, project_name)

local_member_no_abstain_rolling_year <- annual |>
  arrange(query_year) |>
  transmute(
    query_year,
    approval_local_member_negative_or_abstain_rows =
      approval_split_local_member_negative_or_abstain_rows,
    nonapproval_local_member_negative_or_abstain_rows =
      nonapproval_split_local_member_negative_or_abstain_rows,
    approval_local_member_negative_or_abstain_rolling_5 =
      rolling_average_5(approval_split_local_member_negative_or_abstain_rows),
    nonapproval_local_member_negative_or_abstain_rolling_5 =
      rolling_average_5(nonapproval_split_local_member_negative_or_abstain_rows)
  )

local_member_no_abstain_roll_call_rolling_year <- local_member_no_abstain_signature_year |>
  arrange(query_year) |>
  transmute(
    query_year,
    approval_local_member_negative_or_abstain_signature_rows,
    nonapproval_local_member_negative_or_abstain_signature_rows,
    approval_local_member_negative_or_abstain_signature_rolling_5 =
      rolling_average_5(approval_local_member_negative_or_abstain_signature_rows),
    nonapproval_local_member_negative_or_abstain_signature_rolling_5 =
      rolling_average_5(nonapproval_local_member_negative_or_abstain_signature_rows)
  )

approval_member_votes <- bind_rows(lapply(
  sprintf("../input/legistar_%s_broad_recall_member_votes.csv", recall_years),
  \(path) read_csv(path, col_types = cols(.default = col_character()), na = character())
))
nonapproval_member_votes <- read_csv(
  "../output/member_deference_nonapproval_member_votes.csv",
  col_types = cols(.default = col_character()),
  na = character()
)

approval_split_lookup <- split_votes |>
  filter(vote_source_group == "approval_action_detail", history_detail_url != "") |>
  select(
    query_year,
    matter_id,
    matter_file,
    query_matter_type,
    matter_type,
    disposition_group,
    vote_source_group,
    vote_date,
    vote_margin,
    affirmative_count,
    negative_count,
    abstain_count,
    dissent_count,
    total_voting_count,
    simple_majority_approval_margin,
    affected_council_districts,
    local_members_from_roster,
    local_member_final_action_vote_status,
    primary_action_category,
    title,
    matter_url,
    history_detail_url
  )

if (any(duplicated(approval_split_lookup[c("matter_id", "history_detail_url")]))) {
  stop("Approval split-vote lookup must be unique by matter_id and history_detail_url.")
}

nonapproval_split_lookup <- split_votes |>
  filter(vote_source_group == "nonapproval_action_detail") |>
  select(
    query_year,
    matter_id,
    matter_file,
    query_matter_type,
    matter_type,
    disposition_group,
    vote_source_group,
    vote_date,
    vote_margin,
    affirmative_count,
    negative_count,
    abstain_count,
    dissent_count,
    total_voting_count,
    simple_majority_approval_margin,
    affected_council_districts,
    local_members_from_roster,
    local_member_final_action_vote_status,
    primary_action_category,
    title,
    matter_url,
    history_detail_url
  )

if (any(duplicated(nonapproval_split_lookup["matter_id"]))) {
  stop("Nonapproval split-vote lookup must be unique by matter_id.")
}

approval_vote_action_summary <- approval_member_votes |>
  group_by(matter_id, raw_history_detail_url = history_detail_url) |>
  summarise(
    raw_member_vote_rows = n(),
    raw_negative_count = sum(vote == "Negative"),
    raw_abstain_count = sum(vote == "Abstain"),
    raw_dissent_count = raw_negative_count + raw_abstain_count,
    .groups = "drop"
  )

approval_url_match <- approval_split_lookup |>
  left_join(
    approval_vote_action_summary,
    by = c("matter_id", "history_detail_url" = "raw_history_detail_url"),
    relationship = "many-to-one"
  ) |>
  filter(!is.na(raw_member_vote_rows)) |>
  mutate(
    raw_history_detail_url = history_detail_url,
    member_vote_match_method = "matter_id_history_detail_url"
  )

if (any(
  approval_url_match$raw_negative_count != approval_url_match$negative_count |
    approval_url_match$raw_abstain_count != approval_url_match$abstain_count
)) {
  stop("Approval URL-matched member-vote counts disagree with the decision panel.")
}

approval_url_unmatched <- approval_split_lookup |>
  anti_join(
    approval_url_match |> select(matter_id, history_detail_url),
    by = c("matter_id", "history_detail_url")
  )

approval_count_match_candidates <- approval_url_unmatched |>
  inner_join(approval_vote_action_summary, by = "matter_id", relationship = "one-to-many") |>
  filter(raw_negative_count == negative_count, raw_abstain_count == abstain_count) |>
  group_by(matter_id, history_detail_url) |>
  mutate(candidate_actions = n()) |>
  ungroup()

approval_count_match <- approval_count_match_candidates |>
  filter(candidate_actions == 1L) |>
  mutate(member_vote_match_method = "matter_id_vote_count") |>
  select(-candidate_actions)

approval_member_match_lookup <- bind_rows(
  approval_url_match,
  approval_count_match
) |>
  transmute(
    query_year,
    matter_id,
    matter_file,
    query_matter_type,
    matter_type,
    disposition_group,
    vote_source_group,
    vote_date,
    vote_margin,
    affirmative_count,
    negative_count,
    abstain_count,
    dissent_count,
    total_voting_count,
    simple_majority_approval_margin,
    affected_council_districts,
    local_members_from_roster,
    local_member_final_action_vote_status,
    primary_action_category,
    title,
    matter_url,
    final_history_detail_url = history_detail_url,
    raw_history_detail_url,
    raw_member_vote_rows,
    raw_negative_count,
    raw_abstain_count,
    member_vote_match_method
  )

if (any(duplicated(approval_member_match_lookup[c("matter_id", "final_history_detail_url")]))) {
  stop("Approval member-vote match lookup must be unique by matter_id and final_history_detail_url.")
}

approval_unmatched_member_vote_actions <- approval_split_lookup |>
  anti_join(
    approval_member_match_lookup |>
      transmute(matter_id, history_detail_url = final_history_detail_url),
    by = c("matter_id", "history_detail_url")
  ) |>
  left_join(
    approval_count_match_candidates |>
      count(matter_id, history_detail_url, name = "matching_candidate_actions"),
    by = c("matter_id", "history_detail_url"),
    relationship = "one-to-one"
  ) |>
  mutate(
    matching_candidate_actions = coalesce(matching_candidate_actions, 0L),
    member_vote_match_method = case_when(
      matching_candidate_actions > 1L ~ "unmatched_ambiguous_vote_count",
      TRUE ~ "unmatched_no_vote_count_match"
    )
  )

approval_nonaffirmative_members <- approval_member_votes |>
  filter(vote %in% c("Negative", "Abstain")) |>
  inner_join(
    approval_member_match_lookup,
    by = c("matter_id", "history_detail_url" = "raw_history_detail_url"),
    relationship = "many-to-one",
    suffix = c("_raw", "")
  ) |>
  transmute(
    query_year,
    matter_id,
    matter_file,
    query_matter_type,
    matter_type,
    disposition_group,
    vote_source_group,
    vote_date,
    vote_margin,
    negative_count,
    abstain_count,
    dissent_count,
    affected_council_districts,
    local_members_from_roster,
    local_member_final_action_vote_status,
    person_name,
    vote,
    member_vote_match_method,
    title,
    matter_url,
    history_detail_url = final_history_detail_url,
    raw_history_detail_url = history_detail_url
  )

nonapproval_nonaffirmative_members <- nonapproval_member_votes |>
  filter(vote %in% c("Negative", "Abstain")) |>
  inner_join(
    nonapproval_split_lookup,
    by = "matter_id",
    relationship = "many-to-one",
    suffix = c("_raw", "")
  ) |>
  transmute(
    query_year,
    matter_id,
    matter_file,
    query_matter_type,
    matter_type,
    disposition_group,
    vote_source_group,
    vote_date,
    vote_margin,
    negative_count,
    abstain_count,
    dissent_count,
    affected_council_districts,
    local_members_from_roster,
    local_member_final_action_vote_status,
    person_name,
    vote,
    member_vote_match_method = "nonapproval_final_action_vote_rows",
    title,
    matter_url,
    history_detail_url,
    raw_history_detail_url = history_detail_url
  )

nonaffirmative_member_rows <- bind_rows(
  approval_nonaffirmative_members,
  nonapproval_nonaffirmative_members
) |>
  distinct() |>
  mutate(
    dissent_member_is_local = member_in_local_roster(person_name, local_members_from_roster),
    dissent_member_local_status = case_when(
      is.na(dissent_member_is_local) ~ "local_roster_missing_or_unmatched",
      dissent_member_is_local ~ "local_member",
      TRUE ~ "nonlocal_member"
    )
  ) |>
  arrange(query_year, matter_file, person_name, vote)

approval_final_vote_lookup <- decision |>
  filter(
    matter_in_main_vote_sample,
    vote_source_group == "approval_action_detail",
    has_affected_council_district,
    has_local_member_vote_observed,
    history_detail_url != ""
  ) |>
  select(
    query_year,
    matter_id,
    matter_file,
    query_matter_type,
    matter_type,
    disposition_group,
    vote_source_group,
    vote_date,
    vote_margin,
    affirmative_count,
    negative_count,
    abstain_count,
    dissent_count,
    total_voting_count,
    simple_majority_approval_margin,
    affected_council_districts,
    local_members_from_roster,
    local_member_final_action_vote_status,
    primary_action_category,
    title,
    matter_url,
    history_detail_url
  )

if (any(duplicated(approval_final_vote_lookup[c("matter_id", "history_detail_url")]))) {
  stop("Approval final-vote lookup must be unique by matter_id and history_detail_url.")
}

nonapproval_final_vote_lookup <- decision |>
  filter(
    matter_in_main_vote_sample,
    vote_source_group == "nonapproval_action_detail",
    has_affected_council_district,
    has_local_member_vote_observed
  ) |>
  select(
    query_year,
    matter_id,
    matter_file,
    query_matter_type,
    matter_type,
    disposition_group,
    vote_source_group,
    vote_date,
    vote_margin,
    affirmative_count,
    negative_count,
    abstain_count,
    dissent_count,
    total_voting_count,
    simple_majority_approval_margin,
    affected_council_districts,
    local_members_from_roster,
    local_member_final_action_vote_status,
    primary_action_category,
    title,
    matter_url,
    history_detail_url
  )

if (any(duplicated(nonapproval_final_vote_lookup["matter_id"]))) {
  stop("Nonapproval final-vote lookup must be unique by matter_id.")
}

if (any(duplicated(approval_member_match_lookup[c("matter_id", "raw_history_detail_url")]))) {
  stop("Approval split member-vote lookup must be unique by matter_id and raw_history_detail_url.")
}

approval_exact_final_member_vote_rows <- approval_member_votes |>
  inner_join(
    approval_final_vote_lookup,
    by = c("matter_id", "history_detail_url"),
    relationship = "many-to-one",
    suffix = c("_raw", "")
  ) |>
  transmute(
    query_year,
    matter_id,
    matter_file,
    query_matter_type,
    matter_type,
    disposition_group,
    vote_source_group,
    vote_date,
    vote_margin,
    affirmative_count,
    negative_count,
    abstain_count,
    dissent_count,
    total_voting_count,
    simple_majority_approval_margin,
    affected_council_districts,
    local_members_from_roster,
    local_member_final_action_vote_status,
    primary_action_category,
    person_name,
    vote,
    member_vote_match_method = "matter_id_history_detail_url",
    title,
    matter_url,
    history_detail_url
  )

approval_split_final_member_vote_rows <- approval_member_votes |>
  inner_join(
    approval_member_match_lookup,
    by = c("matter_id", "history_detail_url" = "raw_history_detail_url"),
    relationship = "many-to-one",
    suffix = c("_raw", "")
  ) |>
  transmute(
    query_year,
    matter_id,
    matter_file,
    query_matter_type,
    matter_type,
    disposition_group,
    vote_source_group,
    vote_date,
    vote_margin,
    affirmative_count,
    negative_count,
    abstain_count,
    dissent_count,
    total_voting_count,
    simple_majority_approval_margin,
    affected_council_districts,
    local_members_from_roster,
    local_member_final_action_vote_status,
    primary_action_category,
    person_name,
    vote,
    member_vote_match_method,
    title,
    matter_url,
    history_detail_url = final_history_detail_url
  )

approval_final_member_vote_rows <- bind_rows(
  approval_exact_final_member_vote_rows |>
    anti_join(
      approval_member_match_lookup |> distinct(matter_id),
      by = "matter_id"
    ),
  approval_split_final_member_vote_rows
)

nonapproval_final_member_vote_rows <- nonapproval_member_votes |>
  inner_join(
    nonapproval_final_vote_lookup,
    by = "matter_id",
    relationship = "many-to-one",
    suffix = c("_raw", "")
  ) |>
  transmute(
    query_year,
    matter_id,
    matter_file,
    query_matter_type,
    matter_type,
    disposition_group,
    vote_source_group,
    vote_date,
    vote_margin,
    affirmative_count,
    negative_count,
    abstain_count,
    dissent_count,
    total_voting_count,
    simple_majority_approval_margin,
    affected_council_districts,
    local_members_from_roster,
    local_member_final_action_vote_status,
    primary_action_category,
    person_name,
    vote,
    member_vote_match_method = "nonapproval_final_action_vote_rows",
    title,
    matter_url,
    history_detail_url
  )

final_member_vote_rows <- bind_rows(
  approval_final_member_vote_rows,
  nonapproval_final_member_vote_rows
) |>
  distinct() |>
  mutate(
    member_is_local = member_in_local_roster(person_name, local_members_from_roster),
    member_local_status = case_when(
      is.na(member_is_local) ~ "local_roster_missing_or_unmatched",
      member_is_local ~ "local_member",
      TRUE ~ "nonlocal_member"
    ),
    period = period_from_query_year(query_year)
  )

final_member_vote_matter_summary <- final_member_vote_rows |>
  filter(vote %in% c("Affirmative", "Negative", "Abstain")) |>
  group_by(matter_id) |>
  summarise(
    final_member_vote_rows = n(),
    nonlocal_voting_member_count = sum(member_local_status == "nonlocal_member"),
    local_voting_member_count = sum(member_local_status == "local_member"),
    unclassified_voting_member_count = sum(member_local_status == "local_roster_missing_or_unmatched"),
    nonlocal_no_abstain_count = sum(
      member_local_status == "nonlocal_member" & vote %in% c("Negative", "Abstain")
    ),
    nonlocal_no_count = sum(member_local_status == "nonlocal_member" & vote == "Negative"),
    nonlocal_abstain_count = sum(member_local_status == "nonlocal_member" & vote == "Abstain"),
    local_no_abstain_count = sum(
      member_local_status == "local_member" & vote %in% c("Negative", "Abstain")
    ),
    unclassified_no_abstain_count = sum(
      member_local_status == "local_roster_missing_or_unmatched" &
        vote %in% c("Negative", "Abstain")
    ),
    nonlocal_no_abstain_members = collapse_values(
      person_name[member_local_status == "nonlocal_member" & vote %in% c("Negative", "Abstain")]
    ),
    nonlocal_no_members = collapse_values(
      person_name[member_local_status == "nonlocal_member" & vote == "Negative"]
    ),
    nonlocal_abstain_members = collapse_values(
      person_name[member_local_status == "nonlocal_member" & vote == "Abstain"]
    ),
    .groups = "drop"
  )

if (nrow(final_member_vote_matter_summary) != n_distinct(final_member_vote_matter_summary$matter_id)) {
  stop("Final member-vote matter summary must be unique by matter_id.")
}

nonlocal_dissent_matter_rows <- decision |>
  filter(
    matter_in_main_vote_sample,
    has_affected_council_district,
    has_local_member_vote_observed
  ) |>
  left_join(final_member_vote_matter_summary, by = "matter_id", relationship = "one-to-one") |>
  mutate(
    final_member_vote_match_status = case_when(
      !is.na(final_member_vote_rows) ~ "matched_final_member_votes",
      dissent_count == 0L ~ "no_dissent_member_vote_rows_not_needed",
      TRUE ~ "missing_final_member_votes_for_split_vote"
    ),
    nonlocal_voting_member_count = if_else(
      is.na(nonlocal_voting_member_count),
      NA_integer_,
      nonlocal_voting_member_count
    ),
    across(
      c(
        final_member_vote_rows,
        local_voting_member_count,
        unclassified_voting_member_count,
        nonlocal_no_abstain_count,
        nonlocal_no_count,
        nonlocal_abstain_count,
        local_no_abstain_count,
        unclassified_no_abstain_count
      ),
      ~ coalesce(.x, 0L)
    ),
    any_nonlocal_no_abstain = nonlocal_no_abstain_count > 0L,
    any_nonlocal_no = nonlocal_no_count > 0L,
    any_nonlocal_abstain = nonlocal_abstain_count > 0L,
    nonlocal_no_abstain_share = if_else(
      nonlocal_voting_member_count > 0L,
      nonlocal_no_abstain_count / nonlocal_voting_member_count,
      NA_real_
    ),
    nonlocal_no_share = if_else(
      nonlocal_voting_member_count > 0L,
      nonlocal_no_count / nonlocal_voting_member_count,
      NA_real_
    ),
    nonlocal_abstain_share = if_else(
      nonlocal_voting_member_count > 0L,
      nonlocal_abstain_count / nonlocal_voting_member_count,
      NA_real_
    ),
    nonlocal_dissent_size_group = case_when(
      nonlocal_no_abstain_count == 0L ~ "0",
      nonlocal_no_abstain_count == 1L ~ "1",
      nonlocal_no_abstain_count == 2L ~ "2",
      nonlocal_no_abstain_count <= 4L ~ "3-4",
      nonlocal_no_abstain_count <= 9L ~ "5-9",
      TRUE ~ "10+"
    ),
    simple_majority_margin_group = case_when(
      simple_majority_approval_margin <= 0L ~ "<=0",
      simple_majority_approval_margin <= 5L ~ "1-5",
      simple_majority_approval_margin <= 10L ~ "6-10",
      simple_majority_approval_margin <= 20L ~ "11-20",
      TRUE ~ "21+"
    ),
    period = period_from_query_year(query_year)
  ) |>
  select(
    query_year,
    period,
    matter_id,
    matter_file,
    query_matter_type,
    matter_type,
    disposition_group,
    vote_source_group,
    vote_date,
    vote_margin,
    affirmative_count,
    negative_count,
    abstain_count,
    dissent_count,
    total_voting_count,
    simple_majority_approval_margin,
    simple_majority_margin_group,
    affected_council_districts,
    local_members_from_roster,
    local_member_final_action_vote_status,
    primary_action_category,
    final_member_vote_match_status,
    final_member_vote_rows,
    nonlocal_voting_member_count,
    local_voting_member_count,
    unclassified_voting_member_count,
    nonlocal_no_abstain_count,
    nonlocal_no_count,
    nonlocal_abstain_count,
    local_no_abstain_count,
    unclassified_no_abstain_count,
    any_nonlocal_no_abstain,
    any_nonlocal_no,
    any_nonlocal_abstain,
    nonlocal_no_abstain_share,
    nonlocal_no_share,
    nonlocal_abstain_share,
    nonlocal_dissent_size_group,
    nonlocal_no_abstain_members,
    nonlocal_no_members,
    nonlocal_abstain_members,
    title,
    matter_url,
    history_detail_url
  ) |>
  arrange(query_year, vote_date, matter_file)

nonlocal_dissent_year_source <- nonlocal_dissent_matter_rows |>
  group_by(query_year, vote_source_group) |>
  summarise(
    observed_local_member_matter_rows = n(),
    member_vote_matched_matter_rows = sum(final_member_vote_match_status == "matched_final_member_votes"),
    split_matter_rows = sum(dissent_count > 0L),
    any_nonlocal_no_abstain_matter_rows = sum(any_nonlocal_no_abstain),
    any_nonlocal_no_matter_rows = sum(any_nonlocal_no),
    any_nonlocal_abstain_matter_rows = sum(any_nonlocal_abstain),
    one_or_two_nonlocal_dissent_matter_rows = sum(nonlocal_no_abstain_count %in% c(1L, 2L)),
    three_plus_nonlocal_dissent_matter_rows = sum(nonlocal_no_abstain_count >= 3L),
    nonlocal_voting_member_votes = sum(nonlocal_voting_member_count, na.rm = TRUE),
    nonlocal_no_abstain_votes = sum(nonlocal_no_abstain_count),
    nonlocal_no_votes = sum(nonlocal_no_count),
    nonlocal_abstain_votes = sum(nonlocal_abstain_count),
    mean_nonlocal_no_abstain_count_when_any = if_else(
      any(any_nonlocal_no_abstain),
      mean(nonlocal_no_abstain_count[any_nonlocal_no_abstain]),
      NA_real_
    ),
    median_simple_majority_margin_when_any = if_else(
      any(any_nonlocal_no_abstain),
      median(simple_majority_approval_margin[any_nonlocal_no_abstain], na.rm = TRUE),
      NA_real_
    ),
    .groups = "drop"
  )

nonlocal_dissent_year_all <- nonlocal_dissent_matter_rows |>
  group_by(query_year) |>
  summarise(
    vote_source_group = "all_final_action_detail",
    observed_local_member_matter_rows = n(),
    member_vote_matched_matter_rows = sum(final_member_vote_match_status == "matched_final_member_votes"),
    split_matter_rows = sum(dissent_count > 0L),
    any_nonlocal_no_abstain_matter_rows = sum(any_nonlocal_no_abstain),
    any_nonlocal_no_matter_rows = sum(any_nonlocal_no),
    any_nonlocal_abstain_matter_rows = sum(any_nonlocal_abstain),
    one_or_two_nonlocal_dissent_matter_rows = sum(nonlocal_no_abstain_count %in% c(1L, 2L)),
    three_plus_nonlocal_dissent_matter_rows = sum(nonlocal_no_abstain_count >= 3L),
    nonlocal_voting_member_votes = sum(nonlocal_voting_member_count, na.rm = TRUE),
    nonlocal_no_abstain_votes = sum(nonlocal_no_abstain_count),
    nonlocal_no_votes = sum(nonlocal_no_count),
    nonlocal_abstain_votes = sum(nonlocal_abstain_count),
    mean_nonlocal_no_abstain_count_when_any = if_else(
      any(any_nonlocal_no_abstain),
      mean(nonlocal_no_abstain_count[any_nonlocal_no_abstain]),
      NA_real_
    ),
    median_simple_majority_margin_when_any = if_else(
      any(any_nonlocal_no_abstain),
      median(simple_majority_approval_margin[any_nonlocal_no_abstain], na.rm = TRUE),
      NA_real_
    ),
    .groups = "drop"
  )

nonlocal_dissent_year <- bind_rows(
  nonlocal_dissent_year_source,
  nonlocal_dissent_year_all
) |>
  mutate(
    any_nonlocal_no_abstain_matter_share =
      any_nonlocal_no_abstain_matter_rows / observed_local_member_matter_rows,
    any_nonlocal_no_matter_share =
      any_nonlocal_no_matter_rows / observed_local_member_matter_rows,
    any_nonlocal_abstain_matter_share =
      any_nonlocal_abstain_matter_rows / observed_local_member_matter_rows,
    nonlocal_no_abstain_vote_share = if_else(
      nonlocal_voting_member_votes > 0L,
      nonlocal_no_abstain_votes / nonlocal_voting_member_votes,
      NA_real_
    ),
    nonlocal_no_vote_share = if_else(
      nonlocal_voting_member_votes > 0L,
      nonlocal_no_votes / nonlocal_voting_member_votes,
      NA_real_
    ),
    nonlocal_abstain_vote_share = if_else(
      nonlocal_voting_member_votes > 0L,
      nonlocal_abstain_votes / nonlocal_voting_member_votes,
      NA_real_
    )
  ) |>
  arrange(query_year, vote_source_group)

nonlocal_dissent_rolling_year <- nonlocal_dissent_year |>
  group_by(vote_source_group) |>
  arrange(query_year, .by_group = TRUE) |>
  mutate(
    any_nonlocal_no_abstain_matter_share_rolling_5 = rolling_rate_5(
      any_nonlocal_no_abstain_matter_rows,
      observed_local_member_matter_rows
    ),
    any_nonlocal_no_matter_share_rolling_5 = rolling_rate_5(
      any_nonlocal_no_matter_rows,
      observed_local_member_matter_rows
    ),
    any_nonlocal_abstain_matter_share_rolling_5 = rolling_rate_5(
      any_nonlocal_abstain_matter_rows,
      observed_local_member_matter_rows
    ),
    nonlocal_no_abstain_vote_share_rolling_5 = rolling_rate_5(
      nonlocal_no_abstain_votes,
      nonlocal_voting_member_votes
    ),
    nonlocal_no_vote_share_rolling_5 = rolling_rate_5(
      nonlocal_no_votes,
      nonlocal_voting_member_votes
    ),
    nonlocal_abstain_vote_share_rolling_5 = rolling_rate_5(
      nonlocal_abstain_votes,
      nonlocal_voting_member_votes
    )
  ) |>
  ungroup()

nonlocal_dissent_size_distribution <- nonlocal_dissent_matter_rows |>
  filter(vote_source_group == "approval_action_detail") |>
  count(period, nonlocal_dissent_size_group, name = "matter_rows") |>
  group_by(period) |>
  mutate(
    period_matter_rows = sum(matter_rows),
    matter_share = matter_rows / period_matter_rows
  ) |>
  ungroup() |>
  arrange(period, nonlocal_dissent_size_group)

nonlocal_dissent_member_period_summary <- final_member_vote_rows |>
  filter(
    vote_source_group == "approval_action_detail",
    member_local_status == "nonlocal_member",
    vote %in% c("Affirmative", "Negative", "Abstain")
  ) |>
  group_by(period, person_name) |>
  summarise(
    nonlocal_vote_opportunities = n(),
    nonlocal_no_abstain_votes = sum(vote %in% c("Negative", "Abstain")),
    nonlocal_no_votes = sum(vote == "Negative"),
    nonlocal_abstain_votes = sum(vote == "Abstain"),
    nonlocal_no_abstain_vote_rate = nonlocal_no_abstain_votes / nonlocal_vote_opportunities,
    nonlocal_no_vote_rate = nonlocal_no_votes / nonlocal_vote_opportunities,
    nonlocal_abstain_vote_rate = nonlocal_abstain_votes / nonlocal_vote_opportunities,
    first_year = min(query_year),
    last_year = max(query_year),
    .groups = "drop"
  ) |>
  arrange(period, desc(nonlocal_no_abstain_votes), person_name)

nonlocal_dissent_member_concentration <- final_member_vote_rows |>
  filter(
    member_local_status == "nonlocal_member",
    vote %in% c("Negative", "Abstain")
  ) |>
  mutate(
    nonlocal_dissent_vote_type = recode(vote, Negative = "no", Abstain = "abstain")
  ) |>
  count(period, vote_source_group, nonlocal_dissent_vote_type, person_name,
        name = "nonlocal_dissent_votes") |>
  group_by(period, vote_source_group, nonlocal_dissent_vote_type) |>
  arrange(desc(nonlocal_dissent_votes), person_name, .by_group = TRUE) |>
  summarise(
    total_nonlocal_dissent_votes = sum(nonlocal_dissent_votes),
    top_5_nonlocal_dissent_votes = sum(head(nonlocal_dissent_votes, 5L)),
    top_5_nonlocal_dissent_vote_share =
      top_5_nonlocal_dissent_votes / total_nonlocal_dissent_votes,
    top_5_nonlocal_dissenters = collapse_values(
      head(str_c(person_name, " (", nonlocal_dissent_votes, ")"), 5L)
    ),
    .groups = "drop"
  ) |>
  arrange(period, vote_source_group, nonlocal_dissent_vote_type)

nonlocal_dissent_pivotality_period <- nonlocal_dissent_matter_rows |>
  filter(
    vote_source_group == "approval_action_detail",
    any_nonlocal_no_abstain
  ) |>
  count(period, simple_majority_margin_group, name = "matter_rows") |>
  group_by(period) |>
  mutate(
    period_nonlocal_dissent_matter_rows = sum(matter_rows),
    matter_share = matter_rows / period_nonlocal_dissent_matter_rows
  ) |>
  ungroup() |>
  arrange(period, simple_majority_margin_group)

nonlocal_dissent_action_category_period <- nonlocal_dissent_matter_rows |>
  filter(vote_source_group == "approval_action_detail") |>
  group_by(period, primary_action_category) |>
  summarise(
    observed_local_member_matter_rows = n(),
    any_nonlocal_no_abstain_matter_rows = sum(any_nonlocal_no_abstain),
    any_nonlocal_no_matter_rows = sum(any_nonlocal_no),
    any_nonlocal_abstain_matter_rows = sum(any_nonlocal_abstain),
    nonlocal_voting_member_votes = sum(nonlocal_voting_member_count, na.rm = TRUE),
    nonlocal_no_abstain_votes = sum(nonlocal_no_abstain_count),
    nonlocal_no_votes = sum(nonlocal_no_count),
    nonlocal_abstain_votes = sum(nonlocal_abstain_count),
    .groups = "drop"
  ) |>
  mutate(
    any_nonlocal_no_abstain_matter_share =
      any_nonlocal_no_abstain_matter_rows / observed_local_member_matter_rows,
    any_nonlocal_no_matter_share =
      any_nonlocal_no_matter_rows / observed_local_member_matter_rows,
    any_nonlocal_abstain_matter_share =
      any_nonlocal_abstain_matter_rows / observed_local_member_matter_rows,
    nonlocal_no_abstain_vote_share = if_else(
      nonlocal_voting_member_votes > 0L,
      nonlocal_no_abstain_votes / nonlocal_voting_member_votes,
      NA_real_
    )
  ) |>
  arrange(period, desc(observed_local_member_matter_rows), primary_action_category)

unanimity_category_period <- nonlocal_dissent_matter_rows |>
  filter(vote_source_group == "approval_action_detail") |>
  group_by(period, primary_action_category) |>
  summarise(
    matter_rows = n(),
    unanimous_matter_rows = sum(dissent_count == 0L),
    unanimity_share = unanimous_matter_rows / matter_rows,
    .groups = "drop"
  ) |>
  group_by(period) |>
  mutate(
    period_matter_rows = sum(matter_rows),
    category_share = matter_rows / period_matter_rows,
    overall_unanimity_share = sum(unanimous_matter_rows) / sum(matter_rows)
  ) |>
  ungroup()

baseline_unanimity <- unanimity_category_period |>
  filter(period == "1998-2002") |>
  select(
    primary_action_category,
    baseline_matter_rows = matter_rows,
    baseline_category_share = category_share,
    baseline_unanimity_share = unanimity_share
  )

baseline_overall_unanimity <- unanimity_category_period |>
  filter(period == "1998-2002") |>
  distinct(overall_unanimity_share) |>
  pull(overall_unanimity_share)

period_overall_unanimity <- unanimity_category_period |>
  distinct(period, overall_unanimity_share)

unanimity_composition_decomposition <- crossing(
  period = unique(unanimity_category_period$period[unanimity_category_period$period != "1998-2002"]),
  primary_action_category = unique(unanimity_category_period$primary_action_category)
) |>
  left_join(
    unanimity_category_period,
    by = c("period", "primary_action_category"),
    relationship = "one-to-one"
  ) |>
  left_join(period_overall_unanimity, by = "period", relationship = "many-to-one") |>
  mutate(
    matter_rows = coalesce(matter_rows, 0L),
    unanimous_matter_rows = coalesce(unanimous_matter_rows, 0L),
    period_matter_rows = coalesce(period_matter_rows, 0L),
    category_share = coalesce(category_share, 0),
    overall_unanimity_share = coalesce(overall_unanimity_share.x, overall_unanimity_share.y)
  ) |>
  select(-overall_unanimity_share.x, -overall_unanimity_share.y) |>
  left_join(baseline_unanimity, by = "primary_action_category", relationship = "many-to-one") |>
  mutate(
    baseline_category_status = if_else(
      is.na(baseline_unanimity_share),
      "missing_from_1998_2002_baseline",
      "observed_in_1998_2002_baseline"
    ),
    target_unanimity_share_for_decomposition = if_else(
      matter_rows == 0L,
      baseline_unanimity_share,
      unanimity_share
    ),
    composition_component = (category_share - baseline_category_share) * baseline_unanimity_share,
    within_category_component =
      category_share * (target_unanimity_share_for_decomposition - baseline_unanimity_share)
  ) |>
  group_by(period) |>
  mutate(
    decomposition_composition_sum = sum(composition_component, na.rm = TRUE),
    decomposition_within_sum = sum(within_category_component, na.rm = TRUE),
    decomposition_total_sum = decomposition_composition_sum + decomposition_within_sum,
    observed_change_in_unanimity = first(overall_unanimity_share) - baseline_overall_unanimity
  ) |>
  ungroup() |>
  arrange(period, primary_action_category)

nonaffirmative_member_summary <- nonaffirmative_member_rows |>
  group_by(person_name, vote_source_group, vote) |>
  summarise(
    nonaffirmative_vote_rows = n(),
    matter_rows = n_distinct(matter_id),
    first_year = min(query_year),
    last_year = max(query_year),
    .groups = "drop"
  ) |>
  arrange(desc(nonaffirmative_vote_rows), person_name, vote_source_group, vote)

member_match_qc <- tibble(
  check_name = c(
    "split_vote_rows",
    "approval_split_vote_rows",
    "approval_split_vote_rows_matched_by_url",
    "approval_split_vote_rows_matched_by_count",
    "approval_split_vote_rows_unmatched_to_member_vote_action",
    "approval_split_vote_rows_with_nonaffirmative_member_rows",
    "nonapproval_split_vote_rows",
    "nonapproval_split_vote_rows_with_nonaffirmative_member_rows",
    "nonaffirmative_member_rows",
    "nonlocal_dissent_matter_rows",
    "approval_observed_local_member_matter_rows",
    "approval_observed_local_member_matter_rows_with_exact_final_member_votes",
    "nonapproval_observed_local_member_matter_rows",
    "nonapproval_observed_local_member_matter_rows_with_final_member_votes",
    "final_member_vote_rows"
  ),
  value = c(
    nrow(split_votes),
    nrow(approval_split_lookup),
    nrow(approval_url_match),
    nrow(approval_count_match),
    nrow(approval_unmatched_member_vote_actions),
    n_distinct(approval_nonaffirmative_members$matter_id),
    nrow(nonapproval_split_lookup),
    n_distinct(nonapproval_nonaffirmative_members$matter_id),
    nrow(nonaffirmative_member_rows),
    nrow(nonlocal_dissent_matter_rows),
    nrow(approval_final_vote_lookup),
    n_distinct(approval_final_member_vote_rows$matter_id),
    nrow(nonapproval_final_vote_lookup),
    n_distinct(nonapproval_final_member_vote_rows$matter_id),
    nrow(final_member_vote_rows)
  )
)

write_csv(annual, "../output/council_land_use_split_vote_annual_decomposition.csv")
write_csv(dissent_size_year, "../output/council_land_use_split_vote_dissent_size_year.csv")
write_csv(local_member_alignment_year, "../output/council_land_use_split_vote_local_member_alignment_year.csv")
write_csv(matter_type_year, "../output/council_land_use_split_vote_matter_type_year.csv")
write_csv(roll_call_signature_year, "../output/council_land_use_split_vote_roll_call_signature_year.csv")
write_csv(top_roll_call_signatures, "../output/council_land_use_split_vote_top_roll_call_signatures.csv")
write_csv(
  missing_geography_roll_call_repair_queue,
  "../output/council_land_use_missing_geography_roll_call_repair_queue.csv"
)
write_csv(top_matter_examples, "../output/council_land_use_split_vote_top_matter_examples.csv")
write_csv(nonaffirmative_member_rows, "../output/council_land_use_split_vote_nonaffirmative_member_rows.csv")
write_csv(nonaffirmative_member_summary, "../output/council_land_use_split_vote_nonaffirmative_member_summary.csv")
write_csv(member_match_qc, "../output/council_land_use_split_vote_member_match_qc.csv")
write_csv(
  approval_unmatched_member_vote_actions,
  "../output/council_land_use_split_vote_unmatched_member_vote_actions.csv"
)
write_csv(
  local_member_no_abstain_roll_call_events,
  "../output/council_land_use_local_member_no_abstain_roll_call_events.csv"
)
write_csv(
  local_member_no_abstain_matter_events,
  "../output/council_land_use_local_member_no_abstain_matter_events.csv"
)
write_csv(
  local_member_no_abstain_denominator_year,
  "../output/council_land_use_local_member_no_abstain_roll_call_rate_year.csv"
)
write_csv(
  local_member_no_abstain_rate_rolling_year,
  "../output/council_land_use_local_member_no_abstain_roll_call_rate_rolling_year.csv"
)
write_csv(
  local_member_no_abstain_rate_rolling_3_year,
  "../output/council_land_use_local_member_no_abstain_roll_call_rate_rolling_3_year.csv"
)
write_csv(
  multi_district_signature_year,
  "../output/council_land_use_roll_call_multi_district_year.csv"
)
write_csv(
  multi_district_signature_rolling_year,
  "../output/council_land_use_roll_call_multi_district_rolling_year.csv"
)
write_csv(
  local_member_no_abstain_rolling_year,
  "../output/council_land_use_local_member_no_abstain_rolling_year.csv"
)
write_csv(
  local_member_no_abstain_signature_year,
  "../output/council_land_use_local_member_no_abstain_roll_call_year.csv"
)
write_csv(
  local_member_no_abstain_roll_call_rolling_year,
  "../output/council_land_use_local_member_no_abstain_roll_call_rolling_year.csv"
)
write_csv(
  seed_case_overlap,
  "../output/council_land_use_seed_overrule_case_overlap.csv"
)
write_csv(
  nonlocal_dissent_matter_rows,
  "../output/council_land_use_nonlocal_dissent_matter_rows.csv"
)
write_csv(
  nonlocal_dissent_year,
  "../output/council_land_use_nonlocal_dissent_year.csv"
)
write_csv(
  nonlocal_dissent_rolling_year,
  "../output/council_land_use_nonlocal_dissent_rolling_year.csv"
)
write_csv(
  nonlocal_dissent_size_distribution,
  "../output/council_land_use_nonlocal_dissent_size_distribution.csv"
)
write_csv(
  nonlocal_dissent_member_period_summary,
  "../output/council_land_use_nonlocal_dissent_member_period_summary.csv"
)
write_csv(
  nonlocal_dissent_member_concentration,
  "../output/council_land_use_nonlocal_dissent_member_concentration.csv"
)
write_csv(
  nonlocal_dissent_pivotality_period,
  "../output/council_land_use_nonlocal_dissent_pivotality_period.csv"
)
write_csv(
  nonlocal_dissent_action_category_period,
  "../output/council_land_use_nonlocal_dissent_action_category_period.csv"
)
write_csv(
  unanimity_composition_decomposition,
  "../output/council_land_use_unanimity_composition_decomposition.csv"
)
write_csv(
  final_member_vote_rows,
  "../output/council_land_use_final_member_vote_rows_with_local_status.csv"
)

decomposition_plot <- annual |>
  select(
    query_year,
    split_vote_rows,
    approval_split_vote_rows,
    nonapproval_split_vote_rows,
    approval_split_local_member_negative_or_abstain_rows
  ) |>
  pivot_longer(-query_year, names_to = "series", values_to = "matter_count") |>
  mutate(
    series = recode(
      series,
      split_vote_rows = "Any split final-action vote",
      approval_split_vote_rows = "Approval split vote",
      nonapproval_split_vote_rows = "Nonapproval split vote",
      approval_split_local_member_negative_or_abstain_rows = "Approval split: local member no/abstain"
    )
  ) |>
  ggplot(aes(x = query_year, y = matter_count, color = series)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.75) +
  geom_point(size = 1.6) +
  scale_x_continuous(breaks = plot_year_breaks) +
  labs(x = NULL, y = "Matters", color = NULL)

ggsave(
  "../output/council_land_use_split_vote_decomposition.pdf",
  decomposition_plot,
  width = 7.8,
  height = 4.6
)

dissent_size_plot <- dissent_size_year |>
  group_by(query_year, dissent_size_group) |>
  summarise(split_vote_rows = sum(split_vote_rows), .groups = "drop") |>
  mutate(
    dissent_size_group = factor(dissent_size_group, levels = c("1", "2-4", "5-9", "10+"))
  ) |>
  ggplot(aes(x = query_year, y = split_vote_rows, fill = dissent_size_group)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_col(width = 0.75) +
  scale_x_continuous(breaks = plot_year_breaks) +
  labs(x = NULL, y = "Split-vote matters", fill = "No/abstain votes")

ggsave(
  "../output/council_land_use_split_vote_dissent_size.pdf",
  dissent_size_plot,
  width = 7.8,
  height = 4.6
)

local_member_plot <- annual |>
  select(
    query_year,
    split_vote_rows_with_local_member_vote,
    approval_split_local_member_negative_or_abstain_rows,
    nonapproval_split_local_member_negative_or_abstain_rows
  ) |>
  pivot_longer(-query_year, names_to = "series", values_to = "matter_count") |>
  mutate(
    series = recode(
      series,
      split_vote_rows_with_local_member_vote = "Split vote with local-member vote observed",
      approval_split_local_member_negative_or_abstain_rows = "Approval split: local member no/abstain",
      nonapproval_split_local_member_negative_or_abstain_rows = "Nonapproval split: local member no/abstain"
    )
  ) |>
  ggplot(aes(x = query_year, y = matter_count, color = series)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.75) +
  geom_point(size = 1.6) +
  scale_x_continuous(breaks = plot_year_breaks) +
  labs(x = NULL, y = "Matters", color = NULL)

ggsave(
  "../output/council_land_use_split_vote_local_member_alignment.pdf",
  local_member_plot,
  width = 7.8,
  height = 4.6
)

local_member_no_abstain_roll_call_rate_plot <- local_member_no_abstain_denominator_year |>
  transmute(
    query_year,
    share = approval_local_member_negative_or_abstain_signature_share
  ) |>
  filter(!is.na(share)) |>
  ggplot(aes(x = query_year, y = share)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.85, color = "#F8766D") +
  geom_point(size = 1.8, color = "#F8766D") +
  scale_x_continuous(breaks = plot_year_breaks) +
  scale_y_continuous(labels = function(x) paste0(round(100 * x), "%")) +
  labs(x = NULL, y = "Share of observed local-member signatures")

ggsave(
  "../output/council_land_use_local_member_no_abstain_roll_call_rate_trends.pdf",
  local_member_no_abstain_roll_call_rate_plot,
  width = 7.8,
  height = 4.6
)

local_member_no_abstain_roll_call_rate_rolling_3_plot <- local_member_no_abstain_rate_rolling_3_year |>
  transmute(
    query_year,
    share = approval_local_member_negative_or_abstain_signature_share_rolling_3
  ) |>
  filter(!is.na(share)) |>
  ggplot(aes(x = query_year, y = share)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.85, color = "#F8766D") +
  geom_point(size = 1.8, color = "#F8766D") +
  scale_x_continuous(breaks = plot_year_breaks) +
  scale_y_continuous(labels = function(x) paste0(round(100 * x), "%")) +
  labs(x = NULL, y = "3-year trailing share")

ggsave(
  "../output/council_land_use_local_member_no_abstain_roll_call_rate_rolling_3_trends.pdf",
  local_member_no_abstain_roll_call_rate_rolling_3_plot,
  width = 7.8,
  height = 4.6
)

local_member_no_abstain_roll_call_rate_rolling_plot <- local_member_no_abstain_rate_rolling_year |>
  transmute(
    query_year,
    share = approval_local_member_negative_or_abstain_signature_share_rolling_5
  ) |>
  filter(!is.na(share)) |>
  ggplot(aes(x = query_year, y = share)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.85, color = "#F8766D") +
  geom_point(size = 1.8, color = "#F8766D") +
  scale_x_continuous(breaks = plot_year_breaks) +
  scale_y_continuous(labels = function(x) paste0(round(100 * x), "%")) +
  labs(x = NULL, y = "5-year trailing share")

ggsave(
  "../output/council_land_use_local_member_no_abstain_roll_call_rate_rolling_trends.pdf",
  local_member_no_abstain_roll_call_rate_rolling_plot,
  width = 7.8,
  height = 4.6
)

multi_district_signature_rolling_plot <- multi_district_signature_rolling_year |>
  select(
    query_year,
    approval_multi_district_observed_signature_share_rolling_5,
    approval_single_district_local_no_abstain_signature_share_rolling_5,
    approval_multi_district_local_no_abstain_signature_share_rolling_5
  ) |>
  pivot_longer(-query_year, names_to = "series", values_to = "share") |>
  filter(!is.na(share)) |>
  mutate(
    series = recode(
      series,
      approval_multi_district_observed_signature_share_rolling_5 =
        "Approval signatures affecting multiple districts",
      approval_single_district_local_no_abstain_signature_share_rolling_5 =
        "Local no/abstain rate: single-district",
      approval_multi_district_local_no_abstain_signature_share_rolling_5 =
        "Local no/abstain rate: multi-district"
    )
  ) |>
  ggplot(aes(x = query_year, y = share, color = series)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.85) +
  geom_point(size = 1.8) +
  scale_x_continuous(breaks = plot_year_breaks) +
  scale_y_continuous(labels = function(x) paste0(round(100 * x), "%")) +
  labs(x = NULL, y = "5-year trailing share", color = NULL)

ggsave(
  "../output/council_land_use_roll_call_multi_district_rolling_trends.pdf",
  multi_district_signature_rolling_plot,
  width = 7.8,
  height = 4.6
)

local_member_no_abstain_plot <- annual |>
  transmute(
    query_year,
    matter_count = approval_split_local_member_negative_or_abstain_rows
  ) |>
  ggplot(aes(x = query_year, y = matter_count)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.75, color = "#F8766D") +
  geom_point(size = 1.8, color = "#F8766D") +
  scale_x_continuous(breaks = plot_year_breaks) +
  labs(x = NULL, y = "Matters")

ggsave(
  "../output/council_land_use_local_member_no_abstain_trends.pdf",
  local_member_no_abstain_plot,
  width = 7.8,
  height = 4.6
)

local_member_no_abstain_rolling_plot <- local_member_no_abstain_rolling_year |>
  transmute(
    query_year,
    matter_count = approval_local_member_negative_or_abstain_rolling_5
  ) |>
  filter(!is.na(matter_count)) |>
  ggplot(aes(x = query_year, y = matter_count)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.85, color = "#F8766D") +
  geom_point(size = 1.8, color = "#F8766D") +
  scale_x_continuous(breaks = plot_year_breaks) +
  labs(x = NULL, y = "5-year trailing average of matters")

ggsave(
  "../output/council_land_use_local_member_no_abstain_rolling_trends.pdf",
  local_member_no_abstain_rolling_plot,
  width = 7.8,
  height = 4.6
)

local_member_no_abstain_roll_call_plot <- local_member_no_abstain_signature_year |>
  transmute(
    query_year,
    signature_count = approval_local_member_negative_or_abstain_signature_rows
  ) |>
  ggplot(aes(x = query_year, y = signature_count)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.75, color = "#F8766D") +
  geom_point(size = 1.8, color = "#F8766D") +
  scale_x_continuous(breaks = plot_year_breaks) +
  labs(x = NULL, y = "Roll-call signatures")

ggsave(
  "../output/council_land_use_local_member_no_abstain_roll_call_trends.pdf",
  local_member_no_abstain_roll_call_plot,
  width = 7.8,
  height = 4.6
)

local_member_no_abstain_roll_call_rolling_plot <- local_member_no_abstain_roll_call_rolling_year |>
  transmute(
    query_year,
    signature_count = approval_local_member_negative_or_abstain_signature_rolling_5
  ) |>
  filter(!is.na(signature_count)) |>
  ggplot(aes(x = query_year, y = signature_count)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.85, color = "#F8766D") +
  geom_point(size = 1.8, color = "#F8766D") +
  scale_x_continuous(breaks = plot_year_breaks) +
  labs(x = NULL, y = "5-year trailing average of roll-call signatures")

ggsave(
  "../output/council_land_use_local_member_no_abstain_roll_call_rolling_trends.pdf",
  local_member_no_abstain_roll_call_rolling_plot,
  width = 7.8,
  height = 4.6
)

nonlocal_dissent_trends_plot <- nonlocal_dissent_rolling_year |>
  filter(vote_source_group == "approval_action_detail") |>
  select(
    query_year,
    any_nonlocal_no_abstain_matter_share,
    any_nonlocal_no_abstain_matter_share_rolling_5,
    nonlocal_no_abstain_vote_share,
    nonlocal_no_abstain_vote_share_rolling_5
  ) |>
  pivot_longer(-query_year, names_to = "series", values_to = "share") |>
  mutate(
    metric = case_when(
      str_detect(series, "^any_") ~ "Matters with any nonlocal no/abstain",
      TRUE ~ "Nonlocal no/abstain votes per nonlocal voting member"
    ),
    series_type = if_else(str_detect(series, "rolling_5$"), "5-year trailing", "Annual")
  ) |>
  filter(!is.na(share)) |>
  ggplot(aes(x = query_year, y = share, color = series_type, alpha = series_type)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.8) +
  geom_point(size = 1.5) +
  facet_wrap(~metric, ncol = 1, scales = "free_y") +
  scale_x_continuous(breaks = plot_year_breaks) +
  scale_y_continuous(labels = function(x) paste0(format(round(100 * x, 1), nsmall = 1), "%")) +
  scale_color_manual(values = c("Annual" = "grey65", "5-year trailing" = "#1f78b4")) +
  scale_alpha_manual(values = c("Annual" = 0.55, "5-year trailing" = 1)) +
  labs(x = NULL, y = NULL, color = NULL, alpha = NULL)

ggsave(
  "../output/council_land_use_nonlocal_dissent_trends.pdf",
  nonlocal_dissent_trends_plot,
  width = 7.8,
  height = 6.2
)

nonlocal_dissent_vote_type_plot <- nonlocal_dissent_rolling_year |>
  filter(vote_source_group == "approval_action_detail") |>
  select(
    query_year,
    nonlocal_no_vote_share_rolling_5,
    nonlocal_abstain_vote_share_rolling_5
  ) |>
  pivot_longer(-query_year, names_to = "series", values_to = "share") |>
  mutate(
    series = recode(
      series,
      nonlocal_no_vote_share_rolling_5 = "No votes",
      nonlocal_abstain_vote_share_rolling_5 = "Abstentions"
    )
  ) |>
  filter(!is.na(share)) |>
  ggplot(aes(x = query_year, y = share, color = series)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.85) +
  geom_point(size = 1.7) +
  scale_x_continuous(breaks = plot_year_breaks) +
  scale_y_continuous(labels = function(x) paste0(format(round(100 * x, 1), nsmall = 1), "%")) +
  labs(x = NULL, y = "5-year trailing share of nonlocal voting member votes", color = NULL)

ggsave(
  "../output/council_land_use_nonlocal_dissent_vote_type_trends.pdf",
  nonlocal_dissent_vote_type_plot,
  width = 7.8,
  height = 4.6
)

nonlocal_dissent_pivotality_plot <- nonlocal_dissent_pivotality_period |>
  mutate(
    simple_majority_margin_group = factor(
      simple_majority_margin_group,
      levels = c("<=0", "1-5", "6-10", "11-20", "21+")
    )
  ) |>
  ggplot(aes(x = period, y = matter_share, fill = simple_majority_margin_group)) +
  geom_col(width = 0.72) +
  scale_y_continuous(labels = function(x) paste0(round(100 * x), "%")) +
  labs(
    x = NULL,
    y = "Share of approval matters with nonlocal dissent",
    fill = "Yes votes minus 26"
  )

ggsave(
  "../output/council_land_use_nonlocal_dissent_pivotality.pdf",
  nonlocal_dissent_pivotality_plot,
  width = 7.8,
  height = 4.6
)
