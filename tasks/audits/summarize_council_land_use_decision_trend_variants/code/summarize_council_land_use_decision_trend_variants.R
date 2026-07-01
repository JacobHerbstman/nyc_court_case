suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tidyr)
})

theme_set(theme_minimal(base_size = 11))
plot_year_breaks <- seq(1998, 2025, 3)

rolling_average_5 <- function(x) {
  vapply(
    seq_along(x),
    function(i) {
      if (i < 5L) {
        return(NA_real_)
      }
      mean(x[(i - 4L):i], na.rm = TRUE)
    },
    numeric(1)
  )
}

rolling_average_4 <- function(x) {
  vapply(
    seq_along(x),
    function(i) {
      if (i < 4L) {
        return(NA_real_)
      }
      mean(x[(i - 3L):i], na.rm = TRUE)
    },
    numeric(1)
  )
}

rolling_average_3 <- function(x) {
  vapply(
    seq_along(x),
    function(i) {
      if (i < 3L) {
        return(NA_real_)
      }
      mean(x[(i - 2L):i], na.rm = TRUE)
    },
    numeric(1)
  )
}

rolling_rate_3 <- function(numerator, denominator) {
  vapply(
    seq_along(numerator),
    function(i) {
      if (i < 3L) {
        return(NA_real_)
      }
      window_denominator <- sum(denominator[(i - 2L):i], na.rm = TRUE)
      if (window_denominator == 0L) {
        return(NA_real_)
      }
      sum(numerator[(i - 2L):i], na.rm = TRUE) / window_denominator
    },
    numeric(1)
  )
}

rolling_rate_4 <- function(numerator, denominator) {
  vapply(
    seq_along(numerator),
    function(i) {
      if (i < 4L) {
        return(NA_real_)
      }
      window_denominator <- sum(denominator[(i - 3L):i], na.rm = TRUE)
      if (window_denominator == 0L) {
        return(NA_real_)
      }
      sum(numerator[(i - 3L):i], na.rm = TRUE) / window_denominator
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

normalize_application_key <- function(x) {
  out <- str_to_upper(coalesce(as.character(x), ""))
  out <- str_replace_all(out, "[^A-Z0-9]", "")
  out <- str_replace(out, "^[A-Z](?=[0-9])", "")
  if_else(out == "", NA_character_, out)
}

canonical_semicolon_values <- function(x, normalize_values = FALSE) {
  vapply(
    str_split(coalesce(as.character(x), ""), ";"),
    function(parts) {
      parts <- str_trim(parts)
      parts <- parts[parts != ""]
      if (normalize_values) {
        parts <- normalize_application_key(parts)
        parts <- parts[!is.na(parts)]
      }
      paste(sort(unique(parts)), collapse = ";")
    },
    character(1)
  )
}

classify_action_code <- function(action_code) {
  case_when(
    action_code %in% c("ZM", "ZR") ~ "Zoning map/text amendments",
    action_code == "ZS" ~ "Zoning special permits",
    action_code %in% c("ZA", "ZC", "ZJ") ~ "Zoning administrative actions",
    action_code %in% c("HA", "HD", "HG", "HC", "HL", "HM", "HO", "HP", "HU") ~ "Housing/urban renewal",
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
    query_year >= 1998 & query_year <= 2002 ~ "1998-2002",
    query_year >= 2003 & query_year <= 2009 ~ "2003-2009",
    query_year >= 2010 & query_year <= 2017 ~ "2010-2017",
    query_year >= 2018 & query_year <= 2025 ~ "2018-2025",
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

decision <- read_csv(
  "../input/council_land_use_decision_panel.csv",
  col_types = cols(.default = col_character()),
  na = character()
)
local_votes <- read_csv(
  "../input/council_land_use_local_member_votes.csv",
  col_types = cols(.default = col_character()),
  na = character()
)
validation <- read_csv(
  "../input/council_land_use_decision_universe_validation_summary.csv",
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
    parsed_vote_rows = suppressWarnings(as.integer(parsed_vote_rows)),
    affirmative_count = suppressWarnings(as.integer(affirmative_count)),
    negative_count = suppressWarnings(as.integer(negative_count)),
    abstain_count = suppressWarnings(as.integer(abstain_count)),
    approval_vote_source = vote_source %in% c(
      "approval_action_detail",
      "approval_action_detail_nonfinal_disposition"
    ),
    nonapproval_vote_source = vote_source == "nonapproval_action_detail",
    local_member_negative_or_abstain = local_member_final_action_vote_status ==
      "local_member_negative_or_abstain",
    local_member_affirmative_only = local_member_final_action_vote_status == "local_member_affirmative_only"
  )

action_key_rows <- decision |>
  transmute(
    matter_id,
    query_year,
    application_key = str_split(coalesce(as.character(application_keys), ""), ";")
  ) |>
  unnest_longer(application_key, keep_empty = TRUE) |>
  mutate(
    application_key = normalize_application_key(application_key),
    action_code = if_else(
      str_detect(application_key, "^[0-9]{6,7}A?[A-Z]{3}$"),
      str_sub(application_key, -3, -2),
      NA_character_
    ),
    action_category = classify_action_code(action_code)
  )

action_category_rows <- action_key_rows |>
  mutate(category_priority = unname(action_category_priority[action_category])) |>
  group_by(matter_id) |>
  arrange(category_priority, .by_group = TRUE) |>
  summarize(
    query_year = first(query_year),
    primary_action_category = first(action_category),
    parsed_application_key_count = sum(!is.na(action_code)),
    distinct_action_category_count = n_distinct(action_category),
    .groups = "drop"
  )

if (nrow(action_category_rows) != n_distinct(action_category_rows$matter_id)) {
  stop("Council action category rows must be unique by matter_id.")
}

action_category_summary <- action_category_rows |>
  count(primary_action_category, name = "matter_rows") |>
  mutate(
    all_matter_rows = sum(matter_rows),
    parsed_matter_rows = sum(matter_rows[primary_action_category != "No parsed application key"]),
    share_all_matter_rows = matter_rows / all_matter_rows,
    share_parsed_matter_rows = if_else(
      primary_action_category == "No parsed application key",
      NA_real_,
      matter_rows / parsed_matter_rows
    )
  ) |>
  arrange(desc(matter_rows))

action_category_period <- action_category_rows |>
  mutate(
    period = period_from_query_year(query_year),
    years_in_period = case_when(
      period == "1998-2002" ~ 5L,
      period == "2003-2009" ~ 7L,
      period == "2010-2017" ~ 8L,
      period == "2018-2025" ~ 8L,
      TRUE ~ NA_integer_
    )
  ) |>
  filter(!is.na(period)) |>
  count(period, years_in_period, primary_action_category, name = "matter_rows") |>
  group_by(period) |>
  mutate(
    period_matter_rows = sum(matter_rows),
    period_parsed_matter_rows = sum(matter_rows[primary_action_category != "No parsed application key"]),
    annual_mean_matter_rows = matter_rows / years_in_period,
    share_all_matter_rows = matter_rows / period_matter_rows,
    share_parsed_matter_rows = if_else(
      primary_action_category == "No parsed application key",
      NA_real_,
      matter_rows / period_parsed_matter_rows
    )
  ) |>
  ungroup() |>
  arrange(period, desc(matter_rows))

action_code_summary <- action_key_rows |>
  filter(!is.na(action_code)) |>
  count(action_code, action_category, name = "application_key_rows") |>
  mutate(
    parsed_application_key_rows = sum(application_key_rows),
    share_parsed_application_key_rows = application_key_rows / parsed_application_key_rows
  ) |>
  arrange(desc(application_key_rows))

local_votes <- local_votes |>
  mutate(
    query_year = suppressWarnings(as.integer(query_year)),
    local_member_vote_found = str_to_lower(local_member_vote_found) == "true"
  )

year <- decision |>
  group_by(query_year) |>
  summarise(
    matter_rows = n(),
    adopted_rows = sum(disposition_group == "adopted"),
    disapproved_rows = sum(disposition_group == "disapproved"),
    filed_by_council_other_rows = sum(disposition_group == "filed_by_council_other"),
    filed_withdrawal_or_motion_rows = sum(disposition_group == "filed_withdrawal_or_motion"),
    filed_end_of_session_rows = sum(disposition_group == "filed_end_of_session"),
    other_nonadopted_rows = sum(!disposition_group %in% c(
      "adopted",
      "disapproved",
      "filed_by_council_other",
      "filed_withdrawal_or_motion",
      "filed_end_of_session"
    )),
    nonadopted_rows = sum(disposition_group != "adopted"),
    main_vote_sample_rows = sum(matter_in_main_vote_sample),
    not_fetched_rows = sum(vote_source == "not_fetched"),
    affected_district_rows = sum(has_affected_council_district),
    local_member_roster_rows = sum(has_local_member_from_roster),
    local_member_vote_observed_rows = sum(has_local_member_vote_observed),
    approval_vote_sample_rows = sum(approval_vote_source),
    nonapproval_vote_sample_rows = sum(nonapproval_vote_source),
    approval_local_member_negative_or_abstain_rows = sum(
      approval_vote_source & local_member_negative_or_abstain
    ),
    approval_local_member_affirmative_only_rows = sum(
      approval_vote_source & local_member_affirmative_only
    ),
    nonapproval_local_member_negative_or_abstain_rows = sum(
      nonapproval_vote_source & local_member_negative_or_abstain
    ),
    nonapproval_local_member_affirmative_only_rows = sum(
      nonapproval_vote_source & local_member_affirmative_only
    ),
    approval_local_member_vote_observed_rows = sum(approval_vote_source & has_local_member_vote_observed),
    nonapproval_local_member_vote_observed_rows = sum(nonapproval_vote_source & has_local_member_vote_observed),
    split_vote_rows = sum(
      matter_in_main_vote_sample &
        (
          coalesce(negative_count, 0L) > 0L |
            coalesce(abstain_count, 0L) > 0L
        )
    ),
    .groups = "drop"
  ) |>
  mutate(
    unanimous_rows = main_vote_sample_rows - split_vote_rows,
    adopted_share = adopted_rows / matter_rows,
    nonadopted_share = nonadopted_rows / matter_rows,
    disapproved_share = disapproved_rows / matter_rows,
    filed_by_council_other_share = filed_by_council_other_rows / matter_rows,
    filed_withdrawal_or_motion_share = filed_withdrawal_or_motion_rows / matter_rows,
    main_vote_sample_share = main_vote_sample_rows / matter_rows,
    not_fetched_share = not_fetched_rows / matter_rows,
    affected_district_share = affected_district_rows / matter_rows,
    local_member_vote_observed_share = local_member_vote_observed_rows / matter_rows,
    split_vote_share_of_vote_sample = split_vote_rows / main_vote_sample_rows,
    unanimous_share_of_vote_sample = unanimous_rows / main_vote_sample_rows,
    approval_local_member_negative_or_abstain_share = if_else(
      approval_local_member_vote_observed_rows > 0,
      approval_local_member_negative_or_abstain_rows / approval_local_member_vote_observed_rows,
      NA_real_
    ),
    nonapproval_local_member_negative_or_abstain_share = if_else(
      nonapproval_local_member_vote_observed_rows > 0,
      nonapproval_local_member_negative_or_abstain_rows / nonapproval_local_member_vote_observed_rows,
      NA_real_
    )
  ) |>
  arrange(query_year)

trend_long <- year |>
  select(
    query_year,
    matter_rows,
    adopted_rows,
    nonadopted_rows,
    disapproved_rows,
    filed_by_council_other_rows,
    filed_withdrawal_or_motion_rows,
    filed_end_of_session_rows,
    main_vote_sample_rows,
    affected_district_rows,
    local_member_vote_observed_rows,
    split_vote_rows,
    unanimous_rows,
    approval_local_member_negative_or_abstain_rows,
    nonapproval_local_member_negative_or_abstain_rows,
    adopted_share,
    nonadopted_share,
    disapproved_share,
    main_vote_sample_share,
    affected_district_share,
    local_member_vote_observed_share,
    split_vote_share_of_vote_sample,
    unanimous_share_of_vote_sample,
    approval_local_member_negative_or_abstain_share,
    nonapproval_local_member_negative_or_abstain_share
  ) |>
  pivot_longer(-query_year, names_to = "outcome_id", values_to = "value")

local_vote_year <- local_votes |>
  group_by(query_year, vote_source, local_member_final_action_vote_category) |>
  summarise(
    local_member_vote_rows = n(),
    matter_rows = n_distinct(matter_id),
    .groups = "drop"
  ) |>
  arrange(query_year, vote_source, local_member_final_action_vote_category)

local_member_rollcall_position_rows <- decision |>
  filter(
    !as.character(matter_id) %in% c("450009", "444462")
  ) |>
  mutate(
    local_member_rollcall_adoption_position = case_when(
      approval_vote_source & local_member_affirmative_only ~ "supports_adoption",
      approval_vote_source & local_member_negative_or_abstain ~ "opposes_adoption",
      nonapproval_vote_source & local_member_affirmative_only ~ "opposes_adoption",
      nonapproval_vote_source & local_member_negative_or_abstain ~ "supports_adoption",
      TRUE ~ NA_character_
    ),
    local_member_rollcall_vote_context = case_when(
      approval_vote_source & local_member_affirmative_only ~ "yes_on_approval_motion",
      approval_vote_source & local_member_negative_or_abstain ~ "no_or_abstain_on_approval_motion",
      nonapproval_vote_source & local_member_affirmative_only ~ "yes_on_nonapproval_motion",
      nonapproval_vote_source & local_member_negative_or_abstain ~ "no_or_abstain_on_nonapproval_motion",
      TRUE ~ NA_character_
    )
  ) |>
  filter(!is.na(local_member_rollcall_adoption_position))

local_member_rollcall_event_rows <- local_member_rollcall_position_rows |>
  mutate(
    canonical_zap_project_ids = canonical_semicolon_values(zap_project_ids),
    canonical_application_keys = canonical_semicolon_values(application_keys, normalize_values = TRUE),
    local_member_rollcall_event_key = case_when(
      canonical_zap_project_ids != "" ~ str_c("zap:", canonical_zap_project_ids),
      canonical_application_keys != "" ~ str_c("app:", canonical_application_keys),
      TRUE ~ str_c("matter:", matter_id)
    )
  ) |>
  group_by(local_member_rollcall_event_key, local_member_rollcall_adoption_position) |>
  summarise(
    query_year = min(query_year, na.rm = TRUE),
    event_matter_rows = n(),
    adopted_event = any(disposition_group == "adopted"),
    nonadopted_event = !adopted_event,
    .groups = "drop"
  )

local_member_rollcall_adoption_position_year <- local_member_rollcall_position_rows |>
  group_by(query_year, local_member_rollcall_adoption_position) |>
  summarise(
    matter_rows = n(),
    adopted_rows = sum(disposition_group == "adopted"),
    nonadopted_rows = sum(disposition_group != "adopted"),
    yes_on_approval_motion_rows = sum(
      local_member_rollcall_vote_context == "yes_on_approval_motion"
    ),
    no_or_abstain_on_approval_motion_rows = sum(
      local_member_rollcall_vote_context == "no_or_abstain_on_approval_motion"
    ),
    yes_on_nonapproval_motion_rows = sum(
      local_member_rollcall_vote_context == "yes_on_nonapproval_motion"
    ),
    no_or_abstain_on_nonapproval_motion_rows = sum(
      local_member_rollcall_vote_context == "no_or_abstain_on_nonapproval_motion"
    ),
    .groups = "drop"
  ) |>
  mutate(
    adoption_rate = adopted_rows / matter_rows
  ) |>
  arrange(query_year, local_member_rollcall_adoption_position)

local_member_rollcall_adoption_position_event_year <- local_member_rollcall_event_rows |>
  group_by(query_year, local_member_rollcall_adoption_position) |>
  summarise(
    event_rows = n(),
    adopted_events = sum(adopted_event),
    nonadopted_events = sum(nonadopted_event),
    source_matter_rows = sum(event_matter_rows),
    .groups = "drop"
  ) |>
  mutate(
    adoption_rate = adopted_events / event_rows
  ) |>
  arrange(query_year, local_member_rollcall_adoption_position)

local_member_rollcall_adoption_position_period <- local_member_rollcall_position_rows |>
  mutate(period = period_from_query_year(query_year)) |>
  filter(!is.na(period)) |>
  group_by(period, local_member_rollcall_adoption_position) |>
  summarise(
    matter_rows = n(),
    adopted_rows = sum(disposition_group == "adopted"),
    nonadopted_rows = sum(disposition_group != "adopted"),
    yes_on_approval_motion_rows = sum(
      local_member_rollcall_vote_context == "yes_on_approval_motion"
    ),
    no_or_abstain_on_approval_motion_rows = sum(
      local_member_rollcall_vote_context == "no_or_abstain_on_approval_motion"
    ),
    yes_on_nonapproval_motion_rows = sum(
      local_member_rollcall_vote_context == "yes_on_nonapproval_motion"
    ),
    no_or_abstain_on_nonapproval_motion_rows = sum(
      local_member_rollcall_vote_context == "no_or_abstain_on_nonapproval_motion"
    ),
    .groups = "drop"
  ) |>
  mutate(
    adoption_rate = adopted_rows / matter_rows
  ) |>
  arrange(period, local_member_rollcall_adoption_position)

pre_post <- trend_long |>
  filter(outcome_id %in% c(
    "matter_rows",
    "adopted_rows",
    "nonadopted_rows",
    "disapproved_rows",
    "filed_by_council_other_rows",
    "filed_withdrawal_or_motion_rows",
    "split_vote_rows",
    "unanimous_rows",
    "approval_local_member_negative_or_abstain_rows",
    "nonapproval_local_member_negative_or_abstain_rows",
    "nonadopted_share",
    "disapproved_share",
    "split_vote_share_of_vote_sample",
    "unanimous_share_of_vote_sample",
    "approval_local_member_negative_or_abstain_share",
    "nonapproval_local_member_negative_or_abstain_share"
  )) |>
  mutate(period = if_else(query_year <= 2001, "1998-2001", "2002-2025")) |>
  group_by(outcome_id, period) |>
  summarise(annual_mean = mean(value, na.rm = TRUE), .groups = "drop") |>
  pivot_wider(names_from = period, values_from = annual_mean) |>
  mutate(
    post_minus_pre = `2002-2025` - `1998-2001`,
    post_over_pre = if_else(`1998-2001` != 0, `2002-2025` / `1998-2001`, NA_real_)
  ) |>
  arrange(outcome_id)

zmzr_vs_docket_year <- year |>
  select(query_year, total_land_use_matters = matter_rows) |>
  left_join(
    action_category_rows |>
      filter(primary_action_category == "Zoning map/text amendments") |>
      count(query_year, name = "zoning_map_text_matters"),
    by = "query_year",
    relationship = "one-to-one"
  ) |>
  mutate(
    zoning_map_text_matters = coalesce(zoning_map_text_matters, 0L),
    non_zoning_map_text_matters = total_land_use_matters - zoning_map_text_matters,
    zoning_map_text_share = zoning_map_text_matters / total_land_use_matters
  ) |>
  arrange(query_year)

write_csv(year, "../output/council_land_use_decision_trends_year.csv")
write_csv(trend_long, "../output/council_land_use_decision_trends_long.csv")
write_csv(local_vote_year, "../output/council_land_use_local_member_vote_trends_year.csv")
write_csv(
  local_member_rollcall_adoption_position_year,
  "../output/council_land_use_local_member_rollcall_adoption_position_year.csv"
)
write_csv(
  local_member_rollcall_adoption_position_event_year,
  "../output/council_land_use_local_member_rollcall_adoption_position_event_year.csv"
)
write_csv(
  local_member_rollcall_adoption_position_period,
  "../output/council_land_use_local_member_rollcall_adoption_position_period.csv"
)
write_csv(pre_post, "../output/council_land_use_pre_post_2002_summary.csv")
write_csv(action_category_summary, "../output/council_land_use_action_category_summary.csv")
write_csv(action_category_period, "../output/council_land_use_action_category_period.csv")
write_csv(action_code_summary, "../output/council_land_use_action_code_summary.csv")
write_csv(zmzr_vs_docket_year, "../output/council_land_use_zmzr_vs_docket_year.csv")

volume_plot <- year |>
  select(query_year, matter_rows, adopted_rows, nonadopted_rows) |>
  pivot_longer(-query_year, names_to = "series", values_to = "matter_count") |>
  mutate(
    series = recode(
      series,
      matter_rows = "All land-use matters",
      adopted_rows = "Adopted",
      nonadopted_rows = "Not adopted"
    )
  ) |>
  ggplot(aes(x = query_year, y = matter_count, color = series)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.75) +
  geom_point(size = 1.6) +
  scale_x_continuous(breaks = plot_year_breaks) +
  labs(x = NULL, y = "Matters", color = NULL)

ggsave(
  "../output/council_land_use_decision_volume_trends.pdf",
  volume_plot,
  width = 7.5,
  height = 4.5
)

nonapproval_plot <- year |>
  select(
    query_year,
    disapproved_rows,
    filed_by_council_other_rows,
    filed_withdrawal_or_motion_rows,
    filed_end_of_session_rows
  ) |>
  pivot_longer(-query_year, names_to = "series", values_to = "matter_count") |>
  mutate(
    series = recode(
      series,
      disapproved_rows = "Disapproved",
      filed_by_council_other_rows = "Filed by Council",
      filed_withdrawal_or_motion_rows = "Withdrawal or motion to file",
      filed_end_of_session_rows = "Filed end of session"
    )
  ) |>
  ggplot(aes(x = query_year, y = matter_count, color = series)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.75) +
  geom_point(size = 1.6) +
  scale_x_continuous(breaks = plot_year_breaks) +
  labs(x = NULL, y = "Matters", color = NULL)

ggsave(
  "../output/council_land_use_nonapproval_trends.pdf",
  nonapproval_plot,
  width = 7.5,
  height = 4.5
)

coverage_plot <- year |>
  select(
    query_year,
    main_vote_sample_share,
    affected_district_share,
    local_member_vote_observed_share
  ) |>
  pivot_longer(-query_year, names_to = "series", values_to = "share") |>
  mutate(
    series = recode(
      series,
      main_vote_sample_share = "Parsed final-action vote detail",
      affected_district_share = "Affected district observed",
      local_member_vote_observed_share = "Local-member vote observed"
    )
  ) |>
  ggplot(aes(x = query_year, y = share, color = series)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.75) +
  geom_point(size = 1.6) +
  scale_x_continuous(breaks = plot_year_breaks) +
  scale_y_continuous(labels = function(x) paste0(round(100 * x), "%")) +
  labs(x = NULL, y = "Share of matters", color = NULL)

ggsave(
  "../output/council_land_use_vote_coverage_trends.pdf",
  coverage_plot,
  width = 7.5,
  height = 4.5
)

local_member_plot <- year |>
  select(
    query_year,
    approval_local_member_negative_or_abstain_rows,
    nonapproval_local_member_negative_or_abstain_rows,
    split_vote_rows
  ) |>
  pivot_longer(-query_year, names_to = "series", values_to = "matter_count") |>
  mutate(
    series = recode(
      series,
      approval_local_member_negative_or_abstain_rows = "Approval: local member negative/abstain",
      nonapproval_local_member_negative_or_abstain_rows = "Non-approval: local member negative/abstain",
      split_vote_rows = "Any split final-action vote"
    )
  ) |>
  ggplot(aes(x = query_year, y = matter_count, color = series)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.75) +
  geom_point(size = 1.6) +
  scale_x_continuous(breaks = plot_year_breaks) +
  labs(x = NULL, y = "Matters", color = NULL)

ggsave(
  "../output/council_land_use_local_member_vote_trends.pdf",
  local_member_plot,
  width = 7.5,
  height = 4.5
)

approval_over_objection_rolling4_plot <- year |>
  arrange(query_year) |>
  transmute(
    query_year,
    annual_matter_count = approval_local_member_negative_or_abstain_rows,
    rolling_4_matter_count = rolling_average_4(approval_local_member_negative_or_abstain_rows)
  ) |>
  ggplot(aes(x = query_year)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(aes(y = annual_matter_count), color = "grey70", linewidth = 0.55) +
  geom_point(aes(y = annual_matter_count), color = "grey60", size = 1.4, alpha = 0.8) +
  geom_line(aes(y = rolling_4_matter_count), color = "#1f78b4", linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = rolling_4_matter_count), color = "#1f78b4", size = 1.6, na.rm = TRUE) +
  scale_x_continuous(breaks = plot_year_breaks) +
  labs(
    x = NULL,
    y = "Approved matters with local member no/abstain",
    caption = "Grey series is the annual raw count. Blue series is the trailing 4-year average."
)

ggsave(
  "../output/council_land_use_approval_over_local_member_objection_rolling4.pdf",
  approval_over_objection_rolling4_plot,
  width = 7.5,
  height = 4.5
)

approval_over_objection_rolling4_clean_plot <- approval_over_objection_rolling4_plot +
  labs(
    title = "Trend over time: approved matters over local member opposition",
    x = "Year",
    y = "Approved matters (4-year rolling avg.)",
    caption = "Grey series is the annual raw count."
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_approval_over_local_member_objection_rolling4_clean.pdf",
  approval_over_objection_rolling4_clean_plot,
  width = 7.5,
  height = 4.5
)

approval_over_objection_rolling5_year <- year |>
  filter(query_year <= 2024) |>
  arrange(query_year) |>
  transmute(
    query_year,
    annual_matter_count = approval_local_member_negative_or_abstain_rows,
    rolling_5_matter_count = rolling_average_5(approval_local_member_negative_or_abstain_rows)
  )

approval_over_objection_rolling5_with_raw_clean_plot <- approval_over_objection_rolling5_year |>
  ggplot(aes(x = query_year)) +
  geom_line(aes(y = annual_matter_count), color = "grey70", linewidth = 0.55) +
  geom_point(aes(y = annual_matter_count), color = "grey60", size = 1.4, alpha = 0.8) +
  geom_line(aes(y = rolling_5_matter_count), color = "#1f78b4", linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = rolling_5_matter_count), color = "#1f78b4", size = 1.6, na.rm = TRUE) +
  scale_x_continuous(breaks = seq(1998, 2024, 2)) +
  labs(
    title = "Trend over time: approved matters over local member opposition",
    x = "Year",
    y = "Approved matters (5-year rolling avg.)",
    caption = "Grey series is the annual raw count."
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_approval_over_local_member_objection_rolling.pdf",
  approval_over_objection_rolling5_with_raw_clean_plot,
  width = 7.5,
  height = 4.5
)

ggsave(
  "../output/council_land_use_approval_over_local_member_objection_rolling5_with_raw_clean.pdf",
  approval_over_objection_rolling5_with_raw_clean_plot,
  width = 7.5,
  height = 4.5
)

approval_over_objection_rolling5_clean_plot <- approval_over_objection_rolling5_year |>
  filter(!is.na(rolling_5_matter_count)) |>
  ggplot(aes(x = query_year, y = rolling_5_matter_count)) +
  geom_line(color = "#1f78b4", linewidth = 0.95) +
  geom_point(color = "#1f78b4", size = 1.6) +
  scale_x_continuous(breaks = seq(1998, 2024, 2)) +
  expand_limits(y = 0) +
  labs(
    title = "Trend over time: approved matters over local member opposition",
    x = "Year",
    y = "Approved matters (5-year rolling avg.)"
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_approval_over_local_member_objection_rolling5_clean.pdf",
  approval_over_objection_rolling5_clean_plot,
  width = 7.5,
  height = 4.5
)

unanimous_vote_rolling_plot <- year |>
  arrange(query_year) |>
  transmute(
    query_year,
    annual_share = unanimous_share_of_vote_sample,
    rolling_5_share = rolling_average_5(unanimous_share_of_vote_sample)
  ) |>
  ggplot(aes(x = query_year)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(aes(y = annual_share), color = "grey70", linewidth = 0.55) +
  geom_point(aes(y = annual_share), color = "grey60", size = 1.4, alpha = 0.8) +
  geom_line(aes(y = rolling_5_share), color = "#1f78b4", linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = rolling_5_share), color = "#1f78b4", size = 1.6, na.rm = TRUE) +
  scale_x_continuous(breaks = plot_year_breaks) +
  scale_y_continuous(labels = function(x) paste0(round(100 * x), "%")) +
  labs(
    x = NULL,
    y = "Share of parsed final-vote matters",
    caption = "Grey series is the annual raw share. Blue series is the trailing 5-year average."
  )

ggsave(
  "../output/council_land_use_unanimous_vote_share_rolling.pdf",
  unanimous_vote_rolling_plot,
  width = 7.5,
  height = 4.5
)

matter_volume_rolling_plot <- year |>
  arrange(query_year) |>
  transmute(
    query_year,
    annual_matter_count = matter_rows,
    rolling_5_matter_count = rolling_average_5(matter_rows)
  ) |>
  ggplot(aes(x = query_year)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(aes(y = annual_matter_count), color = "grey70", linewidth = 0.55) +
  geom_point(aes(y = annual_matter_count), color = "grey60", size = 1.4, alpha = 0.8) +
  geom_line(aes(y = rolling_5_matter_count), color = "#1f78b4", linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = rolling_5_matter_count), color = "#1f78b4", size = 1.6, na.rm = TRUE) +
  scale_x_continuous(breaks = plot_year_breaks) +
  labs(
    x = NULL,
    y = "Land-use matters",
    caption = "Grey series is the annual raw count. Blue series is the trailing 5-year average."
  )

ggsave(
  "../output/council_land_use_matter_volume_rolling.pdf",
  matter_volume_rolling_plot,
  width = 7.5,
  height = 4.5
)

zmzr_vs_docket_plot <- zmzr_vs_docket_year |>
  select(query_year, total_land_use_matters, zoning_map_text_matters) |>
  pivot_longer(-query_year, names_to = "series", values_to = "matter_count") |>
  mutate(
    series = recode(
      series,
      total_land_use_matters = "All Council land-use matters",
      zoning_map_text_matters = "Zoning map/text amendments (ZM/ZR)"
    )
  ) |>
  ggplot(aes(x = query_year, y = matter_count, color = series)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(linewidth = 0.85) +
  geom_point(size = 1.6) +
  scale_x_continuous(breaks = plot_year_breaks) +
  scale_color_manual(values = c(
    "All Council land-use matters" = "grey45",
    "Zoning map/text amendments (ZM/ZR)" = "#1f78b4"
  )) +
  labs(
    x = NULL,
    y = "Matters",
    color = NULL,
    caption = "Annual raw counts; no indexing or smoothing."
  )

ggsave(
  "../output/council_land_use_zmzr_vs_docket_raw.pdf",
  zmzr_vs_docket_plot,
  width = 7.5,
  height = 4.5
)

zmzr_vs_docket_rolling3_plot <- zmzr_vs_docket_year |>
  arrange(query_year) |>
  transmute(
    query_year,
    `All Council land-use matters` = total_land_use_matters,
    `Zoning map/text amendments (ZM/ZR)` = zoning_map_text_matters,
    `All Council land-use matters, trailing 3-year average` =
      rolling_average_3(total_land_use_matters),
    `Zoning map/text amendments (ZM/ZR), trailing 3-year average` =
      rolling_average_3(zoning_map_text_matters)
  ) |>
  pivot_longer(-query_year, names_to = "series", values_to = "matter_count") |>
  mutate(
    smooth_type = if_else(str_detect(series, "trailing 3-year average"), "rolling_3", "annual"),
    base_series = str_remove(series, ", trailing 3-year average$")
  ) |>
  ggplot(aes(x = query_year, y = matter_count, color = base_series)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(
    data = function(x) filter(x, smooth_type == "annual"),
    linewidth = 0.45,
    alpha = 0.35
  ) +
  geom_point(
    data = function(x) filter(x, smooth_type == "annual"),
    size = 1.1,
    alpha = 0.35
  ) +
  geom_line(
    data = function(x) filter(x, smooth_type == "rolling_3"),
    linewidth = 0.95,
    na.rm = TRUE
  ) +
  geom_point(
    data = function(x) filter(x, smooth_type == "rolling_3"),
    size = 1.5,
    na.rm = TRUE
  ) +
  scale_x_continuous(breaks = plot_year_breaks) +
  scale_color_manual(values = c(
    "All Council land-use matters" = "grey45",
    "Zoning map/text amendments (ZM/ZR)" = "#1f78b4"
  )) +
  labs(
    x = NULL,
    y = "Matters",
    color = NULL,
    caption = "Muted lines are annual raw counts. Dark lines are trailing 3-year averages."
  )

ggsave(
  "../output/council_land_use_zmzr_vs_docket_rolling3.pdf",
  zmzr_vs_docket_rolling3_plot,
  width = 7.5,
  height = 4.5
)

local_member_rollcall_position_plot <- local_member_rollcall_adoption_position_year |>
  complete(
    query_year = seq(min(year$query_year), max(year$query_year)),
    local_member_rollcall_adoption_position = c("supports_adoption", "opposes_adoption"),
    fill = list(matter_rows = 0L, adopted_rows = 0L, nonadopted_rows = 0L)
  ) |>
  arrange(local_member_rollcall_adoption_position, query_year) |>
  group_by(local_member_rollcall_adoption_position) |>
  mutate(
    adoption_rate = if_else(matter_rows > 0L, adopted_rows / matter_rows, NA_real_),
    adoption_rate_rolling_3 = rolling_rate_3(adopted_rows, matter_rows)
  ) |>
  ungroup() |>
  mutate(
    local_member_rollcall_adoption_position = recode(
      local_member_rollcall_adoption_position,
      supports_adoption = "Local member roll call supports adoption",
      opposes_adoption = "Local member roll call opposes adoption"
    )
  ) |>
  ggplot(aes(x = query_year, color = local_member_rollcall_adoption_position)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(aes(y = adoption_rate), linewidth = 0.45, alpha = 0.35) +
  geom_point(aes(y = adoption_rate), size = 1.1, alpha = 0.35, na.rm = TRUE) +
  geom_line(aes(y = adoption_rate_rolling_3), linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = adoption_rate_rolling_3), size = 1.5, na.rm = TRUE) +
  scale_x_continuous(breaks = plot_year_breaks) +
  scale_y_continuous(labels = function(x) paste0(round(100 * x), "%")) +
  scale_color_manual(values = c(
    "Local member roll call supports adoption" = "#1b9e77",
    "Local member roll call opposes adoption" = "#d95f02"
  )) +
  labs(
    x = NULL,
    y = "Council adoption rate",
    color = NULL,
    caption = "Dark = trailing 3-year aggregate."
  )

ggsave(
  "../output/council_land_use_adoption_by_local_member_rollcall_position_rolling3.pdf",
  local_member_rollcall_position_plot,
  width = 7.5,
  height = 4.5
)

local_member_rollcall_position_rolling5_exclude2025_plot <- local_member_rollcall_adoption_position_year |>
  filter(query_year <= 2024) |>
  complete(
    query_year = seq(min(year$query_year), 2024),
    local_member_rollcall_adoption_position = c("supports_adoption", "opposes_adoption"),
    fill = list(matter_rows = 0L, adopted_rows = 0L, nonadopted_rows = 0L)
  ) |>
  arrange(local_member_rollcall_adoption_position, query_year) |>
  group_by(local_member_rollcall_adoption_position) |>
  mutate(
    adoption_rate = if_else(matter_rows > 0L, adopted_rows / matter_rows, NA_real_),
    adoption_rate_rolling_5 = rolling_rate_5(adopted_rows, matter_rows)
  ) |>
  ungroup() |>
  mutate(
    local_member_rollcall_adoption_position = recode(
      local_member_rollcall_adoption_position,
      supports_adoption = "Local member roll call supports adoption",
      opposes_adoption = "Local member roll call opposes adoption"
    )
  ) |>
  ggplot(aes(x = query_year, color = local_member_rollcall_adoption_position)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(aes(y = adoption_rate), linewidth = 0.45, alpha = 0.35) +
  geom_point(aes(y = adoption_rate), size = 1.1, alpha = 0.35, na.rm = TRUE) +
  geom_line(aes(y = adoption_rate_rolling_5), linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = adoption_rate_rolling_5), size = 1.5, na.rm = TRUE) +
  scale_x_continuous(breaks = seq(1998, 2024, 3)) +
  scale_y_continuous(labels = function(x) paste0(round(100 * x), "%")) +
  scale_color_manual(values = c(
    "Local member roll call supports adoption" = "#1b9e77",
    "Local member roll call opposes adoption" = "#d95f02"
  )) +
  labs(
    x = NULL,
    y = "Council adoption rate",
    color = NULL,
    caption = "Dark = trailing 5-year aggregate. 2025 excluded."
  )

ggsave(
  "../output/council_land_use_adoption_by_local_member_rollcall_position_rolling5_exclude2025.pdf",
  local_member_rollcall_position_rolling5_exclude2025_plot,
  width = 7.5,
  height = 4.5
)

local_member_rollcall_opposition_rolling5_exclude2025_plot <- local_member_rollcall_adoption_position_year |>
  filter(
    query_year <= 2024,
    local_member_rollcall_adoption_position == "opposes_adoption"
  ) |>
  complete(
    query_year = seq(min(year$query_year), 2024),
    fill = list(matter_rows = 0L, adopted_rows = 0L, nonadopted_rows = 0L)
  ) |>
  arrange(query_year) |>
  mutate(
    adoption_rate_rolling_5 = rolling_rate_5(adopted_rows, matter_rows)
  ) |>
  filter(!is.na(adoption_rate_rolling_5)) |>
  ggplot(aes(x = query_year, y = adoption_rate_rolling_5)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(color = "#d95f02", linewidth = 0.95) +
  geom_point(color = "#d95f02", size = 1.6) +
  scale_x_continuous(breaks = seq(2002, 2024, 2)) +
  scale_y_continuous(
    labels = function(x) paste0(round(100 * x), "%"),
    breaks = seq(0, 0.45, 0.05)
  ) +
  expand_limits(y = 0) +
  labs(
    x = NULL,
    y = "Council adoption rate",
    caption = "Trailing 5-year aggregate among matters where the local member roll call opposes adoption. 2025 excluded."
  )

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_exclude2025.pdf",
  local_member_rollcall_opposition_rolling5_exclude2025_plot,
  width = 7.5,
  height = 4.5
)

local_member_rollcall_opposition_rolling4_plot <- local_member_rollcall_adoption_position_year |>
  filter(local_member_rollcall_adoption_position == "opposes_adoption") |>
  complete(
    query_year = seq(min(year$query_year), max(year$query_year)),
    fill = list(matter_rows = 0L, adopted_rows = 0L, nonadopted_rows = 0L)
  ) |>
  arrange(query_year) |>
  mutate(
    adoption_rate_rolling_4 = rolling_rate_4(adopted_rows, matter_rows)
  ) |>
  filter(!is.na(adoption_rate_rolling_4)) |>
  ggplot(aes(x = query_year, y = adoption_rate_rolling_4)) +
  geom_vline(xintercept = 2002, linetype = "dashed", linewidth = 0.35, color = "grey45") +
  geom_line(color = "#d95f02", linewidth = 0.95) +
  geom_point(color = "#d95f02", size = 1.6) +
  scale_x_continuous(breaks = seq(2001, max(year$query_year), 2)) +
  scale_y_continuous(
    labels = function(x) paste0(round(100 * x), "%"),
    breaks = seq(0, 0.45, 0.05)
  ) +
  expand_limits(y = 0) +
  labs(
    x = NULL,
    y = "Council adoption rate",
    caption = "Trailing 4-year aggregate among matters where the local member roll call opposes adoption."
  )

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling4.pdf",
  local_member_rollcall_opposition_rolling4_plot,
  width = 7.5,
  height = 4.5
)

local_member_rollcall_opposition_rolling4_clean_plot <-
  local_member_rollcall_opposition_rolling4_plot +
  labs(
    title = "Trend over time: adoption over local member roll-call opposition",
    x = "Year",
    y = "Council adoption rate (4-year rolling avg.)",
    caption = "Matters where the local member roll call opposes adoption."
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling4_clean.pdf",
  local_member_rollcall_opposition_rolling4_clean_plot,
  width = 7.5,
  height = 4.5
)

local_member_rollcall_opposition_rolling5_year <- local_member_rollcall_adoption_position_year |>
  filter(
    query_year <= 2024,
    local_member_rollcall_adoption_position == "opposes_adoption"
  ) |>
  complete(
    query_year = seq(min(year$query_year), 2024),
    fill = list(matter_rows = 0L, adopted_rows = 0L, nonadopted_rows = 0L)
  ) |>
  arrange(query_year) |>
  mutate(
    adoption_rate = if_else(matter_rows > 0L, adopted_rows / matter_rows, NA_real_),
    adoption_rate_rolling_5 = rolling_rate_5(adopted_rows, matter_rows)
  )

local_member_rollcall_opposition_rolling5_with_raw_clean_plot <-
  local_member_rollcall_opposition_rolling5_year |>
  ggplot(aes(x = query_year)) +
  geom_line(aes(y = adoption_rate), color = "grey70", linewidth = 0.55, na.rm = TRUE) +
  geom_point(aes(y = adoption_rate), color = "grey60", size = 1.4, alpha = 0.8, na.rm = TRUE) +
  geom_line(aes(y = adoption_rate_rolling_5), color = "#d95f02", linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = adoption_rate_rolling_5), color = "#d95f02", size = 1.6, na.rm = TRUE) +
  scale_x_continuous(breaks = seq(1998, 2024, 2)) +
  scale_y_continuous(labels = function(x) paste0(round(100 * x), "%")) +
  labs(
    title = "Trend over time: adoption over local member roll-call opposition",
    x = "Year",
    y = "Council adoption rate (5-year rolling avg.)",
    caption = "Grey series is the annual raw rate."
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.pdf",
  local_member_rollcall_opposition_rolling5_with_raw_clean_plot,
  width = 7.5,
  height = 4.5
)

local_member_rollcall_opposition_rolling5_clean_plot <-
  local_member_rollcall_opposition_rolling5_year |>
  filter(!is.na(adoption_rate_rolling_5)) |>
  ggplot(aes(x = query_year, y = adoption_rate_rolling_5)) +
  geom_line(color = "#d95f02", linewidth = 0.95) +
  geom_point(color = "#d95f02", size = 1.6) +
  scale_x_continuous(breaks = seq(2002, 2024, 2)) +
  scale_y_continuous(
    labels = function(x) paste0(round(100 * x), "%"),
    breaks = seq(0, 0.45, 0.05)
  ) +
  expand_limits(y = 0) +
  labs(
    title = "Trend over time: adoption over local member roll-call opposition",
    x = "Year",
    y = "Council adoption rate (5-year rolling avg.)"
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5.pdf",
  local_member_rollcall_opposition_rolling5_clean_plot,
  width = 7.5,
  height = 4.5
)

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_clean.pdf",
  local_member_rollcall_opposition_rolling5_clean_plot,
  width = 7.5,
  height = 4.5
)

local_member_rollcall_opposition_count_rolling5_year <-
  local_member_rollcall_opposition_rolling5_year |>
  mutate(
    adopted_rows_rolling_5 = rolling_average_5(adopted_rows)
  )

local_member_rollcall_opposition_count_rolling3_with_raw_clean_plot <-
  local_member_rollcall_opposition_count_rolling5_year |>
  mutate(
    adopted_rows_rolling_3 = rolling_average_3(adopted_rows)
  ) |>
  ggplot(aes(x = query_year)) +
  geom_line(aes(y = adopted_rows), color = "grey70", linewidth = 0.55, na.rm = TRUE) +
  geom_point(aes(y = adopted_rows), color = "grey60", size = 1.4, alpha = 0.8, na.rm = TRUE) +
  geom_line(aes(y = adopted_rows_rolling_3), color = "#d95f02", linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = adopted_rows_rolling_3), color = "#d95f02", size = 1.6, na.rm = TRUE) +
  scale_x_continuous(breaks = seq(1998, 2024, 2)) +
  expand_limits(y = 0) +
  labs(
    title = "Trend over time: adoption over local member roll-call opposition",
    x = "Year",
    y = "Council adoption count",
    caption = "Grey series is the annual raw count."
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling3_with_raw_clean.pdf",
  local_member_rollcall_opposition_count_rolling3_with_raw_clean_plot,
  width = 7.5,
  height = 4.5
)

local_member_rollcall_opposition_count_rolling4_with_raw_clean_plot <-
  local_member_rollcall_opposition_count_rolling5_year |>
  mutate(
    adopted_rows_rolling_4 = rolling_average_4(adopted_rows)
  ) |>
  ggplot(aes(x = query_year)) +
  geom_line(aes(y = adopted_rows), color = "grey70", linewidth = 0.55, na.rm = TRUE) +
  geom_point(aes(y = adopted_rows), color = "grey60", size = 1.4, alpha = 0.8, na.rm = TRUE) +
  geom_line(aes(y = adopted_rows_rolling_4), color = "#d95f02", linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = adopted_rows_rolling_4), color = "#d95f02", size = 1.6, na.rm = TRUE) +
  scale_x_continuous(breaks = seq(1998, 2024, 2)) +
  expand_limits(y = 0) +
  labs(
    title = "Trend over time: adoption over local member roll-call opposition",
    x = "Year",
    y = "Council adoption count",
    caption = "Grey series is the annual raw count."
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling4_with_raw_clean.pdf",
  local_member_rollcall_opposition_count_rolling4_with_raw_clean_plot,
  width = 7.5,
  height = 4.5
)

local_member_rollcall_opposition_count_rolling5_with_raw_clean_plot <-
  local_member_rollcall_opposition_count_rolling5_year |>
  ggplot(aes(x = query_year)) +
  geom_line(aes(y = adopted_rows), color = "grey70", linewidth = 0.55, na.rm = TRUE) +
  geom_point(aes(y = adopted_rows), color = "grey60", size = 1.4, alpha = 0.8, na.rm = TRUE) +
  geom_line(aes(y = adopted_rows_rolling_5), color = "#d95f02", linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = adopted_rows_rolling_5), color = "#d95f02", size = 1.6, na.rm = TRUE) +
  scale_x_continuous(breaks = seq(1998, 2024, 2)) +
  expand_limits(y = 0) +
  labs(
    title = "Trend over time: adoption over local member roll-call opposition",
    x = "Year",
    y = "Council adoption count",
    caption = "Grey series is the annual raw count."
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling5_with_raw_clean.pdf",
  local_member_rollcall_opposition_count_rolling5_with_raw_clean_plot,
  width = 7.5,
  height = 4.5
)

local_member_rollcall_opposition_count_rolling5_clean_plot <-
  local_member_rollcall_opposition_count_rolling5_year |>
  filter(!is.na(adopted_rows_rolling_5)) |>
  ggplot(aes(x = query_year, y = adopted_rows_rolling_5)) +
  geom_line(color = "#d95f02", linewidth = 0.95) +
  geom_point(color = "#d95f02", size = 1.6) +
  scale_x_continuous(breaks = seq(2002, 2024, 2)) +
  expand_limits(y = 0) +
  labs(
    title = "Trend over time: adoption over local member roll-call opposition",
    x = "Year",
    y = "Council adoption count (5-year rolling avg.)"
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling5_clean.pdf",
  local_member_rollcall_opposition_count_rolling5_clean_plot,
  width = 7.5,
  height = 4.5
)

local_member_rollcall_opposition_event_rolling5_year <-
  local_member_rollcall_adoption_position_event_year |>
  filter(
    query_year <= 2024,
    local_member_rollcall_adoption_position == "opposes_adoption"
  ) |>
  complete(
    query_year = seq(min(year$query_year), 2024),
    fill = list(event_rows = 0L, adopted_events = 0L, nonadopted_events = 0L, source_matter_rows = 0L)
  ) |>
  arrange(query_year) |>
  mutate(
    adoption_rate = if_else(event_rows > 0L, adopted_events / event_rows, NA_real_),
    adoption_rate_rolling_5 = rolling_rate_5(adopted_events, event_rows),
    adopted_events_rolling_5 = rolling_average_5(adopted_events)
  )

local_member_rollcall_opposition_event_rolling5_with_raw_clean_plot <-
  local_member_rollcall_opposition_event_rolling5_year |>
  ggplot(aes(x = query_year)) +
  geom_line(aes(y = adoption_rate), color = "grey70", linewidth = 0.55, na.rm = TRUE) +
  geom_point(aes(y = adoption_rate), color = "grey60", size = 1.4, alpha = 0.8, na.rm = TRUE) +
  geom_line(aes(y = adoption_rate_rolling_5), color = "#d95f02", linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = adoption_rate_rolling_5), color = "#d95f02", size = 1.6, na.rm = TRUE) +
  scale_x_continuous(breaks = seq(1998, 2024, 2)) +
  scale_y_continuous(labels = function(x) paste0(round(100 * x), "%")) +
  labs(
    title = "Trend over time: adoption over local member roll-call opposition",
    x = "Year",
    y = "Council adoption rate (unique events, 5-year rolling avg.)",
    caption = "Grey series is the annual raw event rate."
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_event_rolling5_with_raw_clean.pdf",
  local_member_rollcall_opposition_event_rolling5_with_raw_clean_plot,
  width = 7.5,
  height = 4.5
)

local_member_rollcall_opposition_event_rolling5_clean_plot <-
  local_member_rollcall_opposition_event_rolling5_year |>
  filter(!is.na(adoption_rate_rolling_5)) |>
  ggplot(aes(x = query_year, y = adoption_rate_rolling_5)) +
  geom_line(color = "#d95f02", linewidth = 0.95) +
  geom_point(color = "#d95f02", size = 1.6) +
  scale_x_continuous(breaks = seq(2002, 2024, 2)) +
  scale_y_continuous(
    labels = function(x) paste0(round(100 * x), "%"),
    breaks = seq(0, 0.45, 0.05)
  ) +
  expand_limits(y = 0) +
  labs(
    title = "Trend over time: adoption over local member roll-call opposition",
    x = "Year",
    y = "Council adoption rate (unique events, 5-year rolling avg.)"
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_event_rolling5_clean.pdf",
  local_member_rollcall_opposition_event_rolling5_clean_plot,
  width = 7.5,
  height = 4.5
)

local_member_rollcall_opposition_event_count_rolling5_with_raw_clean_plot <-
  local_member_rollcall_opposition_event_rolling5_year |>
  ggplot(aes(x = query_year)) +
  geom_line(aes(y = adopted_events), color = "grey70", linewidth = 0.55, na.rm = TRUE) +
  geom_point(aes(y = adopted_events), color = "grey60", size = 1.4, alpha = 0.8, na.rm = TRUE) +
  geom_line(aes(y = adopted_events_rolling_5), color = "#d95f02", linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = adopted_events_rolling_5), color = "#d95f02", size = 1.6, na.rm = TRUE) +
  scale_x_continuous(breaks = seq(1998, 2024, 2)) +
  expand_limits(y = 0) +
  labs(
    title = "Trend over time: adoption over local member roll-call opposition",
    x = "Year",
    y = "Council adoption count (unique events)",
    caption = "Grey series is the annual raw event count."
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_event_count_rolling5_with_raw_clean.pdf",
  local_member_rollcall_opposition_event_count_rolling5_with_raw_clean_plot,
  width = 7.5,
  height = 4.5
)

local_member_rollcall_opposition_event_count_rolling5_clean_plot <-
  local_member_rollcall_opposition_event_rolling5_year |>
  filter(!is.na(adopted_events_rolling_5)) |>
  ggplot(aes(x = query_year, y = adopted_events_rolling_5)) +
  geom_line(color = "#d95f02", linewidth = 0.95) +
  geom_point(color = "#d95f02", size = 1.6) +
  scale_x_continuous(breaks = seq(2002, 2024, 2)) +
  expand_limits(y = 0) +
  labs(
    title = "Trend over time: adoption over local member roll-call opposition",
    x = "Year",
    y = "Council adoption count (unique events, 5-year rolling avg.)"
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_event_count_rolling5_clean.pdf",
  local_member_rollcall_opposition_event_count_rolling5_clean_plot,
  width = 7.5,
  height = 4.5
)
