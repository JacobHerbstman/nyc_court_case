# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_council_land_use_decision_trends/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tidyr)
})

theme_set(theme_minimal(base_size = 11))

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

year <- decision |>
  group_by(query_year) |>
  summarise(
    matter_rows = n(),
    adopted_rows = sum(disposition_group == "adopted"),
    nonadopted_rows = sum(disposition_group != "adopted"),
    main_vote_sample_rows = sum(matter_in_main_vote_sample),
    affected_district_rows = sum(has_affected_council_district),
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
    adopted_share = adopted_rows / matter_rows,
    nonadopted_share = nonadopted_rows / matter_rows,
    main_vote_sample_share = main_vote_sample_rows / matter_rows,
    affected_district_share = affected_district_rows / matter_rows,
    local_member_vote_observed_share = local_member_vote_observed_rows / matter_rows,
    split_vote_share_of_vote_sample = split_vote_rows / main_vote_sample_rows,
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

local_member_rollcall_position_rows <- decision |>
  filter(!as.character(matter_id) %in% c("450009", "444462")) |>
  mutate(
    local_member_rollcall_adoption_position = case_when(
      approval_vote_source & local_member_affirmative_only ~ "supports_adoption",
      approval_vote_source & local_member_negative_or_abstain ~ "opposes_adoption",
      nonapproval_vote_source & local_member_affirmative_only ~ "opposes_adoption",
      nonapproval_vote_source & local_member_negative_or_abstain ~ "supports_adoption",
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
    .groups = "drop"
  ) |>
  mutate(adoption_rate = adopted_rows / matter_rows) |>
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
  mutate(adoption_rate = adopted_events / event_rows) |>
  arrange(query_year, local_member_rollcall_adoption_position)

write_csv(year, "../output/council_land_use_decision_trends_year.csv")
write_csv(
  local_member_rollcall_adoption_position_year,
  "../output/council_land_use_local_member_rollcall_adoption_position_year.csv"
)
write_csv(
  local_member_rollcall_adoption_position_event_year,
  "../output/council_land_use_local_member_rollcall_adoption_position_event_year.csv"
)

plot_rate_5 <- local_member_rollcall_adoption_position_year |>
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
    adoption_rate_rolling_5 = rolling_rate_5(adopted_rows, matter_rows),
    adopted_rows_rolling_5 = rolling_average_5(adopted_rows)
  )

rate_with_raw_plot <- plot_rate_5 |>
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
  rate_with_raw_plot,
  width = 7.5,
  height = 4.5
)

count_with_raw_plot <- plot_rate_5 |>
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
  count_with_raw_plot,
  width = 7.5,
  height = 4.5
)

plot_event_5 <- local_member_rollcall_adoption_position_event_year |>
  filter(
    query_year <= 2024,
    local_member_rollcall_adoption_position == "opposes_adoption"
  ) |>
  complete(
    query_year = seq(min(year$query_year), 2024),
    fill = list(event_rows = 0L, adopted_events = 0L, nonadopted_events = 0L)
  ) |>
  arrange(query_year) |>
  mutate(
    adoption_rate = if_else(event_rows > 0L, adopted_events / event_rows, NA_real_),
    adoption_rate_rolling_5 = rolling_rate_5(adopted_events, event_rows),
    adopted_events_rolling_5 = rolling_average_5(adopted_events)
  )

event_rate_with_raw_plot <- plot_event_5 |>
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
    caption = "Unique land-use events; grey series is the annual raw rate."
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_event_rolling5_with_raw_clean.pdf",
  event_rate_with_raw_plot,
  width = 7.5,
  height = 4.5
)

event_count_with_raw_plot <- plot_event_5 |>
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
    y = "Council adoption count",
    caption = "Unique land-use events; grey series is the annual raw count."
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_event_count_rolling5_with_raw_clean.pdf",
  event_count_with_raw_plot,
  width = 7.5,
  height = 4.5
)
