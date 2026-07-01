suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tidyr)
})

theme_set(theme_minimal(base_size = 11))

plot_years <- 1998:2025

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

rolling_average_5 <- function(value) {
  vapply(
    seq_along(value),
    function(i) {
      if (i < 5L) {
        return(NA_real_)
      }
      mean(value[(i - 4L):i], na.rm = TRUE)
    },
    numeric(1)
  )
}

decision <- read_csv(
  "../input/council_land_use_decision_panel.csv",
  col_types = cols(.default = col_character()),
  na = character()
)

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

local_member_rollcall_position_rows <- decision |>
  filter(!as.character(matter_id) %in% c("450009", "444462")) |>
  filter(disposition_group %in% c("adopted", "disapproved")) |>
  mutate(
    event_id = case_when(
      str_squish(zap_project_ids) != "" ~ paste0("zap:", str_squish(zap_project_ids)),
      str_squish(application_keys) != "" ~ paste0("application:", str_squish(application_keys)),
      TRUE ~ paste0("matter:", matter_id)
    ),
    local_member_rollcall_adoption_position = case_when(
      approval_vote_source & local_member_affirmative_only ~ "supports_adoption",
      approval_vote_source & local_member_negative_or_abstain ~ "opposes_adoption",
      nonapproval_vote_source & local_member_affirmative_only ~ "opposes_adoption",
      nonapproval_vote_source & local_member_negative_or_abstain ~ "supports_adoption",
      TRUE ~ NA_character_
    )
  ) |>
  filter(!is.na(local_member_rollcall_adoption_position))

local_member_rollcall_position_events <- local_member_rollcall_position_rows |>
  group_by(query_year, event_id) |>
  summarise(
    has_adoption_vote = any(approval_vote_source),
    local_member_rollcall_adoption_position = case_when(
      has_adoption_vote & any(approval_vote_source & local_member_negative_or_abstain) ~ "opposes_adoption",
      has_adoption_vote & any(approval_vote_source & local_member_affirmative_only) ~ "supports_adoption",
      any(nonapproval_vote_source & local_member_affirmative_only) ~ "opposes_adoption",
      any(nonapproval_vote_source & local_member_negative_or_abstain) ~ "supports_adoption",
      TRUE ~ NA_character_
    ),
    adopted_event = has_adoption_vote,
    .groups = "drop"
  )

adoption_over_local_member_opposition_year <- local_member_rollcall_position_events |>
  filter(!is.na(local_member_rollcall_adoption_position)) |>
  group_by(query_year) |>
  summarise(
    event_rows = n(),
    override_events = sum(adopted_event & local_member_rollcall_adoption_position == "opposes_adoption"),
    non_override_events = event_rows - override_events,
    .groups = "drop"
  ) |>
  mutate(override_share = override_events / event_rows) |>
  arrange(query_year)

plot_rate_5 <- adoption_over_local_member_opposition_year |>
  filter(query_year %in% plot_years) |>
  complete(
    query_year = plot_years,
    fill = list(event_rows = 0L, override_events = 0L, non_override_events = 0L)
  ) |>
  arrange(query_year) |>
  mutate(
    override_share = if_else(event_rows > 0L, override_events / event_rows, NA_real_),
    override_share_rolling_5 = rolling_rate_5(override_events, event_rows),
    override_events_rolling_5 = rolling_average_5(override_events)
  )

rate_with_raw_plot <- plot_rate_5 |>
  ggplot(aes(x = query_year)) +
  geom_line(aes(y = override_share), color = "grey70", linewidth = 0.55, na.rm = TRUE) +
  geom_point(aes(y = override_share), color = "grey60", size = 1.4, alpha = 0.8, na.rm = TRUE) +
  geom_line(aes(y = override_share_rolling_5), color = "#d95f02", linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = override_share_rolling_5), color = "#d95f02", size = 1.6, na.rm = TRUE) +
  scale_x_continuous(breaks = seq(1998, 2025, 2), limits = range(plot_years)) +
  scale_y_continuous(labels = function(x) paste0(round(100 * x), "%")) +
  labs(
    title = "Trend over time: adopted over local member roll-call opposition",
    x = "Year",
    y = "Share of land-use events (5-year rolling avg.)",
    caption = "Grey series is the annual raw share."
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
  geom_line(aes(y = override_events), color = "grey70", linewidth = 0.55, na.rm = TRUE) +
  geom_point(aes(y = override_events), color = "grey60", size = 1.4, alpha = 0.8, na.rm = TRUE) +
  geom_line(aes(y = override_events_rolling_5), color = "#d95f02", linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = override_events_rolling_5), color = "#d95f02", size = 1.6, na.rm = TRUE) +
  scale_x_continuous(breaks = seq(1998, 2025, 2), limits = range(plot_years)) +
  expand_limits(y = 0) +
  labs(
    title = "Count over time: adopted over local member roll-call opposition",
    x = "Year",
    y = "Land-use events (5-year rolling avg.)",
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

write_csv(
  plot_rate_5,
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.csv",
  na = ""
)
