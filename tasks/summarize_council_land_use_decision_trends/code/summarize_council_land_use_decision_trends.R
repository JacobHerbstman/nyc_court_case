# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_council_land_use_decision_trends/code")

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

plot_rate_5 <- local_member_rollcall_adoption_position_year |>
  filter(
    query_year %in% plot_years,
    local_member_rollcall_adoption_position == "opposes_adoption"
  ) |>
  complete(
    query_year = plot_years,
    fill = list(matter_rows = 0L, adopted_rows = 0L, nonadopted_rows = 0L)
  ) |>
  arrange(query_year) |>
  mutate(
    adoption_rate = if_else(matter_rows > 0L, adopted_rows / matter_rows, NA_real_),
    adoption_rate_rolling_5 = rolling_rate_5(adopted_rows, matter_rows)
  )

rate_with_raw_plot <- plot_rate_5 |>
  ggplot(aes(x = query_year)) +
  geom_line(aes(y = adoption_rate), color = "grey70", linewidth = 0.55, na.rm = TRUE) +
  geom_point(aes(y = adoption_rate), color = "grey60", size = 1.4, alpha = 0.8, na.rm = TRUE) +
  geom_line(aes(y = adoption_rate_rolling_5), color = "#d95f02", linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = adoption_rate_rolling_5), color = "#d95f02", size = 1.6, na.rm = TRUE) +
  scale_x_continuous(breaks = seq(1998, 2025, 2), limits = range(plot_years)) +
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
