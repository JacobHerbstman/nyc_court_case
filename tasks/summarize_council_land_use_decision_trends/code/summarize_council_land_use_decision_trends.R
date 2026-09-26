# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_council_land_use_decision_trends/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tidyr)
})

source("../../_lib/data_reports.R")

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
    query_year = as.integer(query_year),
    n_affected_districts = as.integer(n_affected_districts)
  )

# One row per land-use event (companion matters linked by ZAP project id or application
# key within a query year; see build_council_land_use_decision_panel). The sample is
# adopted or disapproved matters whose local members' roll-call votes give a project
# position. As for matters touching several districts, one local no vote on any of the
# event's matters makes the event locally opposed. Events whose matters disagree on the
# outcome are counted in the log and left out.
events <- decision |>
  filter(project_outcome != "", local_member_project_position != "") |>
  group_by(query_year, event_id) |>
  summarise(
    position = if_else(any(local_member_project_position == "opposes"), "opposes", "supports"),
    mixed_position = n_distinct(local_member_project_position) > 1,
    outcome = if_else(n_distinct(project_outcome) == 1, first(project_outcome), "conflicting"),
    max_affected_districts = max(n_affected_districts),
    .groups = "drop"
  )
message(
  "Events with a local-member position: ", nrow(events),
  "; with both supporting and opposing matters (counted as opposed): ", sum(events$mixed_position),
  "; conflicting outcome (left out): ", sum(events$outcome == "conflicting")
)

events_year <- events |>
  filter(outcome != "conflicting") |>
  group_by(query_year) |>
  summarise(
    event_rows = n(),
    opposed_events = sum(position == "opposes"),
    override_events = sum(position == "opposes" & outcome == "approved"),
    override_events_multi_district = sum(position == "opposes" & outcome == "approved" & max_affected_districts > 1),
    .groups = "drop"
  )

plot_rate_5 <- events_year |>
  filter(query_year %in% plot_years) |>
  complete(
    query_year = plot_years,
    fill = list(event_rows = 0L, opposed_events = 0L, override_events = 0L, override_events_multi_district = 0L)
  ) |>
  arrange(query_year) |>
  mutate(
    non_override_events = event_rows - override_events,
    override_share = if_else(event_rows > 0L, override_events / event_rows, NA_real_),
    override_share_rolling_5 = rolling_rate_5(override_events, event_rows),
    override_events_rolling_5 = rolling_average_5(override_events),
    overruled_share_of_opposed = if_else(opposed_events > 0L, override_events / opposed_events, NA_real_),
    overruled_share_of_opposed_rolling_5 = rolling_rate_5(override_events, opposed_events)
  ) |>
  select(
    query_year, event_rows, override_events, non_override_events, override_share, override_share_rolling_5,
    override_events_rolling_5, opposed_events, overruled_share_of_opposed, overruled_share_of_opposed_rolling_5,
    override_events_multi_district
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

opposed_plot <- plot_rate_5 |>
  ggplot(aes(x = query_year)) +
  geom_line(aes(y = overruled_share_of_opposed), color = "grey70", linewidth = 0.55, na.rm = TRUE) +
  geom_point(aes(y = overruled_share_of_opposed), color = "grey60", size = 1.4, alpha = 0.8, na.rm = TRUE) +
  geom_line(aes(y = overruled_share_of_opposed_rolling_5), color = "#d95f02", linewidth = 0.95, na.rm = TRUE) +
  geom_point(aes(y = overruled_share_of_opposed_rolling_5), color = "#d95f02", size = 1.6, na.rm = TRUE) +
  scale_x_continuous(breaks = seq(1998, 2025, 2), limits = range(plot_years)) +
  scale_y_continuous(labels = function(x) paste0(round(100 * x), "%"), limits = c(0, 1)) +
  labs(
    title = "Adopted despite a local member's roll-call opposition",
    x = "Year",
    y = "Share of locally opposed events adopted (5-year rolling)",
    caption = "Grey series is the annual raw share; denominators are in the matching CSV."
  ) +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_adoption_share_among_local_member_rollcall_opposition_rolling5_with_raw_clean.pdf",
  opposed_plot,
  width = 7.5,
  height = 4.5
)

save_csv(
  plot_rate_5,
  "../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.csv",
  key = "query_year"
)

# Pool years by Council term. Terms are four years except the two-year 2022-2023 and
# 2024-2025 terms that followed the 2020 redistricting.
term_starts <- c(1998, 2002, 2006, 2010, 2014, 2018, 2022, 2024)
term_labels <- c("1998-2001", "2002-2005", "2006-2009", "2010-2013", "2014-2017", "2018-2021", "2022-2023", "2024-2025")

by_term <- events_year |>
  filter(query_year %in% plot_years) |>
  mutate(council_term = term_labels[findInterval(query_year, term_starts)]) |>
  group_by(council_term) |>
  summarise(
    event_rows = sum(event_rows),
    opposed_events = sum(opposed_events),
    override_events = sum(override_events),
    .groups = "drop"
  ) |>
  complete(council_term = term_labels, fill = list(event_rows = 0L, opposed_events = 0L, override_events = 0L)) |>
  mutate(
    council_term = factor(council_term, levels = term_labels),
    overruled_share_of_opposed = if_else(opposed_events > 0L, override_events / opposed_events, NA_real_),
    # Exact (Clopper-Pearson) binomial interval; the counts are small.
    ci_low = if_else(opposed_events > 0L, qbeta(0.025, override_events, opposed_events - override_events + 1), NA_real_),
    ci_high = if_else(opposed_events > 0L, qbeta(0.975, override_events + 1, opposed_events - override_events), NA_real_),
    ci_low = if_else(override_events == 0L, 0, ci_low),
    ci_high = if_else(override_events == opposed_events, 1, ci_high)
  ) |>
  arrange(council_term)

stopifnot(sum(by_term$opposed_events) == sum(events_year$opposed_events[events_year$query_year %in% plot_years]))

term_plot <- by_term |>
  ggplot(aes(x = council_term, y = overruled_share_of_opposed)) +
  geom_errorbar(aes(ymin = ci_low, ymax = ci_high), width = 0.15, color = "grey60", na.rm = TRUE) +
  geom_point(color = "#d95f02", size = 2.6, na.rm = TRUE) +
  geom_text(aes(y = 1.07, label = paste0(override_events, " of ", opposed_events)), size = 3.3) +
  scale_y_continuous(
    labels = function(x) paste0(round(100 * x), "%"),
    breaks = seq(0, 1, 0.25),
    limits = c(0, 1.1)
  ) +
  labs(
    title = "Adopted despite a local member's roll-call opposition, by Council term",
    x = "Council term",
    y = "Share of locally opposed events adopted",
    caption = "Labels: events adopted over local opposition of events with local roll-call opposition. Bars: exact 95% binomial intervals."
  ) +
  theme(
    panel.grid.major.x = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave(
  "../output/council_land_use_adoption_share_among_local_member_rollcall_opposition_by_term.pdf",
  term_plot,
  width = 7.5,
  height = 4.5
)

save_csv(
  by_term,
  "../output/council_land_use_adoption_share_among_local_member_rollcall_opposition_by_term.csv",
  key = "council_term"
)
