# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_text_cpc_trends/code")
# start_year <- 1975
# end_year <- 2025
# moving_window_years <- 3
# minimum_documents_per_moving_window <- 20

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tibble)
  library(tidyr)
})

options(warn = 1)

cli_args <- commandArgs(trailingOnly = TRUE)
if (length(cli_args) != 4) {
  stop(
    "Expected START_YEAR, END_YEAR, MOVING_WINDOW_YEARS, and MINIMUM_DOCUMENTS_PER_MOVING_WINDOW."
  )
}

start_year <- suppressWarnings(as.integer(cli_args[[1]]))
end_year <- suppressWarnings(as.integer(cli_args[[2]]))
moving_window_years <- suppressWarnings(as.integer(cli_args[[3]]))
minimum_documents_per_moving_window <- suppressWarnings(as.integer(cli_args[[4]]))

if (
  any(is.na(c(
    start_year,
    end_year,
    moving_window_years,
    minimum_documents_per_moving_window
  ))) ||
  end_year < start_year ||
  minimum_documents_per_moving_window < 1
) {
  stop("Year and moving-average arguments must be valid integers.")
}
if (moving_window_years < 1 || moving_window_years %% 2 == 0) {
  stop("MOVING_WINDOW_YEARS must be a positive odd integer.")
}

signal_families <- c(
  "substantial_local_opposition",
  "local_request_condition",
  "revision_or_concession",
  "procedural_response",
  "explicit_local_response",
  "approved_unresolved_objection",
  "cb_request_or_opposition",
  "bp_request_or_opposition",
  "councilmember_support_or_request",
  "councilmember_opposition",
  "civic_group_support_or_request",
  "civic_group_opposition",
  "affordability_displacement",
  "traffic_parking",
  "scale_character_preservation",
  "infrastructure_services",
  "environment_open_space",
  "restrictive_declaration",
  "points_of_agreement"
)

signal_labels <- c(
  substantial_local_opposition = "Substantial local opposition",
  local_request_condition = "Substantive local request",
  revision_or_concession = "Revision/concession",
  procedural_response = "Procedural response",
  explicit_local_response = "Explicit response to local actor",
  approved_unresolved_objection = "Approved with objection unresolved",
  cb_request_or_opposition = "CB request/opposition",
  bp_request_or_opposition = "BP request/opposition",
  councilmember_support_or_request = "Councilmember support/request",
  councilmember_opposition = "Councilmember opposition",
  civic_group_support_or_request = "Civic-group support/request",
  civic_group_opposition = "Civic-group opposition",
  affordability_displacement = "Affordability/displacement",
  traffic_parking = "Traffic/parking",
  scale_character_preservation = "Scale/character/preservation",
  infrastructure_services = "Infrastructure/services",
  environment_open_space = "Environment/open space",
  restrictive_declaration = "Restrictive declaration",
  points_of_agreement = "Points of agreement"
)

count_fields <- c(
  "cpc_support_speakers",
  "cpc_opposition_speakers",
  "cb_support_votes",
  "cb_opposition_votes"
)

count_labels <- c(
  cpc_support_speakers = "CPC speakers in support",
  cpc_opposition_speakers = "CPC speakers in opposition",
  cb_support_votes = "CB votes supporting approval",
  cb_opposition_votes = "CB votes supporting disapproval"
)

text_labels <- read_csv(
  "../output/ulurp_cpc_text_labels.csv",
  col_types = cols(.default = col_character()),
  show_col_types = FALSE,
  na = c("", "NA")
) |>
  mutate(
    year = suppressWarnings(as.integer(year)),
    narrative_word_count = suppressWarnings(as.integer(narrative_word_count)),
    across(all_of(count_fields), ~ suppressWarnings(as.integer(.x))),
    councilmember_support_or_request = as.integer(councilmember_position == "support_or_request"),
    councilmember_opposition = as.integer(councilmember_position == "opposition"),
    civic_group_support_or_request = as.integer(civic_group_position == "support_or_request"),
    civic_group_opposition = as.integer(civic_group_position == "opposition"),
    across(all_of(signal_families), ~ suppressWarnings(as.integer(.x)))
  ) |>
  filter(year >= start_year, year <= end_year)

if (nrow(text_labels) == 0 || nrow(text_labels) != n_distinct(text_labels$document_id)) {
  stop("Text labels must be nonempty and unique by document_id.")
}
if (
  any(is.na(as.matrix(text_labels[signal_families]))) ||
  any(!as.matrix(text_labels[signal_families]) %in% c(0L, 1L))
) {
  stop("Document-level text signals must be complete binary indicators.")
}
if (
  any(!text_labels$councilmember_position %in% c("none_or_procedural", "support_or_request", "opposition")) ||
  any(!text_labels$civic_group_position %in% c("none_or_procedural", "support_or_request", "opposition"))
) {
  stop("Actor-position fields contain an invalid value.")
}
if (
  any(!text_labels$analysis_non_pp_flag %in% c("TRUE", "FALSE")) ||
  any(!text_labels$analysis_zm_zr_zs_flag %in% c("TRUE", "FALSE"))
) {
  stop("Analysis-sample flags must be complete TRUE/FALSE values.")
}

sample_labels <- tribble(
  ~application_sample, ~application_sample_label,
  "all_reports", "All CPC narratives",
  "non_pp", "CPC narratives excluding PP actions",
  "zm_zr_zs", "ZM/ZR/ZS CPC narratives"
)

citywide_samples <- bind_rows(
  text_labels |>
    mutate(application_sample = "all_reports"),
  text_labels |>
    filter(analysis_non_pp_flag == "TRUE") |>
    mutate(application_sample = "non_pp"),
  text_labels |>
    filter(analysis_zm_zr_zs_flag == "TRUE") |>
    mutate(application_sample = "zm_zr_zs")
)

observed_citywide_year <- citywide_samples |>
  select(application_sample, year, all_of(signal_families)) |>
  pivot_longer(
    cols = all_of(signal_families),
    names_to = "signal_family",
    values_to = "signal_hit"
  ) |>
  group_by(application_sample, year, signal_family) |>
  summarize(
    document_count = n(),
    hit_document_count = sum(signal_hit),
    .groups = "drop"
  )

centered_sum <- function(year, value, window_years) {
  half_window <- (window_years - 1) / 2
  vapply(year, function(current_year) {
    in_window <- abs(year - current_year) <= half_window
    if (sum(in_window) != window_years) {
      return(NA_real_)
    }
    sum(value[in_window])
  }, numeric(1))
}

citywide_year <- expand_grid(
  application_sample = sample_labels$application_sample,
  year = start_year:end_year,
  signal_family = signal_families
) |>
  left_join(sample_labels, by = "application_sample", relationship = "many-to-one") |>
  left_join(
    observed_citywide_year,
    by = c("application_sample", "year", "signal_family"),
    relationship = "one-to-one"
  ) |>
  mutate(
    document_count = coalesce(document_count, 0L),
    hit_document_count = coalesce(hit_document_count, 0L),
    hit_document_share = if_else(
      document_count > 0,
      hit_document_count / document_count,
      NA_real_
    ),
    signal_label = unname(signal_labels[signal_family])
  ) |>
  group_by(application_sample, application_sample_label, signal_family) |>
  arrange(year, .by_group = TRUE) |>
  mutate(
    moving_window_document_count = centered_sum(
      year,
      document_count,
      moving_window_years
    ),
    moving_window_hit_document_count = centered_sum(
      year,
      hit_document_count,
      moving_window_years
    ),
    moving_window_observed_year_count = centered_sum(
      year,
      as.integer(document_count > 0),
      moving_window_years
    ),
    moving_window_eligible =
      moving_window_observed_year_count == moving_window_years &
      moving_window_document_count >= minimum_documents_per_moving_window,
    hit_document_share_moving_window = if_else(
      moving_window_eligible,
      moving_window_hit_document_count / moving_window_document_count,
      NA_real_
    )
  ) |>
  ungroup()

process_signals <- c(
  "substantial_local_opposition",
  "local_request_condition",
  "revision_or_concession",
  "procedural_response",
  "explicit_local_response",
  "approved_unresolved_objection"
)

actor_signals <- c(
  "cb_request_or_opposition",
  "bp_request_or_opposition",
  "councilmember_support_or_request",
  "councilmember_opposition",
  "civic_group_support_or_request",
  "civic_group_opposition"
)

issue_signals <- c(
  "affordability_displacement",
  "traffic_parking",
  "scale_character_preservation",
  "infrastructure_services",
  "environment_open_space"
)

agreement_signals <- c("restrictive_declaration", "points_of_agreement")

trend_colors <- c(
  "Substantial local opposition" = "#9e3d38",
  "Substantive local request" = "#c47b22",
  "Revision/concession" = "#2474a6",
  "Procedural response" = "#6c757d",
  "Explicit response to local actor" = "#3f7f4f",
  "Approved with objection unresolved" = "#6f3c8c",
  "CB request/opposition" = "#8c564b",
  "BP request/opposition" = "#d08729",
  "Councilmember support/request" = "#2a6fbb",
  "Councilmember opposition" = "#b03a2e",
  "Civic-group support/request" = "#4c956c",
  "Civic-group opposition" = "#7b2f66",
  "Affordability/displacement" = "#b2182b",
  "Traffic/parking" = "#2166ac",
  "Scale/character/preservation" = "#762a83",
  "Infrastructure/services" = "#1b7837",
  "Environment/open space" = "#5c7c2f",
  "Restrictive declaration" = "#7a5aa6",
  "Points of agreement" = "#008080"
)

observed_count_year <- citywide_samples |>
  select(application_sample, year, all_of(count_fields)) |>
  pivot_longer(
    cols = all_of(count_fields),
    names_to = "count_field",
    values_to = "count_value"
  ) |>
  group_by(application_sample, year, count_field) |>
  summarize(
    document_count = n(),
    reported_document_count = sum(!is.na(count_value)),
    reported_total = sum(count_value, na.rm = TRUE),
    .groups = "drop"
  )

count_year <- expand_grid(
  application_sample = sample_labels$application_sample,
  year = start_year:end_year,
  count_field = count_fields
) |>
  left_join(sample_labels, by = "application_sample", relationship = "many-to-one") |>
  left_join(
    observed_count_year,
    by = c("application_sample", "year", "count_field"),
    relationship = "one-to-one"
  ) |>
  mutate(
    document_count = coalesce(document_count, 0L),
    reported_document_count = coalesce(reported_document_count, 0L),
    reported_total = coalesce(reported_total, 0),
    count_label = unname(count_labels[count_field])
  ) |>
  group_by(application_sample, application_sample_label, count_field, count_label) |>
  arrange(year, .by_group = TRUE) |>
  mutate(
    moving_window_document_count = centered_sum(year, document_count, moving_window_years),
    moving_window_reported_document_count = centered_sum(
      year,
      reported_document_count,
      moving_window_years
    ),
    moving_window_reported_total = centered_sum(year, reported_total, moving_window_years),
    reported_document_share_moving_window = if_else(
      moving_window_document_count >= minimum_documents_per_moving_window,
      moving_window_reported_document_count / moving_window_document_count,
      NA_real_
    ),
    mean_count_when_reported_moving_window = if_else(
      moving_window_reported_document_count > 0,
      moving_window_reported_total / moving_window_reported_document_count,
      NA_real_
    )
  ) |>
  ungroup()

length_year <- citywide_samples |>
  group_by(application_sample, year) |>
  summarize(
    document_count = n(),
    total_words = sum(narrative_word_count),
    median_words = median(narrative_word_count),
    .groups = "drop"
  ) |>
  left_join(sample_labels, by = "application_sample", relationship = "many-to-one") |>
  group_by(application_sample, application_sample_label) |>
  arrange(year, .by_group = TRUE) |>
  mutate(
    moving_window_document_count = centered_sum(year, document_count, moving_window_years),
    moving_window_total_words = centered_sum(year, total_words, moving_window_years),
    mean_words_moving_window = if_else(
      moving_window_document_count >= minimum_documents_per_moving_window,
      moving_window_total_words / moving_window_document_count,
      NA_real_
    )
  ) |>
  ungroup()

pdf("../output/ulurp_cpc_text_signal_trends.pdf", width = 11, height = 8.5)
for (sample_id in sample_labels$application_sample) {
  sample_label <- sample_labels |>
    filter(application_sample == sample_id) |>
    pull(application_sample_label)

  print(
    citywide_year |>
      filter(application_sample == sample_id, signal_family == "substantial_local_opposition") |>
      ggplot(aes(x = year, y = document_count)) +
      geom_col(fill = "#5b6770", width = 0.85) +
      labs(
        title = paste0(sample_label, ": annual analysis narratives"),
        x = NULL,
        y = "Analysis narratives"
      ) +
      theme_minimal(base_size = 11) +
      theme(panel.grid.minor = element_blank())
  )

  for (signal_group in list(process_signals, actor_signals, issue_signals, agreement_signals)) {
    title_suffix <- case_when(
      identical(signal_group, process_signals) ~ "process signals",
      identical(signal_group, actor_signals) ~ "actor-position signals",
      identical(signal_group, issue_signals) ~ "issues discussed in review",
      TRUE ~ "formal commitments"
    )

    print(
      citywide_year |>
        filter(
          application_sample == sample_id,
          signal_family %in% signal_group
        ) |>
        ggplot(aes(x = year, y = hit_document_share, color = signal_label)) +
        geom_point(alpha = 0.25, size = 0.9, na.rm = TRUE) +
        geom_line(
          aes(y = hit_document_share_moving_window),
          linewidth = 0.8,
          na.rm = TRUE
        ) +
        scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
        scale_color_manual(values = trend_colors) +
        labs(
          title = paste0(sample_label, ": ", title_suffix),
          x = NULL,
          y = paste0(
            "Share of analysis narratives with signal (pooled ",
            moving_window_years,
            "-year window)"
          ),
          color = NULL
        ) +
        theme_minimal(base_size = 11) +
        theme(
          legend.position = "bottom",
          panel.grid.minor = element_blank()
        )
    )
  }

  print(
    count_year |>
      filter(application_sample == sample_id) |>
      ggplot(aes(x = year, y = reported_document_share_moving_window, color = count_label)) +
      geom_line(linewidth = 0.8, na.rm = TRUE) +
      scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
      labs(
        title = paste0(sample_label, ": exact-count reporting coverage"),
        subtitle = "Blank counts remain missing; zeros require an explicit zero",
        x = NULL,
        y = paste0("Share with exact count (pooled ", moving_window_years, "-year window)"),
        color = NULL
      ) +
      theme_minimal(base_size = 11) +
      theme(legend.position = "bottom", panel.grid.minor = element_blank())
  )

  print(
    count_year |>
      filter(application_sample == sample_id) |>
      ggplot(aes(x = year, y = mean_count_when_reported_moving_window, color = count_label)) +
      geom_line(linewidth = 0.8, na.rm = TRUE) +
      labs(
        title = paste0(sample_label, ": reported participation counts"),
        subtitle = "Means are conditional on an exact count being reported",
        x = NULL,
        y = paste0("Mean exact count (pooled ", moving_window_years, "-year window)"),
        color = NULL
      ) +
      theme_minimal(base_size = 11) +
      theme(legend.position = "bottom", panel.grid.minor = element_blank())
  )

  print(
    length_year |>
      filter(application_sample == sample_id) |>
      ggplot(aes(x = year)) +
      geom_point(aes(y = median_words), color = "#8b8b8b", alpha = 0.35, size = 0.9) +
      geom_line(aes(y = mean_words_moving_window), color = "#2b6f8a", linewidth = 0.9, na.rm = TRUE) +
      labs(
        title = paste0(sample_label, ": CPC narrative length"),
        subtitle = "Points are annual medians; line is the pooled moving-window mean",
        x = NULL,
        y = "Narrative words"
      ) +
      theme_minimal(base_size = 11) +
      theme(panel.grid.minor = element_blank())
  )
}
dev.off()
