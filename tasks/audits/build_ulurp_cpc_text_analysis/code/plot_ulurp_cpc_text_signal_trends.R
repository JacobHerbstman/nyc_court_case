# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/build_ulurp_cpc_text_analysis/code")
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
  "revision_concession",
  "opposition_any",
  "conditions_commitments",
  "restrictive_declaration",
  "substantive_council_member",
  "attribution_council_member",
  "community_board_disapproval",
  "community_board_conditioned_approval",
  "opposition_traffic_parking",
  "opposition_scale_character",
  "opposition_displacement_affordability",
  "opposition_infrastructure"
)

signal_labels <- c(
  revision_concession = "Revision/concession",
  opposition_any = "Any opposition",
  conditions_commitments = "Conditions/commitments",
  restrictive_declaration = "Restrictive declaration",
  substantive_council_member = "Substantive Council mention",
  attribution_council_member = "Council attribution",
  community_board_disapproval = "Community Board disapproval",
  community_board_conditioned_approval = "Community Board conditioned approval",
  opposition_traffic_parking = "Traffic/parking opposition",
  opposition_scale_character = "Scale/character opposition",
  opposition_displacement_affordability = "Displacement/affordability opposition",
  opposition_infrastructure = "Infrastructure opposition"
)

text_labels <- read_csv(
  "../output/ulurp_cpc_text_labels.csv",
  col_types = cols(.default = col_character()),
  show_col_types = FALSE,
  na = c("", "NA")
) |>
  mutate(
    year = suppressWarnings(as.integer(year)),
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

core_signals <- c(
  "revision_concession",
  "opposition_any",
  "conditions_commitments",
  "restrictive_declaration",
  "substantive_council_member",
  "attribution_council_member"
)

opposition_signals <- c(
  "community_board_disapproval",
  "community_board_conditioned_approval",
  "opposition_traffic_parking",
  "opposition_scale_character",
  "opposition_displacement_affordability",
  "opposition_infrastructure"
)

trend_colors <- c(
  "Revision/concession" = "#1b6ca8",
  "Any opposition" = "#aa4a44",
  "Conditions/commitments" = "#4f8f4f",
  "Restrictive declaration" = "#7a5aa6",
  "Substantive Council mention" = "#c27922",
  "Council attribution" = "#607d8b",
  "Community Board disapproval" = "#7a2f2f",
  "Community Board conditioned approval" = "#9a6a18",
  "Traffic/parking opposition" = "#2166ac",
  "Scale/character opposition" = "#762a83",
  "Displacement/affordability opposition" = "#b2182b",
  "Infrastructure opposition" = "#1b7837"
)

pdf("../output/ulurp_cpc_text_signal_trends.pdf", width = 11, height = 8.5)
for (sample_id in sample_labels$application_sample) {
  sample_label <- sample_labels |>
    filter(application_sample == sample_id) |>
    pull(application_sample_label)

  print(
    citywide_year |>
      filter(application_sample == sample_id, signal_family == "opposition_any") |>
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

  for (signal_group in list(core_signals, opposition_signals)) {
    title_suffix <- if (identical(signal_group, core_signals)) {
      "core text signals"
    } else {
      "opposition and review-body signals"
    }

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
}
dev.off()
