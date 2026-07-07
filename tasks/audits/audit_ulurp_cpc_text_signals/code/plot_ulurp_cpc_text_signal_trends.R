# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_ulurp_cpc_text_signals/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
})

centered_ma3 <- function(x) {
  out <- rep(NA_real_, length(x))
  if (length(x) < 3) {
    return(out)
  }

  for (i in 2:(length(x) - 1)) {
    window <- x[(i - 1):(i + 1)]
    if (all(!is.na(window))) {
      out[[i]] <- mean(window)
    }
  }

  out
}

pretty_signal_label <- function(x) {
  case_when(
    x == "revision_concession" ~ "Revision/concession",
    x == "opposition_any" ~ "Any opposition",
    x == "conditions_commitments" ~ "Conditions/commitments",
    x == "restrictive_declaration" ~ "Restrictive declaration",
    x == "substantive_council_member" ~ "Substantive Council mention",
    x == "attribution_council_member" ~ "Council attribution",
    x == "community_board_disapproval" ~ "CB disapproval",
    x == "community_board_conditioned_approval" ~ "CB conditioned approval",
    x == "opposition_traffic_parking" ~ "Traffic/parking opposition",
    x == "opposition_scale_character" ~ "Scale/character opposition",
    x == "opposition_displacement_affordability" ~ "Displacement/affordability opposition",
    x == "opposition_infrastructure" ~ "Infrastructure opposition",
    TRUE ~ str_to_sentence(str_replace_all(x, "_", " "))
  )
}

signal_year <- read_csv(
  "../output/ulurp_cpc_text_signal_year_by_application_sample.csv",
  show_col_types = FALSE,
  na = c("", "NA")
) |>
  filter(section == "all_sections") |>
  mutate(
    year = as.integer(year),
    readable_documents = as.integer(readable_documents),
    hit_document_share = as.numeric(hit_document_share),
    hit_sentences_per_1000_words = as.numeric(hit_sentences_per_1000_words),
    signal_label = pretty_signal_label(signal_family)
  )

if (nrow(signal_year) == 0) {
  stop("No all-section text-signal rows are available for plotting.")
}

signal_year <- signal_year |>
  group_by(application_sample, signal_family) |>
  arrange(year, .by_group = TRUE) |>
  mutate(
    hit_document_share_ma3 = centered_ma3(hit_document_share),
    hit_sentences_per_1000_words_ma3 = centered_ma3(hit_sentences_per_1000_words)
  ) |>
  ungroup()

sample_labels <- signal_year |>
  distinct(application_sample, application_sample_label)

readable_cpc_report_counts <- signal_year |>
  filter(application_sample == "all_reports", signal_family == "opposition_any") |>
  distinct(year, readable_documents) |>
  transmute(
    year,
    count_series = "readable_cpc_reports",
    count_series_label = "Readable CPC reports",
    application_count = readable_documents
  )

earlier_ulurp_counts <- read_csv(
  "../input/citywide_ulurp_application_year.csv",
  show_col_types = FALSE,
  na = c("", "NA")
) |>
  filter(count_unit %in% c("parsed_ulurp_numbers", "zap_project_records")) |>
  transmute(
    year = as.integer(cert_year),
    count_series = count_unit,
    count_series_label = case_when(
      count_unit == "parsed_ulurp_numbers" ~ "Parsed ULURP numbers",
      count_unit == "zap_project_records" ~ "ZAP project records",
      TRUE ~ count_unit
    ),
    application_count = as.numeric(application_count)
  )

count_comparison <- bind_rows(readable_cpc_report_counts, earlier_ulurp_counts) |>
  filter(year >= min(signal_year$year), year <= max(signal_year$year)) |>
  group_by(count_series) |>
  arrange(year, .by_group = TRUE) |>
  mutate(application_count_ma3 = centered_ma3(application_count)) |>
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
  "CB disapproval" = "#7a2f2f",
  "CB conditioned approval" = "#9a6a18",
  "Traffic/parking opposition" = "#2166ac",
  "Scale/character opposition" = "#762a83",
  "Displacement/affordability opposition" = "#b2182b",
  "Infrastructure opposition" = "#1b7837"
)

make_count_plot <- function(sample_id) {
  sample_label <- sample_labels |>
    filter(application_sample == sample_id) |>
    pull(application_sample_label) |>
    first()

  signal_year |>
    filter(application_sample == sample_id, signal_family == "opposition_any") |>
    distinct(year, readable_documents) |>
    ggplot(aes(x = year, y = readable_documents)) +
    geom_col(fill = "#5b6770", width = 0.85) +
    labs(
      title = paste0(sample_label, ": readable CPC reports"),
      x = NULL,
      y = "Readable reports"
    ) +
    theme_minimal(base_size = 11) +
    theme(panel.grid.minor = element_blank())
}

make_signal_plot <- function(sample_id, signal_ids, title_suffix) {
  sample_label <- sample_labels |>
    filter(application_sample == sample_id) |>
    pull(application_sample_label) |>
    first()

  plot_df <- signal_year |>
    filter(application_sample == sample_id, signal_family %in% signal_ids)

  ggplot(plot_df, aes(x = year, y = hit_document_share, color = signal_label)) +
    geom_point(alpha = 0.25, size = 0.9) +
    geom_line(aes(y = hit_document_share_ma3), linewidth = 0.8, na.rm = TRUE) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
    scale_color_manual(values = trend_colors, breaks = pretty_signal_label(signal_ids)) +
    labs(
      title = paste0(sample_label, ": ", title_suffix),
      x = NULL,
      y = "Share of readable reports with signal",
      color = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(
      legend.position = "bottom",
      panel.grid.minor = element_blank()
    )
}

write_sample_pdf <- function(sample_id, output_path) {
  pdf(output_path, width = 11, height = 8.5)
  print(make_count_plot(sample_id))
  print(make_signal_plot(sample_id, core_signals, "core text signals"))
  print(make_signal_plot(sample_id, opposition_signals, "opposition and review-body signals"))
  dev.off()
}

write_sample_pdf("all_reports", "../output/ulurp_cpc_text_signal_trends_all_reports.pdf")
write_sample_pdf("non_pp", "../output/ulurp_cpc_text_signal_trends_non_pp.pdf")
write_sample_pdf("zm_zr_zs", "../output/ulurp_cpc_text_signal_trends_zm_zr_zs.pdf")

pdf("../output/ulurp_cpc_report_count_comparison.pdf", width = 11, height = 8.5)
print(
  ggplot(count_comparison, aes(x = year, y = application_count, color = count_series_label)) +
    geom_point(alpha = 0.28, size = 1) +
    geom_line(aes(y = application_count_ma3), linewidth = 0.9, na.rm = TRUE) +
    scale_color_manual(values = c(
      "Parsed ULURP numbers" = "#1b6ca8",
      "Readable CPC reports" = "#aa4a44",
      "ZAP project records" = "#5b6770"
    )) +
    labs(
      title = "Earlier ULURP count series versus readable CPC report corpus",
      x = NULL,
      y = "Annual count",
      color = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(
      legend.position = "bottom",
      panel.grid.minor = element_blank()
    )
)
dev.off()
