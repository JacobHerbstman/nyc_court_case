# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_zap_housing_pipeline_tercile_trends/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

assert_unique_keys <- function(df, keys, label) {
  duplicate_keys <- df |>
    count(across(all_of(keys)), name = "n") |>
    filter(n > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(label, " is not unique by ", paste(keys, collapse = ", "), ".")
  }
}

plot_outcomes <- tribble(
  ~outcome_name, ~outcome_plot_label, ~plot_order,
  "all_ulurp_apps", "All ULURP", 1,
  "housing_any_candidate_apps", "Housing-oriented", 2,
  "housing_any_rezoning_special_apps", "Housing rezoning/special permit", 3,
  "housing_any_public_land_disposition_apps", "Housing public land/disposition", 4
)

primary_df <- read_csv("../input/zap_housing_cd_year_panel_primary.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(assignment_type = "primary_zap_cd")

bbl_df <- read_csv("../input/zap_housing_cd_year_panel_bbl_fractional.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(assignment_type = "bbl_fractional_current_mappluto")

panel_df <- bind_rows(primary_df, bbl_df) |>
  filter(outcome_name %in% plot_outcomes$outcome_name) |>
  mutate(
    borocd = suppressWarnings(as.integer(borocd)),
    borough_code = suppressWarnings(as.integer(borough_code)),
    year = suppressWarnings(as.integer(year)),
    project_count_observed = suppressWarnings(as.numeric(project_count_observed)),
    project_count = suppressWarnings(as.numeric(project_count)),
    rate_per_10000_occupied_units_1990 = suppressWarnings(as.numeric(rate_per_10000_occupied_units_1990)),
    rate_per_residential_acre = suppressWarnings(as.numeric(rate_per_residential_acre)),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990)),
    residential_acres = suppressWarnings(as.numeric(residential_acres)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro))
  ) |>
  left_join(plot_outcomes, by = "outcome_name", relationship = "many-to-one")

required_cols <- c(
  "assignment_type",
  "borocd",
  "borough_code",
  "borough_name",
  "year",
  "outcome_name",
  "analysis_usability",
  "project_count",
  "rate_per_10000_occupied_units_1990",
  "rate_per_residential_acre",
  "treat_z_boro"
)

missing_cols <- setdiff(required_cols, names(panel_df))
if (length(missing_cols) > 0) {
  stop("Missing required panel columns: ", paste(missing_cols, collapse = ", "))
}

assert_unique_keys(
  panel_df,
  c("assignment_type", "borocd", "year", "outcome_name"),
  "ZAP housing CD-year panel"
)

district_lookup <- panel_df |>
  filter(assignment_type == "primary_zap_cd") |>
  distinct(borocd, borough_code, borough_name, treat_z_boro) |>
  group_by(borough_code, borough_name) |>
  mutate(
    treat_tercile = ntile(treat_z_boro, 3),
    treat_tercile_label = case_when(
      treat_tercile == 1 ~ "Low",
      treat_tercile == 2 ~ "Middle",
      TRUE ~ "High"
    )
  ) |>
  ungroup()

assert_unique_keys(district_lookup, "borocd", "ZAP tercile district lookup")

if (n_distinct(district_lookup$borocd) != 59) {
  stop("Expected 59 CDs in the ZAP tercile district lookup.")
}

tercile_year_df <- panel_df |>
  select(-treat_z_boro) |>
  left_join(
    district_lookup |>
      select(borocd, treat_tercile, treat_tercile_label),
    by = "borocd",
    relationship = "many-to-one"
  ) |>
  mutate(
    plotted_count = if_else(analysis_usability == "not_recommended", NA_real_, project_count),
    plotted_rate_per_10000 = if_else(analysis_usability == "not_recommended", NA_real_, rate_per_10000_occupied_units_1990),
    plotted_rate_per_residential_acre = if_else(analysis_usability == "not_recommended", NA_real_, rate_per_residential_acre),
    support_label = case_when(
      analysis_usability == "usable" ~ "Usable",
      analysis_usability == "limited" ~ "Limited support",
      TRUE ~ "Not recommended"
    ),
    assignment_label = case_when(
      assignment_type == "primary_zap_cd" ~ "Primary ZAP CD",
      assignment_type == "bbl_fractional_current_mappluto" ~ "BBL fractional",
      TRUE ~ assignment_type
    )
  ) |>
  group_by(
    assignment_type,
    assignment_label,
    outcome_name,
    outcome_plot_label,
    plot_order,
    year,
    period,
    treat_tercile,
    treat_tercile_label,
    analysis_usability,
    support_label
  ) |>
  summarise(
    project_count_observed = sum(project_count_observed, na.rm = TRUE),
    project_count = if_else(all(is.na(plotted_count)), NA_real_, sum(plotted_count, na.rm = TRUE)),
    rate_per_10000_occupied_units_1990 = if_else(all(is.na(plotted_rate_per_10000)), NA_real_, sum(plotted_rate_per_10000, na.rm = TRUE)),
    rate_per_residential_acre = if_else(all(is.na(plotted_rate_per_residential_acre)), NA_real_, sum(plotted_rate_per_residential_acre, na.rm = TRUE)),
    cd_count = n_distinct(borocd),
    .groups = "drop"
  ) |>
  arrange(assignment_type, outcome_name, year, treat_tercile)

expected_key_count <- 2 * nrow(plot_outcomes) * 50 * 3
if (nrow(tercile_year_df) != expected_key_count) {
  stop("Unexpected tercile-year row count.")
}

make_plot <- function(df, y_var, y_label, out_pdf) {
  plot_df <- df |>
    mutate(
      outcome_plot_label = factor(outcome_plot_label, levels = plot_outcomes$outcome_plot_label[order(plot_outcomes$plot_order)]),
      treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
      assignment_label = factor(assignment_label, levels = c("Primary ZAP CD", "BBL fractional"))
    )

  p <- ggplot(
    plot_df,
    aes(x = year, y = .data[[y_var]], color = treat_tercile_label, group = treat_tercile_label)
  ) +
    geom_hline(yintercept = 0, linewidth = 0.25, color = "grey75") +
    geom_line(linewidth = 0.45, na.rm = TRUE) +
    geom_point(size = 0.75, na.rm = TRUE) +
    geom_vline(xintercept = 1989, linetype = "dashed", linewidth = 0.25, color = "grey50") +
    facet_grid(outcome_plot_label ~ assignment_label, scales = "free_y") +
    scale_color_manual(values = c("Low" = "#2f6fdd", "Middle" = "#8f8f8f", "High" = "#d84a2b")) +
    scale_x_continuous(breaks = c(1976, 1985, 1990, 2000, 2010, 2020, 2025)) +
    labs(
      x = NULL,
      y = y_label,
      color = "1990 homeownership tercile",
      title = "Raw ZAP/ULURP pipeline trends by homeownership tercile",
      subtitle = "Unsupported BBL-fractional periods are omitted rather than plotted as zeros"
    ) +
    theme_minimal(base_size = 10) +
    theme(
      legend.position = "bottom",
      panel.grid.minor = element_blank(),
      strip.text = element_text(face = "bold", size = 8),
      plot.title = element_text(face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )

  ggsave(out_pdf, p, width = 11, height = 8.5)
}

make_plot(
  tercile_year_df,
  "rate_per_10000_occupied_units_1990",
  "Applications per 10,000 occupied 1990 units",
  "../output/zap_housing_pipeline_tercile_trends_per_10000.pdf"
)

make_plot(
  tercile_year_df,
  "rate_per_residential_acre",
  "Applications per residential acre",
  "../output/zap_housing_pipeline_tercile_trends_per_residential_acre.pdf"
)

qc_df <- bind_rows(
  tibble(
    metric = "district_count",
    value = as.character(n_distinct(district_lookup$borocd)),
    status = if_else(n_distinct(district_lookup$borocd) == 59, "pass", "fail"),
    note = "Standard CDs used to form within-borough treatment terciles."
  ),
  tibble(
    metric = "tercile_year_row_count",
    value = as.character(nrow(tercile_year_df)),
    status = if_else(nrow(tercile_year_df) == expected_key_count, "pass", "fail"),
    note = "Two assignments x four outcomes x 50 years x three terciles."
  ),
  tibble(
    metric = "not_recommended_nonmissing_project_count_cells",
    value = as.character(sum(tercile_year_df$analysis_usability == "not_recommended" & !is.na(tercile_year_df$project_count))),
    status = if_else(sum(tercile_year_df$analysis_usability == "not_recommended" & !is.na(tercile_year_df$project_count)) == 0, "pass", "fail"),
    note = "Unsupported cells should be masked, not plotted as zeros."
  ),
  tibble(
    metric = "bbl_fractional_1976_1984_nonmissing_project_count_cells",
    value = as.character(sum(tercile_year_df$assignment_type == "bbl_fractional_current_mappluto" & tercile_year_df$year <= 1984 & !is.na(tercile_year_df$project_count))),
    status = if_else(sum(tercile_year_df$assignment_type == "bbl_fractional_current_mappluto" & tercile_year_df$year <= 1984 & !is.na(tercile_year_df$project_count)) == 0, "pass", "fail"),
    note = "BBL-fractional geography is not recommended before 1985 and should be blank."
  ),
  tibble(
    metric = "negative_rate_cell_count",
    value = as.character(sum(tercile_year_df$rate_per_10000_occupied_units_1990 < 0, na.rm = TRUE) + sum(tercile_year_df$rate_per_residential_acre < 0, na.rm = TRUE)),
    status = if_else(sum(tercile_year_df$rate_per_10000_occupied_units_1990 < 0, na.rm = TRUE) + sum(tercile_year_df$rate_per_residential_acre < 0, na.rm = TRUE) == 0, "pass", "fail"),
    note = "Rates should be nonnegative."
  ),
  tibble(
    metric = "per_10000_pdf_nonempty",
    value = as.character(file.info("../output/zap_housing_pipeline_tercile_trends_per_10000.pdf")$size),
    status = if_else(file.info("../output/zap_housing_pipeline_tercile_trends_per_10000.pdf")$size > 0, "pass", "fail"),
    note = "Raw trend plot scaled by 1990 occupied units."
  ),
  tibble(
    metric = "per_residential_acre_pdf_nonempty",
    value = as.character(file.info("../output/zap_housing_pipeline_tercile_trends_per_residential_acre.pdf")$size),
    status = if_else(file.info("../output/zap_housing_pipeline_tercile_trends_per_residential_acre.pdf")$size > 0, "pass", "fail"),
    note = "Raw trend plot scaled by residential acres."
  )
)

write_csv(tercile_year_df, "../output/zap_housing_pipeline_tercile_year.csv", na = "")
write_csv(qc_df, "../output/zap_housing_pipeline_tercile_trends_qc.csv", na = "")

if (any(qc_df$status == "fail")) {
  stop("ZAP housing pipeline tercile trend QC failed; inspect ../output/zap_housing_pipeline_tercile_trends_qc.csv")
}
