# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/reconcile_zap_zc_ts/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

nonempty_text <- function(x) {
  !is.na(x) & str_squish(as.character(x)) != ""
}

extract_ulurp_numbers <- function(x) {
  raw_value <- str_replace_all(str_to_upper(coalesce(as.character(x), "")), "[[:space:]]+", "")
  str_extract_all(raw_value, "[A-Z]?[0-9]{6,7}A?[A-Z]{3}")
}

normalize_application_key <- function(x) {
  out <- str_to_upper(coalesce(as.character(x), ""))
  out <- str_replace_all(out, "[^A-Z0-9]", "")
  out <- str_replace(out, "^[A-Z](?=[0-9])", "")
  out <- if_else(out == "", NA_character_, out)
  out
}

trailing_mean <- function(x, window_size) {
  out <- rep(NA_real_, length(x))
  for (i in seq_along(x)) {
    if (i >= window_size && all(!is.na(x[(i - window_size + 1):i]))) {
      out[[i]] <- mean(x[(i - window_size + 1):i])
    }
  }
  out
}

project_df <- read_parquet("../input/zap_project_data.parquet") |>
  mutate(
    project_id = as.character(project_id),
    completed_year = suppressWarnings(as.integer(format(completed_date_parsed, "%Y"))),
    cert_year = suppressWarnings(as.integer(format(certified_referred_date_parsed, "%Y"))),
    approval_year = suppressWarnings(as.integer(format(approval_date_parsed, "%Y"))),
    ulurp_flag = str_to_upper(str_squish(coalesce(as.character(ulurp_non), ""))) == "ULURP"
  )

if (nrow(project_df) != n_distinct(project_df$project_id)) {
  stop("Staged ZAP project data are not unique by project_id.")
}

zap_application_rows <- project_df |>
  filter(public_status == "Completed", nonempty_text(ulurp_numbers)) |>
  transmute(
    project_id,
    public_status,
    ulurp_flag,
    completed_year,
    cert_year,
    approval_year,
    ulurp_application_number = extract_ulurp_numbers(ulurp_numbers)
  ) |>
  unnest_longer(ulurp_application_number) |>
  filter(nonempty_text(ulurp_application_number)) |>
  mutate(
    ulurp_application_number = normalize_application_key(ulurp_application_number),
    ulurp_action_code = str_sub(ulurp_application_number, -3, -2)
  ) |>
  arrange(ulurp_application_number, completed_year, project_id) |>
  distinct(ulurp_application_number, .keep_all = TRUE)

zc_application_rows <- zap_application_rows |>
  filter(ulurp_action_code == "ZC", completed_year >= 1970, completed_year <= 2026)

zc_year <- expand_grid(year = 1970:2026) |>
  left_join(
    zc_application_rows |>
      count(completed_year, name = "zoning_change_count"),
    by = c("year" = "completed_year"),
    relationship = "one-to-one"
  ) |>
  mutate(
    zoning_change_count = coalesce(zoning_change_count, 0L),
    zoning_change_count_ma5 = trailing_mean(zoning_change_count, 5),
    source_dataset = "NYC Open Data ZAP Project Data via staged project snapshot",
    series_definition = "unique parsed ULURP application numbers with action code ZC, counted by project completed year"
  )

write_csv_if_changed(zc_year, "../output/zap_zc_ts_recreated_year.csv")

broad_zap_project_year <- expand_grid(year = 1970:2026) |>
  left_join(
    project_df |>
      filter(ulurp_flag, cert_year >= 1970, cert_year <= 2026) |>
      count(cert_year, name = "broad_certified_ulurp_project_count"),
    by = c("year" = "cert_year"),
    relationship = "one-to-one"
  ) |>
  mutate(
    broad_certified_ulurp_project_count = coalesce(broad_certified_ulurp_project_count, 0L),
    broad_certified_ulurp_project_count_ma5 = trailing_mean(broad_certified_ulurp_project_count, 5)
  )

comparison_year <- zc_year |>
  select(year, zoning_change_count, zoning_change_count_ma5) |>
  left_join(broad_zap_project_year, by = "year", relationship = "one-to-one") |>
  mutate(
    zc_to_broad_certified_ulurp_ratio = if_else(
      broad_certified_ulurp_project_count > 0,
      zoning_change_count / broad_certified_ulurp_project_count,
      NA_real_
    )
  )

write_csv_if_changed(comparison_year, "../output/zap_zc_ts_reconciliation_year.csv")

comparison_summary <- comparison_year |>
  mutate(
    period = case_when(
      year >= 1976 & year <= 1989 ~ "1976-1989",
      year >= 1990 & year <= 1997 ~ "1990-1997",
      year >= 1998 & year <= 2002 ~ "1998-2002",
      year >= 2003 & year <= 2009 ~ "2003-2009",
      year >= 2010 & year <= 2017 ~ "2010-2017",
      year >= 2018 & year <= 2025 ~ "2018-2025",
      TRUE ~ NA_character_
    )
  ) |>
  filter(!is.na(period)) |>
  group_by(period) |>
  summarize(
    years_observed = n(),
    zoning_change_count_sum = sum(zoning_change_count),
    zoning_change_count_mean = mean(zoning_change_count),
    broad_certified_ulurp_project_count_sum = sum(broad_certified_ulurp_project_count),
    broad_certified_ulurp_project_count_mean = mean(broad_certified_ulurp_project_count),
    mean_zc_to_broad_certified_ulurp_ratio = mean(zc_to_broad_certified_ulurp_ratio, na.rm = TRUE),
    .groups = "drop"
  )

write_csv_if_changed(comparison_summary, "../output/zap_zc_ts_reconciliation_summary.csv")

council_application_rows <- read_csv("../input/council_land_use_decision_panel.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    decision_year = suppressWarnings(as.integer(format(as.Date(decision_date, tryFormats = c("%Y-%m-%d", "%m/%d/%Y")), "%Y"))),
    application_key = str_split(coalesce(as.character(application_keys), ""), ";")
  ) |>
  select(matter_id, disposition_group, decision_year, application_key) |>
  unnest_longer(application_key) |>
  mutate(application_key = normalize_application_key(application_key)) |>
  filter(!is.na(application_key), str_detect(application_key, "^[0-9]{6,7}A?[A-Z]{3}$")) |>
  group_by(application_key) |>
  summarize(
    council_matter_count = n_distinct(matter_id),
    council_adopted_matter_count = n_distinct(matter_id[disposition_group == "adopted"]),
    first_council_year = suppressWarnings(min(decision_year, na.rm = TRUE)),
    .groups = "drop"
  ) |>
  mutate(
    first_council_year = if_else(is.infinite(first_council_year), NA_integer_, as.integer(first_council_year))
  )

if (nrow(council_application_rows) != n_distinct(council_application_rows$application_key)) {
  stop("Council application-key rows are not unique by normalized application key.")
}

council_match_summary <- zap_application_rows |>
  filter(completed_year >= 1990, completed_year <= 2025) |>
  left_join(
    council_application_rows,
    by = c("ulurp_application_number" = "application_key"),
    relationship = "one-to-one"
  ) |>
  mutate(
    action_scope = case_when(
      ulurp_action_code == "ZC" ~ "ZC_zoning_certifications",
      ulurp_action_code %in% c("ZM", "ZR", "ZS") ~ "ZM_ZR_ZS_zoning_amendments_special_permits",
      ulurp_action_code %in% c("ZA", "ZC", "ZM", "ZR", "ZS") ~ "ZA_other_zoning_authorizations",
      TRUE ~ "other_action_codes"
    ),
    completed_period = case_when(
      completed_year >= 1990 & completed_year <= 1997 ~ "1990-1997",
      completed_year >= 1998 & completed_year <= 2002 ~ "1998-2002",
      completed_year >= 2003 & completed_year <= 2009 ~ "2003-2009",
      completed_year >= 2010 & completed_year <= 2017 ~ "2010-2017",
      completed_year >= 2018 & completed_year <= 2025 ~ "2018-2025",
      TRUE ~ NA_character_
    ),
    matched_to_council = !is.na(council_matter_count),
    matched_to_adopted_council = coalesce(council_adopted_matter_count, 0L) > 0
  ) |>
  filter(action_scope != "other_action_codes", !is.na(completed_period)) |>
  group_by(action_scope, completed_period) |>
  summarize(
    zap_application_keys = n(),
    matched_to_council = sum(matched_to_council),
    matched_to_adopted_council = sum(matched_to_adopted_council),
    council_match_share = matched_to_council / zap_application_keys,
    adopted_council_match_share = matched_to_adopted_council / zap_application_keys,
    .groups = "drop"
  ) |>
  arrange(action_scope, completed_period)

write_csv_if_changed(council_match_summary, "../output/zap_zc_ts_council_match_summary.csv")

zc_plot <- ggplot(zc_year, aes(x = year, y = zoning_change_count)) +
  geom_vline(xintercept = 1990, color = "grey60", linewidth = 0.4, linetype = "dashed") +
  geom_line(color = "#1f78b4", linewidth = 0.45) +
  geom_point(color = "#1f78b4", fill = "#1f78b4", size = 1.8, shape = 21, stroke = 0.25) +
  scale_x_continuous(breaks = seq(1975, 2025, by = 10), limits = c(1970, 2026)) +
  labs(x = "Year", y = "Zoning Changes") +
  theme_classic(base_size = 13) +
  theme(
    axis.title = element_text(size = 14),
    axis.text = element_text(size = 11)
  )

ggsave("../output/zap_zc_ts_recreated.pdf", zc_plot, width = 7.2, height = 4.8)

comparison_plot_df <- comparison_year |>
  select(
    year,
    zoning_change_count,
    zoning_change_count_ma5,
    broad_certified_ulurp_project_count,
    broad_certified_ulurp_project_count_ma5
  ) |>
  pivot_longer(
    cols = c(
      zoning_change_count,
      zoning_change_count_ma5,
      broad_certified_ulurp_project_count,
      broad_certified_ulurp_project_count_ma5
    ),
    names_to = "series_id",
    values_to = "count"
  ) |>
  mutate(
    panel = case_when(
      str_detect(series_id, "zoning_change") ~ "Parsed ZC application numbers",
      TRUE ~ "Broad certified/referred ULURP project records"
    ),
    line_type = case_when(
      str_detect(series_id, "ma5") ~ "Trailing 5-year average",
      TRUE ~ "Annual value"
    )
  )

comparison_plot <- ggplot(comparison_plot_df, aes(x = year, y = count)) +
  geom_line(
    data = comparison_plot_df |> filter(line_type == "Annual value"),
    color = "grey70",
    linewidth = 0.35
  ) +
  geom_point(
    data = comparison_plot_df |> filter(line_type == "Annual value"),
    color = "grey70",
    fill = "grey70",
    shape = 21,
    size = 1.1,
    stroke = 0.2
  ) +
  geom_line(
    data = comparison_plot_df |> filter(line_type == "Trailing 5-year average"),
    color = "#1f78b4",
    linewidth = 0.75
  ) +
  facet_wrap(~ panel, ncol = 1, scales = "free_y") +
  scale_x_continuous(breaks = seq(1975, 2025, by = 10), limits = c(1970, 2026)) +
  labs(
    x = "Year",
    y = "Annual count",
    caption = "Grey series is annual. Blue series is a trailing 5-year average. Series are different count units."
  ) +
  theme_minimal(base_size = 11) +
  theme(
    panel.grid.minor = element_blank(),
    strip.text = element_text(size = 11),
    plot.caption = element_text(hjust = 0)
  )

ggsave("../output/zap_zc_ts_reconciliation.pdf", comparison_plot, width = 7.2, height = 6.4)

cat("Wrote ZAP ZC reconciliation outputs to ../output\n")
