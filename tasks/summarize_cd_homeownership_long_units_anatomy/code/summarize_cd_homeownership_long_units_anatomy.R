# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_cd_homeownership_long_units_anatomy/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

assert_unique_keys <- function(df, keys, label) {
  duplicate_keys <- df |>
    count(across(all_of(keys)), name = "n") |>
    filter(n > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(label, " is not unique by ", paste(keys, collapse = ", "), ".")
  }
}

district_lookup <- read_csv("../input/cd_homeownership_long_units_series.csv", show_col_types = FALSE, na = c("", "NA")) |>
  distinct(borocd, borough_code, borough_name, treat_pp) |>
  mutate(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    treat_pp = suppressWarnings(as.numeric(treat_pp))
  ) |>
  group_by(borough_code, borough_name) |>
  mutate(
    treat_tercile = ntile(treat_pp, 3),
    treat_tercile_label = case_when(
      treat_tercile == 1 ~ "Low",
      treat_tercile == 2 ~ "Middle",
      TRUE ~ "High"
    )
  ) |>
  ungroup()

assert_unique_keys(district_lookup, "borocd", "long-units anatomy district lookup")

if (n_distinct(district_lookup$borocd) != 59) {
  stop("Expected the long-units anatomy district lookup to cover 59 community districts.")
}

series_df <- read_csv("../input/cd_homeownership_long_units_series.csv", show_col_types = FALSE, na = c("", "NA")) |>
  filter(
    series_kind == "preferred_long_series",
    series_family %in% c("units_built_total", "units_built_50_plus")
  ) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    year = suppressWarnings(as.integer(year)),
    series_family = series_family,
    series_label = series_label,
    outcome_value = suppressWarnings(as.numeric(outcome_value))
  ) |>
  mutate(
    era = case_when(
      year >= 1985 & year <= 1989 ~ "1985-1989",
      year >= 1990 & year <= 1999 ~ "1990-1999",
      year >= 2000 & year <= 2009 ~ "2000-2009",
      year >= 2010 & year <= 2019 ~ "2010-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    )
  ) |>
  filter(!is.na(era)) |>
  left_join(
    district_lookup |>
      select(borocd, borough_code, borough_name, treat_tercile, treat_tercile_label),
    by = c("borocd", "borough_code", "borough_name"),
    relationship = "many-to-one"
  ) |>
  filter(treat_tercile_label == "High")

top_cd_df <- series_df |>
  group_by(series_family, series_label, era, borocd, borough_name, treat_tercile_label) |>
  summarize(units = sum(outcome_value, na.rm = TRUE), .groups = "drop") |>
  group_by(series_family, era) |>
  mutate(
    high_tercile_total = sum(units, na.rm = TRUE),
    share_of_high_tercile_total = if_else(high_tercile_total > 0, units / high_tercile_total, NA_real_),
    rank_within_era = min_rank(desc(units))
  ) |>
  ungroup() |>
  arrange(series_family, era, rank_within_era, desc(units), borocd) |>
  group_by(series_family, era) |>
  slice_head(n = 10) |>
  ungroup()

top_cd_year_df <- series_df |>
  transmute(
    series_family,
    series_label,
    era,
    year,
    borocd,
    borough_name,
    treat_tercile_label,
    units = outcome_value
  ) |>
  group_by(series_family, era) |>
  mutate(
    high_tercile_total = sum(units, na.rm = TRUE),
    share_of_high_tercile_total = if_else(high_tercile_total > 0, units / high_tercile_total, NA_real_),
    rank_within_era = min_rank(desc(units))
  ) |>
  ungroup() |>
  arrange(series_family, era, rank_within_era, desc(units), borocd, year) |>
  group_by(series_family, era) |>
  slice_head(n = 10) |>
  ungroup()

write_csv(top_cd_df, "../output/cd_homeownership_long_units_anatomy_top_cd.csv", na = "")
write_csv(top_cd_year_df, "../output/cd_homeownership_long_units_anatomy_top_cd_year.csv", na = "")

qc_df <- bind_rows(
  tibble(metric = "district_count", value = n_distinct(district_lookup$borocd), note = "Community districts assigned to treatment terciles."),
  tibble(metric = "series_family_count", value = n_distinct(top_cd_df$series_family), note = "Series covered in the anatomy tables."),
  tibble(metric = "era_count", value = n_distinct(top_cd_df$era), note = "Eras covered in the anatomy tables."),
  tibble(metric = "top_cd_row_count", value = nrow(top_cd_df), note = "Top-CD rows written; should equal series by era by 10."),
  tibble(metric = "top_cd_year_row_count", value = nrow(top_cd_year_df), note = "Top-CD-year rows written; should equal series by era by 10."),
  tibble(metric = "min_top_cd_share_sum", value = min(top_cd_df |>
    group_by(series_family, era) |>
    summarize(top10_share = sum(share_of_high_tercile_total, na.rm = TRUE), .groups = "drop") |>
    pull(top10_share), na.rm = TRUE), note = "Minimum share captured by the top 10 CDs within a series-era high-tercile total."),
  tibble(metric = "min_top_cd_year_share_sum", value = min(top_cd_year_df |>
    group_by(series_family, era) |>
    summarize(top10_share = sum(share_of_high_tercile_total, na.rm = TRUE), .groups = "drop") |>
    pull(top10_share), na.rm = TRUE), note = "Minimum share captured by the top 10 CD-years within a series-era high-tercile total.")
)

write_csv(qc_df, "../output/cd_homeownership_long_units_anatomy_qc.csv", na = "")

cat("Wrote long-units anatomy outputs to ../output\n")
