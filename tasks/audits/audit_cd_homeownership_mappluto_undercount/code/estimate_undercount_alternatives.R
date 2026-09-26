# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_cd_homeownership_mappluto_undercount/code")

# Re-estimates the production 5+ unit event study (CD FE, borough-by-period FE,
# period-interacted 1990 controls, 1985-1989 reference) after replacing or
# rescaling the pre-2000 MapPLUTO counts with Census year-built counts, and
# estimates long differences measured entirely from Census/ACS year built.
# The production spec is copied from
# estimate_cd_homeownership_long_units_event_study.R; production files are only read.

suppressPackageStartupMessages({
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(readr)
  library(tidyr)
})

source("../../../_lib/data_reports.R")

event_periods <- c("1970-1974", "1975-1979", "1980-1984", "1985-1989", "1990-1994", "1995-1999",
                   "2000-2004", "2005-2009", "2010-2014", "2015-2019", "2020-2025")
period_of <- function(year) event_periods[findInterval(year, c(seq(1970, 2020, 5), 2026))]
z_score <- function(x) (x - mean(x)) / sd(x)

# Production CD-year series (25v4 MapPLUTO yearbuilt) and treatment
series <- read_csv("../input/cd_homeownership_long_units_series.csv", show_col_types = FALSE, col_types = cols(district_id = "c")) |>
  filter(series_kind == "preferred_long_series", source_family == "mappluto_proxy_25v4",
         series_family %in% c("units_built_total", "units_built_1_4", "units_built_5_plus"),
         year >= 1960, year <= 2025, occupied_units_1990 > 0)

cd_controls <- series |>
  distinct(district_id, borough_code, treat_z_boro, occupied_units_1990, vacancy_rate_1990, median_household_income_1990) |>
  group_by(borough_code) |>
  mutate(
    log_occupied_units_1990_z = z_score(log(occupied_units_1990)),
    vacancy_rate_1990_z = z_score(vacancy_rate_1990),
    median_household_income_1990_z = z_score(median_household_income_1990)
  ) |>
  ungroup() |>
  select(district_id, borough_code, treat_z_boro, log_occupied_units_1990_z, vacancy_rate_1990_z, median_household_income_1990_z)

panel <- series |>
  select(district_id, year, series_family, outcome_value) |>
  pivot_wider(names_from = series_family, values_from = outcome_value) |>
  rename(pluto_total = units_built_total, pluto_1_4 = units_built_1_4, pluto_5_plus = units_built_5_plus) |>
  left_join(cd_controls, by = "district_id", relationship = "many-to-one")

stopifnot(nrow(cd_controls) == 59, !anyDuplicated(panel[c("district_id", "year")]), !anyNA(panel))

# Census/ACS counts matched to MapPLUTO yearbuilt windows. Census windows that
# run to March are converted to the MapPLUTO window length.
cd_windows <- panel |>
  group_by(district_id) |>
  summarise(
    pluto_1970_1979 = sum(pluto_total[year %in% 1970:1979]),
    pluto_1980_1984 = sum(pluto_total[year %in% 1980:1984]),
    pluto_1985_1989 = sum(pluto_total[year %in% 1985:1989]),
    pluto_1990_1999 = sum(pluto_total[year %in% 1990:1999]),
    pluto_5_plus_1960_1979 = sum(pluto_5_plus[year %in% 1960:1979]),
    pluto_5_plus_1980_1999 = sum(pluto_5_plus[year %in% 1980:1999]),
    pluto_5_plus_2000_2019 = sum(pluto_5_plus[year %in% 2000:2019]),
    .groups = "drop"
  ) |>
  left_join(read_csv("../output/census_year_built_cd.csv", show_col_types = FALSE, col_types = cols(borocd = "c")),
            by = c("district_id" = "borocd"), relationship = "one-to-one") |>
  mutate(
    census_1970_1979 = c1990_built_1970_1979,
    census_1980_1984 = c1990_built_1980_1984,
    census_1985_1989 = (c1990_built_1985_1988 + c1990_built_1989_mar1990) * 5 / 5.25,
    census_1990_1999 = c2000_built_1990_mar2000 * 10 / 10.25,
    acs_ratio_1960_1979 = acs2024_occ_5_plus_built_1960_1979 / pluto_5_plus_1960_1979,
    acs_ratio_1980_1999 = acs2024_occ_5_plus_built_1980_1999 / pluto_5_plus_1980_1999,
    acs_ratio_2000_2019 = acs2024_occ_5_plus_built_2000_2019 / pluto_5_plus_2000_2019
  )

# A ratio is undefined where MapPLUTO has no units in the window; those CD-windows keep the MapPLUTO count.
zero_denominators <- cd_windows |>
  summarise(across(c(pluto_1970_1979, pluto_1980_1984, pluto_1985_1989, pluto_1990_1999,
                     pluto_5_plus_1960_1979, pluto_5_plus_1980_1999, pluto_5_plus_2000_2019), ~ sum(.x == 0)))
cat("CDs with zero MapPLUTO units by window (ratio left at 1):\n")
print(as.data.frame(zero_denominators))

ratio_or_one <- function(x) if_else(is.finite(x), x, 1)

# Alternative 5+ outcomes. Additive versions put the entire Census-minus-MapPLUTO
# gap (all building sizes) into 5+ buildings, spread evenly over the window's years.
panel <- panel |>
  filter(year >= 1970) |>
  left_join(cd_windows, by = "district_id", relationship = "many-to-one") |>
  mutate(
    census_gap_per_year = case_when(
      year <= 1979 ~ (census_1970_1979 - pluto_1970_1979) / 10,
      year <= 1984 ~ (census_1980_1984 - pluto_1980_1984) / 5,
      year <= 1989 ~ (census_1985_1989 - pluto_1985_1989) / 5,
      year <= 1999 ~ (census_1990_1999 - pluto_1990_1999) / 10,
      TRUE ~ 0
    ),
    census_ratio = case_when(
      year <= 1979 ~ ratio_or_one(census_1970_1979 / pluto_1970_1979),
      year <= 1984 ~ ratio_or_one(census_1980_1984 / pluto_1980_1984),
      year <= 1989 ~ ratio_or_one(census_1985_1989 / pluto_1985_1989),
      year <= 1999 ~ ratio_or_one(census_1990_1999 / pluto_1990_1999),
      TRUE ~ 1
    ),
    acs_ratio = case_when(
      year <= 1979 ~ ratio_or_one(acs_ratio_1960_1979),
      year <= 1999 ~ ratio_or_one(acs_ratio_1980_1999),
      TRUE ~ ratio_or_one(acs_ratio_2000_2019)
    ),
    y_baseline = pluto_5_plus,
    y_census_reference_additive = pluto_5_plus + if_else(year %in% 1985:1989, census_gap_per_year, 0),
    y_census_pre2000_additive = pluto_5_plus + census_gap_per_year,
    y_census_pre2000_proportional = pluto_5_plus * census_ratio,
    y_acs_5_plus_calibrated = pluto_5_plus * acs_ratio
  )

# DCP Housing Database completed new-building units, used in place of MapPLUTO from 2010
hdb_year <- read_csv("../output/hdb_cd_year.csv", show_col_types = FALSE, col_types = cols(borocd = "c")) |>
  transmute(district_id = borocd, year, hdb_nb_units_5_plus)

panel <- panel |>
  left_join(hdb_year, by = c("district_id", "year"), relationship = "one-to-one") |>
  mutate(
    y_hdb_post_2010 = if_else(year >= 2010, hdb_nb_units_5_plus, pluto_5_plus),
    y_census_reference_hdb_post = if_else(year >= 2010, hdb_nb_units_5_plus, y_census_reference_additive)
  )

stopifnot(!anyNA(panel$y_hdb_post_2010))

specs <- tribble(
  ~spec_id, ~outcome, ~reference_period, ~spec_label,
  "baseline", "y_baseline", "1985-1989", "Production: 25v4 MapPLUTO 5+ units, reference 1985-1989",
  "reference_2000_2004", "y_baseline", "2000-2004", "MapPLUTO 5+ units, reference 2000-2004 (MapPLUTO matches ACS for 2000s vintages)",
  "census_reference_additive", "y_census_reference_additive", "1985-1989", "1985-1989 raised to Census 1990 count; entire all-size gap added to 5+",
  "census_pre2000_additive", "y_census_pre2000_additive", "1985-1989", "All 1970-1999 windows raised to Census 1990/2000 counts; entire gap added to 5+",
  "census_pre2000_proportional", "y_census_pre2000_proportional", "1985-1989", "1970-1999 5+ scaled by CD Census/MapPLUTO all-unit ratio",
  "acs_5_plus_calibrated", "y_acs_5_plus_calibrated", "1985-1989", "5+ scaled by CD ACS 2020-2024 / MapPLUTO 5+ ratio for 1960-79, 1980-99, 2000-19 vintages",
  "hdb_post_2010", "y_hdb_post_2010", "1985-1989", "2010-2025 replaced by DCP Housing Database completed new-building 5+ units",
  "census_reference_hdb_post", "y_census_reference_hdb_post", "1985-1989", "Census 1990 reference (gap added to 5+) and Housing Database 2010-2025"
)

control_vars <- c("log_occupied_units_1990_z", "median_household_income_1990_z", "vacancy_rate_1990_z", "pre_1970_1988_count_z")

# Production event-study specification applied to any outcome column
estimate_event_study <- function(outcome, reference_period) {
  design <- panel |>
    mutate(y = .data[[outcome]], event_period = period_of(year), borough_period = paste(borough_code, event_period)) |>
    group_by(district_id) |>
    mutate(pre_1970_1988_count = mean(y[year <= 1988])) |>
    ungroup()

  pre_count_z <- design |>
    distinct(district_id, pre_1970_1988_count) |>
    mutate(pre_1970_1988_count_z = z_score(pre_1970_1988_count))

  design <- design |>
    left_join(pre_count_z |> select(-pre_1970_1988_count), by = "district_id", relationship = "many-to-one")

  estimated_periods <- setdiff(event_periods, reference_period)
  terms <- c()

  for (period in estimated_periods) {
    suffix <- gsub("-", "_", period)
    in_period <- as.integer(design$event_period == period)
    design[[paste0("treat_x_", suffix)]] <- design$treat_z_boro * in_period
    terms <- c(terms, paste0("treat_x_", suffix))

    for (control in control_vars) {
      design[[paste0(control, "_x_", suffix)]] <- design[[control]] * in_period
      terms <- c(terms, paste0(control, "_x_", suffix))
    }
  }

  model <- feols(as.formula(paste("y ~", paste(terms, collapse = " + "), "| district_id + borough_period")),
                 cluster = ~district_id, data = design)

  tibble(event_period = estimated_periods, term = paste0("treat_x_", gsub("-", "_", estimated_periods))) |>
    mutate(
      estimate = unname(coef(model)[term]),
      std_error = unname(se(model)[term]),
      p_value = unname(pvalue(model)[term])
    ) |>
    bind_rows(tibble(event_period = reference_period, estimate = 0)) |>
    mutate(reference_mean = mean(design$y[design$event_period == reference_period])) |>
    select(-term)
}

event_coefficients <- specs |>
  rowwise() |>
  reframe(spec_id, spec_label, reference_period, estimate_event_study(outcome, reference_period)) |>
  mutate(event_period = factor(event_period, levels = event_periods)) |>
  arrange(factor(spec_id, levels = specs$spec_id), event_period) |>
  mutate(event_period = as.character(event_period))

baseline_post <- event_coefficients |> filter(spec_id == "baseline", event_period %in% c("2010-2014", "2015-2019", "2020-2025"))
cat("Baseline 5+ coefficients (should match production 2010-14 -168.3, 2015-19 -274.1, 2020-25 -357.9):",
    round(baseline_post$estimate, 1), "\n")
stopifnot(abs(baseline_post$estimate - c(-168.329, -274.082, -357.900)) < 0.01)

save_csv(event_coefficients, "../output/undercount_event_study_coefficients.csv", c("spec_id", "event_period"))

# Long differences: production long-difference spec (borough FE, 1990 controls,
# robust SE), comparing MapPLUTO and Census/ACS measures over identical windows.
ld_controls <- cd_controls |>
  left_join(
    panel |>
      filter(year <= 1988) |>
      group_by(district_id) |>
      summarise(pre = mean(pluto_5_plus)) |>
      mutate(pre_1970_1988_count_z = z_score(pre)) |>
      select(-pre),
    by = "district_id", relationship = "one-to-one"
  )

# Annual averages by CD for each measure and vintage window
window_mean <- function(column, first_year, last_year) {
  panel |>
    filter(year >= first_year, year <= last_year) |>
    group_by(district_id) |>
    summarise(value = mean(.data[[column]]), .groups = "drop") |>
    pull(value, name = district_id)
}

hdb <- read_csv("../output/hdb_cd_year.csv", show_col_types = FALSE, col_types = cols(borocd = "c")) |>
  filter(year <= 2019) |>
  group_by(district_id = borocd) |>
  summarise(
    hdb_nb_2010_2019 = mean(hdb_nb_units),
    hdb_nb_5_plus_2010_2019 = mean(hdb_nb_units_5_plus),
    hdb_nb_plus_alterations_2010_2019 = mean(hdb_nb_units + hdb_alteration_units_added),
    .groups = "drop"
  )

cd_measures <- ld_controls |>
  mutate(
    pluto_total_2010_2019 = window_mean("pluto_total", 2010, 2019)[district_id],
    pluto_total_1985_1989 = window_mean("pluto_total", 1985, 1989)[district_id],
    pluto_total_1980_1989 = window_mean("pluto_total", 1980, 1989)[district_id],
    pluto_5_plus_2010_2019 = window_mean("pluto_5_plus", 2010, 2019)[district_id],
    pluto_5_plus_1985_1989 = window_mean("pluto_5_plus", 1985, 1989)[district_id],
    pluto_5_plus_2000_2019 = window_mean("pluto_5_plus", 2000, 2019)[district_id],
    pluto_5_plus_1980_1999 = window_mean("pluto_5_plus", 1980, 1999)[district_id],
    pluto_1_4_2000_2019 = window_mean("pluto_1_4", 2000, 2019)[district_id],
    pluto_1_4_1980_1999 = window_mean("pluto_1_4", 1980, 1999)[district_id]
  ) |>
  left_join(
    cd_windows |>
      transmute(
        district_id,
        census1990_1985_1989 = census_1985_1989 / 5,
        census2000_1980_1989 = c2000_built_1980_1989 / 10,
        acs_total_2010_2019 = acs2024_built_2010_2019 / 10,
        acs_total_1980_1989 = acs2024_built_1980_1989 / 10,
        acs_5_plus_2000_2019 = acs2024_occ_5_plus_built_2000_2019 / 20,
        acs_5_plus_1980_1999 = acs2024_occ_5_plus_built_1980_1999 / 20,
        acs_1_4_2000_2019 = acs2024_occ_1_4_built_2000_2019 / 20,
        acs_1_4_1980_1999 = acs2024_occ_1_4_built_1980_1999 / 20
      ),
    by = "district_id", relationship = "one-to-one"
  ) |>
  left_join(hdb, by = "district_id", relationship = "one-to-one")

stopifnot(nrow(cd_measures) == 59, !anyNA(cd_measures))

# Each row regresses (post - pre) on homeownership. "Gap" rows put two measures of
# the same window in post and pre: the coefficient is how the measurement
# difference varies with homeownership, i.e. the bias it would add to a long difference.
comparisons <- tribble(
  ~comparison_id, ~outcome, ~measure, ~post, ~pre,
  "all_2010s_vs_1985_1989", "All units", "MapPLUTO", "pluto_total_2010_2019", "pluto_total_1985_1989",
  "all_2010s_vs_1985_1989", "All units", "ACS 2020-24 post, Census 1990 pre", "acs_total_2010_2019", "census1990_1985_1989",
  "all_2010s_vs_1985_1989", "All units", "HDB new buildings post, Census 1990 pre", "hdb_nb_2010_2019", "census1990_1985_1989",
  "all_2010s_vs_1985_1989", "All units", "HDB new buildings + alteration additions post, Census 1990 pre", "hdb_nb_plus_alterations_2010_2019", "census1990_1985_1989",
  "all_2010s_vs_1980s", "All units", "MapPLUTO", "pluto_total_2010_2019", "pluto_total_1980_1989",
  "all_2010s_vs_1980s", "All units", "ACS 2020-24 post and pre", "acs_total_2010_2019", "acs_total_1980_1989",
  "all_2010s_vs_1980s", "All units", "ACS 2020-24 post, Census 2000 pre", "acs_total_2010_2019", "census2000_1980_1989",
  "units_5_plus_2010s_vs_1985_1989", "5+ unit buildings", "MapPLUTO", "pluto_5_plus_2010_2019", "pluto_5_plus_1985_1989",
  "units_5_plus_2010s_vs_1985_1989", "5+ unit buildings", "HDB new buildings post, MapPLUTO pre", "hdb_nb_5_plus_2010_2019", "pluto_5_plus_1985_1989",
  "units_5_plus_2000_2019_vs_1980_1999", "5+ unit buildings", "MapPLUTO", "pluto_5_plus_2000_2019", "pluto_5_plus_1980_1999",
  "units_5_plus_2000_2019_vs_1980_1999", "5+ unit buildings", "ACS 2020-24 occupied units", "acs_5_plus_2000_2019", "acs_5_plus_1980_1999",
  "units_1_4_2000_2019_vs_1980_1999", "1-4 unit buildings", "MapPLUTO", "pluto_1_4_2000_2019", "pluto_1_4_1980_1999",
  "units_1_4_2000_2019_vs_1980_1999", "1-4 unit buildings", "ACS 2020-24 occupied units", "acs_1_4_2000_2019", "acs_1_4_1980_1999",
  "gap_1985_1989", "All units", "Census 1990 minus MapPLUTO", "census1990_1985_1989", "pluto_total_1985_1989",
  "gap_1980s", "All units", "Census 2000 minus MapPLUTO", "census2000_1980_1989", "pluto_total_1980_1989",
  "gap_1980s", "All units", "ACS 2020-24 minus MapPLUTO", "acs_total_1980_1989", "pluto_total_1980_1989",
  "gap_2010s", "All units", "ACS 2020-24 minus MapPLUTO", "acs_total_2010_2019", "pluto_total_2010_2019",
  "gap_2010s", "All units", "HDB new buildings minus MapPLUTO", "hdb_nb_2010_2019", "pluto_total_2010_2019",
  "gap_2010s", "5+ unit buildings", "HDB new buildings minus MapPLUTO", "hdb_nb_5_plus_2010_2019", "pluto_5_plus_2010_2019",
  "gap_1980_1999", "5+ unit buildings", "ACS 2020-24 minus MapPLUTO", "acs_5_plus_1980_1999", "pluto_5_plus_1980_1999",
  "gap_2000_2019", "5+ unit buildings", "ACS 2020-24 minus MapPLUTO", "acs_5_plus_2000_2019", "pluto_5_plus_2000_2019"
)

estimate_long_difference <- function(post, pre) {
  model_df <- cd_measures |> mutate(delta = .data[[post]] - .data[[pre]])
  model <- feols(as.formula(paste("delta ~ treat_z_boro +", paste(control_vars, collapse = " + "), "| borough_code")),
                 data = model_df, vcov = "hetero")
  tibble(
    estimate = unname(coef(model)["treat_z_boro"]), std_error = unname(se(model)["treat_z_boro"]),
    p_value = unname(pvalue(model)["treat_z_boro"]),
    post_mean = mean(model_df[[post]]), pre_mean = mean(model_df[[pre]]), n_districts = nobs(model)
  )
}

long_differences <- bind_cols(comparisons, bind_rows(Map(estimate_long_difference, comparisons$post, comparisons$pre)))

save_csv(long_differences, "../output/undercount_long_differences.csv", c("comparison_id", "outcome", "measure"))

plot_df <- event_coefficients |>
  mutate(
    period_index = match(event_period, event_periods),
    spec_id = factor(spec_id, levels = specs$spec_id),
    conf_low = estimate - 1.96 * std_error,
    conf_high = estimate + 1.96 * std_error
  )

pdf("../output/undercount_event_study_coefficients.pdf", width = 11, height = 6.5)
print(
  ggplot(plot_df, aes(period_index, estimate, color = spec_id)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "grey50") +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.15, position = position_dodge(width = 0.6), na.rm = TRUE) +
    geom_point(position = position_dodge(width = 0.6)) +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(x = NULL, y = "5+ units per year per SD of 1990 homeownership", color = NULL,
         title = "5+ unit event study under alternative pre-2000 measures") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1)) +
    guides(color = guide_legend(ncol = 2))
)
invisible(dev.off())

cat("Wrote event-study and long-difference comparisons to ../output\n")
