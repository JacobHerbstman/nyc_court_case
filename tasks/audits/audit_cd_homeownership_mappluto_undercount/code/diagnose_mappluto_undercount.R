# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_cd_homeownership_mappluto_undercount/code")

# Where and why MapPLUTO yearbuilt counts fall short of Census year built:
# CD-by-window comparisons of MapPLUTO (25v4 and 02b), Census 1990/2000,
# ACS 2020-2024, DCP Housing Database, DOB permits and Census BPS permits,
# summarized by within-borough 1990 homeownership tercile and by borough, and
# CD-level regressions of the Census-minus-MapPLUTO gap on candidate explanations.

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(readr)
  library(tidyr)
})

source("../../../_lib/data_reports.R")

treatment <- read_csv("../input/cd_homeownership_1990_measure.csv", show_col_types = FALSE) |>
  transmute(borocd = sprintf("%03d", as.integer(borocd)), borough_code = as.integer(borough_code), borough_name, treat_pp, treat_z_boro) |>
  group_by(borough_code) |>
  mutate(tercile = c("Low", "Middle", "High")[ntile(treat_pp, 3)]) |>
  ungroup()

stopifnot(nrow(treatment) == 59, !anyDuplicated(treatment$borocd))

pluto <- read_csv("../output/mappluto_vintage_cd_year.csv", show_col_types = FALSE, col_types = cols(vintage = "c", borocd = "c"))
census <- read_csv("../output/census_year_built_cd.csv", show_col_types = FALSE, col_types = cols(borocd = "c"))
hdb <- read_csv("../output/hdb_cd_year.csv", show_col_types = FALSE, col_types = cols(borocd = "c"))
dob <- read_csv("../output/dob_permit_cd_year.csv", show_col_types = FALSE, col_types = cols(borocd = "c"))

windows <- tribble(
  ~window, ~first_year, ~last_year,
  "1970-1979", 1970, 1979,
  "1980-1984", 1980, 1984,
  "1985-1989", 1985, 1989,
  "1980-1989", 1980, 1989,
  "1990-1999", 1990, 1999,
  "1980-1999", 1980, 1999,
  "2000-2009", 2000, 2009,
  "2000-2019", 2000, 2019,
  "2010-2019", 2010, 2019
)

# Survey counts for the windows each survey reports (Census windows ending in March keep their own length)
survey_windows <- census |>
  transmute(
    borocd,
    `census1990_units|1970-1979` = c1990_built_1970_1979,
    `census1990_units|1980-1984` = c1990_built_1980_1984,
    `census1990_units|1985-1989` = c1990_built_1985_1988 + c1990_built_1989_mar1990,
    `census1990_units|1980-1989` = c1990_built_1980_1984 + c1990_built_1985_1988 + c1990_built_1989_mar1990,
    `census1990_renter_units|1985-1989` = c1990_renter_built_1985_mar1990,
    `census2000_units|1970-1979` = c2000_built_1970_1979,
    `census2000_units|1980-1989` = c2000_built_1980_1989,
    `census2000_units|1990-1999` = c2000_built_1990_mar2000,
    `acs2024_units|1970-1979` = acs2024_built_1970_1979,
    `acs2024_units|1980-1989` = acs2024_built_1980_1989,
    `acs2024_units|1990-1999` = acs2024_built_1990_1999,
    `acs2024_units|2000-2009` = acs2024_built_2000_2009,
    `acs2024_units|2010-2019` = acs2024_built_2010_2019,
    `acs2024_occ_units_5_plus|1980-1999` = acs2024_occ_5_plus_built_1980_1999,
    `acs2024_occ_units_5_plus|2000-2019` = acs2024_occ_5_plus_built_2000_2019,
    `acs2024_occ_units_1_4|1980-1999` = acs2024_occ_1_4_built_1980_1999,
    `acs2024_occ_units_1_4|2000-2019` = acs2024_occ_1_4_built_2000_2019
  ) |>
  pivot_longer(-borocd, names_to = c(".value", "window"), names_sep = "\\|")

# MapPLUTO and administrative sums over each window. Housing Database sums
# are kept for 2010-2019 only and DOB permit sums for 1990-1999 only.
sum_window <- function(window, first_year, last_year) {
  pluto_sums <- pluto |>
    filter(year >= first_year, year <= last_year) |>
    mutate(heaped = year %% 5 == 0) |>
    group_by(borocd) |>
    summarise(
      pluto25_units = sum(units_built[vintage == "25v4"]),
      pluto25_units_1_4 = sum(units_built_1_4[vintage == "25v4"]),
      pluto25_units_5_plus = sum(units_built_5_plus[vintage == "25v4"]),
      pluto25_units_heaped_year = sum(units_built[vintage == "25v4" & heaped]),
      pluto25_altered_units = sum(units_altered[vintage == "25v4"]),
      pluto25_altered_units_pre1940_5_plus = sum(units_altered_pre1940_5_plus[vintage == "25v4"]),
      pluto02b_units = sum(units_built[vintage == "02b"]),
      pluto02b_units_same_lot_and_year_in_25v4 = sum(units_built_same_lot_and_year_in_25v4[vintage == "02b"]),
      .groups = "drop"
    ) |>
    mutate(across(starts_with("pluto02b_"), ~ if (last_year <= 1999) .x else NA_real_))

  hdb_sums <- hdb |>
    filter(year >= first_year, year <= last_year, window == "2010-2019") |>
    group_by(borocd) |>
    summarise(hdb_nb_units = sum(hdb_nb_units), hdb_nb_units_5_plus = sum(hdb_nb_units_5_plus),
              hdb_alteration_units_added = sum(hdb_alteration_units_added), .groups = "drop")

  dob_sums <- dob |>
    filter(year >= first_year, year <= last_year, window == "1990-1999") |>
    group_by(borocd) |>
    summarise(dob_residential_nb_jobs = sum(residential_jobs_NB), dob_residential_a1_jobs = sum(residential_jobs_A1), .groups = "drop")

  pluto_sums |>
    left_join(hdb_sums, by = "borocd", relationship = "one-to-one") |>
    left_join(dob_sums, by = "borocd", relationship = "one-to-one") |>
    mutate(window = window)
}

window_sums <- bind_rows(Map(sum_window, windows$window, windows$first_year, windows$last_year))

city_owned_2002 <- pluto |>
  filter(vintage == "02b") |>
  group_by(borocd) |>
  summarise(pluto02b_city_owned_units = sum(units_built_city_owned), .groups = "drop")

cd_windows <- window_sums |>
  left_join(survey_windows, by = c("borocd", "window"), relationship = "one-to-one") |>
  left_join(city_owned_2002, by = "borocd", relationship = "many-to-one") |>
  left_join(census |> transmute(borocd, c1990_housing_units, c1990_year_built_allocated,
                                housing_unit_change_1990_2000 = dcp_housing_units_2000 - dcp_housing_units_1990),
            by = "borocd", relationship = "many-to-one") |>
  inner_join(treatment, by = "borocd", relationship = "many-to-one") |>
  relocate(borocd, borough_code, borough_name, tercile, treat_pp, treat_z_boro, window) |>
  arrange(window, borocd)

stopifnot(nrow(cd_windows) == 59 * nrow(windows))

save_csv(cd_windows, "../output/undercount_cd_windows.csv", c("borocd", "window"))

# Tercile and borough totals with MapPLUTO-to-survey ratios
count_cols <- setdiff(names(cd_windows), c("borocd", "borough_code", "borough_name", "tercile", "treat_pp", "treat_z_boro", "window",
                                           "pluto02b_city_owned_units", "c1990_housing_units", "c1990_year_built_allocated",
                                           "housing_unit_change_1990_2000"))

add_ratios <- function(df) {
  df |>
    mutate(
      pluto_over_census1990 = pluto25_units / census1990_units,
      pluto_over_census2000 = pluto25_units / census2000_units,
      pluto_over_acs2024 = pluto25_units / acs2024_units,
      pluto_5_plus_over_acs2024_occ_5_plus = pluto25_units_5_plus / acs2024_occ_units_5_plus,
      pluto_1_4_over_acs2024_occ_1_4 = pluto25_units_1_4 / acs2024_occ_units_1_4,
      pluto_over_hdb = pluto25_units / hdb_nb_units,
      pluto_5_plus_over_hdb_5_plus = pluto25_units_5_plus / hdb_nb_units_5_plus,
      pluto25_over_pluto02b = pluto25_units / pluto02b_units,
      census1990_renter_share = census1990_renter_units / census1990_units
    )
}

tercile_summary <- bind_rows(
  cd_windows |> group_by(window, tercile) |> summarise(across(all_of(count_cols), sum), n_districts = n(), .groups = "drop"),
  cd_windows |> group_by(window) |> summarise(across(all_of(count_cols), sum), n_districts = n(), .groups = "drop") |> mutate(tercile = "All")
) |>
  add_ratios() |>
  mutate(tercile = factor(tercile, levels = c("Low", "Middle", "High", "All"))) |>
  arrange(window, tercile) |>
  mutate(tercile = as.character(tercile))

save_csv(tercile_summary, "../output/undercount_tercile_summary.csv", c("window", "tercile"))

# Census Building Permits Survey (new privately owned units authorized) by borough
bps <- read_parquet("../input/census_bps_borough_year.parquet") |>
  transmute(borough_code = match(county_code, c("061", "005", "047", "081", "085")), year,
            bps_units = total_units, bps_units_5_plus = five_plus_unit_units)

bps_windows <- bind_rows(Map(function(window, first_year, last_year) {
  bps |>
    filter(year >= first_year, year <= last_year) |>
    group_by(borough_code) |>
    summarise(bps_units = sum(bps_units), bps_units_5_plus = sum(bps_units_5_plus), .groups = "drop") |>
    mutate(window = window)
}, windows$window, windows$first_year, windows$last_year))

borough_summary <- cd_windows |>
  group_by(window, borough_code, borough_name) |>
  summarise(across(all_of(count_cols), sum), .groups = "drop") |>
  left_join(bps_windows, by = c("window", "borough_code"), relationship = "one-to-one") |>
  add_ratios() |>
  mutate(pluto_over_bps = pluto25_units / bps_units, pluto_5_plus_over_bps_5_plus = pluto25_units_5_plus / bps_units_5_plus) |>
  arrange(window, borough_code)

save_csv(borough_summary, "../output/undercount_borough_summary.csv", c("window", "borough_code"))

# CD-level gap regressions within borough: does a candidate explanation absorb the homeownership gradient?
gap_df <- cd_windows |>
  filter(window %in% c("1985-1989", "1990-1999")) |>
  mutate(
    gap = if_else(window == "1985-1989", census1990_units, census2000_units) - pluto25_units,
    city_owned_2002_per_1000 = pluto02b_city_owned_units / c1990_housing_units * 1000,
    year_built_allocated_share = c1990_year_built_allocated / c1990_housing_units,
    stock_change_minus_pluto_1990s = housing_unit_change_1990_2000 - pluto25_units
  )

gap_models <- tribble(
  ~model_id, ~covariates,
  "treat_only", "",
  "altered_pre1940_5_plus", "pluto25_altered_units_pre1940_5_plus",
  "city_owned_2002", "pluto02b_city_owned_units",
  "dob_a1_jobs", "dob_residential_a1_jobs",
  "year_built_allocated_share", "year_built_allocated_share",
  "all_candidates", "pluto25_altered_units_pre1940_5_plus + pluto02b_city_owned_units + year_built_allocated_share"
)

estimate_gap_model <- function(gap_window, model_id, covariates) {
  model <- feols(as.formula(paste("gap ~ treat_z_boro", if (nzchar(covariates)) paste("+", covariates), "| borough_code")),
                 data = gap_df[gap_df$window == gap_window, ], vcov = "hetero")
  coefs <- coeftable(model)
  tibble(window = gap_window, model_id, term = rownames(coefs), estimate = coefs[, 1], std_error = coefs[, 2], p_value = coefs[, 4],
         within_r2 = unname(r2(model, "wr2")), gap_mean = mean(gap_df$gap[gap_df$window == gap_window]))
}

gap_specs <- expand_grid(window = c("1985-1989", "1990-1999"), gap_models) |>
  filter(!(window == "1985-1989" & model_id == "dob_a1_jobs")) |>
  mutate(covariates = if_else(window == "1990-1999" & model_id == "all_candidates",
                              paste(covariates, "+ dob_residential_a1_jobs"), covariates))

gap_correlates <- bind_rows(Map(estimate_gap_model, gap_specs$window, gap_specs$model_id, gap_specs$covariates))

save_csv(gap_correlates, "../output/undercount_gap_correlates.csv", c("window", "model_id", "term"))

# Figure: MapPLUTO relative to each survey by tercile and vintage window
plot_df <- tercile_summary |>
  filter(tercile != "All") |>
  select(window, tercile, pluto_over_census1990, pluto_over_census2000, pluto_over_acs2024, pluto_over_hdb,
         pluto_5_plus_over_acs2024_occ_5_plus, pluto_1_4_over_acs2024_occ_1_4) |>
  pivot_longer(-c(window, tercile), names_to = "comparison", values_to = "ratio") |>
  filter(!is.na(ratio)) |>
  mutate(
    comparison = recode(comparison,
      pluto_over_census1990 = "All units: vs Census 1990", pluto_over_census2000 = "All units: vs Census 2000",
      pluto_over_acs2024 = "All units: vs ACS 2020-24", pluto_over_hdb = "All units: vs Housing Database",
      pluto_5_plus_over_acs2024_occ_5_plus = "5+ buildings: vs ACS occupied", pluto_1_4_over_acs2024_occ_1_4 = "1-4 buildings: vs ACS occupied"),
    tercile = factor(tercile, levels = c("Low", "Middle", "High")),
    window = factor(window, levels = windows$window)
  )

pdf("../output/undercount_ratio_by_tercile.pdf", width = 11, height = 6.5)
print(
  ggplot(plot_df, aes(window, ratio, color = tercile, group = tercile)) +
    geom_hline(yintercept = 1, linetype = "dashed", color = "grey50") +
    geom_point(size = 2) +
    geom_line() +
    facet_wrap(~comparison, scales = "free_x") +
    scale_color_manual(values = c(Low = "#b2182b", Middle = "#999999", High = "#2166ac")) +
    labs(x = "Year-built window", y = "MapPLUTO 25v4 units / comparison source", color = "1990 homeownership tercile (within borough)") +
    theme_minimal(base_size = 10) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
invisible(dev.off())

cat("Wrote undercount diagnostics to ../output\n")
