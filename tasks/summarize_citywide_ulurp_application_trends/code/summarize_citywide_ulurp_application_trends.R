# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_citywide_ulurp_application_trends/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

has_action_code <- function(x, code) {
  raw_value <- str_to_upper(coalesce(as.character(x), ""))
  str_detect(raw_value, paste0("(^|[^A-Z0-9])", code, "([^A-Z0-9]|$)"))
}

has_ulurp_code <- function(x, code) {
  raw_value <- str_replace_all(str_to_upper(coalesce(as.character(x), "")), "\\s+", "")
  str_detect(raw_value, paste0("[0-9]{6,7}A?", code, "[A-Z]"))
}

nonempty_text <- function(x) {
  !is.na(x) & str_squish(as.character(x)) != ""
}

extract_ulurp_numbers <- function(x) {
  raw_value <- str_replace_all(str_to_upper(coalesce(as.character(x), "")), "\\s+", "")
  str_extract_all(raw_value, "\\b[CN]?[0-9]{6,7}A?[A-Z]{3}\\b")
}

period_from_year <- function(year_value) {
  case_when(
    year_value >= 1976 & year_value <= 1979 ~ "1976-1979",
    year_value >= 1980 & year_value <= 1984 ~ "1980-1984",
    year_value >= 1985 & year_value <= 1989 ~ "1985-1989",
    year_value >= 1990 & year_value <= 1994 ~ "1990-1994",
    year_value >= 1995 & year_value <= 1999 ~ "1995-1999",
    year_value >= 2000 & year_value <= 2004 ~ "2000-2004",
    year_value >= 2005 & year_value <= 2009 ~ "2005-2009",
    year_value >= 2010 & year_value <= 2014 ~ "2010-2014",
    year_value >= 2015 & year_value <= 2019 ~ "2015-2019",
    year_value >= 2020 & year_value <= 2025 ~ "2020-2025",
    TRUE ~ NA_character_
  )
}

centered_ma3 <- function(x) {
  out <- rep(NA_real_, length(x))
  if (length(x) < 3) {
    return(out)
  }

  for (i in 2:(length(x) - 1)) {
    window <- x[(i - 1):(i + 1)]
    if (all(!is.na(window))) {
      out[i] <- mean(window)
    }
  }

  out
}

project_df <- read_parquet("../input/zap_project_data.parquet") |>
  mutate(
    project_id = as.character(project_id),
    ulurp_flag = str_to_upper(str_squish(coalesce(as.character(ulurp_non), ""))) == "ULURP",
    borocd = if_else(
      !is.na(community_district_standardized),
      sprintf("%03d", suppressWarnings(as.integer(community_district_standardized))),
      NA_character_
    ),
    cert_year = suppressWarnings(as.integer(format(certified_referred_date_parsed, "%Y"))),
    app_filed_year = suppressWarnings(as.integer(format(app_filed_date_parsed, "%Y"))),
    noticed_year = suppressWarnings(as.integer(format(noticed_date_parsed, "%Y"))),
    approval_year = suppressWarnings(as.integer(format(approval_date_parsed, "%Y"))),
    completed_year = suppressWarnings(as.integer(format(completed_date_parsed, "%Y"))),
    project_use_text = str_to_upper(str_squish(paste(
      coalesce(as.character(project_name), ""),
      coalesce(as.character(project_brief), "")
    ))),
    all_text = str_to_upper(str_squish(paste(
      coalesce(as.character(project_name), ""),
      coalesce(as.character(project_brief), ""),
      coalesce(as.character(primary_applicant), ""),
      coalesce(as.character(ceqr_leadagency), "")
    ))),
    mih_flag_bool = str_to_lower(str_squish(coalesce(as.character(mih_flag), ""))) %in% c("true", "t", "yes", "y", "1"),
    hpd_text_flag = str_detect(all_text, "\\b(HPD|HOUSING PRESERVATION|DEPARTMENT OF HOUSING)\\b")
  )

if (nrow(project_df) != n_distinct(project_df$project_id)) {
  stop("Staged ZAP project data are not unique by project_id.")
}

district_lookup <- read_csv("../input/cd_baseline_1990_controls.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    borocd = sprintf("%03d", suppressWarnings(as.integer(borocd))),
    borough_code = suppressWarnings(as.integer(borough_code)),
    borough_name = borough_name,
    treat_pp = suppressWarnings(as.numeric(treat_pp)),
    median_household_income_1990 = suppressWarnings(as.numeric(median_household_income_1990_1999_dollars_exact)),
    poverty_share_1990 = suppressWarnings(as.numeric(poverty_share_1990_exact)),
    white_share_1990 = suppressWarnings(as.numeric(white_share_1990_nhgis)),
    black_share_1990 = suppressWarnings(as.numeric(black_share_1990_nhgis)),
    hispanic_share_1990 = suppressWarnings(as.numeric(hispanic_share_1990_nhgis))
  ) |>
  group_by(borough_code, borough_name) |>
  mutate(
    treat_tercile = ntile(treat_pp, 3),
    income_tercile = ntile(median_household_income_1990, 3),
    poverty_tercile = ntile(poverty_share_1990, 3),
    white_share_tercile = ntile(white_share_1990, 3),
    black_share_tercile = ntile(black_share_1990, 3),
    hispanic_share_tercile = ntile(hispanic_share_1990, 3),
    treat_tercile_label = case_when(
      treat_tercile == 1 ~ "Low",
      treat_tercile == 2 ~ "Middle",
      TRUE ~ "High"
    )
  ) |>
  ungroup()

if (nrow(district_lookup) != n_distinct(district_lookup$borocd)) {
  stop("Homeownership district lookup is not unique by borocd.")
}

if (n_distinct(district_lookup$borocd) != 59) {
  stop("Expected homeownership district lookup to contain 59 CDs.")
}

demographic_stratifiers <- tribble(
  ~stratifier_id, ~stratifier_label, ~tercile_col,
  "income", "1990 median household income", "income_tercile",
  "poverty", "1990 poverty share", "poverty_tercile",
  "white_share", "1990 white share", "white_share_tercile",
  "black_share", "1990 Black share", "black_share_tercile",
  "hispanic_share", "1990 Hispanic share", "hispanic_share_tercile"
)

hpd_housing_codes <- c("HA", "HD", "HO", "HU", "HP", "HG", "HC", "HL", "HM")

for (code_value in hpd_housing_codes) {
  project_df[[paste0("actions_has_", str_to_lower(code_value))]] <- has_action_code(project_df$actions, code_value)
  project_df[[paste0("ulurp_has_", str_to_lower(code_value))]] <- has_ulurp_code(project_df$ulurp_numbers, code_value)
}

ulurp_df <- project_df |>
  filter(ulurp_flag) |>
  mutate(
    hpd_housing_action_flag = if_any(all_of(c(
      paste0("actions_has_", str_to_lower(hpd_housing_codes)),
      paste0("ulurp_has_", str_to_lower(hpd_housing_codes))
    )), identity),
    garage_or_parking_text_flag = str_detect(
      project_use_text,
      "GARAGE|PARKING|PARKING FACILITY|PARKING LOT|MUNICIPAL GARAGE"
    ),
    incinerator_or_waste_text_flag = str_detect(
      project_use_text,
      "INCINERATOR|WASTE TRANSFER|TRANSFER STATION|SOLID WASTE|SANITATION|REFUSE|RECYCLING|COMPOST|LANDFILL"
    ),
    jail_or_correction_text_flag = str_detect(
      project_use_text,
      "JAIL|PRISON|CORRECTION|DETENTION|DETENTION CENTER|RIKERS"
    ),
    utility_or_infrastructure_text_flag = str_detect(
      project_use_text,
      "UTILITY|POWER PLANT|SUBSTATION|SEWAGE|WASTEWATER|WATER POLLUTION CONTROL|GAS MAIN|ELECTRIC"
    ),
    shelter_or_institution_text_flag = str_detect(
      project_use_text,
      "HOMELESS|SHELTER|TRANSITIONAL HOUSING|TRANSITIONAL RESIDENCE|TEMPORARY HOUSING|FOSTER HOME|GROUP HOME|NURSING HOME|DORMITORY|RESIDENTIAL TREATMENT"
    ),
    negative_amenity_text_flag = garage_or_parking_text_flag |
      incinerator_or_waste_text_flag |
      jail_or_correction_text_flag |
      utility_or_infrastructure_text_flag,
    housing_unit_text_flag = str_detect(
      project_use_text,
      "HOUSING UNITS?|DWELLING|DWELLINGS|APARTMENT|APARTMENTS|CONDOMINIUM|CONDOMINIUMS|CONDO|CONDOS|RESIDENTIAL UNITS?|RESIDENTIAL BUILDING|RESIDENTIAL DEVELOPMENT|NEW RESIDENTIAL|RESIDENTIAL.{0,80}UNITS?|RESDL.{0,80}UNITS?|RENTAL.{0,80}UNITS?|SINGLE FAMILY|TOWNHOUSE|TOWNHOUSES|NEHEMIAH HOMES|[0-9]-FAMILY HOME|[0-9][0-9, -]*[A-Z0-9 *-]{0,30}(UNIT|UNITS|DWELLING|DWELLINGS|APARTMENT|APARTMENTS)"
    ),
    residential_strict_text_flag = str_detect(
      project_use_text,
      "\\b(RESIDENTIAL|RESIDENCE|RESIDENCES|HOUSING|DWELLING|DWELLINGS|APARTMENT|APARTMENTS|AFFORDABLE HOUSING|INCLUSIONARY HOUSING|SUPPORTIVE HOUSING|SENIOR HOUSING|HOMELESS SHELTER)\\b"
    ),
    residential_broad_text_flag = residential_strict_text_flag |
      str_detect(project_use_text, "\\b(MIXED[ -]?USE|AFFORD|RESIDENT\\b|MIH\\b|UDAAP|URBAN DEVELOPMENT ACTION AREA|DORMITORY|SENIOR|SUPPORTIVE)\\b"),
    residential_candidate_flag = residential_broad_text_flag | mih_flag_bool | hpd_text_flag | hpd_housing_action_flag,
    housing_production_conservative_flag = (housing_unit_text_flag | mih_flag_bool) &
      !negative_amenity_text_flag &
      !shelter_or_institution_text_flag,
    cert_period = period_from_year(cert_year)
  ) |>
  left_join(
    district_lookup |>
      transmute(
        borocd,
        home_borough_code = borough_code,
        home_borough_name = borough_name,
        treat_tercile,
        treat_tercile_label
      ),
    by = "borocd",
    relationship = "many-to-one"
  )

project_year_counts <- expand_grid(
  cert_year = 1976:2025,
  outcome_id = c(
    "all_ulurp_applications",
    "residential_strict_text_applications",
    "residential_broad_text_applications",
    "residential_candidate_applications",
    "housing_production_conservative_applications",
    "garage_or_parking_applications",
    "incinerator_or_waste_applications",
    "jail_or_correction_applications",
    "utility_or_infrastructure_applications",
    "negative_amenity_applications",
    "shelter_or_institution_applications"
  )
) |>
  left_join(
    ulurp_df |>
      filter(cert_year >= 1976, cert_year <= 2025) |>
      transmute(
        cert_year,
        all_ulurp_applications = TRUE,
        residential_strict_text_applications = residential_strict_text_flag,
        residential_broad_text_applications = residential_broad_text_flag,
        residential_candidate_applications = residential_candidate_flag,
        housing_production_conservative_applications = housing_production_conservative_flag,
        garage_or_parking_applications = garage_or_parking_text_flag,
        incinerator_or_waste_applications = incinerator_or_waste_text_flag,
        jail_or_correction_applications = jail_or_correction_text_flag,
        utility_or_infrastructure_applications = utility_or_infrastructure_text_flag,
        negative_amenity_applications = negative_amenity_text_flag,
        shelter_or_institution_applications = shelter_or_institution_text_flag
      ) |>
      pivot_longer(
        cols = c(
          all_ulurp_applications,
          residential_strict_text_applications,
          residential_broad_text_applications,
          residential_candidate_applications,
          housing_production_conservative_applications,
          garage_or_parking_applications,
          incinerator_or_waste_applications,
          jail_or_correction_applications,
          utility_or_infrastructure_applications,
          negative_amenity_applications,
          shelter_or_institution_applications
        ),
        names_to = "outcome_id",
        values_to = "included_flag"
      ) |>
      group_by(cert_year, outcome_id) |>
      summarize(application_count = sum(included_flag, na.rm = TRUE), .groups = "drop"),
    by = c("cert_year", "outcome_id"),
    relationship = "one-to-one"
  ) |>
  mutate(
    application_count = coalesce(application_count, 0L),
    count_unit = "zap_project_records"
  )

parsed_ulurp_number_project <- ulurp_df |>
  filter(cert_year >= 1976, cert_year <= 2025, nonempty_text(ulurp_numbers)) |>
  transmute(
    project_id,
    borocd,
    cert_year,
    residential_strict_text_flag,
    residential_broad_text_flag,
    residential_candidate_flag,
    housing_production_conservative_flag,
    garage_or_parking_text_flag,
    incinerator_or_waste_text_flag,
    jail_or_correction_text_flag,
    utility_or_infrastructure_text_flag,
    negative_amenity_text_flag,
    shelter_or_institution_text_flag,
    ulurp_application_number = extract_ulurp_numbers(ulurp_numbers)
  ) |>
  unnest_longer(ulurp_application_number) |>
  filter(nonempty_text(ulurp_application_number)) |>
  mutate(ulurp_application_number = str_to_upper(str_squish(ulurp_application_number)))

parsed_ulurp_number_base <- parsed_ulurp_number_project |>
  arrange(ulurp_application_number, cert_year, project_id) |>
  group_by(ulurp_application_number) |>
  summarize(
    cert_year = first(cert_year),
    assigned_borocd = {
      nonmissing_borocd <- sort(unique(borocd[nonempty_text(borocd)]))
      if (length(nonmissing_borocd) == 1) nonmissing_borocd[[1]] else NA_character_
    },
    source_project_count = n_distinct(project_id),
    nonmissing_primary_cd_count = n_distinct(borocd[nonempty_text(borocd)]),
    residential_strict_text_flag = any(residential_strict_text_flag, na.rm = TRUE),
    residential_broad_text_flag = any(residential_broad_text_flag, na.rm = TRUE),
    residential_candidate_flag = any(residential_candidate_flag, na.rm = TRUE),
    housing_production_conservative_flag = any(housing_production_conservative_flag, na.rm = TRUE),
    garage_or_parking_text_flag = any(garage_or_parking_text_flag, na.rm = TRUE),
    incinerator_or_waste_text_flag = any(incinerator_or_waste_text_flag, na.rm = TRUE),
    jail_or_correction_text_flag = any(jail_or_correction_text_flag, na.rm = TRUE),
    utility_or_infrastructure_text_flag = any(utility_or_infrastructure_text_flag, na.rm = TRUE),
    negative_amenity_text_flag = any(negative_amenity_text_flag, na.rm = TRUE),
    shelter_or_institution_text_flag = any(shelter_or_institution_text_flag, na.rm = TRUE),
    conflicting_cert_year_count = n_distinct(cert_year),
    .groups = "drop"
  ) |>
  left_join(
    district_lookup |>
      transmute(
        assigned_borocd = borocd,
        home_borough_code = borough_code,
        home_borough_name = borough_name,
        treat_tercile,
        treat_tercile_label
      ),
    by = "assigned_borocd",
    relationship = "many-to-one"
  )

number_year_counts <- expand_grid(
  cert_year = 1976:2025,
  outcome_id = c(
    "all_ulurp_applications",
    "residential_strict_text_applications",
    "residential_broad_text_applications",
    "residential_candidate_applications",
    "housing_production_conservative_applications",
    "garage_or_parking_applications",
    "incinerator_or_waste_applications",
    "jail_or_correction_applications",
    "utility_or_infrastructure_applications",
    "negative_amenity_applications",
    "shelter_or_institution_applications"
  )
) |>
  left_join(
    parsed_ulurp_number_base |>
      transmute(
        cert_year,
        all_ulurp_applications = TRUE,
        residential_strict_text_applications = residential_strict_text_flag,
        residential_broad_text_applications = residential_broad_text_flag,
        residential_candidate_applications = residential_candidate_flag,
        housing_production_conservative_applications = housing_production_conservative_flag,
        garage_or_parking_applications = garage_or_parking_text_flag,
        incinerator_or_waste_applications = incinerator_or_waste_text_flag,
        jail_or_correction_applications = jail_or_correction_text_flag,
        utility_or_infrastructure_applications = utility_or_infrastructure_text_flag,
        negative_amenity_applications = negative_amenity_text_flag,
        shelter_or_institution_applications = shelter_or_institution_text_flag
      ) |>
      pivot_longer(
        cols = c(
          all_ulurp_applications,
          residential_strict_text_applications,
          residential_broad_text_applications,
          residential_candidate_applications,
          housing_production_conservative_applications,
          garage_or_parking_applications,
          incinerator_or_waste_applications,
          jail_or_correction_applications,
          utility_or_infrastructure_applications,
          negative_amenity_applications,
          shelter_or_institution_applications
        ),
        names_to = "outcome_id",
        values_to = "included_flag"
      ) |>
      group_by(cert_year, outcome_id) |>
      summarize(application_count = sum(included_flag, na.rm = TRUE), .groups = "drop"),
    by = c("cert_year", "outcome_id"),
    relationship = "one-to-one"
  ) |>
  mutate(
    application_count = coalesce(application_count, 0L),
    count_unit = "parsed_ulurp_numbers"
  )

citywide_year_counts <- bind_rows(project_year_counts, number_year_counts) |>
  mutate(
    count_unit_label = case_when(
      count_unit == "zap_project_records" ~ "ZAP project records",
      count_unit == "parsed_ulurp_numbers" ~ "Parsed ULURP numbers",
      TRUE ~ count_unit
    ),
    outcome_label = case_when(
      outcome_id == "all_ulurp_applications" ~ "All ULURP applications",
      outcome_id == "residential_strict_text_applications" ~ "Residential applications: strict text",
      outcome_id == "residential_broad_text_applications" ~ "Residential applications: broad text",
      outcome_id == "residential_candidate_applications" ~ "Housing-oriented applications: candidate",
      outcome_id == "housing_production_conservative_applications" ~ "Housing-production applications: conservative",
      outcome_id == "garage_or_parking_applications" ~ "Garage/parking applications: text proxy",
      outcome_id == "incinerator_or_waste_applications" ~ "Waste/sanitation applications: text proxy",
      outcome_id == "jail_or_correction_applications" ~ "Jail/correction applications: text proxy",
      outcome_id == "utility_or_infrastructure_applications" ~ "Utility/infrastructure applications: text proxy",
      outcome_id == "negative_amenity_applications" ~ "Negative-amenity applications: text proxy",
      outcome_id == "shelter_or_institution_applications" ~ "Shelter/institution applications: text proxy",
      TRUE ~ outcome_id
    ),
    period = period_from_year(cert_year)
  ) |>
  group_by(count_unit, outcome_id) |>
  arrange(cert_year, .by_group = TRUE) |>
  mutate(application_count_ma3 = centered_ma3(application_count)) |>
  ungroup() |>
  arrange(count_unit, outcome_id, cert_year)

write_csv_if_changed(citywide_year_counts, "../output/citywide_ulurp_application_year.csv")

tercile_borough_grid <- expand_grid(
  cert_year = 1976:2025,
  outcome_id = c(
    "all_ulurp_applications",
    "residential_candidate_applications",
    "housing_production_conservative_applications",
    "garage_or_parking_applications",
    "incinerator_or_waste_applications",
    "jail_or_correction_applications",
    "utility_or_infrastructure_applications",
    "negative_amenity_applications",
    "shelter_or_institution_applications"
  ),
  district_lookup |>
    distinct(borough_code, borough_name, treat_tercile, treat_tercile_label) |>
    transmute(
      home_borough_code = borough_code,
      home_borough_name = borough_name,
      treat_tercile,
      treat_tercile_label
    )
)

project_tercile_borough_counts <- tercile_borough_grid |>
  left_join(
    ulurp_df |>
      filter(cert_year >= 1976, cert_year <= 2025, !is.na(treat_tercile), !is.na(home_borough_code)) |>
      transmute(
        cert_year,
        home_borough_code,
        home_borough_name,
        treat_tercile,
        treat_tercile_label,
        all_ulurp_applications = TRUE,
        residential_candidate_applications = residential_candidate_flag,
        housing_production_conservative_applications = housing_production_conservative_flag,
        garage_or_parking_applications = garage_or_parking_text_flag,
        incinerator_or_waste_applications = incinerator_or_waste_text_flag,
        jail_or_correction_applications = jail_or_correction_text_flag,
        utility_or_infrastructure_applications = utility_or_infrastructure_text_flag,
        negative_amenity_applications = negative_amenity_text_flag,
        shelter_or_institution_applications = shelter_or_institution_text_flag
      ) |>
      pivot_longer(
        cols = c(
          all_ulurp_applications,
          residential_candidate_applications,
          housing_production_conservative_applications,
          garage_or_parking_applications,
          incinerator_or_waste_applications,
          jail_or_correction_applications,
          utility_or_infrastructure_applications,
          negative_amenity_applications,
          shelter_or_institution_applications
        ),
        names_to = "outcome_id",
        values_to = "included_flag"
      ) |>
      group_by(cert_year, outcome_id, home_borough_code, home_borough_name, treat_tercile, treat_tercile_label) |>
      summarize(application_count = sum(included_flag, na.rm = TRUE), .groups = "drop"),
    by = c("cert_year", "outcome_id", "home_borough_code", "home_borough_name", "treat_tercile", "treat_tercile_label"),
    relationship = "one-to-one"
  ) |>
  mutate(
    application_count = coalesce(application_count, 0L),
    count_unit = "zap_project_records"
  )

number_tercile_borough_counts <- tercile_borough_grid |>
  left_join(
    parsed_ulurp_number_base |>
      filter(
        cert_year >= 1976,
        cert_year <= 2025,
        conflicting_cert_year_count == 1,
        nonmissing_primary_cd_count == 1,
        !is.na(treat_tercile),
        !is.na(home_borough_code)
      ) |>
      transmute(
        cert_year,
        home_borough_code,
        home_borough_name,
        treat_tercile,
        treat_tercile_label,
        all_ulurp_applications = TRUE,
        residential_candidate_applications = residential_candidate_flag,
        housing_production_conservative_applications = housing_production_conservative_flag,
        garage_or_parking_applications = garage_or_parking_text_flag,
        incinerator_or_waste_applications = incinerator_or_waste_text_flag,
        jail_or_correction_applications = jail_or_correction_text_flag,
        utility_or_infrastructure_applications = utility_or_infrastructure_text_flag,
        negative_amenity_applications = negative_amenity_text_flag,
        shelter_or_institution_applications = shelter_or_institution_text_flag
      ) |>
      pivot_longer(
        cols = c(
          all_ulurp_applications,
          residential_candidate_applications,
          housing_production_conservative_applications,
          garage_or_parking_applications,
          incinerator_or_waste_applications,
          jail_or_correction_applications,
          utility_or_infrastructure_applications,
          negative_amenity_applications,
          shelter_or_institution_applications
        ),
        names_to = "outcome_id",
        values_to = "included_flag"
      ) |>
      group_by(cert_year, outcome_id, home_borough_code, home_borough_name, treat_tercile, treat_tercile_label) |>
      summarize(application_count = sum(included_flag, na.rm = TRUE), .groups = "drop"),
    by = c("cert_year", "outcome_id", "home_borough_code", "home_borough_name", "treat_tercile", "treat_tercile_label"),
    relationship = "one-to-one"
  ) |>
  mutate(
    application_count = coalesce(application_count, 0L),
    count_unit = "parsed_ulurp_numbers"
  )

tercile_year_counts <- bind_rows(project_tercile_borough_counts, number_tercile_borough_counts) |>
  group_by(count_unit, outcome_id, cert_year, home_borough_code, home_borough_name) |>
  mutate(borough_application_total = sum(application_count, na.rm = TRUE)) |>
  ungroup() |>
  mutate(
    borough_tercile_share = if_else(
      borough_application_total > 0,
      application_count / borough_application_total,
      NA_real_
    )
  ) |>
  group_by(count_unit, outcome_id, cert_year, treat_tercile, treat_tercile_label) |>
  summarize(
    application_count = sum(application_count, na.rm = TRUE),
    borough_application_total = sum(borough_application_total, na.rm = TRUE),
    borough_application_share = if_else(borough_application_total > 0, application_count / borough_application_total, NA_real_),
    mean_borough_application_share = mean(borough_tercile_share, na.rm = TRUE),
    boroughs_with_positive_application_total = sum(borough_application_total > 0, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    count_unit_label = case_when(
      count_unit == "zap_project_records" ~ "ZAP project records",
      count_unit == "parsed_ulurp_numbers" ~ "Parsed ULURP numbers",
      TRUE ~ count_unit
    ),
    outcome_label = case_when(
      outcome_id == "all_ulurp_applications" ~ "All ULURP applications",
      outcome_id == "residential_candidate_applications" ~ "Housing-oriented applications: candidate",
      outcome_id == "housing_production_conservative_applications" ~ "Housing-production applications: conservative",
      outcome_id == "garage_or_parking_applications" ~ "Garage/parking applications: text proxy",
      outcome_id == "incinerator_or_waste_applications" ~ "Waste/sanitation applications: text proxy",
      outcome_id == "jail_or_correction_applications" ~ "Jail/correction applications: text proxy",
      outcome_id == "utility_or_infrastructure_applications" ~ "Utility/infrastructure applications: text proxy",
      outcome_id == "negative_amenity_applications" ~ "Negative-amenity applications: text proxy",
      outcome_id == "shelter_or_institution_applications" ~ "Shelter/institution applications: text proxy",
      TRUE ~ outcome_id
    ),
    period = period_from_year(cert_year)
  ) |>
  group_by(count_unit, outcome_id, treat_tercile, treat_tercile_label) |>
  arrange(cert_year, .by_group = TRUE) |>
  mutate(
    application_count_ma3 = centered_ma3(application_count),
    borough_application_share_ma3 = centered_ma3(borough_application_share),
    mean_borough_application_share_ma3 = centered_ma3(mean_borough_application_share)
  ) |>
  ungroup() |>
  arrange(count_unit, outcome_id, cert_year, treat_tercile)

write_csv_if_changed(tercile_year_counts, "../output/citywide_ulurp_application_tercile_year.csv")

demographic_outcome_ids <- c(
  "all_ulurp_applications",
  "housing_production_conservative_applications",
  "negative_amenity_applications",
  "shelter_or_institution_applications"
)

demographic_project_tercile_borough_counts <- list()
demographic_number_tercile_borough_counts <- list()

for (i in seq_len(nrow(demographic_stratifiers))) {
  stratifier_lookup <- district_lookup |>
    transmute(
      borocd,
      home_borough_code = borough_code,
      home_borough_name = borough_name,
      stratifier_id = demographic_stratifiers$stratifier_id[[i]],
      stratifier_label = demographic_stratifiers$stratifier_label[[i]],
      stratifier_tercile = .data[[demographic_stratifiers$tercile_col[[i]]]],
      stratifier_tercile_label = case_when(
        stratifier_tercile == 1 ~ "Low",
        stratifier_tercile == 2 ~ "Middle",
        stratifier_tercile == 3 ~ "High",
        TRUE ~ NA_character_
      )
    )

  if (nrow(stratifier_lookup) != n_distinct(stratifier_lookup$borocd)) {
    stop("Demographic stratifier lookup is not unique by borocd.")
  }

  stratifier_borough_grid <- expand_grid(
    cert_year = 1976:2025,
    outcome_id = demographic_outcome_ids,
    stratifier_lookup |>
      distinct(
        home_borough_code,
        home_borough_name,
        stratifier_id,
        stratifier_label,
        stratifier_tercile,
        stratifier_tercile_label
      )
  )

  demographic_project_tercile_borough_counts[[i]] <- stratifier_borough_grid |>
    left_join(
      ulurp_df |>
        filter(cert_year >= 1976, cert_year <= 2025, !is.na(home_borough_code)) |>
        left_join(
          stratifier_lookup |>
            select(borocd, stratifier_id, stratifier_label, stratifier_tercile, stratifier_tercile_label),
          by = "borocd",
          relationship = "many-to-one"
        ) |>
        filter(!is.na(stratifier_tercile)) |>
        transmute(
          cert_year,
          home_borough_code,
          home_borough_name,
          stratifier_id,
          stratifier_label,
          stratifier_tercile,
          stratifier_tercile_label,
          all_ulurp_applications = TRUE,
          housing_production_conservative_applications = housing_production_conservative_flag,
          negative_amenity_applications = negative_amenity_text_flag,
          shelter_or_institution_applications = shelter_or_institution_text_flag
        ) |>
        pivot_longer(
          cols = all_of(demographic_outcome_ids),
          names_to = "outcome_id",
          values_to = "included_flag"
        ) |>
        group_by(
          cert_year,
          outcome_id,
          home_borough_code,
          home_borough_name,
          stratifier_id,
          stratifier_label,
          stratifier_tercile,
          stratifier_tercile_label
        ) |>
        summarize(application_count = sum(included_flag, na.rm = TRUE), .groups = "drop"),
      by = c(
        "cert_year",
        "outcome_id",
        "home_borough_code",
        "home_borough_name",
        "stratifier_id",
        "stratifier_label",
        "stratifier_tercile",
        "stratifier_tercile_label"
      ),
      relationship = "one-to-one"
    ) |>
    mutate(
      application_count = coalesce(application_count, 0L),
      count_unit = "zap_project_records"
    )

  demographic_number_tercile_borough_counts[[i]] <- stratifier_borough_grid |>
    left_join(
      parsed_ulurp_number_base |>
        filter(
          cert_year >= 1976,
          cert_year <= 2025,
          conflicting_cert_year_count == 1,
          nonmissing_primary_cd_count == 1,
          !is.na(home_borough_code)
        ) |>
        left_join(
          stratifier_lookup |>
            transmute(
              assigned_borocd = borocd,
              stratifier_id,
              stratifier_label,
              stratifier_tercile,
              stratifier_tercile_label
            ),
          by = "assigned_borocd",
          relationship = "many-to-one"
        ) |>
        filter(!is.na(stratifier_tercile)) |>
        transmute(
          cert_year,
          home_borough_code,
          home_borough_name,
          stratifier_id,
          stratifier_label,
          stratifier_tercile,
          stratifier_tercile_label,
          all_ulurp_applications = TRUE,
          housing_production_conservative_applications = housing_production_conservative_flag,
          negative_amenity_applications = negative_amenity_text_flag,
          shelter_or_institution_applications = shelter_or_institution_text_flag
        ) |>
        pivot_longer(
          cols = all_of(demographic_outcome_ids),
          names_to = "outcome_id",
          values_to = "included_flag"
        ) |>
        group_by(
          cert_year,
          outcome_id,
          home_borough_code,
          home_borough_name,
          stratifier_id,
          stratifier_label,
          stratifier_tercile,
          stratifier_tercile_label
        ) |>
        summarize(application_count = sum(included_flag, na.rm = TRUE), .groups = "drop"),
      by = c(
        "cert_year",
        "outcome_id",
        "home_borough_code",
        "home_borough_name",
        "stratifier_id",
        "stratifier_label",
        "stratifier_tercile",
        "stratifier_tercile_label"
      ),
      relationship = "one-to-one"
    ) |>
    mutate(
      application_count = coalesce(application_count, 0L),
      count_unit = "parsed_ulurp_numbers"
    )
}

demographic_tercile_year_counts <- bind_rows(
  bind_rows(demographic_project_tercile_borough_counts),
  bind_rows(demographic_number_tercile_borough_counts)
) |>
  group_by(count_unit, outcome_id, cert_year, home_borough_code, home_borough_name, stratifier_id, stratifier_label) |>
  mutate(borough_application_total = sum(application_count, na.rm = TRUE)) |>
  ungroup() |>
  mutate(
    borough_tercile_share = if_else(
      borough_application_total > 0,
      application_count / borough_application_total,
      NA_real_
    )
  ) |>
  group_by(count_unit, outcome_id, cert_year, stratifier_id, stratifier_label, stratifier_tercile, stratifier_tercile_label) |>
  summarize(
    application_count = sum(application_count, na.rm = TRUE),
    borough_application_total = sum(borough_application_total, na.rm = TRUE),
    borough_application_share = if_else(borough_application_total > 0, application_count / borough_application_total, NA_real_),
    mean_borough_application_share = mean(borough_tercile_share, na.rm = TRUE),
    boroughs_with_positive_application_total = sum(borough_application_total > 0, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    count_unit_label = case_when(
      count_unit == "zap_project_records" ~ "ZAP project records",
      count_unit == "parsed_ulurp_numbers" ~ "Parsed ULURP numbers",
      TRUE ~ count_unit
    ),
    outcome_label = case_when(
      outcome_id == "all_ulurp_applications" ~ "All ULURP applications",
      outcome_id == "housing_production_conservative_applications" ~ "Housing-production applications: conservative",
      outcome_id == "negative_amenity_applications" ~ "Negative-amenity applications: text proxy",
      outcome_id == "shelter_or_institution_applications" ~ "Shelter/institution applications: text proxy",
      TRUE ~ outcome_id
    ),
    period = period_from_year(cert_year)
  ) |>
  group_by(count_unit, outcome_id, stratifier_id, stratifier_tercile, stratifier_tercile_label) |>
  arrange(cert_year, .by_group = TRUE) |>
  mutate(
    application_count_ma3 = centered_ma3(application_count),
    borough_application_share_ma3 = centered_ma3(borough_application_share),
    mean_borough_application_share_ma3 = centered_ma3(mean_borough_application_share)
  ) |>
  ungroup() |>
  arrange(count_unit, stratifier_id, outcome_id, cert_year, stratifier_tercile)

write_csv_if_changed(demographic_tercile_year_counts, "../output/citywide_ulurp_demographic_tercile_year.csv")

period_summary <- citywide_year_counts |>
  filter(!is.na(period)) |>
  group_by(count_unit, count_unit_label, outcome_id, outcome_label, period) |>
  summarize(
    years = n_distinct(cert_year),
    total_applications = sum(application_count, na.rm = TRUE),
    annual_mean = mean(application_count, na.rm = TRUE),
    annual_min = min(application_count, na.rm = TRUE),
    annual_max = max(application_count, na.rm = TRUE),
    .groups = "drop"
  ) |>
  group_by(count_unit, outcome_id) |>
  mutate(
    base_1985_1989_annual_mean = annual_mean[period == "1985-1989"][1],
    change_from_1985_1989 = annual_mean - base_1985_1989_annual_mean,
    pct_change_from_1985_1989 = 100 * (annual_mean / base_1985_1989_annual_mean - 1)
  ) |>
  ungroup() |>
  arrange(outcome_id, period)

write_csv_if_changed(period_summary, "../output/citywide_ulurp_application_period_summary.csv")

source_coverage <- project_df |>
  mutate(
    source_group = if_else(ulurp_flag, "ulurp_rows", "non_ulurp_rows"),
    reference_year = if_else(ulurp_flag, cert_year, project_reference_year)
  ) |>
  filter(!is.na(reference_year), reference_year >= 1976, reference_year <= 2025) |>
  group_by(source_group, reference_year) |>
  summarize(
    project_count = n(),
    ulurp_non_nonmissing_share = mean(nonempty_text(ulurp_non)),
    certified_referred_nonmissing_share = mean(!is.na(certified_referred_date_parsed)),
    app_filed_nonmissing_share = mean(!is.na(app_filed_date_parsed)),
    noticed_nonmissing_share = mean(!is.na(noticed_date_parsed)),
    approval_nonmissing_share = mean(!is.na(approval_date_parsed)),
    completed_nonmissing_share = mean(!is.na(completed_date_parsed)),
    project_name_nonmissing_share = mean(nonempty_text(project_name)),
    project_brief_nonmissing_share = mean(nonempty_text(project_brief)),
    actions_nonmissing_share = mean(nonempty_text(actions)),
    ulurp_numbers_nonmissing_share = mean(nonempty_text(ulurp_numbers)),
    .groups = "drop"
  ) |>
  arrange(source_group, reference_year)

write_csv_if_changed(source_coverage, "../output/citywide_ulurp_application_source_coverage.csv")

residential_examples <- ulurp_df |>
  filter(cert_year >= 1985, cert_year <= 1994) |>
  mutate(
    residential_definition = case_when(
      residential_strict_text_flag ~ "strict_text",
      residential_broad_text_flag ~ "broad_text_only",
      residential_candidate_flag ~ "candidate_only",
      TRUE ~ "not_residential_candidate"
    )
  ) |>
  group_by(residential_definition) |>
  arrange(cert_year, project_id, .by_group = TRUE) |>
  slice_head(n = 25) |>
  ungroup() |>
  select(
    residential_definition,
    project_id,
    cert_year,
    project_name,
    project_brief,
    primary_applicant,
    applicant_type,
    borough,
    community_district,
    actions,
    ulurp_numbers,
    housing_unit_text_flag,
    housing_production_conservative_flag,
    negative_amenity_text_flag,
    garage_or_parking_text_flag,
    incinerator_or_waste_text_flag,
    jail_or_correction_text_flag,
    utility_or_infrastructure_text_flag,
    shelter_or_institution_text_flag,
    residential_strict_text_flag,
    residential_broad_text_flag,
    mih_flag_bool,
    hpd_text_flag,
    hpd_housing_action_flag,
    residential_candidate_flag
  )

write_csv_if_changed(residential_examples, "../output/citywide_ulurp_residential_flag_examples.csv")

negative_amenity_summary <- ulurp_df |>
  filter(cert_year >= 1976, cert_year <= 2025) |>
  transmute(
    cert_period,
    garage_or_parking = garage_or_parking_text_flag,
    incinerator_or_waste = incinerator_or_waste_text_flag,
    jail_or_correction = jail_or_correction_text_flag,
    utility_or_infrastructure = utility_or_infrastructure_text_flag,
    shelter_or_institution = shelter_or_institution_text_flag,
    any_negative_amenity = negative_amenity_text_flag,
    residential_candidate_flag,
    housing_production_conservative_flag
  ) |>
  pivot_longer(
    cols = c(
      garage_or_parking,
      incinerator_or_waste,
      jail_or_correction,
      utility_or_infrastructure,
      shelter_or_institution,
      any_negative_amenity
    ),
    names_to = "flag",
    values_to = "flag_value"
  ) |>
  filter(flag_value, !is.na(cert_period)) |>
  group_by(cert_period, flag) |>
  summarize(
    project_records = n(),
    residential_candidate_records = sum(residential_candidate_flag, na.rm = TRUE),
    conservative_housing_records = sum(housing_production_conservative_flag, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(flag, cert_period)

write_csv_if_changed(negative_amenity_summary, "../output/citywide_ulurp_negative_amenity_summary.csv")

negative_amenity_examples <- ulurp_df |>
  filter(cert_year >= 1985, cert_year <= 1994) |>
  mutate(
    flag = case_when(
      shelter_or_institution_text_flag ~ "shelter_or_institution",
      incinerator_or_waste_text_flag ~ "incinerator_or_waste",
      garage_or_parking_text_flag ~ "garage_or_parking",
      jail_or_correction_text_flag ~ "jail_or_correction",
      utility_or_infrastructure_text_flag ~ "utility_or_infrastructure",
      TRUE ~ NA_character_
    )
  ) |>
  filter(!is.na(flag)) |>
  group_by(flag) |>
  arrange(cert_year, project_id, .by_group = TRUE) |>
  slice_head(n = 25) |>
  ungroup() |>
  select(
    flag,
    project_id,
    cert_year,
    project_name,
    project_brief,
    primary_applicant,
    applicant_type,
    borough,
    community_district,
    actions,
    ulurp_numbers,
    residential_candidate_flag,
    housing_unit_text_flag,
    housing_production_conservative_flag,
    negative_amenity_text_flag,
    shelter_or_institution_text_flag
  )

write_csv_if_changed(negative_amenity_examples, "../output/citywide_ulurp_negative_amenity_examples.csv")

manual_validation_labels <- read_csv("../input/citywide_ulurp_residential_manual_validation_labels.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(project_id = as.character(project_id))

manual_validation_duplicate_label_count <- nrow(manual_validation_labels) - n_distinct(manual_validation_labels$project_id)

if (manual_validation_duplicate_label_count > 0) {
  stop("Manual ULURP validation labels are not unique by project_id.")
}

manual_validation_sample <- manual_validation_labels |>
  left_join(
    ulurp_df |>
      transmute(
        project_id,
        cert_year,
        validation_period = case_when(
          cert_year >= 1985 & cert_year <= 1989 ~ "1985-1989",
          cert_year >= 1990 & cert_year <= 1994 ~ "1990-1994",
          TRUE ~ "outside_validation_window"
        ),
        validation_stratum = case_when(
          residential_strict_text_flag ~ "candidate_strict_text",
          residential_broad_text_flag ~ "candidate_broad_only",
          residential_candidate_flag ~ "candidate_code_or_hpd_only",
          TRUE ~ "not_candidate"
        ),
        project_name,
        project_brief,
        primary_applicant,
        applicant_type,
        borough,
        community_district,
        actions,
        ulurp_numbers,
        residential_candidate_flag,
        housing_unit_text_flag,
        housing_production_conservative_flag,
        negative_amenity_text_flag,
        shelter_or_institution_text_flag,
        residential_strict_text_flag,
        residential_broad_text_flag,
        mih_flag_bool,
        hpd_text_flag,
        hpd_housing_action_flag
      ),
    by = "project_id",
    relationship = "one-to-one"
  ) |>
  mutate(
    manual_confirmed_housing_units = as.logical(manual_confirmed_housing_units),
    manual_housing_related_inclusive = as.logical(manual_housing_related_inclusive),
    manual_possible_housing_related = as.logical(manual_possible_housing_related),
    proxy_status = if_else(residential_candidate_flag, "proxy_positive", "proxy_negative")
  ) |>
  arrange(validation_period, validation_stratum, cert_year, project_id)

manual_validation_unmatched_count <- sum(is.na(manual_validation_sample$cert_year))

if (manual_validation_unmatched_count > 0) {
  stop("Some manual validation labels do not match staged ULURP project rows.")
}

write_csv_if_changed(manual_validation_sample, "../output/citywide_ulurp_residential_manual_validation_sample.csv")

manual_validation_summary <- bind_rows(
  manual_validation_sample |>
    group_by(summary_level = "overall", validation_period = "all", validation_stratum = "all", proxy_status) |>
    summarize(
      sample_rows = n(),
      confirmed_housing_units = sum(manual_confirmed_housing_units, na.rm = TRUE),
      housing_related_inclusive = sum(manual_housing_related_inclusive, na.rm = TRUE),
      possible_housing_related = sum(manual_possible_housing_related, na.rm = TRUE),
      confirmed_housing_units_share = mean(manual_confirmed_housing_units, na.rm = TRUE),
      housing_related_inclusive_share = mean(manual_housing_related_inclusive, na.rm = TRUE),
      possible_housing_related_share = mean(manual_possible_housing_related, na.rm = TRUE),
      .groups = "drop"
    ),
  manual_validation_sample |>
    group_by(summary_level = "period", validation_period, validation_stratum = "all", proxy_status) |>
    summarize(
      sample_rows = n(),
      confirmed_housing_units = sum(manual_confirmed_housing_units, na.rm = TRUE),
      housing_related_inclusive = sum(manual_housing_related_inclusive, na.rm = TRUE),
      possible_housing_related = sum(manual_possible_housing_related, na.rm = TRUE),
      confirmed_housing_units_share = mean(manual_confirmed_housing_units, na.rm = TRUE),
      housing_related_inclusive_share = mean(manual_housing_related_inclusive, na.rm = TRUE),
      possible_housing_related_share = mean(manual_possible_housing_related, na.rm = TRUE),
      .groups = "drop"
    ),
  manual_validation_sample |>
    group_by(summary_level = "period_stratum", validation_period, validation_stratum, proxy_status) |>
    summarize(
      sample_rows = n(),
      confirmed_housing_units = sum(manual_confirmed_housing_units, na.rm = TRUE),
      housing_related_inclusive = sum(manual_housing_related_inclusive, na.rm = TRUE),
      possible_housing_related = sum(manual_possible_housing_related, na.rm = TRUE),
      confirmed_housing_units_share = mean(manual_confirmed_housing_units, na.rm = TRUE),
      housing_related_inclusive_share = mean(manual_housing_related_inclusive, na.rm = TRUE),
      possible_housing_related_share = mean(manual_possible_housing_related, na.rm = TRUE),
      .groups = "drop"
    )
) |>
  arrange(summary_level, validation_period, validation_stratum, proxy_status)

write_csv_if_changed(manual_validation_summary, "../output/citywide_ulurp_residential_manual_validation_summary.csv")

plot_number_main_df <- citywide_year_counts |>
  filter(
    count_unit == "parsed_ulurp_numbers",
    outcome_id %in% c(
      "all_ulurp_applications",
      "residential_candidate_applications"
    )
  ) |>
  mutate(outcome_label = factor(outcome_label, levels = c(
    "All ULURP applications",
    "Housing-oriented applications: candidate"
  )))

plot_project_main_df <- citywide_year_counts |>
  filter(
    count_unit == "zap_project_records",
    outcome_id %in% c(
      "all_ulurp_applications",
      "residential_candidate_applications"
    )
  ) |>
  mutate(outcome_label = factor(outcome_label, levels = c(
    "All ULURP applications",
    "Housing-oriented applications: candidate"
  )))

plot_tercile_number_df <- tercile_year_counts |>
  filter(
    count_unit == "parsed_ulurp_numbers",
    outcome_id %in% c(
      "all_ulurp_applications",
      "residential_candidate_applications",
      "housing_production_conservative_applications"
    )
  ) |>
  mutate(
    outcome_label = factor(outcome_label, levels = c(
      "All ULURP applications",
      "Housing-oriented applications: candidate",
      "Housing-production applications: conservative"
    )),
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High"))
  )

plot_tercile_project_df <- tercile_year_counts |>
  filter(
    count_unit == "zap_project_records",
    outcome_id %in% c(
      "all_ulurp_applications",
      "residential_candidate_applications",
      "housing_production_conservative_applications"
    )
  ) |>
  mutate(
    outcome_label = factor(outcome_label, levels = c(
      "All ULURP applications",
      "Housing-oriented applications: candidate",
      "Housing-production applications: conservative"
    )),
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High"))
  )

diagnostic_tercile_outcome_levels <- c(
  "Housing-production applications: conservative",
  "Negative-amenity applications: text proxy",
  "Shelter/institution applications: text proxy",
  "Garage/parking applications: text proxy",
  "Waste/sanitation applications: text proxy",
  "Jail/correction applications: text proxy",
  "Utility/infrastructure applications: text proxy"
)

plot_tercile_diagnostic_number_df <- tercile_year_counts |>
  filter(
    count_unit == "parsed_ulurp_numbers",
    outcome_id %in% c(
      "housing_production_conservative_applications",
      "negative_amenity_applications",
      "shelter_or_institution_applications",
      "garage_or_parking_applications",
      "incinerator_or_waste_applications",
      "jail_or_correction_applications",
      "utility_or_infrastructure_applications"
    )
  ) |>
  mutate(
    outcome_label = factor(outcome_label, levels = diagnostic_tercile_outcome_levels),
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High"))
  )

plot_tercile_diagnostic_project_df <- tercile_year_counts |>
  filter(
    count_unit == "zap_project_records",
    outcome_id %in% c(
      "housing_production_conservative_applications",
      "negative_amenity_applications",
      "shelter_or_institution_applications",
      "garage_or_parking_applications",
      "incinerator_or_waste_applications",
      "jail_or_correction_applications",
      "utility_or_infrastructure_applications"
    )
  ) |>
  mutate(
    outcome_label = factor(outcome_label, levels = diagnostic_tercile_outcome_levels),
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High"))
  )

plot_tercile_diagnostic_period_df <- bind_rows(
  plot_tercile_diagnostic_number_df,
  plot_tercile_diagnostic_project_df
) |>
  filter(!is.na(period)) |>
  group_by(count_unit, count_unit_label, outcome_id, outcome_label, period, treat_tercile_label) |>
  summarize(
    annual_mean_applications = mean(application_count, na.rm = TRUE),
    years = n_distinct(cert_year),
    .groups = "drop"
  ) |>
  mutate(
    period = factor(
      period,
      levels = c(
        "1976-1979",
        "1980-1984",
        "1985-1989",
        "1990-1994",
        "1995-1999",
        "2000-2004",
        "2005-2009",
        "2010-2014",
        "2015-2019",
        "2020-2025"
      )
    ),
    outcome_label = factor(outcome_label, levels = diagnostic_tercile_outcome_levels),
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High"))
  )

demographic_outcome_levels <- c(
  "All ULURP applications",
  "Housing-production applications: conservative",
  "Negative-amenity applications: text proxy",
  "Shelter/institution applications: text proxy"
)

demographic_stratifier_levels <- demographic_stratifiers$stratifier_label

plot_demographic_number_df <- demographic_tercile_year_counts |>
  filter(count_unit == "parsed_ulurp_numbers") |>
  mutate(
    outcome_label = factor(outcome_label, levels = demographic_outcome_levels),
    stratifier_label = factor(stratifier_label, levels = demographic_stratifier_levels),
    stratifier_tercile_label = factor(stratifier_tercile_label, levels = c("Low", "Middle", "High"))
  )

plot_demographic_project_df <- demographic_tercile_year_counts |>
  filter(count_unit == "zap_project_records") |>
  mutate(
    outcome_label = factor(outcome_label, levels = demographic_outcome_levels),
    stratifier_label = factor(stratifier_label, levels = demographic_stratifier_levels),
    stratifier_tercile_label = factor(stratifier_tercile_label, levels = c("Low", "Middle", "High"))
  )

plot_demographic_period_df <- bind_rows(
  plot_demographic_number_df,
  plot_demographic_project_df
) |>
  filter(!is.na(period)) |>
  group_by(count_unit, count_unit_label, outcome_id, outcome_label, period, stratifier_id, stratifier_label, stratifier_tercile_label) |>
  summarize(
    annual_mean_applications = mean(application_count, na.rm = TRUE),
    years = n_distinct(cert_year),
    .groups = "drop"
  ) |>
  mutate(
    period = factor(
      period,
      levels = c(
        "1976-1979",
        "1980-1984",
        "1985-1989",
        "1990-1994",
        "1995-1999",
        "2000-2004",
        "2005-2009",
        "2010-2014",
        "2015-2019",
        "2020-2025"
      )
    ),
    outcome_label = factor(outcome_label, levels = demographic_outcome_levels),
    stratifier_label = factor(stratifier_label, levels = demographic_stratifier_levels),
    stratifier_tercile_label = factor(stratifier_tercile_label, levels = c("Low", "Middle", "High"))
  )

tercile_colors <- c("Low" = "#2f64b1", "Middle" = "#7f7f7f", "High" = "#d94832")

pdf("../output/citywide_ulurp_application_tercile_trends.pdf", width = 11, height = 8.5)
print(
  ggplot(plot_tercile_number_df, aes(x = cert_year, y = application_count, color = treat_tercile_label)) +
    geom_vline(xintercept = 1989.5, color = "#666666", linetype = "dashed", linewidth = 0.35) +
    geom_line(alpha = 0.35, linewidth = 0.45, na.rm = TRUE) +
    geom_line(aes(y = application_count_ma3), linewidth = 0.9, na.rm = TRUE) +
    facet_wrap(~outcome_label, ncol = 1, scales = "free_y") +
    scale_color_manual(values = tercile_colors) +
    scale_x_continuous(breaks = seq(1975, 2025, by = 5)) +
    labs(
      title = "ULURP application numbers by 1990 homeownership tercile",
      subtitle = "Primary ZAP community district joined to within-borough 1990 homeownership terciles. Thick lines are centered three-year moving averages.",
      x = NULL,
      y = "Applications",
      color = "Homeownership tercile"
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(plot_tercile_number_df, aes(x = cert_year, y = borough_application_share, color = treat_tercile_label)) +
    geom_vline(xintercept = 1989.5, color = "#666666", linetype = "dashed", linewidth = 0.35) +
    geom_line(alpha = 0.35, linewidth = 0.45, na.rm = TRUE) +
    geom_line(aes(y = borough_application_share_ma3), linewidth = 0.9, na.rm = TRUE) +
    facet_wrap(~outcome_label, ncol = 1) +
    scale_color_manual(values = tercile_colors) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
    scale_x_continuous(breaks = seq(1975, 2025, by = 5)) +
    labs(
      title = "Pooled within-borough ULURP application-number shares by homeownership tercile",
      subtitle = "Application-weighted shares pool borough-year totals; unweighted borough means are included in the CSV.",
      x = NULL,
      y = "Pooled within-borough share",
      color = "Homeownership tercile"
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(plot_tercile_project_df, aes(x = cert_year, y = application_count, color = treat_tercile_label)) +
    geom_vline(xintercept = 1989.5, color = "#666666", linetype = "dashed", linewidth = 0.35) +
    geom_line(alpha = 0.35, linewidth = 0.45, na.rm = TRUE) +
    geom_line(aes(y = application_count_ma3), linewidth = 0.9, na.rm = TRUE) +
    facet_wrap(~outcome_label, ncol = 1, scales = "free_y") +
    scale_color_manual(values = tercile_colors) +
    scale_x_continuous(breaks = seq(1975, 2025, by = 5)) +
    labs(
      title = "ZAP ULURP project records by 1990 homeownership tercile",
      subtitle = "Project records are more complete historically but can bundle multiple ULURP application numbers.",
      x = NULL,
      y = "ZAP project records",
      color = "Homeownership tercile"
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(plot_tercile_project_df, aes(x = cert_year, y = borough_application_share, color = treat_tercile_label)) +
    geom_vline(xintercept = 1989.5, color = "#666666", linetype = "dashed", linewidth = 0.35) +
    geom_line(alpha = 0.35, linewidth = 0.45, na.rm = TRUE) +
    geom_line(aes(y = borough_application_share_ma3), linewidth = 0.9, na.rm = TRUE) +
    facet_wrap(~outcome_label, ncol = 1) +
    scale_color_manual(values = tercile_colors) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
    scale_x_continuous(breaks = seq(1975, 2025, by = 5)) +
    labs(
      title = "Pooled within-borough ZAP project-record shares by homeownership tercile",
      subtitle = "Application-weighted shares pool borough-year totals; unweighted borough means are included in the CSV.",
      x = NULL,
      y = "Pooled within-borough share",
      color = "Homeownership tercile"
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
dev.off()

pdf("../output/citywide_ulurp_housing_facility_tercile_trends.pdf", width = 11, height = 8.5)
print(
  ggplot(plot_tercile_diagnostic_number_df, aes(x = cert_year, y = application_count, color = treat_tercile_label)) +
    geom_vline(xintercept = 1989.5, color = "#666666", linetype = "dashed", linewidth = 0.35) +
    geom_line(alpha = 0.35, linewidth = 0.45, na.rm = TRUE) +
    geom_line(aes(y = application_count_ma3), linewidth = 0.9, na.rm = TRUE) +
    facet_wrap(~outcome_label, ncol = 2, scales = "free_y") +
    scale_color_manual(values = tercile_colors) +
    scale_x_continuous(breaks = seq(1975, 2025, by = 5)) +
    labs(
      title = "ULURP housing-production and facility application numbers by homeownership tercile",
      subtitle = "Primary ZAP community district joined to within-borough 1990 homeownership terciles. Thick lines are centered three-year moving averages.",
      x = NULL,
      y = "Applications",
      color = "Homeownership tercile"
    ) +
    theme_minimal(base_size = 10) +
    theme(legend.position = "bottom")
)
print(
  ggplot(
    plot_tercile_diagnostic_period_df |> filter(count_unit == "parsed_ulurp_numbers"),
    aes(x = period, y = annual_mean_applications, color = treat_tercile_label, group = treat_tercile_label)
  ) +
    geom_vline(xintercept = 3.5, color = "#666666", linetype = "dashed", linewidth = 0.35) +
    geom_line(linewidth = 0.75) +
    geom_point(size = 1.4) +
    facet_wrap(~outcome_label, ncol = 2, scales = "free_y") +
    scale_color_manual(values = tercile_colors) +
    labs(
      title = "Five-year average ULURP housing-production and facility application numbers",
      subtitle = "Period averages stabilize sparse categories such as jail, waste, utility, and shelter applications.",
      x = NULL,
      y = "Mean annual applications",
      color = "Homeownership tercile"
    ) +
    theme_minimal(base_size = 10) +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "bottom"
    )
)
print(
  ggplot(plot_tercile_diagnostic_project_df, aes(x = cert_year, y = application_count, color = treat_tercile_label)) +
    geom_vline(xintercept = 1989.5, color = "#666666", linetype = "dashed", linewidth = 0.35) +
    geom_line(alpha = 0.35, linewidth = 0.45, na.rm = TRUE) +
    geom_line(aes(y = application_count_ma3), linewidth = 0.9, na.rm = TRUE) +
    facet_wrap(~outcome_label, ncol = 2, scales = "free_y") +
    scale_color_manual(values = tercile_colors) +
    scale_x_continuous(breaks = seq(1975, 2025, by = 5)) +
    labs(
      title = "ZAP housing-production and facility project records by homeownership tercile",
      subtitle = "Project records are more complete historically but can bundle multiple ULURP application numbers.",
      x = NULL,
      y = "ZAP project records",
      color = "Homeownership tercile"
    ) +
    theme_minimal(base_size = 10) +
    theme(legend.position = "bottom")
)
print(
  ggplot(
    plot_tercile_diagnostic_period_df |> filter(count_unit == "zap_project_records"),
    aes(x = period, y = annual_mean_applications, color = treat_tercile_label, group = treat_tercile_label)
  ) +
    geom_vline(xintercept = 3.5, color = "#666666", linetype = "dashed", linewidth = 0.35) +
    geom_line(linewidth = 0.75) +
    geom_point(size = 1.4) +
    facet_wrap(~outcome_label, ncol = 2, scales = "free_y") +
    scale_color_manual(values = tercile_colors) +
    labs(
      title = "Five-year average ZAP housing-production and facility project records",
      subtitle = "Project records are more complete historically but can bundle multiple ULURP application numbers.",
      x = NULL,
      y = "Mean annual ZAP project records",
      color = "Homeownership tercile"
    ) +
    theme_minimal(base_size = 10) +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "bottom"
    )
)
dev.off()

pdf("../output/citywide_ulurp_demographic_tercile_trends.pdf", width = 16, height = 9)
print(
  ggplot(plot_demographic_number_df, aes(x = cert_year, y = application_count, color = stratifier_tercile_label)) +
    geom_vline(xintercept = 1989.5, color = "#666666", linetype = "dashed", linewidth = 0.35) +
    geom_line(alpha = 0.25, linewidth = 0.35, na.rm = TRUE) +
    geom_line(aes(y = application_count_ma3), linewidth = 0.75, na.rm = TRUE) +
    facet_grid(outcome_label ~ stratifier_label, scales = "free_y") +
    scale_color_manual(values = tercile_colors) +
    scale_x_continuous(breaks = seq(1975, 2025, by = 10)) +
    labs(
      title = "ULURP application numbers by 1990 demographic tercile",
      subtitle = "Primary ZAP community district joined to within-borough 1990 income, poverty, and race-share terciles. Thick lines are centered three-year moving averages.",
      x = NULL,
      y = "Applications",
      color = "Within-borough tercile"
    ) +
    theme_minimal(base_size = 8) +
    theme(
      legend.position = "bottom",
      strip.text.x = element_text(size = 7),
      strip.text.y = element_text(size = 7),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )
)
print(
  ggplot(
    plot_demographic_period_df |> filter(count_unit == "parsed_ulurp_numbers"),
    aes(x = period, y = annual_mean_applications, color = stratifier_tercile_label, group = stratifier_tercile_label)
  ) +
    geom_vline(xintercept = 3.5, color = "#666666", linetype = "dashed", linewidth = 0.35) +
    geom_line(linewidth = 0.75) +
    geom_point(size = 1.2) +
    facet_grid(outcome_label ~ stratifier_label, scales = "free_y") +
    scale_color_manual(values = tercile_colors) +
    labs(
      title = "Five-year average ULURP application numbers by 1990 demographic tercile",
      subtitle = "Period averages stabilize sparse categories. Terciles are calculated within borough.",
      x = NULL,
      y = "Mean annual applications",
      color = "Within-borough tercile"
    ) +
    theme_minimal(base_size = 8) +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "bottom",
      strip.text.x = element_text(size = 7),
      strip.text.y = element_text(size = 7)
    )
)
print(
  ggplot(plot_demographic_project_df, aes(x = cert_year, y = application_count, color = stratifier_tercile_label)) +
    geom_vline(xintercept = 1989.5, color = "#666666", linetype = "dashed", linewidth = 0.35) +
    geom_line(alpha = 0.25, linewidth = 0.35, na.rm = TRUE) +
    geom_line(aes(y = application_count_ma3), linewidth = 0.75, na.rm = TRUE) +
    facet_grid(outcome_label ~ stratifier_label, scales = "free_y") +
    scale_color_manual(values = tercile_colors) +
    scale_x_continuous(breaks = seq(1975, 2025, by = 10)) +
    labs(
      title = "ZAP project records by 1990 demographic tercile",
      subtitle = "Project records are more complete historically but can bundle multiple ULURP application numbers.",
      x = NULL,
      y = "ZAP project records",
      color = "Within-borough tercile"
    ) +
    theme_minimal(base_size = 8) +
    theme(
      legend.position = "bottom",
      strip.text.x = element_text(size = 7),
      strip.text.y = element_text(size = 7),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )
)
print(
  ggplot(
    plot_demographic_period_df |> filter(count_unit == "zap_project_records"),
    aes(x = period, y = annual_mean_applications, color = stratifier_tercile_label, group = stratifier_tercile_label)
  ) +
    geom_vline(xintercept = 3.5, color = "#666666", linetype = "dashed", linewidth = 0.35) +
    geom_line(linewidth = 0.75) +
    geom_point(size = 1.2) +
    facet_grid(outcome_label ~ stratifier_label, scales = "free_y") +
    scale_color_manual(values = tercile_colors) +
    labs(
      title = "Five-year average ZAP project records by 1990 demographic tercile",
      subtitle = "Project records are more complete historically but can bundle multiple ULURP application numbers.",
      x = NULL,
      y = "Mean annual ZAP project records",
      color = "Within-borough tercile"
    ) +
    theme_minimal(base_size = 8) +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "bottom",
      strip.text.x = element_text(size = 7),
      strip.text.y = element_text(size = 7)
    )
)
dev.off()

pdf("../output/citywide_ulurp_application_trends.pdf", width = 11, height = 8.5)
print(
  ggplot(plot_number_main_df, aes(x = cert_year, y = application_count, color = outcome_label)) +
    geom_vline(xintercept = 1989.5, color = "#666666", linetype = "dashed", linewidth = 0.35) +
    geom_line(alpha = 0.35, linewidth = 0.45) +
    geom_line(aes(y = application_count_ma3), linewidth = 0.9, na.rm = TRUE) +
    scale_color_manual(values = c(
      "All ULURP applications" = "#333333",
      "Housing-oriented applications: candidate" = "#2f7d32"
    )) +
    scale_x_continuous(breaks = seq(1975, 2025, by = 5)) +
    labs(
      title = "Citywide ULURP application numbers by certification/referral year",
      subtitle = "Counts parse distinct ULURP numbers from ZAP project rows. Thin lines are annual counts; thick lines are centered three-year moving averages.",
      x = NULL,
      y = "Applications",
      color = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(plot_project_main_df, aes(x = cert_year, y = application_count, color = outcome_label)) +
    geom_vline(xintercept = 1989.5, color = "#666666", linetype = "dashed", linewidth = 0.35) +
    geom_line(alpha = 0.35, linewidth = 0.45) +
    geom_line(aes(y = application_count_ma3), linewidth = 0.9, na.rm = TRUE) +
    scale_color_manual(values = c(
      "All ULURP applications" = "#333333",
      "Housing-oriented applications: candidate" = "#2f7d32"
    )) +
    scale_x_continuous(breaks = seq(1975, 2025, by = 5)) +
    labs(
      title = "Citywide ZAP ULURP project records by certification/referral year",
      subtitle = "Project records are more complete historically but can bundle multiple ULURP application numbers.",
      x = NULL,
      y = "ZAP project records",
      color = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
print(
  ggplot(source_coverage |> filter(source_group == "ulurp_rows"), aes(x = reference_year)) +
    geom_vline(xintercept = 1989.5, color = "#666666", linetype = "dashed", linewidth = 0.35) +
    geom_line(aes(y = certified_referred_nonmissing_share, color = "Certified/referred date"), linewidth = 0.75) +
    geom_line(aes(y = app_filed_nonmissing_share, color = "Application filed date"), linewidth = 0.75) +
    geom_line(aes(y = noticed_nonmissing_share, color = "Noticed date"), linewidth = 0.75) +
    geom_line(aes(y = project_brief_nonmissing_share, color = "Project brief"), linewidth = 0.75) +
    geom_line(aes(y = ulurp_numbers_nonmissing_share, color = "ULURP numbers"), linewidth = 0.75) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 1), limits = c(0, 1)) +
    scale_x_continuous(breaks = seq(1975, 2025, by = 5)) +
    labs(
      title = "ZAP source-field coverage for ULURP rows",
      x = NULL,
      y = "Nonmissing share",
      color = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
dev.off()

expected_output_paths <- c(
  "../output/citywide_ulurp_application_year.csv",
  "../output/citywide_ulurp_application_tercile_year.csv",
  "../output/citywide_ulurp_demographic_tercile_year.csv",
  "../output/citywide_ulurp_application_period_summary.csv",
  "../output/citywide_ulurp_application_source_coverage.csv",
  "../output/citywide_ulurp_residential_flag_examples.csv",
  "../output/citywide_ulurp_negative_amenity_summary.csv",
  "../output/citywide_ulurp_negative_amenity_examples.csv",
  "../output/citywide_ulurp_residential_manual_validation_sample.csv",
  "../output/citywide_ulurp_residential_manual_validation_summary.csv",
  "../output/citywide_ulurp_application_tercile_trends.pdf",
  "../output/citywide_ulurp_housing_facility_tercile_trends.pdf",
  "../output/citywide_ulurp_demographic_tercile_trends.pdf",
  "../output/citywide_ulurp_application_trends.pdf"
)

main_window <- ulurp_df |>
  filter(cert_year >= 1976, cert_year <= 2025)

missing_year_count <- sum(ulurp_df$ulurp_flag & is.na(ulurp_df$cert_year), na.rm = TRUE)
outside_plot_window_count <- sum(!is.na(ulurp_df$cert_year) & (ulurp_df$cert_year < 1976 | ulurp_df$cert_year > 2025), na.rm = TRUE)
duplicate_project_id_count <- nrow(project_df) - n_distinct(project_df$project_id)
duplicate_parsed_ulurp_number_count <- nrow(parsed_ulurp_number_project) - n_distinct(parsed_ulurp_number_project$ulurp_application_number)
conflicting_ulurp_number_year_count <- sum(parsed_ulurp_number_base$conflicting_cert_year_count > 1, na.rm = TRUE)
negative_count_cells <- sum(citywide_year_counts$application_count < 0, na.rm = TRUE)
negative_tercile_count_cells <- sum(tercile_year_counts$application_count < 0, na.rm = TRUE)
negative_demographic_tercile_count_cells <- sum(demographic_tercile_year_counts$application_count < 0, na.rm = TRUE)
year_outcome_duplicate_count <- nrow(citywide_year_counts) - nrow(distinct(citywide_year_counts, count_unit, cert_year, outcome_id))
tercile_year_duplicate_count <- nrow(tercile_year_counts) - nrow(distinct(tercile_year_counts, count_unit, cert_year, outcome_id, treat_tercile))
tercile_year_gap_count <- tercile_year_counts |>
  count(count_unit, outcome_id, cert_year, name = "tercile_rows") |>
  filter(tercile_rows != 3) |>
  nrow()
demographic_tercile_year_duplicate_count <- nrow(demographic_tercile_year_counts) -
  nrow(distinct(demographic_tercile_year_counts, count_unit, cert_year, outcome_id, stratifier_id, stratifier_tercile))
demographic_tercile_year_gap_count <- demographic_tercile_year_counts |>
  count(count_unit, outcome_id, cert_year, stratifier_id, name = "tercile_rows") |>
  filter(tercile_rows != 3) |>
  nrow()
demographic_missing_tercile_count <- district_lookup |>
  summarize(
    value = sum(is.na(income_tercile)) +
      sum(is.na(poverty_tercile)) +
      sum(is.na(white_share_tercile)) +
      sum(is.na(black_share_tercile)) +
      sum(is.na(hispanic_share_tercile))
  ) |>
  pull(value)
ulurp_project_with_tercile_share <- ulurp_df |>
  filter(cert_year >= 1976, cert_year <= 2025) |>
  summarize(value = mean(!is.na(treat_tercile))) |>
  pull(value)
ulurp_project_multi_cd_count <- ulurp_df |>
  filter(cert_year >= 1976, cert_year <= 2025) |>
  summarize(value = sum(community_district_multi_flag, na.rm = TRUE)) |>
  pull(value)
parsed_ulurp_number_single_cd_share <- parsed_ulurp_number_base |>
  summarize(value = mean(nonmissing_primary_cd_count == 1)) |>
  pull(value)
parsed_ulurp_number_no_cd_count <- sum(parsed_ulurp_number_base$nonmissing_primary_cd_count == 0, na.rm = TRUE)
parsed_ulurp_number_multiple_cd_count <- sum(parsed_ulurp_number_base$nonmissing_primary_cd_count > 1, na.rm = TRUE)
pre_1985_1989_total_mean <- period_summary |>
  filter(count_unit == "parsed_ulurp_numbers", outcome_id == "all_ulurp_applications", period == "1985-1989") |>
  pull(annual_mean)
post_1990_1994_total_mean <- period_summary |>
  filter(count_unit == "parsed_ulurp_numbers", outcome_id == "all_ulurp_applications", period == "1990-1994") |>
  pull(annual_mean)
pre_1985_1989_residential_mean <- period_summary |>
  filter(count_unit == "parsed_ulurp_numbers", outcome_id == "residential_candidate_applications", period == "1985-1989") |>
  pull(annual_mean)
post_1990_1994_residential_mean <- period_summary |>
  filter(count_unit == "parsed_ulurp_numbers", outcome_id == "residential_candidate_applications", period == "1990-1994") |>
  pull(annual_mean)
pre_1985_1989_ulurp_numbers_nonmissing_share <- ulurp_df |>
  filter(cert_year >= 1985, cert_year <= 1989) |>
  summarize(value = mean(nonempty_text(ulurp_numbers))) |>
  pull(value)
post_1990_1994_ulurp_numbers_nonmissing_share <- ulurp_df |>
  filter(cert_year >= 1990, cert_year <= 1994) |>
  summarize(value = mean(nonempty_text(ulurp_numbers))) |>
  pull(value)
output_nonempty_count <- sum(file.exists(expected_output_paths) & file.info(expected_output_paths)$size > 0)

qc_df <- bind_rows(
  tibble(metric = "staged_project_count", value = as.character(nrow(project_df)), status = "pass", note = "Rows in staged ZAP project file."),
  tibble(metric = "duplicate_project_id_count", value = as.character(duplicate_project_id_count), status = if_else(duplicate_project_id_count == 0, "pass", "fail"), note = "Staged ZAP project IDs should be unique."),
  tibble(metric = "ulurp_project_count", value = as.character(nrow(ulurp_df)), status = if_else(nrow(ulurp_df) > 0, "pass", "fail"), note = "Rows flagged as ULURP."),
  tibble(metric = "ulurp_certification_window_count", value = as.character(nrow(main_window)), status = if_else(nrow(main_window) > 0, "pass", "fail"), note = "ULURP rows with certification/referral years in 1976-2025."),
  tibble(metric = "parsed_ulurp_number_count", value = as.character(nrow(parsed_ulurp_number_base)), status = if_else(nrow(parsed_ulurp_number_base) > 0, "pass", "fail"), note = "Distinct parsed ULURP application numbers in 1976-2025."),
  tibble(metric = "duplicate_parsed_ulurp_number_count", value = as.character(duplicate_parsed_ulurp_number_count), status = "pass", note = "Repeated parsed application numbers across project rows before collapsing to one row per number."),
  tibble(metric = "conflicting_ulurp_number_year_count", value = as.character(conflicting_ulurp_number_year_count), status = if_else(conflicting_ulurp_number_year_count == 0, "pass", "warning"), note = "Parsed application numbers attached to more than one certification/referral year."),
  tibble(metric = "ulurp_missing_cert_year_count", value = as.character(missing_year_count), status = "pass", note = "ULURP rows excluded from main trend because certification/referral year is missing."),
  tibble(metric = "ulurp_cert_year_outside_1976_2025_count", value = as.character(outside_plot_window_count), status = "pass", note = "ULURP rows outside the full-year plot window; 2026 is excluded as partial."),
  tibble(metric = "year_min", value = as.character(min(citywide_year_counts$cert_year, na.rm = TRUE)), status = if_else(min(citywide_year_counts$cert_year, na.rm = TRUE) == 1976, "pass", "fail"), note = "Minimum plotted certification/referral year."),
  tibble(metric = "year_max", value = as.character(max(citywide_year_counts$cert_year, na.rm = TRUE)), status = if_else(max(citywide_year_counts$cert_year, na.rm = TRUE) == 2025, "pass", "fail"), note = "Maximum plotted certification/referral year."),
  tibble(metric = "year_outcome_duplicate_count", value = as.character(year_outcome_duplicate_count), status = if_else(year_outcome_duplicate_count == 0, "pass", "fail"), note = "Citywide year-outcome rows should be unique."),
  tibble(metric = "tercile_year_duplicate_count", value = as.character(tercile_year_duplicate_count), status = if_else(tercile_year_duplicate_count == 0, "pass", "fail"), note = "Tercile year-outcome rows should be unique."),
  tibble(metric = "tercile_year_gap_count", value = as.character(tercile_year_gap_count), status = if_else(tercile_year_gap_count == 0, "pass", "fail"), note = "Each count-unit, outcome, and year should have exactly three treatment tercile rows."),
  tibble(metric = "demographic_tercile_year_duplicate_count", value = as.character(demographic_tercile_year_duplicate_count), status = if_else(demographic_tercile_year_duplicate_count == 0, "pass", "fail"), note = "Demographic-tercile year-outcome rows should be unique."),
  tibble(metric = "demographic_tercile_year_gap_count", value = as.character(demographic_tercile_year_gap_count), status = if_else(demographic_tercile_year_gap_count == 0, "pass", "fail"), note = "Each count-unit, outcome, year, and demographic stratifier should have exactly three tercile rows."),
  tibble(metric = "demographic_missing_tercile_count", value = as.character(demographic_missing_tercile_count), status = if_else(demographic_missing_tercile_count == 0, "pass", "fail"), note = "All CDs should have nonmissing income, poverty, and race-share terciles."),
  tibble(metric = "negative_count_cells", value = as.character(negative_count_cells), status = if_else(negative_count_cells == 0, "pass", "fail"), note = "Application counts should not be negative."),
  tibble(metric = "negative_tercile_count_cells", value = as.character(negative_tercile_count_cells), status = if_else(negative_tercile_count_cells == 0, "pass", "fail"), note = "Tercile application counts should not be negative."),
  tibble(metric = "negative_demographic_tercile_count_cells", value = as.character(negative_demographic_tercile_count_cells), status = if_else(negative_demographic_tercile_count_cells == 0, "pass", "fail"), note = "Demographic-tercile application counts should not be negative."),
  tibble(metric = "manual_validation_sample_count", value = as.character(nrow(manual_validation_sample)), status = if_else(nrow(manual_validation_sample) > 0, "pass", "fail"), note = "Rows in hand-coded housing-oriented validation sample."),
  tibble(metric = "manual_validation_duplicate_label_count", value = as.character(manual_validation_duplicate_label_count), status = if_else(manual_validation_duplicate_label_count == 0, "pass", "fail"), note = "Manual validation labels should be unique by project_id."),
  tibble(metric = "manual_validation_unmatched_count", value = as.character(manual_validation_unmatched_count), status = if_else(manual_validation_unmatched_count == 0, "pass", "fail"), note = "Manual validation labels should match staged ULURP project rows."),
  tibble(metric = "homeownership_district_count", value = as.character(n_distinct(district_lookup$borocd)), status = if_else(n_distinct(district_lookup$borocd) == 59, "pass", "fail"), note = "CDs with 1990 homeownership treatment tercile assignments."),
  tibble(metric = "ulurp_project_with_tercile_share", value = formatC(ulurp_project_with_tercile_share, format = "f", digits = 3), status = if_else(ulurp_project_with_tercile_share >= 0.85, "pass", "warning"), note = "Share of 1976-2025 ULURP project records assigned to a homeownership tercile through primary ZAP CD."),
  tibble(metric = "ulurp_project_multi_cd_count", value = as.character(ulurp_project_multi_cd_count), status = "pass", note = "ULURP project records with multi-CD text; these are assigned by the staged primary CD for this descriptive split."),
  tibble(metric = "parsed_ulurp_number_single_cd_share", value = formatC(parsed_ulurp_number_single_cd_share, format = "f", digits = 3), status = if_else(parsed_ulurp_number_single_cd_share >= 0.85, "pass", "warning"), note = "Share of parsed ULURP application numbers with exactly one primary CD among source project rows."),
  tibble(metric = "parsed_ulurp_number_no_cd_count", value = as.character(parsed_ulurp_number_no_cd_count), status = "pass", note = "Parsed ULURP application numbers omitted from tercile plots because no source project has a usable primary CD."),
  tibble(metric = "parsed_ulurp_number_multiple_cd_count", value = as.character(parsed_ulurp_number_multiple_cd_count), status = "pass", note = "Parsed ULURP application numbers omitted from tercile plots because source project rows point to multiple primary CDs."),
  tibble(metric = "pre_1985_1989_ulurp_numbers_nonmissing_share", value = formatC(pre_1985_1989_ulurp_numbers_nonmissing_share, format = "f", digits = 3), status = if_else(pre_1985_1989_ulurp_numbers_nonmissing_share >= 0.75, "pass", "warning"), note = "ULURP-number field support for certification years 1985-1989."),
  tibble(metric = "post_1990_1994_ulurp_numbers_nonmissing_share", value = formatC(post_1990_1994_ulurp_numbers_nonmissing_share, format = "f", digits = 3), status = if_else(post_1990_1994_ulurp_numbers_nonmissing_share >= 0.75, "pass", "warning"), note = "ULURP-number field support for certification years 1990-1994."),
  tibble(metric = "pre_1985_1989_total_annual_mean", value = formatC(pre_1985_1989_total_mean, format = "f", digits = 1), status = "pass", note = "Mean annual parsed all-ULURP application numbers in 1985-1989."),
  tibble(metric = "post_1990_1994_total_annual_mean", value = formatC(post_1990_1994_total_mean, format = "f", digits = 1), status = "pass", note = "Mean annual parsed all-ULURP application numbers in 1990-1994."),
  tibble(metric = "pre_1985_1989_residential_candidate_annual_mean", value = formatC(pre_1985_1989_residential_mean, format = "f", digits = 1), status = "pass", note = "Mean annual parsed residential-candidate ULURP application numbers in 1985-1989."),
  tibble(metric = "post_1990_1994_residential_candidate_annual_mean", value = formatC(post_1990_1994_residential_mean, format = "f", digits = 1), status = "pass", note = "Mean annual parsed residential-candidate ULURP application numbers in 1990-1994."),
  tibble(metric = "output_nonempty_count", value = as.character(output_nonempty_count), status = if_else(output_nonempty_count == length(expected_output_paths), "pass", "fail"), note = "Expected non-QC outputs that exist and are nonempty.")
)

status_flag <- all(qc_df$status != "fail")

qc_df <- bind_rows(
  qc_df,
  tibble(metric = "status", value = as.character(as.integer(status_flag)), status = if_else(status_flag, "pass", "fail"), note = "One means the citywide ULURP trend task passed QC.")
)

write_csv_if_changed(qc_df, "../output/citywide_ulurp_application_trends_qc.csv")

if (!status_flag) {
  stop("Citywide ULURP application trend QC failed; inspect ../output/citywide_ulurp_application_trends_qc.csv.")
}

cat("Wrote citywide ULURP application trends to ../output\n")
