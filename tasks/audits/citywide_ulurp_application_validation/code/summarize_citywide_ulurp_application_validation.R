suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

has_action_code <- function(x, code) {
  raw_value <- str_to_upper(coalesce(as.character(x), ""))
  str_detect(raw_value, paste0("(^|[^A-Z0-9])", code, "([^A-Z0-9]|$)"))
}

has_ulurp_code <- function(x, code) {
  raw_value <- str_replace_all(str_to_upper(coalesce(as.character(x), "")), "\\s+", "")
  str_detect(raw_value, paste0("[0-9]{6,7}A?", code, "[A-Z]"))
}

extract_ulurp_numbers <- function(x) {
  raw_value <- str_replace_all(str_to_upper(coalesce(as.character(x), "")), "\\s+", "")
  str_extract_all(raw_value, "\\b[CN]?[0-9]{6,7}A?[A-Z]{3}\\b")
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
    cert_year = suppressWarnings(as.integer(format(certified_referred_date_parsed, "%Y"))),
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
    garage_or_parking_text_flag = str_detect(project_use_text, "GARAGE|PARKING|PARKING FACILITY|PARKING LOT|MUNICIPAL GARAGE"),
    incinerator_or_waste_text_flag = str_detect(project_use_text, "INCINERATOR|WASTE TRANSFER|TRANSFER STATION|SOLID WASTE|SANITATION|REFUSE|RECYCLING|COMPOST|LANDFILL"),
    jail_or_correction_text_flag = str_detect(project_use_text, "JAIL|PRISON|CORRECTION|DETENTION|DETENTION CENTER|RIKERS"),
    utility_or_infrastructure_text_flag = str_detect(project_use_text, "UTILITY|POWER PLANT|SUBSTATION|SEWAGE|WASTEWATER|WATER POLLUTION CONTROL|GAS MAIN|ELECTRIC"),
    shelter_or_institution_text_flag = str_detect(project_use_text, "HOMELESS|SHELTER|TRANSITIONAL HOUSING|TRANSITIONAL RESIDENCE|TEMPORARY HOUSING|FOSTER HOME|GROUP HOME|NURSING HOME|DORMITORY|RESIDENTIAL TREATMENT"),
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
    residential_candidate_flag = residential_broad_text_flag |
      mih_flag_bool |
      hpd_text_flag |
      hpd_housing_action_flag,
    housing_production_conservative_flag = (housing_unit_text_flag | mih_flag_bool) &
      !negative_amenity_text_flag &
      !shelter_or_institution_text_flag
  )

outcome_ids <- c(
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

project_year_counts <- expand_grid(cert_year = 1976:2025, outcome_id = outcome_ids) |>
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
      pivot_longer(all_of(outcome_ids), names_to = "outcome_id", values_to = "included_flag") |>
      group_by(cert_year, outcome_id) |>
      summarize(application_count = sum(included_flag, na.rm = TRUE), .groups = "drop"),
    by = c("cert_year", "outcome_id"),
    relationship = "one-to-one"
  ) |>
  mutate(
    application_count = coalesce(application_count, 0L),
    count_unit = "zap_project_records"
  )

parsed_ulurp_number_base <- ulurp_df |>
  filter(cert_year >= 1976, cert_year <= 2025) |>
  transmute(
    project_id,
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
  unnest_longer(ulurp_application_number, keep_empty = FALSE) |>
  filter(!is.na(ulurp_application_number), str_squish(ulurp_application_number) != "") |>
  arrange(ulurp_application_number, cert_year, project_id) |>
  group_by(ulurp_application_number) |>
  summarize(
    cert_year = first(cert_year),
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
    .groups = "drop"
  )

number_year_counts <- expand_grid(cert_year = 1976:2025, outcome_id = outcome_ids) |>
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
      pivot_longer(all_of(outcome_ids), names_to = "outcome_id", values_to = "included_flag") |>
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
    )
  ) |>
  group_by(count_unit, outcome_id) |>
  arrange(cert_year, .by_group = TRUE) |>
  mutate(application_count_ma3 = centered_ma3(application_count)) |>
  ungroup() |>
  arrange(count_unit, outcome_id, cert_year)

if (nrow(citywide_year_counts) != nrow(distinct(citywide_year_counts, count_unit, cert_year, outcome_id))) {
  stop("Citywide ULURP yearly series is not unique by count unit, year, and outcome.")
}

if (min(citywide_year_counts$cert_year) != 1976 || max(citywide_year_counts$cert_year) != 2025) {
  stop("Citywide ULURP yearly series does not cover 1976-2025.")
}

write_csv_if_changed(citywide_year_counts, "../output/citywide_ulurp_application_validation_year.csv")
