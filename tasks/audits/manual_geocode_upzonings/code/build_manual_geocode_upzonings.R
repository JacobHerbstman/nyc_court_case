# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/manual_geocode_upzonings/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

write_lines_if_changed <- function(lines, out_path) {
  temp_path <- tempfile(fileext = ".txt")
  writeLines(lines, temp_path, useBytes = TRUE)
  copy_if_changed(temp_path, out_path)
}

project_classification <- read_csv("../input/zap_zoning_map_special_permit_project_classification.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    project_id = as.character(project_id),
    completed_year = suppressWarnings(as.integer(completed_year)),
    project_name = as.character(project_name),
    project_brief = as.character(project_brief),
    borough = as.character(borough),
    community_district = as.character(community_district),
    has_zoning_map_change = as.logical(has_zoning_map_change),
    has_zoning_special_permit = as.logical(has_zoning_special_permit),
    included_zm_plus_residential_zs = as.logical(included_zm_plus_residential_zs),
    increased_residential_proxy = as.logical(increased_residential_proxy),
    mixed_use_text_flag = as.logical(mixed_use_text_flag)
  ) |>
  filter(!is.na(project_id), project_id != "")

if (nrow(project_classification) != n_distinct(project_classification$project_id)) {
  stop("ZAP zoning project classification is not unique by project_id.")
}

project_ccd2010 <- read_csv("../input/zap_zoning_map_special_permit_project_ccd2010_fractional.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    project_id = as.character(project_id),
    ccd2010_district_id = sprintf("%02d", suppressWarnings(as.integer(ccd2010_district_id))),
    ccd2010_assignment_weight = suppressWarnings(as.numeric(ccd2010_assignment_weight))
  ) |>
  filter(!is.na(project_id), project_id != "", !is.na(ccd2010_district_id))

if (nrow(project_ccd2010) != nrow(distinct(project_ccd2010, project_id, ccd2010_district_id))) {
  stop("ZAP project-CCD2010 assignment is not unique by project_id and district.")
}

zap_project_data <- read_parquet("../input/zap_project_data.parquet") |>
  transmute(
    project_id = as.character(project_id),
    zap_borough = as.character(borough),
    zap_community_district = as.character(community_district),
    zap_cc_district = as.character(cc_district),
    zap_council_district_first = suppressWarnings(as.integer(council_district_first))
  ) |>
  filter(!is.na(project_id), project_id != "")

if (nrow(zap_project_data) != n_distinct(zap_project_data$project_id)) {
  stop("Staged ZAP project data is not unique by project_id.")
}

zap_project_bbl <- read_parquet("../input/zap_project_bbl.parquet") |>
  transmute(
    project_id = as.character(project_id),
    bbl_standardized = as.character(bbl_standardized),
    bbl_validated = as.logical(is_validated),
    bbl_borough = as.character(validated_borough_name)
  ) |>
  filter(!is.na(project_id), project_id != "", !is.na(bbl_standardized), bbl_standardized != "") |>
  distinct(project_id, bbl_standardized, .keep_all = TRUE)

if (nrow(zap_project_bbl) != nrow(distinct(zap_project_bbl, project_id, bbl_standardized))) {
  stop("Staged ZAP project-BBL data is not unique by project_id and BBL.")
}

ccdist2010_bbl_lookup <- read_parquet("../input/ccdist2010_mappluto_bbl_lookup.parquet") |>
  transmute(bbl_standardized = as.character(bbl)) |>
  filter(!is.na(bbl_standardized), bbl_standardized != "") |>
  distinct()

manual_geocodes <- read_csv("manual_geocode_upzonings.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(project_id = as.character(project_id)) |>
  filter(!is.na(project_id), project_id != "")

if (nrow(manual_geocodes) != n_distinct(manual_geocodes$project_id)) {
  stop("Manual geocode file is not unique by project_id.")
}

zap_bbl_summary <- zap_project_bbl |>
  group_by(project_id) |>
  summarize(
    zap_bbl_count = n_distinct(bbl_standardized),
    zap_bbl_validated_count = sum(bbl_validated %in% TRUE, na.rm = TRUE),
    zap_bbl_examples = paste(head(sort(unique(bbl_standardized)), 5), collapse = ";"),
    .groups = "drop"
  )

ccd_bbl_summary <- zap_project_bbl |>
  inner_join(ccdist2010_bbl_lookup, by = "bbl_standardized", relationship = "many-to-one") |>
  group_by(project_id) |>
  summarize(ccd2010_bbl_count = n_distinct(bbl_standardized), .groups = "drop")

manual_columns <- c(
  "project_id",
  "geocode_review_status",
  "proper_geocode_address",
  "proper_geocode_city",
  "proper_geocode_state",
  "proper_geocode_zip",
  "address_source_url",
  "address_source_note",
  "geocoder",
  "geocode_longitude",
  "geocode_latitude",
  "geocode_match_quality",
  "geocode_source_url",
  "geocode_source_note",
  "ccd2010_district_id_override",
  "manual_review_notes"
)

missing_manual_columns <- setdiff(manual_columns, names(manual_geocodes))
if (length(missing_manual_columns) > 0) {
  stop("manual_geocode_upzonings.csv is missing columns: ", paste(missing_manual_columns, collapse = ", "))
}

missing_queue <- project_classification |>
  filter(
    completed_year >= 1980,
    completed_year <= 2025,
    included_zm_plus_residential_zs,
    increased_residential_proxy
  ) |>
  anti_join(project_ccd2010 |> distinct(project_id), by = "project_id") |>
  left_join(zap_project_data, by = "project_id", relationship = "one-to-one") |>
  left_join(zap_bbl_summary, by = "project_id", relationship = "one-to-one") |>
  left_join(ccd_bbl_summary, by = "project_id", relationship = "one-to-one") |>
  mutate(
    zap_bbl_count = coalesce(zap_bbl_count, 0L),
    zap_bbl_validated_count = coalesce(zap_bbl_validated_count, 0L),
    ccd2010_bbl_count = coalesce(ccd2010_bbl_count, 0L),
    current_assignment_gap = case_when(
      zap_bbl_count == 0 ~ "no_project_bbl_in_zap_bbl_table",
      ccd2010_bbl_count == 0 ~ "project_bbl_not_in_current_mappluto_ccd2010_lookup",
      TRUE ~ "unexpected_assigned"
    ),
    research_text = str_to_upper(str_squish(paste(project_name, project_brief, zap_borough, zap_community_district, sep = " "))),
    address_like_flag = str_detect(
      research_text,
      "(^|[^0-9])[0-9]{1,5}[ -][A-Z0-9]|[0-9]{1,5} .*(STREET| ST|AVENUE| AVE|ROAD| RD|BOULEVARD| BLVD|PLACE| PL|DRIVE| DR|LANE| LN|PARKWAY| PKWY|BROADWAY)"
    ),
    area_like_flag = str_detect(
      research_text,
      "REZONING|REZON|URA|URP|URBAN RENEWAL|NEIGHBORHOOD|CORRIDOR|DISTRICT|COMMONS|SPECIAL .* DISTRICT|DOWNTOWN|WATERFRONT|JEROME AVENUE|SEWARD PARK|ATLANTIC TERMINAL|WEST HARLEM|SOUTH JAMAICA|SUNNYSIDE|WOODHAVEN|WILLIAMSBURG"
    ),
    multi_community_district_flag = str_detect(coalesce(zap_community_district, community_district, ""), ",|;|/|&| AND "),
    has_zap_council_district_flag = !is.na(zap_council_district_first),
    conservative_single_site_flag = address_like_flag & !area_like_flag & !multi_community_district_flag,
    suggested_manual_strategy = case_when(
      conservative_single_site_flag ~ "geocode_address_or_intersection",
      has_zap_council_district_flag & !multi_community_district_flag ~ "verify_address_then_compare_to_zap_council_district",
      area_like_flag | multi_community_district_flag ~ "research_area_extent_before_geocoding",
      TRUE ~ "research_project_address"
    ),
    suggested_geocode_query = str_squish(paste(project_name, coalesce(zap_borough, borough), "NYC")),
    google_query_primary = str_squish(paste0("\"", project_name, "\" ", coalesce(zap_borough, borough), " ZAP ULURP address")),
    google_query_secondary = str_squish(paste0("\"", project_name, "\" \"", coalesce(zap_community_district, community_district), "\"")),
    google_query_dcp = str_squish(paste0("\"", project_id, "\" ZAP \"", project_name, "\""))
  ) |>
  select(-research_text) |>
  left_join(
    manual_geocodes |> select(all_of(manual_columns)),
    by = "project_id",
    relationship = "one-to-one"
  ) |>
  mutate(
    geocode_review_status = coalesce(geocode_review_status, "needs_research"),
    proper_geocode_city = coalesce(proper_geocode_city, "New York"),
    proper_geocode_state = coalesce(proper_geocode_state, "NY"),
    geocode_longitude = suppressWarnings(as.numeric(geocode_longitude)),
    geocode_latitude = suppressWarnings(as.numeric(geocode_latitude)),
    geocoded_flag = !is.na(geocode_longitude) & !is.na(geocode_latitude),
    ready_for_census_batch_flag = geocode_review_status %in% c("ready_for_batch", "confirmed_address") &
      !is.na(proper_geocode_address) &
      proper_geocode_address != "",
    manual_review_required_flag = !geocoded_flag & geocode_review_status %in% c("needs_research", "area_or_multi_site", "ambiguous")
  ) |>
  arrange(desc(conservative_single_site_flag), completed_year, project_id)

if (nrow(missing_queue) != 118 || nrow(missing_queue) != n_distinct(missing_queue$project_id)) {
  stop("Expected exactly 118 unique missing increased-residential upzoning projects in the manual geocode queue.")
}

census_batch_template <- missing_queue |>
  filter(ready_for_census_batch_flag) |>
  transmute(
    unique_id = project_id,
    street_address = proper_geocode_address,
    city = proper_geocode_city,
    state = proper_geocode_state,
    zip = proper_geocode_zip
  ) |>
  arrange(unique_id)

google_queries <- missing_queue |>
  transmute(
    project_id,
    completed_year,
    project_name,
    current_assignment_gap,
    suggested_manual_strategy,
    conservative_single_site_flag,
    google_query_primary,
    google_query_secondary,
    google_query_dcp
  )

prompt_lines <- c(
  "# Manual Geocode Research Prompt",
  "",
  "We are geocoding NYC ZAP increased-residential zoning-action projects that failed strict BBL-to-2010-Council-district assignment.",
  "",
  "For each project row I paste, research the web and return:",
  "1. A proper geocodable address or intersection in New York City.",
  "2. Whether the project is single-site, named-site, intersection, or area-wide/multi-site.",
  "3. One or more source URLs supporting the address.",
  "4. A confidence label: high, medium, low, or area-wide/manual.",
  "5. A short note explaining the choice.",
  "",
  "Do not invent addresses. If the project is area-wide, say that a point geocode is not conceptually adequate and identify the best representative address only if a documented project site exists.",
  "",
  "Use these output columns:",
  "project_id,proper_geocode_address,proper_geocode_city,proper_geocode_state,proper_geocode_zip,address_source_url,address_source_note,geocode_match_quality,manual_review_notes",
  "",
  "The local queue is in:",
  "/Users/jacobherbstman/Desktop/nyc_court_case/tasks/manual_geocode_upzonings/output/manual_geocode_upzoning_queue.csv",
  "",
  "Recommended batch size: 10-20 projects at a time."
)

write_csv_if_changed(missing_queue, "../output/manual_geocode_upzoning_queue.csv")
write_csv_if_changed(google_queries, "../output/manual_geocode_upzoning_google_queries.csv")
write_csv_if_changed(census_batch_template, "../output/manual_geocode_upzoning_census_batch_template.csv")
write_lines_if_changed(prompt_lines, "../output/manual_geocode_upzoning_chatgpt_prompt.md")

write_csv_if_changed(
  bind_rows(
    tibble(metric = "missing_upzoning_project_count", value = as.character(nrow(missing_queue)), note = "Increased-residential 1980-2025 ZAP project records missing strict BBL-to-CCD2010 assignment."),
    tibble(metric = "no_project_bbl_count", value = as.character(sum(missing_queue$current_assignment_gap == "no_project_bbl_in_zap_bbl_table")), note = "Missing projects without usable staged ZAP BBL rows."),
    tibble(metric = "bbl_not_in_current_mappluto_lookup_count", value = as.character(sum(missing_queue$current_assignment_gap == "project_bbl_not_in_current_mappluto_ccd2010_lookup")), note = "Missing projects with ZAP BBLs that are absent from current MapPLUTO 25v4 CCD2010 lookup."),
    tibble(metric = "conservative_single_site_count", value = as.character(sum(missing_queue$conservative_single_site_flag)), note = "Address-like, non-area-wide, single-community-district rows suitable for first-pass address geocoding."),
    tibble(metric = "area_or_multi_site_count", value = as.character(sum(missing_queue$area_like_flag | missing_queue$multi_community_district_flag)), note = "Rows requiring project-extent research before a point geocode is used."),
    tibble(metric = "has_zap_council_district_count", value = as.character(sum(missing_queue$has_zap_council_district_flag)), note = "Rows with a nonmissing ZAP council district text field."),
    tibble(metric = "manual_rows_entered", value = as.character(nrow(manual_geocodes)), note = "Rows currently entered in code/manual_geocode_upzonings.csv."),
    tibble(metric = "ready_for_census_batch_count", value = as.character(nrow(census_batch_template)), note = "Rows with a manually confirmed address ready for Census batch geocoding."),
    tibble(metric = "geocoded_manual_row_count", value = as.character(sum(missing_queue$geocoded_flag)), note = "Rows with manually entered longitude and latitude."),
    tibble(metric = "status", value = as.character(as.integer(nrow(missing_queue) == 118 && nrow(missing_queue) == n_distinct(missing_queue$project_id))), note = "One means the manual geocode queue has the expected unique project rows.")
  ),
  "../output/manual_geocode_upzoning_qc.csv"
)

cat("Wrote manual upzoning geocode queue to ../output\n")
