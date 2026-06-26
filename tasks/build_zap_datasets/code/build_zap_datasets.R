# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_zap_datasets/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

normalize_text_field <- function(x) {
  out <- trimws(as.character(x))
  out[out %in% c("", "NA", "N/A", "NULL")] <- NA_character_
  out
}

valid_bbl_format <- function(x) {
  raw_value <- as.character(x)
  !is.na(raw_value) & str_detect(raw_value, "^[1-5][0-9]{9}$")
}

has_multi_geography <- function(x) {
  raw_value <- str_squish(as.character(x))
  !is.na(raw_value) & raw_value != "" & str_detect(raw_value, "[,;/&]|\\band\\b|\\s+-\\s+")
}

has_compact_multi_council <- function(x) {
  raw_value <- str_squish(as.character(x))
  !is.na(raw_value) & str_detect(raw_value, "^[0-9]{3,}$")
}

raw_index <- read_csv("../output/zap_raw_files.csv", show_col_types = FALSE, na = c("", "NA")) |>
  filter(!is.na(raw_parquet_path), file.exists(raw_parquet_path)) |>
  mutate(vintage = as.character(vintage), raw_parquet_path = as.character(raw_parquet_path))

project_row <- raw_index |>
  filter(source_id == "dcp_zap_project_data") |>
  arrange(desc(vintage)) |>
  slice_head(n = 1)

bbl_row <- raw_index |>
  filter(source_id == "dcp_zap_bbl") |>
  arrange(desc(vintage)) |>
  slice_head(n = 1)

if (nrow(project_row) == 0 || nrow(bbl_row) == 0) {
  write_parquet_if_changed(tibble(), "../output/zap_project_data.parquet")
  write_parquet_if_changed(tibble(), "../output/zap_project_bbl.parquet")
  quit(save = "no")
}

raw_project_df <- read_parquet(project_row$raw_parquet_path[[1]]) |>
  as.data.frame() |>
  as_tibble()

missing_raw_project_id_count <- sum(is.na(normalize_text_field(raw_project_df$project_id)))

if (missing_raw_project_id_count > 0) {
  stop("Raw ZAP project data contain missing project_id values; inspect before deduping.")
}

project_df <- raw_project_df |>
  mutate(
    project_id = normalize_text_field(project_id),
    project_name = normalize_text_field(project_name),
    project_brief = normalize_text_field(project_brief),
    project_status = normalize_text_field(project_status),
    public_status = normalize_text_field(public_status),
    ulurp_non = normalize_text_field(ulurp_non),
    actions = normalize_text_field(actions),
    ulurp_numbers = normalize_text_field(ulurp_numbers),
    ceqr_type = normalize_text_field(ceqr_type),
    ceqr_number = normalize_text_field(ceqr_number),
    eas_eis = normalize_text_field(eas_eis),
    ceqr_leadagency = normalize_text_field(ceqr_leadagency),
    primary_applicant = normalize_text_field(primary_applicant),
    applicant_type = normalize_text_field(applicant_type),
    borough = normalize_text_field(borough),
    community_district = normalize_text_field(community_district),
    cc_district = normalize_text_field(cc_district),
    community_district_multi_flag = has_multi_geography(community_district),
    council_district_multi_flag = has_multi_geography(cc_district) | has_compact_multi_council(cc_district),
    borough_code = standardize_borough_code(borough),
    borough_name_standardized = standardize_borough_name(borough),
    community_district_standardized = standardize_community_district(borough, community_district),
    council_district_first = standardize_council_district(cc_district),
    current_milestone = normalize_text_field(current_milestone),
    current_envmilestone = normalize_text_field(current_envmilestone),
    current_milestone_date_parsed = parse_mixed_date(current_milestone_date),
    current_envmilestone_date_parsed = parse_mixed_date(current_envmilestone_date),
    app_filed_date_parsed = parse_mixed_date(app_filed_date),
    noticed_date_parsed = parse_mixed_date(noticed_date),
    certified_referred_date_parsed = parse_mixed_date(certified_referred),
    approval_date_parsed = parse_mixed_date(approval_date),
    completed_date_parsed = parse_mixed_date(completed_date),
    project_reference_date = coalesce(app_filed_date_parsed, noticed_date_parsed, certified_referred_date_parsed, approval_date_parsed, completed_date_parsed),
    project_reference_year = suppressWarnings(as.integer(format(project_reference_date, "%Y"))),
    project_reference_decade = if_else(!is.na(project_reference_year), paste0(floor(project_reference_year / 10) * 10, "s"), NA_character_),
    ulurp_group = case_when(
      str_to_upper(ulurp_non) == "ULURP" ~ "ULURP",
      str_detect(str_to_upper(ulurp_non), "NON") ~ "Non-ULURP",
      TRUE ~ NA_character_
    ),
    input_row_number = row_number()
  ) |>
  arrange(project_id, desc(!is.na(project_reference_date)), desc(!is.na(noticed_date_parsed)), desc(!is.na(approval_date_parsed)), input_row_number) |>
  distinct(project_id, .keep_all = TRUE) |>
  select(-input_row_number)

raw_bbl_df <- read_parquet(bbl_row$raw_parquet_path[[1]]) |>
  as.data.frame() |>
  as_tibble()

bbl_df <- raw_bbl_df |>
  mutate(
    project_id = normalize_text_field(project_id),
    bbl = normalize_text_field(bbl),
    validated_borough = normalize_text_field(validated_borough),
    validated_block = suppressWarnings(as.integer(normalize_text_field(validated_block))),
    validated_lot = suppressWarnings(as.integer(normalize_text_field(validated_lot))),
    validated = normalize_text_field(validated),
    validated_date_parsed = parse_mixed_date(validated_date),
    unverified_borough = normalize_text_field(unverified_borough),
    unverified_block = suppressWarnings(as.integer(normalize_text_field(unverified_block))),
    unverified_lot = suppressWarnings(as.integer(normalize_text_field(unverified_lot))),
    validated_borough_code = standardize_borough_code(validated_borough),
    validated_borough_name = standardize_borough_name(validated_borough),
    bbl_valid_format = valid_bbl_format(bbl),
    bbl_built_from_components = build_bbl(validated_borough, validated_block, validated_lot),
    raw_bbl_conflicts_with_validated_components = !is.na(bbl) &
      bbl_valid_format &
      !is.na(bbl_built_from_components) &
      bbl != bbl_built_from_components,
    bbl_standardized = coalesce_character(if_else(bbl_valid_format, bbl, NA_character_), bbl_built_from_components),
    is_validated = case_when(
      str_to_upper(validated) == "TRUE" ~ TRUE,
      str_to_upper(validated) == "FALSE" ~ FALSE,
      TRUE ~ NA
    ),
    input_row_number = row_number()
  ) |>
  arrange(project_id, bbl_standardized, desc(is_validated), desc(!is.na(validated_date_parsed)), desc(validated_date_parsed), input_row_number) |>
  distinct(project_id, bbl_standardized, .keep_all = TRUE) |>
  select(-input_row_number)

write_parquet_if_changed(project_df, "../output/zap_project_data.parquet")
write_parquet_if_changed(bbl_df, "../output/zap_project_bbl.parquet")

cat("Wrote ZAP dataset outputs to ../output\n")
