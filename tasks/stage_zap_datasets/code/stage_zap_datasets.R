# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_zap_datasets/code")
# zap_raw_files_csv <- "../input/zap_raw_files.csv"
# out_qc_csv <- "../output/zap_stage_qc.csv"
# out_project_parquet <- "../output/zap_project_data.parquet"
# out_bbl_parquet <- "../output/zap_project_bbl.parquet"

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

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4) {
  stop("Expected 4 arguments: zap_raw_files_csv out_qc_csv out_project_parquet out_bbl_parquet")
}

zap_raw_files_csv <- args[1]
out_qc_csv <- args[2]
out_project_parquet <- args[3]
out_bbl_parquet <- args[4]

raw_index <- read_csv(zap_raw_files_csv, show_col_types = FALSE, na = c("", "NA")) |>
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
  write_csv(tibble(status = "missing_zap_source"), out_qc_csv, na = "")
  write_parquet_if_changed(tibble(), out_project_parquet)
  write_parquet_if_changed(tibble(), out_bbl_parquet)
  quit(save = "no")
}

raw_project_df <- read_parquet(project_row$raw_parquet_path[[1]]) |>
  as.data.frame() |>
  as_tibble()

project_input_row_count <- nrow(raw_project_df)

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

bbl_input_row_count <- nrow(raw_bbl_df)

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
    bbl_standardized = coalesce_character(bbl, build_bbl(validated_borough, validated_block, validated_lot)),
    is_validated = case_when(
      str_to_upper(validated) == "TRUE" ~ TRUE,
      str_to_upper(validated) == "FALSE" ~ FALSE,
      TRUE ~ NA
    ),
    input_row_number = row_number()
  ) |>
  arrange(project_id, bbl_standardized, desc(is_validated), desc(!is.na(validated_date_parsed)), input_row_number) |>
  distinct(project_id, bbl_standardized, .keep_all = TRUE) |>
  select(-input_row_number)

write_parquet_if_changed(project_df, out_project_parquet)
write_parquet_if_changed(bbl_df, out_bbl_parquet)

projects_with_any_bbl <- n_distinct(bbl_df$project_id[!is.na(bbl_df$project_id) & !is.na(bbl_df$bbl_standardized)])

qc_df <- tibble(
  project_source_vintage = project_row$vintage[[1]],
  bbl_source_vintage = bbl_row$vintage[[1]],
  project_input_row_count = project_input_row_count,
  project_row_count = nrow(project_df),
  project_duplicate_rows_dropped = project_input_row_count - nrow(project_df),
  project_unique_project_id_count = n_distinct(project_df$project_id),
  project_nonmissing_borough_share = mean(!is.na(project_df$borough_name_standardized)),
  project_nonmissing_cd_share = mean(!is.na(project_df$community_district_standardized)),
  project_min_reference_date = safe_min_date(project_df$project_reference_date),
  project_max_reference_date = safe_max_date(project_df$project_reference_date),
  bbl_input_row_count = bbl_input_row_count,
  bbl_row_count = nrow(bbl_df),
  bbl_duplicate_rows_dropped = bbl_input_row_count - nrow(bbl_df),
  bbl_unique_project_id_count = n_distinct(bbl_df$project_id),
  bbl_unique_project_bbl_count = n_distinct(paste(bbl_df$project_id, bbl_df$bbl_standardized, sep = "_")),
  bbl_nonmissing_bbl_share = mean(!is.na(bbl_df$bbl_standardized)),
  bbl_validated_true_share = mean(bbl_df$is_validated %in% TRUE, na.rm = TRUE),
  project_share_with_any_bbl = projects_with_any_bbl / n_distinct(project_df$project_id)
)

write_csv(qc_df, out_qc_csv, na = "")

cat("Wrote ZAP staging outputs to", dirname(out_qc_csv), "\n")
