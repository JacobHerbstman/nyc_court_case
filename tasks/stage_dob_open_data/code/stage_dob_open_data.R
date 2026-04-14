# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_dob_open_data/code")
# dob_open_data_raw_files_csv <- "../input/dob_open_data_raw_files.csv"
# out_index_csv <- "../output/dob_open_data_files.csv"
# out_qc_csv <- "../output/dob_open_data_qc.csv"
# out_field_dictionary_csv <- "../output/dob_field_dictionary.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4) {
  stop("Expected 4 arguments: dob_open_data_raw_files_csv out_index_csv out_qc_csv out_field_dictionary_csv")
}

dob_open_data_raw_files_csv <- args[1]
out_index_csv <- args[2]
out_qc_csv <- args[3]
out_field_dictionary_csv <- args[4]

matched_column_name <- function(df, candidates) {
  hits <- candidates[candidates %in% names(df)]
  if (length(hits) == 0) {
    return(NA_character_)
  }
  hits[1]
}

append_reason <- function(existing_reason, new_flag, new_reason) {
  existing_reason <- ifelse(is.na(existing_reason), "", existing_reason)
  updated_reason <- ifelse(new_flag, paste(existing_reason, new_reason, sep = ";"), existing_reason)
  updated_reason <- str_replace_all(updated_reason, "^;+|;+$", "")
  updated_reason[updated_reason == ""] <- NA_character_
  updated_reason
}

standardize_job_type <- function(x) {
  raw_value <- str_to_upper(str_squish(as.character(x)))
  out <- rep(NA_character_, length(raw_value))

  out[raw_value %in% c("NB", "NEW BUILDING")] <- "New Building"
  out[raw_value %in% c("DM", "DEMOLITION")] <- "Demolition"
  out[raw_value %in% c("A1", "A2", "A3", "ALT", "ALTERATION", "ALTERATION CO", "ALTERATION CO-NB", "ALT-CO")] <- "Alteration"
  out[is.na(out) & !is.na(raw_value) & raw_value != ""] <- str_to_title(str_to_lower(raw_value[is.na(out) & !is.na(raw_value) & raw_value != ""]))
  out
}

derive_source_record_id <- function(job_number, doc_number, raw_id) {
  combined_id <- ifelse(!is.na(job_number) & !is.na(doc_number), paste(job_number, doc_number, sep = "-"), NA_character_)
  coalesce_character(raw_id, combined_id, job_number)
}

derive_source_label <- function(primary_value, primary_label, fallback_value, fallback_label, third_value = NULL, third_label = NULL) {
  out <- rep(NA_character_, length(primary_value))
  out[!is.na(primary_value) & primary_value != ""] <- primary_label
  fallback_flag <- (is.na(out) | out == "") & !is.na(fallback_value) & fallback_value != ""
  out[fallback_flag] <- fallback_label

  if (!is.null(third_value) && !is.null(third_label)) {
    third_flag <- (is.na(out) | out == "") & !is.na(third_value) & third_value != ""
    out[third_flag] <- third_label
  }

  out[out == ""] <- NA_character_
  out
}

derive_record_date <- function(source_id, filing_date, co_issue_date) {
  if (grepl("certificate_of_occupancy", source_id)) {
    return(list(date = co_issue_date, source = ifelse(!is.na(co_issue_date), "co_issue_date", NA_character_)))
  }

  list(date = filing_date, source = ifelse(!is.na(filing_date), "filing_date", NA_character_))
}

derive_permit_like_date <- function(permit_date, fully_permitted_date, approved_date) {
  permit_like_date <- coalesce(permit_date, fully_permitted_date, approved_date)
  permit_like_source <- derive_source_label(
    ifelse(!is.na(permit_date), "x", NA_character_), "permit_date",
    ifelse(!is.na(fully_permitted_date), "x", NA_character_), "fully_permitted_date",
    ifelse(!is.na(approved_date), "x", NA_character_), "approved_date"
  )

  list(date = permit_like_date, source = permit_like_source)
}

derive_community_district_source <- function(raw_value, standardized_value, borough) {
  raw_num <- suppressWarnings(as.integer(str_extract(as.character(raw_value), "[0-9]{1,3}")))
  borough_code <- standardize_borough_code(borough)
  out <- rep(NA_character_, length(standardized_value))
  out[!is.na(raw_num) & raw_num >= 101 & raw_num <= 595 & !is.na(standardized_value)] <- "raw_three_digit"
  out[!is.na(raw_num) & raw_num >= 1 & raw_num <= 18 & !is.na(borough_code) & !is.na(standardized_value)] <- "borough_prefixed_from_raw"
  out
}

dob_raw_files <- read_csv(dob_open_data_raw_files_csv, show_col_types = FALSE, na = c("", "NA"))
dob_raw_files <- dob_raw_files[!is.na(dob_raw_files$raw_parquet_path) & file.exists(dob_raw_files$raw_parquet_path), ]

if (nrow(dob_raw_files) == 0) {
  write_csv(tibble(), out_index_csv, na = "")
  write_csv(tibble(), out_qc_csv, na = "")
  write_csv(tibble(), out_field_dictionary_csv, na = "")
  quit(save = "no")
}

index_rows <- list()
qc_rows <- list()
field_rows <- list()

for (i in seq_len(nrow(dob_raw_files))) {
  row <- dob_raw_files[i, ]
  dob_df <- read_parquet(row$raw_parquet_path) %>%
    as.data.frame() %>%
    as_tibble()

  job_number_raw <- coalesce_character(
    pick_first_existing(dob_df, c("job_filing_number")),
    pick_first_existing(dob_df, c("job_number", "job")),
    pick_first_existing(dob_df, c("job_filing_name"))
  )
  doc_number_raw <- pick_first_existing(dob_df, c("doc", "doc_number"))
  raw_record_id <- coalesce_character(
    pick_first_existing(dob_df, c("job_filing_number")),
    pick_first_existing(dob_df, c("job_filing_name"))
  )
  borough_raw <- pick_first_existing(dob_df, c("borough"))
  house_number_raw <- coalesce_character(
    pick_first_existing(dob_df, c("house_no", "house", "house_number")),
    pick_first_existing(dob_df, c("number"))
  )
  street_name_raw <- coalesce_character(
    pick_first_existing(dob_df, c("street_name")),
    pick_first_existing(dob_df, c("street"))
  )
  block_raw <- pick_first_existing(dob_df, c("block"))
  lot_raw <- pick_first_existing(dob_df, c("lot"))
  bbl_raw <- pick_first_existing(dob_df, c("bbl"))
  bbl_built <- build_bbl(borough_raw, block_raw, lot_raw)
  bbl_value <- coalesce_character(bbl_raw, bbl_built)
  bin_raw <- coalesce_character(
    pick_first_existing(dob_df, c("bin")),
    pick_first_existing(dob_df, c("bin_number")),
    pick_first_existing(dob_df, c("gis_bin"))
  )
  community_raw <- coalesce_character(
    pick_first_existing(dob_df, c("community_board")),
    pick_first_existing(dob_df, c("commmunity_board")),
    pick_first_existing(dob_df, c("community___board")),
    pick_first_existing(dob_df, c("community_district")),
    pick_first_existing(dob_df, c("community_districts"))
  )
  council_raw <- coalesce_character(
    pick_first_existing(dob_df, c("gis_council_district")),
    pick_first_existing(dob_df, c("council_district")),
    pick_first_existing(dob_df, c("city_council_districts"))
  )

  filing_date <- parse_mixed_date(coalesce_character(
    pick_first_existing(dob_df, c("pre_filing_date")),
    pick_first_existing(dob_df, c("filing_date")),
    pick_first_existing(dob_df, c("submitted_date"))
  ))
  approved_date <- parse_mixed_date(coalesce_character(
    pick_first_existing(dob_df, c("approved_date")),
    pick_first_existing(dob_df, c("approved"))
  ))
  permit_date <- parse_mixed_date(coalesce_character(
    pick_first_existing(dob_df, c("first_permit_date"))
  ))
  fully_permitted_date <- parse_mixed_date(coalesce_character(
    pick_first_existing(dob_df, c("fully_permitted"))
  ))
  co_issue_date <- parse_mixed_date(coalesce_character(
    pick_first_existing(dob_df, c("c_o_issue_date")),
    pick_first_existing(dob_df, c("c_of_o_issuance_date")),
    pick_first_existing(dob_df, c("certificate_of_occupancy_date"))
  ))
  current_status_date <- parse_mixed_date(coalesce_character(
    pick_first_existing(dob_df, c("current_status_date"))
  ))
  signoff_date <- parse_mixed_date(coalesce_character(
    pick_first_existing(dob_df, c("signoff_date"))
  ))

  record_date_info <- derive_record_date(row$source_id, filing_date, co_issue_date)
  permit_like_info <- derive_permit_like_date(permit_date, fully_permitted_date, approved_date)

  job_type_raw <- pick_first_existing(dob_df, c("job_type"))
  job_type_standard <- standardize_job_type(job_type_raw)
  community_district <- standardize_community_district(borough_raw, community_raw)
  council_district <- standardize_council_district(council_raw)
  existing_dwelling_units <- suppressWarnings(as.numeric(coalesce_character(
    pick_first_existing(dob_df, c("existing_dwelling_units")),
    pick_first_existing(dob_df, c("ex_dwelling_unit"))
  )))
  proposed_dwelling_units <- suppressWarnings(as.numeric(coalesce_character(
    pick_first_existing(dob_df, c("proposed_dwelling_units")),
    pick_first_existing(dob_df, c("pr_dwelling_unit")),
    pick_first_existing(dob_df, c("number_of_dwelling_units"))
  )))
  net_dwelling_units <- ifelse(
    !is.na(proposed_dwelling_units) & !is.na(existing_dwelling_units),
    proposed_dwelling_units - existing_dwelling_units,
    ifelse(job_type_standard == "New Building", proposed_dwelling_units, NA_real_)
  )

  staged_df <- tibble(
    source_id = row$source_id,
    source_record_id = derive_source_record_id(job_number_raw, doc_number_raw, raw_record_id),
    job_number = job_number_raw,
    doc_number = doc_number_raw,
    borough_code = standardize_borough_code(borough_raw),
    borough_name = standardize_borough_name(borough_raw),
    house_number = house_number_raw,
    street_name = street_name_raw,
    address = combine_address(house_number_raw, street_name_raw),
    address_source = derive_source_label(house_number_raw, "house_number_and_street_name", house_number_raw, "number_and_street_name"),
    block = as.character(block_raw),
    lot = as.character(lot_raw),
    bbl = bbl_value,
    bbl_source = derive_source_label(bbl_raw, "raw_bbl", bbl_built, "built_from_borough_block_lot"),
    bin = bin_raw,
    bin_source = derive_source_label(
      pick_first_existing(dob_df, c("bin")), "raw_bin",
      pick_first_existing(dob_df, c("bin_number")), "bin_number",
      pick_first_existing(dob_df, c("gis_bin")), "gis_bin"
    ),
    job_type_raw = job_type_raw,
    job_type_standard = job_type_standard,
    record_status = coalesce_character(
      pick_first_existing(dob_df, c("filing_status")),
      pick_first_existing(dob_df, c("job_status_descrp")),
      pick_first_existing(dob_df, c("job_status")),
      pick_first_existing(dob_df, c("application_status_raw")),
      pick_first_existing(dob_df, c("c_of_o_status"))
    ),
    community_district_raw = community_raw,
    community_district = community_district,
    community_district_source = derive_community_district_source(community_raw, community_district, borough_raw),
    council_district_raw = council_raw,
    council_district = council_district,
    council_district_source = ifelse(!is.na(council_district), "raw_council_district", NA_character_),
    filing_date = filing_date,
    approved_date = approved_date,
    permit_date = permit_date,
    fully_permitted_date = fully_permitted_date,
    permit_like_date = permit_like_info$date,
    permit_like_date_source = permit_like_info$source,
    co_issue_date = co_issue_date,
    current_status_date = current_status_date,
    signoff_date = signoff_date,
    record_date = record_date_info$date,
    record_year = suppressWarnings(as.integer(format(record_date_info$date, "%Y"))),
    record_year_source = record_date_info$source,
    existing_dwelling_units = existing_dwelling_units,
    proposed_dwelling_units = proposed_dwelling_units,
    net_dwelling_units = net_dwelling_units,
    source_raw_path = row$raw_path,
    raw_parquet_path = row$raw_parquet_path,
    pull_date = row$pull_date,
    unresolved_reason = NA_character_
  )

  staged_df$unresolved_reason <- append_reason(
    staged_df$unresolved_reason,
    is.na(staged_df$bbl) & is.na(staged_df$bin) & is.na(staged_df$address),
    "missing_bbl_bin_address"
  )
  staged_df$unresolved_reason <- append_reason(
    staged_df$unresolved_reason,
    is.na(staged_df$record_year),
    "missing_record_year"
  )
  staged_df$unresolved_reason <- append_reason(
    staged_df$unresolved_reason,
    is.na(staged_df$community_district),
    "missing_community_district"
  )
  staged_df$unresolved_reason <- append_reason(
    staged_df$unresolved_reason,
    is.na(staged_df$council_district),
    "missing_council_district"
  )

  out_parquet_local <- file.path("..", "output", paste0(row$source_id, ".parquet"))
  out_parquet <- file.path("..", "..", "stage_dob_open_data", "output", paste0(row$source_id, ".parquet"))
  write_parquet_if_changed(staged_df, out_parquet_local)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    raw_path = row$raw_path,
    raw_parquet_path = row$raw_parquet_path,
    parquet_path = out_parquet,
    pull_date = row$pull_date,
    status = "staged"
  )

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    status = "staged",
    row_count = nrow(staged_df),
    start_date = safe_min_date(staged_df$record_date),
    end_date = safe_max_date(staged_df$record_date),
    nonmissing_bbl_share = mean(!is.na(staged_df$bbl)),
    nonmissing_bin_share = mean(!is.na(staged_df$bin)),
    nonmissing_address_share = mean(!is.na(staged_df$address)),
    nonmissing_cd_share = mean(!is.na(staged_df$community_district)),
    nonmissing_council_share = mean(!is.na(staged_df$council_district)),
    nonmissing_record_year_share = mean(!is.na(staged_df$record_year)),
    unresolved_share = mean(!is.na(staged_df$unresolved_reason)),
    nb_row_count = sum(staged_df$job_type_standard == "New Building", na.rm = TRUE)
  )

  field_rows[[i]] <- bind_rows(
    tibble(source_id = row$source_id, canonical_field = "source_record_id", matched_raw_column = matched_column_name(dob_df, c("job_filing_number", "job_filing_name", "job_number", "job")), note = "Primary identifier used for staged source_record_id."),
    tibble(source_id = row$source_id, canonical_field = "job_number", matched_raw_column = matched_column_name(dob_df, c("job_filing_number", "job_number", "job", "job_filing_name")), note = "Job number field used in staging."),
    tibble(source_id = row$source_id, canonical_field = "doc_number", matched_raw_column = matched_column_name(dob_df, c("doc", "doc_number")), note = "Document number field used in staging."),
    tibble(source_id = row$source_id, canonical_field = "house_number", matched_raw_column = matched_column_name(dob_df, c("house_no", "house", "house_number", "number")), note = "House or building number field."),
    tibble(source_id = row$source_id, canonical_field = "street_name", matched_raw_column = matched_column_name(dob_df, c("street_name", "street")), note = "Street name field."),
    tibble(source_id = row$source_id, canonical_field = "bbl", matched_raw_column = matched_column_name(dob_df, c("bbl")), note = "If missing, staging builds BBL from borough, block, and lot."),
    tibble(source_id = row$source_id, canonical_field = "bin", matched_raw_column = matched_column_name(dob_df, c("bin", "bin_number", "gis_bin")), note = "BIN priority order."),
    tibble(source_id = row$source_id, canonical_field = "community_district", matched_raw_column = matched_column_name(dob_df, c("community_board", "commmunity_board", "community___board", "community_district", "community_districts")), note = "Small values are borough-prefixed to current three-digit community districts."),
    tibble(source_id = row$source_id, canonical_field = "council_district", matched_raw_column = matched_column_name(dob_df, c("gis_council_district", "council_district", "city_council_districts")), note = "Citywide council district field."),
    tibble(source_id = row$source_id, canonical_field = "filing_date", matched_raw_column = matched_column_name(dob_df, c("pre_filing_date", "filing_date", "submitted_date")), note = "Primary filing date candidates."),
    tibble(source_id = row$source_id, canonical_field = "approved_date", matched_raw_column = matched_column_name(dob_df, c("approved_date", "approved")), note = "Approved date candidates."),
    tibble(source_id = row$source_id, canonical_field = "permit_date", matched_raw_column = matched_column_name(dob_df, c("first_permit_date")), note = "Permit issuance date candidate."),
    tibble(source_id = row$source_id, canonical_field = "fully_permitted_date", matched_raw_column = matched_column_name(dob_df, c("fully_permitted")), note = "BIS fully permitted field."),
    tibble(source_id = row$source_id, canonical_field = "co_issue_date", matched_raw_column = matched_column_name(dob_df, c("c_o_issue_date", "c_of_o_issuance_date", "certificate_of_occupancy_date")), note = "Certificate of occupancy date candidates."),
    tibble(source_id = row$source_id, canonical_field = "existing_dwelling_units", matched_raw_column = matched_column_name(dob_df, c("existing_dwelling_units", "ex_dwelling_unit")), note = "Existing dwelling units candidates."),
    tibble(source_id = row$source_id, canonical_field = "proposed_dwelling_units", matched_raw_column = matched_column_name(dob_df, c("proposed_dwelling_units", "pr_dwelling_unit", "number_of_dwelling_units")), note = "Proposed or issued dwelling units candidates.")
  )
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
write_csv(bind_rows(field_rows), out_field_dictionary_csv, na = "")
cat("Wrote DOB staging outputs to", dirname(out_index_csv), "\n")
