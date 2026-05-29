# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_dob_open_data/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

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

dob_raw_files <- read_csv("../input/dob_open_data_raw_files.csv", show_col_types = FALSE, na = c("", "NA")) |>
  filter(!is.na(raw_parquet_path), file.exists(raw_parquet_path)) |>
  mutate(
    source_id = as.character(source_id),
    pull_date = as.character(pull_date),
    raw_parquet_path = as.character(raw_parquet_path)
  )

if (nrow(dob_raw_files) == 0) {
  write_csv(tibble(), "../output/dob_open_data_files.csv", na = "")
  quit(save = "no")
}

duplicate_source_pull_files <- dob_raw_files |>
  count(source_id, pull_date, name = "file_count") |>
  filter(file_count > 1)

if (nrow(duplicate_source_pull_files) > 0) {
  stop(
    "DOB raw index has multiple raw parquet files for the same source_id and pull_date: ",
    paste(paste(duplicate_source_pull_files$source_id, duplicate_source_pull_files$pull_date, sep = ":"), collapse = ", ")
  )
}

multi_pull_sources <- dob_raw_files |>
  distinct(source_id, pull_date) |>
  count(source_id, name = "pull_date_count") |>
  filter(pull_date_count > 1)

if (nrow(multi_pull_sources) > 0) {
  stop(
    "DOB staging writes source-level parquet names, so each source_id must have one pull_date. Multiple pull dates found for: ",
    paste(multi_pull_sources$source_id, collapse = ", ")
  )
}

index_rows <- list()

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
}

write_csv(bind_rows(index_rows), "../output/dob_open_data_files.csv", na = "")
cat("Wrote DOB staging outputs to ../output\n")
