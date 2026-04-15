suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(tibble)
})

matched_permit_column_name <- function(df, candidates) {
  hits <- candidates[candidates %in% names(df)]

  if (length(hits) == 0) {
    return(NA_character_)
  }

  hits[1]
}

pick_permit_value <- function(df, candidates) {
  hits <- candidates[candidates %in% names(df)]

  if (length(hits) == 0) {
    return(rep(NA_character_, nrow(df)))
  }

  out <- as.character(df[[hits[1]]])

  if (length(hits) > 1) {
    for (i in seq(2, length(hits))) {
      next_value <- as.character(df[[hits[i]]])
      replace_flag <- is.na(out) | str_squish(out) == ""
      out[replace_flag] <- next_value[replace_flag]
    }
  }

  out <- str_squish(out)
  out[out == ""] <- NA_character_
  out
}

pick_permit_numeric <- function(df, candidates) {
  suppressWarnings(as.numeric(pick_permit_value(df, candidates)))
}

append_reason <- function(existing_reason, new_flag, new_reason) {
  existing_reason <- ifelse(is.na(existing_reason), "", existing_reason)
  updated_reason <- ifelse(new_flag, paste(existing_reason, new_reason, sep = ";"), existing_reason)
  updated_reason <- str_replace_all(updated_reason, "^;+|;+$", "")
  updated_reason[updated_reason == ""] <- NA_character_
  updated_reason
}

standardize_dob_permit_job_type <- function(x) {
  raw_value <- str_to_upper(str_squish(as.character(x)))
  out <- rep(NA_character_, length(raw_value))

  out[raw_value %in% c("NB", "NEW BUILDING")] <- "New Building"
  out[raw_value %in% c("DM", "DEMOLITION")] <- "Demolition"
  out[raw_value %in% c("A1", "A2", "A3", "ALT", "ALTERATION", "ALTERATION CO", "ALTERATION CO-NB", "ALT-CO")] <- "Alteration"
  out[raw_value %in% c("SG", "SIGN")] <- "Sign"
  out[is.na(out) & !is.na(raw_value) & raw_value != ""] <- str_to_title(str_to_lower(raw_value[is.na(out) & !is.na(raw_value) & raw_value != ""]))
  out
}

standardize_residential_flag <- function(x) {
  raw_value <- str_to_upper(str_squish(as.character(x)))
  out <- rep(NA_character_, length(raw_value))

  out[raw_value %in% c("Y", "YES", "TRUE", "1")] <- "YES"
  out[raw_value %in% c("N", "NO", "FALSE", "0")] <- "NO"
  out
}

build_dob_permit_identifier <- function(permit_number_or_si, job_number, job_doc_number, issuance_date, work_type, bbl, address) {
  issuance_date_chr <- ifelse(is.na(issuance_date), NA_character_, as.character(issuance_date))

  permit_id <- ifelse(
    !is.na(permit_number_or_si) & permit_number_or_si != "",
    paste("permit_si", permit_number_or_si, sep = "|"),
    NA_character_
  )

  job_doc_id <- ifelse(
    !is.na(job_number) & job_number != "" & !is.na(job_doc_number) & job_doc_number != "",
    paste("job_doc", job_number, job_doc_number, issuance_date_chr, work_type, bbl, address, sep = "|"),
    NA_character_
  )

  job_id <- ifelse(
    !is.na(job_number) & job_number != "",
    paste("job", job_number, issuance_date_chr, work_type, bbl, address, sep = "|"),
    NA_character_
  )

  coalesce_character(permit_id, job_doc_id, job_id)
}

build_dob_permit_match_location <- function(bbl, bin, address) {
  bbl_key <- ifelse(!is.na(bbl) & bbl != "", paste("bbl", bbl, sep = "|"), NA_character_)
  bin_key <- ifelse(!is.na(bin) & bin != "", paste("bin", bin, sep = "|"), NA_character_)
  address_key <- ifelse(!is.na(address) & address != "", paste("address", address, sep = "|"), NA_character_)

  coalesce_character(bbl_key, bin_key, address_key)
}

build_dob_permit_comparison_key <- function(job_number, job_doc_number, issuance_date, work_type, bbl, bin, address) {
  issuance_date_chr <- ifelse(is.na(issuance_date), NA_character_, as.character(issuance_date))
  work_type_chr <- ifelse(is.na(work_type), "", as.character(work_type))
  location_key <- build_dob_permit_match_location(bbl = bbl, bin = bin, address = address)

  job_doc_id <- ifelse(
    !is.na(job_number) & job_number != "" & !is.na(job_doc_number) & job_doc_number != "",
    paste("job_doc", job_number, job_doc_number, issuance_date_chr, work_type_chr, location_key, sep = "|"),
    NA_character_
  )

  job_id <- ifelse(
    !is.na(job_number) & job_number != "",
    paste("job", job_number, issuance_date_chr, work_type_chr, location_key, sep = "|"),
    NA_character_
  )

  coalesce_character(job_doc_id, job_id)
}

canonicalize_dob_permit_source <- function(raw_df, source_id, dataset_id, pull_date, source_raw_path) {
  borough_raw <- pick_permit_value(raw_df, c("borough"))
  house_number <- pick_permit_value(raw_df, c("house__", "house", "number"))
  street_name <- pick_permit_value(raw_df, c("street_name", "street"))
  block <- pick_permit_value(raw_df, c("block"))
  lot <- pick_permit_value(raw_df, c("lot"))
  bbl_raw <- pick_permit_value(raw_df, c("bbl"))
  bbl <- coalesce_character(bbl_raw, build_bbl(borough_raw, block, lot))
  bin <- pick_permit_value(raw_df, c("bin__", "bin"))
  address <- combine_address(house_number, street_name)
  issuance_date <- parse_mixed_date(pick_permit_value(raw_df, c("issuance_date")))
  record_year <- ifelse(is.na(issuance_date), NA_integer_, suppressWarnings(as.integer(format(issuance_date, "%Y"))))
  job_number <- pick_permit_value(raw_df, c("job__", "job"))
  job_doc_number <- pick_permit_value(raw_df, c("job_doc___", "job_doc"))
  permit_number_or_si <- pick_permit_value(raw_df, c("permit_si_no"))
  work_type <- pick_permit_value(raw_df, c("work_type"))
  job_type <- standardize_dob_permit_job_type(pick_permit_value(raw_df, c("job_type")))
  community_district <- standardize_community_district(borough_raw, pick_permit_value(raw_df, c("community_board")))
  council_district <- standardize_council_district(pick_permit_value(raw_df, c("gis_council_district", "council_district")))
  residential_flag <- standardize_residential_flag(pick_permit_value(raw_df, c("residential")))
  bldg_type <- pick_permit_value(raw_df, c("bldg_type"))
  latitude <- pick_permit_numeric(raw_df, c("gis_latitude", "latitude"))
  longitude <- pick_permit_numeric(raw_df, c("gis_longitude", "longitude"))
  permit_identifier <- build_dob_permit_identifier(
    permit_number_or_si = permit_number_or_si,
    job_number = job_number,
    job_doc_number = job_doc_number,
    issuance_date = issuance_date,
    work_type = work_type,
    bbl = bbl,
    address = address
  )

  permit_identifier[is.na(permit_identifier) | permit_identifier == ""] <- paste("row", seq_len(nrow(raw_df))[is.na(permit_identifier) | permit_identifier == ""], sep = "|")

  unresolved_reason <- rep(NA_character_, nrow(raw_df))
  unresolved_reason <- append_reason(unresolved_reason, is.na(issuance_date), "missing_issuance_date")
  unresolved_reason <- append_reason(unresolved_reason, is.na(bbl) & is.na(bin) & is.na(address), "missing_all_location_keys")
  unresolved_reason <- append_reason(
    unresolved_reason,
    !is.na(latitude) & !is.na(longitude) & (latitude < 40.0 | latitude > 41.5 | longitude < -75.5 | longitude > -73.0),
    "invalid_lat_lon_range"
  )

  tibble(
    source_id = source_id,
    dataset_id = dataset_id,
    pull_date = pull_date,
    source_raw_path = source_raw_path,
    borough = standardize_borough_name(borough_raw),
    block = block,
    lot = lot,
    bbl = bbl,
    bin = bin,
    house_number = house_number,
    street_name = street_name,
    address = address,
    issuance_date = issuance_date,
    record_year = record_year,
    job_number = job_number,
    job_doc_number = job_doc_number,
    permit_number_or_si = permit_number_or_si,
    permit_identifier = permit_identifier,
    job_type = job_type,
    work_type = work_type,
    community_district = community_district,
    council_district = council_district,
    residential_flag = residential_flag,
    bldg_type = bldg_type,
    latitude = latitude,
    longitude = longitude,
    unresolved_reason = unresolved_reason
  ) %>%
    group_by(permit_identifier) %>%
    mutate(source_record_sequence = row_number()) %>%
    ungroup() %>%
    mutate(
      source_record_id = paste(source_id, permit_identifier, source_record_sequence, sep = ":"),
      source_precedence = NA_character_,
      harmonization_reason = NA_character_
    ) %>%
    select(
      source_id,
      dataset_id,
      source_record_id,
      permit_identifier,
      job_number,
      job_doc_number,
      permit_number_or_si,
      issuance_date,
      record_year,
      job_type,
      work_type,
      borough,
      block,
      lot,
      bbl,
      bin,
      house_number,
      street_name,
      address,
      community_district,
      council_district,
      residential_flag,
      bldg_type,
      latitude,
      longitude,
      source_precedence,
      harmonization_reason,
      unresolved_reason,
      pull_date,
      source_raw_path
    )
}
