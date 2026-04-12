# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_dob_open_data/code")
# source_catalog_csv <- "../input/source_catalog.csv"
# out_index_csv <- "../output/dob_open_data_files.csv"
# out_qc_csv <- "../output/dob_open_data_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(lubridate)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: source_catalog_csv out_index_csv out_qc_csv")
}

source_catalog_csv <- args[1]
out_index_csv <- args[2]
out_qc_csv <- args[3]

source_catalog <- read_csv(source_catalog_csv, show_col_types = FALSE, na = c("", "NA"))
dob_rows <- source_catalog |> filter(str_detect(source_id, "^dob_"))

parse_mixed_date <- function(x) {
  parsed <- suppressWarnings(parse_date_time(
    as.character(x),
    orders = c("ymd HMS", "ymd HM", "ymd", "mdy HMS", "mdy HM", "mdy", "m/d/Y", "m/d/Y H:M:S"),
    tz = "America/New_York"
  ))
  as.Date(parsed)
}

safe_min_date <- function(x) {
  if (all(is.na(x))) {
    return(NA_character_)
  }
  as.character(min(x, na.rm = TRUE))
}

safe_max_date <- function(x) {
  if (all(is.na(x))) {
    return(NA_character_)
  }
  as.character(max(x, na.rm = TRUE))
}

index_rows <- list()
qc_rows <- list()
pull_date <- format(Sys.Date(), "%Y%m%d")

for (i in seq_len(nrow(dob_rows))) {
  row <- dob_rows[i, ]
  raw_dir <- file.path("..", "..", "..", "data_raw", row$source_id, pull_date)
  raw_path <- file.path(raw_dir, row$expected_filename)

  status <- if (file.exists(raw_path)) {
    "already_present"
  } else if (looks_downloadable(row$official_url)) {
    download_with_status(row$official_url, raw_path)
  } else {
    "non_downloadable_url"
  }

  if (!file.exists(raw_path)) {
    index_rows[[i]] <- tibble(
      source_id = row$source_id,
      raw_path = raw_path,
      parquet_path = NA_character_,
      pull_date = pull_date,
      status = status
    )

    qc_rows[[i]] <- tibble(
      source_id = row$source_id,
      status = status,
      row_count = NA_real_,
      start_date = NA_character_,
      end_date = NA_character_,
      nonmissing_bbl_share = NA_real_,
      nonmissing_bin_share = NA_real_,
      nonmissing_address_share = NA_real_,
      nonmissing_cd_share = NA_real_,
      nonmissing_council_share = NA_real_
    )
    next
  }

  dob_df <- read_csv(raw_path, show_col_types = FALSE, guess_max = 50000)
  names(dob_df) <- normalize_names(names(dob_df))

  staged_df <- tibble(
    source_id = row$source_id,
    source_record_id = pick_first_existing(dob_df, c("job_filing_number", "job_number", "job", "job__", "id")),
    job_number = pick_first_existing(dob_df, c("job_number", "job__", "job_filing_number")),
    doc_number = pick_first_existing(dob_df, c("doc__", "doc_number")),
    borough = pick_first_existing(dob_df, c("borough")),
    house_number = pick_first_existing(dob_df, c("house_no", "house__", "house_number")),
    street_name = pick_first_existing(dob_df, c("street_name")),
    block = pick_first_existing(dob_df, c("block")),
    lot = pick_first_existing(dob_df, c("lot")),
    bbl = pick_first_existing(dob_df, c("bbl")),
    bin = pick_first_existing(dob_df, c("bin", "bin__", "bin_number", "gis_bin")),
    job_type = pick_first_existing(dob_df, c("job_type")),
    record_status = pick_first_existing(dob_df, c("filing_status", "job_status_descrp", "job_status", "status")),
    record_date = pick_first_existing(dob_df, c("filing_date", "pre__filing_date", "c_o_issue_date", "certificate_of_occupancy_date")),
    approved_date = pick_first_existing(dob_df, c("approved_date", "approved")),
    permit_or_issue_date = pick_first_existing(dob_df, c("fully_permitted", "c_o_issue_date", "certificate_of_occupancy_date")),
    signoff_date = pick_first_existing(dob_df, c("signoff_date")),
    existing_dwelling_units = pick_first_existing(dob_df, c("existing_dwelling_units", "existingno_of_dwelling_units")),
    proposed_dwelling_units = pick_first_existing(dob_df, c("proposed_dwelling_units", "number_dwelling_units")),
    community_district = pick_first_existing(dob_df, c("community___board", "community_districts", "community_district", "community_board")),
    council_district = pick_first_existing(dob_df, c("gis_council_district", "city_council_districts", "council_district"))
  )

  staged_df$bbl[is.na(staged_df$bbl)] <- build_bbl(staged_df$borough, staged_df$block, staged_df$lot)[is.na(staged_df$bbl)]
  staged_df$address <- combine_address(staged_df$house_number, staged_df$street_name)

  out_parquet_local <- file.path("..", "output", paste0(row$source_id, ".parquet"))
  out_parquet <- file.path("..", "..", "fetch_dob_open_data", "output", paste0(row$source_id, ".parquet"))
  write_parquet_if_changed(staged_df, out_parquet_local)

  parsed_dates <- parse_mixed_date(staged_df$record_date)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    raw_path = raw_path,
    parquet_path = out_parquet,
    pull_date = pull_date,
    status = status
  )

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    status = status,
    row_count = nrow(staged_df),
    start_date = safe_min_date(parsed_dates),
    end_date = safe_max_date(parsed_dates),
    nonmissing_bbl_share = mean(!is.na(staged_df$bbl)),
    nonmissing_bin_share = mean(!is.na(staged_df$bin)),
    nonmissing_address_share = mean(!is.na(staged_df$address)),
    nonmissing_cd_share = mean(!is.na(staged_df$community_district)),
    nonmissing_council_share = mean(!is.na(staged_df$council_district))
  )
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote DOB Open Data outputs to", dirname(out_index_csv), "\n")
