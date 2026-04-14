# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_dcp_housing_database/code")
# dcp_housing_database_raw_files_csv <- "../input/dcp_housing_database_raw_files.csv"
# out_index_csv <- "../output/dcp_housing_database_files.csv"
# out_qc_csv <- "../output/dcp_housing_database_qc.csv"
# out_city_year_parquet <- "../output/dcp_housing_database_city_year.parquet"
# out_borough_year_parquet <- "../output/dcp_housing_database_borough_year.parquet"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 5) {
  stop("Expected 5 arguments: dcp_housing_database_raw_files_csv out_index_csv out_qc_csv out_city_year_parquet out_borough_year_parquet")
}

dcp_housing_database_raw_files_csv <- args[1]
out_index_csv <- args[2]
out_qc_csv <- args[3]
out_city_year_parquet <- args[4]
out_borough_year_parquet <- args[5]

quarter_order_key <- function(x) {
  x <- toupper(as.character(x))
  year_part <- suppressWarnings(as.integer(str_extract(x, "^[0-9]{2}")))
  quarter_part <- suppressWarnings(as.integer(str_extract(x, "(?<=Q)[0-9]")))
  out <- rep(NA_real_, length(x))
  valid <- !is.na(year_part) & !is.na(quarter_part)
  out[valid] <- (2000 + year_part[valid]) * 10 + quarter_part[valid]
  out
}

raw_index <- read_csv(dcp_housing_database_raw_files_csv, show_col_types = FALSE, na = c("", "NA")) %>%
  filter(!is.na(raw_parquet_path), file.exists(raw_parquet_path)) %>%
  arrange(desc(quarter_order_key(vintage)), desc(vintage))

if (nrow(raw_index) == 0) {
  write_csv(tibble(), out_index_csv, na = "")
  write_csv(tibble(), out_qc_csv, na = "")
  write_parquet_if_changed(tibble(), out_city_year_parquet)
  write_parquet_if_changed(tibble(), out_borough_year_parquet)
  quit(save = "no")
}

index_rows <- list()
qc_rows <- list()
latest_city_year <- tibble()
latest_borough_year <- tibble()

for (i in seq_len(nrow(raw_index))) {
  row <- raw_index[i, ]
  raw_df <- read_parquet(row$raw_parquet_path) %>%
    as.data.frame() %>%
    as_tibble()

  staged_df <- tibble(
    source_id = row$source_id,
    release = row$vintage,
    job_number = suppressWarnings(as.character(raw_df$job_number)),
    job_type = as.character(raw_df$job_type),
    job_status = as.character(raw_df$job_status),
    permit_year = suppressWarnings(as.integer(raw_df$permityear)),
    completion_year = suppressWarnings(as.integer(raw_df$compltyear)),
    classa_init = suppressWarnings(as.numeric(raw_df$classainit)),
    classa_prop = suppressWarnings(as.numeric(raw_df$classaprop)),
    classa_net = suppressWarnings(as.numeric(raw_df$classanet)),
    units_co = suppressWarnings(as.numeric(raw_df$units_co)),
    borough_code = standardize_borough_code(raw_df$boro),
    borough_name = standardize_borough_name(raw_df$boro),
    bin = as.character(raw_df$bin),
    bbl = as.character(raw_df$bbl),
    house_number = as.character(raw_df$addressnum),
    street_name = as.character(raw_df$addressst),
    address = combine_address(raw_df$addressnum, raw_df$addressst),
    community_district = standardize_community_district(raw_df$boro, raw_df$commntydst),
    council_district = standardize_council_district(raw_df$councildst),
    date_filed = parse_mixed_date(raw_df$datefiled),
    date_permit = parse_mixed_date(raw_df$datepermit),
    date_updated = parse_mixed_date(raw_df$datelstupd),
    date_completed = parse_mixed_date(raw_df$datecomplt),
    geom_source = as.character(raw_df$geomsource),
    dcp_edited = as.character(raw_df$dcpedited),
    version = as.character(raw_df$version),
    source_raw_path = row$raw_path
  )

  out_parquet_local <- file.path("..", "output", paste0("dcp_housing_database_project_level_", sanitize_file_stub(row$vintage), ".parquet"))
  out_parquet <- file.path("..", "..", "stage_dcp_housing_database", "output", basename(out_parquet_local))
  write_parquet_if_changed(staged_df, out_parquet_local)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    vintage = row$vintage,
    raw_path = row$raw_path,
    raw_parquet_path = row$raw_parquet_path,
    parquet_path = out_parquet,
    status = "staged"
  )

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    vintage = row$vintage,
    row_count = nrow(staged_df),
    nonmissing_bbl_share = mean(!is.na(staged_df$bbl) & staged_df$bbl != ""),
    nonmissing_bin_share = mean(!is.na(staged_df$bin) & staged_df$bin != ""),
    nonmissing_address_share = mean(!is.na(staged_df$address)),
    nonmissing_cd_share = mean(!is.na(staged_df$community_district)),
    nonmissing_council_share = mean(!is.na(staged_df$council_district)),
    permit_year_start = min(staged_df$permit_year, na.rm = TRUE),
    permit_year_end = max(staged_df$permit_year, na.rm = TRUE),
    completion_year_start = min(staged_df$completion_year, na.rm = TRUE),
    completion_year_end = max(staged_df$completion_year, na.rm = TRUE),
    status = "staged"
  )

  if (i == 1) {
    latest_city_year <- staged_df %>%
      filter(!is.na(permit_year)) %>%
      group_by(permit_year) %>%
      summarise(
        release = first(release),
        permit_year = first(permit_year),
        classa_init = sum(classa_init, na.rm = TRUE),
        classa_prop = sum(classa_prop, na.rm = TRUE),
        classa_net = sum(classa_net, na.rm = TRUE),
        units_co = sum(units_co, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      rename(year = permit_year)

    latest_borough_year <- staged_df %>%
      filter(!is.na(permit_year), !is.na(borough_name)) %>%
      group_by(permit_year, borough_name) %>%
      summarise(
        release = first(release),
        classa_init = sum(classa_init, na.rm = TRUE),
        classa_prop = sum(classa_prop, na.rm = TRUE),
        classa_net = sum(classa_net, na.rm = TRUE),
        units_co = sum(units_co, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      rename(year = permit_year)
  }
}

write_parquet_if_changed(latest_city_year, out_city_year_parquet)
write_parquet_if_changed(latest_borough_year, out_borough_year_parquet)
write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote DCP Housing Database staging outputs to", dirname(out_index_csv), "\n")
