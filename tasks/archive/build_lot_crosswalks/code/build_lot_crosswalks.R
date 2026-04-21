# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_lot_crosswalks/code")
# mappluto_index_csv <- "../input/mappluto_lot_files.csv"
# dob_index_csv <- "../input/dob_open_data_files.csv"
# out_crosswalk_csv <- "../output/lot_identifier_crosswalk.csv"
# out_qc_csv <- "../output/lot_identifier_crosswalk_qc.csv"

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
  stop("Expected 4 arguments: mappluto_index_csv dob_index_csv out_crosswalk_csv out_qc_csv")
}

mappluto_index_csv <- args[1]
dob_index_csv <- args[2]
out_crosswalk_csv <- args[3]
out_qc_csv <- args[4]

mappluto_index <- read_csv(mappluto_index_csv, show_col_types = FALSE, na = c("", "NA"))
dob_index <- read_csv(dob_index_csv, show_col_types = FALSE, na = c("", "NA"))

if (!"parquet_path" %in% names(mappluto_index)) {
  write_csv(tibble(), out_crosswalk_csv, na = "")
  write_csv(tibble(status = "no_mappluto_parquet_available"), out_qc_csv, na = "")
  quit(save = "no")
}

parquet_rows <- mappluto_index[!is.na(mappluto_index$parquet_path) & file.exists(mappluto_index$parquet_path), ]

if (nrow(parquet_rows) == 0) {
  write_csv(tibble(), out_crosswalk_csv, na = "")
  write_csv(tibble(status = "no_mappluto_parquet_available"), out_qc_csv, na = "")
  quit(save = "no")
}

parquet_rows <- parquet_rows |>
  mutate(
    is_current_release = source_id == "dcp_mappluto_current",
    vintage_rank = release_order_key(vintage)
  ) |>
  arrange(desc(is_current_release), desc(vintage_rank), desc(vintage))

first_nonmissing <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & x != ""]
  if (length(x) == 0) {
    return(NA_character_)
  }
  x[1]
}

mappluto_latest <- read_parquet(parquet_rows$parquet_path[1]) |>
  as_tibble() |>
  transmute(
    bbl = as.character(bbl),
    address = as.character(address),
    current_cd = as.character(cd),
    current_council = as.character(council)
  ) |>
  distinct()

mappluto_by_bbl <- mappluto_latest |>
  filter(!is.na(bbl), bbl != "") |>
  group_by(bbl) |>
  summarise(
    mappluto_address = first_nonmissing(address),
    bbl_cd = first_nonmissing(current_cd),
    bbl_council = first_nonmissing(current_council),
    .groups = "drop"
  )

mappluto_by_address <- mappluto_latest |>
  filter(!is.na(address), address != "") |>
  group_by(address) |>
  summarise(
    address_cd = first_nonmissing(current_cd),
    address_council = first_nonmissing(current_council),
    .groups = "drop"
  )

dob_parquets <- dob_index[!is.na(dob_index$parquet_path) & file.exists(dob_index$parquet_path), ]
dob_ids <- if (nrow(dob_parquets) == 0) {
  tibble(bbl = character(), bin = character(), address = character(), source_id = character())
} else {
  bind_rows(lapply(dob_parquets$parquet_path, function(path) {
    read_parquet(path) |>
      as_tibble() |>
      transmute(
        bbl = as.character(bbl),
        bin = as.character(bin),
        address = as.character(address),
        source_id = as.character(source_id)
      )
  })) |>
    distinct()
}

crosswalk <- dob_ids |>
  left_join(mappluto_by_bbl, by = "bbl") |>
  left_join(mappluto_by_address, by = "address") |>
  mutate(
    current_cd = coalesce(bbl_cd, address_cd),
    current_council = coalesce(bbl_council, address_council),
    address = coalesce(address, mappluto_address)
  ) |>
  select(bbl, bin, address, current_cd, current_council, source_id) |>
  distinct()

mappluto_only <- mappluto_latest |>
  filter(!bbl %in% crosswalk$bbl) |>
  transmute(
    bbl = bbl,
    bin = NA_character_,
    address = address,
    current_cd = current_cd,
    current_council = current_council,
    source_id = "dcp_mappluto_current"
  )

crosswalk <- bind_rows(crosswalk, mappluto_only) |> distinct()

write_csv(crosswalk, out_crosswalk_csv, na = "")
write_csv(
  tibble(
    row_count = nrow(crosswalk),
    nonmissing_bbl_share = mean(!is.na(crosswalk$bbl)),
    nonmissing_bin_share = mean(!is.na(crosswalk$bin)),
    nonmissing_address_share = mean(!is.na(crosswalk$address)),
    nonmissing_cd_share = mean(!is.na(crosswalk$current_cd)),
    nonmissing_council_share = mean(!is.na(crosswalk$current_council))
  ),
  out_qc_csv,
  na = ""
)

cat("Wrote lot crosswalk outputs to", dirname(out_crosswalk_csv), "\n")
