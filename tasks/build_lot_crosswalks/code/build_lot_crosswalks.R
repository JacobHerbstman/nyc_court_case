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

parquet_rows <- mappluto_index |> filter(!is.na(parquet_path), file.exists(parquet_path))

if (nrow(parquet_rows) == 0) {
  write_csv(tibble(), out_crosswalk_csv, na = "")
  write_csv(tibble(status = "no_mappluto_parquet_available"), out_qc_csv, na = "")
  quit(save = "no")
}

parquet_rows <- parquet_rows |>
  mutate(vintage_rank = if_else(vintage == "current", 999999L, suppressWarnings(as.integer(str_extract(vintage, "[0-9]{4}"))))) |>
  arrange(desc(vintage_rank))

mappluto_latest <- read_parquet(parquet_rows$parquet_path[1]) |>
  as_tibble() |>
  transmute(
    bbl = as.character(bbl),
    address = as.character(address),
    current_cd = as.character(cd),
    current_council = as.character(council)
  ) |>
  distinct()

dob_parquets <- dob_index |> filter(!is.na(parquet_path), file.exists(parquet_path))
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
  left_join(mappluto_latest, by = "bbl") |>
  left_join(
    mappluto_latest |> select(address, address_cd = current_cd, address_council = current_council),
    by = "address"
  ) |>
  mutate(
    current_cd = coalesce(current_cd, address_cd),
    current_council = coalesce(current_council, address_council)
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
