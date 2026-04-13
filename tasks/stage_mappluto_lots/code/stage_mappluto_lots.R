# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_mappluto_lots/code")
# mappluto_raw_files_csv <- "../input/mappluto_raw_files.csv"
# out_index_csv <- "../output/mappluto_lot_files.csv"
# out_qc_csv <- "../output/mappluto_lot_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: mappluto_raw_files_csv out_index_csv out_qc_csv")
}

mappluto_raw_files_csv <- args[1]
out_index_csv <- args[2]
out_qc_csv <- args[3]

mappluto_raw_files <- read_csv(mappluto_raw_files_csv, show_col_types = FALSE, na = c("", "NA"))

jia_codes <- c(164L, 226L, 227L, 228L, 355L, 356L, 480L, 481L, 482L, 483L, 484L, 595L)
min_plausible_yearbuilt <- 1800L

normalize_year_field <- function(x) {
  x_int <- suppressWarnings(as.integer(x))
  x_int[x_int == 0L] <- NA_integer_
  x_int[x_int < min_plausible_yearbuilt] <- NA_integer_
  as.character(x_int)
}

safe_min_int <- function(x) {
  x <- suppressWarnings(as.integer(x))
  if (all(is.na(x))) {
    return(NA_integer_)
  }
  min(x, na.rm = TRUE)
}

safe_max_int <- function(x) {
  x <- suppressWarnings(as.integer(x))
  if (all(is.na(x))) {
    return(NA_integer_)
  }
  max(x, na.rm = TRUE)
}

available_rows <- mappluto_raw_files[!is.na(mappluto_raw_files$raw_parquet_path) & file.exists(mappluto_raw_files$raw_parquet_path), ]

if (nrow(available_rows) == 0) {
  write_csv(tibble(), out_index_csv, na = "")
  write_csv(tibble(), out_qc_csv, na = "")
  quit(save = "no")
}

available_rows <- available_rows |>
  mutate(
    raw_path = as.character(raw_path),
    raw_parquet_path = as.character(raw_parquet_path),
    vintage = as.character(vintage)
  )

index_rows <- list()
qc_rows <- list()

for (i in seq_len(nrow(available_rows))) {
  row <- available_rows[i, ]
  vintage_stub <- sanitize_file_stub(paste(row$source_id, row$vintage, sep = "_"))
  out_parquet_local <- file.path("..", "output", paste0(vintage_stub, ".parquet"))
  out_parquet <- file.path("..", "..", "stage_mappluto_lots", "output", paste0(vintage_stub, ".parquet"))

  lot_table <- read_parquet(row$raw_parquet_path) |>
    as.data.frame() |>
    as_tibble()

  lot_table$yearbuilt <- normalize_year_field(lot_table$yearbuilt)
  lot_table$yearalter1 <- normalize_year_field(lot_table$yearalter1)
  lot_table$yearalter2 <- normalize_year_field(lot_table$yearalter2)
  lot_table$is_joint_interest_area <- suppressWarnings(as.integer(lot_table$cd)) %in% jia_codes

  write_parquet_if_changed(lot_table, out_parquet_local)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    vintage = row$vintage,
    raw_path = row$raw_path,
    raw_parquet_path = row$raw_parquet_path,
    parquet_path = out_parquet,
    file_role = row$file_role
  )

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    vintage = row$vintage,
    row_count = nrow(lot_table),
    nonmissing_bbl_share = mean(!is.na(lot_table$bbl)),
    nonmissing_cd_share = mean(!is.na(lot_table$cd)),
    nonmissing_council_share = mean(!is.na(lot_table$council)),
    nonmissing_unitsres_share = mean(!is.na(lot_table$unitsres)),
    nonmissing_yearbuilt_share = mean(!is.na(lot_table$yearbuilt)),
    ordinary_cd_rows = sum(!lot_table$is_joint_interest_area, na.rm = TRUE),
    joint_interest_area_rows = sum(lot_table$is_joint_interest_area, na.rm = TRUE),
    min_yearbuilt = safe_min_int(lot_table$yearbuilt),
    max_yearbuilt = safe_max_int(lot_table$yearbuilt)
  )
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote MapPLUTO staging outputs to", dirname(out_index_csv), "\n")
