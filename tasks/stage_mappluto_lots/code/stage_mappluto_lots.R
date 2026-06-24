# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_mappluto_lots/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

mappluto_raw_files <- read_csv("../input/mappluto_raw_files.csv", show_col_types = FALSE, na = c("", "NA"))

required_index_columns <- c("status", "raw_parquet_path")
missing_index_columns <- setdiff(required_index_columns, names(mappluto_raw_files))

if (length(missing_index_columns) > 0) {
  stop("MapPLUTO raw index is missing required columns: ", paste(missing_index_columns, collapse = ", "))
}

jia_codes <- c(164L, 226L, 227L, 228L, 355L, 356L, 480L, 481L, 482L, 483L, 484L, 595L)
min_plausible_yearbuilt <- 1800L

normalize_text_field <- function(x) {
  out <- trimws(as.character(x))
  out[out %in% c("", "NA", "N/A", "NULL")] <- NA_character_
  out
}

normalize_integer_field <- function(x) {
  suppressWarnings(as.integer(normalize_text_field(x)))
}

normalize_numeric_field <- function(x) {
  suppressWarnings(as.numeric(normalize_text_field(x)))
}

normalize_year_field <- function(x) {
  x_int <- normalize_integer_field(x)
  x_int[x_int == 0L] <- NA_integer_
  x_int[x_int < min_plausible_yearbuilt] <- NA_integer_
  x_int
}

available_rows <- mappluto_raw_files |>
  mutate(
    status = as.character(status),
    raw_parquet_path = as.character(raw_parquet_path)
  ) |>
  filter(status == "loaded", !is.na(raw_parquet_path), file.exists(raw_parquet_path))

if (nrow(available_rows) == 0) {
  write_csv(tibble(), "../output/mappluto_lot_files.csv", na = "")
  quit(save = "no")
}

available_rows <- available_rows |>
  mutate(
    raw_path = as.character(raw_path),
    raw_parquet_path = as.character(raw_parquet_path),
    vintage = as.character(vintage)
  )

index_rows <- list()

for (i in seq_len(nrow(available_rows))) {
  row <- available_rows[i, ]
  vintage_stub <- sanitize_file_stub(paste(row$source_id, row$vintage, sep = "_"))
  out_parquet_local <- file.path("..", "output", paste0(vintage_stub, ".parquet"))
  out_parquet <- file.path("..", "..", "stage_mappluto_lots", "output", paste0(vintage_stub, ".parquet"))

  lot_table <- read_parquet(row$raw_parquet_path) |>
    as.data.frame() |>
    as_tibble() |>
    mutate(
      source_id = as.character(source_id),
      source_vintage = as.character(source_vintage),
      source_raw_path = as.character(source_raw_path),
      borough = standardize_borough_code(borough),
      block = normalize_integer_field(block),
      lot = normalize_integer_field(lot),
      bbl = coalesce_character(normalize_text_field(bbl), build_bbl(borough, block, lot)),
      address = normalize_text_field(address),
      cd = normalize_integer_field(cd),
      zipcode = normalize_text_field(zipcode),
      ct2010 = normalize_text_field(ct2010),
      cb2010 = normalize_text_field(cb2010),
      schooldist = normalize_integer_field(schooldist),
      council = normalize_integer_field(council),
      zonedist1 = normalize_text_field(zonedist1),
      zonedist2 = normalize_text_field(zonedist2),
      zonedist3 = normalize_text_field(zonedist3),
      zonedist4 = normalize_text_field(zonedist4),
      overlay1 = normalize_text_field(overlay1),
      overlay2 = normalize_text_field(overlay2),
      spdist1 = normalize_text_field(spdist1),
      spdist2 = normalize_text_field(spdist2),
      spdist3 = normalize_text_field(spdist3),
      ltdheight = normalize_text_field(ltdheight),
      splitzone = normalize_text_field(splitzone),
      zonemap = normalize_text_field(zonemap),
      zmcode = normalize_text_field(zmcode),
      lotarea = normalize_numeric_field(lotarea),
      bldgarea = normalize_numeric_field(bldgarea),
      comarea = normalize_numeric_field(comarea),
      resarea = normalize_numeric_field(resarea),
      officearea = normalize_numeric_field(officearea),
      retailarea = normalize_numeric_field(retailarea),
      garagearea = normalize_numeric_field(garagearea),
      strgearea = normalize_numeric_field(strgearea),
      factryarea = normalize_numeric_field(factryarea),
      otherarea = normalize_numeric_field(otherarea),
      areasource = normalize_text_field(areasource),
      numbldgs = normalize_integer_field(numbldgs),
      numfloors = normalize_numeric_field(numfloors),
      unitsres = normalize_numeric_field(unitsres),
      unitstotal = normalize_numeric_field(unitstotal),
      lotfront = normalize_numeric_field(lotfront),
      lotdepth = normalize_numeric_field(lotdepth),
      bldgfront = normalize_numeric_field(bldgfront),
      bldgdepth = normalize_numeric_field(bldgdepth),
      yearbuilt = normalize_year_field(yearbuilt),
      yearalter1 = normalize_year_field(yearalter1),
      yearalter2 = normalize_year_field(yearalter2),
      appdate = parse_mixed_date(appdate),
      assessland = normalize_numeric_field(assessland),
      assesstot = normalize_numeric_field(assesstot),
      exempttot = normalize_numeric_field(exempttot),
      histdist = normalize_text_field(histdist),
      landmark = normalize_text_field(landmark),
      builtfar = normalize_numeric_field(builtfar),
      residfar = normalize_numeric_field(residfar),
      commfar = normalize_numeric_field(commfar),
      facilfar = normalize_numeric_field(facilfar),
      firm07_flag = normalize_text_field(firm07_flag),
      pfirm15_flag = normalize_text_field(pfirm15_flag),
      landuse = normalize_text_field(landuse),
      bldgclass = normalize_text_field(bldgclass),
      is_joint_interest_area = cd %in% jia_codes
    )

  write_parquet_if_changed(lot_table, out_parquet_local)
  out_parquet_info <- file.info(out_parquet_local)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    vintage = row$vintage,
    raw_path = row$raw_path,
    raw_parquet_path = row$raw_parquet_path,
    parquet_path = out_parquet,
    parquet_size_bytes = as.numeric(out_parquet_info$size),
    parquet_mtime = format(out_parquet_info$mtime, "%Y-%m-%d %H:%M:%OS6 %z"),
    file_role = row$file_role,
    raw_file_release = if ("raw_file_release" %in% names(row)) row$raw_file_release else NA_character_,
    fetch_status = if ("fetch_status" %in% names(row)) row$fetch_status else NA_character_,
    raw_zip_valid = if ("raw_zip_valid" %in% names(row)) row$raw_zip_valid else NA,
    raw_status = row$status
  )
}

write_csv(bind_rows(index_rows), "../output/mappluto_lot_files.csv", na = "")
cat("Wrote MapPLUTO staging outputs to ../output\n")
