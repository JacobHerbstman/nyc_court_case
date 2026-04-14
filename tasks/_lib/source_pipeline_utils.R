suppressPackageStartupMessages({
  library(arrow)
  library(lubridate)
  library(readr)
  library(stringr)
  library(tibble)
})

normalize_names <- function(x) {
  x <- tolower(x)
  x <- str_replace_all(x, "[^a-z0-9]+", "_")
  x <- str_replace_all(x, "^_|_$", "")
  x
}

pick_first_existing <- function(df, candidates) {
  hits <- candidates[candidates %in% names(df)]

  if (length(hits) == 0) {
    return(rep(NA_character_, nrow(df)))
  }

  as.character(df[[hits[1]]])
}

coalesce_character <- function(...) {
  values <- list(...)
  if (length(values) == 0) {
    return(character())
  }

  out <- as.character(values[[1]])
  if (length(values) == 1) {
    return(out)
  }

  for (i in seq(2, length(values))) {
    next_value <- as.character(values[[i]])
    replace_flag <- is.na(out) | out == ""
    out[replace_flag] <- next_value[replace_flag]
  }

  out[out == ""] <- NA_character_
  out
}

standardize_borough_code <- function(x) {
  raw_value <- str_to_upper(str_squish(as.character(x)))
  out <- rep(NA_character_, length(raw_value))

  out[raw_value %in% c("1", "MN", "MANHATTAN")] <- "1"
  out[raw_value %in% c("2", "BX", "BRONX")] <- "2"
  out[raw_value %in% c("3", "BK", "K", "BROOKLYN")] <- "3"
  out[raw_value %in% c("4", "QN", "Q", "QUEENS")] <- "4"
  out[raw_value %in% c("5", "SI", "R", "STATEN ISLAND")] <- "5"

  numeric_hits <- suppressWarnings(as.integer(str_extract(raw_value, "^[1-5]$")))
  out[!is.na(numeric_hits)] <- as.character(numeric_hits[!is.na(numeric_hits)])
  out
}

standardize_borough_name <- function(x) {
  borough_code <- standardize_borough_code(x)
  out <- rep(NA_character_, length(borough_code))

  out[borough_code == "1"] <- "Manhattan"
  out[borough_code == "2"] <- "Bronx"
  out[borough_code == "3"] <- "Brooklyn"
  out[borough_code == "4"] <- "Queens"
  out[borough_code == "5"] <- "Staten Island"
  out
}

build_bbl <- function(borough, block, lot) {
  borough_num <- suppressWarnings(as.integer(standardize_borough_code(borough)))
  block_num <- suppressWarnings(as.integer(as.character(block)))
  lot_num <- suppressWarnings(as.integer(as.character(lot)))

  valid <- !is.na(borough_num) & !is.na(block_num) & !is.na(lot_num)
  out <- rep(NA_character_, length(valid))
  out[valid] <- sprintf("%01d%05d%04d", borough_num[valid], block_num[valid], lot_num[valid])
  out
}

combine_address <- function(house_number, street_name) {
  house_number <- str_squish(ifelse(is.na(house_number), "", as.character(house_number)))
  street_name <- str_squish(ifelse(is.na(street_name), "", as.character(street_name)))
  address <- str_squish(str_trim(paste(house_number, street_name)))
  address[address == ""] <- NA_character_
  address
}

standardize_community_district <- function(borough, raw_cd) {
  borough_num <- suppressWarnings(as.integer(standardize_borough_code(borough)))
  raw_char <- str_to_upper(str_squish(as.character(raw_cd)))
  raw_num <- suppressWarnings(as.integer(str_extract(raw_char, "[0-9]{1,3}")))
  out <- rep(NA_integer_, length(raw_num))

  out[!is.na(raw_num) & raw_num >= 101 & raw_num <= 595] <- raw_num[!is.na(raw_num) & raw_num >= 101 & raw_num <= 595]

  small_flag <- !is.na(raw_num) & raw_num >= 1 & raw_num <= 18 & !is.na(borough_num)
  out[small_flag] <- borough_num[small_flag] * 100L + raw_num[small_flag]

  out
}

standardize_council_district <- function(raw_value) {
  council_num <- suppressWarnings(as.integer(str_extract(as.character(raw_value), "[0-9]{1,2}")))
  council_num[!is.na(council_num) & (council_num < 1 | council_num > 51)] <- NA_integer_
  council_num
}

parse_mixed_date <- function(x) {
  raw_value <- str_squish(as.character(x))
  raw_value[raw_value == ""] <- NA_character_
  out <- rep(as.Date(NA), length(raw_value))

  ymd_flag <- !is.na(raw_value) & str_detect(raw_value, "^[0-9]{4}[-/][0-9]{1,2}[-/][0-9]{1,2}")
  mdy_flag <- !is.na(raw_value) & str_detect(raw_value, "^[0-9]{1,2}[-/][0-9]{1,2}[-/][0-9]{2,4}")

  if (any(ymd_flag)) {
    out[ymd_flag] <- suppressWarnings(as.Date(parse_date_time(
      raw_value[ymd_flag],
      orders = c("ymd HMS", "ymd HM", "ymd"),
      tz = "America/New_York"
    )))
  }

  if (any(mdy_flag)) {
    out[mdy_flag] <- suppressWarnings(as.Date(parse_date_time(
      raw_value[mdy_flag],
      orders = c("mdy HMS", "mdy HM", "mdy"),
      tz = "America/New_York"
    )))
  }

  remaining_flag <- is.na(out) & !is.na(raw_value)

  if (any(remaining_flag)) {
    out[remaining_flag] <- suppressWarnings(as.Date(parse_date_time(
      raw_value[remaining_flag],
      orders = c(
        "ymd HMS", "ymd HM", "ymd",
        "mdy HMS", "mdy HM", "mdy",
        "Ymd HMS", "Ymd HM", "Ymd"
      ),
      tz = "America/New_York"
    )))
  }

  out[!is.na(out) & out > Sys.Date() + 365] <- NA
  out
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

compute_sha256 <- function(path) {
  if (!file.exists(path)) {
    return(NA_character_)
  }

  out <- system2("shasum", c("-a", "256", path), stdout = TRUE, stderr = FALSE)
  if (length(out) == 0) {
    return(NA_character_)
  }

  str_split_fixed(out[1], "\\s+", 2)[1, 1]
}

copy_if_changed <- function(temp_path, out_path) {
  dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)

  if (file.exists(out_path)) {
    old_hash <- unname(tools::md5sum(out_path))
    new_hash <- unname(tools::md5sum(temp_path))
    if (!is.na(old_hash) && !is.na(new_hash) && identical(old_hash, new_hash)) {
      unlink(temp_path)
      return(invisible(FALSE))
    }
    unlink(out_path)
  }

  file.rename(temp_path, out_path)
  invisible(TRUE)
}

write_csv_if_changed <- function(df, out_path) {
  temp_path <- tempfile(fileext = ".csv")
  write_csv(df, temp_path, na = "")
  copy_if_changed(temp_path, out_path)
}

write_parquet_if_changed <- function(df, out_path) {
  temp_path <- tempfile(fileext = ".parquet")
  write_parquet(df, temp_path)
  copy_if_changed(temp_path, out_path)
}

looks_downloadable <- function(url) {
  if (is.na(url) || str_trim(url) == "") {
    return(FALSE)
  }

  str_detect(
    url,
    "(rows\\.(csv|json)|accessType=DOWNLOAD|\\.zip($|\\?)|\\.csv($|\\?)|\\.geojson($|\\?)|\\.shp($|\\?)|\\.xlsx($|\\?)|\\.xls($|\\?)|\\.txt($|\\?))"
  )
}

download_with_status <- function(url, dest_path) {
  dir.create(dirname(dest_path), recursive = TRUE, showWarnings = FALSE)

  tryCatch(
    {
      download.file(url, destfile = dest_path, mode = "wb", quiet = TRUE)
      "downloaded"
    },
    error = function(e) {
      message(e$message)
      "download_failed"
    }
  )
}

collect_raw_files <- function(source_id) {
  raw_dir <- file.path("..", "..", "..", "data_raw", source_id)
  if (!dir.exists(raw_dir)) {
    return(character())
  }

  list.files(raw_dir, recursive = TRUE, full.names = TRUE, all.files = FALSE)
}

sanitize_file_stub <- function(x) {
  x <- tolower(x)
  x <- str_replace_all(x, "[^a-z0-9]+", "_")
  x <- str_replace_all(x, "^_|_$", "")
  x
}

release_order_key <- function(x) {
  x <- as.character(x)
  base_year <- suppressWarnings(as.integer(str_extract(x, "^[0-9]{2}")))
  version <- suppressWarnings(as.numeric(str_replace(str_extract(x, "(?<=v)[0-9]+(\\.[0-9]+)?"), "_", ".")))
  beta_flag <- str_detect(tolower(x), "beta")

  out <- rep(NA_real_, length(x))
  valid <- !is.na(base_year)
  out[valid] <- (2000 + base_year[valid]) * 100 + ifelse(is.na(version[valid]), 0, version[valid] * 10)
  out[valid & beta_flag] <- out[valid & beta_flag] - 0.5
  out
}
