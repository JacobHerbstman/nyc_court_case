suppressPackageStartupMessages({
  library(arrow)
  library(readr)
  library(stringr)
  library(tibble)
})

normalize_names <- function(x) {
  x |>
    tolower() |>
    str_replace_all("[^a-z0-9]+", "_") |>
    str_replace_all("^_|_$", "")
}

pick_first_existing <- function(df, candidates) {
  hits <- candidates[candidates %in% names(df)]
  if (length(hits) == 0) {
    return(rep(NA_character_, nrow(df)))
  }
  as.character(df[[hits[1]]])
}

build_bbl <- function(borough, block, lot) {
  borough_num <- suppressWarnings(as.integer(str_extract(as.character(borough), "^[0-9]+")))
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
  if (is.na(url) || is.na(url) || str_trim(url) == "") {
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
  x |>
    tolower() |>
    str_replace_all("[^a-z0-9]+", "_") |>
    str_replace_all("^_|_$", "")
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
