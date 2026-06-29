suppressPackageStartupMessages({
  library(stringr)
  library(tibble)
})

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
  x_int[x_int < 1800L] <- NA_integer_
  x_int
}

read_mappluto_dbf <- function(raw_path) {
  dbf_entry <- unzip(raw_path, list = TRUE) |>
    as_tibble() |>
    dplyr::filter(str_detect(tolower(Name), "[.]dbf$")) |>
    dplyr::arrange(Name) |>
    dplyr::pull(Name) |>
    dplyr::first()

  if (is.na(dbf_entry) || !nzchar(dbf_entry)) {
    stop("No DBF found in ", raw_path)
  }

  temp_dir <- tempfile(pattern = "mappluto_dbf_")
  dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  suppressWarnings(unzip(raw_path, files = dbf_entry, exdir = temp_dir, junkpaths = TRUE))
  read_path <- file.path(temp_dir, basename(dbf_entry))

  if (!file.exists(read_path)) {
    stop("Could not extract DBF from ", raw_path)
  }

  foreign::read.dbf(read_path, as.is = TRUE) |>
    as_tibble()
}

read_mappluto_sf <- function(raw_path) {
  if (!str_detect(tolower(raw_path), "[.]zip$")) {
    return(sf::st_read(raw_path, quiet = TRUE, stringsAsFactors = FALSE))
  }

  temp_dir <- tempfile(pattern = "mappluto_sf_")
  dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  suppressWarnings(unzip(raw_path, exdir = temp_dir))
  shp_path <- list.files(temp_dir, pattern = "[.]shp$", recursive = TRUE, full.names = TRUE)[1]

  if (is.na(shp_path) || !nzchar(shp_path)) {
    stop("No shapefile found in ", raw_path)
  }

  sf::st_read(shp_path, quiet = TRUE, stringsAsFactors = FALSE)
}
