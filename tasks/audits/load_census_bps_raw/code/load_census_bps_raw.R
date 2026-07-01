suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

normalize_bps_place_name <- function(x) {
  x |>
    tolower() |>
    str_replace_all("\\.", " ") |>
    str_replace_all("[^a-z0-9 ]+", " ") |>
    str_squish()
}

safe_numeric_col <- function(x) {
  suppressWarnings(as.numeric(str_trim(as.character(x))))
}

bps_files <- read_csv("../input/census_bps_files.csv", show_col_types = FALSE, na = c("", "NA")) |>
  filter(file_role == "annual_place_ascii", file.exists(raw_path)) |>
  mutate(vintage = as.integer(vintage)) |>
  arrange(vintage)

if (nrow(bps_files) == 0) {
  write_csv_if_changed(tibble(), "../output/census_bps_raw_files.csv")
  quit(save = "no")
}

index_rows <- list()

for (i in seq_len(nrow(bps_files))) {
  row <- bps_files[i, ]
  raw_lines <- readLines(row$raw_path, warn = FALSE, encoding = "UTF-8")
  raw_lines <- raw_lines[raw_lines != "\032"]
  raw_lines <- raw_lines[!str_detect(raw_lines, "^(Survey|Date|\\s*$)")]
  split_rows_all <- strsplit(raw_lines, ",", fixed = TRUE)
  parseable_line <- lengths(split_rows_all) %in% c(35L, 38L, 41L)
  split_rows <- split_rows_all[parseable_line]
  dropped_line_count <- sum(!parseable_line)

  if (length(split_rows) == 0) {
    index_rows[[i]] <- tibble(
      year = row$vintage,
      raw_path = row$raw_path,
      raw_parquet_path = NA_character_,
      status = "no_parseable_rows"
    )
    next
  }

  if (dropped_line_count > 0) {
    index_rows[[i]] <- tibble(
      year = row$vintage,
      raw_path = row$raw_path,
      raw_parquet_path = NA_character_,
      status = "unexpected_line_width"
    )
    next
  }

  parsed_matrix <- do.call(rbind, split_rows)
  schema_fields <- ncol(parsed_matrix)
  parsed_df <- as_tibble(
    as.data.frame(parsed_matrix, stringsAsFactors = FALSE),
    .name_repair = ~ paste0("v", seq_along(.x))
  )
  name_col <- c(`35` = 11L, `38` = 14L, `41` = 17L)[as.character(schema_fields)]

  if (is.na(name_col)) {
    stop("Unexpected BPS schema width ", schema_fields, " in ", row$raw_path)
  }

  raw_bps_df <- parsed_df |>
    transmute(
      year = row$vintage,
      schema_fields = schema_fields,
      survey_date = as.character(.data[["v1"]]),
      state_code = str_pad(str_trim(as.character(.data[["v2"]])), width = 2, side = "left", pad = "0"),
      permit_id = str_trim(as.character(.data[["v3"]])),
      county_code = str_pad(str_trim(as.character(.data[["v4"]])), width = 3, side = "left", pad = "0"),
      place_name_raw = as.character(.data[[paste0("v", name_col)]]),
      place_name_normalized = normalize_bps_place_name(as.character(.data[[paste0("v", name_col)]])),
      one_unit_units = safe_numeric_col(.data[[paste0("v", name_col + 2L)]]),
      two_unit_units = safe_numeric_col(.data[[paste0("v", name_col + 5L)]]),
      three_four_unit_units = safe_numeric_col(.data[[paste0("v", name_col + 8L)]]),
      five_plus_unit_units = safe_numeric_col(.data[[paste0("v", name_col + 11L)]]),
      source_raw_path = row$raw_path
    ) |>
    mutate(total_units = rowSums(across(c(one_unit_units, two_unit_units, three_four_unit_units, five_plus_unit_units)), na.rm = TRUE))

  out_parquet_local <- file.path("..", "output", paste0("census_bps_raw_", row$vintage, ".parquet"))
  out_parquet <- file.path("..", "..", "load_census_bps_raw", "output", basename(out_parquet_local))
  write_parquet_if_changed(raw_bps_df, out_parquet_local)

  index_rows[[i]] <- tibble(
    year = row$vintage,
    raw_path = row$raw_path,
    raw_parquet_path = out_parquet,
    status = "parsed"
  )
}

write_csv_if_changed(bind_rows(index_rows), "../output/census_bps_raw_files.csv")
cat("Wrote raw Census BPS outputs to ../output\n")
