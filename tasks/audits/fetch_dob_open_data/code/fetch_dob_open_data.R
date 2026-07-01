suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../../_lib/source_pipeline_utils.R")

source_catalog <- read_csv("../input/source_catalog.csv", show_col_types = FALSE, na = c("", "NA"))
dob_open_data_source_ids <- c(
  "dob_bis_job_filings",
  "dob_now_build_job_filings",
  "dob_bis_certificate_of_occupancy",
  "dob_now_certificate_of_occupancy"
)
dob_rows <- source_catalog |>
  filter(source_id %in% dob_open_data_source_ids) |>
  arrange(match(source_id, dob_open_data_source_ids))

if (nrow(dob_rows) != length(dob_open_data_source_ids) || !setequal(dob_rows$source_id, dob_open_data_source_ids)) {
  stop("Source catalog must contain the scripted non-permit DOB Open Data rows.")
}

index_rows <- list()
raw_root <- file.path("..", "..", "..", "..", "data_raw")
today <- format(Sys.Date(), "%Y%m%d")
existing_pull_dates <- lapply(seq_len(nrow(dob_rows)), function(i) {
  raw_dir <- file.path(raw_root, dob_rows$source_id[i])
  if (!dir.exists(raw_dir)) {
    return(character())
  }

  date_dirs <- basename(list.dirs(raw_dir, recursive = FALSE, full.names = TRUE))
  date_dirs <- date_dirs[str_detect(date_dirs, "^[0-9]{8}$")]
  date_dirs[vapply(
    date_dirs,
    function(date_value) file.exists(file.path(raw_dir, date_value, dob_rows$expected_filename[i])),
    logical(1)
  )] |>
    sort()
})
common_pull_dates <- Reduce(intersect, existing_pull_dates)
pull_date <- if (length(common_pull_dates) == 0) {
  today
} else if (today %in% common_pull_dates) {
  today
} else {
  max(common_pull_dates)
}

for (i in seq_len(nrow(dob_rows))) {
  row <- dob_rows[i, ]
  raw_dir <- file.path(raw_root, row$source_id, pull_date)
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
      pull_date = pull_date,
      status = status
    )
    next
  }

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    raw_path = raw_path,
    pull_date = pull_date,
    status = status
  )
}

write_csv_if_changed(bind_rows(index_rows), "../output/dob_open_data_files.csv")
cat("Wrote DOB Open Data fetch outputs to ../output\n")
