# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_dof_421a_exempt_properties/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

raw_dir <- "../output/raw"

if (file.exists("../output/dof_421a_raw_files.csv")) {
  existing_inventory <- read_csv("../output/dof_421a_raw_files.csv", show_col_types = FALSE, na = c("", "NA"))
  if (
    nrow(existing_inventory) > 0 &&
      all(file.exists(existing_inventory$raw_path)) &&
      all(file.info(existing_inventory$raw_path)$size > 0)
  ) {
    write_csv_if_changed(existing_inventory, "../output/dof_421a_raw_files.csv")
    cat("Using existing DOF 421-a exemption Excel files in", raw_dir, "\n")
    quit(save = "no")
  }
}

source_page <- "https://www.nyc.gov/site/finance/property/benefits-421a.page"
html_lines <- readLines(source_page, warn = FALSE)
html <- paste(html_lines, collapse = "\n")

hrefs <- str_match_all(html, "href=\"([^\"]+\\.(?:xls|xlsx))\"")[[1]][, 2]
hrefs <- unique(hrefs[str_detect(hrefs, "/421a/")])
hrefs <- hrefs[str_detect(basename(hrefs), "^(421a_|manhattan_421a|bronx_421a|brooklyn_421a|queens_421a|statenisland_421a)")]
hrefs <- hrefs[!str_detect(str_to_lower(hrefs), "fces|additional|suspension")]

if (length(hrefs) == 0) {
  stop("No 421-a Excel links found on official DOF page.")
}

source_urls <- ifelse(str_detect(hrefs, "^https?://"), hrefs, paste0("https://www.nyc.gov", hrefs))

inventory <- tibble(
  source_url = source_urls,
  file_name = basename(source_urls),
  raw_path = file.path(raw_dir, basename(source_urls))
) %>%
  mutate(
    fiscal_code = str_extract(source_url, "(?<=/421a/)(2526|2425|2223|2021|1819|1718|1617|1516|1415)(?=/)"),
    fiscal_year_start = case_when(
      fiscal_code == "2526" ~ 2025L,
      fiscal_code == "2425" ~ 2024L,
      fiscal_code == "2223" ~ 2022L,
      fiscal_code == "2021" ~ 2020L,
      fiscal_code == "1819" ~ 2018L,
      fiscal_code == "1718" ~ 2017L,
      fiscal_code == "1617" ~ 2016L,
      fiscal_code == "1516" ~ 2015L,
      fiscal_code == "1415" ~ 2014L,
      is.na(fiscal_code) & str_detect(file_name, "^[a-z]+_421a\\.xls$") ~ 2013L,
      TRUE ~ NA_integer_
    ),
    fiscal_year_end = fiscal_year_start + 1L,
    borough_file = case_when(
      str_detect(file_name, "manhattan") ~ "Manhattan",
      str_detect(file_name, "bronx") ~ "Bronx",
      str_detect(file_name, "brooklyn") ~ "Brooklyn",
      str_detect(file_name, "queens") ~ "Queens",
      str_detect(file_name, "statenisland") ~ "Staten Island",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(fiscal_year_start), !is.na(borough_file)) %>%
  arrange(fiscal_year_end, borough_file)

for (i in seq_len(nrow(inventory))) {
  if (!file.exists(inventory$raw_path[i])) {
    temp_file <- tempfile(fileext = tools::file_ext(inventory$file_name[i]))
    download.file(inventory$source_url[i], temp_file, mode = "wb", quiet = TRUE)
    copy_if_changed(temp_file, inventory$raw_path[i])
  }
}

inventory <- inventory %>%
  mutate(
    file_size_bytes = file.info(raw_path)$size,
    status = ifelse(!is.na(file_size_bytes) & file_size_bytes > 0, "available", "failed")
  )

write_csv_if_changed(inventory, "../output/dof_421a_raw_files.csv")

if (!all(inventory$status == "available")) {
  stop("At least one parsed DOF 421-a Excel file failed to download.")
}

if (min(inventory$fiscal_year_end, na.rm = TRUE) > 2014 || max(inventory$fiscal_year_end, na.rm = TRUE) < 2025) {
  stop("DOF 421-a fiscal-year coverage is outside the expected range.")
}

cat("Fetched DOF 421-a exemption Excel files to", raw_dir, "\n")
