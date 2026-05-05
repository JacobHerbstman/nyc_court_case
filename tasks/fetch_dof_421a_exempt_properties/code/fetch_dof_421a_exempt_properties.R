# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_dof_421a_exempt_properties/code")
# raw_dir <- "../output/raw"
# out_files_csv <- "../output/dof_421a_raw_files.csv"
# out_qc_csv <- "../output/dof_421a_fetch_qc.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: raw_dir out_files_csv out_qc_csv")
}

raw_dir <- args[1]
out_files_csv <- args[2]
out_qc_csv <- args[3]

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
  temp_file <- tempfile(fileext = tools::file_ext(inventory$file_name[i]))
  download.file(inventory$source_url[i], temp_file, mode = "wb", quiet = TRUE)
  copy_if_changed(temp_file, inventory$raw_path[i])
}

inventory <- inventory %>%
  mutate(
    file_size_bytes = file.info(raw_path)$size,
    status = ifelse(!is.na(file_size_bytes) & file_size_bytes > 0, "downloaded", "failed")
  )

qc_df <- bind_rows(
  tibble(metric = "source_page", value = source_page, status = "pass", note = "Official DOF 421-a exemption page parsed for Excel links."),
  tibble(metric = "downloaded_file_count", value = as.character(sum(inventory$status == "downloaded")), status = if_else(all(inventory$status == "downloaded"), "pass", "fail"), note = "All parsed borough-year Excel files should download."),
  tibble(metric = "fiscal_year_min", value = as.character(min(inventory$fiscal_year_end, na.rm = TRUE)), status = if_else(min(inventory$fiscal_year_end, na.rm = TRUE) <= 2014, "pass", "fail"), note = "Expected archival support to FY2013/14."),
  tibble(metric = "fiscal_year_max", value = as.character(max(inventory$fiscal_year_end, na.rm = TRUE)), status = if_else(max(inventory$fiscal_year_end, na.rm = TRUE) >= 2025, "pass", "fail"), note = "Expected current support through at least FY2024/25."),
  tibble(metric = "missing_borough_count", value = as.character(sum(is.na(inventory$borough_file))), status = if_else(sum(is.na(inventory$borough_file)) == 0, "pass", "fail"), note = "Each Excel file should map to a borough.")
)

write_csv_if_changed(inventory, out_files_csv)
write_csv_if_changed(qc_df, out_qc_csv)

if (any(qc_df$status == "fail")) {
  stop("DOF 421-a fetch QC failed.")
}

cat("Fetched DOF 421-a exemption Excel files to", raw_dir, "\n")
