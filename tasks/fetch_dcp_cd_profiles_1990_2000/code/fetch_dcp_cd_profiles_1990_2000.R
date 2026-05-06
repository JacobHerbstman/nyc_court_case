# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_dcp_cd_profiles_1990_2000/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

source_catalog <- read_csv("../input/source_catalog.csv", show_col_types = FALSE, na = c("", "NA"))
source_row <- source_catalog %>% filter(source_id == "dcp_cd_profiles_1990_2000")

if (nrow(source_row) != 1) {
  stop("Source catalog must contain exactly one dcp_cd_profiles_1990_2000 row.")
}

profile_urls <- tibble(
  borough_code = c("bk", "bx", "mn", "qn", "si"),
  borough_name = c("Brooklyn", "Bronx", "Manhattan", "Queens", "Staten Island"),
  official_url = c(
    "https://www.nyc.gov/assets/planning/download/pdf/data-maps/nyc-population/census2000/1990-2000_bk_cd_profile.pdf",
    "https://www.nyc.gov/assets/planning/download/pdf/data-maps/nyc-population/census2000/1990-2000_bx_cd_profile.pdf",
    "https://www.nyc.gov/assets/planning/download/pdf/data-maps/nyc-population/census2000/1990-2000_mn_cd_profile.pdf",
    "https://www.nyc.gov/assets/planning/download/pdf/data-maps/nyc-population/census2000/1990-2000_qn_cd_profile.pdf",
    "https://www.nyc.gov/assets/planning/download/pdf/data-maps/census/census2000/1990-2000_si_cd_profile.pdf"
  )
)

pull_date <- resolve_raw_pull_date(list(
  dcp_cd_profiles_1990_2000 = paste0("1990-2000_", profile_urls$borough_code, "_cd_profile.pdf")
))
inventory_rows <- vector("list", nrow(profile_urls))

for (i in seq_len(nrow(profile_urls))) {
  row <- profile_urls[i, ]
  pdf_path <- file.path(
    "..", "..", "..", "data_raw", "dcp_cd_profiles_1990_2000", pull_date,
    paste0("1990-2000_", row$borough_code, "_cd_profile.pdf")
  )

  pdf_status <- if (file.exists(pdf_path)) {
    "already_present"
  } else {
    download_with_status(row$official_url, pdf_path)
  }

  inventory_rows[[i]] <- tibble(
    source_id = "dcp_cd_profiles_1990_2000",
    pull_date = pull_date,
    borough_code = row$borough_code,
    borough_name = row$borough_name,
    file_role = "borough_profile_pdf",
    raw_path = pdf_path,
    status = pdf_status,
    official_url = row$official_url
  )
}

file_inventory <- bind_rows(inventory_rows) %>%
  arrange(borough_code)

checksum_table <- file_inventory %>%
  mutate(checksum_sha256 = if_else(file.exists(raw_path), vapply(raw_path, compute_sha256, character(1)), NA_character_)) %>%
  select(source_id, pull_date, borough_code, borough_name, file_role, raw_path, checksum_sha256)

provenance_table <- file_inventory %>%
  transmute(
    source_id,
    pull_date,
    borough_code,
    borough_name,
    official_url,
    note = "Official DCP borough profile PDF for the 1990 and 2000 community district census profiles."
  )

write_csv_if_changed(file_inventory, "../output/dcp_cd_profiles_1990_2000_files.csv")
write_csv_if_changed(checksum_table, "../output/dcp_cd_profiles_1990_2000_checksums.csv")
write_csv_if_changed(provenance_table, "../output/dcp_cd_profiles_1990_2000_provenance.csv")

cat("Wrote DCP 1990-2000 community district profile fetch outputs to ../output\n")
