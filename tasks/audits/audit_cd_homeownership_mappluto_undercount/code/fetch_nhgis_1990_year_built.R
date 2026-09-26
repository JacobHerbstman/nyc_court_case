# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_cd_homeownership_mappluto_undercount/code")

# Submits the 1990 STF3 year-built tract extract to IPUMS NHGIS and saves the
# returned CSV zip unchanged. Requires IPUMS_API_KEY in ~/.Renviron.

suppressPackageStartupMessages({
  library(ipumsr)
})

if (!nzchar(Sys.getenv("IPUMS_API_KEY"))) {
  stop("IPUMS_API_KEY is not set. Run ipumsr::set_ipums_api_key(\"<key>\", save = TRUE) and restart R.")
}

extract <- define_extract_from_json("nhgis_1990_year_built_extract.json")
ready_extract <- wait_for_extract(submit_extract(extract), verbose = TRUE)

download_dir <- tempfile(pattern = "nhgis_year_built_", tmpdir = "../temp")
dir.create(download_dir)
downloaded_zip <- download_extract(ready_extract, download_dir = download_dir, progress = FALSE)

zip_listing <- unzip(downloaded_zip, list = TRUE)$Name
table_csv <- grep("ds123_1990_tract\\.csv$", zip_listing, value = TRUE)

if (length(downloaded_zip) != 1 || length(table_csv) != 1) {
  stop("Expected one NHGIS zip containing one ds123 tract CSV.")
}

header <- readLines(unz(downloaded_zip, table_csv), n = 1)

if (!all(sapply(c("EX7001", "EX7008", "EYA001", "EYA016", "EZZ001", "EZZ002"), grepl, header))) {
  stop("NHGIS tract CSV is missing expected year-built columns.")
}

invisible(file.copy(downloaded_zip, "../output/nhgis_1990_stf3_year_built_tract_csv.zip", overwrite = TRUE))
unlink(download_dir, recursive = TRUE)

cat("Saved NHGIS extract", ready_extract$number, "to ../output/nhgis_1990_stf3_year_built_tract_csv.zip\n")
