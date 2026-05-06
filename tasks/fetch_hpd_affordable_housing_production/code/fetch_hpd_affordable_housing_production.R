# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_hpd_affordable_housing_production/code")

suppressPackageStartupMessages({
  library(readr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

source_csv_url <- "https://data.cityofnewyork.us/api/views/hg8x-zxpr/rows.csv?accessType=DOWNLOAD"
source_metadata_url <- "https://data.cityofnewyork.us/api/views/hg8x-zxpr.json"

temp_csv <- tempfile(fileext = ".csv")
temp_json <- tempfile(fileext = ".json")

download.file(source_csv_url, temp_csv, mode = "wb", quiet = TRUE)
download.file(source_metadata_url, temp_json, mode = "wb", quiet = TRUE)

copy_if_changed(temp_csv, "../output/hpd_affordable_housing_production_by_building.csv")
copy_if_changed(temp_json, "../output/hpd_affordable_housing_production_by_building_metadata.json")

row_count <- nrow(read_csv("../output/hpd_affordable_housing_production_by_building.csv", show_col_types = FALSE, n_max = Inf, progress = FALSE))

qc_df <- tibble(
  metric = c("source_url", "metadata_url", "row_count", "csv_bytes", "metadata_bytes"),
  value = c(source_csv_url, source_metadata_url, as.character(row_count), as.character(file.info("../output/hpd_affordable_housing_production_by_building.csv")$size), as.character(file.info("../output/hpd_affordable_housing_production_by_building_metadata.json")$size)),
  status = c("pass", "pass", ifelse(row_count > 0, "pass", "fail"), ifelse(file.info("../output/hpd_affordable_housing_production_by_building.csv")$size > 0, "pass", "fail"), ifelse(file.info("../output/hpd_affordable_housing_production_by_building_metadata.json")$size > 0, "pass", "fail")),
  note = c(
    "Official NYC Open Data HPD Affordable Housing Production by Building CSV endpoint.",
    "Official NYC Open Data dataset metadata endpoint.",
    "Downloaded row count.",
    "Downloaded CSV size.",
    "Downloaded metadata size."
  )
)

write_csv_if_changed(qc_df, "../output/hpd_affordable_housing_production_fetch_qc.csv")

if (any(qc_df$status == "fail")) {
  stop("HPD affordable housing production fetch QC failed.")
}

cat("Fetched HPD affordable housing production by building to ../output\n")
