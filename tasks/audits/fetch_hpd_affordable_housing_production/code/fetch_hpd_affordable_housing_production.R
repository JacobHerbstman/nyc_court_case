suppressPackageStartupMessages({
  library(readr)
})

source("../../../_lib/source_pipeline_utils.R")

source_csv_url <- "https://data.cityofnewyork.us/api/views/hg8x-zxpr/rows.csv?accessType=DOWNLOAD"

if (!file.exists("../output/hpd_affordable_housing_production_by_building.csv")) {
  temp_csv <- tempfile(fileext = ".csv")
  download.file(source_csv_url, temp_csv, mode = "wb", quiet = TRUE)
  copy_if_changed(temp_csv, "../output/hpd_affordable_housing_production_by_building.csv")
}

row_count <- nrow(read_csv("../output/hpd_affordable_housing_production_by_building.csv", show_col_types = FALSE, n_max = Inf, progress = FALSE))

if (row_count == 0 || file.info("../output/hpd_affordable_housing_production_by_building.csv")$size == 0) {
  stop("HPD affordable housing production download is empty.")
}

Sys.setFileTime("../output/hpd_affordable_housing_production_by_building.csv", Sys.time())

cat("Fetched HPD affordable housing production by building to ../output\n")
