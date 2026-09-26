# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_cd_homeownership_mappluto_undercount/code")
# table_county <- "B25034_005"

# Saves one ACS 2020-2024 five-year tract query for one NYC county unchanged.
# Requires CENSUS_API_KEY in ~/.Renviron; the key is never written to disk.

if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  stopifnot(length(args) == 1)
  table_county <- args[1]
}

table_id <- sub("_.*", "", table_county)
county_code <- sub(".*_", "", table_county)

stopifnot(table_id %in% c("B25034", "B25127"), county_code %in% c("005", "047", "061", "081", "085"))

if (!nzchar(Sys.getenv("CENSUS_API_KEY"))) {
  stop("CENSUS_API_KEY is not set in ~/.Renviron.")
}

query_url <- paste0(
  "https://api.census.gov/data/2024/acs/acs5?get=NAME,GEO_ID,group(", table_id, ")",
  "&for=tract:*&in=state:36%20county:", county_code
)

temp_path <- tempfile(tmpdir = "../temp", fileext = ".json")
download.file(paste0(query_url, "&key=", Sys.getenv("CENSUS_API_KEY")), temp_path, quiet = TRUE)

response <- jsonlite::fromJSON(temp_path)

if (!is.matrix(response) || nrow(response) < 2 || !paste0(table_id, "_001E") %in% response[1, ]) {
  stop("ACS response for ", table_county, " is not a tract table.")
}

invisible(file.rename(temp_path, paste0("../output/acs5_2024_", table_county, ".json")))

cat("Saved", nrow(response) - 1, "tracts from", query_url, "\n")
