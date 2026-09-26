# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_cd_homeownership_mappluto_undercount/code")

# DCP Housing Database (25Q4) completed jobs by community district and
# completion year, 2010-2025: units in completed new buildings (by building
# size) and net units added by completed alterations (conversions/enlargements).

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tidyr)
})

source("../../../_lib/data_reports.R")

standard_cds <- c(sprintf("1%02d", 1:12), sprintf("2%02d", 1:12), sprintf("3%02d", 1:18), sprintf("4%02d", 1:14), sprintf("5%02d", 1:3))

jobs <- read_csv(
  unz("../input/nychdb_25q4_csv.zip", "HousingDB_post2010.csv"),
  col_types = cols_only(Job_Number = "c", Job_Type = "c", Job_Status = "c", CompltYear = "i",
                        ClassAProp = "d", ClassANet = "d", CommntyDst = "c")
) |>
  filter(Job_Status == "5. Completed Construction", CompltYear >= 2010, CompltYear <= 2025, CommntyDst %in% standard_cds)

stopifnot(!anyDuplicated(jobs$Job_Number))

cd_year <- jobs |>
  group_by(borocd = CommntyDst, year = CompltYear) |>
  summarise(
    hdb_nb_units = sum(ClassAProp[Job_Type == "New Building"], na.rm = TRUE),
    hdb_nb_units_1_4 = sum(ClassAProp[Job_Type == "New Building" & ClassAProp < 5], na.rm = TRUE),
    hdb_nb_units_5_plus = sum(ClassAProp[Job_Type == "New Building" & ClassAProp >= 5], na.rm = TRUE),
    hdb_alteration_units_added = sum(pmax(ClassANet[Job_Type == "Alteration"], 0), na.rm = TRUE),
    .groups = "drop"
  )

cd_year <- expand_grid(borocd = standard_cds, year = 2010:2025) |>
  left_join(cd_year, by = c("borocd", "year"), relationship = "one-to-one") |>
  mutate(across(starts_with("hdb_"), ~ coalesce(.x, 0)))

save_csv(cd_year, "../output/hdb_cd_year.csv", c("borocd", "year"))

cat("Wrote ../output/hdb_cd_year.csv\n")
