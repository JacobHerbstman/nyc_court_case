# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_cd_homeownership_mappluto_undercount/code")

# DOB permit issuance jobs on residential buildings by community district and
# year of first issuance, 1989-2002: new buildings (NB) and major alterations
# (A1). Counts are jobs, not units; this file records no unit counts, and its
# coverage before about 1993 is incomplete.

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(tidyr)
})

source("../../../_lib/data_reports.R")

standard_cds <- c(sprintf("1%02d", 1:12), sprintf("2%02d", 1:12), sprintf("3%02d", 1:18), sprintf("4%02d", 1:14), sprintf("5%02d", 1:3))

permits <- read_csv_arrow(
  "../input/dob_permit_issuance_current.csv",
  col_select = c("Job #", "Job Type", "Community Board", "Residential", "Issuance Date"),
  col_types = schema(`Job #` = string(), `Job Type` = string(), `Community Board` = string(), Residential = string(), `Issuance Date` = string()),
  as_data_frame = TRUE
) |>
  transmute(
    job_number = `Job #`,
    job_type = `Job Type`,
    borocd = `Community Board`,
    residential = Residential == "YES",
    issuance_year = suppressWarnings(as.integer(substr(`Issuance Date`, 7, 10)))
  ) |>
  filter(job_type %in% c("NB", "A1"), !is.na(job_number))

# One community district and first issuance year per job
jobs <- permits |>
  group_by(job_number, job_type) |>
  summarise(
    borocd = first(borocd),
    cd_count = n_distinct(borocd),
    residential = any(residential, na.rm = TRUE),
    first_year = suppressWarnings(min(issuance_year, na.rm = TRUE)),
    .groups = "drop"
  )

cat("Jobs with more than one community board:", sum(jobs$cd_count > 1), "of", nrow(jobs), "(dropped)\n")

cd_year <- jobs |>
  filter(cd_count == 1, residential, borocd %in% standard_cds, first_year >= 1989, first_year <= 2002) |>
  count(borocd, year = first_year, job_type) |>
  pivot_wider(names_from = job_type, values_from = n, names_prefix = "residential_jobs_", values_fill = 0)

cd_year <- expand_grid(borocd = standard_cds, year = 1989:2002) |>
  left_join(cd_year, by = c("borocd", "year"), relationship = "one-to-one") |>
  mutate(across(starts_with("residential_jobs_"), ~ coalesce(.x, 0L))) |>
  select(borocd, year, residential_jobs_NB, residential_jobs_A1)

save_csv(cd_year, "../output/dob_permit_cd_year.csv", c("borocd", "year"))

cat("Wrote ../output/dob_permit_cd_year.csv\n")
