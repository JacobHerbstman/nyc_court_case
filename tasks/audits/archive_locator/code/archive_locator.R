# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/archive_locator/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
})

archive_requests <- read_csv("../input/archive_requests.csv", show_col_types = FALSE, na = c("", "NA"))

queue <- archive_requests %>%
  mutate(
    archive_lane = case_when(
      str_detect(request_id, "dob") ~ "dob_foil",
      str_detect(request_id, "archives") ~ "municipal_archives",
      str_detect(request_id, "library") ~ "municipal_library",
      str_detect(request_id, "acris") ~ "acris_followup",
      TRUE ~ "other"
    )
  ) %>%
  select(request_id, archive_lane, custodian, portal_or_contact, records_requested, date_range, submitted_date, status, returned_filename, notes)

write_csv(queue, "../output/archive_locator_queue.csv", na = "")
cat("Wrote archive locator queue to ../output/archive_locator_queue.csv\n")
