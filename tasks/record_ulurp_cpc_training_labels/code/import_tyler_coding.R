# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/record_ulurp_cpc_training_labels/code")

source("../../_lib/data_reports.R")

# Preserve the supplied columns, values, incomplete rows, and uncoded assignments.
coding <- readxl::read_excel("../input/cpc_llm_training_labels_tyler.xlsx",
  sheet = "Coding", col_types = "text", trim_ws = FALSE)
coding <- coding[!is.na(coding$review_id), ]
stopifnot(!anyNA(coding$document_id), !anyDuplicated(coding$document_id),
  !anyDuplicated(coding$review_id))
save_csv(coding, "../output/ulurp_cpc_training_labels_tyler.csv", "document_id")
