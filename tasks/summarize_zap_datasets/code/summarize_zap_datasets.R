# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_zap_datasets/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

stage_qc <- read_csv("../input/zap_stage_qc.csv", show_col_types = FALSE, na = c("", "NA"))
project_df <- read_parquet("../input/zap_project_data.parquet") |>
  as.data.frame() |>
  as_tibble() |>
  mutate(project_id = as.character(project_id))
bbl_df <- read_parquet("../input/zap_project_bbl.parquet") |>
  as.data.frame() |>
  as_tibble() |>
  mutate(
    project_id = as.character(project_id),
    bbl_standardized = as.character(bbl_standardized)
  )

if (nrow(project_df) == 0) {
  write_csv(tibble(), "../output/zap_project_counts_by_decade_status.csv", na = "")
  write_csv(tibble(), "../output/zap_bbl_link_completeness.csv", na = "")
  write_csv(tibble(), "../output/zap_geography_coverage.csv", na = "")
  write_csv(bind_rows(stage_qc, tibble(metric = "status", value = "missing_staged_zap_project_data")), "../output/zap_summary_qc.csv", na = "")
  quit(save = "no")
}

if (any(is.na(project_df$project_id))) {
  stop("ZAP project data contains missing project_id values.")
}

if (n_distinct(project_df$project_id) != nrow(project_df)) {
  stop("ZAP project data must be unique by project_id before summarizing.")
}

counts_df <- project_df |>
  mutate(
    project_status = as.character(project_status),
    ulurp_group = as.character(ulurp_group),
    project_reference_decade = as.character(project_reference_decade)
  ) |>
  group_by(project_reference_decade, project_status, ulurp_group) |>
  summarise(project_count = n(), .groups = "drop") |>
  arrange(project_reference_decade, project_status, ulurp_group)

project_bbl_flags <- project_df |>
  transmute(project_id) |>
  left_join(
    bbl_df |>
      filter(!is.na(project_id), !is.na(bbl_standardized)) |>
      group_by(project_id) |>
      summarise(
        bbl_count = n(),
        distinct_bbl_count = n_distinct(bbl_standardized),
        any_validated_bbl = any(is_validated %in% TRUE),
        .groups = "drop"
      ),
    by = "project_id",
    relationship = "one-to-one"
  ) |>
  mutate(
    has_any_bbl = !is.na(bbl_count) & bbl_count > 0,
    any_validated_bbl = coalesce(any_validated_bbl, FALSE)
  )

link_df <- tibble(
  project_count = nrow(project_df),
  project_count_with_any_bbl = sum(project_bbl_flags$has_any_bbl, na.rm = TRUE),
  project_share_with_any_bbl = mean(project_bbl_flags$has_any_bbl, na.rm = TRUE),
  project_count_with_validated_bbl = sum(project_bbl_flags$any_validated_bbl, na.rm = TRUE),
  project_share_with_validated_bbl = mean(project_bbl_flags$any_validated_bbl, na.rm = TRUE),
  bbl_row_count = nrow(bbl_df),
  distinct_project_bbl_count = n_distinct(paste(
    bbl_df$project_id[!is.na(bbl_df$project_id) & !is.na(bbl_df$bbl_standardized)],
    bbl_df$bbl_standardized[!is.na(bbl_df$project_id) & !is.na(bbl_df$bbl_standardized)],
    sep = "_"
  )),
  distinct_bbl_count = n_distinct(bbl_df$bbl_standardized[!is.na(bbl_df$bbl_standardized)])
)

geo_df <- project_df |>
  mutate(
    borough_name_standardized = as.character(borough_name_standardized),
    community_district_standardized = suppressWarnings(as.integer(community_district_standardized))
  ) |>
  left_join(
    project_bbl_flags |>
      select(project_id, has_any_bbl, any_validated_bbl),
    by = "project_id",
    relationship = "many-to-one"
  ) |>
  group_by(borough_name_standardized, community_district_standardized) |>
  summarise(
    project_count = n(),
    project_count_with_any_bbl = sum(has_any_bbl, na.rm = TRUE),
    project_count_with_validated_bbl = sum(any_validated_bbl, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(borough_name_standardized, community_district_standardized)

summary_qc <- bind_rows(
  tibble(
    metric = names(stage_qc),
    value = vapply(stage_qc[1, ], function(x) as.character(x[[1]]), character(1))
  ),
  tibble(
    metric = c(
      "summary_project_min_reference_year",
      "summary_project_max_reference_year",
      "summary_nonmissing_borough_share",
      "summary_nonmissing_cd_share"
    ),
    value = c(
      as.character(min(project_df$project_reference_year, na.rm = TRUE)),
      as.character(max(project_df$project_reference_year, na.rm = TRUE)),
      as.character(mean(!is.na(project_df$borough_name_standardized))),
      as.character(mean(!is.na(project_df$community_district_standardized)))
    )
  )
)

write_csv(counts_df, "../output/zap_project_counts_by_decade_status.csv", na = "")
write_csv(link_df, "../output/zap_bbl_link_completeness.csv", na = "")
write_csv(geo_df, "../output/zap_geography_coverage.csv", na = "")
write_csv(summary_qc, "../output/zap_summary_qc.csv", na = "")

cat("Wrote ZAP summary outputs to ../output\n")
