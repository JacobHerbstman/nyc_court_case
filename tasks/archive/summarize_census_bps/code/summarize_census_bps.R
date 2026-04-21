# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_census_bps/code")
# census_bps_index_csv <- "../input/census_bps_index.csv"
# census_bps_qc_csv <- "../input/census_bps_qc.csv"
# census_bps_borough_year_parquet <- "../input/census_bps_borough_year.parquet"
# census_bps_city_year_parquet <- "../input/census_bps_city_year.parquet"
# out_summary_csv <- "../output/census_bps_audit_summary.csv"
# out_variable_csv <- "../output/census_bps_variable_quality.csv"
# out_anomaly_csv <- "../output/census_bps_anomalies.csv"
# out_benchmark_csv <- "../output/census_bps_benchmark_rows.csv"
# out_figures_pdf <- "../output/census_bps_figures.pdf"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 9) {
  stop("Expected 9 arguments: census_bps_index_csv census_bps_qc_csv census_bps_borough_year_parquet census_bps_city_year_parquet out_summary_csv out_variable_csv out_anomaly_csv out_benchmark_csv out_figures_pdf")
}

census_bps_index_csv <- args[1]
census_bps_qc_csv <- args[2]
census_bps_borough_year_parquet <- args[3]
census_bps_city_year_parquet <- args[4]
out_summary_csv <- args[5]
out_variable_csv <- args[6]
out_anomaly_csv <- args[7]
out_benchmark_csv <- args[8]
out_figures_pdf <- args[9]

index_df <- read_csv(census_bps_index_csv, show_col_types = FALSE, na = c("", "NA"))
qc_df <- read_csv(census_bps_qc_csv, show_col_types = FALSE, na = c("", "NA"))

if (!file.exists(census_bps_borough_year_parquet) || !file.exists(census_bps_city_year_parquet)) {
  write_csv(tibble(status = "no_staged_bps_files"), out_summary_csv, na = "")
  write_csv(tibble(), out_variable_csv, na = "")
  write_csv(tibble(), out_anomaly_csv, na = "")
  write_csv(tibble(), out_benchmark_csv, na = "")
  pdf(out_figures_pdf, width = 10, height = 7)
  plot.new()
  text(0.5, 0.5, "No staged BPS files available")
  dev.off()
  quit(save = "no")
}

borough_year_df <- read_parquet(census_bps_borough_year_parquet) %>%
  as.data.frame() %>%
  as_tibble()

city_year_df <- read_parquet(census_bps_city_year_parquet) %>%
  as.data.frame() %>%
  as_tibble()

summary_df <- city_year_df %>%
  left_join(
    qc_df %>%
      select(year, schema_fields, matched_borough_count, duplicate_match_count, unmatched_expected_count, county_match_share, status),
    by = "year"
  ) %>%
  mutate(
    city_one_to_four_units = city_one_unit_units + city_two_unit_units + city_three_four_unit_units,
    city_five_plus_share = city_five_plus_unit_units / city_total_units
  ) %>%
  arrange(year)

variable_df <- qc_df %>%
  transmute(
    year,
    schema_fields,
    raw_row_count,
    matched_borough_count,
    duplicate_match_count,
    unmatched_expected_count,
    county_match_share,
    status
  ) %>%
  arrange(year)

anomaly_df <- bind_rows(
  qc_df %>%
    filter(status != "ok" | unmatched_expected_count > 0 | duplicate_match_count > 0 | county_match_share < 1) %>%
    transmute(
      year,
      issue = "year_level_review_required",
      detail = paste(
        "matched_borough_count=", matched_borough_count,
        ";duplicate_match_count=", duplicate_match_count,
        ";unmatched_expected_count=", unmatched_expected_count,
        ";county_match_share=", county_match_share
      )
    ),
  borough_year_df %>%
    filter(!county_matches_name) %>%
    transmute(year, issue = "county_name_mismatch", detail = paste(borough_name, county_code, sep = " / "))
) %>%
  arrange(year, issue)

benchmark_rows <- borough_year_df %>%
  filter(year %in% c(1980, 1990, 2000, 2010, 2024)) %>%
  arrange(year, borough_name)

write_csv(summary_df, out_summary_csv, na = "")
write_csv(variable_df, out_variable_csv, na = "")
write_csv(anomaly_df, out_anomaly_csv, na = "")
write_csv(benchmark_rows, out_benchmark_csv, na = "")

pdf(out_figures_pdf, width = 11, height = 8.5)

print(
  ggplot(summary_df, aes(x = year, y = city_total_units)) +
    geom_line(color = "#2a6f97", linewidth = 0.7) +
    labs(title = "NYC Building Permit Units from Census BPS", x = "Year", y = "City total units") +
    theme_minimal(base_size = 12)
)

print(
  ggplot(borough_year_df, aes(x = year, y = total_units, color = borough_name)) +
    geom_line(linewidth = 0.7) +
    labs(title = "BPS Units by Borough", x = "Year", y = "Units", color = NULL) +
    theme_minimal(base_size = 12)
)

print(
  ggplot(qc_df, aes(x = year, y = matched_borough_count)) +
    geom_line(color = "#c97c5d", linewidth = 0.7) +
    geom_hline(yintercept = 5, linetype = "dashed") +
    labs(title = "Matched Borough Rows by Year", x = "Year", y = "Matched rows") +
    theme_minimal(base_size = 12)
)

dev.off()

cat("Wrote Census BPS summary outputs to", dirname(out_summary_csv), "\n")
