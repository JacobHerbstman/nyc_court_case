# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/write_cd_homeownership_validation_memo/code")
# exact_tercile_csv <- "../input/cd_homeownership_exact_decadal_validation_tercile.csv"
# exact_comparison_csv <- "../input/cd_homeownership_exact_decadal_validation_comparison.csv"
# proxy_overlap_metrics_csv <- "../input/cd_homeownership_proxy_overlap_metrics.csv"
# long_sensitivity_summary_csv <- "../input/cd_homeownership_long_units_sensitivity_summary.csv"
# out_decision_table_csv <- "../output/cd_homeownership_validation_decision_table.csv"
# out_memo_md <- "../output/cd_homeownership_validation_memo.md"

suppressPackageStartupMessages({
  library(dplyr)
  library(glue)
  library(readr)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 6) {
  stop("Expected 6 arguments: exact_tercile_csv exact_comparison_csv proxy_overlap_metrics_csv long_sensitivity_summary_csv out_decision_table_csv out_memo_md")
}

exact_tercile_csv <- args[1]
exact_comparison_csv <- args[2]
proxy_overlap_metrics_csv <- args[3]
long_sensitivity_summary_csv <- args[4]
out_decision_table_csv <- args[5]
out_memo_md <- args[6]

exact_tercile_df <- read_csv(exact_tercile_csv, show_col_types = FALSE, na = c("", "NA"))
exact_comparison_df <- read_csv(
  exact_comparison_csv,
  col_types = cols(
    decade = col_character(),
    metric = col_character(),
    value = col_double()
  )
)
proxy_metrics_df <- read_csv(proxy_overlap_metrics_csv, show_col_types = FALSE, na = c("", "NA"))
sensitivity_df <- read_csv(long_sensitivity_summary_csv, show_col_types = FALSE, na = c("", "NA"))

exact_high_1980s <- exact_comparison_df |>
  filter(decade == "1980s", metric == "exact_high_tercile_share") |>
  pull(value)

exact_high_1990s <- exact_comparison_df |>
  filter(decade == "1990s", metric == "exact_high_tercile_share") |>
  pull(value)

exact_pass <- length(exact_high_1980s) == 1 && length(exact_high_1990s) == 1 && exact_high_1990s < exact_high_1980s

metric_value <- function(df, family, metric_name) {
  value <- df |>
    filter(outcome_family == family, metric == metric_name) |>
    pull(value)

  if (length(value) == 0) {
    return(NA_real_)
  }

  value[[1]]
}

overlap_total_corr <- metric_value(proxy_metrics_df, "total_nb_units", "borough_year_tercile_share_corr")
overlap_total_resid <- metric_value(proxy_metrics_df, "total_nb_units", "cd_year_residual_corr")
overlap_50_corr <- metric_value(proxy_metrics_df, "units_50_plus", "borough_year_tercile_share_corr")
overlap_50_resid <- metric_value(proxy_metrics_df, "units_50_plus", "cd_year_residual_corr")

overlap_total_result <- case_when(
  !is.na(overlap_total_corr) && overlap_total_corr >= 0.75 && !is.na(overlap_total_resid) && overlap_total_resid > 0 ~ "Pass",
  !is.na(overlap_total_corr) && overlap_total_corr > 0.5 && !is.na(overlap_total_resid) && overlap_total_resid > 0 ~ "Partial",
  TRUE ~ "Fail"
)

overlap_50_result <- case_when(
  !is.na(overlap_50_corr) && overlap_50_corr >= 0.65 && !is.na(overlap_50_resid) && overlap_50_resid > 0 ~ "Pass",
  !is.na(overlap_50_corr) && overlap_50_corr > 0.4 && !is.na(overlap_50_resid) && overlap_50_resid > 0 ~ "Partial",
  TRUE ~ "Fail"
)

sensitivity_check <- function(series_key) {
  subset_df <- sensitivity_df |>
    filter(
      series_family == series_key,
      scenario_type %in% c("leave_one_borough_out", "top5_out")
    )

  if (nrow(subset_df) == 0) {
    return("Fail")
  }

  if (all(subset_df$decline_survives_2020s, na.rm = TRUE)) {
    return("Pass")
  }

  if (any(subset_df$decline_survives_2020s, na.rm = TRUE)) {
    return("Partial")
  }

  "Fail"
}

broad_total_result <- sensitivity_check("units_built_total")
broad_50_result <- sensitivity_check("units_built_50_plus")

decision_df <- bind_rows(
  tibble(
    check_name = "Exact 1980s vs 1990s decadal validation",
    result = if_else(exact_pass, "Pass", "Fail"),
    interpretation = glue("Exact DCP high-tercile share moves from {round(exact_high_1980s, 3)} in the 1980s to {round(exact_high_1990s, 3)} in the 1990s."),
    implication = if_else(exact_pass, "Early decline appears in exact decadal counts, which supports the pre-2000 part of the story.", "The early decline does not survive the exact decadal check, so the long pre-2000 pattern is likely overstated by the proxy.")
  ),
  tibble(
    check_name = "Proxy overlap validation: total units",
    result = overlap_total_result,
    interpretation = glue("Borough-year tercile-share correlation = {round(overlap_total_corr, 3)}; CD-year residual correlation = {round(overlap_total_resid, 3)}."),
    implication = case_when(
      overlap_total_result == "Pass" ~ "The proxy reproduces the observed total-units allocation object reasonably well after 2010.",
      overlap_total_result == "Partial" ~ "The proxy tracks the observed total-units allocation object only moderately well; use it as a descriptive bridge, not as a measured flow.",
      TRUE ~ "The proxy does not track the observed total-units allocation object well enough for strong reliance."
    )
  ),
  tibble(
    check_name = "Proxy overlap validation: 50+ units",
    result = overlap_50_result,
    interpretation = glue("Borough-year tercile-share correlation = {round(overlap_50_corr, 3)}; CD-year residual correlation = {round(overlap_50_resid, 3)}."),
    implication = case_when(
      overlap_50_result == "Pass" ~ "The proxy reproduces the observed 50+ allocation object reasonably well after 2010.",
      overlap_50_result == "Partial" ~ "The proxy captures some but not all of the observed 50+ allocation pattern.",
      TRUE ~ "The proxy is too noisy on the 50+ margin for confident historical interpretation."
    )
  ),
  tibble(
    check_name = "Sensitivity: total units broadness",
    result = broad_total_result,
    interpretation = glue("Leave-one-out and top-5-out checks for total units return {broad_total_result}."),
    implication = case_when(
      broad_total_result == "Pass" ~ "The pooled total-units pattern is not driven by one borough or a handful of extreme CD-years.",
      broad_total_result == "Partial" ~ "The pooled total-units pattern survives some exclusions but is not fully uniform.",
      TRUE ~ "The pooled total-units pattern looks concentrated rather than structural."
    )
  ),
  tibble(
    check_name = "Sensitivity: 50+ units broadness",
    result = broad_50_result,
    interpretation = glue("Leave-one-out and top-5-out checks for 50+ units return {broad_50_result}."),
    implication = case_when(
      broad_50_result == "Pass" ~ "The 50+ margin looks broad rather than concentrated in one borough or a few extreme CD-years.",
      broad_50_result == "Partial" ~ "The 50+ margin remains directionally present but some of the pooled pattern is concentrated.",
      TRUE ~ "The 50+ pattern is fragile to borough or top-CD exclusions."
    )
  )
) |>
  mutate(
    result = factor(result, levels = c("Pass", "Partial", "Fail"))
  )

write_csv(decision_df, out_decision_table_csv, na = "")

overall_take <- case_when(
  exact_pass && overlap_total_result != "Fail" && overlap_50_result != "Fail" && broad_total_result != "Fail" && broad_50_result != "Fail" ~ "Promising descriptive project with a stronger empirical foundation.",
  exact_pass && (overlap_total_result == "Pass" || overlap_50_result == "Pass") ~ "Promising descriptive project, but the historical bridge still needs caution.",
  TRUE ~ "Interesting descriptive pattern, but not yet strong enough to lean hard on the long-series mechanism."
)

memo_lines <- c(
  "# Homeownership-Housing Validation Memo",
  "",
  "## Bottom Line",
  "",
  overall_take,
  "",
  "## Decision Table",
  "",
  "| Check | Result | Interpretation | Implication |",
  "|---|---|---|---|",
  apply(
    decision_df |>
      mutate(result = as.character(result)),
    1,
    function(row) glue("| {row[['check_name']]} | {row[['result']]} | {row[['interpretation']]} | {row[['implication']]} |")
  ),
  "",
  "## Reading",
  "",
  "- `Pass` means the validation supports using the current long-series pattern as meaningful descriptive evidence.",
  "- `Partial` means the validation is directionally supportive but still leaves material uncertainty.",
  "- `Fail` means the current validation does not support leaning on that part of the story."
)

writeLines(memo_lines, out_memo_md)

cat("Wrote validation memo outputs to", dirname(out_decision_table_csv), "\n")
