# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/write_cd_homeownership_validation_memo_tex/code")
# decision_table_csv <- "../input/cd_homeownership_validation_decision_table.csv"
# exact_comparison_csv <- "../input/cd_homeownership_exact_decadal_validation_comparison.csv"
# proxy_overlap_metrics_csv <- "../input/cd_homeownership_proxy_overlap_metrics.csv"
# sensitivity_summary_csv <- "../input/cd_homeownership_long_units_sensitivity_summary.csv"
# out_tex <- "../output/cd_homeownership_validation_memo.tex"

suppressPackageStartupMessages({
  library(dplyr)
  library(glue)
  library(knitr)
  library(readr)
  library(stringr)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 5) {
  stop("Expected 5 arguments: decision_table_csv exact_comparison_csv proxy_overlap_metrics_csv sensitivity_summary_csv out_tex")
}

decision_table_csv <- args[1]
exact_comparison_csv <- args[2]
proxy_overlap_metrics_csv <- args[3]
sensitivity_summary_csv <- args[4]
out_tex <- args[5]

latex_escape <- function(x) {
  x |>
    str_replace_all("\\\\", "\\\\textbackslash{}") |>
    str_replace_all("([#$%&_{}])", "\\\\\\1") |>
    str_replace_all("~", "\\\\textasciitilde{}") |>
    str_replace_all("\\^", "\\\\textasciicircum{}")
}

make_latex_table <- function(df, align = NULL) {
  capture.output(
    knitr::kable(
      df,
      format = "latex",
      booktabs = TRUE,
      longtable = FALSE,
      align = align,
      escape = TRUE,
      linesep = ""
    )
  )
}

decision_df <- read_csv(decision_table_csv, show_col_types = FALSE, na = c("", "NA"))
exact_df <- read_csv(
  exact_comparison_csv,
  col_types = cols(
    decade = col_character(),
    metric = col_character(),
    value = col_double()
  )
)
proxy_df <- read_csv(proxy_overlap_metrics_csv, show_col_types = FALSE, na = c("", "NA"))
sensitivity_df <- read_csv(sensitivity_summary_csv, show_col_types = FALSE, na = c("", "NA"))

exact_high_1980s <- exact_df |>
  filter(decade == "1980s", metric == "exact_high_tercile_share") |>
  pull(value)

exact_high_1990s <- exact_df |>
  filter(decade == "1990s", metric == "exact_high_tercile_share") |>
  pull(value)

proxy_high_1980s <- exact_df |>
  filter(decade == "1980s", metric == "proxy_high_tercile_share") |>
  pull(value)

proxy_high_1990s <- exact_df |>
  filter(decade == "1990s", metric == "proxy_high_tercile_share") |>
  pull(value)

metric_value <- function(df, family, metric_name) {
  value <- df |>
    filter(outcome_family == family, metric == metric_name) |>
    pull(value)

  if (length(value) == 0) {
    return(NA_real_)
  }

  value[[1]]
}

overlap_table <- tibble(
  Outcome = c("Total new-building units", "50+ new-building units", "50+ new-building project counts"),
  `Borough-year tercile corr` = c(
    round(metric_value(proxy_df, "total_nb_units", "borough_year_tercile_share_corr"), 3),
    round(metric_value(proxy_df, "units_50_plus", "borough_year_tercile_share_corr"), 3),
    round(metric_value(proxy_df, "projects_50_plus", "borough_year_tercile_share_corr"), 3)
  ),
  `CD-year residual corr` = c(
    round(metric_value(proxy_df, "total_nb_units", "cd_year_residual_corr"), 3),
    round(metric_value(proxy_df, "units_50_plus", "cd_year_residual_corr"), 3),
    round(metric_value(proxy_df, "projects_50_plus", "cd_year_residual_corr"), 3)
  ),
  `Borough-year tercile MAE` = c(
    round(metric_value(proxy_df, "total_nb_units", "borough_year_tercile_share_mae"), 3),
    round(metric_value(proxy_df, "units_50_plus", "borough_year_tercile_share_mae"), 3),
    round(metric_value(proxy_df, "projects_50_plus", "borough_year_tercile_share_mae"), 3)
  )
)

decision_table <- decision_df |>
  transmute(
    Check = check_name,
    Result = result,
    Interpretation = interpretation
  )

sensitivity_table <- sensitivity_df |>
  filter(
    scenario_type %in% c("leave_one_borough_out", "top5_out"),
    series_family %in% c("units_built_total", "units_built_50_plus")
  ) |>
  transmute(
    Scenario = scenario_name,
    Series = if_else(series_family == "units_built_total", "Total units", "50+ units"),
    `1985-1989` = round(`1985-1989`, 3),
    `2020-2025` = round(`2020-2025`, 3),
    `2020s minus 1985-1989` = round(change_2020s_vs_1985_1989, 3),
    `Decline survives` = if_else(decline_survives_2020s, "Yes", "No")
  ) |>
  arrange(Series, Scenario)

overall_take <- decision_df$result
if (all(overall_take %in% c("Pass", "Partial")) && any(overall_take == "Pass")) {
  bottom_line <- "Promising descriptive project with a stronger empirical foundation."
} else {
  bottom_line <- "Interesting descriptive pattern, but not yet strong enough to lean hard on the long-series mechanism."
}

tex_lines <- c(
  "\\documentclass[11pt]{article}",
  "\\usepackage[margin=1in]{geometry}",
  "\\usepackage{booktabs}",
  "\\usepackage{graphicx}",
  "\\usepackage{float}",
  "\\usepackage{longtable}",
  "\\usepackage{array}",
  "\\usepackage{caption}",
  "\\usepackage{pdflscape}",
  "\\usepackage{hyperref}",
  "\\hypersetup{colorlinks=true, linkcolor=blue, urlcolor=blue, citecolor=blue}",
  "\\begin{document}",
  "",
  "\\begin{center}",
  "{\\LARGE Homeownership-Housing Validation Memo}",
  "\\end{center}",
  "",
  "\\section*{Bottom Line}",
  latex_escape(bottom_line),
  "",
  glue("The exact DCP decadal check shows the high-homeownership tercile falling from {round(exact_high_1980s, 3)} in the 1980s to {round(exact_high_1990s, 3)} in the 1990s. The corresponding proxy shares move from {round(proxy_high_1980s, 3)} to {round(proxy_high_1990s, 3)}, so the proxy overstates the decline but gets the direction right."),
  "",
  "\\section*{Decision Table}",
  make_latex_table(decision_table, align = c("l", "l", "p{4.5in}")),
  "",
  "\\section*{Overlap Validation}",
  "These are the direct post-2010 proxy-vs-observed checks on the same within-borough allocation object used in the main figures.",
  "",
  make_latex_table(overlap_table, align = c("l", "c", "c", "c")),
  "",
  "\\section*{Sensitivity Checks}",
  "The table below records whether the high-homeownership tercile still has a lower 2020--2025 share than in 1985--1989 after leave-one-borough-out and top-5-out exclusions.",
  "",
  make_latex_table(sensitivity_table, align = c("l", "l", "c", "c", "c", "c")),
  "",
  "\\section*{Figures}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.9\\textwidth]{../input/cd_homeownership_exact_decadal_validation_plot.pdf}",
  "\\caption{Exact DCP decade shares versus the MapPLUTO proxy for the 1980s and 1990s.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.95\\textwidth]{../input/cd_homeownership_proxy_overlap_plots.pdf}",
  "\\caption{Proxy-overlap validation for 2010--2025: tercile shares over time, borough-year share scatter, and CD-year residual scatter.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.95\\textwidth]{../input/cd_homeownership_long_units_sensitivity_plots.pdf}",
  "\\caption{Sensitivity checks for pooled, borough-specific, leave-one-out, top-5-out, and level-style complements.}",
  "\\end{figure}",
  "",
  "\\end{document}"
)

writeLines(tex_lines, out_tex)

cat("Wrote validation memo TeX to", out_tex, "\n")
