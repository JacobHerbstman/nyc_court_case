# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/write_cd_homeownership_next_stage_memo_tex/code")
# anatomy_top_cd_csv <- "../input/cd_homeownership_long_units_anatomy_top_cd.csv"
# borough_era_csv <- "../input/cd_homeownership_long_units_borough_era_shares.csv"
# overlap_error_csv <- "../input/cd_homeownership_long_units_borough_overlap_error.csv"
# exact_splice_csv <- "../input/cd_homeownership_long_units_exact_era_splice_total_units.csv"
# level_era_csv <- "../input/cd_homeownership_long_units_level_era.csv"
# levels_qc_csv <- "../input/cd_homeownership_long_units_levels_developability_qc.csv"
# out_tex <- "../output/cd_homeownership_next_stage_memo.tex"

suppressPackageStartupMessages({
  library(dplyr)
  library(knitr)
  library(readr)
  library(stringr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 7) {
  stop("Expected 7 arguments: anatomy_top_cd_csv borough_era_csv overlap_error_csv exact_splice_csv level_era_csv levels_qc_csv out_tex")
}

anatomy_top_cd_csv <- args[1]
borough_era_csv <- args[2]
overlap_error_csv <- args[3]
exact_splice_csv <- args[4]
level_era_csv <- args[5]
levels_qc_csv <- args[6]
out_tex <- args[7]

latex_escape <- function(x) {
  x |>
    str_replace_all("\\\\", "\\\\textbackslash{}") |>
    str_replace_all("([#$%&_{}])", "\\\\\\1") |>
    str_replace_all("~", "\\\\textasciitilde{}") |>
    str_replace_all("\\^", "\\\\textasciicircum{}")
}

fmt_num <- function(x, digits = 3) {
  ifelse(is.na(x), NA_character_, formatC(x, format = "f", digits = digits, big.mark = ","))
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

anatomy_df <- read_csv(anatomy_top_cd_csv, show_col_types = FALSE, na = c("", "NA"))
borough_era_df <- read_csv(borough_era_csv, show_col_types = FALSE, na = c("", "NA"))
overlap_error_df <- read_csv(overlap_error_csv, show_col_types = FALSE, na = c("", "NA"))
exact_splice_df <- read_csv(exact_splice_csv, show_col_types = FALSE, na = c("", "NA"))
level_era_df <- read_csv(level_era_csv, show_col_types = FALSE, na = c("", "NA"))
levels_qc_df <- read_csv(levels_qc_csv, show_col_types = FALSE, na = c("", "NA"))

exact_splice_table <- exact_splice_df |>
  mutate(
    borough_share = round(100 * borough_share, 1)
  ) |>
  select(Era = era, Tercile = treat_tercile_label, `Share (%)` = borough_share)

borough_change_table <- borough_era_df |>
  filter(
    treat_tercile_label == "High",
    series_family %in% c("units_built_total", "units_built_50_plus", "projects_built_50_plus"),
    era %in% c("1985-1989", "2020-2025")
  ) |>
  select(series_family, borough_name, era, borough_share) |>
  mutate(
    series_label = case_when(
      series_family == "units_built_total" ~ "Total units",
      series_family == "units_built_50_plus" ~ "50+ units",
      TRUE ~ "50+ projects"
    ),
    borough_share = round(100 * borough_share, 1)
  ) |>
  select(-series_family) |>
  tidyr::pivot_wider(names_from = era, values_from = borough_share) |>
  mutate(`2020s - 1980s` = round(`2020-2025` - `1985-1989`, 1)) |>
  arrange(match(series_label, c("Total units", "50+ units", "50+ projects")), borough_name) |>
  rename(Series = series_label, Borough = borough_name)

top_cd_table <- anatomy_df |>
  filter(
    series_family == "units_built_50_plus",
    era %in% c("1985-1989", "2020-2025")
  ) |>
  group_by(era) |>
  slice_head(n = 5) |>
  ungroup() |>
  transmute(
    Era = era,
    CD = borocd,
    Borough = borough_name,
    Units = round(units),
    `Share of high-tercile total (%)` = round(100 * share_of_high_tercile_total, 1)
  )

overlap_summary_table <- overlap_error_df |>
  filter(series_family %in% c("units_built_total", "units_built_50_plus", "projects_built_50_plus")) |>
  group_by(series_family, era, treat_tercile_label) |>
  summarize(
    signed_error = mean(signed_error, na.rm = TRUE),
    abs_error = mean(abs_error, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    Series = case_when(
      series_family == "units_built_total" ~ "Total units",
      series_family == "units_built_50_plus" ~ "50+ units",
      TRUE ~ "50+ projects"
    ),
    `Mean signed error` = round(signed_error, 3),
    `Mean abs. error` = round(abs_error, 3)
  ) |>
  select(Series, Era = era, Tercile = treat_tercile_label, `Mean signed error`, `Mean abs. error`)

levels_table <- level_era_df |>
  filter(metric %in% c(
    "units_built_total_per_10000_occupied_1990",
    "units_built_50_plus_per_10000_occupied_1990",
    "projects_50_plus_per_cd_year",
    "gross_add_units_per_res_acre"
  )) |>
  mutate(
    Metric = case_when(
      metric == "units_built_total_per_10000_occupied_1990" ~ "Total units per 10,000 occupied units",
      metric == "units_built_50_plus_per_10000_occupied_1990" ~ "50+ units per 10,000 occupied units",
      metric == "projects_50_plus_per_cd_year" ~ "50+ projects per CD-year",
      TRUE ~ "Gross additions per residential acre"
    ),
    `Value` = round(metric_value, 3)
  ) |>
  select(Metric, Era = era, Tercile = treat_tercile_label, Value)

resid_50_corr <- levels_qc_df |>
  filter(metric == "avg_annual_50_plus_units_residual_corr") |>
  pull(value)

resid_gross_corr <- levels_qc_df |>
  filter(metric == "avg_annual_gross_add_units_residual_corr") |>
  pull(value)

tex_lines <- c(
  "\\documentclass[11pt]{article}",
  "\\usepackage[margin=1in]{geometry}",
  "\\usepackage{booktabs}",
  "\\usepackage{graphicx}",
  "\\usepackage{float}",
  "\\usepackage{longtable}",
  "\\usepackage{array}",
  "\\usepackage{pdflscape}",
  "\\usepackage{hyperref}",
  "\\hypersetup{colorlinks=true, linkcolor=blue, urlcolor=blue, citecolor=blue}",
  "\\setlength{\\parskip}{0.6em}",
  "\\setlength{\\parindent}{0pt}",
  "\\begin{document}",
  "",
  "\\begin{center}",
  "{\\LARGE Next-Stage Homeownership and Housing Memo}",
  "\\end{center}",
  "",
  "\\section*{Bottom Line}",
  "The descriptive project is stronger after the new checks. The early decline is not just a current-PLUTO artifact, the pooled patterns are not just a few outliers, and the levels story is also negative for the high-homeownership tercile. The main narrowing is substantive: Manhattan is central to the early 50+ baseline, and the first-pass developability residual plots do not yet deliver a strong conditional negative relationship.",
  "",
  "\\section*{What Strengthened}",
  paste0(
    "The exact DCP era splice shows the high-homeownership tercile's total-units share falling from ",
    fmt_num(100 * exact_splice_df$borough_share[exact_splice_df$era == '1980s exact' & exact_splice_df$treat_tercile_label == 'High'], 1),
    "\\% in the exact 1980s to ",
    fmt_num(100 * exact_splice_df$borough_share[exact_splice_df$era == '1990s exact' & exact_splice_df$treat_tercile_label == 'High'], 1),
    "\\% in the exact 1990s, then to ",
    fmt_num(100 * exact_splice_df$borough_share[exact_splice_df$era == '2010s observed' & exact_splice_df$treat_tercile_label == 'High'], 1),
    "\\% in observed 2010s and ",
    fmt_num(100 * exact_splice_df$borough_share[exact_splice_df$era == '2020s observed' & exact_splice_df$treat_tercile_label == 'High'], 1),
    "\\% in observed 2020s."
  ),
  "The overlap error tables do not show the proxy mechanically overstating the high-homeownership share in one direction after 2010. For the high tercile, mean signed errors are small relative to the underlying share movements.",
  "",
  "\\section*{What Narrowed}",
  "The anatomy tables make the early 50+ story much more specific. In 1985--1989, the top four high-tercile contributors to 50+ units are Manhattan 108, 106, 105, and 101, and together they account for most of the high-tercile 50+ baseline. By 2020--2025, the later high-tercile 50+ margin is more mixed, led by Queens 412, then Brooklyn 317, Bronx 212, and only then Manhattan 108 and 106.",
  paste0(
    "The first-pass developability residual exercise is weak. After residualizing on borough plus baseline built-form controls, the residual correlation is only ",
    fmt_num(resid_50_corr, 3),
    " for average annual 50+ units and ",
    fmt_num(resid_gross_corr, 3),
    " for average annual gross additions."
  ),
  "",
  "\\section*{Key Tables}",
  "\\subsection*{Exact-to-Observed Era Splice}",
  make_latex_table(exact_splice_table, align = c("l", "l", "r")),
  "",
  "\\subsection*{High-Tercile Borough Changes}",
  make_latex_table(borough_change_table, align = c("l", "l", "r", "r", "r")),
  "",
  "\\subsection*{Top High-Tercile 50+ Contributors}",
  make_latex_table(top_cd_table, align = c("l", "l", "l", "r", "r")),
  "",
  "\\subsection*{Proxy Overlap Errors}",
  make_latex_table(overlap_summary_table, align = c("l", "l", "l", "r", "r")),
  "",
  "\\subsection*{Levels and Rates}",
  make_latex_table(levels_table, align = c("l", "l", "l", "r")),
  "",
  "\\section*{Figures}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.88\\textwidth]{../input/cd_homeownership_total_units_tercile.pdf}",
  "\\caption{Pooled within-borough total-units share by 1990 homeownership tercile.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.88\\textwidth]{../input/cd_homeownership_units_50_plus_tercile.pdf}",
  "\\caption{Pooled within-borough 50+ units share by 1990 homeownership tercile.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.88\\textwidth]{../input/cd_homeownership_exact_decadal_validation_plot.pdf}",
  "\\caption{Exact DCP decade shares versus the MapPLUTO proxy for the 1980s and 1990s.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[page=1,width=0.95\\textwidth]{../input/cd_homeownership_long_units_borough_plots.pdf}",
  "\\caption{Borough-specific pooled total-units shares.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[page=2,width=0.95\\textwidth]{../input/cd_homeownership_long_units_borough_plots.pdf}",
  "\\caption{Borough-specific pooled 50+ units shares.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[page=3,width=0.95\\textwidth]{../input/cd_homeownership_long_units_borough_plots.pdf}",
  "\\caption{Borough-specific pooled 50+ project shares.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[page=4,width=0.88\\textwidth]{../input/cd_homeownership_long_units_borough_plots.pdf}",
  "\\caption{Exact DCP 1980s and 1990s splice to observed 2010s and 2020s total units.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[page=1,width=0.95\\textwidth]{../input/cd_homeownership_long_units_levels_developability_plots.pdf}",
  "\\caption{Levels and rates: total units, 50+ units, and 50+ project counts.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[page=2,width=0.88\\textwidth]{../input/cd_homeownership_long_units_levels_developability_plots.pdf}",
  "\\caption{Observed gross additions per residential acre after 2010.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[page=3,width=0.88\\textwidth]{../input/cd_homeownership_long_units_levels_developability_plots.pdf}",
  "\\caption{First-pass residualized developability plots.}",
  "\\end{figure}",
  "",
  "\\section*{Interpretation}",
  "The cleanest safe statement is now: homeowner-heavy community districts lost within-borough share of large new housing over time, especially on the 50+ margin, and that pattern is not purely a current-PLUTO artifact or a few extreme CD-years. The next unresolved step is mechanism. The main live alternatives remain Manhattan-specific pre-period composition, city targeting of low-homeownership places, and baseline developability.",
  "",
  "\\end{document}"
)

writeLines(tex_lines, out_tex)

cat("Wrote next-stage memo TeX to", out_tex, "\n")
