# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/redevelopment_potential_first_pass/code")
# input_inventory_csv <- "../output/input_inventory.csv"
# redev_baseline_csv <- "../output/cd_redevelopment_potential_baseline.csv"
# redev_qc_csv <- "../output/cd_redevelopment_potential_qc.csv"
# redev_corr_csv <- "../output/cd_redevelopment_potential_index_correlations.csv"
# redev_sensitivity_csv <- "../output/cd_redevelopment_potential_sensitivity.csv"
# cell_summary_csv <- "../output/tables/two_by_two_cell_summary.csv"
# era_outcomes_csv <- "../output/tables/two_by_two_era_outcomes.csv"
# model_summary_csv <- "../output/tables/redev_interaction_model_summary.csv"
# borough_sensitivity_csv <- "../output/tables/borough_sensitivity_redev.csv"
# manhattan_anatomy_csv <- "../output/tables/manhattan_baseline_anatomy.csv"
# out_md <- "../output/redevelopment_potential_first_pass_memo.md"
# out_tex <- "../output/redevelopment_potential_first_pass_memo.tex"

suppressPackageStartupMessages({
  library(dplyr)
  library(knitr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 12) {
  stop("Expected 12 arguments: input_inventory_csv redev_baseline_csv redev_qc_csv redev_corr_csv redev_sensitivity_csv cell_summary_csv era_outcomes_csv model_summary_csv borough_sensitivity_csv manhattan_anatomy_csv out_md out_tex")
}

input_inventory_csv <- args[1]
redev_baseline_csv <- args[2]
redev_qc_csv <- args[3]
redev_corr_csv <- args[4]
redev_sensitivity_csv <- args[5]
cell_summary_csv <- args[6]
era_outcomes_csv <- args[7]
model_summary_csv <- args[8]
borough_sensitivity_csv <- args[9]
manhattan_anatomy_csv <- args[10]
out_md <- args[11]
out_tex <- args[12]

fmt_num <- function(x, digits = 3) {
  ifelse(is.na(x), NA_character_, formatC(x, format = "f", digits = digits, big.mark = ","))
}

make_latex_table <- function(df) {
  capture.output(
    knitr::kable(
      df,
      format = "latex",
      booktabs = TRUE,
      longtable = FALSE,
      escape = TRUE,
      linesep = ""
    )
  )
}

input_inventory_df <- read_csv(input_inventory_csv, show_col_types = FALSE, na = c("", "NA"))
redev_baseline_df <- read_csv(redev_baseline_csv, show_col_types = FALSE, na = c("", "NA"))
redev_qc_df <- read_csv(redev_qc_csv, show_col_types = FALSE, na = c("", "NA"))
redev_corr_df <- read_csv(redev_corr_csv, show_col_types = FALSE, na = c("", "NA"))
redev_sensitivity_df <- read_csv(redev_sensitivity_csv, show_col_types = FALSE, na = c("", "NA"))
cell_summary_df <- read_csv(cell_summary_csv, show_col_types = FALSE, na = c("", "NA"))
era_outcomes_df <- read_csv(era_outcomes_csv, show_col_types = FALSE, na = c("", "NA"))
model_summary_df <- read_csv(model_summary_csv, show_col_types = FALSE, na = c("", "NA"))
borough_sensitivity_df <- read_csv(borough_sensitivity_csv, show_col_types = FALSE, na = c("", "NA"))
manhattan_anatomy_df <- read_csv(manhattan_anatomy_csv, show_col_types = FALSE, na = c("", "NA"))

headline_interactions <- model_summary_df |>
  filter(
    sample_label == "all_nyc",
    index_name == "A",
    control_layer == "4_all_blocks",
    functional_form == "linear_occ",
    term_group == "homeowner_x_redev",
    outcome_family %in% c("units_built_total", "gross_add_units", "nb_gross_units_50_plus")
  )

non_manhattan_interactions <- model_summary_df |>
  filter(
    sample_label == "non_manhattan_only",
    index_name == "A",
    control_layer == "4_all_blocks",
    functional_form == "linear_occ",
    term_group == "homeowner_x_redev",
    outcome_family %in% c("units_built_total", "gross_add_units", "nb_gross_units_50_plus")
  )

classification <- case_when(
  all(headline_interactions$estimate[headline_interactions$era %in% c("2010-2019", "2020-2025", "2015-2019")] < 0, na.rm = TRUE) &&
    sum(non_manhattan_interactions$estimate < 0, na.rm = TRUE) >= 2 ~ "A. Strong support",
  sum(headline_interactions$estimate < 0, na.rm = TRUE) >= 2 ~ "B. Mixed support",
  TRUE ~ "C. Weak support"
)

cell_table <- cell_summary_df |>
  transmute(
    Cell = two_by_two_cell,
    Label = two_by_two_label,
    `N CDs` = n_cd,
    `Mean treat z` = round(mean_treat_z_boro, 2),
    `Mean redev A z` = round(mean_redev_A_z_boro, 2),
    `Mean built FAR` = round(mean_built_far, 2),
    `Mean unused FAR` = round(mean_unused_res_far, 2)
  )

interaction_table <- headline_interactions |>
  mutate(
    Outcome = case_when(
      outcome_family == "units_built_total" ~ "Total units",
      outcome_family == "gross_add_units" ~ "Gross additions",
      TRUE ~ "50+ units"
    ),
    Estimate = round(estimate, 3),
    `Std. Error` = round(std_error, 3)
  ) |>
  select(Outcome, Era = era, Estimate, `Std. Error`)

borough_table <- borough_sensitivity_df |>
  filter(row_type == "interaction_regression", sample_label %in% c("all_nyc", "non_manhattan_only", "leave_out_manhattan", "drop_101_105_106_108"), outcome_family %in% c("units_built_total", "nb_gross_units_50_plus")) |>
  mutate(
    Outcome = case_when(
      outcome_family == "units_built_total" ~ "Total units",
      TRUE ~ "50+ units"
    ),
    Estimate = round(estimate, 3)
  ) |>
  select(Sample = sample_label, Outcome, Era = era, Estimate)

manhattan_table <- manhattan_anatomy_df |>
  filter(row_type == "cd_summary") |>
  transmute(
    CD = borocd,
    `Treat z` = round(treat_z_boro, 2),
    `A z` = round(redev_potential_A_z_boro, 2),
    `Built FAR` = round(cd_mean_built_far_lot_weighted, 2),
    `Max res FAR` = round(cd_mean_max_resid_far_lot_weighted, 2),
    `Unused res FAR` = round(cd_mean_unused_res_far_lot_weighted, 2)
  )

main_corr <- redev_corr_df |>
  filter(scope_name == "city", index_1 == "A", index_2 == "C") |>
  pull(correlation)

release_corr <- redev_sensitivity_df |>
  filter(comparison_name == "18v1.1_vs_25v4", metric == "redev_potential_A_z_boro") |>
  pull(value)

hh_high_redev_gap <- era_outcomes_df |>
  filter(
    outcome_family == "units_built_total",
    metric == "per_10000_occupied_1990",
    two_by_two_cell %in% c("LH", "HH"),
    era %in% c("2010-2019", "2020-2025")
  ) |>
  select(era, two_by_two_cell, value) |>
  pivot_wider(names_from = two_by_two_cell, values_from = value) |>
  mutate(gap_hh_minus_lh = HH - LH)

md_lines <- c(
  "# Redevelopment Potential First Pass",
  "",
  "## Objective",
  "This task tests whether the homeownership pattern is concentrated where redevelopment is economically plausible. The main diagnostic object is `treat_z_boro × redevelopment potential`, not homeownership alone.",
  "",
  "## Data",
  paste0(
    "The task uses the canonical treatment file, the exact 1990 baseline controls, the permit-year DCP supply panel, the stitched long units series, and the earliest available archived MapPLUTO release `18v1.1`. The input inventory covers ",
    nrow(input_inventory_df),
    " canonical objects."
  ),
  "",
  "Important limitation: the earliest available MapPLUTO archive in the repo is 2018, so redevelopment potential is built from a post-treatment descriptive stock file rather than a pre-1990 baseline.",
  "",
  "## Redevelopment Potential Construction",
  paste0(
    "Index A is the within-borough z-score of `log(cd_sum_unused_res_floor_area)`. Index C is a standardized composite of unused FAR, underbuilt land, vacant/low-intensity land, protected land, and built FAR. In the pooled city data, the correlation between A and C is ",
    fmt_num(main_corr, 3),
    ". The A index is stable across the `18v1.1` and current `25v4` releases, with a correlation of ",
    fmt_num(release_corr, 3),
    "."
  ),
  "",
  "## Treatment × Redevelopment Relationship",
  paste0(
    "The 2x2 cell split leaves a nontrivial `high homeowner × high redevelopment potential` cell. In the `HH` cell, the mean treatment z-score is ",
    fmt_num(cell_summary_df$mean_treat_z_boro[cell_summary_df$two_by_two_cell == "HH"], 2),
    " and the mean redevelopment A z-score is ",
    fmt_num(cell_summary_df$mean_redev_A_z_boro[cell_summary_df$two_by_two_cell == "HH"], 2),
    "."
  ),
  "",
  "## Main Descriptive Results",
  paste0(
    "The key comparison is `HH` versus `LH`. For total units per 10,000 occupied units, the `HH - LH` gap is ",
    fmt_num(hh_high_redev_gap$gap_hh_minus_lh[hh_high_redev_gap$era == "2010-2019"], 2),
    " in `2010-2019` and ",
    fmt_num(hh_high_redev_gap$gap_hh_minus_lh[hh_high_redev_gap$era == "2020-2025"], 2),
    " in `2020-2025`."
  ),
  "",
  "## Regression Diagnostics",
  "The interaction regressions remain descriptive. The headline coefficient of interest is the homeowner × redevelopment interaction. Negative values suggest that high-homeowner CDs underproduce housing specifically where redevelopment potential is high.",
  "",
  "## Manhattan and Borough Composition",
  "The task directly reports Manhattan anatomy and borough-split sensitivities. Manhattan remains important, but the sample-split and leave-one-out checks determine whether the interaction survives outside the classic Manhattan baseline CDs.",
  "",
  "## Interpretation",
  paste0("Classification: **", classification, "**."),
  "",
  "## Next Data Needs",
  "- ZAP / ULURP applications and outcomes to test the discretionary pipeline directly.",
  "- DOF or floorspace-price measures for a cleaner demand-weighted redevelopment index.",
  "- HPD/public-site or subsidy splits for post-2010 project outcomes.",
  "- Historical zoning maps or stronger allowed-density baselines if this first pass remains promising."
)

writeLines(md_lines, out_md)

tex_lines <- c(
  "\\documentclass[11pt]{article}",
  "\\usepackage[margin=1in]{geometry}",
  "\\usepackage{booktabs}",
  "\\usepackage{graphicx}",
  "\\usepackage{float}",
  "\\usepackage{hyperref}",
  "\\setlength{\\parskip}{0.6em}",
  "\\setlength{\\parindent}{0pt}",
  "\\begin{document}",
  "\\begin{center}",
  "{\\LARGE Redevelopment Potential First Pass}",
  "\\end{center}",
  "",
  "\\section*{Objective}",
  "This task tests whether the homeownership pattern is concentrated where redevelopment is economically plausible. The main diagnostic object is homeowner exposure $\\times$ redevelopment potential. The task is diagnostic, not causal.",
  "",
  "\\section*{Data and Source Limits}",
  paste0(
    "The input inventory covers ", nrow(input_inventory_df), " canonical objects. The earliest archived MapPLUTO release available in the repo is \\texttt{18v1.1}, so redevelopment potential is constructed from a post-treatment descriptive stock file rather than a pre-1990 baseline."
  ),
  "",
  "\\section*{Key Tables}",
  "\\subsection*{2x2 Cell Summary}",
  make_latex_table(cell_table),
  "",
  "\\subsection*{Headline Interaction Estimates}",
  make_latex_table(interaction_table),
  "",
  "\\subsection*{Sample Sensitivity}",
  make_latex_table(borough_table),
  "",
  "\\subsection*{Manhattan Baseline Anatomy}",
  make_latex_table(manhattan_table),
  "",
  "\\section*{Interpretation}",
  paste0("Classification: \\textbf{", classification, "}."),
  "",
  "\\section*{Figures}",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.9\\textwidth]{figures/homeownership_vs_redev_potential_scatter.pdf}",
  "\\caption{Treatment versus redevelopment potential scatterplots.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.9\\textwidth]{figures/redev_potential_maps.pdf}",
  "\\caption{Current community-district maps of treatment and redevelopment potential.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.9\\textwidth]{figures/two_by_two_total_units_paths.pdf}",
  "\\caption{2x2 total-units paths. The long series marks the 2010 source switch.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.9\\textwidth]{figures/two_by_two_50plus_units_paths.pdf}",
  "\\caption{2x2 50+ units paths.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.9\\textwidth]{figures/two_by_two_gross_additions_paths.pdf}",
  "\\caption{2x2 gross-additions paths.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.9\\textwidth]{figures/two_by_two_50plus_project_count_paths.pdf}",
  "\\caption{2x2 large-project margins.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.9\\textwidth]{figures/nested_controls_coefficients.pdf}",
  "\\caption{Nested-controls interaction diagnostics.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.9\\textwidth]{figures/redev_interaction_event_coefficients.pdf}",
  "\\caption{Event-style homeowner $\\times$ redevelopment interaction coefficients.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.9\\textwidth]{figures/borough_specific_two_by_two_paths.pdf}",
  "\\caption{Borough-specific 2x2 paths.}",
  "\\end{figure}",
  "",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.9\\textwidth]{figures/non_manhattan_redev_paths.pdf}",
  "\\caption{Sample-split 2x2 paths for non-Manhattan comparisons.}",
  "\\end{figure}",
  "",
  "\\section*{Next Data Needs}",
  "\\begin{itemize}",
  "\\item ZAP / ULURP applications and outcomes.",
  "\\item DOF or floorspace-price measures for a cleaner demand-weighted index.",
  "\\item HPD/public-site or subsidy splits for post-2010 project outcomes.",
  "\\item Historical zoning maps if this interaction remains promising.",
  "\\end{itemize}",
  "\\end{document}"
)

writeLines(tex_lines, out_tex)

cat("Wrote redevelopment memo markdown and TeX to", dirname(out_md), "\n")
