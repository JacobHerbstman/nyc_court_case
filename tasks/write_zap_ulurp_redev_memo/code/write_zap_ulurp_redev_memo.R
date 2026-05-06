# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/write_zap_ulurp_redev_memo/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

fmt_num <- function(x, digits = 3) {
  ifelse(is.na(x), "NA", formatC(x, digits = digits, format = "f", big.mark = ","))
}

escape_tex <- function(x) {
  x <- str_replace_all(x, "\\\\", "\\\\textbackslash{}")
  x <- str_replace_all(x, "([#$%&_{}])", "\\\\\\1")
  x <- str_replace_all(x, "~", "\\\\textasciitilde{}")
  x <- str_replace_all(x, "\\^", "\\\\textasciicircum{}")
  x
}

base_df <- read_csv("../input/zap_ulurp_redev_project_base.csv", show_col_types = FALSE, na = c("", "NA"), guess_max = Inf)
qc_df <- read_csv("../input/zap_ulurp_redev_project_base_qc.csv", show_col_types = FALSE, na = c("", "NA"))
era_summary_df <- read_csv("../input/zap_ulurp_redev_2x2_era_summary.csv", show_col_types = FALSE, na = c("", "NA"))
model_summary_df <- read_csv("../input/zap_ulurp_redev_model_summary.csv", show_col_types = FALSE, na = c("", "NA"))
nested_diag_df <- read_csv("../input/zap_ulurp_redev_nested_diagnostics.csv", show_col_types = FALSE, na = c("", "NA"))

total_projects <- nrow(base_df)
private_projects <- sum(base_df$private_applicant %in% TRUE, na.rm = TRUE)
public_projects <- sum(base_df$public_applicant %in% TRUE, na.rm = TRUE)
hh_projects <- sum(base_df$two_by_two_cell_A == "HH", na.rm = TRUE)
missing_applicant <- qc_df %>% filter(metric == "missing_applicant_type_row_count") %>% pull(value)

get_era_value <- function(outcome_family, era, cell) {
  era_summary_df %>%
    filter(summary_family == "two_by_two", outcome_family == !!outcome_family, era == !!era, two_by_two_cell_A == !!cell) %>%
    summarise(value = first(value)) %>%
    pull(value)
}

get_model_estimate <- function(outcome_family, sample_label, control_layer, era, functional_form) {
  model_summary_df %>%
    filter(
      sample_label == !!sample_label,
      control_layer == !!control_layer,
      term_group == "homeowner_x_redev",
      outcome_family == !!outcome_family,
      era == !!era,
      functional_form == !!functional_form
    ) %>%
    summarise(
      estimate = first(estimate),
      std_error = first(std_error),
      p_value = first(p_value)
    )
}

private_share_2010s <- era_summary_df %>%
  filter(summary_family == "applicant_split", outcome_family == "private_initial_apps_share", era == "2010-2019") %>%
  pull(value)

public_hpd_share_2010s <- era_summary_df %>%
  filter(summary_family == "applicant_split", outcome_family == "public_hpd_apps_share", era == "2010-2019") %>%
  pull(value)

rezoning_share_2010s <- era_summary_df %>%
  filter(summary_family == "action_split", outcome_family == "rezoning_or_special_apps_share", era == "2010-2019") %>%
  pull(value)

public_land_share_2010s <- era_summary_df %>%
  filter(summary_family == "action_split", outcome_family == "public_land_or_disposition_apps_share", era == "2010-2019") %>%
  pull(value)

hh_apps_2010s <- get_era_value("initial_apps_per_10k", "2010-2019", "HH")
lh_apps_2010s <- get_era_value("initial_apps_per_10k", "2010-2019", "LH")
hh_private_apps_2010s <- get_era_value("private_initial_apps_per_10k", "2010-2019", "HH")
lh_private_apps_2010s <- get_era_value("private_initial_apps_per_10k", "2010-2019", "LH")
hh_completion_2010_2015 <- get_era_value("completion_share", "2010-2015", "HH")
lh_completion_2010_2015 <- get_era_value("completion_share", "2010-2015", "LH")
hh_failure_2010_2015 <- get_era_value("failure_share", "2010-2015", "HH")
lh_failure_2010_2015 <- get_era_value("failure_share", "2010-2015", "LH")
hh_yield_2010_2015 <- get_era_value("linked_nb_50_plus_rate_0_10", "2010-2015", "HH")
lh_yield_2010_2015 <- get_era_value("linked_nb_50_plus_rate_0_10", "2010-2015", "LH")
hh_add_units_2010_2015 <- get_era_value("linked_gross_add_units_per_app_0_10", "2010-2015", "HH")
lh_add_units_2010_2015 <- get_era_value("linked_gross_add_units_per_app_0_10", "2010-2015", "LH")

private_apps_model <- get_model_estimate("private_initial_apps", "all_nyc", "4_all_blocks", "2020-2025", "linear_occ")
public_hpd_model <- get_model_estimate("public_hpd_apps", "all_nyc", "4_all_blocks", "2020-2025", "linear_occ")
completion_model <- get_model_estimate("completion_share", "all_nyc", "4_all_blocks", "2010-2015", "linear_share")
failure_model <- get_model_estimate("failure_share", "all_nyc", "4_all_blocks", "2010-2015", "linear_share")
yield_model <- get_model_estimate("linked_nb_50_plus_rate", "all_nyc", "4_all_blocks", "2016-2020", "linear_yield")
gross_add_model <- get_model_estimate("linked_gross_add_units_per_app", "all_nyc", "4_all_blocks", "2016-2020", "linear_yield")

non_manhattan_private <- get_model_estimate("private_initial_apps", "non_manhattan_only", "4_all_blocks", "2020-2025", "linear_occ")
manhattan_private <- get_model_estimate("private_initial_apps", "manhattan_only", "4_all_blocks", "2020-2025", "linear_occ")

interpretation_bucket <- "B. Mixed support"
if (!is.na(private_apps_model$estimate) &&
    private_apps_model$estimate < 0 &&
    !is.na(yield_model$estimate) &&
    yield_model$estimate < 0 &&
    !is.na(non_manhattan_private$estimate) &&
    non_manhattan_private$estimate < 0) {
  interpretation_bucket <- "A. Stronger support for a local-control channel"
}
if (!is.na(private_apps_model$estimate) &&
    private_apps_model$estimate >= 0 &&
    !is.na(public_hpd_model$estimate) &&
    public_hpd_model$estimate < 0) {
  interpretation_bucket <- "C. Weaker support; evidence leans more toward city-targeting than local private entry"
}

nested_excerpt_df <- nested_diag_df %>%
  filter(
    outcome_family %in% c("private_initial_apps", "public_hpd_apps", "completion_share", "failure_share", "linked_nb_50_plus_rate", "linked_gross_add_units_per_app"),
    term_group == "homeowner_x_redev"
  ) %>%
  arrange(outcome_family, era)

md_lines <- c(
  "# ZAP / ULURP Mechanism First Pass",
  "",
  "## Objective",
  "This task asks whether the homeowner-exposure pattern is showing up in the discretionary planning pipeline itself. The focus is applications, attrition, and approval-to-build yield for housing-oriented ULURP projects, interacted with the existing community-district redevelopment-potential measure.",
  "",
  "## Data Used",
  "- Staged ZAP project data, restricted to the existing housing-oriented ULURP universe.",
  "- Exact project-level ZAP-to-HDB exact-BBL linkage for post-2010 build-out yield.",
  "- CD redevelopment-potential baseline from the existing first-pass redevelopment task.",
  "- No community-board votes, borough-president recommendations, CPC vote details, council vote details, or testimony data are in the current repo.",
  "",
  "## Project Base",
  paste0("- Housing-oriented ULURP project count: ", fmt_num(total_projects, 0), "."),
  paste0("- Private applicant projects: ", fmt_num(private_projects, 0), "; public applicant projects: ", fmt_num(public_projects, 0), "."),
  paste0("- High-homeowner / high-redevelopment (`HH`) projects: ", fmt_num(hh_projects, 0), "."),
  paste0("- Missing applicant-type rows in staged ZAP: ", fmt_num(missing_applicant, 0), "."),
  "",
  "## Descriptive 2x2 Patterns",
  paste0("- Applications per 10,000 occupied units, `2010-2019`: `LH = ", fmt_num(lh_apps_2010s), "`, `HH = ", fmt_num(hh_apps_2010s), "`."),
  paste0("- Private applications per 10,000, `2010-2019`: `LH = ", fmt_num(lh_private_apps_2010s), "`, `HH = ", fmt_num(hh_private_apps_2010s), "`."),
  paste0("- Completion share, `2010-2015` mature cohorts: `LH = ", fmt_num(lh_completion_2010_2015), "`, `HH = ", fmt_num(hh_completion_2010_2015), "`."),
  paste0("- Failure share, `2010-2015` mature cohorts: `LH = ", fmt_num(lh_failure_2010_2015), "`, `HH = ", fmt_num(hh_failure_2010_2015), "`."),
  paste0("- Linked `50+` build-out rate, `2010-2015`: `LH = ", fmt_num(lh_yield_2010_2015), "`, `HH = ", fmt_num(hh_yield_2010_2015), "`."),
  paste0("- Linked gross-add units per app, `2010-2015`: `LH = ", fmt_num(lh_add_units_2010_2015), "`, `HH = ", fmt_num(hh_add_units_2010_2015), "`."),
  "",
  "## Applicant And Action Mix",
  paste0("- Private-app share in `2010-2019`: ", fmt_num(private_share_2010s), "."),
  paste0("- Public-HPD proxy share in `2010-2019`: ", fmt_num(public_hpd_share_2010s), "."),
  paste0("- Rezoning/special-permit share in `2010-2019`: ", fmt_num(rezoning_share_2010s), "."),
  paste0("- Public-land/disposition share in `2010-2019`: ", fmt_num(public_land_share_2010s), "."),
  "",
  "## Interaction Diagnostics",
  paste0("- All-controls homeowner × redevelopment estimate for private applications, `2020-2025`: `", fmt_num(private_apps_model$estimate), "` (SE `", fmt_num(private_apps_model$std_error), "`)."),
  paste0("- All-controls homeowner × redevelopment estimate for public-HPD applications, `2020-2025`: `", fmt_num(public_hpd_model$estimate), "` (SE `", fmt_num(public_hpd_model$std_error), "`)."),
  paste0("- All-controls homeowner × redevelopment estimate for completion share, `2010-2015`: `", fmt_num(completion_model$estimate), "` (SE `", fmt_num(completion_model$std_error), "`)."),
  paste0("- All-controls homeowner × redevelopment estimate for failure share, `2010-2015`: `", fmt_num(failure_model$estimate), "` (SE `", fmt_num(failure_model$std_error), "`)."),
  paste0("- All-controls homeowner × redevelopment estimate for linked `50+` build-out yield, `2016-2020` relative to `2010-2015` using the identified `0-5` window: `", fmt_num(yield_model$estimate), "` (SE `", fmt_num(yield_model$std_error), "`)."),
  paste0("- All-controls homeowner × redevelopment estimate for linked gross-add units per app, `2016-2020` relative to `2010-2015` using the identified `0-5` window: `", fmt_num(gross_add_model$estimate), "` (SE `", fmt_num(gross_add_model$std_error), "`)."),
  "",
  "## Manhattan Sensitivity",
  paste0("- Private-application interaction, non-Manhattan only, `2020-2025`: `", fmt_num(non_manhattan_private$estimate), "`."),
  paste0("- Private-application interaction, Manhattan only, `2020-2025`: `", fmt_num(manhattan_private$estimate), "`."),
  "- The regression output also includes leave-one-borough-out and `101/105/106/108` drop samples for the same interaction object.",
  "",
  "## Interpretation",
  paste0("- Classification: **", interpretation_bucket, "**."),
  "- Read this as a staged-data diagnostic only. A stronger private-application gap points more toward local discretionary pressure or deterrence. A stronger public/HPD gap points more toward city-targeting. A yield gap with similar application rates points more toward later-stage approval-to-build friction.",
  "",
  "## What This Cannot Yet Say",
  "- This phase does not observe local opposition directly.",
  "- It does not include community-board votes, BP recommendations, CPC vote detail, council vote detail, hearing testimony, or organized-opposition measures.",
  "- It does not yet split post-2010 HDB production into a clean HPD/public-site/subsidized taxonomy outside the ZAP-linked sample.",
  "",
  "## Next Data Needs",
  "- Community-board, BP, CPC, and council process data for the same ZAP projects.",
  "- Cleaner public/private and subsidy flags for post-2010 housing production.",
  "- If the mechanism remains interesting, a next pass should trace application entry, approval, and completion separately within the same project cohorts."
)

temp_md <- tempfile(fileext = ".md")
writeLines(md_lines, temp_md)
copy_if_changed(temp_md, "../output/zap_ulurp_redev_memo.md")

tex_lines <- c(
  "\\documentclass[11pt]{article}",
  "\\usepackage[margin=1in]{geometry}",
  "\\usepackage{booktabs}",
  "\\usepackage{graphicx}",
  "\\usepackage{hyperref}",
  "\\usepackage{float}",
  "\\usepackage{longtable}",
  "\\begin{document}",
  "\\title{ZAP / ULURP Mechanism First Pass}",
  "\\date{}",
  "\\maketitle",
  "\\section*{Objective}",
  "This memo asks whether the homeowner-exposure pattern appears in the discretionary planning pipeline itself. The object is \\textit{homeowner exposure $\\times$ redevelopment potential}, using only staged ZAP, redevelopment, and HDB-linked data already in the repo.",
  "\\section*{Data And Construction}",
  "\\begin{itemize}",
  paste0("\\item Housing-oriented ULURP project rows in the staged ZAP universe: ", escape_tex(fmt_num(total_projects, 0)), "."),
  paste0("\\item Private applicants: ", escape_tex(fmt_num(private_projects, 0)), "; public applicants: ", escape_tex(fmt_num(public_projects, 0)), "."),
  paste0("\\item High-homeowner / high-redevelopment projects: ", escape_tex(fmt_num(hh_projects, 0)), "."),
  paste0("\\item Missing applicant-type rows in staged ZAP: ", escape_tex(fmt_num(missing_applicant, 0)), "."),
  "\\item No community-board votes, BP recommendations, CPC vote details, council vote details, or testimony data are currently in the repo.",
  "\\end{itemize}",
  "\\section*{Headline Descriptive Comparisons}",
  "\\begin{center}",
  "\\begin{tabular}{lrr}",
  "\\toprule",
  "Outcome & LH & HH \\\\",
  "\\midrule",
  paste0("Applications per 10,000, 2010--2019 & ", escape_tex(fmt_num(lh_apps_2010s)), " & ", escape_tex(fmt_num(hh_apps_2010s)), " \\\\"),
  paste0("Private applications per 10,000, 2010--2019 & ", escape_tex(fmt_num(lh_private_apps_2010s)), " & ", escape_tex(fmt_num(hh_private_apps_2010s)), " \\\\"),
  paste0("Completion share, 2010--2015 & ", escape_tex(fmt_num(lh_completion_2010_2015)), " & ", escape_tex(fmt_num(hh_completion_2010_2015)), " \\\\"),
  paste0("Failure share, 2010--2015 & ", escape_tex(fmt_num(lh_failure_2010_2015)), " & ", escape_tex(fmt_num(hh_failure_2010_2015)), " \\\\"),
  paste0("Linked 50+ build-out rate, 2010--2015 & ", escape_tex(fmt_num(lh_yield_2010_2015)), " & ", escape_tex(fmt_num(hh_yield_2010_2015)), " \\\\"),
  paste0("Linked gross-add units per app, 2010--2015 & ", escape_tex(fmt_num(lh_add_units_2010_2015)), " & ", escape_tex(fmt_num(hh_add_units_2010_2015)), " \\\\"),
  "\\bottomrule",
  "\\end{tabular}",
  "\\end{center}",
  "\\section*{Applicant And Action Mix}",
  "\\begin{itemize}",
  paste0("\\item Private-app share in 2010--2019: ", escape_tex(fmt_num(private_share_2010s)), "."),
  paste0("\\item Public-HPD proxy share in 2010--2019: ", escape_tex(fmt_num(public_hpd_share_2010s)), "."),
  paste0("\\item Rezoning / special-permit share in 2010--2019: ", escape_tex(fmt_num(rezoning_share_2010s)), "."),
  paste0("\\item Public-land / disposition share in 2010--2019: ", escape_tex(fmt_num(public_land_share_2010s)), "."),
  "\\end{itemize}",
  "\\section*{Regression Diagnostics}",
  "\\begin{center}",
  "\\begin{tabular}{lrr}",
  "\\toprule",
  "Outcome & Estimate & SE \\\\",
  "\\midrule",
  paste0("Private applications, 2020--2025 & ", escape_tex(fmt_num(private_apps_model$estimate)), " & ", escape_tex(fmt_num(private_apps_model$std_error)), " \\\\"),
  paste0("Public HPD applications, 2020--2025 & ", escape_tex(fmt_num(public_hpd_model$estimate)), " & ", escape_tex(fmt_num(public_hpd_model$std_error)), " \\\\"),
  paste0("Completion share, 2010--2015 & ", escape_tex(fmt_num(completion_model$estimate)), " & ", escape_tex(fmt_num(completion_model$std_error)), " \\\\"),
  paste0("Failure share, 2010--2015 & ", escape_tex(fmt_num(failure_model$estimate)), " & ", escape_tex(fmt_num(failure_model$std_error)), " \\\\"),
  paste0("Linked 50+ yield, 2016--2020 vs 2010--2015 & ", escape_tex(fmt_num(yield_model$estimate)), " & ", escape_tex(fmt_num(yield_model$std_error)), " \\\\"),
  paste0("Linked gross-add units per app, 2016--2020 vs 2010--2015 & ", escape_tex(fmt_num(gross_add_model$estimate)), " & ", escape_tex(fmt_num(gross_add_model$std_error)), " \\\\"),
  "\\bottomrule",
  "\\end{tabular}",
  "\\end{center}",
  "\\section*{Manhattan Sensitivity}",
  "\\begin{itemize}",
  paste0("\\item Non-Manhattan private-application interaction, 2020--2025: ", escape_tex(fmt_num(non_manhattan_private$estimate)), "."),
  paste0("\\item Manhattan private-application interaction, 2020--2025: ", escape_tex(fmt_num(manhattan_private$estimate)), "."),
  "\\item The model summary also includes leave-one-borough-out and `101/105/106/108` exclusion samples.",
  "\\end{itemize}",
  "\\section*{Interpretation}",
  paste0("\\textbf{Classification: ", escape_tex(interpretation_bucket), ".}"),
  "This phase uses only staged ZAP, redevelopment, and HDB-linked data. It does not contain local-opposition vote, recommendation, or testimony data, so the results should be read as a mechanism diagnostic rather than a causal test. The next decisive outside-data step is community-board, borough-president, CPC, and council process data tied to the same projects.",
  "\\section*{Figures}",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.95\\textwidth,page=1]{../input/zap_ulurp_redev_plots.pdf}",
  "\\caption{ZAP mechanism outcomes by homeowner and redevelopment cell.}",
  "\\end{figure}",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.95\\textwidth,page=2]{../input/zap_ulurp_redev_plots.pdf}",
  "\\caption{Applicant and action-family mix over time.}",
  "\\end{figure}",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.95\\textwidth,page=1]{../input/zap_ulurp_redev_coefficients.pdf}",
  "\\caption{Headline homeowner $\\times$ redevelopment interaction coefficients.}",
  "\\end{figure}",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.95\\textwidth,page=2]{../input/zap_ulurp_redev_coefficients.pdf}",
  "\\caption{Sample-split sensitivity for the interaction coefficient.}",
  "\\end{figure}",
  "\\end{document}"
)

temp_tex <- tempfile(fileext = ".tex")
writeLines(tex_lines, temp_tex)
copy_if_changed(temp_tex, "../output/zap_ulurp_redev_memo.tex")

cat("Wrote ZAP ULURP redevelopment memo sources to ../output\n")
