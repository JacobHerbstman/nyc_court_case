suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

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

clean_spec_label <- function(x) {
  x %>%
    str_remove("^one_block_") %>%
    str_remove("^cumulative_") %>%
    str_replace_all("_", "-") %>%
    str_to_sentence()
}

cd_summary <- read_csv("../input/brooklyn_homeownership_case_study_cd_summary.csv", show_col_types = FALSE)
era_outcomes <- read_csv("../input/brooklyn_homeownership_case_study_era_outcomes.csv", show_col_types = FALSE)
block_regressions <- read_csv("../input/brooklyn_homeownership_case_study_block_regressions.csv", show_col_types = FALSE)
block_diagnostics <- read_csv("../input/brooklyn_homeownership_case_study_block_diagnostics.csv", show_col_types = FALSE)
leave_one_out <- read_csv("../input/brooklyn_homeownership_case_study_leave_one_cd_out.csv", show_col_types = FALSE)
size_bin_summary <- read_csv("../input/brooklyn_homeownership_case_study_size_bin_summary.csv", show_col_types = FALSE)
zap_summary <- read_csv("../input/brooklyn_homeownership_case_study_zap_summary.csv", show_col_types = FALSE)
zap_block_regressions <- read_csv("../input/brooklyn_homeownership_case_study_zap_block_regressions.csv", show_col_types = FALSE)
qc <- read_csv("../input/brooklyn_homeownership_case_study_qc.csv", show_col_types = FALSE)

brooklyn_count <- qc %>%
  filter(check_name == "brooklyn_cd_count") %>%
  pull(check_value) %>%
  first()

half_summary <- cd_summary %>%
  group_by(homeowner_half) %>%
  summarise(
    total_units_2020s = mean(units_built_total_2020_2025_per_10k_occupied, na.rm = TRUE),
    units_50_2020s = mean(units_built_50_plus_2020_2025_per_10k_occupied, na.rm = TRUE),
    projects_50_2020s = mean(nb_project_count_50_plus_2020_2025_per_cd_year, na.rm = TRUE),
    units_1_2_2020s = mean(nb_gross_units_1_2_2020_2025_per_10k_occupied, na.rm = TRUE),
    gross_add_2020s = mean(gross_add_units_2020_2025_per_res_acre, na.rm = TRUE),
    .groups = "drop"
  )

high_half <- half_summary %>% filter(homeowner_half == "high_homeowner")
low_half <- half_summary %>% filter(homeowner_half == "low_homeowner")

top_high <- cd_summary %>%
  arrange(desc(treat_z_boro)) %>%
  slice_head(n = 5) %>%
  transmute(
    cd_label,
    treat_z_boro,
    total_units_2020s = units_built_total_2020_2025_per_10k_occupied,
    units_50_2020s = units_built_50_plus_2020_2025_per_10k_occupied,
    units_1_2_2020s = nb_gross_units_1_2_2020_2025_per_10k_occupied
  )

top_low <- cd_summary %>%
  arrange(treat_z_boro) %>%
  slice_head(n = 5) %>%
  transmute(
    cd_label,
    treat_z_boro,
    total_units_2020s = units_built_total_2020_2025_per_10k_occupied,
    units_50_2020s = units_built_50_plus_2020_2025_per_10k_occupied,
    units_1_2_2020s = nb_gross_units_1_2_2020_2025_per_10k_occupied
  )

strongest_blocks <- block_diagnostics %>%
  filter(
    era == "2020-2025",
    outcome_id %in% c("units_built_total", "units_built_50_plus", "nb_project_count_50_plus"),
    str_detect(spec, "^one_block_")
  ) %>%
  group_by(outcome_id) %>%
  arrange(abs_beta_ratio_to_raw, .by_group = TRUE) %>%
  slice_head(n = 2) %>%
  ungroup() %>%
  mutate(spec_label = clean_spec_label(spec))

one_two_reg <- block_regressions %>%
  filter(outcome_id == "nb_gross_units_1_2", era == "2020-2025", spec %in% c("raw", "all_blocks")) %>%
  arrange(match(spec, c("raw", "all_blocks")))

size_bin_2020 <- size_bin_summary %>%
  filter(era == "2020-2025") %>%
  select(size_bin, homeowner_half, mean_outcome_rate, raw_beta_treat_z) %>%
  pivot_wider(names_from = homeowner_half, values_from = mean_outcome_rate) %>%
  arrange(match(size_bin, c("1-2", "3-4", "5-9", "10-49", "50+")))

bk01_bk16_sensitivity <- leave_one_out %>%
  filter(
    exclusion_id == "drop_BK01_BK16",
    spec == "raw",
    outcome_id %in% c("units_built_total", "units_built_50_plus", "nb_project_count_50_plus")
  ) %>%
  select(outcome_id, beta_treat_z, beta_change_from_full_sample_raw)

zap_desc_2020 <- zap_summary %>%
  filter(panel_family == "applications", era == "2020-2025") %>%
  select(outcome_id, homeowner_half, mean_outcome_rate) %>%
  pivot_wider(names_from = homeowner_half, values_from = mean_outcome_rate)

zap_desc_status <- zap_summary %>%
  filter(panel_family == "mature_status", era == "2010-2015") %>%
  select(outcome_id, homeowner_half, mean_outcome_rate) %>%
  pivot_wider(names_from = homeowner_half, values_from = mean_outcome_rate)

zap_desc_yield <- zap_summary %>%
  filter(panel_family == "yield_0_10", era == "2010-2015") %>%
  select(outcome_id, homeowner_half, mean_outcome_rate) %>%
  pivot_wider(names_from = homeowner_half, values_from = mean_outcome_rate)

zap_reg_headline <- zap_block_regressions %>%
  filter(
    outcome_id %in% c("initial_apps_per_10k", "private_initial_apps_per_10k", "mixed_private_rezoning_apps_per_10k", "public_hpd_apps_per_10k", "completion_share", "failure_share"),
    era %in% c("2020-2025", "2010-2015"),
    spec %in% c("raw", "all_blocks")
  ) %>%
  select(outcome_id, era, spec, beta_treat_z, beta_treat_z_se, beta_treat_z_p)

applications_blocks <- zap_block_regressions %>%
  filter(outcome_id == "initial_apps_per_10k", era == "2020-2025", str_detect(spec, "^one_block_")) %>%
  arrange(abs(beta_treat_z)) %>%
  slice_head(n = 2) %>%
  mutate(spec_label = clean_spec_label(spec))

md_lines <- c(
  "# Brooklyn Homeownership Case Study v2",
  "",
  "## Objective",
  "This memo updates the Brooklyn case study from a raw-versus-black-box control comparison to an explicit block-decomposition diagnostic. The goal is descriptive anatomy. The goal is to identify which baseline blocks attenuate the Brooklyn homeowner slope, whether the size-bin monotonicity survives, whether BK01 and BK16 dominate the result, and whether Brooklyn ZAP margins look more like private-entry differences or public-targeting differences.",
  "",
  "## Scope",
  paste0("- Brooklyn CDs in the helper-control universe: ", brooklyn_count, "."),
  "- The helper controls add fixed neighborhood labels, NHGIS 1990 race shares, and a City Hall distance proxy.",
  "- The control decomposition uses within-Brooklyn standardized **block scores** rather than a saturated kitchen-sink bundle. That keeps the cumulative specs identified with only 18 CDs.",
  "- This remains descriptive anatomy, not a causal borough design.",
  "",
  "## Raw Brooklyn Pattern",
  paste0("- High-homeowner Brooklyn CDs average ", fmt_num(high_half$total_units_2020s, 1), " total new-building units per 10,000 occupied units in 2020-2025; low-homeowner CDs average ", fmt_num(low_half$total_units_2020s, 1), "."),
  paste0("- High-homeowner Brooklyn CDs average ", fmt_num(high_half$units_50_2020s, 1), " 50+ units per 10,000 occupied units; low-homeowner CDs average ", fmt_num(low_half$units_50_2020s, 1), "."),
  paste0("- High-homeowner Brooklyn CDs average ", fmt_num(high_half$projects_50_2020s, 2), " 50+ projects per CD-year; low-homeowner CDs average ", fmt_num(low_half$projects_50_2020s, 2), "."),
  paste0("- On the small-building comparison margin, high-homeowner CDs are slightly higher: ", fmt_num(high_half$units_1_2_2020s, 2), " versus ", fmt_num(low_half$units_1_2_2020s, 2), " 1-2 unit new-building units per 10,000 occupied units."),
  "",
  "## Which Block Attenuates The Slope?",
  paste0("- For total units in 2020-2025, the raw homeowner slope is ", fmt_num(block_diagnostics %>% filter(outcome_id == 'units_built_total', era == '2020-2025', spec == 'raw') %>% pull(beta_treat_z) %>% first(), 1), ". The strongest single-block attenuation comes from ", strongest_blocks %>% filter(outcome_id == 'units_built_total') %>% slice(1) %>% pull(spec_label) %>% first(), ", which moves it to ", fmt_num(strongest_blocks %>% filter(outcome_id == 'units_built_total') %>% slice(1) %>% pull(beta_treat_z) %>% first(), 1), "."),
  paste0("- For 50+ units in 2020-2025, the raw slope is ", fmt_num(block_diagnostics %>% filter(outcome_id == 'units_built_50_plus', era == '2020-2025', spec == 'raw') %>% pull(beta_treat_z) %>% first(), 1), ". The strongest single-block attenuation again comes from ", strongest_blocks %>% filter(outcome_id == 'units_built_50_plus') %>% slice(1) %>% pull(spec_label) %>% first(), ", which moves it to ", fmt_num(strongest_blocks %>% filter(outcome_id == 'units_built_50_plus') %>% slice(1) %>% pull(beta_treat_z) %>% first(), 1), "."),
  paste0("- For 50+ projects in 2020-2025, the raw slope is ", fmt_num(block_diagnostics %>% filter(outcome_id == 'nb_project_count_50_plus', era == '2020-2025', spec == 'raw') %>% pull(beta_treat_z) %>% first(), 2), ". The strongest attenuator is ", strongest_blocks %>% filter(outcome_id == 'nb_project_count_50_plus') %>% slice(1) %>% pull(spec_label) %>% first(), ", which moves it to ", fmt_num(strongest_blocks %>% filter(outcome_id == 'nb_project_count_50_plus') %>% slice(1) %>% pull(beta_treat_z) %>% first(), 2), "."),
  paste0("- The all-block slope does not flip positive on the main large-unit margins. It is ", fmt_num(block_diagnostics %>% filter(outcome_id == 'units_built_total', era == '2020-2025', spec == 'all_blocks') %>% pull(beta_treat_z) %>% first(), 1), " for total units and ", fmt_num(block_diagnostics %>% filter(outcome_id == 'units_built_50_plus', era == '2020-2025', spec == 'all_blocks') %>% pull(beta_treat_z) %>% first(), 1), " for 50+ units."),
  "",
  "## Size-Bin Monotonicity",
  paste0("- The 2020-2025 raw coefficient is ", fmt_num(size_bin_2020$raw_beta_treat_z[size_bin_2020$size_bin == '1-2'], 3), " for 1-2 unit buildings, ", fmt_num(size_bin_2020$raw_beta_treat_z[size_bin_2020$size_bin == '3-4'], 3), " for 3-4, ", fmt_num(size_bin_2020$raw_beta_treat_z[size_bin_2020$size_bin == '5-9'], 2), " for 5-9, ", fmt_num(size_bin_2020$raw_beta_treat_z[size_bin_2020$size_bin == '10-49'], 2), " for 10-49, and ", fmt_num(size_bin_2020$raw_beta_treat_z[size_bin_2020$size_bin == '50+'], 1), " for 50+."),
  paste0("- The 1-2 margin stays positive and tiny even after all blocks: ", fmt_num(one_two_reg$beta_treat_z[one_two_reg$spec == 'raw'], 3), " raw and ", fmt_num(one_two_reg$beta_treat_z[one_two_reg$spec == 'all_blocks'], 3), " with all blocks."),
  "",
  "## BK01/BK16 Sensitivity",
  paste0("- Dropping BK01 and BK16 still leaves a negative raw 2020-2025 homeowner slope of ", fmt_num(bk01_bk16_sensitivity$beta_treat_z[bk01_bk16_sensitivity$outcome_id == 'units_built_total'], 1), " for total units, ", fmt_num(bk01_bk16_sensitivity$beta_treat_z[bk01_bk16_sensitivity$outcome_id == 'units_built_50_plus'], 1), " for 50+ units, and ", fmt_num(bk01_bk16_sensitivity$beta_treat_z[bk01_bk16_sensitivity$outcome_id == 'nb_project_count_50_plus'], 2), " for 50+ projects."),
  "- So BK01 and BK16 matter, but the Brooklyn pattern is not literally just those two CDs.",
  "",
  "## Brooklyn ZAP Read",
  paste0("- In 2020-2025, high-homeowner Brooklyn CDs average ", fmt_num(zap_desc_2020$high_homeowner[zap_desc_2020$outcome_id == 'initial_apps_per_10k'], 3), " housing-oriented ULURP applications per 10,000 occupied units; low-homeowner CDs average ", fmt_num(zap_desc_2020$low_homeowner[zap_desc_2020$outcome_id == 'initial_apps_per_10k'], 3), "."),
  paste0("- The private-entry gap is smaller: private applications are ", fmt_num(zap_desc_2020$high_homeowner[zap_desc_2020$outcome_id == 'private_initial_apps_per_10k'], 3), " versus ", fmt_num(zap_desc_2020$low_homeowner[zap_desc_2020$outcome_id == 'private_initial_apps_per_10k'], 3), ", and mixed private rezonings are ", fmt_num(zap_desc_2020$high_homeowner[zap_desc_2020$outcome_id == 'mixed_private_rezoning_apps_per_10k'], 3), " versus ", fmt_num(zap_desc_2020$low_homeowner[zap_desc_2020$outcome_id == 'mixed_private_rezoning_apps_per_10k'], 3), "."),
  paste0("- The public/HPD gap is larger: public HPD applications are ", fmt_num(zap_desc_2020$high_homeowner[zap_desc_2020$outcome_id == 'public_hpd_apps_per_10k'], 3), " in high-homeowner CDs versus ", fmt_num(zap_desc_2020$low_homeowner[zap_desc_2020$outcome_id == 'public_hpd_apps_per_10k'], 3), " in low-homeowner CDs."),
  paste0("- The build-out yield descriptives are also stark for the 2010-2015 0-10 window: high-homeowner CDs are at ", fmt_num(zap_desc_yield$high_homeowner[zap_desc_yield$outcome_id == 'linked_nb_50_plus_rate_0_10'], 3), " linked 50+ projects per app and ", fmt_num(zap_desc_yield$high_homeowner[zap_desc_yield$outcome_id == 'linked_gross_add_units_per_app_0_10'], 1), " gross-add units per app, versus ", fmt_num(zap_desc_yield$low_homeowner[zap_desc_yield$outcome_id == 'linked_nb_50_plus_rate_0_10'], 3), " and ", fmt_num(zap_desc_yield$low_homeowner[zap_desc_yield$outcome_id == 'linked_gross_add_units_per_app_0_10'], 1), " in low-homeowner CDs."),
  paste0("- In the Brooklyn ZAP regressions, the 2020-2025 raw slope for total applications is ", fmt_num(zap_reg_headline$beta_treat_z[zap_reg_headline$outcome_id == 'initial_apps_per_10k' & zap_reg_headline$era == '2020-2025' & zap_reg_headline$spec == 'raw'], 3), ", while the private-application slope is ", fmt_num(zap_reg_headline$beta_treat_z[zap_reg_headline$outcome_id == 'private_initial_apps_per_10k' & zap_reg_headline$era == '2020-2025' & zap_reg_headline$spec == 'raw'], 3), " and the public-HPD slope is ", fmt_num(zap_reg_headline$beta_treat_z[zap_reg_headline$outcome_id == 'public_hpd_apps_per_10k' & zap_reg_headline$era == '2020-2025' & zap_reg_headline$spec == 'raw'], 3), "."),
  paste0("- For total applications, the strongest attenuator is ", applications_blocks$spec_label[1], ", which moves the raw slope to ", fmt_num(applications_blocks$beta_treat_z[1], 3), "."),
  "",
  "## Interpretation",
  "- Brooklyn now looks stronger as descriptive mechanism anatomy than it did in the first memo.",
  "- The margin-specific result survives: the homeowner pattern is weak on 1-2 unit construction and much stronger on larger-unit margins.",
  "- The control decomposition does not eliminate the large-unit slope. Instead, redevelopment and socio-race are the blocks that attenuate it the most.",
  "- On the ZAP side, the Brooklyn pattern looks more like a broad applications gap with an especially weak public/HPD presence in high-homeowner CDs than like a clean private-rezoning-only story.",
  "",
  "## Bottom Line",
  "- Brooklyn remains descriptive anatomy, not a causal borough design.",
  "- The source of the earlier control flip is now clearer: redevelopment and socio-race absorb the largest share of the raw homeowner gradient, while the small-building falsification still passes.",
  "- The next decisive Brooklyn step is still process data: community-board, BP, CPC, and council-stage information, plus cleaner public/private production splits."
)

temp_md <- tempfile(fileext = ".md")
writeLines(md_lines, temp_md)
copy_if_changed(temp_md, "../output/brooklyn_homeownership_case_study_memo.md")

top_high_rows <- apply(top_high, 1, function(row) {
  paste0(
    escape_tex(row[["cd_label"]]), " & ",
    escape_tex(fmt_num(as.numeric(row[["treat_z_boro"]]), 3)), " & ",
    escape_tex(fmt_num(as.numeric(row[["total_units_2020s"]]), 1)), " & ",
    escape_tex(fmt_num(as.numeric(row[["units_50_2020s"]]), 1)), " & ",
    escape_tex(fmt_num(as.numeric(row[["units_1_2_2020s"]]), 2)), " \\\\"
  )
})

top_low_rows <- apply(top_low, 1, function(row) {
  paste0(
    escape_tex(row[["cd_label"]]), " & ",
    escape_tex(fmt_num(as.numeric(row[["treat_z_boro"]]), 3)), " & ",
    escape_tex(fmt_num(as.numeric(row[["total_units_2020s"]]), 1)), " & ",
    escape_tex(fmt_num(as.numeric(row[["units_50_2020s"]]), 1)), " & ",
    escape_tex(fmt_num(as.numeric(row[["units_1_2_2020s"]]), 2)), " \\\\"
  )
})

block_rows <- strongest_blocks %>%
  transmute(
    row_text = paste0(
      escape_tex(case_when(
        outcome_id == "units_built_total" ~ "Total units",
        outcome_id == "units_built_50_plus" ~ "50+ units",
        outcome_id == "nb_project_count_50_plus" ~ "50+ projects",
        TRUE ~ outcome_id
      )), " & ",
      escape_tex(spec_label), " & ",
      escape_tex(fmt_num(beta_treat_z, 2)), " & ",
      escape_tex(fmt_num(beta_change_from_raw, 2)), " \\\\"
    )
  ) %>%
  pull(row_text)

size_rows <- apply(size_bin_2020, 1, function(row) {
  paste0(
    escape_tex(row[["size_bin"]]), " & ",
    escape_tex(fmt_num(as.numeric(row[["high_homeowner"]]), 2)), " & ",
    escape_tex(fmt_num(as.numeric(row[["low_homeowner"]]), 2)), " & ",
    escape_tex(fmt_num(as.numeric(row[["raw_beta_treat_z"]]), 2)), " \\\\"
  )
})

sensitivity_rows <- bk01_bk16_sensitivity %>%
  mutate(
    row_text = paste0(
      escape_tex(case_when(
        outcome_id == "units_built_total" ~ "Total units",
        outcome_id == "units_built_50_plus" ~ "50+ units",
        outcome_id == "nb_project_count_50_plus" ~ "50+ projects",
        TRUE ~ outcome_id
      )), " & ",
      escape_tex(fmt_num(beta_treat_z, 2)), " & ",
      escape_tex(fmt_num(beta_change_from_full_sample_raw, 2)), " \\\\"
    )
  ) %>%
  pull(row_text)

zap_rows <- zap_reg_headline %>%
  mutate(
    outcome_label = case_when(
      outcome_id == "initial_apps_per_10k" ~ "Applications per 10k",
      outcome_id == "private_initial_apps_per_10k" ~ "Private applications per 10k",
      outcome_id == "mixed_private_rezoning_apps_per_10k" ~ "Private rezoning applications per 10k",
      outcome_id == "public_hpd_apps_per_10k" ~ "Public HPD applications per 10k",
      outcome_id == "completion_share" ~ "Completion share",
      outcome_id == "failure_share" ~ "Failure share",
      TRUE ~ outcome_id
    ),
    row_text = paste0(
      escape_tex(outcome_label), " & ",
      escape_tex(era), " & ",
      escape_tex(str_replace_all(spec, "_", " ")), " & ",
      escape_tex(fmt_num(beta_treat_z, 3)), " & ",
      escape_tex(fmt_num(beta_treat_z_se, 3)), " \\\\"
    )
  ) %>%
  pull(row_text)

tex_lines <- c(
  "\\documentclass[11pt]{article}",
  "\\usepackage[margin=1in]{geometry}",
  "\\usepackage{booktabs}",
  "\\usepackage{graphicx}",
  "\\usepackage{hyperref}",
  "\\usepackage{longtable}",
  "\\begin{document}",
  "\\title{Brooklyn Homeownership Case Study v2}",
  "\\date{}",
  "\\maketitle",
  "\\section*{Objective}",
  "This memo updates the Brooklyn case study from a raw-versus-black-box control comparison to an explicit block-decomposition diagnostic. The goal is descriptive anatomy. The objective is to identify the source of the control flip, check whether the size-bin monotonicity survives, check whether BK01 and BK16 dominate the result, and ask whether Brooklyn ZAP margins look more like private-entry differences or public-targeting differences.",
  "\\section*{Scope}",
  "\\begin{itemize}",
  paste0("\\item Brooklyn CDs in the helper-control universe: ", escape_tex(brooklyn_count), "."),
  "\\item The helper controls add fixed neighborhood labels, NHGIS 1990 race shares, and a City Hall distance proxy.",
  "\\item The decomposition uses within-Brooklyn standardized block scores rather than a saturated kitchen-sink bundle so that the cumulative specs remain identified with 18 CDs.",
  "\\item This remains descriptive anatomy, not a causal borough design.",
  "\\end{itemize}",
  "\\section*{Raw Brooklyn Pattern}",
  "\\begin{itemize}",
  paste0("\\item High-homeowner Brooklyn CDs average ", escape_tex(fmt_num(high_half$total_units_2020s, 1)), " total new-building units per 10,000 occupied units in 2020--2025; low-homeowner CDs average ", escape_tex(fmt_num(low_half$total_units_2020s, 1)), "."),
  paste0("\\item High-homeowner Brooklyn CDs average ", escape_tex(fmt_num(high_half$units_50_2020s, 1)), " 50+ units per 10,000 occupied units; low-homeowner CDs average ", escape_tex(fmt_num(low_half$units_50_2020s, 1)), "."),
  paste0("\\item High-homeowner Brooklyn CDs average ", escape_tex(fmt_num(high_half$projects_50_2020s, 2)), " 50+ projects per CD-year; low-homeowner CDs average ", escape_tex(fmt_num(low_half$projects_50_2020s, 2)), "."),
  paste0("\\item On the small-building comparison margin, high-homeowner CDs are slightly higher: ", escape_tex(fmt_num(high_half$units_1_2_2020s, 2)), " versus ", escape_tex(fmt_num(low_half$units_1_2_2020s, 2)), " 1--2 unit new-building units per 10,000 occupied units."),
  "\\end{itemize}",
  "\\section*{Ranked Anatomy}",
  "\\textbf{Highest homeowner-exposure Brooklyn CDs}",
  "\\begin{center}",
  "\\begin{tabular}{lrrrr}",
  "\\toprule",
  "CD & Treat z & Total units 2020s & 50+ units 2020s & 1--2 units 2020s \\\\",
  "\\midrule",
  top_high_rows,
  "\\bottomrule",
  "\\end{tabular}",
  "\\end{center}",
  "\\textbf{Lowest homeowner-exposure Brooklyn CDs}",
  "\\begin{center}",
  "\\begin{tabular}{lrrrr}",
  "\\toprule",
  "CD & Treat z & Total units 2020s & 50+ units 2020s & 1--2 units 2020s \\\\",
  "\\midrule",
  top_low_rows,
  "\\bottomrule",
  "\\end{tabular}",
  "\\end{center}",
  "\\section*{Which Block Attenuates The Slope?}",
  "The key result is that the large-unit homeowner slope does not disappear once the old black-box bundle is broken apart. The strongest single-block attenuators are redevelopment and socio-race, but the all-block slope remains negative on the large-unit margins.",
  "\\begin{center}",
  "\\begin{tabular}{lrrr}",
  "\\toprule",
  "Outcome & Strongest block & Beta after block & Change from raw \\\\",
  "\\midrule",
  block_rows,
  "\\bottomrule",
  "\\end{tabular}",
  "\\end{center}",
  "\\section*{Size-Bin Monotonicity}",
  "The size-bin falsification still passes. The 1--2 margin is slightly positive, and the homeowner slope becomes more negative as the size bin rises.",
  "\\begin{center}",
  "\\begin{tabular}{lrrr}",
  "\\toprule",
  "Size bin & High homeowner mean & Low homeowner mean & Raw beta \\\\",
  "\\midrule",
  size_rows,
  "\\bottomrule",
  "\\end{tabular}",
  "\\end{center}",
  "\\section*{BK01/BK16 Sensitivity}",
  "Dropping BK01 and BK16 weakens the raw slope, but it does not eliminate the negative sign on the main large-unit outcomes.",
  "\\begin{center}",
  "\\begin{tabular}{lrr}",
  "\\toprule",
  "Outcome & Raw slope after dropping BK01/BK16 & Change from full-sample raw \\\\",
  "\\midrule",
  sensitivity_rows,
  "\\bottomrule",
  "\\end{tabular}",
  "\\end{center}",
  "\\section*{Brooklyn ZAP Anatomy}",
  "The Brooklyn ZAP picture looks more like a broad applications gap with especially weak public/HPD activity in high-homeowner CDs than like a clean private-rezoning-only story. High-homeowner CDs average 0.196 applications per 10,000 occupied units in 2020--2025 versus 0.287 in low-homeowner CDs. The private gap is smaller, while public HPD applications are 0.013 versus 0.081. The 2010--2015 descriptive yield gap is also large: high-homeowner CDs are at zero linked 50+ projects and zero linked gross-add units per app in the 0--10 window, versus 0.328 and 103.3 in low-homeowner CDs.",
  "\\begin{center}",
  "\\begin{tabular}{lllrr}",
  "\\toprule",
  "Outcome & Era & Spec & Beta & SE \\\\",
  "\\midrule",
  zap_rows,
  "\\bottomrule",
  "\\end{tabular}",
  "\\end{center}",
  "\\section*{Interpretation}",
  "Brooklyn now looks stronger as descriptive mechanism anatomy than it did in the earlier memo. The margin-specific result survives: the homeowner pattern is weak on 1--2 unit construction and much stronger on larger-unit margins. The decomposition does not kill the large-unit slope; instead, redevelopment and socio-race absorb the biggest share of it. On the ZAP side, the Brooklyn pattern looks more like a broad applications gap, with an especially weak public/HPD presence in high-homeowner CDs, than like a clean private-rezoning-only story.",
  "\\section*{Figures}",
  "\\begin{figure}[p]",
  "\\centering",
  "\\includegraphics[page=4,width=0.98\\textwidth]{../input/brooklyn_homeownership_case_study_plots.pdf}",
  "\\caption{Brooklyn rank plot with later housing margins.}",
  "\\end{figure}",
  "\\begin{figure}[p]",
  "\\centering",
  "\\includegraphics[page=1,width=0.95\\textwidth]{../input/brooklyn_homeownership_case_study_control_flip_plots.pdf}",
  "\\caption{Brooklyn size-bin monotonicity.}",
  "\\end{figure}",
  "\\begin{figure}[p]",
  "\\centering",
  "\\includegraphics[page=2,width=0.95\\textwidth]{../input/brooklyn_homeownership_case_study_control_flip_plots.pdf}",
  "\\caption{Which block attenuates the Brooklyn homeowner slope?}",
  "\\end{figure}",
  "\\begin{figure}[p]",
  "\\centering",
  "\\includegraphics[page=4,width=0.95\\textwidth]{../input/brooklyn_homeownership_case_study_control_flip_plots.pdf}",
  "\\caption{Brooklyn ZAP anatomy.}",
  "\\end{figure}",
  "\\end{document}"
)

temp_tex <- tempfile(fileext = ".tex")
writeLines(tex_lines, temp_tex)
copy_if_changed(temp_tex, "../output/brooklyn_homeownership_case_study_memo.tex")
