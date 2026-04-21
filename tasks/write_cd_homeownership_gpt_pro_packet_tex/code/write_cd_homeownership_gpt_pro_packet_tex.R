# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/write_cd_homeownership_gpt_pro_packet_tex/code")
# cd_homeownership_1990_measure_csv <- "../input/cd_homeownership_1990_measure.csv"
# comparison_summary_csv <- "../input/cd_homeownership_1990_measure_comparison_summary.csv"
# proxy_validation_summary_csv <- "../input/mappluto_construction_proxy_validation_summary.csv"
# proxy_decadal_csv <- "../input/mappluto_construction_proxy_vs_decadal.csv"
# long_city_year_csv <- "../input/cd_homeownership_long_units_city_year.csv"
# long_tercile_year_csv <- "../input/cd_homeownership_long_units_tercile_year.csv"
# long_tercile_era_csv <- "../input/cd_homeownership_long_units_tercile_era.csv"
# out_tex <- "../output/cd_homeownership_gpt_pro_packet.tex"
# out_total_units_pdf <- "../output/cd_homeownership_total_units_tercile.pdf"
# out_units_50_plus_pdf <- "../output/cd_homeownership_units_50_plus_tercile.pdf"
# out_city_totals_pdf <- "../output/cd_homeownership_long_city_totals.pdf"
# out_validation_scatter_pdf <- "../output/cd_homeownership_proxy_validation_scatter_1990s.pdf"

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(knitr)
  library(readr)
  library(stringr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 12) {
  stop("Expected 12 arguments: cd_homeownership_1990_measure_csv comparison_summary_csv proxy_validation_summary_csv proxy_decadal_csv long_city_year_csv long_tercile_year_csv long_tercile_era_csv out_tex out_total_units_pdf out_units_50_plus_pdf out_city_totals_pdf out_validation_scatter_pdf")
}

cd_homeownership_1990_measure_csv <- args[1]
comparison_summary_csv <- args[2]
proxy_validation_summary_csv <- args[3]
proxy_decadal_csv <- args[4]
long_city_year_csv <- args[5]
long_tercile_year_csv <- args[6]
long_tercile_era_csv <- args[7]
out_tex <- args[8]
out_total_units_pdf <- args[9]
out_units_50_plus_pdf <- args[10]
out_city_totals_pdf <- args[11]
out_validation_scatter_pdf <- args[12]

latex_escape <- function(x) {
  x |>
    str_replace_all("\\\\", "\\\\textbackslash{}") |>
    str_replace_all("([#$%&_{}])", "\\\\\\1") |>
    str_replace_all("~", "\\\\textasciitilde{}") |>
    str_replace_all("\\^", "\\\\textasciicircum{}")
}

fmt_num <- function(x, digits = 1) {
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

treatment_df <- read_csv(cd_homeownership_1990_measure_csv, show_col_types = FALSE, na = c("", "NA"))
comparison_summary_df <- read_csv(comparison_summary_csv, show_col_types = FALSE, na = c("", "NA"))
proxy_validation_df <- read_csv(proxy_validation_summary_csv, show_col_types = FALSE, na = c("", "NA"))
proxy_decadal_df <- read_csv(proxy_decadal_csv, show_col_types = FALSE, na = c("", "NA"))
long_city_year_df <- read_csv(long_city_year_csv, show_col_types = FALSE, na = c("", "NA"))
long_tercile_year_df <- read_csv(long_tercile_year_csv, show_col_types = FALSE, na = c("", "NA"))
long_tercile_era_df <- read_csv(long_tercile_era_csv, show_col_types = FALSE, na = c("", "NA"))

treatment_overall <- treatment_df |>
  summarize(
    h_cd_min_pct = min(h_cd_1990_pct, na.rm = TRUE),
    h_cd_max_pct = max(h_cd_1990_pct, na.rm = TRUE),
    treat_min_pp = min(treat_pp, na.rm = TRUE),
    treat_max_pp = max(treat_pp, na.rm = TRUE)
  )

treatment_borough_table <- treatment_df |>
  group_by(borough_name) |>
  summarize(
    districts = n(),
    borough_homeownership_pct = round(first(h_b_1990_pct), 1),
    cd_homeownership_min_pct = round(min(h_cd_1990_pct, na.rm = TRUE), 1),
    cd_homeownership_max_pct = round(max(h_cd_1990_pct, na.rm = TRUE), 1),
    treat_sd_pp = round(sd(treat_pp, na.rm = TRUE), 1),
    .groups = "drop"
  ) |>
  arrange(match(borough_name, c("Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"))) |>
  rename(
    Borough = borough_name,
    Districts = districts,
    `Borough H%` = borough_homeownership_pct,
    `CD min H%` = cd_homeownership_min_pct,
    `CD max H%` = cd_homeownership_max_pct,
    `SD treat pp` = treat_sd_pp
  )

overlay_table <- comparison_summary_df |>
  filter(scope == "overall") |>
  transmute(
    `corr(Hcd)` = round(correlation_homeowner_share, 4),
    `MAE Hcd gap pp` = round(mae_homeowner_share_gap_pp, 3),
    `max abs Hcd gap pp` = round(max_abs_homeowner_share_gap_pp, 3),
    `corr(treat)` = round(correlation_treat_pp, 4),
    `MAE treat gap pp` = round(mae_treat_gap_pp, 3),
    `max abs treat gap pp` = round(max_abs_treat_gap_pp, 3)
  )

proxy_validation_table <- proxy_validation_df |>
  filter(metric %in% c(
    "city_year_corr_proxy_vs_hdb_nb_units_2010_2025",
    "city_year_corr_proxy_vs_hdb_50_plus_units_2010_2025",
    "borough_year_corr_proxy_vs_hdb_nb_units_2010_2025",
    "borough_year_corr_proxy_vs_hdb_unit_shares_2010_2025",
    "city_total_ratio_proxy_to_hdb_nb_units_2010_2025",
    "cd_corr_proxy_vs_exact_1990s_structure_built_2000",
    "cd_corr_proxy_vs_exact_1980s_structure_built_2000",
    "city_total_ratio_proxy_to_exact_1990s_structure_built_2000",
    "city_total_ratio_proxy_to_exact_1980s_structure_built_2000"
  )) |>
  transmute(
    Metric = case_when(
      metric == "city_year_corr_proxy_vs_hdb_nb_units_2010_2025" ~ "City-year corr: proxy vs HDB total NB units",
      metric == "city_year_corr_proxy_vs_hdb_50_plus_units_2010_2025" ~ "City-year corr: proxy vs HDB 50+ NB units",
      metric == "borough_year_corr_proxy_vs_hdb_nb_units_2010_2025" ~ "Borough-year corr: proxy vs HDB total NB units",
      metric == "borough_year_corr_proxy_vs_hdb_unit_shares_2010_2025" ~ "Borough-year corr: proxy vs HDB unit shares",
      metric == "city_total_ratio_proxy_to_hdb_nb_units_2010_2025" ~ "City total ratio: proxy / HDB total NB units",
      metric == "cd_corr_proxy_vs_exact_1990s_structure_built_2000" ~ "CD corr: proxy 1990s vs exact 2000 structure-built",
      metric == "cd_corr_proxy_vs_exact_1980s_structure_built_2000" ~ "CD corr: proxy 1980s vs exact 2000 structure-built",
      metric == "city_total_ratio_proxy_to_exact_1990s_structure_built_2000" ~ "City ratio: proxy 1990s / exact 2000 structure-built",
      metric == "city_total_ratio_proxy_to_exact_1980s_structure_built_2000" ~ "City ratio: proxy 1980s / exact 2000 structure-built",
      TRUE ~ metric
    ),
    Value = round(as.numeric(value), 3)
  )

era_share_table <- long_tercile_era_df |>
  filter(series_family %in% c("units_built_total", "units_built_50_plus")) |>
  select(series_label, era, treat_tercile_label, borough_outcome_share) |>
  mutate(
    series_label = recode(series_label, "Units built: total" = "Total units", "Units built: 50+" = "50+ unit buildings"),
    borough_outcome_share = round(100 * borough_outcome_share, 1)
  ) |>
  tidyr::pivot_wider(names_from = treat_tercile_label, values_from = borough_outcome_share) |>
  arrange(match(series_label, c("Total units", "50+ unit buildings")), era) |>
  rename(
    Series = series_label,
    Era = era
  )

city_total_table <- long_city_year_df |>
  filter(
    (series_family == "units_built_total" & year %in% c(1980, 1985, 1989, 1990, 1995, 2000, 2005, 2009, 2010, 2015, 2020, 2025)) |
      (series_family == "gross_add_units_observed" & year %in% c(2010, 2015, 2020, 2025))
  ) |>
  transmute(
    Year = year,
    Series = case_when(
      series_family == "units_built_total" ~ "Units built: total",
      series_family == "gross_add_units_observed" ~ "Gross additions observed",
      TRUE ~ series_label
    ),
    `City units` = round(city_outcome_total)
  ) |>
  arrange(Year, Series)

proxy_1990s_scatter_df <- proxy_decadal_df |>
  filter(
    geography_level == "cd",
    decade == "1990s",
    comparator_source == "dcp_profiles_exact_2000_structure_built"
  )

total_units_plot_df <- long_tercile_year_df |>
  filter(series_family == "units_built_total") |>
  mutate(treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")))

units_50_plus_plot_df <- long_tercile_year_df |>
  filter(series_family == "units_built_50_plus") |>
  mutate(treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")))

city_totals_plot_df <- long_city_year_df |>
  filter(series_family %in% c("units_built_total", "gross_add_units_observed")) |>
  mutate(
    series_label = factor(case_when(
      series_family == "units_built_total" ~ "Units built: total",
      series_family == "gross_add_units_observed" ~ "Gross additions observed"
    ), levels = c("Units built: total", "Gross additions observed"))
  )

ggsave(
  out_total_units_pdf,
  ggplot(total_units_plot_df, aes(x = year, y = borough_outcome_share, color = treat_tercile_label)) +
    geom_line(linewidth = 0.9) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = "Within-borough share", color = "Homeownership tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom"),
  width = 8.5,
  height = 4.75,
  device = "pdf"
)

ggsave(
  out_units_50_plus_pdf,
  ggplot(units_50_plus_plot_df, aes(x = year, y = borough_outcome_share, color = treat_tercile_label)) +
    geom_line(linewidth = 0.9) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = "Within-borough share", color = "Homeownership tercile") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom"),
  width = 8.5,
  height = 4.75,
  device = "pdf"
)

ggsave(
  out_city_totals_pdf,
  ggplot(city_totals_plot_df, aes(x = year, y = city_outcome_total, color = series_label)) +
    geom_line(linewidth = 0.9) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    scale_color_manual(values = c("Units built: total" = "#1b6ca8", "Gross additions observed" = "#d65f0e")) +
    labs(x = NULL, y = "Citywide units", color = NULL) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom"),
  width = 8.5,
  height = 4.75,
  device = "pdf"
)

ggsave(
  out_validation_scatter_pdf,
  ggplot(proxy_1990s_scatter_df, aes(x = comparator_units, y = proxy_units)) +
    geom_point(color = "#1b6ca8", alpha = 0.8) +
    geom_smooth(method = "lm", se = FALSE, color = "#d65f0e", linewidth = 0.8) +
    labs(
      x = "Exact 2000 CD count: units in structures built in the 1990s",
      y = "PLUTO proxy: surviving residential units built in the 1990s"
    ) +
    theme_minimal(base_size = 11),
  width = 6.5,
  height = 4.5,
  device = "pdf"
)

total_units_high_1985 <- era_share_table$High[era_share_table$Series == "Total units" & era_share_table$Era == "1985-1989"]
total_units_high_2020 <- era_share_table$High[era_share_table$Series == "Total units" & era_share_table$Era == "2020-2025"]
units_50_high_1985 <- era_share_table$High[era_share_table$Series == "50+ unit buildings" & era_share_table$Era == "1985-1989"]
units_50_high_2020 <- era_share_table$High[era_share_table$Series == "50+ unit buildings" & era_share_table$Era == "2020-2025"]

tex_lines <- c(
  "\\documentclass[11pt]{article}",
  "\\usepackage[margin=1in]{geometry}",
  "\\usepackage{booktabs}",
  "\\usepackage{graphicx}",
  "\\usepackage{float}",
  "\\usepackage{longtable}",
  "\\usepackage{array}",
  "\\usepackage[hidelinks]{hyperref}",
  "\\setlength{\\parskip}{0.6em}",
  "\\setlength{\\parindent}{0pt}",
  "\\begin{document}",
  "\\title{CD Homeownership and Housing Production Packet}",
  "\\date{April 17, 2026}",
  "\\maketitle",
  "\\section*{What This Packet Is}",
  "This memo packages the current state of the exact 1990 homeownership exposure and the housing-production series used to study it. It is designed as a compact handoff document for external review.",
  "\\section*{Bottom Line}",
  paste0(
    "The exact 1990 treatment varies substantially across community districts: CD homeownership runs from ",
    fmt_num(treatment_overall$h_cd_min_pct, 1),
    "\\% to ",
    fmt_num(treatment_overall$h_cd_max_pct, 1),
    "\\%, and borough-centered exposure $treat\\_pp$ runs from ",
    fmt_num(treatment_overall$treat_min_pp, 1),
    " to ",
    fmt_num(treatment_overall$treat_max_pp, 1),
    " percentage points."
  ),
  "The exact DCP-based treatment is extremely close to the earlier NHGIS overlay approximation, so the treatment side of the project now looks settled.",
  paste0(
    "For the pre-2010 period, the MapPLUTO \\texttt{yearbuilt} proxy looks usable as a descriptive bridge. It tracks exact DCP 2000 \\emph{year structure built} counts closely: CD correlation is ",
    fmt_num(as.numeric(proxy_validation_table$Value[proxy_validation_table$Metric == "CD corr: proxy 1990s vs exact 2000 structure-built"]), 3),
    " for the 1990s and ",
    fmt_num(as.numeric(proxy_validation_table$Value[proxy_validation_table$Metric == "CD corr: proxy 1980s vs exact 2000 structure-built"]), 3),
    " for the 1980s."
  ),
  paste0(
    "The strongest mechanism figures are the within-borough share plots for total units and 50+ unit buildings. In the preferred long series, the high-homeownership tercile's share of total units falls from ",
    fmt_num(total_units_high_1985, 1),
    "\\% in 1985--1989 to ",
    fmt_num(total_units_high_2020, 1),
    "\\% in 2020--2025. For 50+ unit buildings it falls from ",
    fmt_num(units_50_high_1985, 1),
    "\\% to ",
    fmt_num(units_50_high_2020, 1),
    "\\%."
  ),
  "\\section*{How The Data Are Built}",
  "\\subsection*{Exact 1990 Homeownership Treatment}",
  "\\begin{enumerate}",
  "\\item Source: official DCP 1990--2000 community-district profile PDFs, parsed from the HOUSING TENURE table.",
  "\\item Exact CD counts: \\texttt{owner\\_occupied\\_units\\_1990} and \\texttt{occupied\\_units\\_1990} come directly from those tables.",
  "\\item Exact borough baseline: borough means are computed by summing those same exact CD counts within borough, not by using separate borough totals.",
  "\\item Final treatment:",
  "\\begin{itemize}",
  "\\item $h\\_{cd,1990} = owner\\_occupied\\_units\\_1990 / occupied\\_units\\_1990$",
  "\\item $h\\_{b,1990} = \\sum\\_{cd \\in b} owner\\_occupied\\_units\\_1990 / \\sum\\_{cd \\in b} occupied\\_units\\_1990$",
  "\\item $treat\\_pp = 100 \\times (h\\_{cd,1990} - h\\_{b,1990})$",
  "\\item \\texttt{treat\\_z\\_boro} is the within-borough z-score of \\texttt{treat\\_pp}",
  "\\end{itemize}",
  "\\end{enumerate}",
  "\\subsection*{Pre-2010 Units Series: MapPLUTO Proxy}",
  "\\begin{enumerate}",
  "\\item Source: current 25v4 staged MapPLUTO lot file.",
  "\\item Restriction: standard 59 CDs only, no joint-interest-area rows, \\texttt{yearbuilt} in 1980--2025, and \\texttt{unitsres > 0}.",
  "\\item Interpretation: each lot contributes its current surviving residential units to the build year given by current \\texttt{yearbuilt}.",
  "\\item This is a surviving-stock proxy, not an exact measured annual flow. It is best interpreted as surviving residential stock built in year $t$.",
  "\\end{enumerate}",
  "\\subsection*{Post-2010 Units Series: Observed DCP Housing Database}",
  "\\begin{enumerate}",
  "\\item Source: DCP Housing Database project-level file.",
  "\\item Timing: completion year, not permit year, for the long compatible series.",
  "\\item Preferred compatible observed series for 2010--2025: new-building units only (\\texttt{classa\\_prop} on \\texttt{job\\_type == 'New Building'}).",
  "\\item Separate observed-only series: gross additions observed, which adds new-building units plus positive-net alteration units. This is reported for post-2010 reference only because there is no directly compatible pre-2010 proxy for alteration-created units.",
  "\\end{enumerate}",
  "\\subsection*{Long 1980--2025 Series}",
  "\\begin{enumerate}",
  "\\item 1980--2009: MapPLUTO \\texttt{yearbuilt} proxy",
  "\\item 2010--2025: DCP Housing Database completion-year new-building units",
  "\\item This gives a single compatible long series for units built. The source switch in 2010 is marked in the figures.",
  "\\end{enumerate}",
  "\\section*{Summary Tables}",
  "\\subsection*{Table 1. Exact Treatment Variation By Borough}"
)

tex_lines <- c(tex_lines, make_latex_table(treatment_borough_table))
tex_lines <- c(tex_lines, "\\subsection*{Table 2. Exact-vs-Overlay Audit}")
tex_lines <- c(tex_lines, make_latex_table(overlay_table))
tex_lines <- c(tex_lines, "\\subsection*{Table 3. MapPLUTO Proxy Validation}")
tex_lines <- c(tex_lines, make_latex_table(proxy_validation_table, align = c("l", "r")))
tex_lines <- c(tex_lines, "\\subsection*{Table 4. Era Shares In The Preferred Long Series}")
tex_lines <- c(tex_lines, make_latex_table(era_share_table))
tex_lines <- c(tex_lines, "\\subsection*{Table 5. Selected Citywide Totals}")
tex_lines <- c(tex_lines, make_latex_table(city_total_table))
tex_lines <- c(
  tex_lines,
  "\\clearpage",
  "\\section*{Key Figures}",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.88\\textwidth]{cd_homeownership_total_units_tercile.pdf}",
  "\\caption*{Within-borough share of total units by 1990 homeownership tercile}",
  "\\end{figure}",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.88\\textwidth]{cd_homeownership_units_50_plus_tercile.pdf}",
  "\\caption*{Within-borough share of 50+ unit buildings by 1990 homeownership tercile}",
  "\\end{figure}",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.88\\textwidth]{cd_homeownership_long_city_totals.pdf}",
  "\\caption*{Citywide long units series, with 2010 source switch marked}",
  "\\end{figure}",
  "\\begin{figure}[H]",
  "\\centering",
  "\\includegraphics[width=0.78\\textwidth]{cd_homeownership_proxy_validation_scatter_1990s.pdf}",
  "\\caption*{Validation of the 1990s PLUTO proxy against exact DCP 2000 structure-built counts}",
  "\\end{figure}",
  "\\section*{Interpretation Notes}",
  "\\begin{itemize}",
  "\\item The exact treatment looks settled. The earlier overlay version was already very close, but the final analysis should use the exact DCP CD counts.",
  "\\item The pre-2010 PLUTO series is best used descriptively. Its strongest validation is against DCP's exact 2000 year-structure-built table, not against net stock growth.",
  "\\item The long units series is intentionally conservative: it stitches together only the most comparable concept across periods, namely units built in new structures.",
  "\\item The two most persuasive mechanism figures are the total-units and 50+-units within-borough share plots. They show that higher-homeownership CDs especially lose the large-building margin over time.",
  "\\end{itemize}",
  "\\end{document}"
)

writeLines(tex_lines, out_tex)

cat("Wrote LaTeX packet outputs to", dirname(out_tex), "\n")
