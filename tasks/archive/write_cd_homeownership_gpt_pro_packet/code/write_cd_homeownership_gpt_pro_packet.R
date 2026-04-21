# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/write_cd_homeownership_gpt_pro_packet/code")
# cd_homeownership_1990_measure_csv <- "../input/cd_homeownership_1990_measure.csv"
# comparison_summary_csv <- "../input/cd_homeownership_1990_measure_comparison_summary.csv"
# proxy_validation_summary_csv <- "../input/mappluto_construction_proxy_validation_summary.csv"
# proxy_decadal_csv <- "../input/mappluto_construction_proxy_vs_decadal.csv"
# long_city_year_csv <- "../input/cd_homeownership_long_units_city_year.csv"
# long_tercile_year_csv <- "../input/cd_homeownership_long_units_tercile_year.csv"
# long_tercile_era_csv <- "../input/cd_homeownership_long_units_tercile_era.csv"
# out_md <- "../output/cd_homeownership_gpt_pro_packet.md"
# out_total_units_png <- "../output/cd_homeownership_total_units_tercile.png"
# out_units_50_plus_png <- "../output/cd_homeownership_units_50_plus_tercile.png"
# out_city_totals_png <- "../output/cd_homeownership_long_city_totals.png"
# out_validation_scatter_png <- "../output/cd_homeownership_proxy_validation_scatter_1990s.png"

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
  stop("Expected 12 arguments: cd_homeownership_1990_measure_csv comparison_summary_csv proxy_validation_summary_csv proxy_decadal_csv long_city_year_csv long_tercile_year_csv long_tercile_era_csv out_md out_total_units_png out_units_50_plus_png out_city_totals_png out_validation_scatter_png")
}

cd_homeownership_1990_measure_csv <- args[1]
comparison_summary_csv <- args[2]
proxy_validation_summary_csv <- args[3]
proxy_decadal_csv <- args[4]
long_city_year_csv <- args[5]
long_tercile_year_csv <- args[6]
long_tercile_era_csv <- args[7]
out_md <- args[8]
out_total_units_png <- args[9]
out_units_50_plus_png <- args[10]
out_city_totals_png <- args[11]
out_validation_scatter_png <- args[12]

fmt_num <- function(x, digits = 1) {
  ifelse(is.na(x), NA_character_, formatC(x, format = "f", digits = digits, big.mark = ","))
}

fmt_pct <- function(x, digits = 1) {
  ifelse(is.na(x), NA_character_, paste0(formatC(100 * x, format = "f", digits = digits), "%"))
}

fmt_md_table <- function(df, digits = 2) {
  capture.output(knitr::kable(df, format = "pipe", digits = digits, na = ""))
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
    district_count = n(),
    h_cd_min_pct = min(h_cd_1990_pct, na.rm = TRUE),
    h_cd_max_pct = max(h_cd_1990_pct, na.rm = TRUE),
    treat_min_pp = min(treat_pp, na.rm = TRUE),
    treat_max_pp = max(treat_pp, na.rm = TRUE)
  )

treatment_borough_table <- treatment_df |>
  group_by(borough_name) |>
  summarize(
    districts = n(),
    borough_homeownership_pct = first(h_b_1990_pct),
    cd_homeownership_min_pct = min(h_cd_1990_pct, na.rm = TRUE),
    cd_homeownership_max_pct = max(h_cd_1990_pct, na.rm = TRUE),
    treat_sd_pp = sd(treat_pp, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(match(borough_name, c("Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"))) |>
  mutate(
    borough_homeownership_pct = round(borough_homeownership_pct, 1),
    cd_homeownership_min_pct = round(cd_homeownership_min_pct, 1),
    cd_homeownership_max_pct = round(cd_homeownership_max_pct, 1),
    treat_sd_pp = round(treat_sd_pp, 1)
  )

overlay_table <- comparison_summary_df |>
  filter(scope == "overall") |>
  transmute(
    `corr(H_cd)` = round(correlation_homeowner_share, 4),
    `MAE(H_cd gap, pp)` = round(mae_homeowner_share_gap_pp, 3),
    `max |H_cd gap| (pp)` = round(max_abs_homeowner_share_gap_pp, 3),
    `corr(treat_pp)` = round(correlation_treat_pp, 4),
    `MAE(treat gap, pp)` = round(mae_treat_gap_pp, 3),
    `max |treat gap| (pp)` = round(max_abs_treat_gap_pp, 3)
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
    metric = case_when(
      metric == "city_year_corr_proxy_vs_hdb_nb_units_2010_2025" ~ "City-year corr: proxy vs HDB total NB units, 2010-2025",
      metric == "city_year_corr_proxy_vs_hdb_50_plus_units_2010_2025" ~ "City-year corr: proxy vs HDB 50+ NB units, 2010-2025",
      metric == "borough_year_corr_proxy_vs_hdb_nb_units_2010_2025" ~ "Borough-year corr: proxy vs HDB total NB units, 2010-2025",
      metric == "borough_year_corr_proxy_vs_hdb_unit_shares_2010_2025" ~ "Borough-year corr: proxy vs HDB city shares, total units",
      metric == "city_total_ratio_proxy_to_hdb_nb_units_2010_2025" ~ "City total ratio: proxy / HDB total NB units, 2010-2025",
      metric == "cd_corr_proxy_vs_exact_1990s_structure_built_2000" ~ "CD corr: proxy 1990s vs exact 2000 structure-built counts",
      metric == "cd_corr_proxy_vs_exact_1980s_structure_built_2000" ~ "CD corr: proxy 1980s vs exact 2000 structure-built counts",
      metric == "city_total_ratio_proxy_to_exact_1990s_structure_built_2000" ~ "City ratio: proxy 1990s / exact 2000 structure-built counts",
      metric == "city_total_ratio_proxy_to_exact_1980s_structure_built_2000" ~ "City ratio: proxy 1980s / exact 2000 structure-built counts",
      TRUE ~ metric
    ),
    value = round(as.numeric(value), 3)
  )

era_share_table <- long_tercile_era_df |>
  filter(series_family %in% c("units_built_total", "units_built_50_plus")) |>
  select(series_label, era, treat_tercile_label, borough_outcome_share) |>
  mutate(
    series_label = recode(series_label, "Units built: total" = "Total units", "Units built: 50+" = "50+ unit buildings"),
    borough_outcome_share = round(100 * borough_outcome_share, 1)
  ) |>
  tidyr::pivot_wider(
    names_from = treat_tercile_label,
    values_from = borough_outcome_share
  ) |>
  arrange(match(series_label, c("Total units", "50+ unit buildings")), era)

city_total_table <- long_city_year_df |>
  filter(
    (series_family == "units_built_total" & year %in% c(1980, 1985, 1989, 1990, 1995, 2000, 2005, 2009, 2010, 2015, 2020, 2025)) |
      (series_family == "gross_add_units_observed" & year %in% c(2010, 2015, 2020, 2025))
  ) |>
  transmute(
    year,
    series = case_when(
      series_family == "units_built_total" ~ "Units built: total",
      series_family == "gross_add_units_observed" ~ "Gross additions observed",
      TRUE ~ series_label
    ),
    city_total_units = round(city_outcome_total)
  ) |>
  arrange(year, series)

proxy_1990s_scatter_df <- proxy_decadal_df |>
  filter(
    geography_level == "cd",
    decade == "1990s",
    comparator_source == "dcp_profiles_exact_2000_structure_built"
  )

total_units_plot_df <- long_tercile_year_df |>
  filter(series_family == "units_built_total") |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High"))
  )

units_50_plus_plot_df <- long_tercile_year_df |>
  filter(series_family == "units_built_50_plus") |>
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High"))
  )

city_totals_plot_df <- long_city_year_df |>
  filter(series_family %in% c("units_built_total", "gross_add_units_observed")) |>
  mutate(
    series_label = factor(case_when(
      series_family == "units_built_total" ~ "Units built: total",
      series_family == "gross_add_units_observed" ~ "Gross additions observed"
    ), levels = c("Units built: total", "Gross additions observed"))
  )

ggsave(
  out_total_units_png,
  ggplot(total_units_plot_df, aes(x = year, y = borough_outcome_share, color = treat_tercile_label)) +
    geom_line(linewidth = 0.9) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = "Within-borough share", color = "Homeownership tercile") +
    theme_minimal(base_size = 12) +
    theme(legend.position = "bottom"),
  width = 9,
  height = 5,
  dpi = 200
)

ggsave(
  out_units_50_plus_png,
  ggplot(units_50_plus_plot_df, aes(x = year, y = borough_outcome_share, color = treat_tercile_label)) +
    geom_line(linewidth = 0.9) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    scale_color_manual(values = c("Low" = "#3366CC", "Middle" = "#999999", "High" = "#CC3311")) +
    labs(x = NULL, y = "Within-borough share", color = "Homeownership tercile") +
    theme_minimal(base_size = 12) +
    theme(legend.position = "bottom"),
  width = 9,
  height = 5,
  dpi = 200
)

ggsave(
  out_city_totals_png,
  ggplot(city_totals_plot_df, aes(x = year, y = city_outcome_total, color = series_label)) +
    geom_line(linewidth = 0.9) +
    geom_vline(xintercept = 2010, linetype = "dashed", color = "#666666") +
    scale_color_manual(values = c("Units built: total" = "#1b6ca8", "Gross additions observed" = "#d65f0e")) +
    labs(x = NULL, y = "Citywide units", color = NULL) +
    theme_minimal(base_size = 12) +
    theme(legend.position = "bottom"),
  width = 9,
  height = 5,
  dpi = 200
)

ggsave(
  out_validation_scatter_png,
  ggplot(proxy_1990s_scatter_df, aes(x = comparator_units, y = proxy_units)) +
    geom_point(color = "#1b6ca8", alpha = 0.8) +
    geom_smooth(method = "lm", se = FALSE, color = "#d65f0e", linewidth = 0.8) +
    labs(
      x = "Exact 2000 CD count: units in structures built in the 1990s",
      y = "PLUTO proxy: surviving residential units built in the 1990s"
    ) +
    theme_minimal(base_size = 12),
  width = 7,
  height = 5,
  dpi = 200
)

lines_out <- c(
  "# CD Homeownership and Housing Production Packet",
  "",
  "## What This Packet Is",
  "",
  "This note packages the current state of the exact 1990 homeownership exposure and the housing-production series used to study it. It is meant to be handed to GPT Pro or another reviewer as a compact orientation memo.",
  "",
  "## Bottom Line",
  "",
  paste0(
    "The exact 1990 treatment varies a lot across CDs: CD homeownership runs from `",
    fmt_num(treatment_overall$h_cd_min_pct, 1),
    "%` to `",
    fmt_num(treatment_overall$h_cd_max_pct, 1),
    "%`, and borough-centered exposure `treat_pp` runs from `",
    fmt_num(treatment_overall$treat_min_pp, 1),
    "` to `",
    fmt_num(treatment_overall$treat_max_pp, 1),
    "` percentage points."
  ),
  "",
  paste0(
    "The exact DCP-based treatment is extremely close to the earlier NHGIS overlay approximation: overall `corr(H_cd) = ",
    fmt_num(overlay_table$`corr(H_cd)`, 4),
    "`, `corr(treat_pp) = ",
    fmt_num(overlay_table$`corr(treat_pp)`, 4),
    "`, and max absolute CD gap is only `",
    fmt_num(overlay_table$`max |H_cd gap| (pp)`, 3),
    "` pp."
  ),
  "",
  paste0(
    "For the pre-2010 period, the MapPLUTO `yearbuilt` proxy looks usable as a descriptive bridge. It tracks exact DCP 2000 `year structure built` counts closely: CD correlation is `",
    fmt_num(as.numeric(proxy_validation_table$value[proxy_validation_table$metric == "CD corr: proxy 1990s vs exact 2000 structure-built counts"]), 3),
    "` for the 1990s and `",
    fmt_num(as.numeric(proxy_validation_table$value[proxy_validation_table$metric == "CD corr: proxy 1980s vs exact 2000 structure-built counts"]), 3),
    "` for the 1980s."
  ),
  "",
  paste0(
    "The two strongest mechanism plots are the long-series within-borough share plots for total units and `50+` unit buildings. In the preferred long series, the high-homeownership tercile's share of total units falls from `",
    fmt_num(era_share_table$High[era_share_table$series_label == 'Total units' & era_share_table$era == '1985-1989'], 1),
    "%` in `1985-1989` to `",
    fmt_num(era_share_table$High[era_share_table$series_label == 'Total units' & era_share_table$era == '2020-2025'], 1),
    "%` in `2020-2025`. For `50+` unit buildings it falls from `",
    fmt_num(era_share_table$High[era_share_table$series_label == '50+ unit buildings' & era_share_table$era == '1985-1989'], 1),
    "%` to `",
    fmt_num(era_share_table$High[era_share_table$series_label == '50+ unit buildings' & era_share_table$era == '2020-2025'], 1),
    "%`."
  ),
  "",
  "## Exact Treatment Construction",
  "",
  "1. Source: official DCP 1990-2000 community-district profile PDFs, parsed from the `HOUSING TENURE` table.",
  "2. Exact CD counts: `owner_occupied_units_1990` and `occupied_units_1990` are taken directly from those CD tables.",
  "3. Exact borough baseline: borough means are computed by summing those same exact CD counts within borough, not by using separate borough totals.",
  "4. Final treatment:",
  "   - `h_cd_1990 = owner_occupied_units_1990 / occupied_units_1990`",
  "   - `h_b_1990 = sum_cd(owner_occupied_units_1990) / sum_cd(occupied_units_1990)` within borough",
  "   - `treat_pp = 100 * (h_cd_1990 - h_b_1990)`",
  "   - `treat_z_boro` is the within-borough z-score of `treat_pp`",
  "",
  "## Pre-2010 Units Series: MapPLUTO Proxy",
  "",
  "1. Source: current `25v4` staged MapPLUTO lot file.",
  "2. Restriction: standard 59 CDs only, no joint-interest-area rows, `yearbuilt` in `1980-2025`, and `unitsres > 0`.",
  "3. Interpretation: each lot contributes its **current surviving residential units** to the build year given by current `yearbuilt`.",
  "4. This is a surviving-stock proxy, not an exact measured annual flow. It is best interpreted as 'surviving residential stock built in year t.'",
  "5. Series built from the proxy:",
  "   - `units_built_total`",
  "   - `units_built_1_4`",
  "   - `units_built_5_plus`",
  "   - `units_built_50_plus`",
  "",
  "## Post-2010 Units Series: Observed DCP Housing Database",
  "",
  "1. Source: DCP Housing Database project-level file.",
  "2. Timing: completion year, not permit year, for the long compatible series.",
  "3. Preferred compatible observed series for `2010-2025`: new-building units only (`classa_prop` on `job_type == 'New Building'`).",
  "4. Separate observed-only series: `gross_add_units_observed`, which adds new-building units plus positive-net alteration units. This is included for post-2010 reference only because there is no directly compatible pre-2010 proxy for alteration-created units.",
  "",
  "## Long 1980-2025 Series",
  "",
  "1. `1980-2009`: MapPLUTO `yearbuilt` proxy",
  "2. `2010-2025`: DCP Housing Database completion-year new-building units",
  "3. This gives a single compatible long series for units built. The source switches in 2010 and that switch is marked in the key figures.",
  "",
  "## Table 1. Exact Treatment Variation By Borough",
  ""
)

lines_out <- c(lines_out, fmt_md_table(treatment_borough_table, digits = 1), "")
lines_out <- c(lines_out, "## Table 2. Exact-vs-Overlay Audit", "")
lines_out <- c(lines_out, fmt_md_table(overlay_table, digits = 4), "")
lines_out <- c(lines_out, "## Table 3. MapPLUTO Proxy Validation", "")
lines_out <- c(lines_out, fmt_md_table(proxy_validation_table, digits = 3), "")
lines_out <- c(lines_out, "## Table 4. Era Shares In The Preferred Long Series", "")
lines_out <- c(lines_out, fmt_md_table(era_share_table, digits = 1), "")
lines_out <- c(lines_out, "## Table 5. Selected Citywide Totals", "")
lines_out <- c(lines_out, fmt_md_table(city_total_table, digits = 0), "")

lines_out <- c(
  lines_out,
  "",
  "## Figures",
  "",
  paste0("![Total units share by tercile](", out_total_units_png, ")"),
  "",
  paste0("![50+ unit buildings share by tercile](", out_units_50_plus_png, ")"),
  "",
  paste0("![Citywide long totals](", out_city_totals_png, ")"),
  "",
  paste0("![Proxy validation scatter](", out_validation_scatter_png, ")"),
  "",
  "## Interpretation Notes",
  "",
  "- The exact treatment looks settled. The earlier overlay version was already very close, but the final analysis should use the exact DCP CD counts.",
  "- The pre-2010 PLUTO series is best used descriptively. Its strongest validation is against DCP's exact 2000 `year structure built` table, not against net stock growth.",
  "- The long units series is intentionally conservative: it stitches together only the most comparable concept across periods, namely units built in new structures.",
  "- The two most persuasive mechanism figures are the total-units and `50+`-units within-borough share plots. They show that higher-homeownership CDs do not just lose some abstract share; they especially lose the large-building margin over time.",
  ""
)

writeLines(lines_out, out_md)

cat("Wrote GPT Pro packet outputs to", dirname(out_md), "\n")
