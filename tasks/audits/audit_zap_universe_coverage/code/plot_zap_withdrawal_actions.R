# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_zap_universe_coverage/code")
# start_year <- 1975
# end_year <- 2025
# period_starts <- "1975,1990,2002,2014"
# ma_years <- 3

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(tidyr)
})
source("../../../_lib/data_reports.R")

if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  stopifnot(length(args) == 4)
  start_year <- as.integer(args[1])
  end_year <- as.integer(args[2])
  period_starts <- args[3]
  ma_years <- as.integer(args[4])
}
period_starts <- as.integer(strsplit(period_starts, ",", fixed = TRUE)[[1]])
stopifnot(!anyNA(c(start_year, end_year, period_starts)), start_year < end_year,
  period_starts[1] == start_year, all(diff(period_starts) > 0),
  max(period_starts) <= end_year)
stopifnot(!is.na(ma_years), ma_years >= 3L, ma_years %% 2L == 1L,
  ma_years <= end_year - start_year + 1L)
period_labels <- paste0(period_starts, "-", c(period_starts[-1] - 1L, end_year))

projects <- read_csv("../output/zap_project_coverage.csv",
  col_types = cols(.default = col_character()), show_col_types = FALSE) |>
  filter(ulurp_scope == "explicit_ulurp") |>
  mutate(
    withdrawn = project_status == "Withdrawn-Other",
    terminated = project_status %in% c("Terminated", "Terminated-Applicant Unresponsive"),
    across(c(action_evidence_present, has_zoning_action, has_zoning_map_action, pp_only),
      ~ .x == "True"),
    linked_cpc_report_count = as.integer(linked_cpc_report_count),
    entry_date = as.Date(certified_referred, format = "%m/%d/%Y"),
    year = as.integer(format(entry_date, "%Y")),
    period = case_when(
      is.na(year) ~ "Missing date",
      year < start_year ~ paste0("Before ", start_year),
      year > end_year ~ paste0("After ", end_year),
      TRUE ~ as.character(cut(year, breaks = c(period_starts, end_year + 1L),
        right = FALSE, labels = period_labels))
    )
  )
stopifnot(!anyDuplicated(projects$project_id),
  all(is.na(projects$certified_referred) | !is.na(projects$entry_date)),
  all(!projects$has_zoning_map_action | projects$has_zoning_action),
  all(!projects$pp_only | !projects$has_zoning_action))

geography <- read_csv("../output/zap_withdrawal_project_terciles.csv",
  col_types = cols(.default = col_character()), show_col_types = FALSE)
stopifnot(!anyDuplicated(geography$project_id),
  setequal(projects$project_id, geography$project_id))
geography <- geography[match(projects$project_id, geography$project_id), ]
stopifnot(identical(projects$project_status, geography$project_status),
  identical(projects$certified_referred, geography$certified_referred))
projects <- projects |>
  left_join(select(geography, project_id, tercile), by = "project_id",
    relationship = "one-to-one")
stopifnot(!anyNA(projects$tercile))

# Samples overlap. Each counts a project once, irrespective of actions or reports.
sample_rows <- bind_rows(
  mutate(projects, sample = "all_ulurp"),
  projects |> filter(has_zoning_action) |> mutate(sample = "zoning"),
  projects |> filter(has_zoning_map_action) |> mutate(sample = "zoning_map"),
  projects |> filter(pp_only) |> mutate(sample = "pp_only"),
  projects |> filter(!action_evidence_present) |> mutate(sample = "unknown_actions")
)
stopifnot(!anyDuplicated(sample_rows[c("sample", "project_id")]))

annual <- sample_rows |>
  filter(year >= start_year, year <= end_year) |>
  group_by(sample, year) |>
  summarize(projects = n(), withdrawn = sum(withdrawn), terminated = sum(terminated),
    action_evidence_projects = sum(action_evidence_present),
    cpc_linked_projects = sum(linked_cpc_report_count > 0), .groups = "drop") |>
  complete(sample, year = start_year:end_year,
    fill = list(projects = 0L, withdrawn = 0L, terminated = 0L,
      action_evidence_projects = 0L, cpc_linked_projects = 0L)) |>
  mutate(withdrawal_share = if_else(projects > 0L, withdrawn / projects, NA_real_),
    withdrawn_terminated_share = if_else(projects > 0L,
      (withdrawn + terminated) / projects, NA_real_)) |>
  arrange(sample, year)

period <- sample_rows |>
  group_by(sample, period) |>
  summarize(projects = n(), withdrawn = sum(withdrawn), terminated = sum(terminated),
    action_evidence_projects = sum(action_evidence_present),
    cpc_linked_projects = sum(linked_cpc_report_count > 0), .groups = "drop") |>
  mutate(withdrawal_share = withdrawn / projects,
    withdrawn_terminated_share = (withdrawn + terminated) / projects) |>
  arrange(sample, period)
stopifnot(sum(period$projects[period$sample == "all_ulurp"]) == nrow(projects),
  sum(period$withdrawn[period$sample == "all_ulurp"]) == sum(projects$withdrawn),
  all(annual$withdrawn + annual$terminated <= annual$projects))
save_csv(annual, "../output/zap_withdrawal_action_year.csv", c("sample", "year"))
save_csv(period, "../output/zap_withdrawal_action_period.csv", c("sample", "period"))

# Reuse the existing district assignments; retain Unassigned in every table.
tercile_rows <- sample_rows |> filter(sample %in% c("zoning_map", "zoning"))
tercile_dated <- tercile_rows |> filter(year >= start_year, year <= end_year)
tercile_annual <- tercile_dated |>
  group_by(sample, year, tercile) |>
  summarize(projects = n(), withdrawn = sum(withdrawn), terminated = sum(terminated),
    .groups = "drop") |>
  complete(sample = c("zoning_map", "zoning"), year = start_year:end_year,
    tercile = c("Low", "Middle", "High", "Unassigned"),
    fill = list(projects = 0L, withdrawn = 0L, terminated = 0L)) |>
  mutate(withdrawal_share = if_else(projects > 0L, withdrawn / projects, NA_real_),
    withdrawn_terminated_share = if_else(projects > 0L,
      (withdrawn + terminated) / projects, NA_real_)) |>
  arrange(sample, year, tercile)
tercile_period <- tercile_rows |>
  group_by(sample, period, tercile) |>
  summarize(projects = n(), withdrawn = sum(withdrawn), terminated = sum(terminated),
    .groups = "drop") |>
  complete(sample, period, tercile = c("Low", "Middle", "High", "Unassigned"),
    fill = list(projects = 0L, withdrawn = 0L, terminated = 0L)) |>
  mutate(withdrawal_share = if_else(projects > 0L, withdrawn / projects, NA_real_),
    withdrawn_terminated_share = if_else(projects > 0L,
      (withdrawn + terminated) / projects, NA_real_)) |>
  arrange(sample, period, tercile)
stopifnot(identical(
  tercile_annual |> group_by(sample, year) |>
    summarize(across(c(projects, withdrawn, terminated), sum), .groups = "drop"),
  annual |> filter(sample %in% c("zoning_map", "zoning")) |>
    select(sample, year, projects, withdrawn, terminated)))
stopifnot(identical(
  tercile_period |> group_by(sample, period) |>
    summarize(across(c(projects, withdrawn, terminated), sum), .groups = "drop"),
  period |> filter(sample %in% c("zoning_map", "zoning")) |>
    select(sample, period, projects, withdrawn, terminated)))
save_csv(tercile_annual, "../output/zap_withdrawal_action_tercile_year.csv",
  c("sample", "year", "tercile"))
save_csv(tercile_period, "../output/zap_withdrawal_action_tercile_period.csv",
  c("sample", "period", "tercile"))

labels <- c(all_ulurp = "All ULURP projects",
  zoning = "Zoning changes / special permits", zoning_map = "Zoning-map changes")
plot_rows <- annual |>
  filter(sample %in% names(labels)) |>
  transmute(sample, year, `Withdrawn (%)` = 100 * withdrawal_share,
    `Withdrawn projects` = withdrawn) |>
  pivot_longer(c(`Withdrawn (%)`, `Withdrawn projects`), names_to = "measure") |>
  mutate(sample = factor(sample, levels = names(labels)),
    measure = factor(measure, levels = c("Withdrawn (%)", "Withdrawn projects")))
missing_date <- projects |> filter(is.na(year))
figure <- ggplot(plot_rows, aes(year, value, color = sample)) +
  geom_vline(xintercept = period_starts[-1] - 0.5, linetype = 3, color = "grey75") +
  geom_line(linewidth = 0.7, na.rm = TRUE) +
  facet_wrap(~ measure, ncol = 1, scales = "free_y") +
  scale_color_manual(values = c("#888888", "#0072B2", "#D55E00"), labels = labels) +
  scale_x_continuous(breaks = seq(start_year, end_year, 5), limits = c(start_year, end_year)) +
  labs(title = "Withdrawals by recorded land-use action",
    subtitle = "Current project status; grouped by reported certification / referral year",
    x = "Reported certification / referral year", y = NULL, color = NULL,
    caption = paste0(
      "One project per sample; zoning samples overlap. Zoning = ZM/ZR/ZS; zoning-map = ZM. Action type does not establish project size.\n",
      "Each share divides withdrawals by all projects in that sample and year, including ongoing projects. Terminations are separate in the CSV.\n",
      nrow(missing_date), " ULURP projects lack the date (", sum(missing_date$withdrawn),
      " withdrawn). Date clusters are retained; later cohorts have less follow-up.\n",
      "ZAP snapshot: September 14, 2026. These are cohort status shares, not dated withdrawal events or final failure probabilities.")) +
  theme_minimal(base_size = 11) +
  theme(legend.position = "bottom", panel.grid.minor = element_blank(),
    plot.caption = element_text(hjust = 0, size = 8),
    strip.text = element_text(hjust = 0, face = "bold"),
    plot.margin = margin(12, 14, 12, 12))
ggsave("../output/zap_withdrawals_by_action.pdf", figure, width = 11, height = 8)

geography_coverage <- tercile_dated |>
  group_by(sample) |>
  summarize(projects = n(), assigned = sum(tercile != "Unassigned"),
    assigned_withdrawn = sum(withdrawn & tercile != "Unassigned"),
    withdrawn = sum(withdrawn), .groups = "drop") |>
  arrange(match(sample, c("zoning_map", "zoning")))
stopifnot(all(geography_coverage$assigned <= geography_coverage$projects),
  all(geography_coverage$assigned_withdrawn <= geography_coverage$withdrawn))
tercile_plot <- tercile_annual |>
  filter(tercile != "Unassigned") |>
  transmute(sample, year, tercile, `Withdrawn (%)` = 100 * withdrawal_share,
    `Projects in denominator` = projects) |>
  pivot_longer(c(`Withdrawn (%)`, `Projects in denominator`), names_to = "measure") |>
  mutate(sample = factor(sample, levels = c("zoning_map", "zoning")),
    tercile = factor(tercile, levels = c("Low", "Middle", "High")),
    measure = factor(measure, levels = c("Withdrawn (%)", "Projects in denominator")))
tercile_figure <- ggplot(tercile_plot, aes(year, value, color = tercile)) +
  geom_vline(xintercept = period_starts[-1] - 0.5, linetype = 3, color = "grey75") +
  geom_line(linewidth = 0.65, na.rm = TRUE) +
  facet_grid(measure ~ sample, scales = "free_y", labeller = labeller(sample = c(
    zoning_map = "Zoning-map changes (ZM)",
    zoning = "Zoning changes + special permits (ZM/ZR/ZS)"))) +
  scale_color_manual(values = c(Low = "#277da1", Middle = "#d28a18", High = "#a43b52")) +
  scale_x_continuous(breaks = seq(start_year, end_year, 10), limits = c(start_year, end_year)) +
  labs(title = "Zoning-project withdrawals by homeowner tercile",
    subtitle = "Annual cohorts; community districts ranked by 1990 homeownership within borough",
    x = "Reported certification / referral year", y = NULL, color = "Homeowner tercile",
    caption = paste0(
      "Top: withdrawals / all projects in that sample, tercile, and year. Bottom: number of projects in that denominator.\n",
      sprintf("Geography assigned: zoning-map %s/%s dated projects; broader zoning %s/%s. Unassigned projects and missing dates remain in the CSVs.\n",
        geography_coverage$assigned[1], geography_coverage$projects[1],
        geography_coverage$assigned[2], geography_coverage$projects[2]),
      "One project per sample. Multi-district projects enter a tercile only if all listed districts belong to it. Terminations are separate.\n",
      "Small annual samples produce large swings. Date clusters are retained; recent cohorts have less follow-up. ZAP snapshot: September 14, 2026.")) +
  theme_minimal(base_size = 10.5) +
  theme(legend.position = "bottom", panel.grid.minor = element_blank(),
    plot.caption = element_text(hjust = 0, size = 8),
    strip.text.x = element_text(face = "bold", size = 10),
    plot.margin = margin(12, 14, 12, 12))
ggsave("../output/zap_withdrawals_by_action_homeowner_tercile.pdf",
  tercile_figure, width = 12, height = 8)

# Equal-weight annual averages; incomplete windows and missing rates stay missing.
tercile_ma_plot <- tercile_plot |>
  group_by(sample, tercile, measure) |>
  arrange(year, .by_group = TRUE) |>
  mutate(value = as.numeric(stats::filter(value,
    rep(1 / ma_years, ma_years), sides = 2))) |>
  ungroup() |>
  mutate(measure = factor(measure,
    levels = c("Withdrawn (%)", "Projects in denominator"),
    labels = c("Withdrawn (%, MA)", "Projects/year (MA)")))
tercile_ma_figure <- tercile_figure %+% tercile_ma_plot +
  labs(subtitle = sprintf("Centered %s-year moving averages; 1990 homeowner terciles within borough", ma_years),
    caption = paste0(
      sprintf("Each point averages %s consecutive annual values, with equal weight for each year. Top: withdrawal percentages. Bottom: annual project counts.\n", ma_years),
      "Full windows only: no shortened endpoint windows; a missing annual rate leaves the average missing. Original annual data and figure are retained.\n",
      "Same overlapping zoning samples and district assignments. Unassigned projects remain in the CSVs. Terminations are separate.\n",
      "Current status by reported certification/referral cohort; ongoing projects are included and recent cohorts have less follow-up. ZAP snapshot: September 14, 2026."))
ggsave("../output/zap_withdrawals_by_action_homeowner_tercile_ma.pdf",
  tercile_ma_figure, width = 12, height = 8)

table_rows <- period |> filter(sample %in% names(labels), period %in% period_labels)
findings <- c("# Withdrawals within zoning projects", "",
  sprintf("Recorded action codes are available for %s of %s explicit ULURP projects, including %s of %s withdrawn or terminated projects.",
    sum(projects$action_evidence_present), nrow(projects),
    sum(projects$action_evidence_present & (projects$withdrawn | projects$terminated)),
    sum(projects$withdrawn | projects$terminated)), "",
  "The classification combines literal bulk action fields, action-code suffixes in application numbers, and saved API actions. Every source remains separate in the project table; unparsed values and differing source sets are flagged. No narrative judgment or manual size classification is used.", "",
  "| Sample | Reported entry years | Projects | Withdrawn | Withdrawn share |",
  "|---|---|---:|---:|---:|")
for (i in seq_len(nrow(table_rows))) {
  row <- table_rows[i, ]
  findings <- c(findings, sprintf("| %s | %s | %s | %s | %.1f%% |",
    labels[[row$sample]], row$period, row$projects, row$withdrawn, 100 * row$withdrawal_share))
}
findings <- c(findings, "",
  "The zoning samples require a recorded ZM, ZR, or ZS action; the narrower sample requires ZM. A disposition paired with a zoning action stays included. Projects with only disposition actions cannot enter these zoning samples. Other substantive projects can be missed by this proxy, and some zoning actions can be small or technical.", "",
  "The period CSV separately retains PP-only projects, projects with no action evidence, missing entry dates, and dates outside the plotted range. No CPC report is required for any withdrawal sample. Report links indicate document availability, not the final project outcome.", "",
  "These are current-status shares among reported entry-year cohorts. They include ongoing projects in the denominator and retain suspicious date clusters. They are not final failure probabilities or counts of withdrawals occurring during each year.")
findings <- c(findings, "", "## By homeowner tercile", "",
  "The same 1990 within-borough community-district terciles are used for both samples. Multi-district projects are assigned only when all listed districts belong to the same tercile. The source assignment table is unchanged.", "",
  sprintf("Among dated projects, geography is assigned for %s/%s zoning-map projects (%s/%s withdrawals) and %s/%s broader zoning projects (%s/%s withdrawals).",
    geography_coverage$assigned[1], geography_coverage$projects[1],
    geography_coverage$assigned_withdrawn[1], geography_coverage$withdrawn[1],
    geography_coverage$assigned[2], geography_coverage$projects[2],
    geography_coverage$assigned_withdrawn[2], geography_coverage$withdrawn[2]), "",
  "Cells show the withdrawn share and total projects in that tercile and cohort. ZM is zoning-map changes; ZM/ZR/ZS adds zoning-text changes and special permits. The samples overlap.", "",
  "| Sample | Reported entry years | Low | Middle | High |", "|---|---|---:|---:|---:|")
tercile_table <- tercile_period |>
  filter(tercile != "Unassigned", period %in% period_labels) |>
  mutate(cell = sprintf("%.1f%% (n=%s)", 100 * withdrawal_share, projects)) |>
  select(sample, period, tercile, cell) |>
  pivot_wider(names_from = tercile, values_from = cell) |>
  arrange(match(sample, c("zoning_map", "zoning")), period)
for (i in seq_len(nrow(tercile_table))) {
  row <- tercile_table[i, ]
  findings <- c(findings, sprintf("| %s | %s | %s | %s | %s |",
    ifelse(row$sample == "zoning_map", "ZM", "ZM/ZR/ZS"),
    row$period, row$Low, row$Middle, row$High))
}
findings <- c(findings, "",
  "The annual figure also shows each tercile's denominator because small cohorts can produce large percentage swings. Unassigned geography, missing dates, and dates outside the plotted range remain in the period CSV. Summing across terciles, including Unassigned, reproduces each unrestricted sample's project, withdrawal, and termination counts.")
writeLines(findings, "../output/zap_withdrawal_action_findings.md")
