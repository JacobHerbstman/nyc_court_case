# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_zap_universe_coverage/code")
# start_year <- 1975
# end_year <- 2025

library(dplyr)
library(readr)
library(tidyr)
source("../../../_lib/data_reports.R")

if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  stopifnot(length(args) == 2)
  start_year <- as.integer(args[1])
  end_year <- as.integer(args[2])
}
stopifnot(start_year <= end_year)

# Match the CPC plots: rank 1990 homeownership within borough, with district ties ordered.
districts <- read_csv("../input/cd_homeownership_1990_measure.csv", show_col_types = FALSE) |>
  arrange(borough_code, treat_pp, borocd) |>
  group_by(borough_code) |>
  mutate(homeowner_tercile = ntile(treat_pp, 3)) |>
  ungroup() |>
  mutate(zap_district = sprintf("%s%02d", c("M", "X", "K", "Q", "R")[borough_code],
                                borocd %% 100))
stopifnot(nrow(districts) == 59, !anyDuplicated(districts$zap_district),
          !anyNA(districts$homeowner_tercile))

projects <- read_csv("../input/zap_project_universe.csv",
                     col_types = cols(.default = col_character())) |>
  filter(ulurp_scope == "explicit_ulurp") |>
  select(project_id, project_status, community_district, certified_referred,
         source_dataset_id, source_snapshot_date) |>
  mutate(year = as.integer(format(as.Date(certified_referred, "%m/%d/%Y"), "%Y")),
         withdrawn = project_status == "Withdrawn-Other",
         terminated = project_status %in% c("Terminated", "Terminated-Applicant Unresponsive"),
         homeowner_tercile = NA_integer_, assignment = "missing_district")
stopifnot(!anyDuplicated(projects$project_id),
          all(is.na(projects$certified_referred) | !is.na(projects$year)))

# A multi-district project enters a tercile only if every listed district belongs to it.
for (i in seq_len(nrow(projects))) {
  if (is.na(projects$community_district[i])) next
  codes <- unique(trimws(strsplit(projects$community_district[i], ",", fixed = TRUE)[[1]]))
  matches <- match(codes, districts$zap_district)
  if (anyNA(matches)) {
    projects$assignment[i] <- "nonstandard_or_unmatched_district"
  } else if (n_distinct(districts$homeowner_tercile[matches]) > 1) {
    projects$assignment[i] <- "crosses_homeowner_terciles"
  } else {
    projects$homeowner_tercile[i] <- districts$homeowner_tercile[matches[1]]
    projects$assignment[i] <- if (length(codes) == 1) "single_district" else "same_tercile_multiple_districts"
  }
}
projects <- projects |>
  mutate(tercile = coalesce(c("Low", "Middle", "High")[homeowner_tercile], "Unassigned"))
dated <- projects |> filter(year >= start_year, year <= end_year)

annual <- dated |>
  group_by(year, tercile) |>
  summarise(projects = n(), withdrawn = sum(withdrawn), terminated = sum(terminated),
            .groups = "drop") |>
  complete(year = start_year:end_year, tercile = c("Low", "Middle", "High", "Unassigned"),
           fill = list(projects = 0L, withdrawn = 0L, terminated = 0L)) |>
  mutate(withdrawn_share = if_else(projects > 0, withdrawn / projects, NA_real_),
         withdrawn_terminated_share = if_else(projects > 0, (withdrawn + terminated) / projects, NA_real_)) |>
  arrange(year, tercile)
stopifnot(sum(annual$projects) == nrow(dated),
          sum(annual$withdrawn) == sum(dated$withdrawn),
          all(annual$withdrawn + annual$terminated <= annual$projects))

periods <- dated |>
  mutate(period = case_when(year < 1990 ~ "1975-1989", year <= 2001 ~ "1990-2001",
                            year <= 2013 ~ "2002-2013", TRUE ~ "2014-2025")) |>
  group_by(period, tercile) |>
  summarise(projects = n(), withdrawn = sum(withdrawn), terminated = sum(terminated),
            .groups = "drop") |>
  mutate(withdrawn_share = withdrawn / projects)
save_csv(projects, "../output/zap_withdrawal_project_terciles.csv", "project_id")
save_csv(annual, "../output/zap_withdrawal_tercile_year.csv", c("year", "tercile"))
save_csv(periods, "../output/zap_withdrawal_tercile_period.csv", c("period", "tercile"))

colors <- c(Low = "#277da1", Middle = "#d28a18", High = "#a43b52")
assigned <- subset(annual, tercile != "Unassigned")
pdf("../output/zap_withdrawals_by_homeowner_tercile.pdf", width = 10, height = 7.5)
par(mfrow = c(2, 1), mar = c(3.4, 4.5, 2, 1), oma = c(4.8, 0, 3.4, 0),
    las = 1, family = "Helvetica", col.axis = "#444444")
for (panel in c("share", "count")) {
  values <- if (panel == "share") 100 * assigned$withdrawn_share else assigned$withdrawn
  plot(NA, xlim = c(start_year, end_year), ylim = c(0, max(values, na.rm = TRUE) * 1.12),
       xlab = "", xaxt = "n", bty = "n",
       ylab = if (panel == "share") "Share withdrawn (%)" else "Withdrawn projects")
  abline(h = axTicks(2), col = "#eeeeee")
  abline(v = 2001.5, col = "#888888", lty = 3)
  for (group in names(colors)) {
    series <- subset(assigned, tercile == group)
    values <- if (panel == "share") 100 * series$withdrawn_share else series$withdrawn
    lines(series$year, values, col = colors[group], lwd = 2)
  }
  axis(1, at = seq(ceiling(start_year / 5) * 5, end_year, 5))
  if (panel == "share") {
    legend("topright", paste(names(colors), "homeownership"), col = colors,
           lty = 1, lwd = 2, bty = "n")
    mtext("Withdrawals / all projects in each tercile and reported year", side = 3, adj = 0, cex = 0.8)
  } else {
    mtext("Reported certification / referral year", side = 1, line = 2.2)
  }
}
mtext("ULURP withdrawals by homeowner tercile", outer = TRUE, side = 3,
      line = 1.9, adj = 0, font = 2, cex = 1.25)
mtext("Community districts ranked by 1990 homeownership within borough; annual observations",
      outer = TRUE, side = 3, line = 0.5, adj = 0, cex = 0.92)
mtext(paste(
  sprintf("Assigned: %s of %s dated projects; %s of %s dated withdrawals. Each project counted once.",
          sum(dated$tercile != "Unassigned"), nrow(dated),
          sum(dated$withdrawn & dated$tercile != "Unassigned"), sum(dated$withdrawn)),
  "Withdrawals only; terminations are separate. Missing dates and unassigned geography are excluded.",
  "Historical date clusters are retained. Recent cohorts have incomplete follow-up. ZAP snapshot: September 14, 2026.",
  sep = "\n"), outer = TRUE, side = 1, line = 3.1, adj = 0, cex = 0.76)
dev.off()
print(periods, n = Inf)
print(count(projects, assignment, wt = as.integer(withdrawn), name = "withdrawn"))
