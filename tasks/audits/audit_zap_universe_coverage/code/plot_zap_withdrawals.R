# setwd("tasks/audits/audit_zap_universe_coverage/code")
# start_year <- 1975
# end_year <- 2025

if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  stopifnot(length(args) == 2)
  start_year <- as.integer(args[1])
  end_year <- as.integer(args[2])
}
stopifnot(start_year <= end_year)

counts <- read.csv("../output/zap_dates_by_year.csv")
counts <- subset(counts, ulurp_scope == "explicit_ulurp" &
                   date_field == "certified_referred")
missing_projects <- sum(counts$projects[counts$year == "missing"])
missing_withdrawals <- sum(counts$projects[counts$year == "missing" &
                                           counts$project_status == "Withdrawn-Other"])
counts <- subset(counts, year != "missing")
counts$year <- as.integer(counts$year)
stopifnot(!anyDuplicated(counts[c("year", "project_status")]))
clusters <- read.csv("../output/zap_date_clusters.csv")
cluster <- subset(clusters, ulurp_scope == "explicit_ulurp" &
                    project_status == "Withdrawn-Other" &
                    date_field == "certified_referred" & date == "11/13/1991")
stopifnot(nrow(cluster) == 1)

years <- start_year:end_year
annual <- data.frame(year = years, projects = 0L, withdrawn = 0L, terminated = 0L)
for (i in seq_along(years)) {
  cohort <- subset(counts, year == years[i])
  annual$projects[i] <- sum(cohort$projects)
  annual$withdrawn[i] <- sum(cohort$projects[cohort$project_status == "Withdrawn-Other"])
  annual$terminated[i] <- sum(cohort$projects[cohort$project_status %in%
    c("Terminated", "Terminated-Applicant Unresponsive")])
}
stopifnot(all(annual$projects > 0),
          all(annual$withdrawn + annual$terminated <= annual$projects))

pdf("../output/zap_withdrawals_over_time.pdf", width = 10, height = 7.5)
par(mfrow = c(2, 1), mar = c(3.4, 4.5, 2, 1), oma = c(4.5, 0, 3.2, 0),
    las = 1, family = "Helvetica", col.axis = "#444444", col.lab = "#333333")
for (panel in c("share", "count")) {
  withdrawn <- annual$withdrawn
  terminal <- annual$withdrawn + annual$terminated
  if (panel == "share") {
    withdrawn <- 100 * withdrawn / annual$projects
    terminal <- 100 * terminal / annual$projects
  }
  plot(annual$year, terminal, type = "n", xlim = c(start_year, end_year),
       ylim = c(0, max(terminal) * 1.15), xlab = "", xaxt = "n",
       ylab = if (panel == "share") "Share of projects (%)" else "Number of projects",
       bty = "n")
  abline(h = axTicks(2), col = "#eeeeee")
  abline(v = 2001.5, col = "#aaaaaa", lty = 3)
  lines(annual$year, terminal, col = "#888888", lwd = 1.8, lty = 2)
  lines(annual$year, withdrawn, col = "#176b9b", lwd = 2.3)
  axis(1, at = seq(1975, 2025, 5))
  if (panel == "share") {
    legend("topright", c("Withdrawn", "Withdrawn or terminated"),
           col = c("#176b9b", "#888888"), lty = c(1, 2), lwd = 2, bty = "n")
    text(1991, max(terminal) * 1.10,
         sprintf("1991: %s withdrawals share one recorded date", cluster$projects),
         cex = 0.8, pos = 4, offset = 0.2)
    text(2001.5, max(terminal) * 0.82, "2001 | 2002", cex = 0.8, pos = 4)
  } else {
    mtext("Reported certification / referral year", side = 1, line = 2.2)
  }
}
mtext("Withdrawals decline across later ULURP project cohorts", outer = TRUE,
      side = 3, line = 1.7, adj = 0, font = 2, cex = 1.25)
mtext("Current project status in the September 14, 2026 ZAP snapshot", outer = TRUE,
      side = 3, line = 0.3, adj = 0, cex = 0.95)
mtext(paste("Each project counted once; explicit ULURP only. These are reported entry-year groups, not withdrawal dates.",
            sprintf("%s projects lack this date, including %s withdrawals. The incomplete 2026 cohort is omitted.",
                    missing_projects, missing_withdrawals),
            "Date clusters are retained. Recent cohorts have incomplete follow-up; shares are not final failure probabilities.",
            sep = "\n"), outer = TRUE, side = 1, line = 3, adj = 0, cex = 0.76)
dev.off()
