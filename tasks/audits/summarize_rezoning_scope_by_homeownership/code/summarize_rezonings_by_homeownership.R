# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/summarize_rezoning_scope_by_homeownership/code")

# Zoning map amendments by 1990 homeownership (within-borough terciles and the
# continuous treat_z_boro) and Council term: land area up-, down- and
# contextually rezoned, residential capacity added and removed, and amendment
# counts. Also the link between direction and local control: who initiated the
# rezoning, whether the CPC report says local actors asked for it, and the
# local member's Council vote.

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(fixest)
  library(ggplot2)
})

source("../../../_lib/data_reports.R")

terms <- c("2002-05", "2006-09", "2010-13", "2014-17", "2018-21", "2022-25")
term_of <- function(date) {
  year <- as.integer(format(date, "%Y"))
  fifelse(year >= 2002 & year <= 2025, terms[pmin((year - 2002) %/% 4 + 1, 6)], NA_character_)
}

cds <- fread("../input/cd_homeownership_1990_measure.csv",
  select = c("borocd", "borough_name", "treat_pp", "treat_z_boro", "median_household_income_1990"))
stopifnot(nrow(cds) == 59, !anyDuplicated(cds$borocd))
cds[, log_income_1990 := log(median_household_income_1990)]
# Terciles within borough, as in summarize_cd_homeownership_long_units_series.
cds[, tercile := factor(c("Low", "Middle", "High")[dplyr::ntile(treat_pp, 3)], c("Low", "Middle", "High")), by = borough_name]
# July 2002 lot area and residential capacity (02b lot polygons).
cds <- merge(cds, fread("../temp/cd_baseline_2002.csv"), by = "borocd")
stopifnot(nrow(cds) == 59)

rezonings <- fread("../output/rezonings.csv", colClasses = list(character = "ulurp"), na.strings = "")
rezonings[, term := term_of(effective)]
rezonings[, initiation := fcase(applicant_type == "DCP" & cpc_local_request %in% TRUE, "DCP, CPC report cites local request",
  applicant_type == "DCP", "DCP, no local request found",
  cpc_local_request %in% TRUE, "Other applicant, local request",
  default = "Private or other public applicant")]
rezonings <- merge(rezonings, cds[, .(primary_cd = borocd, tercile, treat_z_boro)], by = "primary_cd", all.x = TRUE)

# Attributed lot changes inside their footprints, by the lot's own CD. Lot
# classes as in the rezoning table (Furman Center 10% rule).
changes <- as.data.table(read_parquet("../temp/lot_capacity_changes.parquet"))
attributed <- merge(changes[!is.na(ulurp) & !is.na(area_inside)], rezonings[, .(ulurp, term, initiation)], by = "ulurp")
attributed[, lot_class := fcase(far_pre == 0 & far_post == 0, "nonresidential",
  far_post >= 1.1 * far_pre, "up", far_post < 0.9 * far_pre, "down", default = "contextual")]
unattributed <- changes[is.na(ulurp) & interval != "17v1-18v1.1"][, term := term_of(asof_post)]

# 1. CD x term summary --------------------------------------------------------
grid <- CJ(borocd = cds$borocd, term = terms)
counts <- rezonings[!is.na(term) & primary_cd %in% cds$borocd, .(
  rezonings = .N, upzonings = sum(direction == "upzoning"), downzonings = sum(direction == "downzoning"),
  mixed = sum(direction == "mixed"), contextual = sum(direction == "contextual_neutral"),
  downzonings_dcp_requested = sum(direction == "downzoning" & initiation == "DCP, CPC report cites local request"),
  upzonings_dcp = sum(direction == "upzoning" & applicant_type == "DCP")), by = .(borocd = primary_cd, term)]
measured <- attributed[!is.na(term) & cd %in% cds$borocd, .(
  area_upzoned = sum(area_inside[lot_class == "up"]),
  area_downzoned = sum(area_inside[lot_class == "down"]),
  area_contextual = sum(area_inside[lot_class == "contextual"]),
  area_downzoned_dcp_requested = sum(area_inside[lot_class == "down" & initiation == "DCP, CPC report cites local request"]),
  area_contextual_dcp_requested = sum(area_inside[lot_class == "contextual" & initiation == "DCP, CPC report cites local request"]),
  capacity_added = sum(pmax(cap_change_inside, 0)), capacity_removed = sum(pmax(-cap_change_inside, 0)),
  capacity_net = sum(cap_change_inside),
  capacity_removed_dcp_requested = sum(pmax(-cap_change_inside, 0)[initiation == "DCP, CPC report cites local request"]),
  capacity_removed_dcp_other = sum(pmax(-cap_change_inside, 0)[initiation == "DCP, no local request found"]),
  capacity_removed_other_applicant = sum(pmax(-cap_change_inside, 0)[!initiation %like% "^DCP"]),
  capacity_added_dcp = sum(pmax(cap_change_inside, 0)[initiation %like% "^DCP"]),
  capacity_added_other_applicant = sum(pmax(cap_change_inside, 0)[!initiation %like% "^DCP"])), by = .(borocd = cd, term)]
# Unattributed changes have no footprint: full-lot change, a noise gauge only.
noise <- unattributed[!is.na(term) & cd %in% cds$borocd, .(unattributed_capacity_net = sum(cap_change)), by = .(borocd = cd, term)]
cd_term <- Reduce(function(x, y) merge(x, y, by = c("borocd", "term"), all.x = TRUE), list(grid, counts, measured, noise))
value_cols <- setdiff(names(cd_term), c("borocd", "term"))
cd_term[, (value_cols) := lapply(.SD, function(x) fcoalesce(as.numeric(x), 0)), .SDcols = value_cols]
cd_term <- merge(cds, cd_term, by = "borocd")
area_cols <- grep("^area_", names(cd_term), value = TRUE)
capacity_cols <- grep("^capacity_|^unattributed", names(cd_term), value = TRUE)
cd_term[, paste0(area_cols, "_pct_lot_area") := lapply(.SD, function(x) 100 * x / lot_area_2002), .SDcols = area_cols]
cd_term[, paste0(capacity_cols, "_pct_capacity") := lapply(.SD, function(x) 100 * x / capacity_2002), .SDcols = capacity_cols]
save_csv(as.data.frame(cd_term[order(borocd, term)]), "../output/rezoning_cd_term_summary.csv", key = c("borocd", "term"))

# 2. Homeownership gradients ----------------------------------------------------
# Per term and pooled: outcome on treat_z_boro with borough fixed effects and
# heteroskedasticity-robust standard errors. Three specifications: all 59
# community districts; without the four districts behind most early upzoned
# capacity (301, 302, 401, 402); and adding log 1990 median household income,
# the production event study's control. Outcomes that are zero everywhere in a
# term have no estimate.
outcomes <- c("area_upzoned_pct_lot_area", "area_downzoned_pct_lot_area", "area_contextual_pct_lot_area",
  "area_downzoned_dcp_requested_pct_lot_area", "area_contextual_dcp_requested_pct_lot_area", "capacity_added_pct_capacity", "capacity_removed_pct_capacity",
  "capacity_net_pct_capacity", "capacity_removed_dcp_requested_pct_capacity", "capacity_removed_dcp_other_pct_capacity",
  "capacity_removed_other_applicant_pct_capacity", "capacity_added_dcp_pct_capacity",
  "capacity_added_other_applicant_pct_capacity", "unattributed_capacity_net_pct_capacity",
  "upzonings", "downzonings", "downzonings_dcp_requested")
pooled <- cd_term[, lapply(.SD, sum), by = .(borocd, borough_name, treat_z_boro, log_income_1990, lot_area_2002, capacity_2002),
  .SDcols = c("upzonings", "downzonings", "downzonings_dcp_requested", area_cols, capacity_cols)]
pooled[, paste0(area_cols, "_pct_lot_area") := lapply(.SD, function(x) 100 * x / lot_area_2002), .SDcols = area_cols]
pooled[, paste0(capacity_cols, "_pct_capacity") := lapply(.SD, function(x) 100 * x / capacity_2002), .SDcols = capacity_cols]
pooled[, term := "2002-25"]
specifications <- list(
  all_cds = list(drop = integer(0), controls = ""),
  without_301_302_401_402 = list(drop = c(301L, 302L, 401L, 402L), controls = ""),
  income_control = list(drop = integer(0), controls = " + log_income_1990"))
panel_terms <- split(rbind(cd_term, pooled, fill = TRUE), by = "term")
gradients <- rbindlist(lapply(names(specifications), function(spec) rbindlist(lapply(outcomes, function(y) rbindlist(lapply(panel_terms, function(d) {
  d <- d[!borocd %in% specifications[[spec]]$drop]
  if (var(d[[y]]) == 0) return(data.table(specification = spec, outcome = y, term = d$term[1], coefficient = NA_real_,
    std_error = NA_real_, p_value = NA_real_, outcome_mean = mean(d[[y]]), cds = nrow(d)))
  fit <- feols(as.formula(paste0(y, " ~ treat_z_boro", specifications[[spec]]$controls, " | borough_name")), data = d, vcov = "hetero")
  data.table(specification = spec, outcome = y, term = d$term[1], coefficient = coef(fit)[["treat_z_boro"]],
    std_error = se(fit)[["treat_z_boro"]], p_value = pvalue(fit)[["treat_z_boro"]], outcome_mean = mean(d[[y]]), cds = nobs(fit))
}))))))
save_csv(as.data.frame(gradients), "../output/rezoning_homeownership_gradients.csv", key = c("specification", "outcome", "term"))

# 3. Local-control link table ---------------------------------------------------
# Rezonings by direction (and by tercile), with sponsor, CPC-report request
# evidence, Council outcome and the local member's recorded position.
link_rows <- function(d) d[, .(
  rezonings = .N,
  dcp_sponsored = sum(applicant_type == "DCP", na.rm = TRUE),
  with_cpc_report_text = sum(cpc_report_text),
  cpc_local_request = sum(cpc_local_request, na.rm = TRUE),
  cpc_request_names_member = sum(cpc_request_names_member, na.rm = TRUE),
  with_local_member_position = sum(!is.na(local_member_position)),
  local_member_opposed = sum(local_member_position == "opposes", na.rm = TRUE),
  council_disapproved = sum(council_outcome == "disapproved", na.rm = TRUE),
  approved_over_local_opposition = sum(council_override, na.rm = TRUE),
  council_modified = sum(council_modified, na.rm = TRUE),
  amended_application = sum(grepl("^[0-9]{6}[A-Z]ZM", ulurp)),
  area_rezoned = sum(area_rezoned),
  capacity_net = sum(capacity_net),
  median_net_pct_of_rezoned = median(net_pct_of_rezoned, na.rm = TRUE))]
in_terms <- rezonings[!is.na(term)][, tercile := as.character(tercile)]
link <- rbind(
  in_terms[, link_rows(.SD), by = direction][, `:=`(tercile = "All", initiation = "All")],
  in_terms[!is.na(tercile), link_rows(.SD), by = .(direction, tercile)][, initiation := "All"],
  in_terms[, link_rows(.SD), by = .(direction, initiation)][, tercile := "All"],
  in_terms[!is.na(tercile), link_rows(.SD), by = .(direction, tercile, initiation)],
  fill = TRUE)
setcolorder(link, c("direction", "tercile", "initiation"))
save_csv(as.data.frame(link[order(direction, tercile, initiation)]), "../output/rezoning_local_control_link.csv",
  key = c("direction", "tercile", "initiation"))

# 4. Figures ----------------------------------------------------------------------
# Tercile shares pool the tercile's CDs: sum of area (capacity) over sum of
# July 2002 lot area (capacity).
theme_set(theme_minimal(base_size = 11) + theme(panel.grid.minor = element_blank(), legend.position = "bottom",
  axis.text.x = element_text(angle = 45, hjust = 1)))
tercile_colors <- c(Low = "#1b9e77", Middle = "#7570b3", High = "#d95f02")
tercile_scale <- scale_colour_manual(values = tercile_colors, name = "1990 homeownership (within borough)")
term_axis <- labs(x = "Council term (amendment effective date)")
by_tercile <- cd_term[, lapply(.SD, sum), by = .(tercile, term),
  .SDcols = c("lot_area_2002", "capacity_2002", area_cols, capacity_cols, "upzonings", "downzonings", "mixed", "contextual")]
tercile_long <- function(cols, labels, denominator) {
  long <- melt(by_tercile, id.vars = c("tercile", "term", denominator), measure.vars = cols, variable.name = "measure")
  long[, `:=`(pct = 100 * value / get(denominator), measure = factor(measure, cols, labels))]
}

gradient_outcomes <- c(area_upzoned_pct_lot_area = "Lot area upzoned", area_downzoned_pct_lot_area = "Lot area downzoned",
  area_contextual_pct_lot_area = "Lot area contextually rezoned", capacity_added_pct_capacity = "Capacity added",
  capacity_removed_pct_capacity = "Capacity removed")
plot_gradients <- gradients[specification == "all_cds" & outcome %in% names(gradient_outcomes)]
plot_gradients[, `:=`(term = factor(term, c(terms, "2002-25")),
  outcome = factor(gradient_outcomes[outcome], gradient_outcomes))]

area_long <- tercile_long(c("area_upzoned", "area_downzoned", "area_contextual"),
  c("Upzoned", "Downzoned", "Contextual (FAR within 10%)"), "lot_area_2002")
pdf("../output/rezoning_land_area_by_tercile_term.pdf", width = 10, height = 6.5)
print(ggplot(area_long, aes(term, pct, colour = tercile, group = tercile)) + geom_line(linewidth = 0.9) + geom_point(size = 2) +
  facet_wrap(~measure, nrow = 1) + tercile_scale + term_axis +
  labs(y = "% of the tercile's July 2002 lot area", title = "Land area rezoned by zoning map amendments",
    subtitle = "Lot area inside amendment footprints, by each lot's change in maximum residential FAR (10% threshold)"))
print(ggplot(area_long[measure != "Contextual (FAR within 10%)"], aes(term, pct, fill = measure)) + geom_col(position = "dodge") +
  facet_wrap(~tercile, nrow = 1) + scale_fill_manual(values = c(Upzoned = "#2166ac", Downzoned = "#b2182b"), name = NULL) +
  term_axis + labs(y = "% of the tercile's July 2002 lot area", title = "Land area upzoned and downzoned, by homeownership tercile"))
dev.off()

pdf("../output/rezoning_homeownership_gradients.pdf", width = 10, height = 7)
print(ggplot(plot_gradients, aes(term, coefficient)) + geom_hline(yintercept = 0, colour = "grey60") +
  geom_pointrange(aes(ymin = coefficient - 1.96 * std_error, ymax = coefficient + 1.96 * std_error)) +
  facet_wrap(~outcome, nrow = 2) +
  labs(x = "Council term (2002-25 = all terms pooled)", y = "Percentage points per 1 SD of 1990 homeownership\n(area: % of lot area; capacity: % of capacity)",
    title = "Homeownership gradient in land area rezoned and capacity changed, by term",
    subtitle = "Coefficient on treat_z_boro with borough fixed effects, 59 community districts, 95% robust intervals"))
dev.off()

capacity_long <- tercile_long(c("capacity_added", "capacity_removed", "capacity_net"),
  c("Capacity added", "Capacity removed", "Net change"), "capacity_2002")
removed_long <- tercile_long(c("capacity_removed_dcp_requested", "capacity_removed_dcp_other", "capacity_removed_other_applicant"),
  c("DCP, CPC report cites local request", "DCP, no local request found", "Private or other public applicant"), "capacity_2002")
pdf("../output/rezoning_capacity_by_tercile_term.pdf", width = 10, height = 6.5)
print(ggplot(capacity_long, aes(term, pct, colour = tercile, group = tercile)) +
  geom_hline(yintercept = 0, colour = "grey60") + geom_line(linewidth = 0.9) + geom_point(size = 2) +
  facet_wrap(~measure, nrow = 1) + tercile_scale + term_axis +
  labs(y = "% of the tercile's July 2002 residential capacity", title = "Residential capacity changed by zoning map amendments",
    subtitle = "Maximum residential FAR (PLUTO ResidFAR by district) x lot area inside the amendment footprint"))
print(ggplot(removed_long, aes(term, pct, fill = measure)) + geom_col() + facet_wrap(~tercile, nrow = 1) +
  scale_fill_manual(values = c("#d95f02", "#fdb863", "#b2abd2"), name = NULL) + term_axis +
  labs(y = "% of the tercile's July 2002 residential capacity", title = "Capacity removed by zoning map amendments, by who initiated them",
    subtitle = "Local request: the CPC report says the rezoning answered requests or concerns of council members, community boards or civic groups"))
dev.off()

counts_by_sponsor <- rbind(
  rezonings[!is.na(term) & !is.na(tercile), .N, by = .(tercile, term, direction)][, sponsor := "All applicants"],
  rezonings[!is.na(term) & !is.na(tercile) & applicant_type == "DCP", .N, by = .(tercile, term, direction)][, sponsor := "DCP-sponsored"])
counts_by_sponsor <- merge(CJ(tercile = factor(c("Low", "Middle", "High"), c("Low", "Middle", "High")), term = terms,
  direction = c("upzoning", "downzoning", "mixed", "contextual_neutral"), sponsor = c("All applicants", "DCP-sponsored")),
  counts_by_sponsor, by = c("tercile", "term", "direction", "sponsor"), all.x = TRUE)[, N := fcoalesce(N, 0L)]
counts_by_sponsor[, direction := factor(direction, c("upzoning", "downzoning", "mixed", "contextual_neutral"),
  c("Upzonings", "Downzonings", "Mixed", "Contextual / neutral"))]
pdf("../output/rezoning_counts_by_tercile_term.pdf", width = 10, height = 6.5)
print(ggplot(counts_by_sponsor, aes(term, N, colour = tercile, group = tercile)) + geom_line(linewidth = 0.9) + geom_point(size = 2) +
  facet_grid(sponsor ~ direction, scales = "free_y") + tercile_scale + term_axis +
  labs(y = "Zoning map amendments", title = "Zoning map amendments by direction and the primary community district's homeownership tercile",
    subtitle = "Direction from net residential capacity change of the rezoned lots (10% threshold, Furman Center 2010)"))
dev.off()
