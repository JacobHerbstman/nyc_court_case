# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/summarize_rezoning_scope_by_homeownership/code")

# Cross-check the map-based rezoned area against ZAP's project BBL lists. For each
# amendment linked to a ZAP project, compare the lots ZAP lists with the lots whose
# zoning district actually changed inside the amendment footprint. Both use PLUTO
# lot area from the release before the amendment, so the units match. Then redo the
# tercile area shares with ZAP-listed lot area in place of the map-based area.

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
})

source("../../../_lib/data_reports.R")

rezonings <- fread("../output/rezonings.csv", colClasses = "character", na.strings = "")[!is.na(project_id)]
zap_bbls <- as.data.table(read_parquet("../input/zap_project_bbl.parquet",
  col_select = c("project_id", "bbl_standardized", "bbl_valid_format")))[bbl_valid_format == TRUE]
zap_bbls <- unique(zap_bbls[project_id %in% rezonings$project_id, .(project_id, bbl = bbl_standardized)])
panel <- as.data.table(read_parquet("../temp/lot_zoning_panel.parquet", col_select = c("vintage", "bbl", "cd", "lotarea")))
changes <- as.data.table(read_parquet("../temp/lot_capacity_changes.parquet", col_select = c("ulurp", "bbl", "lotarea")))
stopifnot(!anyDuplicated(panel[, .(vintage, bbl)]), !anyDuplicated(rezonings$ulurp))

# ZAP-listed lots with their lot area in the amendment's baseline release.
listed <- merge(zap_bbls, rezonings[, .(project_id, ulurp, vintage = baseline_release)], by = "project_id",
  allow.cartesian = TRUE)
listed <- merge(listed, panel[, .(vintage, bbl, lotarea)], by = c("vintage", "bbl"), all.x = TRUE)
# Lots whose district changed inside the footprint, credited to the amendment.
changed <- unique(changes[!is.na(ulurp), .(ulurp, bbl, lotarea)], by = c("ulurp", "bbl"))

per_rezoning <- merge(
  listed[, .(zap_bbls = .N, zap_bbls_in_pluto = sum(!is.na(lotarea)), zap_lotarea = sum(lotarea, na.rm = TRUE)), by = ulurp],
  changed[, .(map_changed_lots = .N, map_changed_lotarea = sum(lotarea)), by = ulurp], by = "ulurp", all = TRUE)
overlap <- merge(listed[!is.na(lotarea), .(ulurp, bbl, lotarea)], changed[, .(ulurp, bbl)], by = c("ulurp", "bbl"))
per_rezoning <- merge(per_rezoning, overlap[, .(overlap_lots = .N, overlap_lotarea = sum(lotarea)), by = ulurp],
  by = "ulurp", all.x = TRUE)
per_rezoning <- merge(rezonings[, .(ulurp, project_id, project_name, applicant_type, direction, primary_cd, effective)],
  per_rezoning, by = "ulurp", all.x = TRUE)
num <- c("zap_bbls", "zap_bbls_in_pluto", "zap_lotarea", "map_changed_lots", "map_changed_lotarea", "overlap_lots", "overlap_lotarea")
per_rezoning[, (num) := lapply(.SD, function(x) fcoalesce(as.numeric(x), 0)), .SDcols = num]
per_rezoning[, `:=`(zap_area_changed_share = fifelse(zap_lotarea > 0, overlap_lotarea / zap_lotarea, NA_real_),
                    map_area_listed_share = fifelse(map_changed_lotarea > 0, overlap_lotarea / map_changed_lotarea, NA_real_))]
save_csv(as.data.frame(per_rezoning[order(ulurp)]), "../output/rezoning_zap_bbl_crosscheck.csv", key = "ulurp")

# Tercile area shares by the two methods: the amendment's direction, with either the
# ZAP-listed lot area or the map-based changed lot area, over the tercile's 2002 lot area.
cds <- fread("../input/cd_homeownership_1990_measure.csv", select = c("borocd", "borough_name", "treat_pp"))
cds[, tercile := factor(c("Low", "Middle", "High")[dplyr::ntile(treat_pp, 3)], c("Low", "Middle", "High")), by = borough_name]
base <- merge(panel[vintage == "02b", .(lot_area_2002 = sum(lotarea, na.rm = TRUE)), by = .(borocd = cd)], cds, by = "borocd")
by_tercile <- merge(per_rezoning[direction %in% c("upzoning", "downzoning", "contextual_neutral", "mixed")],
  cds[, .(primary_cd = as.character(borocd), tercile)], by = "primary_cd")
by_tercile <- by_tercile[, .(rezonings = .N, with_zap_bbls = sum(zap_bbls > 0),
  zap_lotarea = sum(zap_lotarea), map_changed_lotarea = sum(map_changed_lotarea)), by = .(tercile, direction)]
by_tercile <- merge(by_tercile, base[, .(lot_area_2002 = sum(lot_area_2002)), by = tercile], by = "tercile")
by_tercile[, `:=`(zap_pct_of_lot_area = 100 * zap_lotarea / lot_area_2002,
                  map_pct_of_lot_area = 100 * map_changed_lotarea / lot_area_2002)]
by_tercile[, tercile := as.character(tercile)]
save_csv(as.data.frame(by_tercile[order(direction, tercile)]), "../output/rezoning_zap_bbl_crosscheck_tercile.csv", key = c("tercile", "direction"))

cat(sprintf("%d amendments linked to ZAP; %d have ZAP BBLs; median ZAP area changed %.2f, median map area listed %.2f\n",
  nrow(per_rezoning), sum(per_rezoning$zap_bbls > 0), median(per_rezoning$zap_area_changed_share, na.rm = TRUE),
  median(per_rezoning$map_area_listed_share, na.rm = TRUE)))
