# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/summarize_rezoning_scope_by_homeownership/code")

# Lot-level changes in maximum residential floor area between consecutive
# PLUTO releases, attributed to adopted zoning map amendments (DCP nyzma).
#
# Residential FAR is a fixed lookup from a lot's primary zoning district
# (zonedist1) to the ResidFAR PLUTO reports for that district, so capacity only
# changes when the district changes. Capacity change = (FAR after - FAR before)
# x lot area before, for lots with the same BBL in both releases.

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(sf)
})

panel <- as.data.table(read_parquet("../temp/lot_zoning_panel.parquet"))
releases <- unique(panel[, .(vintage, zoning_asof)])[order(zoning_asof)]

# 1. Residential FAR by zoning district --------------------------------------
# A district's FAR is the modal ResidFAR among single-district lots in 24v1,
# the last release before the December 2024 City of Yes text changes. Districts
# absent from 24v1 take the mode from the closest release that has them:
# 23v1 back to 13v1 (the first with ResidFAR), then 25v4 and 25v1 (25v1 still
# reports 0 for the new R6-1 district).
residfar_releases <- c("24v1", "23v1", "22v1", "21v1", "20v1", "19v1", "18v1.1", "17v1", "16v1",
  "15v1", "14v1", "13v1", "25v4", "25v1")
far_modes <- panel[vintage %in% residfar_releases & is.na(zonedist2) & !is.na(zonedist1) & !is.na(residfar),
  .N, by = .(vintage, zonedist1, residfar)][order(-N)][, .SD[1], by = .(vintage, zonedist1)]
far_modes[, release_rank := match(vintage, residfar_releases)]
far_lookup <- far_modes[order(release_rank), .SD[1], by = zonedist1][, .(zonedist1, far = residfar, far_source = vintage)]

# Paired manufacturing/residence districts not in any release (older notation
# such as M1-2(R6), and districts first mapped in 2025) take the FAR of their
# residence district, which is how PLUTO assigns ResidFAR to the pairs it has.
paired <- data.table(zonedist1 = setdiff(grep("R[0-9]", unique(panel$zonedist1), value = TRUE), far_lookup$zonedist1))
paired[, residence := sub("^.*(R[0-9]+[A-Z]?(-[0-9])?[A-Z]?).*$", "\\1", zonedist1)]
paired <- merge(paired, far_lookup[, .(residence = zonedist1, far)], by = "residence")
far_lookup <- rbind(far_lookup, paired[, .(zonedist1, far, far_source = paste("residence district", residence))])

# Unpaired manufacturing districts retired before 13v1 (M1-3D, M1-6M) get 0,
# as PLUTO gives every unpaired manufacturing district in 24v1 except M1-6D.
retired_m <- setdiff(grep("^M[0-9]", unique(panel$zonedist1), value = TRUE), far_lookup$zonedist1)
far_lookup <- rbind(far_lookup, data.table(zonedist1 = retired_m, far = 0, far_source = "unpaired manufacturing district"))
stopifnot(!anyDuplicated(far_lookup$zonedist1))

# Agreement with PLUTO's own ResidFAR, lot by lot. Before 19v1 PLUTO added a
# 20% attic allowance in R2X, R3, R4 and C3 districts; 25v4 reflects City of Yes.
attic <- "^(R2X|R3|R4|C3)"
agreement <- merge(panel[!is.na(residfar), .(vintage, zonedist1, residfar)], far_lookup, by = "zonedist1")[,
  .(lots = .N, share_equal = mean(abs(far - residfar) < 0.005),
    share_equal_excluding_attic_districts = mean(abs(far - residfar)[!grepl(attic, zonedist1)] < 0.005)), by = vintage]
print(agreement)
cat("zonedist1 codes without a FAR:", paste(sort(setdiff(unique(panel$zonedist1), far_lookup$zonedist1)), collapse = " "), "\n")
fwrite(far_lookup[order(zonedist1)], "../temp/residential_far_by_district.csv")

# 2. Zoning map amendments ---------------------------------------------------
# Adopted amendments, one footprint per ULURP number.
zma <- st_read("/vsizip/../input/nycgiszoningfeatures_202608shp.zip/nycgiszoningfeatures_202608shp/nyzma.shp", quiet = TRUE)
zma$effective <- as.Date(zma$EFFECTIVE, optional = TRUE)
zma <- zma[zma$STATUS == "Adopted" & !is.na(zma$effective) & zma$effective >= as.Date("2001-01-01"), ]
zma$ulurp <- toupper(zma$ULURPNO)
zma <- aggregate(zma[, "effective"], by = list(ulurp = zma$ulurp), FUN = max)
zma <- st_make_valid(zma)

# 3. Lot changes between consecutive releases --------------------------------
# Lots keep their BBL in both releases. Lots coded PARK in either release and
# districts without a FAR are left out. A change is attributed to an amendment
# whose footprint contains the lot's PLUTO coordinate (earlier release, else
# later release) and whose effective date falls in the interval. PLUTO's
# zoning lags adoption (03c shows only some amendments adopted in 2003), so
# amendments effective up to 180 days before the earlier release or 31 days
# after the later one also qualify; the latest amendment inside the interval
# is preferred, then the one closest to it.
setkey(far_lookup, zonedist1)
usable <- function(code) !is.na(code) & !grepl("^PARK", code) & code %chin% far_lookup$zonedist1
changes <- list()
for (i in seq_len(nrow(releases) - 1)) {
  asof_pre <- releases$zoning_asof[i]
  asof_post <- releases$zoning_asof[i + 1]
  pre <- panel[vintage == releases$vintage[i]]
  post <- panel[vintage == releases$vintage[i + 1]]
  lots <- merge(pre[, .(bbl, cd, zd_pre = zonedist1, zd2_pre = zonedist2, lotarea, x_pre = xcoord, y_pre = ycoord)],
    post[, .(bbl, zd_post = zonedist1, zd2_post = zonedist2, x_post = xcoord, y_post = ycoord)], by = "bbl")
  cat(releases$vintage[i], "->", releases$vintage[i + 1], ": lot area in lots kept in both releases",
    round(sum(lots$lotarea, na.rm = TRUE) / sum(pre$lotarea, na.rm = TRUE), 4), "\n")
  lots <- lots[zd_pre != zd_post & usable(zd_pre) & usable(zd_post) & lotarea > 0]
  lots[, `:=`(interval = paste(releases$vintage[i], releases$vintage[i + 1], sep = "-"),
    asof_pre = asof_pre, asof_post = asof_post,
    far_pre = far_lookup[.(zd_pre), far], far_post = far_lookup[.(zd_post), far],
    x = fcoalesce(x_pre, x_post), y = fcoalesce(y_pre, y_post))]

  window <- zma[zma$effective >= asof_pre - 180 & zma$effective < asof_post + 31, ]
  located <- lots[!is.na(x)]
  hits <- st_join(st_as_sf(located[, .(bbl, x, y)], coords = c("x", "y"), crs = st_crs(zma)),
    window[, c("ulurp", "effective")], join = st_within, left = FALSE)
  hits <- as.data.table(st_drop_geometry(hits))
  hits[, gap_days := fifelse(effective < asof_pre, as.numeric(effective - asof_pre),
    fifelse(effective >= asof_post, as.numeric(effective - asof_post), 0))]
  hits <- hits[order(bbl, abs(gap_days), -as.numeric(effective))][, .(ulurp = ulurp[1], gap_days = gap_days[1],
    candidate_amendments = .N), by = bbl]
  changes[[i]] <- merge(lots, hits, by = "bbl", all.x = TRUE)
}
changes <- rbindlist(changes)
changes[, cap_change := (far_post - far_pre) * lotarea]
changes <- changes[, .(interval, asof_pre, asof_post, bbl, cd, lotarea, zd_pre, zd_post, far_pre, far_post,
  cap_change, split_zoned = !is.na(zd2_pre) | !is.na(zd2_post), located = !is.na(x), ulurp, gap_days, candidate_amendments)]
print(changes[, .(changed_lots = .N, unlocated = sum(!located), abs_change_m = sum(abs(cap_change)) / 1e6,
  attributed_share = sum(abs(cap_change)[!is.na(ulurp)]) / sum(abs(cap_change)),
  via_slack_share = sum(abs(cap_change)[!is.na(ulurp) & gap_days != 0]) / sum(abs(cap_change))), by = interval])

# 3b. Area of each attributed lot inside its amendment -------------------------
# Capacity change counts only the part of a lot inside the amendment footprint:
# change inside = (FAR after - FAR before) x area of (lot polygon ∩ footprint),
# in square feet (State Plane Long Island, feet). Lot polygons come from the
# MapPLUTO shapefile (02b, 18v1.1 or 25v4) nearest in time to the interval
# among those that have the BBL. Polygon areas also drop the underwater part
# of waterfront tax lots, which PLUTO's LotArea includes. Lots in none of the
# three releases get no area and are left out of attributed capacity.
geometry_releases <- data.table(geometry_release = c("02b", "18v1.1", "25v4"),
  geometry_date = as.Date(c("2002-07-01", "2018-12-01", "2026-02-01")))
read_polygons <- function(path, layer, fields) {
  lots <- st_read(sprintf("/vsizip/%s/%s", path, layer), quiet = TRUE,
    query = sprintf("SELECT %s FROM \"%s\"", fields, sub("\\.shp$", "", basename(layer))))
  st_transform(lots, st_crs(zma))
}
polygons <- list()
polygons[["02b"]] <- do.call(rbind, lapply(c("Bronx/bxmappluto.shp", "Brooklyn/bkmappluto.shp",
  "Manhattan/mnmappluto.shp", "Queens/qnmappluto.shp", "Staten_Island/simappluto.shp"), function(layer) {
  lots <- read_polygons("../input/mappluto_02b.zip", paste0("MapPLUTO_02B/", layer), "borough, block, lot")
  lots$bbl <- sprintf("%d%05d%04d", match(lots$borough, c("MN", "BX", "BK", "QN", "SI")), as.integer(lots$block), as.integer(lots$lot))
  lots[, "bbl"]
}))
for (v in c("18v1.1", "25v4")) {
  lots <- read_polygons(sprintf("../input/nyc_mappluto_%s_arc_shp.zip", sub(".", "_", v, fixed = TRUE)), "MapPLUTO.shp", "BBL")
  lots$bbl <- sprintf("%.0f", lots$BBL)
  polygons[[v]] <- lots[, "bbl"]
}
for (v in names(polygons)) {
  st_agr(polygons[[v]]) <- "constant"
  stopifnot(!anyDuplicated(polygons[[v]]$bbl))
}
available <- merge(rbindlist(lapply(names(polygons), function(v) data.table(bbl = polygons[[v]]$bbl, geometry_release = v))),
  geometry_releases, by = "geometry_release")

attributed <- unique(changes[!is.na(ulurp), .(interval, bbl, ulurp, midpoint = asof_pre + (asof_post - asof_pre) / 2)])
attributed <- merge(attributed, available, by = "bbl", all.x = TRUE, allow.cartesian = TRUE)
attributed <- attributed[order(interval, bbl, abs(as.numeric(midpoint - geometry_date)))][, .SD[1], by = .(interval, bbl)]
st_agr(zma) <- "constant"
inside <- rbindlist(lapply(split(attributed[!is.na(geometry_release)], by = c("geometry_release", "ulurp")), function(d) {
  lots <- st_make_valid(polygons[[d$geometry_release[1]]][polygons[[d$geometry_release[1]]]$bbl %in% d$bbl, ])
  pieces <- st_intersection(lots, st_geometry(zma[zma$ulurp == d$ulurp[1], ]))
  merge(data.table(bbl = lots$bbl, polygon_area = as.numeric(st_area(lots))),
    data.table(bbl = pieces$bbl, area_inside = as.numeric(st_area(pieces)))[, .(area_inside = sum(area_inside)), by = bbl],
    by = "bbl", all.x = TRUE)[, `:=`(ulurp = d$ulurp[1], geometry_release = d$geometry_release[1], area_inside = fcoalesce(area_inside, 0))]
}))
attributed <- merge(attributed[, .(interval, bbl, ulurp)], inside, by = c("bbl", "ulurp"), all.x = TRUE)
changes <- merge(changes, attributed, by = c("interval", "bbl", "ulurp"), all.x = TRUE)
changes[, cap_change_inside := (far_post - far_pre) * area_inside]

print(changes[!is.na(ulurp), .(attributed_lots = .N, without_polygon = sum(is.na(area_inside)),
  abs_change_without_polygon_m = sum(abs(cap_change)[is.na(area_inside)]) / 1e6,
  abs_change_full_lot_m = sum(abs(cap_change)) / 1e6, abs_change_inside_m = sum(abs(cap_change_inside), na.rm = TRUE) / 1e6,
  lotarea_m = sum(lotarea) / 1e6, area_inside_m = sum(area_inside, na.rm = TRUE) / 1e6,
  split_zoned_share_inside = sum(area_inside[split_zoned], na.rm = TRUE) / sum(polygon_area[split_zoned], na.rm = TRUE),
  unsplit_share_inside = sum(area_inside[!split_zoned], na.rm = TRUE) / sum(polygon_area[!split_zoned], na.rm = TRUE))])
write_parquet(changes, "../temp/lot_capacity_changes.parquet")

# 4. Baselines -----------------------------------------------------------------
# Community district baseline: 02b lot polygons with a mapped district other
# than PARK, their area and residential capacity (FAR x polygon area).
far_02b <- merge(panel[vintage == "02b" & usable(zonedist1), .(bbl, cd, zonedist1)], far_lookup, by = "zonedist1")
polygon_area_02b <- data.table(bbl = polygons[["02b"]]$bbl, area = as.numeric(st_area(polygons[["02b"]])))
cd_baseline <- merge(far_02b, polygon_area_02b, by = "bbl")[, .(lot_area_2002 = sum(area),
  capacity_2002 = sum(far * area)), by = .(borocd = cd)]
cat("02b lots with a usable district matched to a polygon:", nrow(merge(far_02b, polygon_area_02b, by = "bbl")), "of", nrow(far_02b), "\n")
fwrite(cd_baseline[order(borocd)], "../temp/cd_baseline_2002.csv")

# Footprint baseline: lot area and capacity inside each footprint, by
# community district, using the zoning of the last release before the
# effective date and polygons from the shapefile release nearest that date.
baseline_index <- findInterval(zma$effective, releases$zoning_asof)
zma <- zma[baseline_index > 0, ]
zma$baseline <- releases$vintage[baseline_index[baseline_index > 0]]
footprints <- list()
for (v in unique(zma$baseline)) {
  asof <- releases[vintage == v, zoning_asof]
  g <- geometry_releases[which.min(abs(as.numeric(asof - geometry_date))), geometry_release]
  zoned <- merge(panel[vintage == v & usable(zonedist1), .(bbl, cd, zonedist1)], far_lookup, by = "zonedist1")
  lots <- merge(polygons[[g]], zoned[, .(bbl, cd, far)], by = "bbl")
  amendments <- zma[zma$baseline == v, "ulurp"]
  lots <- st_make_valid(lots[lengths(st_intersects(lots, amendments)) > 0, ])
  st_agr(lots) <- "constant"
  st_agr(amendments) <- "constant"
  pieces <- st_intersection(lots, amendments)
  footprints[[v]] <- data.table(ulurp = pieces$ulurp, cd = pieces$cd, far = pieces$far, area = as.numeric(st_area(pieces)))[,
    .(baseline_release = v, geometry_release = g, footprint_lots = .N, footprint_lotarea = sum(area),
      footprint_capacity = sum(far * area)), by = .(ulurp, cd)]
}
footprints <- merge(as.data.table(st_drop_geometry(zma))[, .(ulurp, effective)], rbindlist(footprints), by = "ulurp", all.x = TRUE)
fwrite(footprints[order(effective, ulurp, cd)], "../temp/zma_footprints.csv")
