# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/summarize_rezoning_scope_by_homeownership/code")

# Lot-by-release zoning panel from one PLUTO/MapPLUTO release per year,
# 2002-2025. Keeps only the fields used to measure zoning and residential
# capacity. zoning_asof is the month the release's zoning reflects: the
# ZoningDate field where PLUTO carries one (02b metadata for 02b), otherwise
# the month DCP wrote the release's lot files (an upper bound).

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
})

keep <- c("borough", "block", "lot", "cd", "zonedist1", "zonedist2", "zonedist3", "zonedist4",
  "overlay1", "overlay2", "spdist1", "spdist2", "spdist3", "lotarea", "residfar", "xcoord", "ycoord")

standardize <- function(lots, vintage, zoning_asof) {
  lots <- as.data.table(lots)
  setnames(lots, tolower(names(lots)))
  setnames(lots, "cd2", "cd", skip_absent = TRUE)
  for (field in setdiff(keep, names(lots))) lots[, (field) := NA]
  lots <- lots[, ..keep]
  # Three 07c Staten Island records (block 8050) are corrupted: invalid bytes or
  # non-numeric values in numeric fields. Such records are dropped.
  numeric_fields <- c("block", "lot", "cd", "lotarea", "residfar", "xcoord", "ycoord")
  malformed <- Reduce(`|`, c(
    lapply(lots, function(x) !validUTF8(as.character(x)) %in% TRUE),
    lapply(lots[, ..numeric_fields], function(x) {
      x <- as.character(x)
      x <- trimws(replace(x, which(!validUTF8(x)), "corrupted"))
      !is.na(x) & x != "" & is.na(suppressWarnings(as.numeric(x)))
    })
  ))
  if (any(malformed)) cat(vintage, "malformed records dropped:", sum(malformed), "\n")
  lots <- lots[!malformed]
  text_fields <- c("borough", keep[grepl("^(zonedist|overlay|spdist)", keep)])
  lots[, (text_fields) := lapply(.SD, function(x) {
    x <- toupper(trimws(as.character(x)))
    fifelse(x %in% c("", "NA"), NA_character_, x)
  }), .SDcols = text_fields]
  lots[, borough := fcase(borough %in% c("MN", "1"), 1L, borough %in% c("BX", "2"), 2L,
    borough %in% c("BK", "3"), 3L, borough %in% c("QN", "4"), 4L, borough %in% c("SI", "5"), 5L)]
  lots[, `:=`(block = as.integer(block), lot = as.integer(lot), cd = as.integer(cd),
    lotarea = as.numeric(lotarea), residfar = as.numeric(residfar),
    xcoord = as.numeric(xcoord), ycoord = as.numeric(ycoord))]
  lots[xcoord == 0 | ycoord == 0, `:=`(xcoord = NA, ycoord = NA)]
  lots[, bbl := sprintf("%d%05d%04d", borough, block, lot)]
  lots[, `:=`(vintage = vintage, zoning_asof = as.Date(zoning_asof))]
  stopifnot(!anyNA(lots$borough), !anyNA(lots$block), !anyNA(lots$lot), !anyDuplicated(lots$bbl))
  cat(format(Sys.time(), "%H:%M:%S"), vintage, nrow(lots), "lots\n")
  lots
}

# Borough files are read without their header line and given the header of
# header_file. NUL bytes (padding in some address fields), DOS end-of-file
# characters, and empty or all-comma lines at file ends are dropped. A lot
# record cut short (one in 13v1 Queens) keeps its trailing fields empty.
read_pluto_text <- function(zip_path, files, header_file = files[1], first_line_fix = "") {
  header <- names(fread(cmd = sprintf("unzip -p '%s' '%s'", zip_path, header_file), nrows = 0))
  rbindlist(lapply(files, function(file) {
    unzip_cmd <- sprintf("unzip -p '%s' '%s' | tr -d '\\000\\032' %s | sed -e '1{/^\"*Borough/d;}' -e '/^[[:space:],]*$/d'",
      zip_path, file, first_line_fix)
    lots <- fread(cmd = unzip_cmd, header = FALSE, col.names = header, colClasses = "character",
      strip.white = TRUE, fill = TRUE)
    stopifnot(nrow(lots) == as.integer(system(paste(unzip_cmd, "| wc -l"), intern = TRUE)))
    lots
  }))
}

# The DBF is extracted alone so that no geometry is read.
read_mappluto_dbf <- function(zip_path, dbf) {
  dbf_dir <- tempfile("mappluto_dbf_")
  on.exit(unlink(dbf_dir, recursive = TRUE))
  unzip(zip_path, files = dbf, exdir = dbf_dir, junkpaths = TRUE, unzip = "unzip")
  foreign::read.dbf(file.path(dbf_dir, basename(dbf)), as.is = TRUE)
}

boro_files <- function(zip_path) {
  grep("\\.(txt|csv)$", unzip(zip_path, list = TRUE)$Name, value = TRUE, ignore.case = TRUE)
}

zoning_date <- function(lots) {
  asof <- names(which.max(table(trimws(lots$ZoningDate))))
  as.Date(paste0("01/", asof), format = "%d/%m/%Y")
}

panel <- list()

# 02b MapPLUTO: one DBF per borough; metadata states "Zoning Data - July, 2002".
panel[["02b"]] <- standardize(rbindlist(lapply(
  c("Bronx/bxmappluto.dbf", "Brooklyn/bkmappluto.dbf", "Manhattan/mnmappluto.dbf",
    "Queens/qnmappluto.dbf", "Staten_Island/simappluto.dbf"),
  function(dbf) read_mappluto_dbf("../input/mappluto_02b.zip", paste0("MapPLUTO_02B/", dbf))
), use.names = TRUE, fill = TRUE), "02b", "2002-07-01")

# Tabular PLUTO releases with a ZoningDate field.
for (v in c("03c", "04c", "05d", "07c", "09v1", "10v1")) {
  zip_path <- sprintf("../input/nyc_pluto_%s.zip", v)
  lots <- read_pluto_text(zip_path, boro_files(zip_path))
  panel[[v]] <- standardize(lots, v, zoning_date(lots))
}

# 06c: MN06C.TXT has no header row and BK06C.TXT has its header glued to the
# first lot record, so both are read with the header of BX06C.TXT.
lots <- rbind(
  read_pluto_text("../input/nyc_pluto_06c.zip", c("BX06C.TXT", "MN06C.TXT", "QN06C.TXT", "SI06C.TXT")),
  read_pluto_text("../input/nyc_pluto_06c.zip", "BK06C.TXT", header_file = "BX06C.TXT",
    first_line_fix = "| sed '1s/^.*\"PLUTOMapID\"//'")
)
panel[["06c"]] <- standardize(lots, "06c", zoning_date(lots))

# Tabular PLUTO releases without a ZoningDate field: month the lot files were written.
for (v in c("11v1", "12v1", "13v1", "14v1", "15v1", "16v1", "17v1")) {
  zip_path <- sprintf("../input/nyc_pluto_%s.zip", v)
  files <- boro_files(zip_path)
  written <- unzip(zip_path, list = TRUE)
  asof <- format(max(written$Date[written$Name %in% files]), "%Y-%m-01")
  panel[[v]] <- standardize(read_pluto_text(zip_path, files), v, asof)
}

# MapPLUTO shapefile releases 2018-2025 (clipped MapPLUTO.dbf).
for (v in c("18v1_1", "19v1", "20v1", "21v1", "22v1", "23v1", "24v1", "25v1", "25v4")) {
  zip_path <- sprintf("../input/nyc_mappluto_%s_arc_shp.zip", v)
  written <- unzip(zip_path, list = TRUE)
  asof <- format(written$Date[written$Name == "MapPLUTO.dbf"], "%Y-%m-01")
  panel[[v]] <- standardize(read_mappluto_dbf(zip_path, "MapPLUTO.dbf"), sub("_", ".", v), asof)
}

panel <- rbindlist(panel, use.names = TRUE)
setcolorder(panel, c("vintage", "zoning_asof", "bbl"))
stopifnot(uniqueN(panel$vintage) == 24, !anyDuplicated(panel[, .(vintage, bbl)]))
write_parquet(panel, "../temp/lot_zoning_panel.parquet")

print(panel[, .(lots = .N, zoning_asof = first(zoning_asof), lotarea_bn = sum(lotarea, na.rm = TRUE) / 1e9,
  missing_xy = mean(is.na(xcoord)), residfar_present = mean(!is.na(residfar))), by = vintage])
