# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/summarize_rezoning_scope_by_homeownership/code")

# One row per adopted zoning map amendment (ULURP number) effective between the
# 02b and 25v4 zoning dates: measured residential capacity change, direction,
# sponsor, Council action and CPC-report evidence of a local request.

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(stringr)
})

source("../../../_lib/data_reports.R")

changes <- as.data.table(read_parquet("../temp/lot_capacity_changes.parquet"))
footprints <- fread("../temp/zma_footprints.csv", colClasses = list(character = "ulurp"))
footprints <- footprints[effective >= as.Date("2002-07-01") & effective < as.Date("2026-02-01")]

# Footprint totals; the primary community district holds most footprint lot area.
rezonings <- footprints[order(ulurp, -footprint_lotarea), .(effective = effective[1], baseline_release = baseline_release[1],
  primary_cd = cd[1], footprint_cds = paste(sort(unique(na.omit(cd))), collapse = ";"),
  footprint_lots = sum(footprint_lots), footprint_lotarea = sum(footprint_lotarea),
  footprint_capacity = sum(footprint_capacity)), by = ulurp]

# 1. Capacity change ----------------------------------------------------------
# Areas are the part of each changed lot's polygon inside the footprint
# (area_inside, sq ft); capacity change is the FAR change times that area.
# Lots without a polygon are counted, with their full-lot change, but add no
# area or capacity.
# Lot classes follow Furman Center (2010): upzoned if capacity rises to at least
# 110% of the pre-rezoning capacity, downzoned if it falls below 90%,
# contextual-only otherwise. Lots with no residential FAR before or after
# (manufacturing to manufacturing) are in no class.
attributed <- changes[!is.na(ulurp)]
attributed[, lot_class := fcase(far_pre == 0 & far_post == 0, "nonresidential",
  far_post >= 1.1 * far_pre, "up", far_post < 0.9 * far_pre, "down", default = "contextual")]
capacity <- attributed[, .(changed_lots = uniqueN(bbl),
  lots_without_polygon = uniqueN(bbl[is.na(area_inside)]),
  full_lot_change_without_polygon = sum(cap_change[is.na(area_inside)]),
  lots_up = uniqueN(bbl[lot_class == "up"]), lots_down = uniqueN(bbl[lot_class == "down"]),
  lots_contextual = uniqueN(bbl[lot_class == "contextual"]),
  area_rezoned = sum(area_inside, na.rm = TRUE),
  area_upzoned = sum(area_inside[lot_class == "up"], na.rm = TRUE),
  area_downzoned = sum(area_inside[lot_class == "down"], na.rm = TRUE),
  area_contextual = sum(area_inside[lot_class == "contextual"], na.rm = TRUE),
  rezoned_lot_capacity_before = sum(far_pre * area_inside, na.rm = TRUE),
  capacity_added = sum(pmax(cap_change_inside, 0), na.rm = TRUE),
  capacity_removed = sum(pmax(-cap_change_inside, 0), na.rm = TRUE),
  capacity_net = sum(cap_change_inside, na.rm = TRUE)), by = ulurp]
rezonings <- merge(rezonings, capacity, by = "ulurp", all.x = TRUE)
count_cols <- setdiff(names(capacity), "ulurp")
rezonings[, (count_cols) := lapply(.SD, function(x) fcoalesce(as.numeric(x), 0)), .SDcols = count_cols]

# Direction, from net change relative to the rezoned lots' capacity before.
rezonings[, `:=`(
  net_pct_of_rezoned = fifelse(rezoned_lot_capacity_before > 0, 100 * capacity_net / rezoned_lot_capacity_before, NA_real_),
  net_pct_of_footprint = fifelse(footprint_capacity > 0, 100 * capacity_net / footprint_capacity, NA_real_),
  added_pct_of_rezoned = fifelse(rezoned_lot_capacity_before > 0, 100 * capacity_added / rezoned_lot_capacity_before, NA_real_),
  removed_pct_of_rezoned = fifelse(rezoned_lot_capacity_before > 0, 100 * capacity_removed / rezoned_lot_capacity_before, NA_real_))]
rezonings[, direction := fcase(
  area_rezoned == 0, "no_district_change_measured",
  capacity_net > 0 & capacity_net >= 0.1 * rezoned_lot_capacity_before, "upzoning",
  capacity_net < 0 & -capacity_net >= 0.1 * rezoned_lot_capacity_before, "downzoning",
  capacity_added >= 0.1 * rezoned_lot_capacity_before & capacity_removed >= 0.1 * rezoned_lot_capacity_before, "mixed",
  default = "contextual_neutral")]

# 2. Sponsor (ZAP) ------------------------------------------------------------
zap <- fread("../input/zap_project_universe.csv", colClasses = "character",
  select = c("project_id", "project_name", "applicant_type", "primary_applicant", "ulurp_numbers"))
zap <- zap[, .(ulurp = str_remove(str_remove_all(toupper(unlist(strsplit(ulurp_numbers, ";"))), "[^A-Z0-9]"), "^[CNM]")),
  by = .(project_id, project_name, applicant_type, primary_applicant)]
zap <- zap[str_detect(ulurp, "ZM")]
stopifnot(!anyDuplicated(zap$ulurp))
rezonings <- merge(rezonings, zap, by = "ulurp", all.x = TRUE)

# 3. Council action (Legistar decision panel) ---------------------------------
# All Legistar matters listing the ULURP number. As in the decision trends,
# one local no vote on any matter makes the local member position "opposes".
council <- fread("../input/council_land_use_decision_panel.csv", colClasses = "character",
  select = c("matter_file", "application_keys", "decision_action", "project_outcome",
    "negative_count", "local_member_project_position", "affected_council_districts"))
council <- council[, .(ulurp = str_squish(unlist(strsplit(application_keys, ";")))),
  by = .(matter_file, decision_action, project_outcome, negative_count,
    local_member_project_position, affected_council_districts)]
council <- council[ulurp %chin% rezonings$ulurp, .(
  council_matters = paste(sort(unique(matter_file)), collapse = ";"),
  council_outcome = fcase(any(project_outcome == "disapproved"), "disapproved",
    any(project_outcome == "approved"), "approved", default = "no_final_action"),
  council_modified = any(str_detect(decision_action, "Approved with Modifications")),
  council_no_votes = suppressWarnings(max(as.numeric(negative_count), na.rm = TRUE)),
  local_member_position = fcase(any(local_member_project_position == "opposes"), "opposes",
    any(local_member_project_position == "supports"), "supports", default = NA_character_),
  council_districts = paste(sort(unique(unlist(strsplit(affected_council_districts[affected_council_districts != ""], ";")))), collapse = ";")),
  by = ulurp]
council[!is.finite(council_no_votes), council_no_votes := NA]
rezonings <- merge(rezonings, council, by = "ulurp", all.x = TRUE)
rezonings[, council_override := council_outcome == "approved" & local_member_position %chin% "opposes"]

# 4. CPC report: was the rezoning requested by local actors? -------------------
# A sentence counts when it states that the rezoning or study was undertaken
# at the request of, or in response to requests or concerns of, local actors
# (Council Members, elected officials, community boards, civic groups,
# residents). Sentences about modifications, applicants, developers or other
# ULURP applications are excluded. code/cpc_request_hand_check.csv records a
# reading of every hit and of 30 random DCP-sponsored non-hits.
ask <- "(at the (request|urging) of|in response to (the |a )?([a-z-]+ ){0,2}(requests?|concerns?|calls?|letter)|respond(s)? to (the |a )?([a-z-]+ ){0,2}(requests?|concerns?)|(spurring|prompted|prompting) a request|(based on|following) (the )?requests|advocacy (by|of|from)|study (that )?(was )?requested by|\\(as requested by)"
initiate <- "(undert\\w+|initiat\\w+|develop\\w*|propos\\w+|craft\\w+|conduct\\w+|implement\\w+|select\\w+|rezonings?|zoning (proposal|changes|map amendments?|study)|study|proposals?)"
dept <- "(the Department|DCP|City Planning|the City)"
request <- regex(paste0(initiate, "\\b[^.]{0,150}", ask, "|", ask, "[^.]{0,250}\\b", dept, "[^.]{0,80}\\b", initiate, "|",
  "rezoning( proposal| study)? (is|was) (proposed )?", ask, "|",
  "(requested that|asked|approached) ", dept, "( of City Planning)?( to)? (study|undertake|conduct|request)"), ignore_case = TRUE)
local_actor <- regex("council ?(member|woman|man)s?|elected (officials?|representatives?)|community boards?|civic|homeowners?|associations?|residents|taxpayers|community (groups|organizations|members)|neighborhood (groups|organizations)", ignore_case = TRUE)
member <- regex("council ?(member|woman|man)s?|elected (officials?|representatives?)", ignore_case = TRUE)
excluded <- regex("(previous|prior) (rezoning|application|land use)|^in (19|20)[0-9]{2}\\b|applicant|\\brepresentative\\b|developer|development team|(is|was|were) modified|modified at the request", ignore_case = TRUE)

reports <- fread("../input/ulurp_cpc_report_manifest.csv", colClasses = "character",
  select = c("application_key", "text_status", "local_text_path"))[text_status == "text_extracted" & application_key %chin% rezonings$ulurp]
cpc <- reports[, {
  # All reports for the application; text paths are relative to the corpus task's code folder.
  text <- str_squish(paste(unlist(lapply(file.path("../../../build_ulurp_cpc_report_corpus/code", local_text_path),
    readLines, warn = FALSE)), collapse = " "))
  sentences <- unlist(str_split(text, "(?<=[a-z0-9\\)]\\.)\\s+(?=[A-Z])"))
  cites_other <- vapply(str_extract_all(sentences, "\\b[0-9]{6}\\b"), function(x) any(x != substr(application_key, 1, 6)), logical(1))
  hit <- str_detect(sentences, request) & str_detect(sentences, local_actor) & !str_detect(sentences, excluded) & !cites_other
  .(cpc_local_request = any(hit), cpc_request_names_member = any(str_detect(sentences[hit], member)),
    cpc_request_sentence = if (any(hit)) str_sub(sentences[hit][1], 1, 400) else NA_character_)
}, by = .(ulurp = application_key)]
rezonings <- merge(rezonings, cpc, by = "ulurp", all.x = TRUE)
rezonings[, cpc_report_text := !is.na(cpc_local_request)]

hand_check <- fread("cpc_request_hand_check.csv", colClasses = "character")
stopifnot(setequal(hand_check[regex_hit == "TRUE"]$ulurp, rezonings[cpc_local_request == TRUE]$ulurp),
  all(hand_check[regex_hit == "FALSE"]$ulurp %chin% rezonings[cpc_local_request == FALSE]$ulurp))
cat("CPC request regex: precision", hand_check[regex_hit == "TRUE", mean(hand_label == "requested")],
  "; share of checked DCP non-hits that were requests", hand_check[regex_hit == "FALSE", mean(hand_label == "requested")], "\n")

setcolorder(rezonings, c("ulurp", "effective", "project_id", "project_name", "applicant_type", "primary_cd", "footprint_cds", "direction"))
save_csv(as.data.frame(rezonings[order(effective, ulurp)]), "../output/rezonings.csv", key = "ulurp")
print(rezonings[, .N, by = .(direction, applicant_type)][order(direction, applicant_type)])
