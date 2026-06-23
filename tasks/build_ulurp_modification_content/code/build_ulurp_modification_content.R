# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_ulurp_modification_content/code")
# content_mode <- "first_pass"

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

cli_args <- commandArgs(trailingOnly = TRUE)
if (length(cli_args) != 1) {
  stop("Usage: Rscript build_ulurp_modification_content.R <content_mode>")
}

content_mode <- as.character(cli_args[1])

if (!content_mode %in% c("first_pass")) {
  stop("Unsupported content_mode: ", content_mode)
}

collapse_values <- function(x) {
  values <- unique(str_squish(as.character(x)))
  values <- values[!is.na(values) & values != ""]
  if (length(values) == 0) {
    return(NA_character_)
  }

  paste(values, collapse = "; ")
}

to_logical_flag <- function(x) {
  str_to_upper(str_squish(as.character(x))) %in% c("TRUE", "T", "1", "YES")
}

assert_unique_keys <- function(df, key_cols, df_name) {
  duplicate_keys <- df |>
    count(across(all_of(key_cols)), name = "source_row_count") |>
    filter(source_row_count > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(df_name, " is not unique by ", paste(key_cols, collapse = ", "), ".")
  }
}

quantity_patterns <- tribble(
  ~quantity_field, ~quantity_label, ~quantity_pattern,
  "units", "Residential units", "\\b\\d[\\d,]*\\s+(?:dwelling\\s+|residential\\s+)?units?\\b",
  "affordable_units", "Affordable units", "\\b\\d[\\d,]*\\s+affordable\\s+(?:dwelling\\s+|residential\\s+)?units?\\b",
  "stories", "Building stories", "\\b\\d{1,3}\\s*-?\\s*stor(?:y|ies)\\b",
  "height_ft", "Height in feet", "\\b\\d{2,4}\\s*(?:feet|ft\\.?)(?:\\b|\\s)",
  "parking_spaces", "Parking spaces", "\\b\\d[\\d,]*\\s+(?:accessory\\s+)?(?:parking\\s+)?spaces?\\b",
  "zoning_floor_area_sf", "Zoning floor area square feet", "\\b\\d[\\d,]*\\s*(?:square\\s+feet|sq\\.?\\s*ft\\.?)\\b"
)

extract_quantity_rows <- function(df, text_col, source_doc_col, page_col, stage, extraction_method, confidence) {
  out <- list()
  out_i <- 1

  if (nrow(df) == 0) {
    return(tibble())
  }

  for (i in seq_len(nrow(df))) {
    text_value <- str_squish(as.character(df[[text_col]][i]))
    if (is.na(text_value) || text_value == "") {
      next
    }

    for (j in seq_len(nrow(quantity_patterns))) {
      match_location <- str_locate(text_value, regex(quantity_patterns$quantity_pattern[j], ignore_case = TRUE))[1, ]
      if (is.na(match_location[1])) {
        next
      }

      match_text <- str_sub(text_value, match_location[1], match_location[2])
      raw_number <- str_extract(match_text, "\\d[\\d,]*")
      quantity_value <- suppressWarnings(as.numeric(str_replace_all(raw_number, ",", "")))
      snippet <- str_squish(str_sub(
        text_value,
        max(1, match_location[1] - 220),
        min(str_length(text_value), match_location[2] + 220)
      ))

      out[[out_i]] <- tibble(
        project_id = as.character(df$project_id[i]),
        project_name = as.character(df$project_name[i]),
        stage = stage,
        quantity_field = quantity_patterns$quantity_field[j],
        quantity_label = quantity_patterns$quantity_label[j],
        quantity_value = quantity_value,
        quantity_missing_status = if_else(is.na(quantity_value), "NA_not_stated", "observed"),
        source_doc = as.character(df[[source_doc_col]][i]),
        page = as.character(df[[page_col]][i]),
        snippet = snippet,
        extraction_method = extraction_method,
        confidence = confidence
      )
      out_i <- out_i + 1
    }
  }

  bind_rows(out)
}

modification_category <- function(keyword_family) {
  case_when(
    keyword_family == "unit_quantity" ~ "Q-UN_units",
    keyword_family == "affordability" ~ "A-AF_affordability",
    keyword_family == "parking" ~ "P-PK_parking",
    keyword_family %in% c("height_or_bulk", "design") ~ "D-BK_design_or_bulk",
    keyword_family == "cost_mitigation" ~ "C-MT_cost_or_infrastructure_mitigation",
    keyword_family %in% c("local_benefit_commitment", "points_of_agreement") ~ "B-LB_local_benefit_commitment",
    keyword_family == "modification_signal" ~ "O-MD_unspecified_modification_signal",
    TRUE ~ "O-UN_uncategorized"
  )
}

spine <- read_csv("../input/ulurp_modification_project_spine.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    project_id = as.character(project_id),
    cert_year = suppressWarnings(as.integer(cert_year)),
    council_modification_signal = to_logical_flag(council_modification_signal),
    linked_gross_add_units_0_10 = suppressWarnings(as.numeric(linked_gross_add_units_0_10)),
    linked_net_units_0_10 = suppressWarnings(as.numeric(linked_net_units_0_10))
  )

zap_links <- read_csv("../input/ulurp_modification_zap_document_links.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    project_id = as.character(project_id),
    source_priority = suppressWarnings(as.integer(source_priority)),
    a_application_flag = to_logical_flag(a_application_flag),
    m_report_candidate_flag = to_logical_flag(m_report_candidate_flag)
  )

zap_docket <- read_csv("../input/ulurp_modification_zap_docket_text.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(project_id = as.character(project_id))

council_links <- read_csv("../input/ulurp_modification_council_document_links.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    document_id = as.character(document_id),
    project_ids = as.character(project_ids),
    matter_id = as.character(matter_id),
    matter_file = as.character(matter_file),
    source_priority = suppressWarnings(as.integer(source_priority)),
    points_of_agreement_candidate = to_logical_flag(points_of_agreement_candidate),
    committee_report_candidate = to_logical_flag(committee_report_candidate),
    m_matter_candidate = to_logical_flag(m_matter_candidate)
  )

council_snippets <- read_csv("../input/ulurp_modification_council_document_snippets.csv", show_col_types = FALSE, na = c("", "NA")) |>
  mutate(
    snippet_id = as.character(snippet_id),
    document_id = as.character(document_id),
    project_ids = as.character(project_ids),
    matter_id = as.character(matter_id),
    matter_file = as.character(matter_file)
  )

assert_unique_keys(spine, "project_id", "Modification spine")
assert_unique_keys(zap_links, c("project_id", "source_type", "document_url", "document_title"), "ZAP document links")
assert_unique_keys(council_links, "document_id", "Council document links")
assert_unique_keys(council_snippets, "snippet_id", "Council document snippets")

project_brief_text <- spine |>
  transmute(
    project_id,
    project_name,
    source_doc = "zap_ulurp_project_base.csv:project_brief",
    page = "NA_not_stated",
    source_text = project_brief
  )

docket_text <- zap_docket |>
  transmute(
    project_id,
    project_name,
    source_doc = coalesce(project_page_url, api_url, "NA_doc_missing"),
    page = "NA_not_stated",
    source_text = docket_description
  )

council_snippet_project <- council_snippets |>
  separate_rows(project_ids, sep = ";\\s*") |>
  rename(project_id = project_ids) |>
  filter(project_id != "") |>
  left_join(
    spine |>
      select(
        project_id,
        project_name,
        council_modification_signal,
        council_modified_action_count,
        council_outcome,
        stratum,
        local_member_names,
        local_member_vote_statuses,
        member_deference_vote_signals
      ),
    by = "project_id",
    relationship = "many-to-one"
  ) |>
  filter(!is.na(project_name))

project_brief_quantities <- extract_quantity_rows(
  project_brief_text,
  "source_text",
  "source_doc",
  "page",
  "certified_project_brief_first_pass",
  "regex_project_brief",
  "low"
)

docket_quantities <- extract_quantity_rows(
  docket_text,
  "source_text",
  "source_doc",
  "page",
  "cpc_docket_description_first_pass",
  "regex_zap_docket_description",
  "low"
)

council_quantity_source <- council_snippet_project |>
  filter(keyword_family %in% c("unit_quantity", "affordability", "height_or_bulk", "parking")) |>
  transmute(
    project_id,
    project_name,
    source_doc = coalesce(source_doc, "NA_doc_missing"),
    page = coalesce(page, "NA_not_stated"),
    source_text = snippet
  )

council_quantities <- extract_quantity_rows(
  council_quantity_source,
  "source_text",
  "source_doc",
  "page",
  "council_adopted_first_pass",
  "regex_council_snippet",
  "low"
)

built_quantities <- spine |>
  transmute(
    project_id,
    project_name,
    stage = "built_0_10",
    quantity_field = "units_built_0_10",
    quantity_label = "HDB-linked gross added units within 10 years",
    quantity_value = linked_gross_add_units_0_10,
    quantity_missing_status = case_when(
      is.na(linked_gross_add_units_0_10) ~ "NA_doc_missing",
      linked_gross_add_units_0_10 == 0 ~ "true_zero",
      TRUE ~ "observed"
    ),
    source_doc = "zap_ulurp_project_base.csv:hdb_buildout_link",
    page = "NA_not_stated",
    snippet = case_when(
      is.na(linked_gross_add_units_0_10) ~ "NA_doc_missing",
      TRUE ~ paste0("linked_gross_add_units_0_10=", linked_gross_add_units_0_10)
    ),
    extraction_method = "existing_hdb_buildout_link",
    confidence = if_else(quantity_missing_status %in% c("observed", "true_zero"), "medium", "low")
  )

project_versions <- bind_rows(
  project_brief_quantities,
  docket_quantities,
  council_quantities,
  built_quantities
) |>
  mutate(
    source_doc = coalesce(source_doc, "NA_doc_missing"),
    page = coalesce(page, "NA_not_stated"),
    snippet = coalesce(snippet, "NA_doc_missing"),
    confidence = coalesce(confidence, "low")
  ) |>
  arrange(project_id, stage, quantity_field, source_doc, snippet) |>
  group_by(project_id, stage, quantity_field) |>
  mutate(project_version_id = sprintf("%s_%s_%s_%03d", project_id, stage, quantity_field, row_number())) |>
  ungroup() |>
  select(
    project_version_id,
    project_id,
    project_name,
    stage,
    quantity_field,
    quantity_label,
    quantity_value,
    quantity_missing_status,
    source_doc,
    page,
    snippet,
    extraction_method,
    confidence
  )

council_modification_evidence <- council_snippet_project |>
  filter(
    council_modification_signal |
      keyword_family %in% c("modification_signal", "points_of_agreement")
  ) |>
  mutate(
    modification_category_code = modification_category(keyword_family),
    source_doc = coalesce(source_doc, "NA_doc_missing"),
    page = coalesce(page, "NA_not_stated"),
    snippet = coalesce(snippet, "NA_doc_missing"),
    extraction_method = coalesce(extraction_method, "legistar_action_detail_text"),
    confidence = if_else(document_family %in% c("council_action_detail_modification_signal", "points_of_agreement_candidate"), "medium", "low"),
    modification_stage = "council_stage",
    local_member_attribution = "not_attributed",
    source_gap_flag = FALSE,
    source_gap_reason = NA_character_
  ) |>
  transmute(
    project_id,
    project_name,
    modification_stage,
    modification_category_code,
    keyword_family,
    document_family,
    matter_id,
    matter_file,
    council_outcome,
    stratum,
    council_modification_signal,
    council_modified_action_count,
    local_member_names,
    local_member_vote_statuses,
    member_deference_vote_signals,
    local_member_attribution,
    source_doc,
    page,
    snippet,
    extraction_method,
    confidence,
    source_gap_flag,
    source_gap_reason
  ) |>
  distinct()

source_gap_projects <- spine |>
  filter(council_modification_signal) |>
  anti_join(
    council_modification_evidence |>
      distinct(project_id),
    by = "project_id"
  ) |>
  transmute(
    project_id,
    project_name,
    modification_stage = "council_stage",
    modification_category_code = "source_gap_no_council_stage_modification_text",
    keyword_family = "source_gap",
    document_family = "NA_doc_missing",
    matter_id = as.character(council_matter_ids),
    matter_file = as.character(council_matter_files),
    council_outcome,
    stratum,
    council_modification_signal,
    council_modified_action_count,
    local_member_names,
    local_member_vote_statuses,
    member_deference_vote_signals,
    local_member_attribution = "not_attributed",
    source_doc = "NA_doc_missing",
    page = "NA_doc_missing",
    snippet = "NA_doc_missing",
    extraction_method = "source_gap_queue",
    confidence = "low",
    source_gap_flag = TRUE,
    source_gap_reason = "Council modification signal is present, but no Council-stage modification snippet was extracted."
  )

discrete_modifications <- bind_rows(council_modification_evidence, source_gap_projects) |>
  arrange(project_id, modification_stage, modification_category_code, matter_id, source_doc, snippet) |>
  group_by(project_id) |>
  mutate(modification_id = sprintf("%s_MOD_%03d", project_id, row_number())) |>
  ungroup() |>
  select(modification_id, everything())

council_commitment_snippets <- council_snippet_project |>
  filter(keyword_family %in% c("cost_mitigation", "local_benefit_commitment", "points_of_agreement")) |>
  transmute(
    project_id,
    project_name,
    commitment_stage = "council_stage",
    commitment_category = modification_category(keyword_family),
    keyword_family,
    document_family,
    matter_id,
    matter_file,
    source_doc = coalesce(source_doc, "NA_doc_missing"),
    page = coalesce(page, "NA_not_stated"),
    snippet = coalesce(snippet, "NA_doc_missing"),
    extraction_method = coalesce(extraction_method, "legistar_action_detail_text"),
    confidence = if_else(keyword_family == "points_of_agreement", "medium", "low")
  ) |>
  distinct()

council_commitment_links <- council_links |>
  filter(points_of_agreement_candidate) |>
  separate_rows(project_ids, sep = ";\\s*") |>
  rename(project_id = project_ids) |>
  filter(project_id != "") |>
  left_join(
    spine |>
      select(project_id, project_name),
    by = "project_id",
    relationship = "many-to-one"
  ) |>
  filter(!is.na(project_name)) |>
  transmute(
    project_id,
    project_name,
    commitment_stage = "council_stage",
    commitment_category = "B-LB_local_benefit_commitment",
    keyword_family = "points_of_agreement",
    document_family,
    matter_id,
    matter_file,
    source_doc = source_url,
    page = "NA_not_stated",
    snippet = coalesce(source_label, matter_title, "NA_not_stated"),
    extraction_method = "legistar_document_link_candidate",
    confidence = "low"
  ) |>
  distinct()

zap_commitment_links <- zap_links |>
  filter(document_family %in% c("points_of_agreement", "restrictive_declaration")) |>
  transmute(
    project_id,
    project_name,
    commitment_stage = "cpc_or_applicant_stage",
    commitment_category = "B-LB_local_benefit_commitment",
    keyword_family = document_family,
    document_family,
    matter_id = NA_character_,
    matter_file = NA_character_,
    source_doc = document_url,
    page = "NA_not_stated",
    snippet = coalesce(document_title, source_container_title, "NA_not_stated"),
    extraction_method = "zap_document_link_candidate",
    confidence = "low"
  ) |>
  distinct()

commitments <- bind_rows(council_commitment_snippets, council_commitment_links, zap_commitment_links) |>
  arrange(project_id, commitment_stage, commitment_category, source_doc, snippet) |>
  group_by(project_id) |>
  mutate(commitment_id = sprintf("%s_COMMIT_%03d", project_id, row_number())) |>
  ungroup() |>
  select(commitment_id, everything())

citywide_text_district_modifications <- spine |>
  filter(stratum == "D") |>
  mutate(
    citywide_districts = coalesce(council_affected_districts, as.character(council_district_first), "NA_not_stated")
  ) |>
  separate_rows(citywide_districts, sep = ";\\s*") |>
  mutate(
    affected_council_district = str_extract(citywide_districts, "\\d{1,2}"),
    affected_council_district = coalesce(affected_council_district, "NA_not_stated"),
    district_missing_status = if_else(affected_council_district == "NA_not_stated", "NA_not_stated", "observed"),
    modification_stage = "citywide_text_or_text_amendment",
    modification_category_code = "TXT-DIST_citywide_text_district_link",
    source_doc = coalesce(council_matter_urls, "NA_doc_missing"),
    page = "NA_not_stated",
    snippet = coalesce(council_titles, project_brief, "NA_doc_missing"),
    extraction_method = "spine_citywide_text_stratum",
    confidence = "low"
  ) |>
  arrange(project_id, affected_council_district) |>
  group_by(project_id) |>
  mutate(citywide_text_modification_id = sprintf("%s_TXT_%03d", project_id, row_number())) |>
  ungroup() |>
  select(
    citywide_text_modification_id,
    project_id,
    project_name,
    cert_year,
    stratum,
    affected_council_district,
    district_missing_status,
    modification_stage,
    modification_category_code,
    source_doc,
    page,
    snippet,
    extraction_method,
    confidence
  )

known_case_patterns <- tribble(
  ~validation_case, ~pattern,
  "Dock Street", "DOCK STREET",
  "NY Blood Center", "BLOOD CENTER",
  "Industry City", "INDUSTRY CITY",
  "80 Flatbush", "80 FLATBUSH",
  "Innovation QNS", "INNOVATION QNS|INNOVATION QUEENS",
  "One45", "ONE45|ONE 45",
  "Bruckner Boulevard", "BRUCKNER",
  "East New York", "EAST NEW YORK",
  "Inwood", "INWOOD",
  "Haven Green", "HAVEN GREEN",
  "Greenpoint-Williamsburg", "GREENPOINT|WILLIAMSBURG",
  "Hudson Yards", "HUDSON YARDS"
)

known_case_source <- spine |>
  mutate(search_text = str_to_upper(str_squish(paste(project_name, project_brief, council_titles)))) |>
  select(project_id, project_name, search_text)

known_case_queue <- tidyr::expand_grid(known_case_source, known_case_patterns) |>
  filter(str_detect(search_text, regex(pattern, ignore_case = TRUE))) |>
  transmute(
    project_id,
    project_name,
    queue_reason = paste0("validation_case_", validation_case),
    source_gap_flag = FALSE,
    confidence = "manual_review_required",
    source_doc = "ulurp_modification_project_spine.csv",
    page = "NA_not_stated",
    snippet = str_squish(str_sub(search_text, 1, 500))
  )

source_gap_queue <- source_gap_projects |>
  transmute(
    project_id,
    project_name,
    queue_reason = "approve_with_mods_without_extracted_council_stage_modification",
    source_gap_flag,
    confidence = "low",
    source_doc,
    page,
    snippet
  )

low_confidence_queue <- discrete_modifications |>
  filter(confidence == "low") |>
  transmute(
    project_id,
    project_name,
    queue_reason = paste0("low_confidence_", modification_category_code),
    source_gap_flag,
    confidence,
    source_doc,
    page,
    snippet
  )

manual_review_queue <- bind_rows(known_case_queue, source_gap_queue, low_confidence_queue) |>
  distinct() |>
  arrange(project_id, queue_reason, source_doc, snippet) |>
  group_by(project_id) |>
  mutate(manual_review_id = sprintf("%s_REVIEW_%03d", project_id, row_number())) |>
  ungroup() |>
  select(manual_review_id, everything())

assert_unique_keys(project_versions, "project_version_id", "Project-version quantities")
assert_unique_keys(discrete_modifications, "modification_id", "Discrete modifications")
assert_unique_keys(commitments, "commitment_id", "Side commitments")
assert_unique_keys(citywide_text_district_modifications, "citywide_text_modification_id", "Citywide text district modifications")
assert_unique_keys(manual_review_queue, "manual_review_id", "Manual review queue")

missing_quantity_metadata <- project_versions |>
  filter(quantity_missing_status == "observed") |>
  filter(
    is.na(source_doc) | source_doc == "" |
      is.na(page) | page == "" |
      is.na(snippet) | snippet == "" |
      is.na(extraction_method) | extraction_method == "" |
      is.na(confidence) | confidence == ""
  )

approve_with_mods_without_resolution <- spine |>
  filter(council_modification_signal) |>
  anti_join(
    discrete_modifications |>
      filter(modification_stage == "council_stage") |>
      distinct(project_id),
    by = "project_id"
  )

qc_rows <- tribble(
  ~check_name, ~check_value, ~status,
  "spine_project_rows", nrow(spine), if_else(nrow(spine) > 0, "pass", "fail"),
  "project_version_rows", nrow(project_versions), if_else(nrow(project_versions) > 0, "pass", "fail"),
  "discrete_modification_rows", nrow(discrete_modifications), if_else(nrow(discrete_modifications) > 0, "pass", "fail"),
  "commitment_rows", nrow(commitments), "pass",
  "citywide_text_district_rows", nrow(citywide_text_district_modifications), "pass",
  "manual_review_rows", nrow(manual_review_queue), "pass",
  "observed_quantity_rows_missing_metadata", nrow(missing_quantity_metadata), if_else(nrow(missing_quantity_metadata) == 0, "pass", "fail"),
  "approve_with_mods_without_modification_or_source_gap", nrow(approve_with_mods_without_resolution), if_else(nrow(approve_with_mods_without_resolution) == 0, "pass", "fail")
)

write_csv_if_changed(project_versions, "../output/ulurp_modification_project_versions.csv")
write_csv_if_changed(discrete_modifications, "../output/ulurp_modification_discrete_modifications.csv")
write_csv_if_changed(commitments, "../output/ulurp_modification_commitments.csv")
write_csv_if_changed(citywide_text_district_modifications, "../output/ulurp_modification_citywide_text_district_modifications.csv")
write_csv_if_changed(manual_review_queue, "../output/ulurp_modification_manual_review_queue.csv")

if (any(qc_rows$status == "fail")) {
  stop("ULURP modification content QC failed.")
}
