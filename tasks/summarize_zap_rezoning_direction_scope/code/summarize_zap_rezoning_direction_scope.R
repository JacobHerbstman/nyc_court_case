# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_zap_rezoning_direction_scope/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

# Small output and model helpers.

write_lines_if_changed <- function(lines, out_path) {
  temp_path <- tempfile(fileext = ".tex")
  writeLines(lines, temp_path, useBytes = TRUE)
  copy_if_changed(temp_path, out_path)
}

z_score <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  x_sd <- sd(x, na.rm = TRUE)

  if (is.na(x_sd) || x_sd == 0) {
    return(rep(0, length(x)))
  }

  (x - mean(x, na.rm = TRUE)) / x_sd
}

format_decimal <- function(x, digits = 3) {
  if_else(is.na(x), "", formatC(x, format = "f", digits = digits))
}

format_p_value <- function(x) {
  case_when(
    is.na(x) ~ "",
    x < 0.001 ~ "$<0.001$",
    TRUE ~ formatC(x, format = "f", digits = 3)
  )
}

significance_stars <- function(x) {
  case_when(
    is.na(x) ~ "",
    x < 0.01 ~ "***",
    x < 0.05 ~ "**",
    x < 0.1 ~ "*",
    TRUE ~ ""
  )
}

model_nobs <- function(model) {
  if (!is.null(model$nobs)) {
    return(as.integer(model$nobs))
  }

  length(model$residuals)
}

coeftable_df <- function(model) {
  coef_table <- as.data.frame(coeftable(model))
  coef_table$term <- rownames(coef_table)
  rownames(coef_table) <- NULL

  statistic_col <- if ("t value" %in% names(coef_table)) "t value" else "z value"
  p_value_col <- if ("Pr(>|t|)" %in% names(coef_table)) "Pr(>|t|)" else "Pr(>|z|)"

  coef_table |>
    transmute(
      term,
      estimate = Estimate,
      std_error = `Std. Error`,
      statistic = .data[[statistic_col]],
      p_value = .data[[p_value_col]]
    )
}

confint_df <- function(model) {
  out <- as.data.frame(confint(model))
  out$term <- rownames(out)
  rownames(out) <- NULL
  names(out)[1:2] <- c("conf_low", "conf_high")
  out
}

extract_model_terms <- function(model, requested_terms_df) {
  requested_terms_df |>
    left_join(coeftable_df(model), by = "term", relationship = "many-to-one") |>
    left_join(confint_df(model), by = "term", relationship = "many-to-one")
}

regression_table_row <- function(row_label, values) {
  paste0("    ", row_label, " & ", paste(values, collapse = " & "), " \\\\")
}

sanitize_period <- function(x) {
  str_replace_all(x, "-", "_")
}

event_periods <- c(
  "1980-1984",
  "1985-1989",
  "1990-1994",
  "1995-1999",
  "2000-2004",
  "2005-2009",
  "2010-2014",
  "2015-2019",
  "2020-2025"
)
reference_event_period <- "1985-1989"
estimated_event_periods <- event_periods[event_periods != reference_event_period]

# Text parser setup.

event_period_from_year <- function(year) {
  case_when(
    year >= 1980 & year <= 1984 ~ "1980-1984",
    year >= 1985 & year <= 1989 ~ "1985-1989",
    year >= 1990 & year <= 1994 ~ "1990-1994",
    year >= 1995 & year <= 1999 ~ "1995-1999",
    year >= 2000 & year <= 2004 ~ "2000-2004",
    year >= 2005 & year <= 2009 ~ "2005-2009",
    year >= 2010 & year <= 2014 ~ "2010-2014",
    year >= 2015 & year <= 2019 ~ "2015-2019",
    year >= 2020 & year <= 2025 ~ "2020-2025",
    TRUE ~ NA_character_
  )
}

clean_project_text <- function(x) {
  x |>
    str_replace_all("[*]", " ") |>
    str_replace_all("−|–|—", "-") |>
    str_replace_all("'", "") |>
    str_replace_all("\\bDISTRI\\s+CT\\b", "DISTRICT") |>
    str_replace_all("\\bREONE\\b", "REZONE") |>
    str_replace_all("\\bNONING\\b", "ZONING") |>
    str_replace_all("\\bFORM\\s+(?=AN?\\s+[RCM]-?\\s*[0-9]|[RCM]-?\\s*[0-9])", "FROM ") |>
    str_replace_all("\\bRO\\s+(?=[RCM6]-?\\s*[0-9])", "TO ") |>
    str_replace_all("\\bTO\\s*&\\s*(?=[RCM]-?\\s*[0-9])", "TO ") |>
    str_replace_all("\\b([RCM])\\s+([0-9]{1,2})\\b", "\\1\\2") |>
    str_replace_all("\\b([RCM][0-9]{1,2})-\\s+([0-9A-Z]+)\\b", "\\1-\\2") |>
    str_replace_all("\\bC8[0O]([1-4])\\b", "C8-\\1") |>
    str_replace_all("\\bC([0-9])([0-9])([A-Z])\\b", "C\\1-\\2\\3") |>
    str_replace_all(
      "\\b(TO|FROM|INTO|REZONE|CHANGE|EXTEND|EXISTING|PROPOSED|DISTRICT|ZONE|WITH)\\s+6([1-8])-([0-9A-Z]+)\\b",
      "\\1 C\\2-\\3"
    ) |>
    str_replace_all("\\bTO(?=[RCM]-?[0-9])", "TO ") |>
    str_replace_all("\\bFROM(?=[RCM]-?[0-9])", "FROM ") |>
    str_replace_all("\\bEXISTING(?=[RCM]-?[0-9])", "EXISTING ") |>
    str_replace_all("\\bCURRENT(?=[RCM]-?[0-9])", "CURRENT ") |>
    str_replace_all("\\bPROPOSED(?=[RCM]-?[0-9])", "PROPOSED ") |>
    str_replace_all("\\b([RCM]-?[0-9]{1,2}(?:-[0-9A-Z]+)?[A-Z]?)TO\\b", "\\1 TO") |>
    str_replace_all("([RCM]-?[0-9]{1,2}(?:-[0-9A-Z]+|[A-Z]+)?)\\(", "\\1/") |>
    str_replace_all("MX\\s*\\(", "MX ") |>
    str_replace_all("\\)", " ") |>
    str_to_upper() |>
    str_squish()
}

normalize_zoning_code <- function(x) {
  x |>
    str_to_upper() |>
    str_replace_all("−|–|—", "-") |>
    str_replace("^([RCM])-([0-9])", "\\1\\2") |>
    str_replace_all("[^A-Z0-9.-]", "") |>
    str_replace("[.]$", "")
}

zoning_code_regex <- "\\b(?:R-?[0-9]{1,2}(?:-[0-9A-Z]+|[A-Z]+)?|C-?[0-9](?:-[0-9A-Z]+|[A-Z]+)?|M-?[0-9](?:-[0-9A-Z]+|[A-Z]+)?)\\b"
zoning_side_pattern <- paste0(
  "(?:", zoning_code_regex, ")(?:\\s*(?:,|&|AND|/)\\s*(?:", zoning_code_regex, "))*"
)
zoning_to_prefix_pattern <- paste0(
  "(?:A\\s+|AN\\s+|THE\\s+|LIC\\s+|CONTEXTUAL\\s+(?:ZONE|ZONING|DISTRICT)\\s+|",
  "A\\s+SPECIAL\\s+MIXED[- ]USE\\s+DISTRICT\\s+|",
  "SPECIAL\\s+MIXED[- ]USE\\s+DISTRICT\\s+|MX\\s+)?"
)
zoning_to_connector_pattern <- paste0(
  "\\s+(?:TO|INTO)\\s+",
  zoning_to_prefix_pattern
)
zoning_pair_pattern <- paste0(
  "(", zoning_side_pattern, ")",
  zoning_to_connector_pattern,
  "(", zoning_side_pattern, ")"
)
zoning_existing_proposed_pattern <- paste0(
  "\\b(?:EXISTING|CURRENT|CURRENTLY ZONED|PRIOR ZONING|PRIOR)\\s+",
  "(", zoning_side_pattern, ")",
  ".{0,160}?\\b(?:PROPOSED|PROPOSING|ESTABLISH|ESTABLISHING|REZONE TO|REZONING TO|TO)\\s+",
  "(?:A\\s+|AN\\s+|THE\\s+|A\\s+SPECIAL\\s+MIXED[- ]USE\\s+DISTRICT\\s+|",
  "SPECIAL\\s+MIXED[- ]USE\\s+DISTRICT\\s+|MX\\s+)?",
  "(", zoning_side_pattern, ")"
)
zoning_proposed_existing_pattern <- paste0(
  "\\b(?:PROP|PROPOSED|PROPOSING|ESTABLISH|ESTABLISHING)\\s+",
  zoning_to_prefix_pattern,
  "(", zoning_side_pattern, ")",
  ".{0,160}?\\b(?:FROM|IN)\\s+(?:EXISTING|CURRENT|CURRENTLY ZONED|PRIOR ZONING|PRIOR)\\s+",
  "(", zoning_side_pattern, ")"
)
zoning_district_to_pattern <- paste0(
  "(", zoning_side_pattern, ")",
  "\\s+(?:DISTRICT|DISTRI CT|ZONING DISTRICT|AREA|ZONE|ZONING)?\\s+",
  "(?:TO|INTO)\\s+",
  zoning_to_prefix_pattern,
  "(", zoning_side_pattern, ")"
)
zoning_establish_within_existing_pattern <- paste0(
  "\\b(?:ESTABLISH|ESTABLISHING|CREATE|CREATING|CHANGE TO|CHANGING TO)\\s+",
  zoning_to_prefix_pattern,
  "(", zoning_side_pattern, ")",
  "\\s+(?:DISTRICT|DISTRI CT|ZONING DISTRICT|AREA|ZONE|ZONING)?\\s+",
  "(?:WITHIN|IN|ON)\\s+(?:THE\\s+)?(?:EXISTING|CURRENT|CURRENTLY ZONED|PRIOR ZONING|PRIOR)\\s+",
  "(", zoning_side_pattern, ")"
)
zoning_replace_with_pattern <- paste0(
  "\\b(?:REPLACE|REPLACING)\\s+",
  "(?:ALL\\s+OR\\s+PORTIONS\\s+OF\\s+|THE\\s+|EXISTING\\s+|CURRENT\\s+|PRIOR\\s+)*",
  "(", zoning_side_pattern, ")",
  ".{0,200}?\\b(?:WITH|BY)\\s+",
  zoning_to_prefix_pattern,
  "(", zoning_side_pattern, ")"
)
zoning_proposed_from_pattern <- paste0(
  "\\b(?:PROP|PROPOSED|PROPOSING)\\s+",
  zoning_to_prefix_pattern,
  "(", zoning_side_pattern, ")",
  "\\s+(?:ZO|ZONE|ZONING|DISTRICT|DIST)?\\s+",
  ".{0,80}?\\bFROM\\s+",
  "(?:AN?\\s+|THE\\s+|EXISTING\\s+|CURRENT\\s+|PRIOR\\s+)*",
  "(", zoning_side_pattern, ")"
)
zoning_extend_within_pattern <- paste0(
  "\\b(?:EXTEND|EXTENSION\\s+OF|EXTENSION)\\s+",
  "(?:AN?\\s+|THE\\s+|EXISTING\\s+|PRESENT\\s+)?",
  zoning_to_prefix_pattern,
  "(", zoning_side_pattern, ")",
  "\\s+(?:ZONE|ZONING|DISTRICT|DIST)?\\s+",
  ".{0,160}?\\b(?:WITHIN|OVER|INTO)\\s+",
  "(?:AN?\\s+|THE\\s+|EXISTING\\s+|CURRENT\\s+|PRIOR\\s+)*",
  "(", zoning_side_pattern, ")"
)

is_commercial_overlay_code <- function(x) {
  str_detect(x, "^C[12](?:-[1-5])?$")
}

is_c1_c2_code <- function(x) {
  str_detect(x, "^C[12](?:-[0-9A-Z]+|[A-Z]+)?$")
}

is_standalone_c1_c2_code <- function(x) {
  is_c1_c2_code(x) & !is_commercial_overlay_code(x)
}

extract_zoning_codes <- function(x) {
  if (is.na(x) || x == "") {
    return(character())
  }

  out <- normalize_zoning_code(unique(unlist(str_extract_all(x, zoning_code_regex))))
  sort(out[!is.na(out) & out != ""])
}

collapse_zoning_codes <- function(x) {
  paste(extract_zoning_codes(x), collapse = "; ")
}

collapse_zoning_codes_if <- function(x, predicate) {
  out <- extract_zoning_codes(x)
  paste(out[predicate(out)], collapse = "; ")
}

count_zoning_codes <- function(x) {
  length(extract_zoning_codes(x))
}

extract_zoning_side <- function(x) {
  raw_codes <- extract_zoning_codes(x)
  kept_codes <- raw_codes
  ignored_overlay_codes <- character()
  ignored_mixed_use_component_codes <- character()
  combined_mixed_use_side_flag <- str_detect(
    coalesce(x, ""),
    "\\bM-?[0-9](?:-[0-9A-Z]+|[A-Z]+)?\\s*/\\s*R-?[0-9]|\\bR-?[0-9](?:-[0-9A-Z]+|[A-Z]+)?\\s*/\\s*M-?[0-9]"
  )

  if (any(str_detect(raw_codes, "^R"))) {
    ignored_overlay_codes <- raw_codes[is_commercial_overlay_code(raw_codes)]
    ignored_mixed_use_component_codes <- if (combined_mixed_use_side_flag) raw_codes[str_detect(raw_codes, "^M")] else character()
    kept_codes <- raw_codes[!is_commercial_overlay_code(raw_codes)]
    if (combined_mixed_use_side_flag) {
      kept_codes <- kept_codes[!str_detect(kept_codes, "^M")]
    }
  }

  list(
    kept_codes = kept_codes,
    ignored_overlay_codes = ignored_overlay_codes,
    ignored_mixed_use_component_codes = ignored_mixed_use_component_codes
  )
}

build_zoning_pair_rows <- function(project_id, matches, from_column, to_column, parser_stage) {
  if (nrow(matches) == 0) {
    return(tibble(
      project_id = character(),
      match_index = integer(),
      from_zoning_code = character(),
      to_zoning_code = character(),
      parser_stage = character(),
      ignored_commercial_overlay_codes = character(),
      ignored_mixed_use_component_codes = character()
    ))
  }

  bind_rows(lapply(seq_len(nrow(matches)), function(i) {
    from_side <- extract_zoning_side(matches[i, from_column])
    to_side <- extract_zoning_side(matches[i, to_column])
    ignored_overlay_codes <- sort(unique(c(from_side$ignored_overlay_codes, to_side$ignored_overlay_codes)))
    ignored_mixed_use_component_codes <- sort(unique(c(
      from_side$ignored_mixed_use_component_codes,
      to_side$ignored_mixed_use_component_codes
    )))

    if (length(from_side$kept_codes) == 0 || length(to_side$kept_codes) == 0) {
      return(tibble(
        project_id = character(),
        match_index = integer(),
        from_zoning_code = character(),
        to_zoning_code = character(),
        parser_stage = character(),
        ignored_commercial_overlay_codes = character(),
        ignored_mixed_use_component_codes = character()
      ))
    }

    expand_grid(
      project_id = project_id,
      match_index = i,
      from_zoning_code = from_side$kept_codes,
      to_zoning_code = to_side$kept_codes
    ) |>
      mutate(
        parser_stage = parser_stage,
        ignored_commercial_overlay_codes = paste(ignored_overlay_codes, collapse = "; "),
        ignored_mixed_use_component_codes = paste(ignored_mixed_use_component_codes, collapse = "; ")
      )
  })) |>
    distinct(
      project_id,
      match_index,
      from_zoning_code,
      to_zoning_code,
      parser_stage,
      ignored_commercial_overlay_codes,
      ignored_mixed_use_component_codes,
      .keep_all = TRUE
    )
}

extract_zoning_pairs <- function(project_id, project_text) {
  bind_rows(
    build_zoning_pair_rows(
      project_id,
      str_match_all(project_text, zoning_pair_pattern)[[1]],
      from_column = 2,
      to_column = 3,
      parser_stage = "primary_transition"
    ),
    build_zoning_pair_rows(
      project_id,
      str_match_all(project_text, zoning_existing_proposed_pattern)[[1]],
      from_column = 2,
      to_column = 3,
      parser_stage = "existing_to_proposed_context"
    ),
    build_zoning_pair_rows(
      project_id,
      str_match_all(project_text, zoning_proposed_existing_pattern)[[1]],
      from_column = 3,
      to_column = 2,
      parser_stage = "proposed_from_existing_context"
    ),
    build_zoning_pair_rows(
      project_id,
      str_match_all(project_text, zoning_district_to_pattern)[[1]],
      from_column = 2,
      to_column = 3,
      parser_stage = "district_to_context"
    ),
    build_zoning_pair_rows(
      project_id,
      str_match_all(project_text, zoning_establish_within_existing_pattern)[[1]],
      from_column = 3,
      to_column = 2,
      parser_stage = "establish_within_existing_context"
    ),
    build_zoning_pair_rows(
      project_id,
      str_match_all(project_text, zoning_replace_with_pattern)[[1]],
      from_column = 2,
      to_column = 3,
      parser_stage = "replace_with_context"
    ),
    build_zoning_pair_rows(
      project_id,
      str_match_all(project_text, zoning_proposed_from_pattern)[[1]],
      from_column = 3,
      to_column = 2,
      parser_stage = "proposed_from_context"
    ),
    build_zoning_pair_rows(
      project_id,
      str_match_all(project_text, zoning_extend_within_pattern)[[1]],
      from_column = 3,
      to_column = 2,
      parser_stage = "extend_within_context"
    )
  ) |>
    distinct(
      project_id,
      from_zoning_code,
      to_zoning_code,
      parser_stage,
      ignored_commercial_overlay_codes,
      ignored_mixed_use_component_codes,
      .keep_all = TRUE
    )
}

# Residential FAR lookup used to score parsed zoning-code transitions.

zoning_far_dictionary <- read_csv("nyc_zoning_district_lookup.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    zoning_code = normalize_zoning_code(zoning_code),
    district_family = as.character(district_family),
    residential_allowed_flag = as.logical(residential_allowed_flag),
    commercial_allowed_flag = as.logical(commercial_allowed_flag),
    manufacturing_allowed_flag = as.logical(manufacturing_allowed_flag),
    contextual_flag = as.logical(contextual_flag),
    height_factor_flag = as.logical(height_factor_flag),
    residential_equivalent_code = normalize_zoning_code(residential_equivalent_code),
    approx_resid_far = suppressWarnings(as.numeric(approx_resid_far)),
    standard_resid_far_min = suppressWarnings(as.numeric(standard_resid_far_min)),
    standard_resid_far_max = suppressWarnings(as.numeric(standard_resid_far_max)),
    mih_resid_far = suppressWarnings(as.numeric(mih_resid_far)),
    community_facility_far = suppressWarnings(as.numeric(community_facility_far)),
    commercial_far = suppressWarnings(as.numeric(commercial_far)),
    manufacturing_far = suppressWarnings(as.numeric(manufacturing_far)),
    source_url = as.character(source_url),
    far_source_note = as.character(source_note)
  )

if (nrow(zoning_far_dictionary) != n_distinct(zoning_far_dictionary$zoning_code)) {
  stop("NYC zoning district lookup is not unique by zoning_code.")
}

fallback_resid_far <- function(zoning_code) {
  code <- normalize_zoning_code(zoning_code)
  commercial_suffix <- suppressWarnings(as.integer(str_match(code, "^C[0-9]-?([0-9]+)")[, 2]))

  case_when(
    str_detect(code, "^M[0-9]") ~ 0,
    str_detect(code, "^C[78]") ~ 0,
    str_detect(code, "^R1") ~ 0.50,
    str_detect(code, "^R2") ~ 0.50,
    str_detect(code, "^R3") ~ 0.50,
    str_detect(code, "^R4") ~ 0.75,
    str_detect(code, "^R5") ~ 1.25,
    str_detect(code, "^R6") ~ 2.43,
    str_detect(code, "^R7") ~ 3.44,
    str_detect(code, "^R8") ~ 6.02,
    str_detect(code, "^R9") ~ 7.52,
    str_detect(code, "^R10") ~ 10.00,
    str_detect(code, "^C[12]") & commercial_suffix <= 2 ~ 1.25,
    str_detect(code, "^C[12]") & commercial_suffix %in% c(3, 4) ~ 2.43,
    str_detect(code, "^C[12]") & commercial_suffix %in% c(5, 6) ~ 3.44,
    str_detect(code, "^C[12]") & commercial_suffix == 7 ~ 6.02,
    str_detect(code, "^C[12]") & commercial_suffix == 8 ~ 7.52,
    str_detect(code, "^C[12]") & commercial_suffix >= 9 ~ 10.00,
    str_detect(code, "^C3") ~ 0.50,
    str_detect(code, "^C4") & commercial_suffix == 1 ~ 1.25,
    str_detect(code, "^C4") & commercial_suffix %in% c(2, 3) ~ 2.43,
    str_detect(code, "^C4") & commercial_suffix %in% c(4, 5) ~ 3.44,
    str_detect(code, "^C4") & commercial_suffix >= 6 ~ 10.00,
    str_detect(code, "^C[56]") ~ 10.00,
    TRUE ~ NA_real_
  )
}

direction_levels <- c("upzoning", "downzoning", "mixed", "no_material_residential_change", "unknown")
magnitude_levels <- c("large_up", "moderate_up", "small_up", "mixed", "no_material", "small_down", "moderate_down", "large_down", "unknown")

# Parse and classify completed ZAP zoning map actions.

project_df <- read_csv("../input/zap_zoning_map_special_permit_project_classification.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    project_id = as.character(project_id),
    project_name = as.character(project_name),
    project_brief = as.character(project_brief),
    completed_year = suppressWarnings(as.integer(completed_year)),
    completed_date = as.Date(completed_date),
    event_period = event_period_from_year(completed_year),
    borough_name_standardized = as.character(borough_name_standardized),
    community_district = as.character(community_district),
    borocd_primary = suppressWarnings(as.integer(borocd_primary)),
    applicant_type = as.character(applicant_type),
    primary_applicant = as.character(primary_applicant),
    has_zoning_map_change = as.logical(has_zoning_map_change),
    has_zoning_special_permit = as.logical(has_zoning_special_permit),
    housing_any_candidate_flag = as.logical(housing_any_candidate_flag),
    residential_constraint_text_flag = as.logical(residential_constraint_text_flag),
    increased_residential_proxy = as.logical(increased_residential_proxy),
    zoning_category = as.character(zoning_category),
    project_text = clean_project_text(paste(project_name, project_brief, primary_applicant)),
    text_zoning_codes = vapply(project_text, collapse_zoning_codes, character(1)),
    text_zoning_code_count = vapply(project_text, count_zoning_codes, integer(1)),
    text_commercial_overlay_codes = vapply(
      project_text,
      collapse_zoning_codes_if,
      character(1),
      predicate = is_commercial_overlay_code
    ),
    text_c1_c2_codes = vapply(project_text, collapse_zoning_codes_if, character(1), predicate = is_c1_c2_code),
    text_standalone_c1_c2_codes = vapply(
      project_text,
      collapse_zoning_codes_if,
      character(1),
      predicate = is_standalone_c1_c2_code
    ),
    text_residential_base_codes = vapply(
      project_text,
      collapse_zoning_codes_if,
      character(1),
      predicate = function(x) str_detect(x, "^R")
    ),
    text_manufacturing_codes = vapply(
      project_text,
      collapse_zoning_codes_if,
      character(1),
      predicate = function(x) str_detect(x, "^M")
    ),
    text_other_non_overlay_codes = vapply(
      project_text,
      collapse_zoning_codes_if,
      character(1),
      predicate = function(x) !is_commercial_overlay_code(x) & !str_detect(x, "^R")
    ),
    commercial_overlay_text_flag = text_commercial_overlay_codes != "",
    c1_c2_text_flag = text_c1_c2_codes != "",
    standalone_c1_c2_text_flag = text_standalone_c1_c2_codes != "",
    commercial_overlay_action_flag = commercial_overlay_text_flag &
      str_detect(project_text, "\\b(ADD|ESTABLISH|EXTEND|REMOVE|ELIMINATE|CHANGE|MODIFY)\\b|\\b(OVERLAY|ON EXISTING|WITHIN)\\b"),
    commercial_overlay_explicit_base_flag = commercial_overlay_text_flag &
      text_residential_base_codes != "" &
      text_other_non_overlay_codes == "" &
      str_detect(project_text, "\\b(OVERLAY|ON EXISTING|WITHIN|OVER\\s+R-?[0-9]|FROM\\s+R-?[0-9])\\b"),
    commercial_overlay_use_intent_flag = commercial_overlay_text_flag &
      str_detect(project_text, "RESTAURANT|BANK|DRIVE[- ]?IN|DRIVE[- ]?THRU|RETAIL|STORE|COMMERCIAL|COMM'L|COMML|PARKING|PKING|GARAGE|ACCESSORY|AUTO|GARDEN|TREE SERVICE|FUNERAL"),
    mixed_use_text_flag = str_detect(project_text, "MIXED[- ]USE|SPECIAL MIXED USE|\\bMX\\b|\\bM[0-9][0-9A-Z-]*/R[0-9]"),
    urban_renewal_special_district_text_flag = str_detect(
      project_text,
      "\\b(URP|NDP|URBAN RENEWAL|SPECIAL DIST|SPECIAL DISTRICT|WATERFRONT AREA|ZONING LOT MERGER|NA-[0-9])\\b"
    )
  ) |>
  filter(
    completed_year >= 1980,
    completed_year <= 2025,
    !is.na(event_period),
    has_zoning_map_change
  ) |>
  arrange(completed_year, project_id)

if (nrow(project_df) != n_distinct(project_df$project_id)) {
  stop("Completed ZAP zoning-map project input is not unique by project_id.")
}

pair_df <- bind_rows(lapply(seq_len(nrow(project_df)), function(i) {
  extract_zoning_pairs(project_df$project_id[[i]], project_df$project_text[[i]])
}))

if (nrow(pair_df) == 0) {
  pair_df <- tibble(
    project_id = character(),
    match_index = integer(),
    from_zoning_code = character(),
    to_zoning_code = character(),
    parser_stage = character(),
    ignored_commercial_overlay_codes = character(),
    ignored_mixed_use_component_codes = character()
  )
}

zoning_code_lookup <- bind_rows(
  pair_df |> transmute(zoning_code = from_zoning_code),
  pair_df |> transmute(zoning_code = to_zoning_code)
) |>
  filter(!is.na(zoning_code), zoning_code != "") |>
  distinct(zoning_code) |>
  left_join(zoning_far_dictionary, by = "zoning_code", relationship = "one-to-one") |>
  mutate(
    lookup_found_flag = !is.na(far_source_note),
    fallback_far = if_else(lookup_found_flag, NA_real_, fallback_resid_far(zoning_code)),
    approx_resid_far = coalesce(approx_resid_far, fallback_far),
    far_source_note = case_when(
      !is.na(far_source_note) ~ far_source_note,
      !is.na(fallback_far) ~ "Fallback family-level residential FAR approximation",
      TRUE ~ NA_character_
    )
  ) |>
  select(zoning_code, approx_resid_far, far_source_note)

pair_df <- pair_df |>
  left_join(
    zoning_code_lookup |>
      rename(from_resid_far = approx_resid_far, from_far_source_note = far_source_note),
    by = c("from_zoning_code" = "zoning_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    zoning_code_lookup |>
      rename(to_resid_far = approx_resid_far, to_far_source_note = far_source_note),
    by = c("to_zoning_code" = "zoning_code"),
    relationship = "many-to-one"
  ) |>
  mutate(
    from_commercial_overlay_flag = is_commercial_overlay_code(from_zoning_code),
    to_commercial_overlay_flag = is_commercial_overlay_code(to_zoning_code),
    from_c1_c2_flag = is_c1_c2_code(from_zoning_code),
    to_c1_c2_flag = is_c1_c2_code(to_zoning_code),
    from_standalone_c1_c2_flag = is_standalone_c1_c2_code(from_zoning_code),
    to_standalone_c1_c2_flag = is_standalone_c1_c2_code(to_zoning_code),
    commercial_overlay_pair_flag = from_commercial_overlay_flag | to_commercial_overlay_flag,
    c1_c2_pair_flag = from_c1_c2_flag | to_c1_c2_flag,
    standalone_c1_c2_pair_flag = from_standalone_c1_c2_flag | to_standalone_c1_c2_flag,
    far_delta = to_resid_far - from_resid_far,
    known_pair_flag = !is.na(from_resid_far) & !is.na(to_resid_far),
    unknown_code_flag = is.na(from_resid_far) | is.na(to_resid_far)
  ) |>
  arrange(project_id, match_index, from_zoning_code, to_zoning_code)

pair_summary <- pair_df |>
  group_by(project_id) |>
  summarize(
    parsed_pair_count = n(),
    known_pair_count = sum(known_pair_flag),
    unknown_code_count = sum(unknown_code_flag),
    context_pair_count = sum(parser_stage != "primary_transition"),
    commercial_overlay_pair_count = sum(commercial_overlay_pair_flag),
    c1_c2_pair_count = sum(c1_c2_pair_flag),
    standalone_c1_c2_pair_count = sum(standalone_c1_c2_pair_flag),
    parser_stages = paste(sort(unique(parser_stage)), collapse = "; "),
    parsed_zoning_changes = paste(unique(paste0(from_zoning_code, " to ", to_zoning_code)), collapse = "; "),
    commercial_overlay_pair_codes = paste(sort(unique(c(from_zoning_code[from_commercial_overlay_flag], to_zoning_code[to_commercial_overlay_flag]))), collapse = "; "),
    c1_c2_pair_codes = paste(sort(unique(c(from_zoning_code[from_c1_c2_flag], to_zoning_code[to_c1_c2_flag]))), collapse = "; "),
    ignored_commercial_overlay_codes = paste(sort(unique(ignored_commercial_overlay_codes[ignored_commercial_overlay_codes != ""])), collapse = "; "),
    ignored_mixed_use_component_codes = paste(sort(unique(ignored_mixed_use_component_codes[ignored_mixed_use_component_codes != ""])), collapse = "; "),
    unrecognized_zoning_codes = paste(sort(unique(c(from_zoning_code[is.na(from_resid_far)], to_zoning_code[is.na(to_resid_far)]))), collapse = "; "),
    project_net_far_delta = if (known_pair_count > 0) mean(far_delta[known_pair_flag]) else NA_real_,
    project_gross_up_far_delta = if (known_pair_count > 0) sum(pmax(far_delta[known_pair_flag], 0)) / known_pair_count else NA_real_,
    project_gross_down_far_delta = if (known_pair_count > 0) sum(abs(pmin(far_delta[known_pair_flag], 0))) / known_pair_count else NA_real_,
    project_max_abs_far_delta = if (known_pair_count > 0) max(abs(far_delta[known_pair_flag])) else NA_real_,
    has_positive_far_delta = any(far_delta[known_pair_flag] > 0.05),
    has_negative_far_delta = any(far_delta[known_pair_flag] < -0.05),
    .groups = "drop"
  )

project_classification <- project_df |>
  left_join(pair_summary, by = "project_id", relationship = "one-to-one") |>
  mutate(
    parsed_pair_count = coalesce(parsed_pair_count, 0L),
    known_pair_count = coalesce(known_pair_count, 0L),
    unknown_code_count = coalesce(unknown_code_count, 0L),
    context_pair_count = coalesce(context_pair_count, 0L),
    commercial_overlay_pair_count = coalesce(commercial_overlay_pair_count, 0L),
    c1_c2_pair_count = coalesce(c1_c2_pair_count, 0L),
    standalone_c1_c2_pair_count = coalesce(standalone_c1_c2_pair_count, 0L),
    parser_stages = coalesce(parser_stages, ""),
    commercial_overlay_pair_codes = coalesce(commercial_overlay_pair_codes, ""),
    c1_c2_pair_codes = coalesce(c1_c2_pair_codes, ""),
    ignored_commercial_overlay_codes = coalesce(ignored_commercial_overlay_codes, ""),
    ignored_mixed_use_component_codes = coalesce(ignored_mixed_use_component_codes, ""),
    unrecognized_zoning_codes = coalesce(unrecognized_zoning_codes, ""),
    has_positive_far_delta = coalesce(has_positive_far_delta, FALSE),
    has_negative_far_delta = coalesce(has_negative_far_delta, FALSE),
    initial_parse_status = case_when(
      parsed_pair_count == 0 ~ "no_parsed_zoning_change",
      known_pair_count == 0 ~ "no_known_far_pairs",
      unknown_code_count > 0 ~ "partial_unknown_code",
      TRUE ~ "parsed_known_far"
    ),
    initial_rezoning_direction = case_when(
      known_pair_count == 0 ~ "unknown",
      has_positive_far_delta & has_negative_far_delta ~ "mixed",
      has_positive_far_delta ~ "upzoning",
      has_negative_far_delta ~ "downzoning",
      TRUE ~ "no_material_residential_change"
    ),
    commercial_overlay_pair_flag = commercial_overlay_pair_count > 0,
    c1_c2_pair_flag = c1_c2_pair_count > 0,
    standalone_c1_c2_pair_flag = standalone_c1_c2_pair_count > 0,
    ignored_commercial_overlay_flag = ignored_commercial_overlay_codes != "",
    ignored_mixed_use_component_flag = ignored_mixed_use_component_codes != "",
    commercial_overlay_project_flag = commercial_overlay_text_flag |
      commercial_overlay_pair_flag |
      ignored_commercial_overlay_flag,
    c1_c2_project_flag = c1_c2_text_flag | c1_c2_pair_flag,
    context_parser_project_flag = context_pair_count > 0,
    overlay_no_material_high_confidence_flag = initial_rezoning_direction == "unknown" &
      commercial_overlay_project_flag &
      !standalone_c1_c2_text_flag &
      !standalone_c1_c2_pair_flag &
      (
        commercial_overlay_explicit_base_flag |
          (text_residential_base_codes != "" & text_other_non_overlay_codes == "" & str_detect(project_text, "\\b(OVERLAY|ON EXISTING|OVER\\s+R|WITHIN|FROM\\s+R)\\b")) |
          (text_residential_base_codes == "" & text_zoning_code_count > 0 & text_other_non_overlay_codes == "" & commercial_overlay_pair_flag)
      ),
    overlay_no_material_medium_confidence_flag = initial_rezoning_direction == "unknown" &
      commercial_overlay_project_flag &
      !standalone_c1_c2_text_flag &
      !standalone_c1_c2_pair_flag &
      !overlay_no_material_high_confidence_flag &
      text_residential_base_codes != "" &
      text_other_non_overlay_codes == "" &
      commercial_overlay_use_intent_flag &
      !coalesce(housing_any_candidate_flag, FALSE),
    overlay_no_material_rule_flag = overlay_no_material_high_confidence_flag |
      overlay_no_material_medium_confidence_flag,
    project_net_far_delta = if_else(overlay_no_material_rule_flag, 0, project_net_far_delta),
    project_gross_up_far_delta = if_else(overlay_no_material_rule_flag, 0, project_gross_up_far_delta),
    project_gross_down_far_delta = if_else(overlay_no_material_rule_flag, 0, project_gross_down_far_delta),
    project_max_abs_far_delta = if_else(overlay_no_material_rule_flag, 0, project_max_abs_far_delta),
    parse_status = case_when(
      overlay_no_material_rule_flag ~ "overlay_no_material_rule",
      TRUE ~ initial_parse_status
    ),
    rezoning_direction = case_when(
      overlay_no_material_rule_flag ~ "no_material_residential_change",
      TRUE ~ initial_rezoning_direction
    ),
    classification_source_tier = case_when(
      overlay_no_material_high_confidence_flag ~ "auto_overlay_no_material_high",
      overlay_no_material_medium_confidence_flag ~ "auto_overlay_no_material_medium",
      rezoning_direction == "unknown" ~ "unknown",
      context_parser_project_flag & known_pair_count > 0 ~ "auto_context_transition_known_far",
      ignored_mixed_use_component_flag & known_pair_count > 0 ~ "auto_combined_mixed_use_transition_known_far",
      ignored_commercial_overlay_flag & known_pair_count > 0 ~ "auto_combined_overlay_transition_known_far",
      known_pair_count > 0 ~ "auto_primary_transition_known_far",
      TRUE ~ "auto_other_known_direction"
    ),
    magnitude_source_delta = case_when(
      rezoning_direction == "upzoning" ~ project_gross_up_far_delta,
      rezoning_direction == "downzoning" ~ project_gross_down_far_delta,
      rezoning_direction == "mixed" ~ project_max_abs_far_delta,
      rezoning_direction == "no_material_residential_change" ~ 0,
      TRUE ~ NA_real_
    ),
    magnitude_bin = case_when(
      rezoning_direction == "upzoning" & magnitude_source_delta >= 2 ~ "large_up",
      rezoning_direction == "upzoning" & magnitude_source_delta >= 0.5 ~ "moderate_up",
      rezoning_direction == "upzoning" ~ "small_up",
      rezoning_direction == "downzoning" & magnitude_source_delta >= 2 ~ "large_down",
      rezoning_direction == "downzoning" & magnitude_source_delta >= 0.5 ~ "moderate_down",
      rezoning_direction == "downzoning" ~ "small_down",
      rezoning_direction == "mixed" ~ "mixed",
      rezoning_direction == "no_material_residential_change" ~ "no_material",
      TRUE ~ "unknown"
    ),
    rezoning_direction = factor(rezoning_direction, levels = direction_levels),
    magnitude_bin = factor(magnitude_bin, levels = magnitude_levels),
    commercial_overlay_unknown_flag = as.character(rezoning_direction) == "unknown" & commercial_overlay_project_flag,
    mixed_use_unknown_flag = as.character(rezoning_direction) == "unknown" & mixed_use_text_flag,
    missing_direction_reason = case_when(
      as.character(rezoning_direction) != "unknown" ~ "assigned_direction",
      commercial_overlay_action_flag | commercial_overlay_pair_flag ~ "commercial_overlay_or_c1_c2_ambiguous",
      mixed_use_text_flag ~ "mixed_use_or_mx_text_no_direction",
      parse_status == "no_known_far_pairs" & standalone_c1_c2_pair_flag ~ "standalone_c1_c2_without_prior_base",
      parse_status == "no_known_far_pairs" & unknown_code_count > 0 ~ "parsed_transition_without_residential_far",
      parse_status == "no_parsed_zoning_change" & text_zoning_code_count > 0 ~ "zoning_codes_present_no_from_to_transition",
      urban_renewal_special_district_text_flag ~ "urban_renewal_special_district_no_codes",
      TRUE ~ "no_zoning_code_transition"
    )
  )

# Strict scope assignment uses only project-BBL links that map to CCD2010 and current MapPLUTO.

zap_project_bbl <- read_parquet("../input/zap_project_bbl.parquet") |>
  transmute(
    project_id = as.character(project_id),
    bbl_standardized = as.character(bbl_standardized)
  ) |>
  filter(!is.na(project_id), project_id != "", !is.na(bbl_standardized), bbl_standardized != "") |>
  distinct(project_id, bbl_standardized)

if (nrow(zap_project_bbl) != nrow(distinct(zap_project_bbl, project_id, bbl_standardized))) {
  stop("ZAP project-BBL input is not unique by project_id and BBL.")
}

ccdist2010_bbl_lookup <- read_parquet("../input/ccdist2010_mappluto_bbl_lookup.parquet") |>
  transmute(
    bbl_standardized = as.character(bbl),
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    council_district = suppressWarnings(as.integer(council_district))
  ) |>
  filter(!is.na(bbl_standardized), bbl_standardized != "", !is.na(district_id), !is.na(council_district)) |>
  distinct(bbl_standardized, .keep_all = TRUE)

if (nrow(ccdist2010_bbl_lookup) != n_distinct(ccdist2010_bbl_lookup$bbl_standardized)) {
  stop("2010 Council district BBL lookup is not unique by BBL.")
}

mappluto_lot <- read_parquet(
  "../input/dcp_mappluto_current_25v4.parquet",
  col_select = c("bbl", "lotarea", "landuse", "unitsres", "resarea", "is_joint_interest_area")
) |>
  as.data.frame() |>
  as_tibble() |>
  transmute(
    bbl_standardized = as.character(bbl),
    lotarea = suppressWarnings(as.numeric(lotarea)),
    landuse = str_pad(as.character(landuse), width = 2, side = "left", pad = "0"),
    unitsres = suppressWarnings(as.numeric(unitsres)),
    resarea = suppressWarnings(as.numeric(resarea)),
    is_joint_interest_area = as.logical(is_joint_interest_area)
  ) |>
  filter(!coalesce(is_joint_interest_area, FALSE), !is.na(bbl_standardized), bbl_standardized != "") |>
  distinct(bbl_standardized, .keep_all = TRUE) |>
  mutate(
    lot_acres = pmax(coalesce(lotarea, 0), 0) / 43560,
    current_residential_lot_flag = landuse %in% c("01", "02", "03", "04") |
      coalesce(unitsres, 0) > 0 |
      coalesce(resarea, 0) > 0,
    current_residential_lot_acres = if_else(current_residential_lot_flag, lot_acres, 0)
  )

if (nrow(mappluto_lot) != n_distinct(mappluto_lot$bbl_standardized)) {
  stop("Current MapPLUTO lot input is not unique by BBL.")
}

district_lookup <- read_csv("../input/ccdist2010_homeownership_1990_measure.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    council_district = suppressWarnings(as.integer(council_district)),
    borough_code = as.character(suppressWarnings(as.integer(borough_code))),
    borough_name = as.character(borough_name),
    occupied_units_1990 = suppressWarnings(as.numeric(occupied_units_1990)),
    vacancy_rate_1990 = suppressWarnings(as.numeric(vacancy_rate_1990)),
    median_household_income_1990 = suppressWarnings(as.numeric(median_household_income_1990)),
    treat_pp = suppressWarnings(as.numeric(treat_pp)),
    treat_z_boro = suppressWarnings(as.numeric(treat_z_boro))
  ) |>
  filter(!is.na(district_id), !is.na(council_district), occupied_units_1990 > 0) |>
  arrange(treat_z_boro, council_district) |>
  mutate(
    homeowner_tercile = ntile(treat_z_boro, 3),
    homeowner_tercile_label = case_when(
      homeowner_tercile == 1 ~ "Low homeowner",
      homeowner_tercile == 2 ~ "Middle homeowner",
      homeowner_tercile == 3 ~ "High homeowner",
      TRUE ~ NA_character_
    )
  )

if (nrow(district_lookup) != 51 || nrow(district_lookup) != n_distinct(district_lookup$district_id)) {
  stop("Expected exactly 51 unique 2010 Council districts in the treatment lookup.")
}

tercile_denominators <- district_lookup |>
  group_by(homeowner_tercile, homeowner_tercile_label) |>
  summarize(
    council_district_count = n_distinct(district_id),
    occupied_units_1990 = sum(occupied_units_1990, na.rm = TRUE),
    .groups = "drop"
  )

if (any(tercile_denominators$council_district_count != 17)) {
  stop("2010 Council district homeowner terciles must contain 17 districts each.")
}

redev_denominators <- read_csv("../input/ccdist2010_redevelopment_potential.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    residential_acres = suppressWarnings(as.numeric(residential_acres))
  ) |>
  filter(!is.na(district_id)) |>
  distinct(district_id, .keep_all = TRUE)

if (nrow(redev_denominators) != 51 || nrow(redev_denominators) != n_distinct(redev_denominators$district_id)) {
  stop("Expected exactly 51 unique 2010 Council districts in the redevelopment denominator lookup.")
}

district_lookup <- district_lookup |>
  left_join(redev_denominators, by = "district_id", relationship = "one-to-one")

if (any(is.na(district_lookup$residential_acres)) || any(district_lookup$residential_acres <= 0)) {
  stop("2010 Council district residential-acre denominators must be positive and nonmissing.")
}

tercile_denominators <- district_lookup |>
  group_by(homeowner_tercile, homeowner_tercile_label) |>
  summarize(
    council_district_count = n_distinct(district_id),
    occupied_units_1990 = sum(occupied_units_1990, na.rm = TRUE),
    residential_acres = sum(residential_acres, na.rm = TRUE),
    .groups = "drop"
  )

project_bbl_match_quality <- project_classification |>
  select(
    project_id,
    completed_year,
    event_period,
    project_name,
    borough_name_standardized,
    rezoning_direction,
    magnitude_bin,
    parse_status
  ) |>
  left_join(zap_project_bbl, by = "project_id", relationship = "one-to-many") |>
  left_join(ccdist2010_bbl_lookup, by = "bbl_standardized", relationship = "many-to-one") |>
  left_join(mappluto_lot |> select(bbl_standardized, lot_acres), by = "bbl_standardized", relationship = "many-to-one") |>
  group_by(
    project_id,
    completed_year,
    event_period,
    project_name,
    borough_name_standardized,
    rezoning_direction,
    magnitude_bin,
    parse_status
  ) |>
  summarize(
    linked_bbl_count = n_distinct(bbl_standardized[!is.na(bbl_standardized) & bbl_standardized != ""]),
    ccd_matched_bbl_count = n_distinct(bbl_standardized[!is.na(bbl_standardized) & bbl_standardized != "" & !is.na(district_id)]),
    lot_matched_bbl_count = n_distinct(bbl_standardized[!is.na(bbl_standardized) & bbl_standardized != "" & !is.na(lot_acres)]),
    strict_matched_bbl_count = n_distinct(bbl_standardized[!is.na(bbl_standardized) & bbl_standardized != "" & !is.na(district_id) & !is.na(lot_acres)]),
    strict_bbl_match_share = if_else(linked_bbl_count > 0, strict_matched_bbl_count / linked_bbl_count, NA_real_),
    high_confidence_scope_flag = linked_bbl_count > 0 & strict_bbl_match_share >= 0.8,
    .groups = "drop"
  ) |>
  arrange(event_period, project_id)

project_bbl_assigned <- project_classification |>
  select(
    project_id,
    completed_year,
    event_period,
    rezoning_direction,
    magnitude_bin,
    commercial_overlay_project_flag,
    commercial_overlay_unknown_flag,
    c1_c2_project_flag,
    mixed_use_text_flag,
    mixed_use_unknown_flag,
    project_net_far_delta,
    project_gross_up_far_delta,
    project_gross_down_far_delta
  ) |>
  inner_join(zap_project_bbl, by = "project_id", relationship = "one-to-many") |>
  left_join(
    project_bbl_match_quality |>
      select(project_id, linked_bbl_count, strict_bbl_match_share, high_confidence_scope_flag),
    by = "project_id",
    relationship = "many-to-one"
  ) |>
  left_join(ccdist2010_bbl_lookup, by = "bbl_standardized", relationship = "many-to-one") |>
  left_join(mappluto_lot, by = "bbl_standardized", relationship = "many-to-one") |>
  filter(!is.na(district_id), !is.na(lot_acres)) |>
  group_by(project_id) |>
  mutate(
    project_strict_bbl_count = n_distinct(bbl_standardized),
    project_assignment_weight = 1 / project_strict_bbl_count
  ) |>
  ungroup() |>
  mutate(
    gross_up_far_acres = if_else(!is.na(project_gross_up_far_delta), project_gross_up_far_delta * lot_acres, NA_real_),
    gross_down_far_acres = if_else(!is.na(project_gross_down_far_delta), project_gross_down_far_delta * lot_acres, NA_real_),
    net_far_acres = if_else(!is.na(project_net_far_delta), project_net_far_delta * lot_acres, NA_real_)
  )

project_weight_bad_count <- project_bbl_assigned |>
  distinct(project_id, bbl_standardized, project_assignment_weight) |>
  group_by(project_id) |>
  summarize(weight_sum = sum(project_assignment_weight), .groups = "drop") |>
  filter(abs(weight_sum - 1) > 1e-8) |>
  nrow()

project_scope_summary <- project_bbl_assigned |>
  group_by(project_id) |>
  summarize(
    strict_assigned_bbl_count = n_distinct(bbl_standardized),
    strict_assigned_district_count = n_distinct(district_id),
    affected_lot_acres = sum(lot_acres, na.rm = TRUE),
    affected_current_residential_lot_acres = sum(current_residential_lot_acres, na.rm = TRUE),
    gross_up_far_acres = sum(gross_up_far_acres, na.rm = TRUE),
    gross_down_far_acres = sum(gross_down_far_acres, na.rm = TRUE),
    net_far_acres = sum(net_far_acres, na.rm = TRUE),
    high_confidence_gross_up_far_acres = if_else(first(high_confidence_scope_flag), sum(gross_up_far_acres, na.rm = TRUE), 0),
    high_confidence_gross_down_far_acres = if_else(first(high_confidence_scope_flag), sum(gross_down_far_acres, na.rm = TRUE), 0),
    high_confidence_net_far_acres = if_else(first(high_confidence_scope_flag), sum(net_far_acres, na.rm = TRUE), 0),
    .groups = "drop"
  )

project_classification <- project_classification |>
  left_join(project_bbl_match_quality, by = c("project_id", "completed_year", "event_period", "project_name", "borough_name_standardized", "rezoning_direction", "magnitude_bin", "parse_status"), relationship = "one-to-one") |>
  left_join(project_scope_summary, by = "project_id", relationship = "one-to-one") |>
  mutate(
    linked_bbl_count = coalesce(linked_bbl_count, 0L),
    ccd_matched_bbl_count = coalesce(ccd_matched_bbl_count, 0L),
    lot_matched_bbl_count = coalesce(lot_matched_bbl_count, 0L),
    strict_matched_bbl_count = coalesce(strict_matched_bbl_count, 0L),
    high_confidence_scope_flag = coalesce(high_confidence_scope_flag, FALSE),
    strict_assigned_bbl_count = coalesce(strict_assigned_bbl_count, 0L),
    strict_assigned_district_count = coalesce(strict_assigned_district_count, 0L),
    strict_bbl_scope_flag = strict_assigned_bbl_count > 0,
    affected_lot_acres = coalesce(affected_lot_acres, 0),
    affected_current_residential_lot_acres = coalesce(affected_current_residential_lot_acres, 0),
    gross_up_far_acres = coalesce(gross_up_far_acres, 0),
    gross_down_far_acres = coalesce(gross_down_far_acres, 0),
    net_far_acres = coalesce(net_far_acres, 0),
    high_confidence_gross_up_far_acres = coalesce(high_confidence_gross_up_far_acres, 0),
    high_confidence_gross_down_far_acres = coalesce(high_confidence_gross_down_far_acres, 0),
    high_confidence_net_far_acres = coalesce(high_confidence_net_far_acres, 0),
    abs_net_far_acres = abs(net_far_acres)
  ) |>
  arrange(completed_year, project_id)

# Manual/source-reviewed labels are kept separate from parser outputs.

manual_columns <- c(
  "project_id",
  "manual_review_status",
  "manual_rezoning_class",
  "housing_intent",
  "scope_type",
  "capacity_direction",
  "manual_confidence",
  "source_url",
  "source_note",
  "manual_notes"
)

manual_rezoning_raw <- read_csv("manual_rezoning_direction_classification.csv", show_col_types = FALSE, na = c("", "NA"))
missing_manual_columns <- setdiff(manual_columns, names(manual_rezoning_raw))
if (length(missing_manual_columns) > 0) {
  stop("manual_rezoning_direction_classification.csv is missing columns: ", paste(missing_manual_columns, collapse = ", "))
}

manual_rezoning_classification <- manual_rezoning_raw |>
  transmute(
    project_id = as.character(project_id),
    manual_review_status = as.character(manual_review_status),
    manual_rezoning_class = as.character(manual_rezoning_class),
    housing_intent = as.character(housing_intent),
    scope_type = as.character(scope_type),
    capacity_direction = as.character(capacity_direction),
    manual_confidence = as.character(manual_confidence),
    source_url = as.character(source_url),
    source_note = as.character(source_note),
    manual_notes = as.character(manual_notes)
  ) |>
  filter(!is.na(project_id), project_id != "")

if (nrow(manual_rezoning_classification) != n_distinct(manual_rezoning_classification$project_id)) {
  stop("Manual rezoning classification file is not unique by project_id.")
}

source_review_columns <- c(
  "project_id",
  "review_source_status",
  "review_source_rezoning_direction",
  "review_source_rezoning_class",
  "review_source_housing_intent",
  "review_source_scope_type",
  "review_source_scope_blocks",
  "review_source_scope_acres",
  "review_source_scope_lots",
  "review_source_scope_description",
  "review_source_contextual_restriction_flag",
  "review_source_form_restriction_flag",
  "review_source_numeric_far_direction",
  "review_source_confidence",
  "review_source_url",
  "review_source_title",
  "review_source_note"
)

source_rezoning_scope_raw <- read_csv("source_reviewed_rezoning_scope.csv", show_col_types = FALSE, na = c("", "NA"))
missing_source_review_columns <- setdiff(source_review_columns, names(source_rezoning_scope_raw))
if (length(missing_source_review_columns) > 0) {
  stop("source_reviewed_rezoning_scope.csv is missing columns: ", paste(missing_source_review_columns, collapse = ", "))
}

source_rezoning_scope <- source_rezoning_scope_raw |>
  transmute(
    project_id = as.character(project_id),
    review_source_status = as.character(review_source_status),
    review_source_rezoning_direction = as.character(review_source_rezoning_direction),
    review_source_rezoning_class = as.character(review_source_rezoning_class),
    review_source_housing_intent = as.character(review_source_housing_intent),
    review_source_scope_type = as.character(review_source_scope_type),
    review_source_scope_blocks = suppressWarnings(as.numeric(review_source_scope_blocks)),
    review_source_scope_acres = suppressWarnings(as.numeric(review_source_scope_acres)),
    review_source_scope_lots = suppressWarnings(as.numeric(review_source_scope_lots)),
    review_source_scope_description = as.character(review_source_scope_description),
    review_source_contextual_restriction_flag = as.logical(review_source_contextual_restriction_flag),
    review_source_form_restriction_flag = as.logical(review_source_form_restriction_flag),
    review_source_numeric_far_direction = as.character(review_source_numeric_far_direction),
    review_source_confidence = as.character(review_source_confidence),
    review_source_url = as.character(review_source_url),
    review_source_title = as.character(review_source_title),
    review_source_note = as.character(review_source_note)
  ) |>
  filter(!is.na(project_id), project_id != "")

if (nrow(source_rezoning_scope) != n_distinct(source_rezoning_scope$project_id)) {
  stop("Source-reviewed rezoning scope file is not unique by project_id.")
}

bad_source_review_direction <- source_rezoning_scope |>
  filter(!review_source_rezoning_direction %in% direction_levels)

if (nrow(bad_source_review_direction) > 0) {
  stop("Source-reviewed rezoning scope has invalid review_source_rezoning_direction values.")
}

reviewed_scope_levels <- c(
  "source_very_large_neighborhood",
  "source_large_neighborhood",
  "source_neighborhood",
  "source_small_area",
  "strict_very_large_scope",
  "strict_large_scope",
  "strict_parcel_or_small_area",
  "no_scope"
)

project_classification <- project_classification |>
  left_join(manual_rezoning_classification, by = "project_id", relationship = "one-to-one") |>
  left_join(source_rezoning_scope, by = "project_id", relationship = "one-to-one") |>
  mutate(
    manual_label_flag = !is.na(manual_rezoning_class),
    manual_review_status = coalesce(manual_review_status, "not_reviewed"),
    manual_rezoning_class = coalesce(manual_rezoning_class, "not_reviewed"),
    housing_intent = coalesce(housing_intent, "not_reviewed"),
    scope_type = coalesce(scope_type, "not_reviewed"),
    capacity_direction = coalesce(capacity_direction, "not_reviewed"),
    manual_confidence = coalesce(manual_confidence, "not_reviewed"),
    manual_reviewed_direction = case_when(
      manual_review_status != "not_reviewed" & capacity_direction == "up" ~ "upzoning",
      manual_review_status != "not_reviewed" & capacity_direction == "down" ~ "downzoning",
      manual_review_status != "not_reviewed" & capacity_direction == "mixed" ~ "mixed",
      manual_review_status != "not_reviewed" & capacity_direction == "no_material_residential_change" ~ "no_material_residential_change",
      TRUE ~ NA_character_
    ),
    manual_reviewed_direction_flag = manual_reviewed_direction %in% direction_levels,
    review_source_label_flag = !is.na(review_source_rezoning_direction),
    review_source_status = coalesce(review_source_status, "not_reviewed"),
    review_source_rezoning_direction = coalesce(review_source_rezoning_direction, "not_reviewed"),
    review_source_rezoning_class = coalesce(review_source_rezoning_class, "not_reviewed"),
    review_source_housing_intent = coalesce(review_source_housing_intent, "not_reviewed"),
    review_source_scope_type = coalesce(review_source_scope_type, "not_reviewed"),
    review_source_contextual_restriction_flag = coalesce(review_source_contextual_restriction_flag, FALSE),
    review_source_form_restriction_flag = coalesce(review_source_form_restriction_flag, FALSE),
    review_source_numeric_far_direction = coalesce(review_source_numeric_far_direction, "not_reviewed"),
    review_source_confidence = coalesce(review_source_confidence, "not_reviewed"),
    review_source_verified_flag = review_source_status == "official_source_verified",
    reviewed_rezoning_direction = case_when(
      review_source_verified_flag & review_source_rezoning_direction %in% direction_levels ~ review_source_rezoning_direction,
      manual_reviewed_direction_flag ~ manual_reviewed_direction,
      TRUE ~ as.character(rezoning_direction)
    ),
    reviewed_direction_source = case_when(
      review_source_verified_flag & review_source_rezoning_direction %in% direction_levels ~ "official_source_review",
      manual_reviewed_direction_flag ~ "manual_text_review",
      TRUE ~ classification_source_tier
    ),
    reviewed_contextual_or_form_restriction_flag = review_source_verified_flag &
      (review_source_contextual_restriction_flag | review_source_form_restriction_flag),
    reviewed_restrictive_rezoning_flag = reviewed_rezoning_direction == "downzoning" |
      (reviewed_rezoning_direction == "mixed" & reviewed_contextual_or_form_restriction_flag),
    reviewed_policy_scope_source = case_when(
      review_source_verified_flag &
        (!is.na(review_source_scope_blocks) | !is.na(review_source_scope_acres) | !is.na(review_source_scope_lots)) ~ "source_stated_scope",
      strict_bbl_scope_flag ~ "strict_bbl_scope",
      TRUE ~ "no_scope"
    ),
    reviewed_policy_scope_blocks = if_else(review_source_verified_flag, coalesce(review_source_scope_blocks, 0), 0),
    reviewed_policy_scope_acres = if_else(
      review_source_verified_flag,
      coalesce(review_source_scope_acres, if_else(strict_bbl_scope_flag, affected_lot_acres, 0)),
      if_else(strict_bbl_scope_flag, affected_lot_acres, 0)
    ),
    reviewed_scope_bin = case_when(
      review_source_verified_flag & !is.na(review_source_scope_blocks) & review_source_scope_blocks >= 250 ~ "source_very_large_neighborhood",
      review_source_verified_flag & !is.na(review_source_scope_blocks) & review_source_scope_blocks >= 100 ~ "source_large_neighborhood",
      review_source_verified_flag & !is.na(review_source_scope_blocks) & review_source_scope_blocks >= 25 ~ "source_neighborhood",
      review_source_verified_flag & !is.na(review_source_scope_blocks) & review_source_scope_blocks > 0 ~ "source_small_area",
      strict_bbl_scope_flag & affected_lot_acres >= 100 ~ "strict_very_large_scope",
      strict_bbl_scope_flag & affected_lot_acres >= 20 ~ "strict_large_scope",
      strict_bbl_scope_flag ~ "strict_parcel_or_small_area",
      TRUE ~ "no_scope"
    )
  )

project_bbl_match_quality <- project_classification |>
  select(
    project_id,
    completed_year,
    event_period,
    project_name,
    borough_name_standardized,
    rezoning_direction,
    magnitude_bin,
    parse_status,
    linked_bbl_count,
    ccd_matched_bbl_count,
    lot_matched_bbl_count,
    strict_matched_bbl_count,
    strict_bbl_match_share,
    strict_bbl_scope_flag,
    high_confidence_scope_flag
  ) |>
  arrange(event_period, project_id)

source_reviewed_cases <- project_classification |>
  filter(review_source_label_flag) |>
  arrange(desc(review_source_verified_flag), completed_year, project_id) |>
  select(
    project_id,
    completed_year,
    event_period,
    project_name,
    project_brief,
    borough_name_standardized,
    rezoning_direction,
    reviewed_rezoning_direction,
    reviewed_direction_source,
    manual_reviewed_direction,
    reviewed_contextual_or_form_restriction_flag,
    reviewed_restrictive_rezoning_flag,
    reviewed_policy_scope_source,
    reviewed_policy_scope_blocks,
    reviewed_policy_scope_acres,
    reviewed_scope_bin,
    affected_lot_acres,
    net_far_acres,
    review_source_status,
    review_source_rezoning_direction,
    review_source_rezoning_class,
    review_source_housing_intent,
    review_source_scope_type,
    review_source_scope_blocks,
    review_source_scope_acres,
    review_source_scope_lots,
    review_source_scope_description,
    review_source_contextual_restriction_flag,
    review_source_form_restriction_flag,
    review_source_numeric_far_direction,
    review_source_confidence,
    review_source_url,
    review_source_title,
    review_source_note
  )

reviewed_city_year <- expand_grid(
  completed_year = 1980:2025,
  reviewed_rezoning_direction = factor(direction_levels, levels = direction_levels),
  reviewed_scope_bin = factor(reviewed_scope_levels, levels = reviewed_scope_levels)
) |>
  mutate(event_period = event_period_from_year(completed_year)) |>
  left_join(
    project_classification |>
      group_by(completed_year, event_period, reviewed_rezoning_direction, reviewed_scope_bin) |>
      summarize(
        project_count = n_distinct(project_id),
        official_source_reviewed_project_count = sum(review_source_verified_flag),
        source_review_seed_project_count = sum(review_source_label_flag & !review_source_verified_flag),
        restrictive_rezoning_project_count = sum(reviewed_restrictive_rezoning_flag),
        contextual_or_form_restriction_project_count = sum(reviewed_contextual_or_form_restriction_flag),
        strict_scope_project_count = sum(strict_bbl_scope_flag),
        affected_lot_acres = sum(affected_lot_acres, na.rm = TRUE),
        source_scope_blocks = sum(review_source_scope_blocks, na.rm = TRUE),
        source_scope_acres = sum(review_source_scope_acres, na.rm = TRUE),
        reviewed_policy_scope_acres = sum(reviewed_policy_scope_acres, na.rm = TRUE),
        gross_up_far_acres = sum(gross_up_far_acres, na.rm = TRUE),
        gross_down_far_acres = sum(gross_down_far_acres, na.rm = TRUE),
        net_far_acres = sum(net_far_acres, na.rm = TRUE),
        .groups = "drop"
      ),
    by = c("completed_year", "event_period", "reviewed_rezoning_direction", "reviewed_scope_bin"),
    relationship = "one-to-one"
  ) |>
  mutate(
    across(
      c(
        project_count,
        official_source_reviewed_project_count,
        source_review_seed_project_count,
        restrictive_rezoning_project_count,
        contextual_or_form_restriction_project_count,
        strict_scope_project_count
      ),
      ~ coalesce(.x, 0L)
    ),
    across(
      c(
        affected_lot_acres,
        source_scope_blocks,
        source_scope_acres,
        reviewed_policy_scope_acres,
        gross_up_far_acres,
        gross_down_far_acres,
        net_far_acres
      ),
      ~ coalesce(.x, 0)
    )
  ) |>
  arrange(completed_year, reviewed_rezoning_direction, reviewed_scope_bin)

reviewed_period_counts <- reviewed_city_year |>
  group_by(event_period, reviewed_rezoning_direction, reviewed_scope_bin) |>
  summarize(
    project_count = sum(project_count),
    official_source_reviewed_project_count = sum(official_source_reviewed_project_count),
    source_review_seed_project_count = sum(source_review_seed_project_count),
    restrictive_rezoning_project_count = sum(restrictive_rezoning_project_count),
    contextual_or_form_restriction_project_count = sum(contextual_or_form_restriction_project_count),
    strict_scope_project_count = sum(strict_scope_project_count),
    affected_lot_acres = sum(affected_lot_acres),
    source_scope_blocks = sum(source_scope_blocks),
    source_scope_acres = sum(source_scope_acres),
    reviewed_policy_scope_acres = sum(reviewed_policy_scope_acres),
    gross_up_far_acres = sum(gross_up_far_acres),
    gross_down_far_acres = sum(gross_down_far_acres),
    net_far_acres = sum(net_far_acres),
    .groups = "drop"
  ) |>
  mutate(event_period = factor(event_period, levels = event_periods)) |>
  arrange(event_period, reviewed_rezoning_direction, reviewed_scope_bin)

write_csv_if_changed(project_classification, "../output/zap_rezoning_direction_project_classification.csv")
write_csv_if_changed(pair_df, "../output/zap_rezoning_direction_parse_pairs.csv")
write_csv_if_changed(project_bbl_match_quality, "../output/zap_rezoning_direction_scope_match_quality.csv")
write_csv_if_changed(zoning_far_dictionary, "../output/zap_rezoning_direction_zoning_district_lookup.csv")
write_csv_if_changed(source_reviewed_cases, "../output/zap_rezoning_direction_source_reviewed_cases.csv")
write_csv_if_changed(reviewed_city_year, "../output/zap_rezoning_direction_reviewed_city_year.csv")
write_csv_if_changed(reviewed_period_counts, "../output/zap_rezoning_direction_reviewed_period.csv")

# Citywide, district-year, and homeowner-tercile panels.

city_year <- expand_grid(
  completed_year = 1980:2025,
  rezoning_direction = factor(direction_levels, levels = direction_levels),
  magnitude_bin = factor(magnitude_levels, levels = magnitude_levels)
) |>
  mutate(event_period = event_period_from_year(completed_year)) |>
  left_join(
    project_classification |>
      group_by(completed_year, event_period, rezoning_direction, magnitude_bin) |>
      summarize(
        project_count = n_distinct(project_id),
        strict_scope_project_count = sum(strict_bbl_scope_flag),
        high_confidence_scope_project_count = sum(high_confidence_scope_flag),
        affected_bbl_count = sum(strict_assigned_bbl_count, na.rm = TRUE),
        affected_lot_acres = sum(affected_lot_acres, na.rm = TRUE),
        affected_current_residential_lot_acres = sum(affected_current_residential_lot_acres, na.rm = TRUE),
        gross_up_far_acres = sum(gross_up_far_acres, na.rm = TRUE),
        gross_down_far_acres = sum(gross_down_far_acres, na.rm = TRUE),
        net_far_acres = sum(net_far_acres, na.rm = TRUE),
        high_confidence_gross_up_far_acres = sum(high_confidence_gross_up_far_acres, na.rm = TRUE),
        high_confidence_gross_down_far_acres = sum(high_confidence_gross_down_far_acres, na.rm = TRUE),
        high_confidence_net_far_acres = sum(high_confidence_net_far_acres, na.rm = TRUE),
        .groups = "drop"
      ),
    by = c("completed_year", "event_period", "rezoning_direction", "magnitude_bin"),
    relationship = "one-to-one"
  ) |>
  mutate(
    across(c(project_count, strict_scope_project_count, high_confidence_scope_project_count, affected_bbl_count), ~ coalesce(.x, 0L)),
    across(c(affected_lot_acres, affected_current_residential_lot_acres, gross_up_far_acres, gross_down_far_acres, net_far_acres, high_confidence_gross_up_far_acres, high_confidence_gross_down_far_acres, high_confidence_net_far_acres), ~ coalesce(.x, 0))
  ) |>
  arrange(completed_year, rezoning_direction, magnitude_bin)

period_counts <- city_year |>
  group_by(event_period, rezoning_direction, magnitude_bin) |>
  summarize(
    project_count = sum(project_count),
    strict_scope_project_count = sum(strict_scope_project_count),
    high_confidence_scope_project_count = sum(high_confidence_scope_project_count),
    affected_bbl_count = sum(affected_bbl_count),
    affected_lot_acres = sum(affected_lot_acres),
    affected_current_residential_lot_acres = sum(affected_current_residential_lot_acres),
    gross_up_far_acres = sum(gross_up_far_acres),
    gross_down_far_acres = sum(gross_down_far_acres),
    net_far_acres = sum(net_far_acres),
    high_confidence_gross_up_far_acres = sum(high_confidence_gross_up_far_acres),
    high_confidence_gross_down_far_acres = sum(high_confidence_gross_down_far_acres),
    high_confidence_net_far_acres = sum(high_confidence_net_far_acres),
    .groups = "drop"
  ) |>
  mutate(event_period = factor(event_period, levels = event_periods)) |>
  arrange(event_period, rezoning_direction, magnitude_bin)

write_csv_if_changed(city_year, "../output/zap_rezoning_direction_city_year.csv")
write_csv_if_changed(period_counts, "../output/zap_rezoning_direction_period.csv")

diagnostic_year <- project_classification |>
  group_by(completed_year, event_period) |>
  summarize(
    project_count = n_distinct(project_id),
    unknown_project_count = sum(rezoning_direction == "unknown"),
    commercial_overlay_project_count = sum(commercial_overlay_project_flag),
    commercial_overlay_unknown_project_count = sum(commercial_overlay_unknown_flag),
    c1_c2_project_count = sum(c1_c2_project_flag),
    standalone_c1_c2_project_count = sum(standalone_c1_c2_text_flag | standalone_c1_c2_pair_flag),
    mixed_use_text_project_count = sum(mixed_use_text_flag),
    mixed_use_unknown_project_count = sum(mixed_use_unknown_flag),
    housing_candidate_unknown_project_count = sum(rezoning_direction == "unknown" & housing_any_candidate_flag, na.rm = TRUE),
    increased_residential_proxy_unknown_project_count = sum(rezoning_direction == "unknown" & increased_residential_proxy, na.rm = TRUE),
    urban_renewal_special_district_project_count = sum(urban_renewal_special_district_text_flag),
    .groups = "drop"
  ) |>
  arrange(completed_year)

write_csv_if_changed(diagnostic_year, "../output/zap_rezoning_direction_diagnostic_year.csv")

observed_ccd_year <- project_bbl_assigned |>
  left_join(
    district_lookup |>
      select(district_id, homeowner_tercile, homeowner_tercile_label),
    by = "district_id",
    relationship = "many-to-one"
  ) |>
  group_by(district_id, completed_year) |>
  summarize(
    upzoning_project_count = sum(project_assignment_weight[rezoning_direction == "upzoning"], na.rm = TRUE),
    downzoning_project_count = sum(project_assignment_weight[rezoning_direction == "downzoning"], na.rm = TRUE),
    mixed_project_count = sum(project_assignment_weight[rezoning_direction == "mixed"], na.rm = TRUE),
    no_material_project_count = sum(project_assignment_weight[rezoning_direction == "no_material_residential_change"], na.rm = TRUE),
    unknown_project_count = sum(project_assignment_weight[rezoning_direction == "unknown"], na.rm = TRUE),
    commercial_overlay_project_count = sum(project_assignment_weight * as.integer(commercial_overlay_project_flag), na.rm = TRUE),
    commercial_overlay_unknown_project_count = sum(project_assignment_weight * as.integer(commercial_overlay_unknown_flag), na.rm = TRUE),
    c1_c2_project_count = sum(project_assignment_weight * as.integer(c1_c2_project_flag), na.rm = TRUE),
    mixed_use_text_project_count = sum(project_assignment_weight * as.integer(mixed_use_text_flag), na.rm = TRUE),
    mixed_use_unknown_project_count = sum(project_assignment_weight * as.integer(mixed_use_unknown_flag), na.rm = TRUE),
    affected_bbl_count = n_distinct(bbl_standardized),
    affected_lot_acres = sum(lot_acres, na.rm = TRUE),
    affected_current_residential_lot_acres = sum(current_residential_lot_acres, na.rm = TRUE),
    gross_up_far_acres = sum(gross_up_far_acres, na.rm = TRUE),
    gross_down_far_acres = sum(gross_down_far_acres, na.rm = TRUE),
    net_far_acres = sum(net_far_acres, na.rm = TRUE),
    high_confidence_gross_up_far_acres = sum(if_else(high_confidence_scope_flag, gross_up_far_acres, 0), na.rm = TRUE),
    high_confidence_gross_down_far_acres = sum(if_else(high_confidence_scope_flag, gross_down_far_acres, 0), na.rm = TRUE),
    high_confidence_net_far_acres = sum(if_else(high_confidence_scope_flag, net_far_acres, 0), na.rm = TRUE),
    .groups = "drop"
  ) |>
  rename(year = completed_year)

ccd_year_panel <- expand_grid(
  district_lookup,
  year = 1980:2025
) |>
  left_join(observed_ccd_year, by = c("district_id", "year"), relationship = "one-to-one") |>
  mutate(
    across(
      c(
        upzoning_project_count,
        downzoning_project_count,
        mixed_project_count,
        no_material_project_count,
        unknown_project_count,
        commercial_overlay_project_count,
        commercial_overlay_unknown_project_count,
        c1_c2_project_count,
        mixed_use_text_project_count,
        mixed_use_unknown_project_count,
        affected_bbl_count,
        affected_lot_acres,
        affected_current_residential_lot_acres,
        gross_up_far_acres,
        gross_down_far_acres,
        net_far_acres,
        high_confidence_gross_up_far_acres,
        high_confidence_gross_down_far_acres,
        high_confidence_net_far_acres
      ),
      ~ coalesce(.x, 0)
    ),
    event_period = factor(event_period_from_year(year), levels = event_periods),
    upzoning_project_count_per_10000 = 10000 * upzoning_project_count / occupied_units_1990,
    downzoning_project_count_per_10000 = 10000 * downzoning_project_count / occupied_units_1990,
    commercial_overlay_project_count_per_10000 = 10000 * commercial_overlay_project_count / occupied_units_1990,
    commercial_overlay_unknown_project_count_per_10000 = 10000 * commercial_overlay_unknown_project_count / occupied_units_1990,
    mixed_use_text_project_count_per_10000 = 10000 * mixed_use_text_project_count / occupied_units_1990,
    mixed_use_unknown_project_count_per_10000 = 10000 * mixed_use_unknown_project_count / occupied_units_1990,
    gross_up_far_acres_per_residential_acre = gross_up_far_acres / residential_acres,
    gross_down_far_acres_per_residential_acre = gross_down_far_acres / residential_acres,
    net_far_acres_per_residential_acre = net_far_acres / residential_acres,
    high_confidence_gross_up_far_acres_per_residential_acre = high_confidence_gross_up_far_acres / residential_acres,
    high_confidence_gross_down_far_acres_per_residential_acre = high_confidence_gross_down_far_acres / residential_acres,
    high_confidence_net_far_acres_per_residential_acre = high_confidence_net_far_acres / residential_acres
  ) |>
  arrange(district_id, year)

if (nrow(ccd_year_panel) != 51 * 46 || nrow(ccd_year_panel) != nrow(distinct(ccd_year_panel, district_id, year))) {
  stop("CCD-year rezoning direction panel must be unique and complete by 51 districts and 46 years.")
}

write_csv_if_changed(ccd_year_panel, "../output/zap_rezoning_direction_ccd_year_panel.csv")

tercile_year <- expand_grid(
  year = 1980:2025,
  tercile_denominators,
  rezoning_direction = factor(direction_levels, levels = direction_levels),
  magnitude_bin = factor(magnitude_levels, levels = magnitude_levels)
) |>
  mutate(event_period = event_period_from_year(year)) |>
  left_join(
    project_bbl_assigned |>
      left_join(
        district_lookup |>
          select(district_id, homeowner_tercile, homeowner_tercile_label),
        by = "district_id",
        relationship = "many-to-one"
      ) |>
      group_by(
        year = completed_year,
        event_period,
        homeowner_tercile,
        homeowner_tercile_label,
        rezoning_direction,
        magnitude_bin
      ) |>
      summarize(
        project_count = sum(project_assignment_weight, na.rm = TRUE),
        affected_bbl_count = n_distinct(bbl_standardized),
        affected_lot_acres = sum(lot_acres, na.rm = TRUE),
        affected_current_residential_lot_acres = sum(current_residential_lot_acres, na.rm = TRUE),
        gross_up_far_acres = sum(gross_up_far_acres, na.rm = TRUE),
        gross_down_far_acres = sum(gross_down_far_acres, na.rm = TRUE),
        net_far_acres = sum(net_far_acres, na.rm = TRUE),
        high_confidence_gross_up_far_acres = sum(if_else(high_confidence_scope_flag, gross_up_far_acres, 0), na.rm = TRUE),
        high_confidence_gross_down_far_acres = sum(if_else(high_confidence_scope_flag, gross_down_far_acres, 0), na.rm = TRUE),
        high_confidence_net_far_acres = sum(if_else(high_confidence_scope_flag, net_far_acres, 0), na.rm = TRUE),
        .groups = "drop"
      ),
    by = c("year", "event_period", "homeowner_tercile", "homeowner_tercile_label", "rezoning_direction", "magnitude_bin"),
    relationship = "one-to-one"
  ) |>
  mutate(
    across(c(project_count, affected_bbl_count, affected_lot_acres, affected_current_residential_lot_acres, gross_up_far_acres, gross_down_far_acres, net_far_acres, high_confidence_gross_up_far_acres, high_confidence_gross_down_far_acres, high_confidence_net_far_acres), ~ coalesce(.x, 0)),
    project_count_per_10000 = 10000 * project_count / occupied_units_1990,
    affected_bbl_count_per_10000 = 10000 * affected_bbl_count / occupied_units_1990,
    affected_lot_acres_per_residential_acre = affected_lot_acres / residential_acres,
    affected_current_residential_lot_acres_per_residential_acre = affected_current_residential_lot_acres / residential_acres,
    gross_up_far_acres_per_residential_acre = gross_up_far_acres / residential_acres,
    gross_down_far_acres_per_residential_acre = gross_down_far_acres / residential_acres,
    net_far_acres_per_residential_acre = net_far_acres / residential_acres,
    high_confidence_gross_up_far_acres_per_residential_acre = high_confidence_gross_up_far_acres / residential_acres,
    high_confidence_gross_down_far_acres_per_residential_acre = high_confidence_gross_down_far_acres / residential_acres,
    high_confidence_net_far_acres_per_residential_acre = high_confidence_net_far_acres / residential_acres
  ) |>
  arrange(homeowner_tercile, year, rezoning_direction, magnitude_bin)

write_csv_if_changed(tercile_year, "../output/zap_rezoning_direction_tercile_year.csv")

# Exploratory event-study and long-difference estimates.

control_lookup <- district_lookup |>
  transmute(
    district_id,
    borough_code,
    borough_name,
    log_occupied_units_1990 = log(occupied_units_1990),
    median_household_income_1990,
    vacancy_rate_1990
  ) |>
  group_by(borough_code, borough_name) |>
  mutate(
    log_occupied_units_1990_z = z_score(log_occupied_units_1990),
    vacancy_rate_1990_z = z_score(vacancy_rate_1990),
    median_household_income_1990_z = z_score(median_household_income_1990)
  ) |>
  ungroup() |>
  select(district_id, log_occupied_units_1990_z, vacancy_rate_1990_z, median_household_income_1990_z)

event_design_base <- ccd_year_panel |>
  left_join(control_lookup, by = "district_id", relationship = "many-to-one") |>
  mutate(
    log_occupied_units_1990_z = coalesce(log_occupied_units_1990_z, 0),
    vacancy_rate_1990_z = coalesce(vacancy_rate_1990_z, 0),
    median_household_income_1990_z = coalesce(median_household_income_1990_z, 0),
    borough_period = interaction(borough_code, event_period, drop = TRUE)
  )

outcome_dictionary <- tribble(
  ~outcome_var, ~outcome_label, ~outcome_scale,
  "upzoning_project_count_per_10000", "Upzoning project count", "per_10000_occupied_1990",
  "downzoning_project_count_per_10000", "Downzoning project count", "per_10000_occupied_1990",
  "gross_up_far_acres_per_residential_acre", "Gross up FAR-acres", "per_residential_acre",
  "gross_down_far_acres_per_residential_acre", "Gross down FAR-acres", "per_residential_acre",
  "net_far_acres_per_residential_acre", "Net FAR-acres", "per_residential_acre"
)

control_vars <- c("log_occupied_units_1990_z", "median_household_income_1990_z", "vacancy_rate_1990_z", "pre_1980_1988_rate_z")

event_design_long <- bind_rows(lapply(seq_len(nrow(outcome_dictionary)), function(i) {
  outcome_row <- outcome_dictionary[i, ]

  event_design_base |>
    transmute(
      district_id,
      council_district,
      borough_code,
      borough_name,
      year,
      event_period,
      borough_period,
      occupied_units_1990,
      residential_acres,
      treat_z_boro,
      log_occupied_units_1990_z,
      median_household_income_1990_z,
      vacancy_rate_1990_z,
      outcome_var = outcome_row$outcome_var,
      outcome_label = outcome_row$outcome_label,
      outcome_scale = outcome_row$outcome_scale,
      outcome_value = .data[[outcome_row$outcome_var]]
    )
}))

pre_rate_df <- event_design_long |>
  filter(year >= 1980, year <= 1988) |>
  group_by(outcome_var, district_id) |>
  summarize(pre_1980_1988_rate = mean(outcome_value, na.rm = TRUE), .groups = "drop") |>
  group_by(outcome_var) |>
  mutate(pre_1980_1988_rate_z = z_score(pre_1980_1988_rate)) |>
  ungroup() |>
  select(outcome_var, district_id, pre_1980_1988_rate_z)

event_design_long <- event_design_long |>
  left_join(pre_rate_df, by = c("outcome_var", "district_id"), relationship = "many-to-one") |>
  mutate(pre_1980_1988_rate_z = coalesce(pre_1980_1988_rate_z, 0))

event_rows <- list()

for (i in seq_len(nrow(outcome_dictionary))) {
  outcome_row <- outcome_dictionary[i, ]
  model_df <- event_design_long |>
    filter(outcome_var == outcome_row$outcome_var)

  for (period_value in estimated_event_periods) {
    model_df[[paste0("treat_z_boro_x_", sanitize_period(period_value))]] <-
      model_df$treat_z_boro * as.integer(as.character(model_df$event_period) == period_value)

    for (control_var in control_vars) {
      model_df[[paste0(control_var, "_x_", sanitize_period(period_value))]] <-
        model_df[[control_var]] * as.integer(as.character(model_df$event_period) == period_value)
    }
  }

  treat_terms <- paste0("treat_z_boro_x_", sanitize_period(estimated_event_periods))
  control_terms <- unlist(lapply(control_vars, function(control_var) paste0(control_var, "_x_", sanitize_period(estimated_event_periods))))

  event_model <- feols(
    as.formula(paste0("outcome_value ~ ", paste(c(treat_terms, control_terms), collapse = " + "), " | district_id + borough_period")),
    cluster = ~district_id,
    data = model_df
  )

  event_rows[[outcome_row$outcome_var]] <- bind_rows(
    tibble(
      term = NA_character_,
      event_period = reference_event_period,
      is_reference = TRUE,
      estimate = 0,
      std_error = NA_real_,
      statistic = NA_real_,
      p_value = NA_real_,
      conf_low = NA_real_,
      conf_high = NA_real_
    ),
    extract_model_terms(
      event_model,
      tibble(term = treat_terms, event_period = estimated_event_periods, is_reference = FALSE)
    )
  ) |>
    mutate(
      event_period = factor(event_period, levels = event_periods),
      event_period_index = match(as.character(event_period), event_periods),
      outcome_var = outcome_row$outcome_var,
      outcome_label = outcome_row$outcome_label,
      outcome_scale = outcome_row$outcome_scale,
      reference_period = reference_event_period,
      model = "exploratory_district_fe_borough_period_fe_controls",
      control_label = "log occupied units + median income + vacancy + pre-period outcome",
      n_obs = model_nobs(event_model),
      within_r2 = tryCatch(as.numeric(r2(event_model, type = "wr2")), error = function(e) NA_real_)
    )
}

event_df <- bind_rows(event_rows) |>
  arrange(outcome_var, event_period)

missing_event_terms <- event_df |>
  filter(!is_reference, is.na(estimate)) |>
  nrow()

write_csv_if_changed(
  event_design_long |>
    select(
      outcome_var,
      outcome_label,
      outcome_scale,
      district_id,
      council_district,
      borough_code,
      borough_name,
      year,
      event_period,
      outcome_value,
      occupied_units_1990,
      residential_acres,
      treat_z_boro,
      log_occupied_units_1990_z,
      median_household_income_1990_z,
      vacancy_rate_1990_z,
      pre_1980_1988_rate_z
    ) |>
    arrange(outcome_var, district_id, year),
  "../output/zap_rezoning_direction_event_design_panel.csv"
)
write_csv_if_changed(event_df, "../output/zap_rezoning_direction_event_coefficients_5yr_bins.csv")

pdf("../output/zap_rezoning_direction_event_coefficients_5yr_bins.pdf", width = 11, height = 8.5)
print(
  ggplot(event_df, aes(x = event_period_index, y = estimate)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "#666666", linewidth = 0.35) +
    geom_errorbar(
      data = filter(event_df, !is_reference),
      aes(ymin = conf_low, ymax = conf_high),
      width = 0.12,
      linewidth = 0.45,
      color = "#3B6EA8"
    ) +
    geom_line(color = "#3B6EA8", linewidth = 0.75) +
    geom_point(color = "#3B6EA8", size = 1.8) +
    facet_wrap(~ outcome_label, scales = "free_y", ncol = 2) +
    scale_x_continuous(breaks = seq_along(event_periods), labels = event_periods) +
    labs(
      title = "Exploratory rezoning direction and scope event studies",
      x = NULL,
      y = "Coefficient on homeowner exposure"
    ) +
    theme_minimal(base_size = 10) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

window_defs <- tribble(
  ~comparison_id, ~pre_start, ~pre_end, ~post_start, ~post_end,
  "placebo_1985_1989_minus_1980_1984", 1980L, 1984L, 1985L, 1989L,
  "post_1990_1999_minus_1980_1988", 1980L, 1988L, 1990L, 1999L,
  "post_2000_2009_minus_1980_1988", 1980L, 1988L, 2000L, 2009L,
  "post_2010_2019_minus_1980_1988", 1980L, 1988L, 2010L, 2019L,
  "post_2020_2025_minus_1980_1988", 1980L, 1988L, 2020L, 2025L
) |>
  mutate(
    pre_window = paste0(pre_start, "-", pre_end),
    post_window = paste0(post_start, "-", post_end)
  )

long_diff_rows <- list()

for (i in seq_len(nrow(outcome_dictionary))) {
  outcome_row <- outcome_dictionary[i, ]
  outcome_design <- event_design_long |>
    filter(outcome_var == outcome_row$outcome_var)

  for (j in seq_len(nrow(window_defs))) {
    window_row <- window_defs[j, ]

    pre_df <- outcome_design |>
      filter(year >= window_row$pre_start, year <= window_row$pre_end) |>
      group_by(district_id) |>
      summarize(pre_avg = mean(outcome_value, na.rm = TRUE), pre_year_count = n_distinct(year), .groups = "drop")

    post_df <- outcome_design |>
      filter(year >= window_row$post_start, year <= window_row$post_end) |>
      group_by(district_id) |>
      summarize(post_avg = mean(outcome_value, na.rm = TRUE), post_year_count = n_distinct(year), .groups = "drop")

    diff_df <- outcome_design |>
      distinct(
        district_id,
        borough_code,
        borough_name,
        treat_z_boro,
        log_occupied_units_1990_z,
        median_household_income_1990_z,
        vacancy_rate_1990_z,
        pre_1980_1988_rate_z
      ) |>
      left_join(pre_df, by = "district_id", relationship = "one-to-one") |>
      left_join(post_df, by = "district_id", relationship = "one-to-one") |>
      mutate(delta_value = post_avg - pre_avg)

    model_df <- diff_df |>
      select(delta_value, pre_avg, treat_z_boro, borough_code, all_of(control_vars)) |>
      filter(if_all(everything(), ~ !is.na(.x)))

    long_model <- feols(
      as.formula(paste0("delta_value ~ treat_z_boro + ", paste(control_vars, collapse = " + "), " | borough_code")),
      data = model_df,
      vcov = "hetero"
    )

    long_diff_rows[[paste(outcome_row$outcome_var, window_row$comparison_id, sep = "__")]] <-
      extract_model_terms(long_model, tibble(term = "treat_z_boro")) |>
      transmute(
        comparison_id = window_row$comparison_id,
        pre_window = window_row$pre_window,
        post_window = window_row$post_window,
        outcome_var = outcome_row$outcome_var,
        outcome_label = outcome_row$outcome_label,
        outcome_scale = outcome_row$outcome_scale,
        term,
        estimate,
        std_error,
        statistic,
        p_value,
        conf_low,
        conf_high,
        n_districts = model_nobs(long_model),
        initial_outcome_mean = mean(model_df$pre_avg),
        pre_year_count_min = min(diff_df$pre_year_count, na.rm = TRUE),
        post_year_count_min = min(diff_df$post_year_count, na.rm = TRUE),
        model = "exploratory_long_difference_borough_fe_controls"
      )
  }
}

long_diff_df <- bind_rows(long_diff_rows) |>
  mutate(
    row_order = match(outcome_var, outcome_dictionary$outcome_var),
    column_order = case_when(
      comparison_id == "placebo_1985_1989_minus_1980_1984" ~ 1L,
      comparison_id == "post_1990_1999_minus_1980_1988" ~ 2L,
      comparison_id == "post_2000_2009_minus_1980_1988" ~ 3L,
      comparison_id == "post_2010_2019_minus_1980_1988" ~ 4L,
      comparison_id == "post_2020_2025_minus_1980_1988" ~ 5L,
      TRUE ~ NA_integer_
    ),
    column_label = case_when(
      comparison_id == "placebo_1985_1989_minus_1980_1984" ~ "Placebo",
      comparison_id == "post_1990_1999_minus_1980_1988" ~ "1990--1999",
      comparison_id == "post_2000_2009_minus_1980_1988" ~ "2000--2009",
      comparison_id == "post_2010_2019_minus_1980_1988" ~ "2010--2019",
      comparison_id == "post_2020_2025_minus_1980_1988" ~ "2020--2025",
      TRUE ~ comparison_id
    ),
    estimate_label = paste0(format_decimal(estimate, 3), significance_stars(p_value)),
    std_error_label = format_decimal(std_error, 3),
    p_value_label = format_p_value(p_value)
  ) |>
  arrange(row_order, column_order)

if (nrow(long_diff_df) != nrow(outcome_dictionary) * nrow(window_defs)) {
  stop("Long-difference estimates expected one row per outcome and window.")
}

write_csv_if_changed(long_diff_df, "../output/zap_rezoning_direction_long_difference_estimates.csv")

long_diff_wide <- long_diff_df |>
  select(outcome_label, row_order, column_order, column_label, estimate_label, std_error_label) |>
  arrange(row_order, column_order) |>
  mutate(cell_label = paste0(estimate_label, " (", std_error_label, ")")) |>
  select(outcome_label, column_label, cell_label) |>
  pivot_wider(names_from = column_label, values_from = cell_label) |>
  arrange(match(outcome_label, outcome_dictionary$outcome_label))

long_diff_table_outcomes <- c(
  "Upzoning project count",
  "Downzoning project count",
  "Gross up FAR-acres",
  "Gross down FAR-acres",
  "Net FAR-acres"
)

long_diff_table_rows <- vapply(long_diff_table_outcomes, function(label) {
  values <- long_diff_wide |>
    filter(outcome_label == label) |>
    select(Placebo, `1990--1999`, `2000--2009`, `2010--2019`, `2020--2025`) |>
    unlist(use.names = FALSE)

  regression_table_row(label, values)
}, character(1))

table_lines <- c(
  "\\begin{table}[htbp]",
  "    \\centering",
  "    \\begin{threeparttable}",
  "    \\caption{Exploratory Long-Difference Estimates for Rezoning Direction and Scope}",
  "    \\label{tab:zap_rezoning_direction_long_difference}",
  "    \\small",
  "    \\begin{tabular}{lccccc}",
  "    \\toprule",
  regression_table_row("", c("Placebo", "1990--1999", "2000--2009", "2010--2019", "2020--2025")),
  "    \\midrule",
  long_diff_table_rows,
  "    \\bottomrule",
  "    \\end{tabular}",
  "    \\begin{tablenotes}[flushleft]",
  "    \\footnotesize",
  "    \\item \\textit{Notes:} Cells report exploratory coefficients on within-borough standardized 1990 homeownership, with heteroskedasticity-robust standard errors in parentheses. Count outcomes are measured per 10,000 occupied units in 1990. FAR-acre outcomes are measured per baseline residential acre. These parser-based estimates should be interpreted only after reviewing parse and BBL-scope coverage.",
  "    \\end{tablenotes}",
  "    \\end{threeparttable}",
  "\\end{table}"
)

write_lines_if_changed(table_lines, "../output/zap_rezoning_direction_long_difference.tex")

# Coverage, manual-review queues, and parser diagnostics.

parse_coverage <- project_classification |>
  group_by(event_period, rezoning_direction, parse_status) |>
  summarize(project_count = n_distinct(project_id), .groups = "drop") |>
  mutate(event_period = factor(event_period, levels = event_periods)) |>
  arrange(event_period, rezoning_direction, parse_status)

coverage_city <- project_classification |>
  group_by(event_period) |>
  summarize(
    group_type = "city",
    group_value = "Citywide",
    project_count = n(),
    parsed_pair_project_count = sum(parsed_pair_count > 0),
    known_direction_project_count = sum(rezoning_direction != "unknown"),
    unknown_project_count = sum(rezoning_direction == "unknown"),
    strict_scope_project_count = sum(strict_bbl_scope_flag),
    high_confidence_scope_project_count = sum(high_confidence_scope_flag),
    mean_strict_bbl_match_share = mean(strict_bbl_match_share, na.rm = TRUE),
    .groups = "drop"
  )

coverage_borough <- project_classification |>
  group_by(event_period, borough_name_standardized) |>
  summarize(
    group_type = "borough",
    group_value = first(borough_name_standardized),
    project_count = n(),
    parsed_pair_project_count = sum(parsed_pair_count > 0),
    known_direction_project_count = sum(rezoning_direction != "unknown"),
    unknown_project_count = sum(rezoning_direction == "unknown"),
    strict_scope_project_count = sum(strict_bbl_scope_flag),
    high_confidence_scope_project_count = sum(high_confidence_scope_flag),
    mean_strict_bbl_match_share = mean(strict_bbl_match_share, na.rm = TRUE),
    .groups = "drop"
  ) |>
  select(-borough_name_standardized)

coverage_direction <- project_classification |>
  group_by(event_period, rezoning_direction) |>
  summarize(
    group_type = "direction",
    group_value = as.character(first(rezoning_direction)),
    project_count = n(),
    parsed_pair_project_count = sum(parsed_pair_count > 0),
    known_direction_project_count = sum(rezoning_direction != "unknown"),
    unknown_project_count = sum(rezoning_direction == "unknown"),
    strict_scope_project_count = sum(strict_bbl_scope_flag),
    high_confidence_scope_project_count = sum(high_confidence_scope_flag),
    mean_strict_bbl_match_share = mean(strict_bbl_match_share, na.rm = TRUE),
    .groups = "drop"
  ) |>
  select(-rezoning_direction)

coverage_borough_direction <- project_classification |>
  group_by(event_period, borough_name_standardized, rezoning_direction) |>
  summarize(
    group_type = "borough_direction",
    group_value = paste(first(borough_name_standardized), as.character(first(rezoning_direction)), sep = ": "),
    project_count = n(),
    parsed_pair_project_count = sum(parsed_pair_count > 0),
    known_direction_project_count = sum(rezoning_direction != "unknown"),
    unknown_project_count = sum(rezoning_direction == "unknown"),
    strict_scope_project_count = sum(strict_bbl_scope_flag),
    high_confidence_scope_project_count = sum(high_confidence_scope_flag),
    mean_strict_bbl_match_share = mean(strict_bbl_match_share, na.rm = TRUE),
    .groups = "drop"
  ) |>
  select(-borough_name_standardized, -rezoning_direction)

project_tercile_assignment <- project_bbl_assigned |>
  left_join(
    district_lookup |>
      select(district_id, homeowner_tercile, homeowner_tercile_label),
    by = "district_id",
    relationship = "many-to-one"
  ) |>
  filter(!is.na(homeowner_tercile_label)) |>
  group_by(project_id, homeowner_tercile, homeowner_tercile_label) |>
  summarize(project_tercile_weight = sum(project_assignment_weight, na.rm = TRUE), .groups = "drop")

observed_tercile_diagnostic_year <- project_tercile_assignment |>
  left_join(
    project_classification |>
      select(
        project_id,
        completed_year,
        event_period,
        rezoning_direction,
        commercial_overlay_project_flag,
        commercial_overlay_unknown_flag,
        c1_c2_project_flag,
        mixed_use_text_flag,
        mixed_use_unknown_flag
      ),
    by = "project_id",
    relationship = "many-to-one"
  ) |>
  group_by(completed_year, event_period, homeowner_tercile, homeowner_tercile_label) |>
  summarize(
    project_count = sum(project_tercile_weight, na.rm = TRUE),
    unknown_project_count = sum(project_tercile_weight * as.integer(rezoning_direction == "unknown"), na.rm = TRUE),
    commercial_overlay_project_count = sum(project_tercile_weight * as.integer(commercial_overlay_project_flag), na.rm = TRUE),
    commercial_overlay_unknown_project_count = sum(project_tercile_weight * as.integer(commercial_overlay_unknown_flag), na.rm = TRUE),
    c1_c2_project_count = sum(project_tercile_weight * as.integer(c1_c2_project_flag), na.rm = TRUE),
    mixed_use_text_project_count = sum(project_tercile_weight * as.integer(mixed_use_text_flag), na.rm = TRUE),
    mixed_use_unknown_project_count = sum(project_tercile_weight * as.integer(mixed_use_unknown_flag), na.rm = TRUE),
    .groups = "drop"
  ) |>
  rename(year = completed_year)

tercile_diagnostic_year <- expand_grid(
  year = 1980:2025,
  tercile_denominators
) |>
  mutate(event_period = factor(event_period_from_year(year), levels = event_periods)) |>
  left_join(
    observed_tercile_diagnostic_year,
    by = c("year", "event_period", "homeowner_tercile", "homeowner_tercile_label"),
    relationship = "one-to-one"
  ) |>
  mutate(
    across(
      c(
        project_count,
        unknown_project_count,
        commercial_overlay_project_count,
        commercial_overlay_unknown_project_count,
        c1_c2_project_count,
        mixed_use_text_project_count,
        mixed_use_unknown_project_count
      ),
      ~ coalesce(.x, 0)
    ),
    project_count_per_10000 = 10000 * project_count / occupied_units_1990,
    unknown_project_count_per_10000 = 10000 * unknown_project_count / occupied_units_1990,
    commercial_overlay_project_count_per_10000 = 10000 * commercial_overlay_project_count / occupied_units_1990,
    commercial_overlay_unknown_project_count_per_10000 = 10000 * commercial_overlay_unknown_project_count / occupied_units_1990,
    mixed_use_text_project_count_per_10000 = 10000 * mixed_use_text_project_count / occupied_units_1990,
    mixed_use_unknown_project_count_per_10000 = 10000 * mixed_use_unknown_project_count / occupied_units_1990
  ) |>
  arrange(homeowner_tercile, year)

coverage_tercile_direction <- project_tercile_assignment |>
  left_join(
    project_classification |>
      select(
        project_id,
        event_period,
        rezoning_direction,
        parsed_pair_count,
        strict_bbl_scope_flag,
        high_confidence_scope_flag,
        strict_bbl_match_share
      ),
    by = "project_id",
    relationship = "many-to-one"
  ) |>
  group_by(event_period, homeowner_tercile_label, rezoning_direction) |>
  summarize(
    group_type = "homeowner_tercile_direction_strict_scope_only",
    group_value = paste(first(homeowner_tercile_label), as.character(first(rezoning_direction)), sep = ": "),
    project_count = sum(project_tercile_weight, na.rm = TRUE),
    parsed_pair_project_count = sum(project_tercile_weight * as.integer(parsed_pair_count > 0), na.rm = TRUE),
    known_direction_project_count = sum(project_tercile_weight * as.integer(rezoning_direction != "unknown"), na.rm = TRUE),
    unknown_project_count = sum(project_tercile_weight * as.integer(rezoning_direction == "unknown"), na.rm = TRUE),
    strict_scope_project_count = sum(project_tercile_weight * as.integer(strict_bbl_scope_flag), na.rm = TRUE),
    high_confidence_scope_project_count = sum(project_tercile_weight * as.integer(high_confidence_scope_flag), na.rm = TRUE),
    mean_strict_bbl_match_share = mean(strict_bbl_match_share, na.rm = TRUE),
    .groups = "drop"
  ) |>
  select(-homeowner_tercile_label, -rezoning_direction)

coverage_by_period_group <- bind_rows(
  coverage_city,
  coverage_borough,
  coverage_direction,
  coverage_borough_direction,
  coverage_tercile_direction
) |>
  mutate(
    event_period = factor(event_period, levels = event_periods),
    known_direction_share = if_else(project_count > 0, known_direction_project_count / project_count, NA_real_),
    unknown_project_share = if_else(project_count > 0, unknown_project_count / project_count, NA_real_),
    strict_scope_share = if_else(project_count > 0, strict_scope_project_count / project_count, NA_real_),
    high_confidence_scope_share = if_else(project_count > 0, high_confidence_scope_project_count / project_count, NA_real_)
  ) |>
  arrange(event_period, group_type, group_value)

unrecognized_codes <- bind_rows(
  pair_df |>
    filter(is.na(from_resid_far)) |>
    transmute(zoning_code = from_zoning_code, code_position = "from", far_source_note = from_far_source_note, project_id),
  pair_df |>
    filter(is.na(to_resid_far)) |>
    transmute(zoning_code = to_zoning_code, code_position = "to", far_source_note = to_far_source_note, project_id)
) |>
  filter(!is.na(zoning_code), zoning_code != "") |>
  mutate(
    far_issue = if_else(
      !is.na(far_source_note),
      "lookup_without_standalone_residential_far",
      "not_in_lookup_or_fallback"
    )
  ) |>
  count(zoning_code, code_position, far_issue, far_source_note, name = "use_count") |>
  arrange(desc(use_count), zoning_code, code_position, far_issue)

manual_review_scope_threshold <- quantile(
  project_classification$abs_net_far_acres[project_classification$strict_bbl_scope_flag],
  0.95,
  na.rm = TRUE
)

if (is.na(manual_review_scope_threshold)) {
  manual_review_scope_threshold <- Inf
}

source_lookup_acre_threshold <- quantile(
  project_classification$affected_lot_acres[project_classification$rezoning_direction == "unknown" & project_classification$strict_bbl_scope_flag],
  0.90,
  na.rm = TRUE
)

if (is.na(source_lookup_acre_threshold)) {
  source_lookup_acre_threshold <- Inf
}

project_classification_review_flags <- project_classification |>
  mutate(
    nonhousing_review_text_flag = str_detect(
      coalesce(project_text, ""),
      "PARKING|GARAGE|CSO|RETENTION FACILITY|WATER QUALITY|LABORATORY|LIFE SCIENCES|HOSPITAL|UNIVERSITY|COLLEGE|SCHOOL|LIBRARY|SANITATION|POLICE|FIRE|HOTEL|CONVENTION|COMMERCIAL|RETAIL|AUTO|INDUSTRIAL|MANUFACTURING"
    ),
    housing_review_text_flag = str_detect(
      coalesce(project_text, ""),
      "HOUSING|RESIDENTIAL|DWELLING|DWELLING UNITS|APARTMENT|INCLUSIONARY|\\bMIH\\b|AFFORDABLE"
    ),
    restrictive_review_text_flag = str_detect(
      coalesce(project_text, ""),
      "LOWER[- ]DENSITY|CONTEXTUAL|PRESERV(E|ING)|PROTECT|NEIGHBORHOOD CHARACTER|OUT[- ]OF[- ]CHARACTER|INAPPROPRIATE DEVELOPMENT|OVERDEVELOPMENT|LOW[- ]RISE|DETACHED|ONE[- ]FAMILY|TWO[- ]FAMILY"
    ),
    housing_growth_review_text_flag = str_detect(
      coalesce(project_text, ""),
      "FACILITATE.{0,80}(HOUSING|RESIDENTIAL|DWELLING|APARTMENT|MIXED[- ]USE)|CONSTRUCT.{0,80}(HOUSING|RESIDENTIAL|DWELLING|APARTMENT|MIXED[- ]USE)|DEVELOP.{0,80}(HOUSING|RESIDENTIAL|DWELLING|APARTMENT|MIXED[- ]USE)|NEW HOUSING|AFFORDABLE HOUSING|INCLUSIONARY|\\bMIH\\b"
    ),
    text_candidate_direction = case_when(
      rezoning_direction != "unknown" ~ "assigned_direction",
      restrictive_review_text_flag & (housing_growth_review_text_flag | increased_residential_proxy | mixed_use_text_flag) ~ "candidate_mixed_or_conflicting",
      restrictive_review_text_flag ~ "candidate_downzoning",
      mixed_use_text_flag & (housing_review_text_flag | increased_residential_proxy) ~ "candidate_mixed_or_upzoning",
      housing_growth_review_text_flag | increased_residential_proxy ~ "candidate_upzoning",
      commercial_overlay_unknown_flag ~ "candidate_overlay_ambiguous",
      urban_renewal_special_district_text_flag ~ "candidate_special_district_needs_source",
      text_zoning_code_count > 0 ~ "candidate_code_text_needs_parser_or_source",
      TRUE ~ "no_local_text_candidate"
    ),
    text_candidate_confidence = case_when(
      rezoning_direction != "unknown" ~ "not_unresolved",
      text_candidate_direction %in% c("candidate_downzoning", "candidate_upzoning") &
        text_zoning_code_count > 0 ~ "medium",
      text_candidate_direction %in% c("candidate_downzoning", "candidate_upzoning") ~ "low",
      text_candidate_direction %in% c("candidate_mixed_or_upzoning", "candidate_mixed_or_conflicting") ~ "medium",
      text_candidate_direction == "candidate_code_text_needs_parser_or_source" ~ "medium",
      TRUE ~ "low"
    ),
    text_candidate_basis = str_squish(paste(
      if_else(restrictive_review_text_flag, "restrictive_text", ""),
      if_else(housing_growth_review_text_flag, "housing_growth_text", ""),
      if_else(housing_review_text_flag, "housing_text", ""),
      if_else(mixed_use_text_flag, "mixed_use_text", ""),
      if_else(increased_residential_proxy, "increased_residential_proxy", ""),
      if_else(commercial_overlay_unknown_flag, "commercial_overlay_unknown", ""),
      if_else(urban_renewal_special_district_text_flag, "urban_renewal_or_special_district", ""),
      if_else(text_zoning_code_count > 0, "zoning_codes_present", "")
    )),
    source_lookup_priority = case_when(
      rezoning_direction != "unknown" ~ "not_unresolved",
      housing_review_text_flag |
        mixed_use_text_flag |
        increased_residential_proxy |
        affected_lot_acres >= source_lookup_acre_threshold ~ "high",
      text_zoning_code_count > 0 |
        commercial_overlay_unknown_flag |
        urban_renewal_special_district_text_flag ~ "medium",
      TRUE ~ "low"
    ),
    review_reason = str_squish(paste(
      if_else(manual_label_flag, "manual_label_seed", ""),
      if_else(rezoning_direction %in% c("unknown", "mixed"), as.character(rezoning_direction), ""),
      if_else(parse_status != "parsed_known_far", parse_status, ""),
      if_else(strict_bbl_scope_flag & abs_net_far_acres >= manual_review_scope_threshold, "top_scope_change", ""),
      if_else(strict_bbl_scope_flag & !high_confidence_scope_flag, "low_bbl_match_quality", ""),
      if_else(nonhousing_review_text_flag & rezoning_direction %in% c("upzoning", "downzoning", "mixed"), "possible_nonhousing_intent", ""),
      if_else(housing_review_text_flag & rezoning_direction == "unknown", "housing_text_unknown_direction", ""),
      if_else(source_lookup_priority == "high", "high_source_lookup_priority", "")
    ))
  )

top_abs_far_acres <- project_classification |>
  filter(strict_bbl_scope_flag, !is.na(project_net_far_delta)) |>
  arrange(desc(abs_net_far_acres), project_id) |>
  slice_head(n = 50) |>
  select(
    project_id,
    completed_year,
    project_name,
    project_brief,
    borough_name_standardized,
    rezoning_direction,
    magnitude_bin,
    parsed_zoning_changes,
    project_net_far_delta,
    affected_lot_acres,
    gross_up_far_acres,
    gross_down_far_acres,
    net_far_acres,
    abs_net_far_acres
  )

manual_review <- project_classification_review_flags |>
  filter(review_reason != "") |>
  arrange(desc(abs_net_far_acres), completed_year, project_id) |>
  select(
    project_id,
    completed_year,
    project_name,
    project_brief,
    borough_name_standardized,
    review_reason,
    rezoning_direction,
    magnitude_bin,
    parse_status,
    initial_parse_status,
    initial_rezoning_direction,
    classification_source_tier,
    source_lookup_priority,
    text_candidate_direction,
    text_candidate_confidence,
    text_candidate_basis,
    parsed_zoning_changes,
    parser_stages,
    ignored_commercial_overlay_codes,
    ignored_mixed_use_component_codes,
    unrecognized_zoning_codes,
    strict_assigned_bbl_count,
    affected_lot_acres,
    net_far_acres
  )

manual_classification_queue <- project_classification_review_flags |>
  filter(manual_label_flag | review_reason != "") |>
  arrange(desc(manual_label_flag), desc(abs_net_far_acres), completed_year, project_id) |>
  select(
    project_id,
    completed_year,
    project_name,
    project_brief,
    borough_name_standardized,
    event_period,
    rezoning_direction,
    magnitude_bin,
    parse_status,
    initial_parse_status,
    initial_rezoning_direction,
    classification_source_tier,
    source_lookup_priority,
    text_candidate_direction,
    text_candidate_confidence,
    text_candidate_basis,
    parsed_zoning_changes,
    parser_stages,
    ignored_commercial_overlay_codes,
    ignored_mixed_use_component_codes,
    unrecognized_zoning_codes,
    project_net_far_delta,
    affected_lot_acres,
    gross_up_far_acres,
    gross_down_far_acres,
    net_far_acres,
    abs_net_far_acres,
    strict_bbl_scope_flag,
    high_confidence_scope_flag,
    strict_bbl_match_share,
    manual_label_flag,
    manual_review_status,
    manual_rezoning_class,
    housing_intent,
    scope_type,
    capacity_direction,
    manual_confidence,
    source_url,
    source_note,
    manual_notes,
    review_reason
  )

manual_classification_summary <- project_classification |>
  count(
    manual_review_status,
    manual_rezoning_class,
    housing_intent,
    scope_type,
    capacity_direction,
    manual_confidence,
    name = "project_count"
  ) |>
  arrange(manual_review_status, manual_rezoning_class, housing_intent, scope_type, capacity_direction, manual_confidence)

text_candidate_queue <- project_classification_review_flags |>
  filter(rezoning_direction == "unknown") |>
  mutate(remaining_reviewed_unknown_flag = reviewed_rezoning_direction == "unknown") |>
  arrange(
    desc(remaining_reviewed_unknown_flag),
    desc(source_lookup_priority == "high"),
    desc(text_candidate_direction != "no_local_text_candidate"),
    desc(affected_lot_acres),
    completed_year,
    project_id
  ) |>
  select(
    project_id,
    completed_year,
    event_period,
    project_name,
    project_brief,
    borough_name_standardized,
    remaining_reviewed_unknown_flag,
    reviewed_rezoning_direction,
    reviewed_direction_source,
    manual_reviewed_direction_flag,
    manual_reviewed_direction,
    manual_review_status,
    manual_rezoning_class,
    capacity_direction,
    manual_confidence,
    source_lookup_priority,
    missing_direction_reason,
    text_candidate_direction,
    text_candidate_confidence,
    text_candidate_basis,
    text_zoning_codes,
    text_commercial_overlay_codes,
    text_standalone_c1_c2_codes,
    mixed_use_text_flag,
    urban_renewal_special_district_text_flag,
    housing_any_candidate_flag,
    residential_constraint_text_flag,
    increased_residential_proxy,
    linked_bbl_count,
    strict_assigned_bbl_count,
    affected_lot_acres,
    parsed_zoning_changes,
    parser_stages,
    unrecognized_zoning_codes,
    review_reason
  )

text_candidate_summary <- text_candidate_queue |>
  count(
    remaining_reviewed_unknown_flag,
    text_candidate_direction,
    text_candidate_confidence,
    source_lookup_priority,
    missing_direction_reason,
    name = "project_count"
  ) |>
  arrange(desc(project_count), text_candidate_direction, text_candidate_confidence, source_lookup_priority, missing_direction_reason)

manual_prompt_sample <- manual_classification_queue |>
  filter(manual_review_status == "not_reviewed") |>
  slice_head(n = 40)

manual_prompt_records <- manual_prompt_sample |>
  mutate(
    prompt_record = paste0(
      "project_id: ", project_id, "\n",
      "completed_year: ", completed_year, "\n",
      "project_name: ", str_squish(coalesce(project_name, "")), "\n",
      "borough: ", coalesce(borough_name_standardized, ""), "\n",
      "review_reason: ", review_reason, "\n",
      "source_lookup_priority: ", source_lookup_priority, "\n",
      "text_candidate_direction: ", text_candidate_direction, "\n",
      "text_candidate_confidence: ", text_candidate_confidence, "\n",
      "text_candidate_basis: ", text_candidate_basis, "\n",
      "parser_direction: ", as.character(rezoning_direction), "\n",
      "classification_source_tier: ", classification_source_tier, "\n",
      "magnitude_bin: ", as.character(magnitude_bin), "\n",
      "parsed_zoning_changes: ", coalesce(parsed_zoning_changes, ""), "\n",
      "parser_stages: ", coalesce(parser_stages, ""), "\n",
      "ignored_commercial_overlay_codes: ", coalesce(ignored_commercial_overlay_codes, ""), "\n",
      "ignored_mixed_use_component_codes: ", coalesce(ignored_mixed_use_component_codes, ""), "\n",
      "project_net_far_delta: ", format_decimal(project_net_far_delta, 3), "\n",
      "affected_lot_acres: ", format_decimal(affected_lot_acres, 3), "\n",
      "net_far_acres: ", format_decimal(net_far_acres, 3), "\n",
      "project_brief: ", str_trunc(str_squish(coalesce(project_brief, "")), width = 1000), "\n"
    )
  ) |>
  pull(prompt_record)

manual_prompt_lines <- c(
  "# ZAP Rezoning Direction Manual Classification Prompt",
  "",
  "Classify the NYC ZAP zoning-map records below using only the project text and parsed zoning changes shown here. Treat these as suggestions for human review, not final labels.",
  "",
  "Return CSV with exactly these columns:",
  "project_id,manual_review_status,manual_rezoning_class,housing_intent,scope_type,capacity_direction,manual_confidence,source_url,source_note,manual_notes",
  "",
  "Use manual_review_status = chatgpt_suggested_text_only unless you verify a source URL. Use source_url and source_note only if you actually consulted an external source.",
  "",
  "Allowed manual_rezoning_class values: residential_intent_upzoning, residential_intent_downzoning, formal_residential_capacity_change_unclear_intent, nonresidential_institutional_or_civic, commercial_or_parking, infrastructure_or_resilience, industrial_or_manufacturing, mixed_intent, unknown.",
  "Allowed housing_intent values: yes, no, unclear.",
  "Allowed scope_type values: single_site, single_site_or_area_wide, area_wide, area_wide_or_corridor, corridor, unknown.",
  "Allowed capacity_direction values: up, down, mixed, no_material_residential_change, unknown.",
  "Allowed manual_confidence values: high, medium, low.",
  "",
  "Classification guidance:",
  "- Label the formal FAR direction from the zoning change separately from the apparent project intent.",
  "- Do not call C1/C2 overlays standalone residential upzonings unless the underlying residential district change is clear.",
  "- Flag civic, infrastructure, parking, auto, hospital, campus, industrial, or resilience projects as non-housing intent even when the parser detects a formal residential FAR change.",
  "- Use low confidence when the brief only states a zoning code change without a development program.",
  "",
  "Records:",
  "",
  manual_prompt_records
)

unknown_audit <- project_classification_review_flags |>
  filter(rezoning_direction == "unknown") |>
  arrange(missing_direction_reason, event_period, completed_year, project_id) |>
  select(
    project_id,
    completed_year,
    event_period,
    project_name,
    project_brief,
    borough_name_standardized,
    parse_status,
    initial_parse_status,
    initial_rezoning_direction,
    classification_source_tier,
    source_lookup_priority,
    missing_direction_reason,
    text_zoning_codes,
    text_commercial_overlay_codes,
    text_c1_c2_codes,
    text_standalone_c1_c2_codes,
    commercial_overlay_project_flag,
    commercial_overlay_action_flag,
    commercial_overlay_pair_flag,
    commercial_overlay_pair_codes,
    c1_c2_project_flag,
    standalone_c1_c2_text_flag,
    standalone_c1_c2_pair_flag,
    mixed_use_text_flag,
    urban_renewal_special_district_text_flag,
    housing_any_candidate_flag,
    residential_constraint_text_flag,
    increased_residential_proxy,
    zoning_category,
    parsed_zoning_changes,
    parser_stages,
    unrecognized_zoning_codes,
    ignored_commercial_overlay_codes,
    ignored_mixed_use_component_codes,
    linked_bbl_count,
    strict_assigned_bbl_count,
    affected_lot_acres,
    manual_review_status,
    manual_rezoning_class,
    housing_intent,
    capacity_direction,
    manual_confidence
  )

unknown_audit_city_summary <- unknown_audit |>
  group_by(missing_direction_reason) |>
  summarize(
    group_type = "city",
    event_period = "All",
    project_count = n_distinct(project_id),
    strict_scope_project_count = sum(strict_assigned_bbl_count > 0),
    commercial_overlay_project_count = sum(commercial_overlay_project_flag, na.rm = TRUE),
    mixed_use_text_project_count = sum(mixed_use_text_flag, na.rm = TRUE),
    housing_candidate_project_count = sum(housing_any_candidate_flag, na.rm = TRUE),
    increased_residential_proxy_project_count = sum(increased_residential_proxy, na.rm = TRUE),
    zoning_code_present_project_count = sum(text_zoning_codes != "", na.rm = TRUE),
    manual_reviewed_project_count = sum(manual_review_status != "not_reviewed"),
    .groups = "drop"
  )

unknown_audit_period_summary <- unknown_audit |>
  group_by(event_period, missing_direction_reason) |>
  summarize(
    group_type = "period",
    project_count = n_distinct(project_id),
    strict_scope_project_count = sum(strict_assigned_bbl_count > 0),
    commercial_overlay_project_count = sum(commercial_overlay_project_flag, na.rm = TRUE),
    mixed_use_text_project_count = sum(mixed_use_text_flag, na.rm = TRUE),
    housing_candidate_project_count = sum(housing_any_candidate_flag, na.rm = TRUE),
    increased_residential_proxy_project_count = sum(increased_residential_proxy, na.rm = TRUE),
    zoning_code_present_project_count = sum(text_zoning_codes != "", na.rm = TRUE),
    manual_reviewed_project_count = sum(manual_review_status != "not_reviewed"),
    .groups = "drop"
  )

unknown_audit_summary <- bind_rows(
  unknown_audit_city_summary,
  unknown_audit_period_summary
) |>
  mutate(
    unknown_project_share = if_else(
      group_type == "city",
      project_count / sum(project_count[group_type == "city"], na.rm = TRUE),
      project_count / ave(project_count, event_period, group_type, FUN = sum)
    )
  ) |>
  arrange(group_type, event_period, desc(project_count), missing_direction_reason)

resolution_waterfall_city <- project_classification |>
  group_by(classification_source_tier, initial_parse_status, initial_rezoning_direction, rezoning_direction) |>
  summarize(
    group_type = "city",
    event_period = "All",
    project_count = n_distinct(project_id),
    strict_scope_project_count = sum(strict_bbl_scope_flag),
    affected_lot_acres = sum(affected_lot_acres, na.rm = TRUE),
    housing_candidate_project_count = sum(housing_any_candidate_flag, na.rm = TRUE),
    mixed_use_text_project_count = sum(mixed_use_text_flag, na.rm = TRUE),
    commercial_overlay_project_count = sum(commercial_overlay_project_flag, na.rm = TRUE),
    .groups = "drop"
  )

resolution_waterfall_period <- project_classification |>
  group_by(event_period, classification_source_tier, initial_parse_status, initial_rezoning_direction, rezoning_direction) |>
  summarize(
    group_type = "period",
    project_count = n_distinct(project_id),
    strict_scope_project_count = sum(strict_bbl_scope_flag),
    affected_lot_acres = sum(affected_lot_acres, na.rm = TRUE),
    housing_candidate_project_count = sum(housing_any_candidate_flag, na.rm = TRUE),
    mixed_use_text_project_count = sum(mixed_use_text_flag, na.rm = TRUE),
    commercial_overlay_project_count = sum(commercial_overlay_project_flag, na.rm = TRUE),
    .groups = "drop"
  )

resolution_waterfall <- bind_rows(
  resolution_waterfall_city,
  resolution_waterfall_period
) |>
  mutate(
    final_known_direction_flag = rezoning_direction != "unknown",
    resolved_from_initial_unknown_flag = initial_rezoning_direction == "unknown" & rezoning_direction != "unknown",
    source_tier_order = case_when(
      classification_source_tier == "auto_primary_transition_known_far" ~ 1L,
      classification_source_tier == "auto_combined_overlay_transition_known_far" ~ 2L,
      classification_source_tier == "auto_combined_mixed_use_transition_known_far" ~ 3L,
      classification_source_tier == "auto_context_transition_known_far" ~ 4L,
      classification_source_tier == "auto_overlay_no_material_high" ~ 5L,
      classification_source_tier == "auto_overlay_no_material_medium" ~ 6L,
      classification_source_tier == "auto_other_known_direction" ~ 7L,
      TRUE ~ 99L
    )
  ) |>
  arrange(group_type, event_period, source_tier_order, initial_parse_status, initial_rezoning_direction, rezoning_direction)

write_csv_if_changed(parse_coverage, "../output/zap_rezoning_direction_parse_coverage_by_period.csv")
write_csv_if_changed(coverage_by_period_group, "../output/zap_rezoning_direction_coverage_by_period_group.csv")
write_csv_if_changed(unrecognized_codes, "../output/zap_rezoning_direction_unrecognized_zoning_codes.csv")
write_csv_if_changed(manual_review, "../output/zap_rezoning_direction_manual_review.csv")
write_csv_if_changed(manual_classification_queue, "../output/zap_rezoning_direction_manual_classification_queue.csv")
write_lines_if_changed(manual_prompt_lines, "../output/zap_rezoning_direction_manual_classification_chatgpt_prompt.md")
write_csv_if_changed(manual_classification_summary, "../output/zap_rezoning_direction_manual_classification_summary.csv")
write_csv_if_changed(text_candidate_queue, "../output/zap_rezoning_direction_text_candidate_queue.csv")
write_csv_if_changed(text_candidate_summary, "../output/zap_rezoning_direction_text_candidate_summary.csv")
write_csv_if_changed(unknown_audit, "../output/zap_rezoning_direction_unknown_audit.csv")
write_csv_if_changed(unknown_audit_summary, "../output/zap_rezoning_direction_unknown_audit_summary.csv")
write_csv_if_changed(resolution_waterfall, "../output/zap_rezoning_direction_resolution_waterfall.csv")
write_csv_if_changed(tercile_diagnostic_year, "../output/zap_rezoning_direction_tercile_diagnostic_year.csv")
write_csv_if_changed(top_abs_far_acres, "../output/zap_rezoning_direction_top_abs_far_acres.csv")

# Exploratory diagnostic plots.

city_plot_df <- city_year |>
  group_by(completed_year, rezoning_direction) |>
  summarize(project_count = sum(project_count), net_far_acres = sum(net_far_acres), .groups = "drop") |>
  filter(rezoning_direction %in% c("upzoning", "downzoning", "mixed", "unknown"))

pdf("../output/zap_rezoning_direction_city_trends.pdf", width = 11, height = 8.5)
print(
  ggplot(city_plot_df, aes(x = completed_year, y = project_count, color = rezoning_direction)) +
    geom_vline(xintercept = 1989, linetype = "dashed", linewidth = 0.35, color = "gray55") +
    geom_line(linewidth = 0.7) +
    scale_color_manual(values = c(upzoning = "#1B9E77", downzoning = "#D95F02", mixed = "#7570B3", unknown = "#6B6B6B")) +
    scale_x_continuous(breaks = seq(1980, 2020, by = 10), minor_breaks = seq(1980, 2025, by = 5)) +
    labs(
      title = "Completed ZAP zoning map changes by parsed direction",
      x = NULL,
      y = "Project records",
      color = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)
dev.off()

tercile_plot_df <- tercile_year |>
  filter(
    rezoning_direction %in% c("upzoning", "downzoning"),
    magnitude_bin %in% c("large_up", "moderate_up", "small_up", "small_down", "moderate_down", "large_down")
  ) |>
  group_by(year, homeowner_tercile_label, rezoning_direction) |>
  summarize(
    project_count_per_10000 = sum(project_count_per_10000),
    far_acres_per_residential_acre = sum(gross_up_far_acres_per_residential_acre + gross_down_far_acres_per_residential_acre),
    .groups = "drop"
  ) |>
  pivot_longer(
    cols = c(project_count_per_10000, far_acres_per_residential_acre),
    names_to = "outcome",
    values_to = "value"
  ) |>
  mutate(
    homeowner_tercile_label = factor(homeowner_tercile_label, levels = c("Low homeowner", "Middle homeowner", "High homeowner")),
    outcome_label = recode(
      outcome,
      project_count_per_10000 = "Project records per 10,000 occupied units",
      far_acres_per_residential_acre = "Gross FAR-acres per residential acre"
    )
  )

pdf("../output/zap_rezoning_direction_tercile_trends.pdf", width = 11, height = 8.5)
print(
  ggplot(tercile_plot_df, aes(x = year, y = value, color = homeowner_tercile_label)) +
    geom_vline(xintercept = 1989, linetype = "dashed", linewidth = 0.35, color = "gray55") +
    geom_line(linewidth = 0.6, na.rm = TRUE) +
    facet_grid(outcome_label ~ rezoning_direction, scales = "free_y") +
    scale_color_manual(values = c("Low homeowner" = "#2B8CBE", "Middle homeowner" = "#7B7B7B", "High homeowner" = "#D95F0E")) +
    scale_x_continuous(breaks = seq(1980, 2020, by = 10), minor_breaks = seq(1980, 2025, by = 5)) +
    labs(
      title = "Rezoning direction and scope by 2010 Council district homeowner tercile",
      x = NULL,
      y = NULL,
      color = NULL
    ) +
    theme_minimal(base_size = 10) +
    theme(legend.position = "bottom", axis.text.x = element_text(angle = 45, hjust = 1))
)
dev.off()

# Task-level QC.

qc_df <- bind_rows(
  tibble(metric = "zoning_map_project_count_1980_2025", value = as.character(nrow(project_classification)), status = if_else(nrow(project_classification) > 0, "pass", "fail"), note = "Completed 1980-2025 ZAP project records with zoning map changes."),
  tibble(metric = "project_id_duplicate_count", value = as.character(nrow(project_classification) - n_distinct(project_classification$project_id)), status = if_else(nrow(project_classification) == n_distinct(project_classification$project_id), "pass", "fail"), note = "Project classification output must be unique by project_id."),
  tibble(metric = "parsed_any_pair_project_count", value = as.character(sum(project_classification$parsed_pair_count > 0)), status = "pass", note = "Projects with at least one parsed zoning-code transition."),
  tibble(metric = "known_direction_project_count", value = as.character(sum(project_classification$rezoning_direction != "unknown")), status = "pass", note = "Projects with known first-pass residential FAR direction."),
  tibble(metric = "known_direction_share", value = as.character(mean(project_classification$rezoning_direction != "unknown")), status = "pass", note = "Share of ZM projects with known first-pass residential FAR direction."),
  tibble(metric = "unrecognized_zoning_code_count", value = as.character(n_distinct(unrecognized_codes$zoning_code)), status = "pass", note = "Distinct parsed zoning codes without usable standalone residential FAR, including overlays that need underlying district context."),
  tibble(metric = "ignored_commercial_overlay_project_count", value = as.character(sum(!is.na(project_classification$ignored_commercial_overlay_codes) & project_classification$ignored_commercial_overlay_codes != "")), status = "pass", note = "Projects where C1/C2 overlay codes were retained as diagnostics but excluded from residential FAR-direction parsing because an R district was available on the same side."),
  tibble(metric = "commercial_overlay_project_count", value = as.character(sum(project_classification$commercial_overlay_project_flag, na.rm = TRUE)), status = "pass", note = "Projects with C1/C2 overlay text, parsed overlay pairs, or ignored overlay diagnostics."),
  tibble(metric = "commercial_overlay_unknown_project_count", value = as.character(sum(project_classification$commercial_overlay_unknown_flag, na.rm = TRUE)), status = "pass", note = "Unknown-direction projects that include a C1/C2 overlay diagnostic."),
  tibble(metric = "mixed_use_text_project_count", value = as.character(sum(project_classification$mixed_use_text_flag, na.rm = TRUE)), status = "pass", note = "Projects whose text references mixed use, MX, or M/R mixed-use zoning."),
  tibble(metric = "mixed_use_unknown_project_count", value = as.character(sum(project_classification$mixed_use_unknown_flag, na.rm = TRUE)), status = "pass", note = "Unknown-direction projects with mixed-use text diagnostics."),
  tibble(metric = "zoning_lookup_rows", value = as.character(nrow(zoning_far_dictionary)), status = if_else(nrow(zoning_far_dictionary) > 0, "pass", "fail"), note = "Task-local NYC zoning district lookup rows."),
  tibble(metric = "zoning_lookup_unique_code_bad_count", value = as.character(nrow(zoning_far_dictionary) - n_distinct(zoning_far_dictionary$zoning_code)), status = if_else(nrow(zoning_far_dictionary) == n_distinct(zoning_far_dictionary$zoning_code), "pass", "fail"), note = "NYC zoning district lookup must be unique by zoning_code."),
  tibble(metric = "strict_bbl_scope_project_count", value = as.character(sum(project_classification$strict_bbl_scope_flag)), status = "pass", note = "Projects with at least one strict BBL/current-MapPLUTO/CCD2010 scope assignment."),
  tibble(metric = "strict_bbl_scope_project_share", value = as.character(mean(project_classification$strict_bbl_scope_flag)), status = "pass", note = "Share of ZM projects with strict scope assignment."),
  tibble(metric = "high_confidence_scope_project_count", value = as.character(sum(project_classification$high_confidence_scope_flag)), status = "pass", note = "Strict-scope projects with at least 80 percent of linked BBLs matched to CCD2010 and current MapPLUTO."),
  tibble(metric = "low_match_strict_scope_project_count", value = as.character(sum(project_classification$strict_bbl_scope_flag & !project_classification$high_confidence_scope_flag)), status = "pass", note = "Strict-scope projects below the high-confidence BBL match threshold."),
  tibble(metric = "scope_match_quality_rows", value = as.character(nrow(project_bbl_match_quality)), status = if_else(nrow(project_bbl_match_quality) == nrow(project_classification), "pass", "fail"), note = "Project-level BBL link and matching diagnostics."),
  tibble(metric = "coverage_by_period_group_rows", value = as.character(nrow(coverage_by_period_group)), status = if_else(nrow(coverage_by_period_group) > 0, "pass", "fail"), note = "Coverage diagnostics by period, borough, direction, and strict-scope homeowner tercile."),
  tibble(metric = "manual_classification_rows", value = as.character(nrow(manual_rezoning_classification)), status = if_else(nrow(manual_rezoning_classification) > 0, "pass", "fail"), note = "Rows in task-local manual rezoning classification seed file."),
  tibble(metric = "manual_classification_queue_rows", value = as.character(nrow(manual_classification_queue)), status = if_else(nrow(manual_classification_queue) > 0, "pass", "fail"), note = "Projects queued for manual or ChatGPT-assisted classification."),
  tibble(metric = "manual_classification_summary_rows", value = as.character(nrow(manual_classification_summary)), status = if_else(nrow(manual_classification_summary) > 0, "pass", "fail"), note = "Summary rows for current manual classification status."),
  tibble(metric = "text_candidate_queue_rows", value = as.character(nrow(text_candidate_queue)), status = if_else(nrow(text_candidate_queue) == sum(project_classification$rezoning_direction == "unknown"), "pass", "fail"), note = "One text-candidate review row per parser-unknown project, with reviewed/manual status columns."),
  tibble(metric = "text_candidate_summary_rows", value = as.character(nrow(text_candidate_summary)), status = if_else(nrow(text_candidate_summary) > 0, "pass", "fail"), note = "Summary rows for local-text candidate directions among remaining unknown projects."),
  tibble(metric = "unknown_audit_rows", value = as.character(nrow(unknown_audit)), status = if_else(nrow(unknown_audit) == sum(project_classification$rezoning_direction == "unknown"), "pass", "fail"), note = "One unknown-audit row per unknown-direction project."),
  tibble(metric = "unknown_audit_summary_rows", value = as.character(nrow(unknown_audit_summary)), status = if_else(nrow(unknown_audit_summary) > 0, "pass", "fail"), note = "Unknown-direction audit summary by city and period."),
  tibble(metric = "resolution_waterfall_rows", value = as.character(nrow(resolution_waterfall)), status = if_else(nrow(resolution_waterfall) > 0, "pass", "fail"), note = "Classification source-tier waterfall by city and period."),
  tibble(metric = "resolved_from_initial_unknown_count", value = as.character(sum(project_classification$initial_rezoning_direction == "unknown" & project_classification$rezoning_direction != "unknown")), status = "pass", note = "Projects resolved after initial unknown by conservative overlay/no-material rules in the current parser pass."),
  tibble(metric = "source_review_rows", value = as.character(nrow(source_rezoning_scope)), status = if_else(nrow(source_rezoning_scope) > 0, "pass", "fail"), note = "Task-local source-reviewed neighborhood/form-restriction rezoning seeds."),
  tibble(metric = "source_review_unique_key_bad_count", value = as.character(nrow(source_rezoning_scope) - n_distinct(source_rezoning_scope$project_id)), status = if_else(nrow(source_rezoning_scope) == n_distinct(source_rezoning_scope$project_id), "pass", "fail"), note = "Source-reviewed rezoning scope file must be unique by project_id."),
  tibble(metric = "official_source_reviewed_project_count", value = as.character(sum(project_classification$review_source_verified_flag)), status = "pass", note = "Projects with official source-reviewed direction or neighborhood/form-restriction scope."),
  tibble(metric = "manual_text_reviewed_direction_project_count", value = as.character(sum(project_classification$manual_reviewed_direction_flag)), status = "pass", note = "Projects with task-local manual text-review direction labels applied only to the separate reviewed direction field."),
  tibble(metric = "reviewed_known_direction_project_count", value = as.character(sum(project_classification$reviewed_rezoning_direction != "unknown")), status = "pass", note = "Projects with known direction after applying official source-reviewed labels to a separate reviewed direction field."),
  tibble(metric = "reviewed_source_direction_gain_count", value = as.character(sum(project_classification$rezoning_direction == "unknown" & project_classification$reviewed_rezoning_direction != "unknown")), status = "pass", note = "Parser-unknown projects resolved in the separate source-reviewed direction field."),
  tibble(metric = "reviewed_manual_parser_conflict_count", value = as.character(sum(project_classification$manual_reviewed_direction_flag & project_classification$rezoning_direction != "unknown" & project_classification$reviewed_rezoning_direction != as.character(project_classification$rezoning_direction))), status = "pass", note = "Parser-known projects where manual text review changes the separate reviewed direction field."),
  tibble(metric = "source_reviewed_cases_rows", value = as.character(nrow(source_reviewed_cases)), status = if_else(nrow(source_reviewed_cases) == nrow(source_rezoning_scope), "pass", "fail"), note = "One project-level output row per source-reviewed or source-seeded case."),
  tibble(metric = "reviewed_city_year_rows", value = as.character(nrow(reviewed_city_year)), status = if_else(nrow(reviewed_city_year) == 46 * length(direction_levels) * length(reviewed_scope_levels), "pass", "fail"), note = "Reviewed-source city-year output by direction and source/scope bin."),
  tibble(metric = "reviewed_period_rows", value = as.character(nrow(reviewed_period_counts)), status = if_else(nrow(reviewed_period_counts) > 0, "pass", "fail"), note = "Reviewed-source period output by direction and source/scope bin."),
  tibble(metric = "tercile_diagnostic_year_rows", value = as.character(nrow(tercile_diagnostic_year)), status = if_else(nrow(tercile_diagnostic_year) == 46 * 3, "pass", "fail"), note = "Rows in strict-scope homeowner-tercile diagnostic trend output."),
  tibble(metric = "project_assignment_weight_bad_count", value = as.character(project_weight_bad_count), status = if_else(project_weight_bad_count == 0, "pass", "fail"), note = "Project BBL assignment weights should sum to one for strictly assigned projects."),
  tibble(metric = "ccd_year_row_count", value = as.character(nrow(ccd_year_panel)), status = if_else(nrow(ccd_year_panel) == 51 * 46, "pass", "fail"), note = "Rows in the 2010 Council district-year panel."),
  tibble(metric = "ccd_year_unique_key_bad_count", value = as.character(nrow(ccd_year_panel) - nrow(distinct(ccd_year_panel, district_id, year))), status = if_else(nrow(ccd_year_panel) == nrow(distinct(ccd_year_panel, district_id, year)), "pass", "fail"), note = "CCD-year panel must be unique by district and year."),
  tibble(metric = "tercile_year_row_count", value = as.character(nrow(tercile_year)), status = if_else(nrow(tercile_year) == 46 * 3 * length(direction_levels) * length(magnitude_levels), "pass", "fail"), note = "Rows in tercile-year direction-magnitude output."),
  tibble(metric = "event_coefficient_rows", value = as.character(nrow(event_df)), status = if_else(nrow(event_df) == nrow(outcome_dictionary) * length(event_periods), "pass", "fail"), note = "Rows in event-study coefficient output, including reference periods."),
  tibble(metric = "missing_event_treatment_terms", value = as.character(missing_event_terms), status = if_else(missing_event_terms == 0, "pass", "fail"), note = "Requested event-study treatment terms missing from output."),
  tibble(metric = "long_difference_rows", value = as.character(nrow(long_diff_df)), status = if_else(nrow(long_diff_df) == nrow(outcome_dictionary) * nrow(window_defs), "pass", "fail"), note = "Rows in long-difference estimate output."),
  tibble(metric = "top_abs_far_acres_rows", value = as.character(nrow(top_abs_far_acres)), status = if_else(nrow(top_abs_far_acres) == min(50, sum(project_classification$strict_bbl_scope_flag & !is.na(project_classification$project_net_far_delta))), "pass", "fail"), note = "Top 50 absolute FAR-acre changes for manual inspection.")
)

write_csv_if_changed(qc_df, "../output/zap_rezoning_direction_qc.csv")

if (any(qc_df$status == "fail")) {
  stop("Rezoning direction/scope QC failed: ", paste(qc_df$metric[qc_df$status == "fail"], collapse = ", "))
}

cat("Wrote rezoning direction and scope outputs to ../output\n")
