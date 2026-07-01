suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../../_lib/source_pipeline_utils.R")

direction_levels <- c(
  "upzoning",
  "downzoning",
  "mixed",
  "no_material_residential_change",
  "unknown"
)

direction_labels <- c(
  upzoning = "Upzoning",
  downzoning = "Downzoning",
  mixed = "Mixed",
  no_material_residential_change = "No material residential change",
  unknown = "Unknown"
)

direction_colors <- c(
  upzoning = "#1B9E77",
  downzoning = "#D95F02",
  mixed = "#7570B3",
  no_material_residential_change = "#5A5A5A",
  unknown = "#B8B8B8"
)

homeowner_tercile_levels <- c("Low homeowner", "Middle homeowner", "High homeowner")

homeowner_tercile_colors <- c(
  "Low homeowner" = "#2B8CBE",
  "Middle homeowner" = "#6B6B6B",
  "High homeowner" = "#D95F02"
)

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

project_classification <- read_csv(
  "../input/zap_rezoning_direction_project_classification.csv",
  show_col_types = FALSE
)
chatgpt_first_review <- read_csv(
  "../output/zap_rezoning_chatgpt_manual_review_queue.csv",
  show_col_types = FALSE
)
chatgpt_second_review <- read_csv(
  "../output/zap_rezoning_chatgpt_second_manual_review_queue.csv",
  show_col_types = FALSE
)
human_review <- read_csv(
  "zap_rezoning_human_manual_review_verdicts.csv",
  show_col_types = FALSE
)

if (nrow(project_classification) != n_distinct(project_classification$project_id)) {
  stop("Project classification is not unique by project_id.")
}
if (nrow(chatgpt_first_review) != n_distinct(chatgpt_first_review$project_id)) {
  stop("ChatGPT first-review file is not unique by project_id.")
}
if (nrow(chatgpt_second_review) != n_distinct(chatgpt_second_review$project_id)) {
  stop("ChatGPT second-review file is not unique by project_id.")
}
if (nrow(human_review) != n_distinct(human_review$project_id)) {
  stop("Human review verdict file is not unique by project_id.")
}

chatgpt_first_review <- chatgpt_first_review |>
  transmute(
    project_id,
    first_pass_direction = suggested_rezoning_direction,
    first_pass_confidence = suggested_confidence,
    first_pass_class = suggested_rezoning_class,
    first_pass_note = suggested_evidence_note
  )

chatgpt_second_review <- chatgpt_second_review |>
  transmute(
    project_id,
    second_pass_direction,
    second_pass_dominant_capacity_effect = dominant_capacity_effect,
    second_pass_mixed_split_needed = mixed_split_needed,
    second_pass_confidence,
    second_pass_recommendation = review_recommendation,
    second_pass_note
  )

human_review <- human_review |>
  transmute(
    project_id,
    human_direction,
    human_dominant_capacity_effect,
    human_mixed_split_needed,
    human_confidence,
    human_note,
    human_reviewer,
    human_review_date
  )

provisional_project_labels <- project_classification |>
  left_join(chatgpt_first_review, by = "project_id", relationship = "one-to-one") |>
  left_join(chatgpt_second_review, by = "project_id", relationship = "one-to-one") |>
  left_join(human_review, by = "project_id", relationship = "one-to-one") |>
  mutate(
    best_direction = case_when(
      !is.na(human_direction) & human_direction != "" ~ human_direction,
      !is.na(second_pass_direction) & second_pass_direction != "" ~ second_pass_direction,
      !is.na(first_pass_direction) & first_pass_direction != "" ~ first_pass_direction,
      TRUE ~ rezoning_direction
    ),
    best_dominant_capacity_effect = case_when(
      !is.na(human_dominant_capacity_effect) & human_dominant_capacity_effect != "" ~ human_dominant_capacity_effect,
      !is.na(second_pass_dominant_capacity_effect) & second_pass_dominant_capacity_effect != "" ~ second_pass_dominant_capacity_effect,
      TRUE ~ best_direction
    ),
    best_mixed_split_needed = case_when(
      !is.na(human_mixed_split_needed) & human_mixed_split_needed != "" ~ human_mixed_split_needed,
      !is.na(second_pass_mixed_split_needed) & second_pass_mixed_split_needed != "" ~ second_pass_mixed_split_needed,
      best_direction == "mixed" ~ "yes",
      TRUE ~ "no"
    ),
    best_label_source = case_when(
      !is.na(human_direction) & human_direction != "" ~ "human_manual_review",
      !is.na(second_pass_direction) & second_pass_direction != "" ~ "chatgpt_second_pass",
      !is.na(first_pass_direction) & first_pass_direction != "" ~ "chatgpt_first_pass",
      TRUE ~ "parser_or_existing_manual"
    ),
    best_confidence = case_when(
      !is.na(human_confidence) & human_confidence != "" ~ human_confidence,
      !is.na(second_pass_confidence) & second_pass_confidence != "" ~ second_pass_confidence,
      !is.na(first_pass_confidence) & first_pass_confidence != "" ~ first_pass_confidence,
      rezoning_direction == "unknown" ~ "low",
      TRUE ~ "high"
    ),
    completed_year = as.integer(completed_year),
    event_period = factor(event_period, levels = event_periods),
    best_direction = factor(best_direction, levels = direction_levels),
    best_dominant_capacity_effect = factor(best_dominant_capacity_effect, levels = direction_levels)
  )

if (any(is.na(provisional_project_labels$best_direction))) {
  stop("At least one project has a missing provisional best direction.")
}

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
    ),
    homeowner_tercile_label = factor(homeowner_tercile_label, levels = homeowner_tercile_levels)
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

project_bbl_assigned <- provisional_project_labels |>
  select(
    project_id,
    completed_year,
    event_period,
    project_name,
    borough_name_standardized,
    best_direction,
    best_dominant_capacity_effect,
    best_label_source,
    strict_bbl_scope_flag,
    affected_lot_acres,
    affected_current_residential_lot_acres,
    gross_up_far_acres,
    gross_down_far_acres,
    net_far_acres,
    project_gross_up_far_delta,
    project_gross_down_far_delta,
    project_net_far_delta
  ) |>
  inner_join(zap_project_bbl, by = "project_id", relationship = "one-to-many") |>
  left_join(ccdist2010_bbl_lookup, by = "bbl_standardized", relationship = "many-to-one") |>
  filter(!is.na(district_id)) |>
  distinct(project_id, bbl_standardized, .keep_all = TRUE) |>
  group_by(project_id) |>
  mutate(
    project_assigned_bbl_count = n_distinct(bbl_standardized),
    project_assignment_weight = 1 / project_assigned_bbl_count
  ) |>
  ungroup()

project_weight_bad_count <- project_bbl_assigned |>
  group_by(project_id) |>
  summarize(weight_sum = sum(project_assignment_weight), .groups = "drop") |>
  filter(abs(weight_sum - 1) > 1e-8) |>
  nrow()

project_lot_assigned <- project_bbl_assigned |>
  left_join(
    district_lookup |> select(district_id, homeowner_tercile, homeowner_tercile_label),
    by = "district_id",
    relationship = "many-to-one"
  ) |>
  left_join(
    mappluto_lot |> select(bbl_standardized, lot_acres, current_residential_lot_acres),
    by = "bbl_standardized",
    relationship = "many-to-one"
  ) |>
  filter(!is.na(homeowner_tercile), !is.na(lot_acres)) |>
  group_by(project_id) |>
  mutate(project_lot_assignment_weight = 1 / n_distinct(bbl_standardized)) |>
  ungroup() |>
  mutate(
    gross_up_far_acres_lot = coalesce(project_gross_up_far_delta, 0) * lot_acres,
    gross_down_far_acres_lot = coalesce(project_gross_down_far_delta, 0) * lot_acres,
    net_far_acres_lot = coalesce(project_net_far_delta, 0) * lot_acres,
    capacity_far_acres_lot = case_when(
      best_direction == "upzoning" ~ gross_up_far_acres_lot,
      best_direction == "downzoning" ~ gross_down_far_acres_lot,
      TRUE ~ NA_real_
    ),
    known_capacity_far_project = case_when(
      best_direction == "upzoning" ~ !is.na(project_gross_up_far_delta),
      best_direction == "downzoning" ~ !is.na(project_gross_down_far_delta),
      TRUE ~ FALSE
    )
  )

project_lot_weight_bad_count <- project_lot_assigned |>
  group_by(project_id) |>
  summarize(weight_sum = sum(project_lot_assignment_weight), .groups = "drop") |>
  filter(abs(weight_sum - 1) > 1e-8) |>
  nrow()

project_scope_samples <- bind_rows(
  provisional_project_labels |> mutate(scope_sample = "1980-2025"),
  provisional_project_labels |> filter(completed_year >= 1990) |> mutate(scope_sample = "1990-2025")
) |>
  filter(best_direction %in% c("upzoning", "downzoning"), strict_bbl_scope_flag) |>
  mutate(
    capacity_far_acres = case_when(
      best_direction == "upzoning" ~ gross_up_far_acres,
      best_direction == "downzoning" ~ gross_down_far_acres,
      TRUE ~ NA_real_
    ),
    known_capacity_far_project = case_when(
      best_direction == "upzoning" ~ !is.na(project_gross_up_far_delta),
      best_direction == "downzoning" ~ !is.na(project_gross_down_far_delta),
      TRUE ~ FALSE
    )
  )

up_down_scope_summary <- project_scope_samples |>
  group_by(scope_sample, best_direction) |>
  summarize(
    project_count = n_distinct(project_id),
    far_known_project_count = sum(known_capacity_far_project, na.rm = TRUE),
    total_affected_lot_acres = sum(affected_lot_acres, na.rm = TRUE),
    mean_affected_lot_acres = mean(affected_lot_acres, na.rm = TRUE),
    median_affected_lot_acres = median(affected_lot_acres, na.rm = TRUE),
    p90_affected_lot_acres = as.numeric(quantile(affected_lot_acres, 0.9, na.rm = TRUE, names = FALSE)),
    total_capacity_far_acres = sum(capacity_far_acres, na.rm = TRUE),
    mean_capacity_far_acres = mean(capacity_far_acres, na.rm = TRUE),
    median_capacity_far_acres = median(capacity_far_acres, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(scope_sample, best_direction)

project_dominant_tercile <- project_bbl_assigned |>
  left_join(
    district_lookup |> select(district_id, homeowner_tercile, homeowner_tercile_label),
    by = "district_id",
    relationship = "many-to-one"
  ) |>
  filter(!is.na(homeowner_tercile)) |>
  group_by(project_id, homeowner_tercile, homeowner_tercile_label) |>
  summarize(project_tercile_weight = sum(project_assignment_weight, na.rm = TRUE), .groups = "drop") |>
  arrange(project_id, desc(project_tercile_weight), homeowner_tercile) |>
  group_by(project_id) |>
  slice_head(n = 1) |>
  ungroup() |>
  rename(
    dominant_homeowner_tercile = homeowner_tercile,
    dominant_homeowner_tercile_label = homeowner_tercile_label,
    dominant_homeowner_tercile_weight = project_tercile_weight
  )

up_down_top_scope_projects <- provisional_project_labels |>
  filter(best_direction %in% c("upzoning", "downzoning"), affected_lot_acres > 0) |>
  left_join(project_dominant_tercile, by = "project_id", relationship = "one-to-one") |>
  mutate(
    capacity_far_acres = case_when(
      best_direction == "upzoning" ~ gross_up_far_acres,
      best_direction == "downzoning" ~ gross_down_far_acres,
      TRUE ~ NA_real_
    )
  ) |>
  arrange(best_direction, desc(affected_lot_acres), project_id) |>
  group_by(best_direction) |>
  slice_head(n = 25) |>
  ungroup() |>
  select(
    project_id,
    project_name,
    completed_year,
    best_direction,
    dominant_homeowner_tercile_label,
    dominant_homeowner_tercile_weight,
    affected_lot_acres,
    affected_current_residential_lot_acres,
    capacity_far_acres,
    gross_up_far_acres,
    gross_down_far_acres,
    net_far_acres,
    best_label_source,
    best_confidence
  )

tercile_scope_samples <- bind_rows(
  project_lot_assigned |> mutate(scope_sample = "1980-2025"),
  project_lot_assigned |> filter(completed_year >= 1990) |> mutate(scope_sample = "1990-2025")
) |>
  filter(best_direction %in% c("upzoning", "downzoning"))

up_down_scope_by_tercile <- tercile_scope_samples |>
  group_by(scope_sample, homeowner_tercile, homeowner_tercile_label, best_direction) |>
  summarize(
    project_count = sum(project_lot_assignment_weight, na.rm = TRUE),
    far_known_project_count = sum(project_lot_assignment_weight * as.integer(known_capacity_far_project), na.rm = TRUE),
    affected_bbl_count = n_distinct(bbl_standardized),
    affected_lot_acres = sum(lot_acres, na.rm = TRUE),
    affected_current_residential_lot_acres = sum(current_residential_lot_acres, na.rm = TRUE),
    capacity_far_acres = sum(capacity_far_acres_lot, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    affected_lot_acres_per_project = if_else(project_count > 0, affected_lot_acres / project_count, NA_real_),
    affected_residential_lot_acres_per_project = if_else(project_count > 0, affected_current_residential_lot_acres / project_count, NA_real_),
    capacity_far_acres_per_project = if_else(project_count > 0, capacity_far_acres / project_count, NA_real_)
  ) |>
  arrange(scope_sample, homeowner_tercile, best_direction)

provisional_tercile_year_observed <- project_bbl_assigned |>
  left_join(
    district_lookup |> select(district_id, homeowner_tercile, homeowner_tercile_label),
    by = "district_id",
    relationship = "many-to-one"
  ) |>
  filter(!is.na(homeowner_tercile)) |>
  group_by(completed_year, homeowner_tercile, homeowner_tercile_label, best_direction) |>
  summarize(
    project_count = sum(project_assignment_weight, na.rm = TRUE),
    assigned_project_count = n_distinct(project_id),
    .groups = "drop"
  )

provisional_tercile_year <- expand_grid(
  completed_year = 1980:2025,
  tercile_denominators,
  best_direction = factor(direction_levels, levels = direction_levels)
) |>
  left_join(
    provisional_tercile_year_observed,
    by = c("completed_year", "homeowner_tercile", "homeowner_tercile_label", "best_direction"),
    relationship = "one-to-one"
  ) |>
  mutate(
    project_count = coalesce(project_count, 0),
    assigned_project_count = coalesce(assigned_project_count, 0L),
    project_count_per_10000_occupied_units = 10000 * project_count / occupied_units_1990
  ) |>
  group_by(best_direction, homeowner_tercile, homeowner_tercile_label) |>
  arrange(completed_year, .by_group = TRUE) |>
  mutate(
    project_count_3yr = (
      coalesce(lag(project_count), 0) + project_count + coalesce(lead(project_count), 0)
    ) / (
      1 + as.integer(!is.na(lag(project_count))) + as.integer(!is.na(lead(project_count)))
    ),
    project_count_5yr = (
      coalesce(lag(project_count, 2), 0) +
        coalesce(lag(project_count), 0) +
        project_count +
        coalesce(lead(project_count), 0) +
        coalesce(lead(project_count, 2), 0)
    ) / (
      1 +
        as.integer(!is.na(lag(project_count, 2))) +
        as.integer(!is.na(lag(project_count))) +
        as.integer(!is.na(lead(project_count))) +
        as.integer(!is.na(lead(project_count, 2)))
    ),
    project_count_per_10000_3yr = (
      coalesce(lag(project_count_per_10000_occupied_units), 0) +
        project_count_per_10000_occupied_units +
        coalesce(lead(project_count_per_10000_occupied_units), 0)
    ) / (
      1 + as.integer(!is.na(lag(project_count_per_10000_occupied_units))) +
        as.integer(!is.na(lead(project_count_per_10000_occupied_units)))
    ),
    project_count_per_10000_5yr = (
      coalesce(lag(project_count_per_10000_occupied_units, 2), 0) +
        coalesce(lag(project_count_per_10000_occupied_units), 0) +
        project_count_per_10000_occupied_units +
        coalesce(lead(project_count_per_10000_occupied_units), 0) +
        coalesce(lead(project_count_per_10000_occupied_units, 2), 0)
    ) / (
      1 +
        as.integer(!is.na(lag(project_count_per_10000_occupied_units, 2))) +
        as.integer(!is.na(lag(project_count_per_10000_occupied_units))) +
        as.integer(!is.na(lead(project_count_per_10000_occupied_units))) +
        as.integer(!is.na(lead(project_count_per_10000_occupied_units, 2)))
    )
  ) |>
  ungroup() |>
  arrange(completed_year, homeowner_tercile, best_direction)

provisional_city_year <- provisional_project_labels |>
  count(completed_year, best_direction, name = "project_count") |>
  complete(
    completed_year = 1980:2025,
    best_direction = factor(direction_levels, levels = direction_levels),
    fill = list(project_count = 0)
  ) |>
  group_by(completed_year) |>
  mutate(
    total_project_count = sum(project_count),
    project_share = if_else(total_project_count > 0, project_count / total_project_count, 0)
  ) |>
  ungroup() |>
  arrange(completed_year, best_direction)

provisional_city_year <- provisional_city_year |>
  group_by(best_direction) |>
  arrange(completed_year, .by_group = TRUE) |>
  mutate(
    project_count_3yr = (
      coalesce(lag(project_count), 0L) + project_count + coalesce(lead(project_count), 0L)
    ) / (
      1L + as.integer(!is.na(lag(project_count))) + as.integer(!is.na(lead(project_count)))
    )
  ) |>
  ungroup()

provisional_period <- provisional_project_labels |>
  count(event_period, best_direction, name = "project_count") |>
  complete(
    event_period = factor(event_periods, levels = event_periods),
    best_direction = factor(direction_levels, levels = direction_levels),
    fill = list(project_count = 0)
  ) |>
  group_by(event_period) |>
  mutate(
    total_project_count = sum(project_count),
    project_share = if_else(total_project_count > 0, project_count / total_project_count, 0)
  ) |>
  ungroup() |>
  arrange(event_period, best_direction)

label_source_year <- provisional_project_labels |>
  count(completed_year, best_label_source, name = "project_count") |>
  complete(
    completed_year = 1980:2025,
    best_label_source = c(
      "parser_or_existing_manual",
      "chatgpt_first_pass",
      "chatgpt_second_pass",
      "human_manual_review"
    ),
    fill = list(project_count = 0)
  )

unknown_share_year <- provisional_city_year |>
  filter(best_direction == "unknown") |>
  select(completed_year, project_share, project_count, total_project_count)

provisional_qc <- bind_rows(
  tibble(
    metric = "project_count",
    value = as.character(nrow(provisional_project_labels)),
    status = if_else(nrow(provisional_project_labels) == 1347L, "pass", "review"),
    note = "Completed ZAP ZM projects in the provisional best-label trend file."
  ),
  tibble(
    metric = "known_direction_project_count",
    value = as.character(sum(provisional_project_labels$best_direction != "unknown")),
    status = "pass",
    note = "Projects with provisional known direction after parser, ChatGPT, second pass, and human verdicts."
  ),
  tibble(
    metric = "unknown_direction_project_count",
    value = as.character(sum(provisional_project_labels$best_direction == "unknown")),
    status = "pass",
    note = "Projects still unknown in the provisional best-label file."
  ),
  tibble(
    metric = "chatgpt_first_pass_project_count",
    value = as.character(nrow(chatgpt_first_review)),
    status = "pass",
    note = "Projects with first-pass source-review labels."
  ),
  tibble(
    metric = "chatgpt_second_pass_project_count",
    value = as.character(nrow(chatgpt_second_review)),
    status = "pass",
    note = "Projects with second-pass source-review labels."
  ),
  tibble(
    metric = "human_manual_verdict_project_count",
    value = as.character(sum(!is.na(human_review$human_direction) & human_review$human_direction != "")),
    status = "pass",
    note = "Projects with user-provided human verdict labels."
  ),
  tibble(
    metric = "mixed_split_needed_project_count",
    value = as.character(sum(provisional_project_labels$best_mixed_split_needed == "yes")),
    status = "pass",
    note = "Known mixed projects requiring future gross up/down split for magnitude estimates."
  ),
  tibble(
    metric = "tercile_strict_bbl_assigned_project_count",
    value = as.character(n_distinct(project_bbl_assigned$project_id)),
    status = "pass",
    note = "Projects with at least one BBL assigned to a 2010 Council district; tercile plots use only these projects."
  ),
  tibble(
    metric = "tercile_strict_bbl_unassigned_project_count",
    value = as.character(nrow(provisional_project_labels) - n_distinct(project_bbl_assigned$project_id)),
    status = "review",
    note = "Projects not included in homeowner-tercile plots because no BBL-to-CCD assignment is available."
  ),
  tibble(
    metric = "tercile_assignment_weight_bad_project_count",
    value = as.character(project_weight_bad_count),
    status = if_else(project_weight_bad_count == 0L, "pass", "fail"),
    note = "Project assignment weights should sum to one for every BBL-assigned project."
  ),
  tibble(
    metric = "scope_lot_assigned_project_count",
    value = as.character(n_distinct(project_lot_assigned$project_id)),
    status = "pass",
    note = "Projects with at least one current MapPLUTO lot and 2010 Council district assignment for scope diagnostics."
  ),
  tibble(
    metric = "scope_lot_assignment_weight_bad_project_count",
    value = as.character(project_lot_weight_bad_count),
    status = if_else(project_lot_weight_bad_count == 0L, "pass", "fail"),
    note = "Project lot-scope assignment weights should sum to one for every lot-assigned project."
  )
)

write_csv_if_changed(provisional_project_labels, "../output/zap_rezoning_provisional_best_direction_project_labels.csv")
write_csv_if_changed(provisional_city_year, "../output/zap_rezoning_provisional_best_direction_city_year.csv")
write_csv_if_changed(provisional_period, "../output/zap_rezoning_provisional_best_direction_period.csv")
write_csv_if_changed(provisional_tercile_year, "../output/zap_rezoning_provisional_best_direction_tercile_year.csv")
write_csv_if_changed(up_down_scope_summary, "../output/zap_rezoning_provisional_up_down_scope_summary.csv")
write_csv_if_changed(up_down_scope_by_tercile, "../output/zap_rezoning_provisional_up_down_scope_by_tercile.csv")
write_csv_if_changed(up_down_top_scope_projects, "../output/zap_rezoning_provisional_up_down_top_scope_projects.csv")
write_csv_if_changed(provisional_qc, "../output/zap_rezoning_provisional_best_direction_qc.csv")

pdf("../output/zap_rezoning_provisional_best_direction_city_trends.pdf", width = 11, height = 8.5)

print(
  ggplot(
    provisional_city_year,
    aes(x = completed_year, y = project_count_3yr, color = best_direction)
  ) +
    geom_vline(xintercept = 1989, linetype = "dashed", linewidth = 0.35, color = "gray55") +
    geom_line(linewidth = 0.8) +
    scale_color_manual(values = direction_colors, labels = direction_labels, drop = FALSE) +
    scale_x_continuous(breaks = seq(1980, 2020, by = 10), minor_breaks = seq(1980, 2025, by = 5)) +
    labs(
      title = "Completed ZAP zoning map changes by provisional best direction",
      subtitle = "Three-year centered moving average; parser labels overlaid with ChatGPT and human source review",
      x = NULL,
      y = "Project records",
      color = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)

print(
  ggplot(
    provisional_period,
    aes(x = event_period, y = project_count, fill = best_direction)
  ) +
    geom_col(width = 0.75) +
    scale_fill_manual(values = direction_colors, labels = direction_labels, drop = FALSE) +
    labs(
      title = "Completed ZAP zoning map changes by five-year period",
      subtitle = "Counts use provisional best direction labels",
      x = NULL,
      y = "Project records",
      fill = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(
      legend.position = "bottom",
      axis.text.x = element_text(angle = 35, hjust = 1)
    )
)

print(
  ggplot(
    provisional_city_year |>
      filter(best_direction %in% c("upzoning", "downzoning", "mixed")),
    aes(x = completed_year, y = project_count_3yr, color = best_direction)
  ) +
    geom_vline(xintercept = 1989, linetype = "dashed", linewidth = 0.35, color = "gray55") +
    geom_line(linewidth = 0.9) +
    scale_color_manual(values = direction_colors, labels = direction_labels, drop = FALSE) +
    scale_x_continuous(breaks = seq(1980, 2020, by = 10), minor_breaks = seq(1980, 2025, by = 5)) +
    labs(
      title = "Substantive residential-capacity zoning changes",
      subtitle = "Upzoning, downzoning, and mixed projects only; three-year centered moving average",
      x = NULL,
      y = "Project records",
      color = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)

print(
  ggplot(unknown_share_year, aes(x = completed_year, y = project_share)) +
    geom_vline(xintercept = 1989, linetype = "dashed", linewidth = 0.35, color = "gray55") +
    geom_col(fill = "#B8B8B8", width = 0.8) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
    scale_x_continuous(breaks = seq(1980, 2020, by = 10), minor_breaks = seq(1980, 2025, by = 5)) +
    labs(
      title = "Remaining unknown share by completion year",
      subtitle = "Unknowns are retained rather than redistributed",
      x = NULL,
      y = "Share of project records"
    ) +
    theme_minimal(base_size = 11)
)

print(
  ggplot(label_source_year, aes(x = completed_year, y = project_count, fill = best_label_source)) +
    geom_col(width = 0.8) +
    scale_fill_manual(
      values = c(
        parser_or_existing_manual = "#7B7B7B",
        chatgpt_first_pass = "#2B8CBE",
        chatgpt_second_pass = "#41AB5D",
        human_manual_review = "#D95F0E"
      ),
      labels = c(
        parser_or_existing_manual = "Parser / existing manual",
        chatgpt_first_pass = "ChatGPT first pass",
        chatgpt_second_pass = "ChatGPT second pass",
        human_manual_review = "Human verdict"
      ),
      drop = FALSE
    ) +
    scale_x_continuous(breaks = seq(1980, 2020, by = 10), minor_breaks = seq(1980, 2025, by = 5)) +
    labs(
      title = "Source of provisional best direction labels",
      x = NULL,
      y = "Project records",
      fill = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)

dev.off()

pdf("../output/zap_rezoning_provisional_up_down_tercile_trends.pdf", width = 11, height = 8.5)

print(
  ggplot(
    provisional_tercile_year |>
      filter(best_direction == "upzoning"),
    aes(
      x = completed_year,
      y = project_count_per_10000_5yr,
      color = homeowner_tercile_label
    )
  ) +
    geom_vline(xintercept = 1989, linetype = "dashed", linewidth = 0.35, color = "gray55") +
    geom_line(linewidth = 0.9) +
    scale_color_manual(values = homeowner_tercile_colors, drop = FALSE) +
    scale_x_continuous(breaks = seq(1980, 2020, by = 10), minor_breaks = seq(1980, 2025, by = 5)) +
    labs(
      title = "Upzonings by 1990 homeowner tercile",
      subtitle = "Strict BBL-linked assignment to 2010 Council districts; five-year centered moving average",
      x = NULL,
      y = "Project records per 10,000 occupied 1990 units",
      color = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)

print(
  ggplot(
    provisional_tercile_year |>
      filter(best_direction == "downzoning"),
    aes(
      x = completed_year,
      y = project_count_per_10000_5yr,
      color = homeowner_tercile_label
    )
  ) +
    geom_vline(xintercept = 1989, linetype = "dashed", linewidth = 0.35, color = "gray55") +
    geom_line(linewidth = 0.9) +
    scale_color_manual(values = homeowner_tercile_colors, drop = FALSE) +
    scale_x_continuous(breaks = seq(1980, 2020, by = 10), minor_breaks = seq(1980, 2025, by = 5)) +
    labs(
      title = "Downzonings by 1990 homeowner tercile",
      subtitle = "Strict BBL-linked assignment to 2010 Council districts; five-year centered moving average",
      x = NULL,
      y = "Project records per 10,000 occupied 1990 units",
      color = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)

dev.off()

scope_plot_df <- up_down_scope_by_tercile |>
  filter(scope_sample == "1990-2025")

pdf("../output/zap_rezoning_provisional_up_down_scope_by_tercile.pdf", width = 11, height = 8.5)

print(
  ggplot(
    scope_plot_df,
    aes(x = homeowner_tercile_label, y = affected_lot_acres_per_project, fill = best_direction)
  ) +
    geom_col(position = position_dodge(width = 0.75), width = 0.65) +
    scale_fill_manual(
      values = direction_colors[c("upzoning", "downzoning")],
      labels = direction_labels[c("upzoning", "downzoning")]
    ) +
    labs(
      title = "Average linked current lot acres per project by direction and homeowner tercile",
      subtitle = "Post-1990 strict ZAP BBL links joined to current MapPLUTO; not official rezoning-area acreage",
      x = NULL,
      y = "Linked current MapPLUTO lot acres per weighted project",
      fill = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)

print(
  ggplot(
    scope_plot_df,
    aes(x = homeowner_tercile_label, y = affected_lot_acres, fill = best_direction)
  ) +
    geom_col(position = position_dodge(width = 0.75), width = 0.65) +
    scale_fill_manual(
      values = direction_colors[c("upzoning", "downzoning")],
      labels = direction_labels[c("upzoning", "downzoning")]
    ) +
    labs(
      title = "Total linked current lot acres by direction and homeowner tercile",
      subtitle = "Post-1990 strict ZAP BBL links joined to current MapPLUTO; not official rezoning-area acreage",
      x = NULL,
      y = "Linked current MapPLUTO lot acres",
      fill = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)

print(
  ggplot(
    scope_plot_df,
    aes(x = homeowner_tercile_label, y = capacity_far_acres_per_project, fill = best_direction)
  ) +
    geom_col(position = position_dodge(width = 0.75), width = 0.65) +
    scale_fill_manual(
      values = direction_colors[c("upzoning", "downzoning")],
      labels = direction_labels[c("upzoning", "downzoning")]
    ) +
    labs(
      title = "Parsed capacity FAR-acres per project by direction and homeowner tercile",
      subtitle = "Post-1990; parsed FAR change times current MapPLUTO lot area; missing parsed FAR contributes zero",
      x = NULL,
      y = "Capacity FAR-acres per weighted project",
      fill = NULL
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")
)

dev.off()

print("Wrote provisional ZAP rezoning direction trend outputs to ../output")
