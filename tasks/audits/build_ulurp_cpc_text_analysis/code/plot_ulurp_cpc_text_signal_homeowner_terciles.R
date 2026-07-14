# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/build_ulurp_cpc_text_analysis/code")
# start_year <- 1975
# end_year <- 2025
# moving_window_years <- 3
# minimum_documents_per_moving_window <- 20

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

options(warn = 1)

cli_args <- commandArgs(trailingOnly = TRUE)
if (length(cli_args) != 4) {
  stop(
    "Expected START_YEAR, END_YEAR, MOVING_WINDOW_YEARS, and MINIMUM_DOCUMENTS_PER_MOVING_WINDOW."
  )
}

start_year <- suppressWarnings(as.integer(cli_args[[1]]))
end_year <- suppressWarnings(as.integer(cli_args[[2]]))
moving_window_years <- suppressWarnings(as.integer(cli_args[[3]]))
minimum_documents_per_moving_window <- suppressWarnings(as.integer(cli_args[[4]]))

if (
  any(is.na(c(
    start_year,
    end_year,
    moving_window_years,
    minimum_documents_per_moving_window
  ))) ||
  end_year < start_year ||
  minimum_documents_per_moving_window < 1
) {
  stop("Year and moving-average arguments must be valid integers.")
}
if (moving_window_years < 1 || moving_window_years %% 2 == 0) {
  stop("MOVING_WINDOW_YEARS must be a positive odd integer.")
}

signal_families <- c(
  "revision_concession",
  "opposition_any",
  "conditions_commitments",
  "restrictive_declaration",
  "substantive_council_member",
  "attribution_council_member",
  "community_board_disapproval",
  "community_board_conditioned_approval",
  "opposition_traffic_parking",
  "opposition_scale_character",
  "opposition_displacement_affordability",
  "opposition_infrastructure"
)

signal_labels <- c(
  revision_concession = "Revision/concession",
  opposition_any = "Any opposition",
  conditions_commitments = "Conditions/commitments",
  restrictive_declaration = "Restrictive declaration",
  substantive_council_member = "Substantive Council mention",
  attribution_council_member = "Council attribution",
  community_board_disapproval = "Community Board disapproval",
  community_board_conditioned_approval = "Community Board conditioned approval",
  opposition_traffic_parking = "Traffic/parking opposition",
  opposition_scale_character = "Scale/character opposition",
  opposition_displacement_affordability = "Displacement/affordability opposition",
  opposition_infrastructure = "Infrastructure opposition"
)

community_district_corrections <- read_csv(
  "../input/ulurp_cpc_community_district_corrections.csv",
  col_types = cols(.default = col_character()),
  show_col_types = FALSE,
  na = c("", "NA")
)

if (
  nrow(community_district_corrections) !=
    n_distinct(community_district_corrections$application_number)
) {
  stop("Community-district corrections are not unique by application_number.")
}

text_labels <- read_csv(
  "../output/ulurp_cpc_text_labels.csv",
  col_types = cols(.default = col_character()),
  show_col_types = FALSE,
  na = c("", "NA")
) |>
  mutate(
    year = suppressWarnings(as.integer(year)),
    across(all_of(signal_families), ~ suppressWarnings(as.integer(.x)))
  ) |>
  filter(
    year >= start_year,
    year <= end_year
  )

if (nrow(text_labels) == 0 || nrow(text_labels) != n_distinct(text_labels$document_id)) {
  stop("Text labels must be nonempty and unique by document_id.")
}
if (
  any(is.na(as.matrix(text_labels[signal_families]))) ||
  any(!as.matrix(text_labels[signal_families]) %in% c(0L, 1L))
) {
  stop("Document-level text signals must be complete binary indicators.")
}
if (nrow(anti_join(community_district_corrections, text_labels, by = "application_number")) > 0) {
  stop("At least one community-district correction has no analysis narrative.")
}

documents <- text_labels |>
  rename(
    source_official_community_district = community_district,
    official_vote_year = year
  ) |>
  left_join(
    community_district_corrections,
    by = "application_number",
    relationship = "many-to-one"
  )

if (any(
  !is.na(documents$reported_community_district) &
  documents$source_official_community_district != documents$reported_community_district
)) {
  stop("A community-district correction no longer matches its reported source value.")
}

documents <- documents |>
  mutate(
    official_community_district = coalesce(
      corrected_community_district,
      source_official_community_district
    )
  )

project_bbl <- read_parquet(
  "../input/zap_project_bbl.parquet",
  col_select = c("project_id", "bbl_standardized")
) |>
  as.data.frame() |>
  as_tibble() |>
  transmute(
    project_id = str_squish(as.character(project_id)),
    bbl_standardized = str_squish(as.character(bbl_standardized))
  ) |>
  filter(project_id != "", bbl_standardized != "")

if (nrow(project_bbl) != nrow(distinct(project_bbl, project_id, bbl_standardized))) {
  stop("Project-BBL input is not unique by project_id and BBL.")
}

bbl_district_lookup <- read_parquet("../input/ccdist2010_mappluto_bbl_lookup.parquet") |>
  as.data.frame() |>
  as_tibble() |>
  transmute(
    bbl_standardized = as.character(bbl),
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    council_district = suppressWarnings(as.integer(council_district))
  ) |>
  filter(bbl_standardized != "", !is.na(district_id), !is.na(council_district))

if (nrow(bbl_district_lookup) != n_distinct(bbl_district_lookup$bbl_standardized)) {
  stop("2010 Council district BBL lookup is not unique by BBL.")
}

report_projects <- documents |>
  select(document_id, zap_project_ids) |>
  filter(!is.na(zap_project_ids)) |>
  separate_rows(zap_project_ids, sep = ";\\s*") |>
  transmute(document_id, project_id = str_squish(zap_project_ids)) |>
  filter(project_id != "") |>
  distinct(document_id, project_id)

project_bbl_index <- split(project_bbl$bbl_standardized, project_bbl$project_id)
document_bbl <- bind_rows(lapply(seq_len(nrow(report_projects)), function(i) {
  matched_bbl <- project_bbl_index[[report_projects$project_id[[i]]]]
  if (is.null(matched_bbl)) {
    return(NULL)
  }
  tibble(
    document_id = report_projects$document_id[[i]],
    bbl_standardized = matched_bbl
  )
})) |>
  distinct(document_id, bbl_standardized)

bbl_assignment <- document_bbl |>
  inner_join(bbl_district_lookup, by = "bbl_standardized", relationship = "many-to-one") |>
  distinct(document_id, bbl_standardized, district_id, council_district) |>
  count(document_id, district_id, council_district, name = "assigned_bbl_count") |>
  group_by(document_id) |>
  mutate(
    assignment_weight = assigned_bbl_count / sum(assigned_bbl_count)
  ) |>
  ungroup() |>
  select(document_id, district_id, council_district, assignment_weight)

mappluto_lots <- read_parquet(
  "../input/dcp_mappluto_current_25v4.parquet",
  col_select = c("bbl", "cd", "unitsres", "is_joint_interest_area")
) |>
  as.data.frame() |>
  as_tibble() |>
  transmute(
    bbl_standardized = as.character(bbl),
    borocd = suppressWarnings(as.integer(cd)),
    residential_units = pmax(suppressWarnings(as.numeric(unitsres)), 0, na.rm = TRUE),
    is_joint_interest_area = coalesce(as.logical(is_joint_interest_area), FALSE)
  ) |>
  filter(!is_joint_interest_area, !is.na(borocd), bbl_standardized != "")

if (nrow(mappluto_lots) != n_distinct(mappluto_lots$bbl_standardized)) {
  stop("Current MapPLUTO input is not unique by BBL after excluding joint-interest areas.")
}

community_district_crosswalk <- mappluto_lots |>
  filter(residential_units > 0) |>
  inner_join(bbl_district_lookup, by = "bbl_standardized", relationship = "many-to-one") |>
  group_by(borocd, district_id, council_district) |>
  summarize(residential_units = sum(residential_units), .groups = "drop") |>
  group_by(borocd) |>
  mutate(community_district_weight = residential_units / sum(residential_units)) |>
  ungroup()

if (n_distinct(community_district_crosswalk$borocd) != 59) {
  stop("Expected residential fallback weights for 59 standard community districts.")
}

bbl_document_ids <- bbl_assignment |>
  distinct(document_id)

fallback_documents <- documents |>
  anti_join(bbl_document_ids, by = "document_id") |>
  transmute(
    document_id,
    community_district_tokens = str_extract_all(
      str_to_upper(coalesce(official_community_district, "")),
      "(?:MN|BX|BK|QN|SI)\\s*[0-9]{1,2}"
    )
  ) |>
  unnest_longer(community_district_tokens) |>
  mutate(
    borough_prefix = str_extract(community_district_tokens, "^[A-Z]{2}"),
    borough_code = case_when(
      borough_prefix == "MN" ~ 1L,
      borough_prefix == "BX" ~ 2L,
      borough_prefix == "BK" ~ 3L,
      borough_prefix == "QN" ~ 4L,
      borough_prefix == "SI" ~ 5L,
      TRUE ~ NA_integer_
    ),
    community_district_number = suppressWarnings(as.integer(str_extract(
      community_district_tokens,
      "[0-9]{1,2}"
    ))),
    borocd = borough_code * 100L + community_district_number
  ) |>
  filter(!is.na(borocd)) |>
  distinct(document_id, borocd) |>
  group_by(document_id) |>
  mutate(fallback_community_district_count = n()) |>
  ungroup()

community_district_crosswalk_index <- split(
  community_district_crosswalk,
  community_district_crosswalk$borocd
)
fallback_assignment <- bind_rows(lapply(seq_len(nrow(fallback_documents)), function(i) {
  matched_districts <- community_district_crosswalk_index[[
    as.character(fallback_documents$borocd[[i]])
  ]]
  if (is.null(matched_districts)) {
    return(NULL)
  }
  matched_districts |>
    transmute(
      document_id = fallback_documents$document_id[[i]],
      district_id,
      council_district,
      assignment_weight = community_district_weight /
        fallback_documents$fallback_community_district_count[[i]]
    )
})) |>
  group_by(document_id, district_id, council_district) |>
  summarize(
    assignment_weight = sum(assignment_weight),
    .groups = "drop"
  )

district_treatment <- read_csv(
  "../input/ccdist2010_homeownership_1990_measure.csv",
  col_types = cols(.default = col_character()),
  show_col_types = FALSE,
  na = c("", "NA")
) |>
  transmute(
    district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    council_district = suppressWarnings(as.integer(council_district)),
    borough_code = suppressWarnings(as.integer(borough_code)),
    treat_pp = suppressWarnings(as.numeric(treat_pp))
  ) |>
  arrange(borough_code, treat_pp, council_district) |>
  group_by(borough_code) |>
  mutate(
    homeowner_tercile = ntile(treat_pp, 3),
    homeowner_tercile_label = case_when(
      homeowner_tercile == 1L ~ "Low homeowner",
      homeowner_tercile == 2L ~ "Middle homeowner",
      homeowner_tercile == 3L ~ "High homeowner"
    )
  ) |>
  ungroup() |>
  select(
    district_id,
    council_district,
    homeowner_tercile,
    homeowner_tercile_label
  )

tercile_district_counts <- district_treatment |>
  count(homeowner_tercile, homeowner_tercile_label, name = "council_district_count") |>
  arrange(homeowner_tercile)

if (
  nrow(district_treatment) != 51 ||
  nrow(district_treatment) != n_distinct(district_treatment$district_id) ||
  !identical(as.integer(tercile_district_counts$council_district_count), c(19L, 17L, 15L))
) {
  stop("Figure 2 treatment must contain 51 districts split 19/17/15 within-borough terciles.")
}

assignment <- bind_rows(bbl_assignment, fallback_assignment) |>
  left_join(
    documents |>
      select(document_id, official_vote_year),
    by = "document_id",
    relationship = "many-to-one"
  ) |>
  left_join(
    district_treatment,
    by = c("district_id", "council_district"),
    relationship = "many-to-one"
  ) |>
  arrange(official_vote_year, document_id, district_id)

if (any(is.na(assignment$homeowner_tercile))) {
  stop("At least one assigned Council district is missing the Figure 2 treatment.")
}

assignment_weight_failures <- assignment |>
  group_by(document_id) |>
  summarize(assignment_weight_sum = sum(assignment_weight), .groups = "drop") |>
  filter(abs(assignment_weight_sum - 1) > 1e-8)

if (nrow(assignment_weight_failures) > 0) {
  stop("Council district assignment weights do not sum to one within every assigned narrative.")
}

assigned_document_count <- n_distinct(assignment$document_id)
assignment_coverage <- assigned_document_count / nrow(documents)
if (assignment_coverage < 0.99) {
  stop("Council district assignment coverage is below 99 percent.")
}

signal_assignment <- assignment |>
  select(
    document_id,
    official_vote_year,
    homeowner_tercile,
    homeowner_tercile_label,
    assignment_weight
  ) |>
  left_join(
    documents |>
      select(document_id, all_of(signal_families)),
    by = "document_id",
    relationship = "many-to-one"
  ) |>
  pivot_longer(
    cols = all_of(signal_families),
    names_to = "signal_family",
    values_to = "signal_hit"
  )

observed_tercile_year <- signal_assignment |>
  group_by(official_vote_year, homeowner_tercile, homeowner_tercile_label, signal_family) |>
  summarize(
    weighted_document_count = sum(assignment_weight),
    weighted_hit_document_count = sum(assignment_weight * signal_hit),
    .groups = "drop"
  )

centered_sum <- function(year, value, window_years) {
  half_window <- (window_years - 1) / 2
  vapply(year, function(current_year) {
    in_window <- abs(year - current_year) <= half_window
    if (sum(in_window) != window_years) {
      return(NA_real_)
    }
    sum(value[in_window])
  }, numeric(1))
}

tercile_year <- expand_grid(
  official_vote_year = start_year:end_year,
  homeowner_tercile = 1:3,
  signal_family = signal_families
) |>
  left_join(
    tercile_district_counts |>
      select(-council_district_count),
    by = "homeowner_tercile",
    relationship = "many-to-one"
  ) |>
  left_join(
    observed_tercile_year,
    by = c("official_vote_year", "homeowner_tercile", "homeowner_tercile_label", "signal_family"),
    relationship = "one-to-one"
  ) |>
  mutate(
    weighted_document_count = coalesce(weighted_document_count, 0),
    weighted_hit_document_count = coalesce(weighted_hit_document_count, 0)
  ) |>
  group_by(signal_family, homeowner_tercile, homeowner_tercile_label) |>
  arrange(official_vote_year, .by_group = TRUE) |>
  mutate(
    moving_window_weighted_document_count = centered_sum(
      official_vote_year,
      weighted_document_count,
      moving_window_years
    ),
    moving_window_weighted_hit_document_count = centered_sum(
      official_vote_year,
      weighted_hit_document_count,
      moving_window_years
    ),
    moving_window_observed_year_count = centered_sum(
      official_vote_year,
      as.integer(weighted_document_count > 0),
      moving_window_years
    ),
    moving_window_eligible =
      moving_window_observed_year_count == moving_window_years &
      moving_window_weighted_document_count >= minimum_documents_per_moving_window,
    hit_document_share_moving_window = if_else(
      moving_window_eligible,
      moving_window_weighted_hit_document_count / moving_window_weighted_document_count,
      NA_real_
    )
  ) |>
  ungroup() |>
  arrange(match(signal_family, signal_families), official_vote_year, homeowner_tercile)

homeowner_colors <- c(
  "Low homeowner" = "#3366CC",
  "Middle homeowner" = "#999999",
  "High homeowner" = "#CC3311"
)

bbl_document_count <- n_distinct(bbl_assignment$document_id)
fallback_document_count <- n_distinct(fallback_assignment$document_id)
plot_subtitle <- paste0(
  "Archived 2010 Council districts; ",
  scales::comma(bbl_document_count),
  " narratives assigned by project BBL and ",
  scales::comma(fallback_document_count),
  " by official community-district fallback; ",
  scales::comma(nrow(documents) - assigned_document_count),
  " unassigned"
)

pdf("../output/ulurp_cpc_text_signal_homeowner_tercile_trends.pdf", width = 11, height = 8.5)
for (signal_id in signal_families) {
  plot_df <- tercile_year |>
    filter(signal_family == signal_id) |>
    mutate(
      homeowner_tercile_label = factor(
        homeowner_tercile_label,
        levels = c("Low homeowner", "Middle homeowner", "High homeowner")
      )
    )

  print(
    ggplot(
      plot_df,
      aes(
        x = official_vote_year,
        y = hit_document_share_moving_window,
        color = homeowner_tercile_label,
        group = homeowner_tercile_label
      )
    ) +
      geom_point(alpha = 0.22, size = 0.9, na.rm = TRUE) +
      geom_line(linewidth = 0.9, na.rm = TRUE) +
      scale_color_manual(values = homeowner_colors) +
      scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
      labs(
        title = unname(signal_labels[[signal_id]]),
        subtitle = plot_subtitle,
        x = NULL,
        y = paste0(
          "Share of narratives with signal (pooled ",
          moving_window_years,
          "-year window)"
        ),
        color = NULL,
        caption = paste0(
          "Terciles reproduce Figure 2 using the 1990 homeownership measure within borough. Each point pools the centered window; at least ",
          minimum_documents_per_moving_window,
          " weighted narratives are required per window."
        )
      ) +
      theme_minimal(base_size = 11) +
      theme(
        legend.position = "bottom",
        panel.grid.minor = element_blank(),
        plot.caption = element_text(hjust = 0)
      )
  )
}
dev.off()

cat(
  "Assigned ", assigned_document_count, "/", nrow(documents),
  " narratives: ", bbl_document_count, " by project BBL, ",
  fallback_document_count, " by community-district fallback, ",
  nrow(documents) - assigned_document_count, " unassigned.\n",
  sep = ""
)
