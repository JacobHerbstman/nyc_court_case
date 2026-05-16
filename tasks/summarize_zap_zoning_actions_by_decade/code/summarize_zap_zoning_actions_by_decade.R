# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_zap_zoning_actions_by_decade/code")

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

raw_zap_projects <- read_csv("../input/zap_housing_project_base_audited.csv", show_col_types = FALSE, guess_max = Inf, na = c("", "NA"))

council_homeowner_lookup <- read_csv("../input/ccdist2010_homeownership_1990_measure.csv", show_col_types = FALSE, na = c("", "NA")) |>
  transmute(
    ccd2010_district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    ccd2010_council_district = suppressWarnings(as.integer(council_district)),
    ccd2010_borough_code = suppressWarnings(as.integer(borough_code)),
    ccd2010_borough_name = as.character(borough_name),
    ccd2010_treat_pp = suppressWarnings(as.numeric(treat_pp)),
    ccd2010_treat_z_boro = suppressWarnings(as.numeric(treat_z_boro))
  ) |>
  arrange(ccd2010_treat_z_boro, ccd2010_council_district) |>
  mutate(
    ccd2010_homeowner_tercile = ntile(ccd2010_treat_z_boro, 3),
    ccd2010_homeowner_tercile_label = case_when(
      ccd2010_homeowner_tercile == 1 ~ "Low homeowner",
      ccd2010_homeowner_tercile == 2 ~ "Middle homeowner",
      ccd2010_homeowner_tercile == 3 ~ "High homeowner",
      TRUE ~ NA_character_
    )
  )

if (nrow(council_homeowner_lookup) != 51 || nrow(council_homeowner_lookup) != n_distinct(council_homeowner_lookup$ccd2010_district_id)) {
  stop("2010 Council district homeowner lookup must cover exactly 51 unique districts.")
}

council_homeowner_tercile_counts <- council_homeowner_lookup |>
  count(ccd2010_homeowner_tercile, ccd2010_homeowner_tercile_label, name = "council_district_count")

if (any(council_homeowner_tercile_counts$council_district_count != 17)) {
  stop("2010 Council district homeowner terciles must contain 17 districts each.")
}

zap_bbl <- read_parquet("../input/zap_project_bbl.parquet") |>
  transmute(
    project_id = as.character(project_id),
    bbl_standardized = as.character(bbl_standardized)
  ) |>
  filter(!is.na(project_id), project_id != "", !is.na(bbl_standardized), bbl_standardized != "") |>
  distinct(project_id, bbl_standardized)

if (nrow(zap_bbl) != nrow(distinct(zap_bbl, project_id, bbl_standardized))) {
  stop("ZAP project-BBL input is not unique by project_id and BBL.")
}

ccdist2010_bbl_lookup <- read_parquet("../input/ccdist2010_mappluto_bbl_lookup.parquet") |>
  transmute(
    bbl_standardized = as.character(bbl),
    ccd2010_district_id = sprintf("%02d", suppressWarnings(as.integer(district_id))),
    ccd2010_council_district = suppressWarnings(as.integer(council_district))
  ) |>
  filter(!is.na(bbl_standardized), bbl_standardized != "", !is.na(ccd2010_council_district)) |>
  distinct(bbl_standardized, .keep_all = TRUE)

if (nrow(ccdist2010_bbl_lookup) != n_distinct(ccdist2010_bbl_lookup$bbl_standardized)) {
  stop("2010 Council district BBL lookup is not unique by BBL.")
}

zap_projects <- raw_zap_projects |>
  mutate(
    project_id = as.character(project_id),
    completed_date = as.Date(completed_date_parsed),
    completed_year = suppressWarnings(as.integer(format(completed_date, "%Y"))),
    decade = case_when(
      completed_year >= 1976 & completed_year <= 1979 ~ "1970s",
      completed_year >= 1980 & completed_year <= 1989 ~ "1980s",
      completed_year >= 1990 & completed_year <= 1999 ~ "1990s",
      completed_year >= 2000 & completed_year <= 2009 ~ "2000s",
      completed_year >= 2010 & completed_year <= 2019 ~ "2010s",
      completed_year >= 2020 & completed_year <= 2025 ~ "2020s",
      TRUE ~ NA_character_
    ),
    has_zoning_map_change = as.logical(either_has_zm),
    has_zoning_special_permit = as.logical(either_has_zs),
    all_text = str_to_upper(str_squish(paste(
      coalesce(project_name, ""),
      coalesce(project_brief, ""),
      coalesce(primary_applicant, ""),
      coalesce(ceqr_leadagency, "")
    ))),
    mixed_use_text_flag = str_detect(all_text, "MIXED[ -]?USE|MIXED USE|MIXED-USE"),
    residential_unit_text_flag = str_detect(
      all_text,
      "[0-9][0-9, -]*(UNIT|UNITS|DWELLING|DWELLINGS|APARTMENT|APARTMENTS|DU|DUS)"
    ),
    residential_growth_text_flag = str_detect(
      all_text,
      "(FACILITATE|ALLOW|PERMIT|DEVELOP|DEVELOPMENT|CONSTRUCT|CONST|NEW|CONVERSION|CONVERT|REHAB|REHABILITATION|REDEVELOPMENT).{0,180}(RESIDENTIAL|RESDL|HOUSING|HSNG|DWELLING|APARTMENT|APT|UNIT|UNITS|DU|DUS|MIXED[ -]?USE)|((RESIDENTIAL|RESDL|HOUSING|HSNG|MIXED[ -]?USE).{0,180}(PROJECT|FACILITATE|ALLOW|PERMIT|DEVELOP|CONSTRUCT|CONST|NEW|CONVERSION|CONVERT|REHAB|REHABILITATION|REDEVELOPMENT|UNIT|UNITS|DU|DUS))"
    ),
    zoning_to_residential_text_flag = str_detect(
      all_text,
      "(CHANGE|REZO|REZONE|MAP).{0,80}(M[0-9]|C[0-9]|R[0-9A-Z/-]+).{0,80}TO.{0,40}R[0-9]"
    ),
    residential_constraint_text_flag = str_detect(
      all_text,
      "(DOWNZON|LOW[- ]SCALE|LOWER DENSITY|PRESERV|LIMIT FUTURE|REDUCE|REDUCING|RESTRICT|NO NEW RESIDENTIAL|NOT FACILITATE RESIDENTIAL)"
    ),
    increased_residential_proxy = housing_any_candidate_flag &
      !residential_constraint_text_flag &
      (mih_flag_bool | residential_unit_text_flag | residential_growth_text_flag | zoning_to_residential_text_flag),
    minor_residential_proxy = housing_any_candidate_flag &
      !increased_residential_proxy &
      !residential_constraint_text_flag,
    zoning_category = case_when(
      increased_residential_proxy ~ "Increased Residential",
      minor_residential_proxy ~ "Minor Residential",
      TRUE ~ "Nonresidential/Downzoning"
    ),
    included_all_zm_zs = has_zoning_map_change | has_zoning_special_permit,
    included_zm_plus_residential_zs = has_zoning_map_change | (has_zoning_special_permit & housing_any_candidate_flag),
    included_zm_only = has_zoning_map_change
  ) |>
  filter(
    public_status == "Completed",
    completed_year >= 1976,
    completed_year <= 2025,
    included_all_zm_zs
  ) |>
  select(
    project_id,
    project_name,
    project_brief,
    completed_date,
    completed_year,
    decade,
    borough,
    community_district,
    borough_name_standardized,
    borocd_primary,
    applicant_type,
    primary_applicant,
    project_status,
    public_status,
    actions,
    ulurp_numbers,
    has_zoning_map_change,
    has_zoning_special_permit,
    housing_any_candidate_flag,
    housing_strict_text_flag,
    housing_broad_text_flag,
    mih_flag_bool,
    hpd_text_flag,
    mixed_use_text_flag,
    residential_unit_text_flag,
    residential_growth_text_flag,
    zoning_to_residential_text_flag,
    residential_constraint_text_flag,
    increased_residential_proxy,
    minor_residential_proxy,
    zoning_category,
    included_all_zm_zs,
    included_zm_plus_residential_zs,
    included_zm_only
  ) |>
  arrange(completed_year, borough_name_standardized, project_id)

if (nrow(zap_projects) != n_distinct(zap_projects$project_id)) {
  stop("Completed ZM/ZS project classification is not unique by project_id.")
}

count_scope_dictionary <- tribble(
  ~count_scope, ~count_scope_label, ~include_column,
  "all_zm_zs", "All completed ZM/ZS project records", "included_all_zm_zs",
  "zm_plus_residential_zs", "Completed ZM project records plus residential ZS project records", "included_zm_plus_residential_zs",
  "zm_only", "Completed ZM project records only", "included_zm_only"
)

zoning_categories <- c(
  "Increased Residential",
  "Minor Residential",
  "Nonresidential/Downzoning"
)

decade_dictionary <- tribble(
  ~decade, ~year_count_in_decade,
  "1970s", 4L,
  "1980s", 10L,
  "1990s", 10L,
  "2000s", 10L,
  "2010s", 10L,
  "2020s", 6L
)

project_scope_rows <- bind_rows(
  zap_projects |>
    filter(included_all_zm_zs) |>
    mutate(count_scope = "all_zm_zs"),
  zap_projects |>
    filter(included_zm_plus_residential_zs) |>
    mutate(count_scope = "zm_plus_residential_zs"),
  zap_projects |>
    filter(included_zm_only) |>
    mutate(count_scope = "zm_only")
) |>
  left_join(
    count_scope_dictionary |>
      select(count_scope, count_scope_label),
    by = "count_scope",
    relationship = "many-to-one"
  )

if (nrow(project_scope_rows) != nrow(distinct(project_scope_rows, count_scope, project_id))) {
  stop("Project-scope rows are not unique by count_scope and project_id.")
}

year_counts <- expand_grid(
  count_scope_dictionary |>
    select(count_scope, count_scope_label),
  completed_year = 1976:2025,
  zoning_category = zoning_categories
) |>
  mutate(
    decade = case_when(
      completed_year <= 1979 ~ "1970s",
      completed_year <= 1989 ~ "1980s",
      completed_year <= 1999 ~ "1990s",
      completed_year <= 2009 ~ "2000s",
      completed_year <= 2019 ~ "2010s",
      TRUE ~ "2020s"
    )
  ) |>
  left_join(
    project_scope_rows |>
      count(count_scope, count_scope_label, completed_year, zoning_category, name = "project_count"),
    by = c("count_scope", "count_scope_label", "completed_year", "zoning_category"),
    relationship = "one-to-one"
  ) |>
  mutate(
    project_count = coalesce(project_count, 0L),
    zoning_category = factor(zoning_category, levels = zoning_categories)
  ) |>
  arrange(count_scope, completed_year, zoning_category)

decade_counts <- year_counts |>
  group_by(count_scope, count_scope_label, decade, zoning_category) |>
  summarize(project_count = sum(project_count), .groups = "drop") |>
  left_join(decade_dictionary, by = "decade", relationship = "many-to-one") |>
  mutate(
    mean_completed_per_year = project_count / year_count_in_decade,
    zoning_category = factor(zoning_category, levels = zoning_categories)
  ) |>
  arrange(count_scope, decade, zoning_category)

housing_focus_dictionary <- tribble(
  ~outcome_id, ~outcome_label,
  "increased_residential", "Increased residential",
  "increased_residential_mixed_use", "Increased residential with mixed-use text",
  "mixed_use_any_category", "Mixed-use text, any category",
  "residential_signal_only", "Residential signal only / weakly classified"
)

housing_focus_year_counts <- expand_grid(
  completed_year = 1976:2025,
  housing_focus_dictionary
) |>
  mutate(
    decade = case_when(
      completed_year <= 1979 ~ "1970s",
      completed_year <= 1989 ~ "1980s",
      completed_year <= 1999 ~ "1990s",
      completed_year <= 2009 ~ "2000s",
      completed_year <= 2019 ~ "2010s",
      TRUE ~ "2020s"
    )
  ) |>
  left_join(
    project_scope_rows |>
      filter(count_scope == "zm_plus_residential_zs") |>
      transmute(
        completed_year,
        increased_residential = increased_residential_proxy,
        increased_residential_mixed_use = increased_residential_proxy & mixed_use_text_flag,
        mixed_use_any_category = mixed_use_text_flag,
        residential_signal_only = minor_residential_proxy
      ) |>
      pivot_longer(
        cols = c(
          increased_residential,
          increased_residential_mixed_use,
          mixed_use_any_category,
          residential_signal_only
        ),
        names_to = "outcome_id",
        values_to = "included_flag"
      ) |>
      filter(included_flag) |>
      count(completed_year, outcome_id, name = "project_count"),
    by = c("completed_year", "outcome_id"),
    relationship = "one-to-one"
  ) |>
  mutate(project_count = coalesce(project_count, 0L)) |>
  arrange(outcome_id, completed_year)

housing_focus_decade_counts <- housing_focus_year_counts |>
  group_by(outcome_id, outcome_label, decade) |>
  summarize(project_count = sum(project_count), .groups = "drop") |>
  left_join(decade_dictionary, by = "decade", relationship = "many-to-one") |>
  mutate(mean_completed_per_year = project_count / year_count_in_decade) |>
  arrange(outcome_id, decade)

project_ccd2010_fractional <- zap_projects |>
  select(project_id) |>
  inner_join(zap_bbl, by = "project_id", relationship = "one-to-many") |>
  inner_join(ccdist2010_bbl_lookup, by = "bbl_standardized", relationship = "many-to-one") |>
  count(project_id, ccd2010_district_id, ccd2010_council_district, name = "assigned_bbl_count") |>
  group_by(project_id) |>
  mutate(
    project_assigned_bbl_count = sum(assigned_bbl_count),
    ccd2010_assignment_weight = assigned_bbl_count / project_assigned_bbl_count
  ) |>
  ungroup() |>
  left_join(council_homeowner_lookup, by = c("ccd2010_district_id", "ccd2010_council_district"), relationship = "many-to-one") |>
  arrange(project_id, ccd2010_council_district)

if (nrow(project_ccd2010_fractional) != nrow(distinct(project_ccd2010_fractional, project_id, ccd2010_district_id))) {
  stop("Project-2010-Council fractional assignment is not unique by project_id and district.")
}

project_ccd2010_weight_bad_count <- project_ccd2010_fractional |>
  group_by(project_id) |>
  summarize(weight_sum = sum(ccd2010_assignment_weight), .groups = "drop") |>
  filter(abs(weight_sum - 1) > 1e-8) |>
  nrow()

homeowner_tercile_year_counts <- expand_grid(
  completed_year = 1976:2025,
  council_homeowner_tercile_counts
) |>
  mutate(
    decade = case_when(
      completed_year <= 1979 ~ "1970s",
      completed_year <= 1989 ~ "1980s",
      completed_year <= 1999 ~ "1990s",
      completed_year <= 2009 ~ "2000s",
      completed_year <= 2019 ~ "2010s",
      TRUE ~ "2020s"
    )
  ) |>
  left_join(
    project_scope_rows |>
      filter(
        count_scope == "zm_plus_residential_zs",
        increased_residential_proxy
      ) |>
      select(project_id, completed_year) |>
      inner_join(project_ccd2010_fractional, by = "project_id", relationship = "one-to-many") |>
      group_by(completed_year, ccd2010_homeowner_tercile, ccd2010_homeowner_tercile_label) |>
      summarize(project_count = sum(ccd2010_assignment_weight), .groups = "drop"),
    by = c("completed_year", "ccd2010_homeowner_tercile", "ccd2010_homeowner_tercile_label"),
    relationship = "one-to-one"
  ) |>
  mutate(
    project_count = coalesce(project_count, 0)
  ) |>
  arrange(ccd2010_homeowner_tercile, completed_year)

homeowner_tercile_decade_counts <- homeowner_tercile_year_counts |>
  group_by(ccd2010_homeowner_tercile, ccd2010_homeowner_tercile_label, council_district_count, decade) |>
  summarize(project_count = sum(project_count), .groups = "drop") |>
  left_join(decade_dictionary, by = "decade", relationship = "many-to-one") |>
  mutate(
    mean_completed_per_year = project_count / year_count_in_decade
  ) |>
  arrange(ccd2010_homeowner_tercile, decade)

homeowner_tercile_year_smoothed <- homeowner_tercile_year_counts |>
  group_by(ccd2010_homeowner_tercile, ccd2010_homeowner_tercile_label, council_district_count) |>
  arrange(completed_year, .by_group = TRUE) |>
  mutate(
    project_count_3yr = (lag(project_count) + project_count + lead(project_count)) / 3,
    smoothing_window = if_else(
      is.na(project_count_3yr),
      NA_character_,
      paste0(completed_year - 1L, "-", completed_year + 1L)
    )
  ) |>
  ungroup() |>
  filter(!is.na(project_count_3yr)) |>
  arrange(ccd2010_homeowner_tercile, completed_year)

write_csv_if_changed(zap_projects, "../output/zap_zoning_map_special_permit_project_classification.csv")
write_csv_if_changed(project_ccd2010_fractional, "../output/zap_zoning_map_special_permit_project_ccd2010_fractional.csv")
write_csv_if_changed(year_counts, "../output/zap_zoning_map_special_permit_year.csv")
write_csv_if_changed(decade_counts, "../output/zap_zoning_map_special_permit_decade.csv")
write_csv_if_changed(housing_focus_year_counts, "../output/zap_zoning_map_special_permit_housing_focus_year.csv")
write_csv_if_changed(housing_focus_decade_counts, "../output/zap_zoning_map_special_permit_housing_focus_decade.csv")
write_csv_if_changed(homeowner_tercile_year_counts, "../output/zap_zoning_map_special_permit_increased_residential_homeowner_tercile_year.csv")
write_csv_if_changed(homeowner_tercile_decade_counts, "../output/zap_zoning_map_special_permit_increased_residential_homeowner_tercile_decade.csv")
write_csv_if_changed(homeowner_tercile_year_smoothed, "../output/zap_zoning_map_special_permit_increased_residential_homeowner_tercile_year_3yr.csv")

all_zm_zs_plot <- decade_counts |>
  filter(count_scope == "all_zm_zs") |>
  ggplot(aes(x = decade, y = mean_completed_per_year, fill = zoning_category)) +
  geom_col(width = 0.64) +
  scale_fill_manual(
    values = c(
      "Increased Residential" = "#12A69C",
      "Minor Residential" = "#087684",
      "Nonresidential/Downzoning" = "#7DC36D"
    )
  ) +
  labs(
    title = "Completed Zoning Map Changes and Special Permits",
    subtitle = "Literal ZAP project-record count: completed ULURP records with ZM or ZS in actions or ULURP numbers",
    x = NULL,
    y = "Completed project records per year",
    fill = NULL
  ) +
  theme_minimal(base_size = 11) +
  theme(
    legend.position = "bottom",
    panel.grid.major.x = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave("../output/zap_zoning_map_special_permit_all_zm_zs.pdf", all_zm_zs_plot, width = 7.5, height = 4.5)

zm_plus_residential_zs_plot <- decade_counts |>
  filter(count_scope == "zm_plus_residential_zs") |>
  ggplot(aes(x = decade, y = mean_completed_per_year, fill = zoning_category)) +
  geom_col(width = 0.64) +
  scale_fill_manual(
    values = c(
      "Increased Residential" = "#12A69C",
      "Minor Residential" = "#087684",
      "Nonresidential/Downzoning" = "#7DC36D"
    )
  ) +
  labs(
    title = "Completed Zoning Map Changes and Residential Special Permits",
    subtitle = "Narrower count: all completed ZM records plus ZS records only when ZAP text indicates residential/housing content",
    x = NULL,
    y = "Completed project records per year",
    fill = NULL
  ) +
  theme_minimal(base_size = 11) +
  theme(
    legend.position = "bottom",
    panel.grid.major.x = element_blank(),
    panel.grid.minor = element_blank()
  )

ggsave("../output/zap_zoning_map_special_permit_zm_plus_residential_zs.pdf", zm_plus_residential_zs_plot, width = 7.5, height = 4.5)

all_zm_zs_line_plot <- year_counts |>
  filter(count_scope == "all_zm_zs") |>
  ggplot(aes(x = completed_year, y = project_count, color = zoning_category)) +
  geom_line(linewidth = 0.8) +
  geom_point(size = 1.1) +
  scale_color_manual(
    values = c(
      "Increased Residential" = "#12A69C",
      "Minor Residential" = "#087684",
      "Nonresidential/Downzoning" = "#7DC36D"
    )
  ) +
  scale_x_continuous(breaks = seq(1980, 2020, by = 10), minor_breaks = seq(1976, 2025, by = 1)) +
  labs(
    title = "Completed Zoning Map Changes and Special Permits",
    subtitle = "Annual ZAP project-record count: completed ULURP records with ZM or ZS in actions or ULURP numbers",
    x = NULL,
    y = "Completed project records",
    color = NULL
  ) +
  theme_minimal(base_size = 11) +
  theme(
    legend.position = "bottom",
    panel.grid.minor.x = element_blank(),
    panel.grid.minor.y = element_blank()
  )

ggsave("../output/zap_zoning_map_special_permit_all_zm_zs_lines.pdf", all_zm_zs_line_plot, width = 7.5, height = 4.5)

zm_plus_residential_zs_line_plot <- year_counts |>
  filter(count_scope == "zm_plus_residential_zs") |>
  ggplot(aes(x = completed_year, y = project_count, color = zoning_category)) +
  geom_line(linewidth = 0.8) +
  geom_point(size = 1.1) +
  scale_color_manual(
    values = c(
      "Increased Residential" = "#12A69C",
      "Minor Residential" = "#087684",
      "Nonresidential/Downzoning" = "#7DC36D"
    )
  ) +
  scale_x_continuous(breaks = seq(1980, 2020, by = 10), minor_breaks = seq(1976, 2025, by = 1)) +
  labs(
    title = "Completed Zoning Map Changes and Residential Special Permits",
    subtitle = "Annual ZAP count: all completed ZM records plus ZS records only when ZAP text indicates residential/housing content",
    x = NULL,
    y = "Completed project records",
    color = NULL
  ) +
  theme_minimal(base_size = 11) +
  theme(
    legend.position = "bottom",
    panel.grid.minor.x = element_blank(),
    panel.grid.minor.y = element_blank()
  )

ggsave("../output/zap_zoning_map_special_permit_zm_plus_residential_zs_lines.pdf", zm_plus_residential_zs_line_plot, width = 7.5, height = 4.5)

housing_focus_line_plot <- housing_focus_year_counts |>
  ggplot(aes(x = completed_year, y = project_count, color = outcome_label)) +
  geom_line(linewidth = 0.8) +
  scale_color_manual(
    values = c(
      "Increased residential" = "#12A69C",
      "Increased residential with mixed-use text" = "#087684",
      "Mixed-use text, any category" = "#3F88C5",
      "Residential signal only / weakly classified" = "#B7A04B"
    )
  ) +
  scale_x_continuous(breaks = seq(1980, 2020, by = 10), minor_breaks = seq(1976, 2025, by = 1)) +
  labs(
    title = "Residential and Mixed-Use Zoning Actions",
    subtitle = "Annual counts within the narrower ZM plus residential-ZS denominator",
    x = NULL,
    y = "Completed project records",
    color = NULL
  ) +
  theme_minimal(base_size = 11) +
  theme(
    legend.position = "bottom",
    panel.grid.minor.x = element_blank(),
    panel.grid.minor.y = element_blank()
  )

ggsave("../output/zap_zoning_map_special_permit_housing_focus_lines.pdf", housing_focus_line_plot, width = 7.5, height = 4.5)

homeowner_tercile_line_plot <- homeowner_tercile_year_counts |>
  ggplot(aes(x = completed_year, y = project_count, color = ccd2010_homeowner_tercile_label)) +
  geom_line(linewidth = 0.8) +
  scale_color_manual(
    values = c(
      "Low homeowner" = "#2B8CBE",
      "Middle homeowner" = "#7B7B7B",
      "High homeowner" = "#D95F0E"
    )
  ) +
  scale_x_continuous(breaks = seq(1980, 2020, by = 10), minor_breaks = seq(1976, 2025, by = 1)) +
  labs(
    title = "Increased Residential Zoning Actions by 2010 Council District Homeowner Tercile",
    subtitle = "Annual completed ZM plus residential-ZS project records, assigned by BBL to 2010 Council districts; 17 districts per tercile",
    x = NULL,
    y = "Completed project records",
    color = NULL
  ) +
  theme_minimal(base_size = 11) +
  theme(
    legend.position = "bottom",
    panel.grid.minor.x = element_blank(),
    panel.grid.minor.y = element_blank()
  )

ggsave("../output/zap_zoning_map_special_permit_increased_residential_homeowner_tercile_lines.pdf", homeowner_tercile_line_plot, width = 7.5, height = 4.5)

homeowner_tercile_line_3yr_plot <- homeowner_tercile_year_smoothed |>
  ggplot(aes(x = completed_year, y = project_count_3yr, color = ccd2010_homeowner_tercile_label)) +
  geom_line(linewidth = 0.9) +
  scale_color_manual(
    values = c(
      "Low homeowner" = "#2B8CBE",
      "Middle homeowner" = "#7B7B7B",
      "High homeowner" = "#D95F0E"
    )
  ) +
  scale_x_continuous(breaks = seq(1980, 2020, by = 10), minor_breaks = seq(1977, 2024, by = 1)) +
  labs(
    title = "Increased Residential Zoning Actions by 2010 Council District Homeowner Tercile",
    subtitle = "Centered 3-year moving average; completed ZM plus residential-ZS project records, assigned by BBL to 2010 Council districts",
    x = NULL,
    y = "Completed project records, 3-year moving average",
    color = NULL
  ) +
  theme_minimal(base_size = 11) +
  theme(
    legend.position = "bottom",
    panel.grid.minor.x = element_blank(),
    panel.grid.minor.y = element_blank()
  )

ggsave("../output/zap_zoning_map_special_permit_increased_residential_homeowner_tercile_lines_3yr.pdf", homeowner_tercile_line_3yr_plot, width = 7.5, height = 4.5)

increased_residential_project_ids <- project_scope_rows |>
  filter(count_scope == "zm_plus_residential_zs", increased_residential_proxy) |>
  distinct(project_id)

increased_residential_assigned_project_count <- increased_residential_project_ids |>
  inner_join(project_ccd2010_fractional |> distinct(project_id), by = "project_id", relationship = "one-to-one") |>
  nrow()

qc_df <- bind_rows(
  tibble(
    metric = "classified_project_row_count",
    value = as.character(nrow(zap_projects)),
    status = if_else(nrow(zap_projects) > 0, "pass", "fail"),
    note = "Completed 1976-2025 ZAP ULURP project records with a recovered ZM or ZS code."
  ),
  tibble(
    metric = "classified_project_duplicate_id_count",
    value = as.character(nrow(zap_projects) - n_distinct(zap_projects$project_id)),
    status = if_else(nrow(zap_projects) == n_distinct(zap_projects$project_id), "pass", "fail"),
    note = "Project classification must remain unique by project_id."
  ),
  tibble(
    metric = "all_zm_zs_project_count",
    value = as.character(sum(zap_projects$included_all_zm_zs, na.rm = TRUE)),
    status = "pass",
    note = "Literal denominator: completed project records with ZM or ZS in actions or ULURP numbers."
  ),
  tibble(
    metric = "zm_plus_residential_zs_project_count",
    value = as.character(sum(zap_projects$included_zm_plus_residential_zs, na.rm = TRUE)),
    status = "pass",
    note = "Narrower denominator: completed ZM records plus completed ZS records with residential/housing text proxy."
  ),
  tibble(
    metric = "zm_plus_residential_zs_mixed_use_project_count",
    value = as.character(sum(zap_projects$included_zm_plus_residential_zs & zap_projects$mixed_use_text_flag, na.rm = TRUE)),
    status = "pass",
    note = "Projects in the narrower denominator whose ZAP name/brief/applicant/lead-agency text includes mixed-use language."
  ),
  tibble(
    metric = "ccd2010_homeowner_tercile_district_counts",
    value = paste0(council_homeowner_tercile_counts$ccd2010_homeowner_tercile_label, "=", council_homeowner_tercile_counts$council_district_count, collapse = ";"),
    status = if_else(all(council_homeowner_tercile_counts$council_district_count == 17), "pass", "fail"),
    note = "Homeowner terciles use the 2010 Council-district homeownership measure and contain 17 districts each."
  ),
  tibble(
    metric = "project_ccd2010_fractional_weight_bad_count",
    value = as.character(project_ccd2010_weight_bad_count),
    status = if_else(project_ccd2010_weight_bad_count == 0, "pass", "fail"),
    note = "Assigned project weights should sum to one across 2010 Council districts."
  ),
  tibble(
    metric = "increased_residential_ccd2010_assigned_project_count",
    value = as.character(increased_residential_assigned_project_count),
    status = "pass",
    note = "Increased residential projects in the narrower denominator with at least one BBL assigned to a 2010 Council district."
  ),
  tibble(
    metric = "increased_residential_missing_ccd2010_project_count",
    value = as.character(nrow(increased_residential_project_ids) - increased_residential_assigned_project_count),
    status = "pass",
    note = "Increased residential projects in the narrower denominator without a BBL-based 2010 Council district assignment."
  ),
  tibble(
    metric = "increased_residential_definition",
    value = "text_proxy",
    status = "pass",
    note = "Increased Residential is not a native ZAP field. It requires a housing proxy and an MIH, unit-count, residential-growth, or rezoning-to-residential text signal, excluding explicit downzoning/restriction text."
  ),
  tibble(
    metric = "minor_residential_definition",
    value = "text_proxy",
    status = "pass",
    note = "Minor Residential is a housing proxy without the increased-residential signals and without explicit downzoning/restriction text."
  ),
  tibble(
    metric = "decade_year_denominators",
    value = "1970s=4;1980s=10;1990s=10;2000s=10;2010s=10;2020s=6",
    status = "pass",
    note = "The ZAP support window here is 1976-2025, so the 1970s and 2020s are partial decades."
  )
)

write_csv_if_changed(qc_df, "../output/zap_zoning_map_special_permit_qc.csv")

cat("Wrote ZAP zoning map/special permit decade summaries to ../output\n")
