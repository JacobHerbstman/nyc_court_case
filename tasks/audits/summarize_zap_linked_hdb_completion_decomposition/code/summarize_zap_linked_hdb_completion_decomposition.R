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

source("../../../_lib/source_pipeline_utils.R")

assert_unique_keys <- function(df, key_cols, df_name) {
  duplicate_keys <- df %>%
    count(across(all_of(key_cols)), name = "row_count") %>%
    filter(row_count > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(df_name, " is not unique by ", paste(key_cols, collapse = ", "), ".")
  }
}

clean_bbl_string <- function(x) {
  out <- str_squish(as.character(x))
  out[out == "" | out %in% c("NA", "NaN")] <- NA_character_
  out
}

valid_bbl_string <- function(x) {
  !is.na(x) & str_detect(x, "^[1-5][0-9]{9}$")
}

coefficient_row <- function(df, outcome_name, link_status_name, period_name) {
  model_df <- df %>%
    filter(outcome == outcome_name, link_status == link_status_name, period == period_name) %>%
    group_by(borocd, borough_name, treat_z_boro) %>%
    summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
    filter(is.finite(value), !is.na(treat_z_boro), !is.na(borough_name))

  if (nrow(model_df) < 10 || n_distinct(model_df$borough_name) < 2 || n_distinct(model_df$value) < 2) {
    return(tibble(
      outcome = outcome_name,
      link_status = link_status_name,
      period = period_name,
      estimate = NA_real_,
      std_error = NA_real_,
      p_value = NA_real_,
      cds = n_distinct(model_df$borocd)
    ))
  }

  fit <- feols(value ~ treat_z_boro | borough_name, data = model_df, vcov = "hetero")
  ct <- coeftable(fit)

  tibble(
    outcome = outcome_name,
    link_status = link_status_name,
    period = period_name,
    estimate = unname(ct["treat_z_boro", "Estimate"]),
    std_error = unname(ct["treat_z_boro", "Std. Error"]),
    p_value = unname(ct["treat_z_boro", "Pr(>|t|)"]),
    cds = n_distinct(model_df$borocd)
  )
}

cd_base <- read_csv("../input/cd_redevelopment_potential_baseline.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  transmute(
    borocd = as.integer(borocd),
    borough_name = as.character(borough_name),
    occupied_units_1990 = as.numeric(occupied_units_1990),
    residential_acres = as.numeric(residential_acres),
    treat_z_boro = as.numeric(treat_z_boro),
    treat_pp = as.numeric(treat_pp)
  ) %>%
  distinct(borocd, .keep_all = TRUE)

assert_unique_keys(cd_base, "borocd", "CD denominator/treatment table")

if (nrow(cd_base) != 59) {
  stop("Expected 59 CDs in denominator/treatment table.")
}

if (any(is.na(cd_base$treat_z_boro)) || any(is.na(cd_base$occupied_units_1990)) || any(is.na(cd_base$residential_acres))) {
  stop("Treatment and denominator fields must be nonmissing.")
}

district_lookup <- cd_base %>%
  group_by(borough_name) %>%
  mutate(
    treat_tercile = ntile(treat_pp, 3),
    treat_tercile_label = case_when(
      treat_tercile == 1 ~ "Low",
      treat_tercile == 2 ~ "Middle",
      TRUE ~ "High"
    )
  ) %>%
  ungroup()

hdb_jobs <- read_parquet(
  "../input/dcp_housing_database_project_level_25q4.parquet",
  col_select = c("job_number", "job_type", "completion_year", "permit_year", "classa_prop", "classa_net", "community_district", "bbl")
) %>%
  as.data.frame() %>%
  as_tibble() %>%
  transmute(
    job_number = as.character(job_number),
    job_type = str_squish(as.character(job_type)),
    completion_year = suppressWarnings(as.integer(completion_year)),
    permit_year = suppressWarnings(as.integer(permit_year)),
    classa_prop = suppressWarnings(as.numeric(classa_prop)),
    classa_net = suppressWarnings(as.numeric(classa_net)),
    borocd = suppressWarnings(as.integer(community_district)),
    bbl_raw = as.character(bbl),
    bbl = clean_bbl_string(bbl),
    valid_bbl = valid_bbl_string(bbl)
  ) %>%
  filter(job_type == "New Building", classa_prop >= 50, completion_year >= 2010, completion_year <= 2025)

assert_unique_keys(hdb_jobs %>% filter(!is.na(job_number)), "job_number", "50+ unit HDB completion jobs")

zap_links <- read_csv("../input/zap_housing_hdb_link_candidates.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  transmute(
    job_number = as.character(job_number),
    project_id = as.character(project_id),
    permit_lag = suppressWarnings(as.integer(permit_lag)),
    assignment_timing = as.character(assignment_timing),
    within_neg2_10 = as.logical(within_neg2_10),
    within_neg5_15 = as.logical(within_neg5_15)
  ) %>%
  filter(!is.na(job_number))

assert_unique_keys(zap_links, "job_number", "Assigned ZAP-HDB job links")

link_status_levels <- c(
  "ZAP-linked, preferred timing",
  "ZAP BBL match, broad timing only",
  "ZAP BBL match, outside timing",
  "No assigned exact ZAP BBL match",
  "Missing/invalid HDB geography"
)

hdb_classified <- hdb_jobs %>%
  left_join(zap_links, by = "job_number", relationship = "many-to-one") %>%
  mutate(
    link_status = case_when(
      is.na(job_number) | is.na(borocd) | !valid_bbl ~ "Missing/invalid HDB geography",
      !is.na(project_id) & within_neg2_10 %in% TRUE ~ "ZAP-linked, preferred timing",
      !is.na(project_id) & within_neg5_15 %in% TRUE ~ "ZAP BBL match, broad timing only",
      !is.na(project_id) ~ "ZAP BBL match, outside timing",
      TRUE ~ "No assigned exact ZAP BBL match"
    ),
    link_status = factor(link_status, levels = link_status_levels)
  )

year_link_panel <- hdb_classified %>%
  group_by(borocd, year = completion_year, link_status) %>%
  summarise(
    nb_50plus_jobs = n_distinct(job_number),
    nb_50plus_units = sum(classa_prop, na.rm = TRUE),
    .groups = "drop"
  )

cd_year <- crossing(
  borocd = district_lookup$borocd,
  year = 2010:2025,
  link_status = link_status_levels
) %>%
  left_join(district_lookup, by = "borocd", relationship = "many-to-one") %>%
  left_join(year_link_panel, by = c("borocd", "year", "link_status"), relationship = "many-to-one") %>%
  mutate(
    nb_50plus_jobs = coalesce(nb_50plus_jobs, 0L),
    nb_50plus_units = coalesce(nb_50plus_units, 0),
    jobs_per_10000_occ_1990 = 10000 * nb_50plus_jobs / occupied_units_1990,
    units_per_10000_occ_1990 = 10000 * nb_50plus_units / occupied_units_1990,
    jobs_per_residential_acre = nb_50plus_jobs / residential_acres,
    units_per_residential_acre = nb_50plus_units / residential_acres,
    period = case_when(
      year >= 2010 & year <= 2014 ~ "2010-2014",
      year >= 2015 & year <= 2019 ~ "2015-2019",
      year >= 2020 & year <= 2025 ~ "2020-2025",
      TRUE ~ NA_character_
    ),
    link_status = factor(link_status, levels = link_status_levels)
  ) %>%
  arrange(year, borocd, link_status)

long_rates <- cd_year %>%
  select(
    borocd, borough_name, treat_z_boro, period, link_status,
    jobs_per_10000_occ_1990, units_per_10000_occ_1990,
    jobs_per_residential_acre, units_per_residential_acre
  ) %>%
  mutate(link_status = as.character(link_status)) %>%
  pivot_longer(
    cols = c(jobs_per_10000_occ_1990, units_per_10000_occ_1990, jobs_per_residential_acre, units_per_residential_acre),
    names_to = "outcome",
    values_to = "value"
  )

coefficients <- bind_rows(lapply(unique(long_rates$outcome), function(outcome_name) {
  bind_rows(lapply(link_status_levels, function(link_status_name) {
    bind_rows(lapply(unique(long_rates$period), function(period_name) {
      coefficient_row(long_rates, outcome_name, link_status_name, period_name)
    }))
  }))
}))

period_years <- cd_year %>%
  distinct(period, year) %>%
  count(period, name = "period_years")

period_cd <- cd_year %>%
  group_by(period, link_status, borocd) %>%
  summarise(
    period_jobs = sum(nb_50plus_jobs, na.rm = TRUE),
    period_units = sum(nb_50plus_units, na.rm = TRUE),
    occupied_units_1990 = first(occupied_units_1990),
    residential_acres = first(residential_acres),
    .groups = "drop"
  ) %>%
  mutate(
    period_jobs_per_10000_occ_1990 = 10000 * period_jobs / occupied_units_1990,
    period_units_per_10000_occ_1990 = 10000 * period_units / occupied_units_1990,
    period_jobs_per_residential_acre = period_jobs / residential_acres,
    period_units_per_residential_acre = period_units / residential_acres
  )

summary_df <- cd_year %>%
  group_by(period, link_status) %>%
  summarise(
    total_jobs = sum(nb_50plus_jobs, na.rm = TRUE),
    total_units = sum(nb_50plus_units, na.rm = TRUE),
    mean_annual_units_per_10000_occ_1990 = mean(units_per_10000_occ_1990, na.rm = TRUE),
    mean_annual_units_per_residential_acre = mean(units_per_residential_acre, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(period_years, by = "period", relationship = "many-to-one") %>%
  left_join(
    period_cd %>%
      group_by(period, link_status) %>%
      summarise(
        mean_period_units_per_10000_occ_1990 = mean(period_units_per_10000_occ_1990, na.rm = TRUE),
        mean_period_units_per_residential_acre = mean(period_units_per_residential_acre, na.rm = TRUE),
        .groups = "drop"
      ),
    by = c("period", "link_status"),
    relationship = "one-to-one"
  ) %>%
  arrange(period, link_status)

plot_df <- cd_year %>%
  group_by(year, link_status, treat_tercile_label) %>%
  summarise(
    units_per_10000_occ_1990 = mean(units_per_10000_occ_1990, na.rm = TRUE),
    units_per_residential_acre = mean(units_per_residential_acre, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  pivot_longer(
    cols = c(units_per_10000_occ_1990, units_per_residential_acre),
    names_to = "scale",
    values_to = "value"
  ) %>%
  mutate(
    treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")),
    link_status = factor(link_status, levels = link_status_levels),
    scale_label = recode(
      scale,
      units_per_10000_occ_1990 = "Units per 10,000 occupied 1990 units",
      units_per_residential_acre = "Units per baseline residential acre"
    )
  )

plot_obj <- ggplot(plot_df, aes(x = year, y = value, color = treat_tercile_label)) +
  geom_line(linewidth = 0.55, na.rm = TRUE) +
  geom_point(size = 0.9, na.rm = TRUE) +
  facet_wrap(vars(link_status, scale_label), ncol = 2, scales = "free_y") +
  scale_color_manual(values = c("Low" = "#2166ac", "Middle" = "#8c8c8c", "High" = "#d6604d")) +
  labs(
    x = NULL,
    y = NULL,
    color = "1990 homeowner tercile",
    title = "50+ unit new-building completions by exact-BBL ZAP linkage status",
    subtitle = "Completion-year DCP Housing Database jobs; timing categories use permit lag from ZAP certification"
  ) +
  theme_minimal(base_size = 10) +
  theme(legend.position = "bottom")

temp_pdf <- tempfile(fileext = ".pdf")
pdf(temp_pdf, width = 11, height = 10)
print(plot_obj)
dev.off()
copy_if_changed(temp_pdf, "../output/zap_linked_hdb_50plus_tercile_trends.pdf")

write_csv_if_changed(cd_year, "../output/zap_linked_hdb_50plus_cd_year.csv")
write_csv_if_changed(coefficients, "../output/zap_linked_hdb_50plus_coefficients.csv")
write_csv_if_changed(summary_df, "../output/zap_linked_hdb_50plus_summary.csv")

cat("Wrote ZAP-linked HDB completion decomposition outputs to ../output\n")
