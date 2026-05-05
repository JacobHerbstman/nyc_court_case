# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_hdb_public_affordable_421a_split/code")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source("../../_lib/source_pipeline_utils.R")

assert_unique_keys <- function(df, key_cols, df_name) {
  duplicate_keys <- df %>%
    count(across(all_of(key_cols)), name = "row_count") %>%
    filter(row_count > 1)

  if (nrow(duplicate_keys) > 0) {
    stop(df_name, " is not unique by ", paste(key_cols, collapse = ", "), ".")
  }
}

coerce_year_vector <- function(x) {
  if (is.null(x) || length(x) == 0 || all(is.na(x))) {
    return(integer())
  }

  suppressWarnings(as.integer(unlist(x)))
}

count_years_in_window <- function(year_values, lower_bound, upper_bound) {
  mapply(
    function(years, lower, upper) {
      years <- coerce_year_vector(years)
      if (length(years) == 0 || is.na(lower) || is.na(upper)) {
        return(0L)
      }

      sum(!is.na(years) & years >= lower & years <= upper)
    },
    year_values,
    lower_bound,
    upper_bound
  )
}

sum_units_in_window <- function(year_values, unit_values, lower_bound, upper_bound) {
  mapply(
    function(years, units, lower, upper) {
      years <- coerce_year_vector(years)
      units <- suppressWarnings(as.numeric(unlist(units)))

      if (length(years) == 0 || length(units) == 0 || is.na(lower) || is.na(upper)) {
        return(0)
      }

      valid_length <- min(length(years), length(units))
      years <- years[seq_len(valid_length)]
      units <- units[seq_len(valid_length)]
      sum(units[!is.na(years) & years >= lower & years <= upper], na.rm = TRUE)
    },
    year_values,
    unit_values,
    lower_bound,
    upper_bound
  )
}

cd_base <- read_csv("../input/cd_redevelopment_potential_baseline.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  transmute(
    borocd = as.integer(borocd),
    borough_name = as.character(borough_name),
    occupied_units_1990 = as.numeric(occupied_units_1990),
    residential_acres = as.numeric(residential_acres),
    treat_pp = as.numeric(treat_pp),
    treat_z_boro = as.numeric(treat_z_boro)
  ) %>%
  distinct(borocd, .keep_all = TRUE)

assert_unique_keys(cd_base, "borocd", "CD denominator table")

district_lookup <- cd_base %>%
  group_by(borough_name) %>%
  mutate(
    treat_tercile = ntile(treat_pp, 3),
    treat_tercile_label = case_when(treat_tercile == 1 ~ "Low", treat_tercile == 2 ~ "Middle", TRUE ~ "High")
  ) %>%
  ungroup()

hdb_jobs <- read_parquet(
  "../input/dcp_housing_database_project_level_25q4.parquet",
  col_select = c("job_number", "job_type", "permit_year", "completion_year", "classa_prop", "classa_net", "community_district", "bbl")
) %>%
  as.data.frame() %>%
  as_tibble() %>%
  transmute(
    job_number = as.character(job_number),
    job_type = str_squish(as.character(job_type)),
    permit_year = suppressWarnings(as.integer(permit_year)),
    completion_year = suppressWarnings(as.integer(completion_year)),
    classa_prop = suppressWarnings(as.numeric(classa_prop)),
    classa_net = suppressWarnings(as.numeric(classa_net)),
    borocd = suppressWarnings(as.integer(community_district)),
    bbl = as.character(bbl)
  ) %>%
  filter(job_type == "New Building", classa_prop > 0, completion_year >= 2010, completion_year <= 2025)

zap_project_base <- read_csv(
  "../input/zap_housing_project_base_audited.csv",
  col_select = c(project_id, housing_any_hpd_public_housing_apps, housing_any_public_land_disposition_apps),
  col_types = cols(.default = col_character()),
  na = c("", "NA")
) %>%
  transmute(
    project_id = as.character(project_id),
    zap_public_hpd_proxy = as.logical(housing_any_hpd_public_housing_apps),
    zap_public_land_proxy = as.logical(housing_any_public_land_disposition_apps)
  )

assert_unique_keys(zap_project_base, "project_id", "Audited ZAP housing project proxy table")

zap_links_raw <- read_csv("../input/zap_housing_hdb_link_candidates.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  transmute(
    job_number = as.character(job_number),
    project_id = as.character(project_id),
    zap_link_within_neg2_10 = as.logical(within_neg2_10)
  ) %>%
  filter(zap_link_within_neg2_10 %in% TRUE)

assert_unique_keys(zap_links_raw, "job_number", "Assigned ZAP public/proxy links before source merge")

zap_links <- zap_links_raw %>%
  left_join(zap_project_base, by = "project_id", relationship = "many-to-one") %>%
  mutate(
    zap_public_hpd_proxy = coalesce(zap_public_hpd_proxy, FALSE),
    zap_public_land_proxy = coalesce(zap_public_land_proxy, FALSE)
  )

assert_unique_keys(zap_links, "job_number", "Assigned ZAP public/proxy links")

hpd_bbl_year <- read_csv("../input/hpd_affordable_housing_bbl_year.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  transmute(
    bbl = as.character(bbl),
    hpd_year = as.integer(hpd_year),
    hpd_counted_units = as.numeric(hpd_counted_units),
    hpd_total_units = as.numeric(hpd_total_units)
  ) %>%
  distinct(bbl, hpd_year, .keep_all = TRUE)

dof_421a <- read_csv("../input/dof_421a_exempt_bbl_year.csv", show_col_types = FALSE, na = c("", "NA")) %>%
  transmute(
    bbl = as.character(bbl),
    fiscal_year_end = as.integer(fiscal_year_end),
    residential_units_421a = as.numeric(residential_units)
  ) %>%
  distinct(bbl, fiscal_year_end, .keep_all = TRUE)

hpd_bbl_summary <- hpd_bbl_year %>%
  group_by(bbl) %>%
  summarise(
    hpd_year_values = list(hpd_year),
    hpd_counted_unit_values = list(coalesce(hpd_counted_units, 0)),
    hpd_observed_year_min = min(hpd_year, na.rm = TRUE),
    hpd_observed_year_max = max(hpd_year, na.rm = TRUE),
    hpd_observed_year_count = n_distinct(hpd_year),
    .groups = "drop"
  )

tax_bbl_summary <- dof_421a %>%
  group_by(bbl) %>%
  summarise(
    fiscal_year_values = list(fiscal_year_end),
    tax_observed_year_min = min(fiscal_year_end, na.rm = TRUE),
    tax_observed_year_max = max(fiscal_year_end, na.rm = TRUE),
    tax_observed_year_count = n_distinct(fiscal_year_end),
    .groups = "drop"
  )

assert_unique_keys(hpd_bbl_summary, "bbl", "HPD BBL summary")
assert_unique_keys(tax_bbl_summary, "bbl", "421-a BBL summary")

hpd_match <- hdb_jobs %>%
  select(job_number, bbl, permit_year, completion_year) %>%
  left_join(hpd_bbl_summary, by = "bbl", relationship = "many-to-one") %>%
  mutate(
    hpd_year_values = lapply(hpd_year_values, coerce_year_vector),
    hpd_counted_unit_values = lapply(hpd_counted_unit_values, function(x) suppressWarnings(as.numeric(unlist(x)))),
    hpd_match_lower_year = pmax(2014L, coalesce(permit_year, completion_year, 2010L) - 2L),
    hpd_match_upper_year = coalesce(completion_year, permit_year, 2025L) + 5L,
    hpd_match_count = count_years_in_window(hpd_year_values, hpd_match_lower_year, hpd_match_upper_year),
    hpd_counted_units_on_bbl_window = sum_units_in_window(hpd_year_values, hpd_counted_unit_values, hpd_match_lower_year, hpd_match_upper_year),
    hpd_affordable_flag = hpd_match_count > 0
  ) %>%
  select(job_number, hpd_affordable_flag, hpd_match_count, hpd_counted_units_on_bbl_window, hpd_match_lower_year, hpd_match_upper_year, hpd_observed_year_min, hpd_observed_year_max)

tax_match <- hdb_jobs %>%
  select(job_number, bbl, permit_year, completion_year) %>%
  left_join(tax_bbl_summary, by = "bbl", relationship = "many-to-one") %>%
  mutate(
    fiscal_year_values = lapply(fiscal_year_values, coerce_year_vector),
    tax_match_lower_year = coalesce(permit_year, completion_year, 2010L),
    tax_match_upper_year = coalesce(completion_year, permit_year, 2025L) + 8L,
    tax_421a_match_count = count_years_in_window(fiscal_year_values, tax_match_lower_year, tax_match_upper_year),
    tax_421a_flag = tax_421a_match_count > 0
  ) %>%
  select(job_number, tax_421a_flag, tax_421a_match_count, tax_match_lower_year, tax_match_upper_year, tax_observed_year_min, tax_observed_year_max)

job_classified <- hdb_jobs %>%
  left_join(zap_links, by = "job_number", relationship = "many-to-one") %>%
  left_join(hpd_match, by = "job_number", relationship = "one-to-one") %>%
  left_join(tax_match, by = "job_number", relationship = "one-to-one") %>%
  left_join(cd_base, by = "borocd", relationship = "many-to-one") %>%
  mutate(
    hpd_affordable_flag = coalesce(hpd_affordable_flag, FALSE),
    zap_public_hpd_proxy = coalesce(zap_public_hpd_proxy, FALSE),
    zap_public_land_proxy = coalesce(zap_public_land_proxy, FALSE),
    tax_421a_flag = coalesce(tax_421a_flag, FALSE),
    hpd_or_zap_public_hpd = hpd_affordable_flag | zap_public_hpd_proxy,
    priority_category = case_when(
      is.na(borocd) | is.na(bbl) | bbl == "" ~ "uncertain",
      hpd_or_zap_public_hpd ~ "HPD/affordable or public-HPD proxy",
      zap_public_land_proxy ~ "ZAP public land/disposition proxy",
      tax_421a_flag ~ "421-a observed exemption",
      TRUE ~ "residual private/no observed proxy"
    ),
    nb_50plus_flag = classa_prop >= 50,
    units_per_10000_occ_1990 = 10000 * classa_prop / occupied_units_1990,
    units_per_residential_acre = classa_prop / residential_acres
  ) %>%
  arrange(completion_year, borocd, job_number)

cd_year <- crossing(
  borocd = district_lookup$borocd,
  year = 2010:2025,
  priority_category = c("HPD/affordable or public-HPD proxy", "ZAP public land/disposition proxy", "421-a observed exemption", "residual private/no observed proxy", "uncertain"),
  size_margin = c("All new-building units", "50+ unit new-building units")
) %>%
  left_join(district_lookup, by = "borocd", relationship = "many-to-one") %>%
  left_join(
    job_classified %>%
      mutate(year = completion_year) %>%
      bind_rows(job_classified %>% filter(nb_50plus_flag) %>% mutate(year = completion_year, size_margin = "50+ unit new-building units")) %>%
      mutate(size_margin = coalesce(size_margin, "All new-building units")) %>%
      group_by(borocd, year, priority_category, size_margin) %>%
      summarise(
        job_count = n_distinct(job_number),
        gross_units = sum(classa_prop, na.rm = TRUE),
        .groups = "drop"
      ),
    by = c("borocd", "year", "priority_category", "size_margin"),
    relationship = "many-to-one"
  ) %>%
  mutate(
    job_count = coalesce(job_count, 0L),
    gross_units = coalesce(gross_units, 0),
    units_per_10000_occ_1990 = 10000 * gross_units / occupied_units_1990,
    units_per_residential_acre = gross_units / residential_acres
  ) %>%
  arrange(year, borocd, priority_category, size_margin)

summary_df <- job_classified %>%
  group_by(priority_category, nb_50plus_flag) %>%
  summarise(
    jobs = n_distinct(job_number),
    gross_units = sum(classa_prop, na.rm = TRUE),
    hpd_flag_jobs = sum(hpd_affordable_flag, na.rm = TRUE),
    zap_public_hpd_jobs = sum(zap_public_hpd_proxy, na.rm = TRUE),
    zap_public_land_jobs = sum(zap_public_land_proxy, na.rm = TRUE),
    tax_421a_jobs = sum(tax_421a_flag, na.rm = TRUE),
    .groups = "drop"
  )

plot_df <- cd_year %>%
  filter(size_margin == "50+ unit new-building units") %>%
  group_by(year, priority_category, treat_tercile_label) %>%
  summarise(units_per_10000_occ_1990 = mean(units_per_10000_occ_1990, na.rm = TRUE), .groups = "drop") %>%
  mutate(treat_tercile_label = factor(treat_tercile_label, levels = c("Low", "Middle", "High")))

plot_obj <- ggplot(plot_df, aes(x = year, y = units_per_10000_occ_1990, color = treat_tercile_label)) +
  geom_line(linewidth = 0.55, na.rm = TRUE) +
  geom_point(size = 0.8, na.rm = TRUE) +
  facet_wrap(~ priority_category, scales = "free_y", ncol = 1) +
  scale_color_manual(values = c("Low" = "#2166ac", "Middle" = "#8c8c8c", "High" = "#d6604d")) +
  labs(
    x = NULL,
    y = "50+ NB units per 10,000 occupied 1990 units",
    color = "1990 homeowner tercile",
    title = "Post-2010 50+ unit new-building production by public/affordable/421-a proxy"
  ) +
  theme_minimal(base_size = 10) +
  theme(legend.position = "bottom")

temp_pdf <- tempfile(fileext = ".pdf")
pdf(temp_pdf, width = 10.5, height = 10)
print(plot_obj)
dev.off()
copy_if_changed(temp_pdf, "../output/hdb_public_affordable_421a_tercile_trends.pdf")

qc_df <- bind_rows(
  tibble(metric = "job_count", value = nrow(job_classified), status = if_else(nrow(job_classified) > 0, "pass", "fail"), note = "Post-2010 completed HDB new-building jobs classified."),
  tibble(metric = "cd_count", value = n_distinct(cd_year$borocd), status = if_else(n_distinct(cd_year$borocd) == 59, "pass", "fail"), note = "Expected 59 CDs."),
  tibble(metric = "negative_units_count", value = sum(job_classified$classa_prop < 0, na.rm = TRUE), status = if_else(sum(job_classified$classa_prop < 0, na.rm = TRUE) == 0, "pass", "fail"), note = "Unit outcomes must be nonnegative."),
  tibble(metric = "zap_assigned_duplicate_jobs", value = nrow(zap_links) - n_distinct(zap_links$job_number), status = if_else(nrow(zap_links) == n_distinct(zap_links$job_number), "pass", "fail"), note = "ZAP-HDB assigned job links must be unique."),
  tibble(metric = "hpd_affordable_job_count", value = sum(job_classified$hpd_affordable_flag, na.rm = TRUE), status = "pass", note = "Jobs matched to HPD affordable production BBL-years."),
  tibble(metric = "tax_421a_job_count", value = sum(job_classified$tax_421a_flag, na.rm = TRUE), status = "pass", note = "Jobs matched to DOF 421-a exemption BBL-years."),
  tibble(metric = "overlap_hpd_421a_job_count", value = sum(job_classified$hpd_or_zap_public_hpd & job_classified$tax_421a_flag, na.rm = TRUE), status = "pass", note = "Overlap is reported before priority classification.")
)

if (any(qc_df$status == "fail")) {
  write_csv_if_changed(qc_df, "../output/hdb_public_affordable_421a_qc.csv")
  stop("HDB public/affordable/421-a split QC failed.")
}

write_csv_if_changed(job_classified, "../output/hdb_public_affordable_421a_job_classified.csv")
write_csv_if_changed(cd_year, "../output/hdb_public_affordable_421a_cd_year.csv")
write_csv_if_changed(summary_df, "../output/hdb_public_affordable_421a_summary.csv")
write_csv_if_changed(qc_df, "../output/hdb_public_affordable_421a_qc.csv")

cat("Wrote HDB public/affordable/421-a split outputs to ../output\n")
