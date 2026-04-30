SHELL := /bin/bash
.DEFAULT_GOAL := all
.NOTPARALLEL:
SKIP_FETCH ?= 0

ifeq ($(SKIP_FETCH),1)
WORKFLOW_FETCH_PHASE :=
FETCH_CENSUS_BPS :=
FETCH_DCP_BOUNDARIES :=
FETCH_DCP_CD_PROFILES_1990_2000 :=
FETCH_DCP_HOUSING_DATABASE :=
FETCH_DOB_OPEN_DATA :=
FETCH_DOB_PERMIT_ISSUANCE_CURRENT :=
FETCH_MAPPLUTO_ARCHIVE :=
FETCH_NHGIS_EXTRACTS :=
FETCH_ZAP_DATASETS :=
else
WORKFLOW_FETCH_PHASE := fetch
FETCH_CENSUS_BPS := fetch_census_bps
FETCH_DCP_BOUNDARIES := fetch_dcp_boundaries
FETCH_DCP_CD_PROFILES_1990_2000 := fetch_dcp_cd_profiles_1990_2000
FETCH_DCP_HOUSING_DATABASE := fetch_dcp_housing_database
FETCH_DOB_OPEN_DATA := fetch_dob_open_data
FETCH_DOB_PERMIT_ISSUANCE_CURRENT := fetch_dob_permit_issuance_current
FETCH_MAPPLUTO_ARCHIVE := fetch_mappluto_archive
FETCH_NHGIS_EXTRACTS := fetch_nhgis_extracts
FETCH_ZAP_DATASETS := fetch_zap_datasets
endif

.PHONY: all cached-all workflow paper paper-dependencies setup fetch load-raw clean-data build-datasets estimate summarize write check-dag rebuild-check-help
.PHONY: setup_environment source_registry archive_locator
.PHONY: fetch_census_bps fetch_dcp_boundaries fetch_dcp_cd_profiles_1990_2000 fetch_dcp_housing_database fetch_dob_open_data fetch_dob_permit_issuance_current fetch_mappluto_archive fetch_nhgis_extracts fetch_zap_datasets
.PHONY: load_archival_records_raw load_census_bps_raw load_dcp_boundaries_raw load_dcp_cd_profiles_1990_2000_raw load_dcp_housing_database_raw load_dob_open_data_raw load_dob_permit_issuance_current_raw load_mappluto_raw load_nhgis_raw load_zap_raw
.PHONY: stage_archival_records stage_census_bps stage_dcp_boundaries stage_dcp_cd_profiles_1990_2000 stage_dcp_housing_database stage_dob_open_data stage_mappluto_lots stage_nhgis stage_zap_datasets
.PHONY: build_dob_permit_issuance_harmonized build_cd_homeownership_1990_measure build_cd_baseline_1990_controls build_mappluto_construction_proxy cd_homeownership_permit_nb_panel build_zap_housing_cohort_base cd_homeownership_dcp_supply_panel build_cd_homeownership_exact_decadal_validation build_cd_homeownership_long_units_series build_zap_housing_hdb_link redevelopment_potential_first_pass build_brooklyn_homeownership_case_study_controls build_zap_ulurp_redev_base
.PHONY: estimate_zap_housing_cohorts estimate_zap_ulurp_redev_pipeline
.PHONY: summarize_zap_datasets summarize_cd_homeownership_proxy_overlap_validation summarize_zap_housing_cohorts summarize_cd_homeownership_long_units_anatomy summarize_cd_homeownership_long_units_levels_developability summarize_cd_homeownership_long_units_sensitivity summarize_cd_homeownership_long_units_series summarize_cd_homeownership_long_units_borough_details summarize_zap_housing_hdb_link summarize_zap_ulurp_redev_pipeline summarize_brooklyn_homeownership_case_study
.PHONY: write_zap_ulurp_redev_memo write_brooklyn_homeownership_case_study_memo

all: workflow paper

cached-all:
	$(MAKE) SKIP_FETCH=1 all

workflow: setup $(WORKFLOW_FETCH_PHASE) load-raw clean-data build-datasets estimate summarize write

paper: paper-dependencies
	$(MAKE) -C paper

paper-dependencies: redevelopment_potential_first_pass summarize_cd_homeownership_long_units_series summarize_brooklyn_homeownership_case_study

setup: setup_environment source_registry archive_locator

fetch: fetch_census_bps fetch_dcp_boundaries fetch_dcp_cd_profiles_1990_2000 fetch_dcp_housing_database fetch_dob_open_data fetch_dob_permit_issuance_current fetch_mappluto_archive fetch_nhgis_extracts fetch_zap_datasets

load-raw: load_archival_records_raw load_census_bps_raw load_dcp_boundaries_raw load_dcp_cd_profiles_1990_2000_raw load_dcp_housing_database_raw load_dob_open_data_raw load_dob_permit_issuance_current_raw load_mappluto_raw load_nhgis_raw load_zap_raw

clean-data: stage_archival_records stage_census_bps stage_dcp_boundaries stage_dcp_cd_profiles_1990_2000 stage_dcp_housing_database stage_dob_open_data stage_mappluto_lots stage_nhgis stage_zap_datasets

build-datasets: build_dob_permit_issuance_harmonized build_cd_homeownership_1990_measure build_cd_baseline_1990_controls build_mappluto_construction_proxy cd_homeownership_permit_nb_panel build_zap_housing_cohort_base cd_homeownership_dcp_supply_panel build_cd_homeownership_exact_decadal_validation build_cd_homeownership_long_units_series build_zap_housing_hdb_link redevelopment_potential_first_pass build_brooklyn_homeownership_case_study_controls build_zap_ulurp_redev_base

estimate: estimate_zap_housing_cohorts estimate_zap_ulurp_redev_pipeline

summarize: summarize_zap_datasets summarize_cd_homeownership_proxy_overlap_validation summarize_zap_housing_cohorts summarize_cd_homeownership_long_units_anatomy summarize_cd_homeownership_long_units_levels_developability summarize_cd_homeownership_long_units_sensitivity summarize_cd_homeownership_long_units_series summarize_cd_homeownership_long_units_borough_details summarize_zap_housing_hdb_link summarize_zap_ulurp_redev_pipeline summarize_brooklyn_homeownership_case_study

write: write_zap_ulurp_redev_memo write_brooklyn_homeownership_case_study_memo

check-dag:
	$(MAKE) -n all

rebuild-check-help:
	@printf '%s\n' 'Safe rebuild proof:'
	@printf '%s\n' '1. Create a temporary copy outside this working tree.'
	@printf '%s\n' '2. Preserve data_raw/ and existing fetch-task outputs for a cached-source proof.'
	@printf '%s\n' '3. In the temp copy, remove generated non-fetch task input/, output/, and temp/ contents.'
	@printf '%s\n' '4. Run make -n cached-all, then make cached-all, from the temp copy root.'
	@printf '%s\n' '5. Compare regenerated figures, tables, QC files, and paper/paper.pdf to this working tree.'

setup_environment:
	$(MAKE) -C tasks/setup_environment/code

source_registry: setup_environment
	$(MAKE) -C tasks/source_registry/code

archive_locator: source_registry
	$(MAKE) -C tasks/archive_locator/code

fetch_census_bps: source_registry
	$(MAKE) -C tasks/fetch_census_bps/code

fetch_dcp_boundaries: source_registry
	$(MAKE) -C tasks/fetch_dcp_boundaries/code

fetch_dcp_cd_profiles_1990_2000: source_registry
	$(MAKE) -C tasks/fetch_dcp_cd_profiles_1990_2000/code

fetch_dcp_housing_database: source_registry
	$(MAKE) -C tasks/fetch_dcp_housing_database/code

fetch_dob_open_data: source_registry
	$(MAKE) -C tasks/fetch_dob_open_data/code

fetch_dob_permit_issuance_current: source_registry
	$(MAKE) -C tasks/fetch_dob_permit_issuance_current/code

fetch_mappluto_archive: source_registry
	$(MAKE) -C tasks/fetch_mappluto_archive/code

fetch_nhgis_extracts: source_registry
	$(MAKE) -C tasks/fetch_nhgis_extracts/code

fetch_zap_datasets: source_registry
	$(MAKE) -C tasks/fetch_zap_datasets/code

load_archival_records_raw: source_registry
	$(MAKE) -C tasks/load_archival_records_raw/code

load_census_bps_raw: $(FETCH_CENSUS_BPS)
	$(MAKE) -C tasks/load_census_bps_raw/code

load_dcp_boundaries_raw: $(FETCH_DCP_BOUNDARIES)
	$(MAKE) -C tasks/load_dcp_boundaries_raw/code

load_dcp_cd_profiles_1990_2000_raw: $(FETCH_DCP_CD_PROFILES_1990_2000)
	$(MAKE) -C tasks/load_dcp_cd_profiles_1990_2000_raw/code

load_dcp_housing_database_raw: $(FETCH_DCP_HOUSING_DATABASE)
	$(MAKE) -C tasks/load_dcp_housing_database_raw/code

load_dob_open_data_raw: $(FETCH_DOB_OPEN_DATA)
	$(MAKE) -C tasks/load_dob_open_data_raw/code

load_dob_permit_issuance_current_raw: $(FETCH_DOB_PERMIT_ISSUANCE_CURRENT)
	$(MAKE) -C tasks/load_dob_permit_issuance_current_raw/code

load_mappluto_raw: $(FETCH_MAPPLUTO_ARCHIVE)
	$(MAKE) -C tasks/load_mappluto_raw/code

load_nhgis_raw: $(FETCH_NHGIS_EXTRACTS)
	$(MAKE) -C tasks/load_nhgis_raw/code

load_zap_raw: $(FETCH_ZAP_DATASETS)
	$(MAKE) -C tasks/load_zap_raw/code

stage_archival_records: source_registry load_archival_records_raw
	$(MAKE) -C tasks/stage_archival_records/code

stage_census_bps: load_census_bps_raw
	$(MAKE) -C tasks/stage_census_bps/code

stage_dcp_boundaries: load_dcp_boundaries_raw
	$(MAKE) -C tasks/stage_dcp_boundaries/code

stage_dcp_cd_profiles_1990_2000: load_dcp_cd_profiles_1990_2000_raw
	$(MAKE) -C tasks/stage_dcp_cd_profiles_1990_2000/code

stage_dcp_housing_database: load_dcp_housing_database_raw
	$(MAKE) -C tasks/stage_dcp_housing_database/code

stage_dob_open_data: load_dob_open_data_raw
	$(MAKE) -C tasks/stage_dob_open_data/code

stage_mappluto_lots: load_mappluto_raw
	$(MAKE) -C tasks/stage_mappluto_lots/code

stage_nhgis: load_nhgis_raw
	$(MAKE) -C tasks/stage_nhgis/code

stage_zap_datasets: load_zap_raw
	$(MAKE) -C tasks/stage_zap_datasets/code

build_dob_permit_issuance_harmonized: load_dob_permit_issuance_current_raw stage_census_bps
	$(MAKE) -C tasks/build_dob_permit_issuance_harmonized/code

build_cd_homeownership_1990_measure: stage_dcp_cd_profiles_1990_2000
	$(MAKE) -C tasks/build_cd_homeownership_1990_measure/code

build_cd_baseline_1990_controls: build_cd_homeownership_1990_measure stage_dcp_boundaries stage_dcp_cd_profiles_1990_2000 stage_nhgis
	$(MAKE) -C tasks/build_cd_baseline_1990_controls/code

build_mappluto_construction_proxy: build_cd_homeownership_1990_measure stage_mappluto_lots
	$(MAKE) -C tasks/build_mappluto_construction_proxy/code

cd_homeownership_permit_nb_panel: build_cd_homeownership_1990_measure build_dob_permit_issuance_harmonized
	$(MAKE) -C tasks/cd_homeownership_permit_nb_panel/code

build_zap_housing_cohort_base: build_cd_baseline_1990_controls build_cd_homeownership_1990_measure stage_zap_datasets
	$(MAKE) -C tasks/build_zap_housing_cohort_base/code

cd_homeownership_dcp_supply_panel: build_cd_baseline_1990_controls build_cd_homeownership_1990_measure stage_dcp_housing_database
	$(MAKE) -C tasks/cd_homeownership_dcp_supply_panel/code

build_cd_homeownership_exact_decadal_validation: build_cd_homeownership_1990_measure build_mappluto_construction_proxy stage_dcp_cd_profiles_1990_2000
	$(MAKE) -C tasks/build_cd_homeownership_exact_decadal_validation/code

build_cd_homeownership_long_units_series: build_cd_baseline_1990_controls build_cd_homeownership_1990_measure build_mappluto_construction_proxy stage_dcp_housing_database
	$(MAKE) -C tasks/build_cd_homeownership_long_units_series/code

build_zap_housing_hdb_link: build_zap_housing_cohort_base stage_dcp_housing_database stage_zap_datasets
	$(MAKE) -C tasks/build_zap_housing_hdb_link/code

redevelopment_potential_first_pass: build_cd_baseline_1990_controls build_cd_homeownership_1990_measure build_cd_homeownership_long_units_series build_mappluto_construction_proxy cd_homeownership_dcp_supply_panel cd_homeownership_permit_nb_panel stage_dcp_boundaries stage_dcp_housing_database stage_mappluto_lots
	$(MAKE) -C tasks/redevelopment_potential_first_pass/code

build_brooklyn_homeownership_case_study_controls: build_cd_baseline_1990_controls redevelopment_potential_first_pass stage_dcp_boundaries stage_nhgis
	$(MAKE) -C tasks/build_brooklyn_homeownership_case_study_controls/code

build_zap_ulurp_redev_base: build_zap_housing_cohort_base build_zap_housing_hdb_link redevelopment_potential_first_pass stage_zap_datasets
	$(MAKE) -C tasks/build_zap_ulurp_redev_base/code

estimate_zap_housing_cohorts: summarize_zap_housing_cohorts
	$(MAKE) -C tasks/estimate_zap_housing_cohorts/code

estimate_zap_ulurp_redev_pipeline: summarize_zap_ulurp_redev_pipeline
	$(MAKE) -C tasks/estimate_zap_ulurp_redev_pipeline/code

summarize_zap_datasets: stage_zap_datasets
	$(MAKE) -C tasks/summarize_zap_datasets/code

summarize_cd_homeownership_proxy_overlap_validation: build_cd_homeownership_1990_measure build_mappluto_construction_proxy stage_dcp_housing_database
	$(MAKE) -C tasks/summarize_cd_homeownership_proxy_overlap_validation/code

summarize_zap_housing_cohorts: build_zap_housing_cohort_base
	$(MAKE) -C tasks/summarize_zap_housing_cohorts/code

summarize_cd_homeownership_long_units_anatomy: build_cd_homeownership_long_units_series
	$(MAKE) -C tasks/summarize_cd_homeownership_long_units_anatomy/code

summarize_cd_homeownership_long_units_levels_developability: build_cd_baseline_1990_controls build_cd_homeownership_long_units_series build_mappluto_construction_proxy stage_dcp_housing_database stage_mappluto_lots
	$(MAKE) -C tasks/summarize_cd_homeownership_long_units_levels_developability/code

summarize_cd_homeownership_long_units_sensitivity: build_cd_homeownership_long_units_series build_mappluto_construction_proxy stage_dcp_housing_database
	$(MAKE) -C tasks/summarize_cd_homeownership_long_units_sensitivity/code

summarize_cd_homeownership_long_units_series: build_cd_homeownership_long_units_series
	$(MAKE) -C tasks/summarize_cd_homeownership_long_units_series/code

summarize_cd_homeownership_long_units_borough_details: build_cd_homeownership_exact_decadal_validation build_cd_homeownership_long_units_series build_mappluto_construction_proxy stage_dcp_housing_database summarize_cd_homeownership_proxy_overlap_validation
	$(MAKE) -C tasks/summarize_cd_homeownership_long_units_borough_details/code

summarize_zap_housing_hdb_link: build_zap_housing_hdb_link
	$(MAKE) -C tasks/summarize_zap_housing_hdb_link/code

summarize_zap_ulurp_redev_pipeline: build_zap_housing_hdb_link build_zap_ulurp_redev_base
	$(MAKE) -C tasks/summarize_zap_ulurp_redev_pipeline/code

summarize_brooklyn_homeownership_case_study: build_brooklyn_homeownership_case_study_controls build_cd_homeownership_long_units_series cd_homeownership_dcp_supply_panel cd_homeownership_permit_nb_panel stage_dcp_boundaries summarize_zap_ulurp_redev_pipeline
	$(MAKE) -C tasks/summarize_brooklyn_homeownership_case_study/code

write_zap_ulurp_redev_memo: build_zap_ulurp_redev_base estimate_zap_ulurp_redev_pipeline summarize_zap_ulurp_redev_pipeline
	$(MAKE) -C tasks/write_zap_ulurp_redev_memo/code

write_brooklyn_homeownership_case_study_memo: summarize_brooklyn_homeownership_case_study
	$(MAKE) -C tasks/write_brooklyn_homeownership_case_study_memo/code
