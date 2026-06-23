SHELL := bash
include ../../shell_functions.make

all: ../output/zap_housing_project_base_audited.csv

../output/zap_housing_project_base_audited.csv: build_zap_housing_pipeline_from_raw.R ../input/zap_project_data.parquet ../input/cd_homeownership_1990_measure.csv ../input/zap_source_integrity_qc.csv | ../output
	$(R) $<

../input/zap_project_data.parquet: ../../stage_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_source_integrity_qc.csv: ../../stage_zap_source_integrity/output/zap_source_integrity_qc.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_data.parquet ../input/cd_homeownership_1990_measure.csv ../input/zap_source_integrity_qc.csv

include ../../generic.make
