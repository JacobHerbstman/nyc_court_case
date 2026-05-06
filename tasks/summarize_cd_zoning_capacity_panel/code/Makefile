SHELL := bash
include ../../shell_functions.make

all: ../output/cd_zoning_capacity_lot_release.parquet ../output/cd_zoning_capacity_cd_year.csv ../output/cd_zoning_capacity_tercile_trends.pdf ../output/cd_zoning_capacity_coefficients.csv ../output/cd_zoning_capacity_qc.csv

../output/cd_zoning_capacity_qc.csv: summarize_cd_zoning_capacity_panel.R ../../_lib/source_pipeline_utils.R ../input/mappluto_lot_files.csv ../input/cd_redevelopment_potential_baseline.csv | ../output
	$(R) $<

../output/cd_zoning_capacity_lot_release.parquet ../output/cd_zoning_capacity_cd_year.csv ../output/cd_zoning_capacity_tercile_trends.pdf ../output/cd_zoning_capacity_coefficients.csv: ../output/cd_zoning_capacity_qc.csv
	@test -s "$@"

../input/mappluto_lot_files.csv: ../../stage_mappluto_lots/output/mappluto_lot_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_redevelopment_potential_baseline.csv: ../../redevelopment_potential_first_pass/output/cd_redevelopment_potential_baseline.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/mappluto_lot_files.csv ../input/cd_redevelopment_potential_baseline.csv

include ../../generic.make
