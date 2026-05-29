all: ../output/zap_zoning_map_special_permit_project_classification.csv \
	../output/zap_zoning_map_special_permit_project_ccd2010_fractional.csv \
	../output/zap_zoning_map_special_permit_year.csv \
	../output/zap_zoning_map_special_permit_decade.csv \
	../output/zap_zoning_map_special_permit_housing_focus_year.csv \
	../output/zap_zoning_map_special_permit_housing_focus_decade.csv \
	../output/zap_zoning_map_special_permit_increased_residential_homeowner_tercile_year.csv \
	../output/zap_zoning_map_special_permit_increased_residential_homeowner_tercile_decade.csv \
	../output/zap_zoning_map_special_permit_increased_residential_homeowner_tercile_year_3yr.csv \
	../output/zap_zoning_map_special_permit_all_zm_zs.pdf \
	../output/zap_zoning_map_special_permit_zm_plus_residential_zs.pdf \
	../output/zap_zoning_map_special_permit_all_zm_zs_lines.pdf \
	../output/zap_zoning_map_special_permit_zm_plus_residential_zs_lines.pdf \
	../output/zap_zoning_map_special_permit_housing_focus_lines.pdf \
	../output/zap_zoning_map_special_permit_increased_residential_homeowner_tercile_lines.pdf \
	../output/zap_zoning_map_special_permit_increased_residential_homeowner_tercile_lines_3yr.pdf

../output/zap_zoning_map_special_permit_project_classification.csv: summarize_zap_zoning_actions_by_decade.R ../../_lib/source_pipeline_utils.R ../input/zap_housing_project_base_audited.csv ../input/zap_project_bbl.parquet ../input/ccdist2010_mappluto_bbl_lookup.parquet ../input/ccdist2010_homeownership_1990_measure.csv | ../output
	Rscript --vanilla $<

../output/zap_zoning_map_special_permit_project_ccd2010_fractional.csv: ../output/zap_zoning_map_special_permit_project_classification.csv
	@test -s "$@"

../output/zap_zoning_map_special_permit_year.csv: ../output/zap_zoning_map_special_permit_project_classification.csv
	@test -s "$@"

../output/zap_zoning_map_special_permit_decade.csv: ../output/zap_zoning_map_special_permit_project_classification.csv
	@test -s "$@"

../output/zap_zoning_map_special_permit_housing_focus_year.csv: ../output/zap_zoning_map_special_permit_project_classification.csv
	@test -s "$@"

../output/zap_zoning_map_special_permit_housing_focus_decade.csv: ../output/zap_zoning_map_special_permit_project_classification.csv
	@test -s "$@"

../output/zap_zoning_map_special_permit_increased_residential_homeowner_tercile_year.csv: ../output/zap_zoning_map_special_permit_project_classification.csv
	@test -s "$@"

../output/zap_zoning_map_special_permit_increased_residential_homeowner_tercile_decade.csv: ../output/zap_zoning_map_special_permit_project_classification.csv
	@test -s "$@"

../output/zap_zoning_map_special_permit_increased_residential_homeowner_tercile_year_3yr.csv: ../output/zap_zoning_map_special_permit_project_classification.csv
	@test -s "$@"

../output/zap_zoning_map_special_permit_all_zm_zs.pdf: ../output/zap_zoning_map_special_permit_project_classification.csv
	@test -s "$@"

../output/zap_zoning_map_special_permit_zm_plus_residential_zs.pdf: ../output/zap_zoning_map_special_permit_project_classification.csv
	@test -s "$@"

../output/zap_zoning_map_special_permit_all_zm_zs_lines.pdf: ../output/zap_zoning_map_special_permit_project_classification.csv
	@test -s "$@"

../output/zap_zoning_map_special_permit_zm_plus_residential_zs_lines.pdf: ../output/zap_zoning_map_special_permit_project_classification.csv
	@test -s "$@"

../output/zap_zoning_map_special_permit_housing_focus_lines.pdf: ../output/zap_zoning_map_special_permit_project_classification.csv
	@test -s "$@"

../output/zap_zoning_map_special_permit_increased_residential_homeowner_tercile_lines.pdf: ../output/zap_zoning_map_special_permit_project_classification.csv
	@test -s "$@"

../output/zap_zoning_map_special_permit_increased_residential_homeowner_tercile_lines_3yr.pdf: ../output/zap_zoning_map_special_permit_project_classification.csv
	@test -s "$@"

../input/zap_housing_project_base_audited.csv: ../../build_zap_housing_pipeline_from_raw/output/zap_housing_project_base_audited.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../stage_zap_datasets/output/zap_project_bbl.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_mappluto_bbl_lookup.parquet: ../../build_ccd2010_mappluto_construction_proxy/output/ccdist2010_mappluto_bbl_lookup.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_homeownership_1990_measure.csv: ../../build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_housing_project_base_audited.csv ../input/zap_project_bbl.parquet ../input/ccdist2010_mappluto_bbl_lookup.parquet ../input/ccdist2010_homeownership_1990_measure.csv

include ../../generic.make
