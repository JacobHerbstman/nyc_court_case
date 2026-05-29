SHELL := bash
include ../../shell_functions.make

all: ../output/ccdist2010_homeownership_1990_measure.csv \
	../output/ccdist2010_homeownership_1990_map.pdf

../output/ccdist2010_homeownership_1990_measure.csv: build_ccd2010_homeownership_1990_measure.R ../../_lib/source_pipeline_utils.R ../input/nhgis_1990_tract_extract.parquet ../input/nhgis_raw_files.csv ../input/nycc_10cav.zip | ../output
	$(R) $<

../output/ccdist2010_homeownership_1990_map.pdf: ../output/ccdist2010_homeownership_1990_measure.csv
	@test -s "$@"

../input/nhgis_1990_tract_extract.parquet: ../../stage_nhgis/output/nhgis_1990_tract_extract.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/nhgis_raw_files.csv: ../../load_nhgis_raw/output/nhgis_raw_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/nycc_10cav.zip: ../../../data_raw/dcp_boundary_city_council_districts_archive/10C/nycc_10cav.zip | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/nhgis_1990_tract_extract.parquet ../input/nhgis_raw_files.csv ../input/nycc_10cav.zip

include ../../generic.make
