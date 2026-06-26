SHELL := bash
include ../../../shell_functions.make

all: ../output/ccdist2010_redevelopment_potential.csv

../output/ccdist2010_redevelopment_potential.csv: build_ccd2010_redevelopment_potential.R ../../../_lib/source_pipeline_utils.R ../input/ccdist2010_homeownership_1990_measure.csv ../input/mappluto_files.csv ../input/mappluto_02b.zip | ../output
	$(R) $<

../input/ccdist2010_homeownership_1990_measure.csv: ../../../build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/mappluto_files.csv: ../../../fetch_mappluto_archive/output/mappluto_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/mappluto_02b.zip: ../../../../data_raw/dcp_mappluto_archive/02b/mappluto_02b.zip | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ccdist2010_homeownership_1990_measure.csv ../input/mappluto_files.csv ../input/mappluto_02b.zip

include ../../generic.make
