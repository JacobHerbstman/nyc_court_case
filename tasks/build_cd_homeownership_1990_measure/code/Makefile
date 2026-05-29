SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_1990_measure.csv

../output/cd_homeownership_1990_measure.csv: build_cd_homeownership_1990_measure.R ../../_lib/source_pipeline_utils.R ../input/dcp_cd_profiles_1990_2000_files.csv | ../output
	$(R) $<

../input/dcp_cd_profiles_1990_2000_files.csv: ../../stage_dcp_cd_profiles_1990_2000/output/dcp_cd_profiles_1990_2000_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/dcp_cd_profiles_1990_2000_files.csv

include ../../generic.make
