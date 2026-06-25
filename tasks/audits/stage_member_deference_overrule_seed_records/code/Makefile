SHELL := bash
include ../../../shell_functions.make

all: ../output/council_land_use_matter.csv

../output/council_land_use_matter.csv: stage_member_deference_overrule_seed_records.R ../../../_lib/source_pipeline_utils.R ../input/council_land_use_fetch_files.csv | ../output
	$(R) $<

../input/council_land_use_fetch_files.csv: ../../../fetch_council_land_use_records/output/council_land_use_fetch_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/council_land_use_fetch_files.csv

include ../../generic.make
