SHELL := bash
include ../../../shell_functions.make

all: ../output/council_land_use_matter.csv

../output/council_land_use_matter.csv: stage_member_deference_overrule_seed_records.R council_land_use_seed_sources.csv ../../../_lib/source_pipeline_utils.R | ../output
	$(R) $<

link-inputs:

include ../../generic.make
