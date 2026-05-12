SHELL := bash
include ../../shell_functions.make

all: ../output/dof_421a_raw_files.csv ../output/dof_421a_fetch_qc.csv

../output/dof_421a_raw_files.csv: fetch_dof_421a_exempt_properties.R | ../output ../output/raw
	$(R) $<

../output/dof_421a_fetch_qc.csv: ../output/dof_421a_raw_files.csv
	@test -s "$@"

../output/raw:
	mkdir -p $@

include ../../generic.make
