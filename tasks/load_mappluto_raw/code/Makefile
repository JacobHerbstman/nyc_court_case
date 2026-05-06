SHELL := bash
include ../../shell_functions.make

all: ../output/mappluto_raw_files.csv ../output/mappluto_raw_qc.csv

../output/mappluto_raw_files.csv: load_mappluto_raw.R ../input/mappluto_files.csv | ../output
	$(R) $<

../output/mappluto_raw_qc.csv: ../output/mappluto_raw_files.csv
	@test -s "$@"

../input/mappluto_files.csv: ../../fetch_mappluto_archive/output/mappluto_files.csv | ../input
	@test "$$(readlink "$@")" = "../../fetch_mappluto_archive/output/mappluto_files.csv" || ln -sf "../../fetch_mappluto_archive/output/mappluto_files.csv" "$@"

link-inputs: ../input/mappluto_files.csv

include ../../generic.make
