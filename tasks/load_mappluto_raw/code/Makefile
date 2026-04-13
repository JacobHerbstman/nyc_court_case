SHELL := bash
include ../../shell_functions.make

all: ../output/mappluto_raw_files.csv ../output/mappluto_raw_qc.csv

../input/mappluto_files.csv: ../../fetch_mappluto_archive/output/mappluto_files.csv | ../input
	ln -sf "../../fetch_mappluto_archive/output/mappluto_files.csv" "$@"

../output/mappluto_raw_files.csv: load_mappluto_raw.R ../input/mappluto_files.csv | ../output
	$(R) $< ../input/mappluto_files.csv ../output/mappluto_raw_files.csv ../output/mappluto_raw_qc.csv

../output/mappluto_raw_qc.csv: ../output/mappluto_raw_files.csv

link-inputs: ../input/mappluto_files.csv

include ../../generic.make
