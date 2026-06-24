SHELL := bash
include ../../shell_functions.make

all: ../output/mappluto_lot_files.csv

../output/mappluto_lot_files.csv: stage_mappluto_lots.R ../input/mappluto_raw_files.csv | ../output
	$(R) $<

../output/dcp_mappluto_archive_18v1_1.parquet: ../output/mappluto_lot_files.csv
	@test -s "$@"

../output/dcp_mappluto_current_25v4.parquet: ../output/mappluto_lot_files.csv
	@test -s "$@"

../input/mappluto_raw_files.csv: ../../load_mappluto_raw/output/mappluto_raw_files.csv | ../input
	@test "$$(readlink "$@")" = "../../load_mappluto_raw/output/mappluto_raw_files.csv" || ln -sf "../../load_mappluto_raw/output/mappluto_raw_files.csv" "$@"

link-inputs: ../input/mappluto_raw_files.csv

include ../../generic.make
