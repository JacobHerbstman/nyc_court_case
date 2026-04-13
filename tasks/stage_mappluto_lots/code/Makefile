SHELL := bash
include ../../shell_functions.make

all: ../output/mappluto_lot_files.csv ../output/mappluto_lot_qc.csv

../input/mappluto_raw_files.csv: ../../load_mappluto_raw/output/mappluto_raw_files.csv | ../input
	ln -sf "../../load_mappluto_raw/output/mappluto_raw_files.csv" "$@"

../output/mappluto_lot_files.csv: stage_mappluto_lots.R ../input/mappluto_raw_files.csv | ../output
	$(R) $< ../input/mappluto_raw_files.csv ../output/mappluto_lot_files.csv ../output/mappluto_lot_qc.csv

../output/mappluto_lot_qc.csv: ../output/mappluto_lot_files.csv

link-inputs: ../input/mappluto_raw_files.csv

include ../../generic.make
