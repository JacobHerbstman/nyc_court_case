SHELL := bash
include ../../shell_functions.make

all: ../output/mappluto_lot_files.csv ../output/mappluto_lot_qc.csv

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	ln -sf "$<" "$@"

../input/mappluto_download_audit.csv: | ../input
	ln -sf "../../fetch_mappluto_archive/output/mappluto_download_audit.csv" "$@"

../output/mappluto_lot_files.csv: stage_mappluto_lots.R ../input/source_catalog.csv ../input/mappluto_download_audit.csv | ../output
	$(R) $< ../input/source_catalog.csv ../input/mappluto_download_audit.csv ../output/mappluto_lot_files.csv ../output/mappluto_lot_qc.csv

../output/mappluto_lot_qc.csv: ../output/mappluto_lot_files.csv

link-inputs: ../input/source_catalog.csv ../input/mappluto_download_audit.csv

include ../../generic.make
