all: ../output/mappluto_current_lot_lookup.parquet

../output/mappluto_current_lot_lookup.parquet: build_mappluto_current_lookup.R \
	../../_lib/source_pipeline_utils.R \
	../../_lib/mappluto_utils.R \
	../input/mappluto_files.csv | ../output
	Rscript --vanilla $<

../input/mappluto_files.csv: ../../fetch_mappluto_archive/output/mappluto_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/mappluto_files.csv

include ../../generic.make
