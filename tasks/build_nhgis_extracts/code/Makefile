all: ../output/nhgis_1990_tract_extract.parquet \
	../output/nhgis_1990_tract_gis_zip.csv

../output/nhgis_1990_tract_extract.parquet: build_nhgis_extracts.R \
	../../_lib/source_pipeline_utils.R \
	../temp/nhgis_raw_files.csv \
	nhgis_table_map.csv \
	../temp/nhgis_1990_tract_extract_raw.parquet \
	nhgis_income_overrides.csv | ../output
	Rscript --vanilla $<

../output/nhgis_1990_tract_gis_zip.csv: ../output/nhgis_1990_tract_extract.parquet
	@test -s "$@"

../temp/nhgis_raw_files.csv: load_nhgis_raw.R \
	../../_lib/source_pipeline_utils.R \
	../temp/nhgis_extract_downloads.csv \
	nhgis_table_map.csv | ../temp
	Rscript --vanilla $<

../temp/nhgis_1990_tract_extract_raw.parquet: ../temp/nhgis_raw_files.csv
	@test -s "$@"

../temp/nhgis_extract_downloads.csv: fetch_nhgis_extracts.R \
	../../_lib/source_pipeline_utils.R \
	../input/source_catalog.csv \
	nhgis_1990_extract.json \
	nhgis_table_map.csv | ../temp
	Rscript --vanilla $<

../input/source_catalog.csv: ../../source_registry/output/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
