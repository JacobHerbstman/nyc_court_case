all: ../output/zap_project_data.parquet ../output/zap_project_bbl.parquet

../%/zap_project_data.parquet ../%/zap_project_bbl.parquet: build_zap_datasets.R ../../_lib/source_pipeline_utils.R ../input/source_catalog.csv | ../output
	Rscript --vanilla $<

../input/source_catalog.csv: ../../source_registry/output/source_catalog.csv | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
