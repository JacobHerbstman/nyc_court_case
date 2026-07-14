START_YEAR := 1975
END_YEAR := 2025

all: ../output/ulurp_corpus_application_spine.csv

../output/ulurp_corpus_application_spine.csv: build_ulurp_corpus_spine.R ../../_lib/source_pipeline_utils.R ../input/zap_project_data.parquet | ../output
	Rscript --vanilla $< $(START_YEAR) $(END_YEAR)

../input/zap_project_data.parquet: ../../build_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_data.parquet

include ../../generic.make
