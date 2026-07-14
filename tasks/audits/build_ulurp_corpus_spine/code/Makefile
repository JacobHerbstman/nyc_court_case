SHELL := bash
include ../../../shell_functions.make

all: ../output/ulurp_corpus_project_spine.csv \
	../output/ulurp_corpus_application_spine.csv \
	../output/ulurp_corpus_spine_qc.csv

../output/ulurp_corpus_project_spine.csv: build_ulurp_corpus_spine.R ../../../_lib/source_pipeline_utils.R ../input/zap_project_data.parquet | ../output
	$(R) $<

../output/ulurp_corpus_application_spine.csv: ../output/ulurp_corpus_project_spine.csv
	@test -s "$@"

../output/ulurp_corpus_spine_qc.csv: ../output/ulurp_corpus_project_spine.csv
	@test -s "$@"

../input/zap_project_data.parquet: ../../../build_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_data.parquet

include ../../generic.make
