SHELL := bash

SAMPLE_SEED := 20260702
DOCUMENTS_PER_COHORT := 10

all: ../output/ulurp_corpus_sample_document_index.csv \
	../output/ulurp_corpus_sample_document_text.csv \
	../output/ulurp_corpus_sample_document_fetch_failures.csv \
	../output/ulurp_corpus_sample_document_qc.csv

../output/ulurp_corpus_sample_document_index.csv: sample_ulurp_corpus_documents.py ../input/ulurp_corpus_project_spine.csv | ../output
	python3 $< $(SAMPLE_SEED) $(DOCUMENTS_PER_COHORT)

../output/ulurp_corpus_sample_document_text.csv: ../output/ulurp_corpus_sample_document_index.csv
	@test -s "$@"

../output/ulurp_corpus_sample_document_fetch_failures.csv: ../output/ulurp_corpus_sample_document_index.csv
	@test -s "$@"

../output/ulurp_corpus_sample_document_qc.csv: ../output/ulurp_corpus_sample_document_index.csv
	@test -s "$@"

../input/ulurp_corpus_project_spine.csv: ../../build_ulurp_corpus_spine/output/ulurp_corpus_project_spine.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_corpus_project_spine.csv

include ../../generic.make
