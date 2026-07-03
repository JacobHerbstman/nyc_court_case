START_YEAR := 1975
END_YEAR := 2026
REPORT_LIMIT := 0
WORKERS := 1

all: ../output/ulurp_cpc_report_manifest.csv

../output/ulurp_cpc_report_manifest.csv: build_ulurp_cpc_report_corpus.py ../input/ulurp_corpus_application_spine.csv | ../output ../output/cpc_report_pdfs ../output/cpc_report_text
	python3 $< $(START_YEAR) $(END_YEAR) $(REPORT_LIMIT) $(WORKERS)
	touch $@

../input/ulurp_corpus_application_spine.csv: ../../build_ulurp_corpus_spine/output/ulurp_corpus_application_spine.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_corpus_application_spine.csv

../output/cpc_report_pdfs:
	mkdir -p $@

../output/cpc_report_text:
	mkdir -p $@

include ../../generic.make
