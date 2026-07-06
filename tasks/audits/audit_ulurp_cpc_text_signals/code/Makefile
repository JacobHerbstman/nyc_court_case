START_YEAR := 1975
SAMPLE_DOCUMENTS_PER_DECADE := 200
BOILERPLATE_DOC_SHARE := 0.05
KWIC_HITS_PER_RULE := 50
KWIC_CONTEXT_WORDS := 45

all: ../output/ulurp_cpc_text_signal_year.csv \
	../output/ulurp_cpc_text_signal_decade_sample.csv \
	../output/ulurp_cpc_text_signal_kwic_sample.csv \
	../output/ulurp_cpc_text_signal_kwic_assistant_labels.csv \
	../output/ulurp_cpc_text_boilerplate_sentences.csv

../output/ulurp_cpc_text_signal_year.csv: audit_ulurp_cpc_text_signals.py \
	../input/ulurp_cpc_report_manifest.csv \
	../input/flushing_commons_cpc_ocr_manifest.csv | ../output
	python3 $< $(START_YEAR) $(SAMPLE_DOCUMENTS_PER_DECADE) $(BOILERPLATE_DOC_SHARE) $(KWIC_HITS_PER_RULE) $(KWIC_CONTEXT_WORDS)

../output/ulurp_cpc_text_signal_decade_sample.csv \
../output/ulurp_cpc_text_signal_kwic_sample.csv \
../output/ulurp_cpc_text_boilerplate_sentences.csv: ../output/ulurp_cpc_text_signal_year.csv
	@test -s "$@"

../output/ulurp_cpc_text_signal_kwic_assistant_labels.csv: label_ulurp_cpc_text_signal_kwic.py \
	manual_ulurp_cpc_text_signal_kwic_labels.csv \
	../output/ulurp_cpc_text_signal_kwic_sample.csv | ../output
	python3 $<

../input/ulurp_cpc_report_manifest.csv: ../../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/flushing_commons_cpc_ocr_manifest.csv: ../../ocr_ulurp_flushing_commons_cpc_reports/output/flushing_commons_cpc_ocr_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_cpc_report_manifest.csv ../input/flushing_commons_cpc_ocr_manifest.csv

include ../../generic.make
