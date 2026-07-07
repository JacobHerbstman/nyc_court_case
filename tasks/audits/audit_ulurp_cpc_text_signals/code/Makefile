START_YEAR := 1975
END_YEAR := 2025
SAMPLE_DOCUMENTS_PER_DECADE := 200
BOILERPLATE_DOC_SHARE := 0.05
KWIC_HITS_PER_RULE := 50
KWIC_CONTEXT_WORDS := 45

all: ../output/ulurp_cpc_text_signal_year.csv \
	../output/ulurp_cpc_text_signal_year_by_application_sample.csv \
	../output/ulurp_cpc_text_signal_decade_sample.csv \
	../output/ulurp_cpc_text_signal_kwic_sample.csv \
	../output/ulurp_cpc_text_signal_kwic_assistant_labels.csv \
	../output/ulurp_cpc_text_boilerplate_sentences.csv \
	../output/ulurp_cpc_text_signal_trends_all_reports.pdf \
	../output/ulurp_cpc_text_signal_trends_non_pp.pdf \
	../output/ulurp_cpc_text_signal_trends_zm_zr_zs.pdf \
	../output/ulurp_cpc_report_count_comparison.pdf

../output/ulurp_cpc_text_signal_year.csv: audit_ulurp_cpc_text_signals.py \
	label_ulurp_cpc_text_signal_kwic.py \
	../input/ulurp_cpc_report_manifest.csv \
	../input/flushing_commons_cpc_ocr_manifest.csv | ../output
	python3 $< $(START_YEAR) $(END_YEAR) $(SAMPLE_DOCUMENTS_PER_DECADE) $(BOILERPLATE_DOC_SHARE) $(KWIC_HITS_PER_RULE) $(KWIC_CONTEXT_WORDS)

../output/ulurp_cpc_text_signal_year_by_application_sample.csv: ../output/ulurp_cpc_text_signal_year.csv
	@test -s "$@"

../output/ulurp_cpc_text_signal_decade_sample.csv \
../output/ulurp_cpc_text_signal_kwic_sample.csv \
../output/ulurp_cpc_text_boilerplate_sentences.csv: ../output/ulurp_cpc_text_signal_year.csv
	@test -s "$@"

../output/ulurp_cpc_text_signal_kwic_assistant_labels.csv: label_ulurp_cpc_text_signal_kwic.py \
	../input/manual_ulurp_cpc_text_signal_kwic_labels.csv \
	../output/ulurp_cpc_text_signal_kwic_sample.csv | ../output
	python3 $<

../output/ulurp_cpc_text_signal_trends_all_reports.pdf: plot_ulurp_cpc_text_signal_trends.R \
	../output/ulurp_cpc_text_signal_year_by_application_sample.csv \
	../input/citywide_ulurp_application_year.csv | ../output
	Rscript $<

../output/ulurp_cpc_text_signal_trends_non_pp.pdf \
../output/ulurp_cpc_text_signal_trends_zm_zr_zs.pdf \
../output/ulurp_cpc_report_count_comparison.pdf: ../output/ulurp_cpc_text_signal_trends_all_reports.pdf
	@test -s "$@"

../input/ulurp_cpc_report_manifest.csv: ../../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/flushing_commons_cpc_ocr_manifest.csv: ../../ocr_ulurp_flushing_commons_cpc_reports/output/flushing_commons_cpc_ocr_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/citywide_ulurp_application_year.csv: ../../../summarize_citywide_ulurp_application_trends/output/citywide_ulurp_application_year.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/manual_ulurp_cpc_text_signal_kwic_labels.csv: ../../record_ulurp_cpc_text_signal_manual_labels/output/manual_ulurp_cpc_text_signal_kwic_labels.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_cpc_report_manifest.csv ../input/flushing_commons_cpc_ocr_manifest.csv ../input/citywide_ulurp_application_year.csv ../input/manual_ulurp_cpc_text_signal_kwic_labels.csv

include ../../generic.make
