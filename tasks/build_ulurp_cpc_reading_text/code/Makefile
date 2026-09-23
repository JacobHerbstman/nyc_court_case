all: ../output/ulurp_cpc_reading_roster.csv ../output/ulurp_cpc_page_scope.csv ../output/ulurp_cpc_reading_segments.csv

../out%/ulurp_cpc_reading_roster.csv ../out%/ulurp_cpc_page_scope.csv ../out%/ulurp_cpc_reading_segments.csv: build_ulurp_cpc_reading_text.py Makefile \
	../../_lib/data_reports.py ../../_lib/cpc_narratives.py ../../_lib/cpc_pages.py \
	../input/ulurp_cpc_text_labels.csv ../input/ulurp_cpc_narrative_sources.csv \
	../input/ulurp_cpc_report_manifest.csv ../input/ulurp_cpc_page_scope_reviews.csv | ../output ../report
	python3 $<

../input/ulurp_cpc_text_labels.csv: ../../summarize_text_cpc_trends/output/ulurp_cpc_text_labels.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_narrative_sources.csv: ../../summarize_text_cpc_trends/output/ulurp_cpc_narrative_sources.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_report_manifest.csv: ../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_page_scope_reviews.csv: ../../record_ulurp_cpc_source_corrections/output/ulurp_cpc_page_scope_reviews.csv | ../input
	ln -sf $< $@

link-inputs: ../input/ulurp_cpc_text_labels.csv ../input/ulurp_cpc_narrative_sources.csv \
	../input/ulurp_cpc_report_manifest.csv ../input/ulurp_cpc_page_scope_reviews.csv

include ../../generic.make
