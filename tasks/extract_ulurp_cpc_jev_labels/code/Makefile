MAX_STATE_CHARACTERS := 45000
RANDOM_AUDIT_REPORTS := 24
AUDIT_SEED := cpc-jev-corpus-v1-20260919
WORKERS := 1
MINIMUM_INTERVAL := 10
REQUEST_LIMIT := 0
RECOVERY_ATTEMPTS := 2 3 4 5 6
THRESHOLD := 0.5

all: ../output/cpc_jev_requests_v3.jsonl ../output/cpc_jev_roster_v3.csv ../output/cpc_jev_segments_v3.csv ../output/cpc_jev_request_index_v3.csv ../output/cpc_jev_page_scope_v3.csv \
	../output/cpc_jev_successful_labels_v2.csv ../output/cpc_jev_labels_v2.csv ../output/cpc_jev_answers_v2.csv ../output/cpc_jev_usage_v2.csv ../output/cpc_jev_source_coverage_v2.csv

../out%/cpc_jev_requests_v3.jsonl ../out%/cpc_jev_roster_v3.csv ../out%/cpc_jev_segments_v3.csv ../out%/cpc_jev_request_index_v3.csv ../out%/cpc_jev_page_scope_v3.csv: prepare_cpc_jev_corpus.py cpc_jev_codebook_v1.json Makefile ../../_lib/data_reports.py ../../_lib/cpc_narratives.py ../../_lib/cpc_pages.py \
	../input/ulurp_cpc_text_labels.csv ../input/ulurp_cpc_narrative_sources.csv ../input/ulurp_cpc_report_manifest.csv ../input/ulurp_cpc_page_scope_reviews.csv ../input/cpc_jev_responses_v2.jsonl | ../output ../report
	python3 $< $(MAX_STATE_CHARACTERS) $(RANDOM_AUDIT_REPORTS) $(AUDIT_SEED)

../out%/cpc_jev_successful_labels_v2.csv ../out%/cpc_jev_labels_v2.csv ../out%/cpc_jev_answers_v2.csv ../out%/cpc_jev_usage_v2.csv ../out%/cpc_jev_source_coverage_v2.csv: build_cpc_jev_labels.py cpc_jev_codebook_v1.json Makefile ../../_lib/data_reports.py \
	../output/cpc_jev_roster_v2.csv ../output/cpc_jev_segments_v2.csv ../output/cpc_jev_request_index_v2.csv \
	../input/cpc_jev_responses_v2.jsonl ../input/cpc_jev_responses_v1.jsonl ../input/cpc_jev_completed_requests_v1.jsonl ../input/cpc_jev_completed_segments_v1.csv ../input/cpc_jev_report_manifest_v2.csv ../input/cpc_jev_narrative_sources_v2.csv | ../output ../report
	python3 $< $(THRESHOLD)

../output/cpc_jev_requests_v2.jsonl: ../../../data_raw/cpc_jev_corpus/20260919_v2/requests.jsonl | ../output
	cp $< $@

../output/cpc_jev_roster_v2.csv: ../../../data_raw/cpc_jev_corpus/20260919_v2/roster.csv | ../output
	cp $< $@

../output/cpc_jev_segments_v2.csv: ../../../data_raw/cpc_jev_corpus/20260919_v2/segments.csv | ../output
	cp $< $@

../output/cpc_jev_request_index_v2.csv: ../../../data_raw/cpc_jev_corpus/20260919_v2/request_index.csv | ../output
	cp $< $@

.PHONY: acquire acquire-retries
acquire: ../output/cpc_jev_requests_v2.jsonl acquire_cpc_jev_corpus.py ../input/cpc_jev_completed_requests_v1.jsonl
	mkdir -p ../../../data_raw/cpc_jev_corpus/20260919_v2
	python3 acquire_cpc_jev_corpus.py $(WORKERS) $(MINIMUM_INTERVAL) $(REQUEST_LIMIT) 1

acquire-retries: ../output/cpc_jev_requests_v2.jsonl acquire_cpc_jev_corpus.py ../input/cpc_jev_completed_requests_v1.jsonl
	mkdir -p ../../../data_raw/cpc_jev_corpus/20260919_v2
	for attempt in $(RECOVERY_ATTEMPTS); do \
		python3 acquire_cpc_jev_corpus.py $(WORKERS) $(MINIMUM_INTERVAL) $(REQUEST_LIMIT) $$attempt || exit $$?; \
	done

../input/ulurp_cpc_page_scope_reviews.csv: ../../record_ulurp_cpc_source_corrections/output/ulurp_cpc_page_scope_reviews.csv | ../input
	ln -sf $< $@

../input/cpc_jev_report_manifest_v2.csv: ../../../data_raw/cpc_jev_corpus/20260919_v2/report_manifest.csv | ../input
	ln -sf $< $@

../input/cpc_jev_narrative_sources_v2.csv: ../../../data_raw/cpc_jev_corpus/20260919_v2/narrative_sources.csv | ../input
	ln -sf $< $@

../input/cpc_jev_responses_v1.jsonl: ../../../data_raw/cpc_jev_corpus/20260919_v1/responses.jsonl | ../input
	ln -sf $< $@

../input/cpc_jev_completed_requests_v1.jsonl: ../../../data_raw/cpc_jev_corpus/20260919_v1/completed_requests.jsonl | ../input
	ln -sf $< $@

../input/cpc_jev_completed_segments_v1.csv: ../../../data_raw/cpc_jev_corpus/20260919_v1/completed_segments.csv | ../input
	ln -sf $< $@

../input/cpc_jev_responses_v2.jsonl: ../../../data_raw/cpc_jev_corpus/20260919_v2/responses.jsonl | ../input
	ln -sf $< $@

../input/ulurp_cpc_text_labels.csv: ../../summarize_text_cpc_trends/output/ulurp_cpc_text_labels.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_narrative_sources.csv: ../../summarize_text_cpc_trends/output/ulurp_cpc_narrative_sources.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_report_manifest.csv: ../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	ln -sf $< $@

link-inputs: ../input/ulurp_cpc_page_scope_reviews.csv ../input/cpc_jev_report_manifest_v2.csv ../input/cpc_jev_narrative_sources_v2.csv ../input/cpc_jev_responses_v1.jsonl ../input/cpc_jev_completed_requests_v1.jsonl ../input/cpc_jev_completed_segments_v1.csv ../input/ulurp_cpc_text_labels.csv ../input/ulurp_cpc_narrative_sources.csv ../input/ulurp_cpc_report_manifest.csv ../input/cpc_jev_responses_v2.jsonl

include ../../generic.make
