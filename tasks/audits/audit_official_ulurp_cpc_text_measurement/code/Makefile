START_YEAR := 1975
END_YEAR := 2025
DOCUMENTS_PER_DECADE := 100

all: ../output/cpc_attachment_pages.csv ../output/cpc_attachment_reports.csv \
	../output/cpc_attachment_summary.csv ../output/cpc_attachment_findings.md \
	../output/official_ulurp_cpc_text_measurement_summary.csv \
	../output/official_ulurp_cpc_narrative_manifest.csv \
	../output/official_ulurp_cpc_narrative_boundary_sample.csv \
	../output/cpc_excluded_n_reports.csv ../output/cpc_n_report_references.csv \
	../output/cpc_duplicate_narrative_groups.csv ../output/cpc_selection_findings.md

../out%/cpc_attachment_pages.csv ../out%/cpc_attachment_reports.csv \
../out%/cpc_attachment_summary.csv ../out%/cpc_attachment_findings.md: audit_cpc_attachments.py \
	../../../_lib/data_reports.py ../input/official_ulurp_cpc_report_manifest.csv \
	../input/cpc_attachment_manifest_before.csv ../input/cpc_attachment_pages_before.csv \
	../input/cpc_attachment_roster_before.csv ../input/cpc_attachment_pilot_sample.csv | ../output ../report
	python3 $<

../out%/official_ulurp_cpc_narrative_manifest.csv \
../out%/official_ulurp_cpc_text_measurement_summary.csv \
../out%/official_ulurp_cpc_narrative_boundary_sample.csv: audit_official_ulurp_cpc_text_measurement.py Makefile \
	../input/official_ulurp_cpc_report_manifest.csv \
	../input/official_ulurp_cpc_narrative_boundary_exceptions.csv \
	../../../_lib/cpc_narratives.py ../../../_lib/data_reports.py | ../output ../report
	python3 $< $(START_YEAR) $(END_YEAR) $(DOCUMENTS_PER_DECADE)

../out%/cpc_excluded_n_reports.csv ../out%/cpc_n_report_references.csv \
../out%/cpc_duplicate_narrative_groups.csv ../out%/cpc_selection_findings.md: audit_cpc_selection.py Makefile \
	../output/official_ulurp_cpc_narrative_manifest.csv \
	../input/official_cpc_report_index.csv ../input/ulurp_cpc_text_labels.csv \
	../input/ulurp_cpc_narrative_sources.csv ../input/ulurp_cpc_companion_reports.csv \
	../input/official_ulurp_cpc_report_manifest.csv ../input/cpc_selection_reviews.csv \
	../../../_lib/cpc_narratives.py ../../../_lib/data_reports.py | ../output ../report
	python3 $< $(START_YEAR) $(END_YEAR)

../input/cpc_attachment_manifest_before.csv: ../../../../data_raw/cpc_text_repair/20260922_before/report_manifest.csv | ../input
	ln -sf $< $@

../input/cpc_attachment_pages_before.csv: ../../../../data_raw/cpc_text_repair/20260922_before/pages_before.csv | ../input
	ln -sf $< $@

../input/cpc_attachment_roster_before.csv: ../../../../data_raw/cpc_text_repair/20260922_before/cpc_jev_roster_v3.csv | ../input
	ln -sf $< $@

../input/cpc_attachment_pilot_sample.csv: ../../../../data_raw/cpc_subagent_pilot/20260922_sol_expansion/sample_snapshot.csv | ../input
	ln -sf $< $@

../input/official_ulurp_cpc_report_manifest.csv: ../../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	ln -sf "$<" "$@"

../input/official_ulurp_cpc_narrative_boundary_exceptions.csv: ../../../record_ulurp_cpc_source_corrections/output/ulurp_cpc_narrative_boundary_exceptions.csv | ../input
	ln -sf "$<" "$@"

../input/official_cpc_report_index.csv: ../../../fetch_ulurp_cpc_report_index/output/official_cpc_report_index.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_text_labels.csv: ../../../summarize_text_cpc_trends/output/ulurp_cpc_text_labels.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_narrative_sources.csv: ../../../summarize_text_cpc_trends/output/ulurp_cpc_narrative_sources.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_companion_reports.csv: ../../../record_ulurp_cpc_source_corrections/output/ulurp_cpc_companion_reports.csv | ../input
	ln -sf $< $@

../input/cpc_selection_reviews.csv: ../../record_ulurp_cpc_source_validation/output/cpc_selection_reviews.csv | ../input
	ln -sf $< $@

link-inputs: ../input/cpc_attachment_manifest_before.csv ../input/cpc_attachment_pages_before.csv \
	../input/cpc_attachment_roster_before.csv ../input/cpc_attachment_pilot_sample.csv \
	../input/official_ulurp_cpc_report_manifest.csv \
	../input/official_ulurp_cpc_narrative_boundary_exceptions.csv \
	../input/official_cpc_report_index.csv ../input/ulurp_cpc_text_labels.csv \
	../input/ulurp_cpc_narrative_sources.csv ../input/ulurp_cpc_companion_reports.csv \
	../input/cpc_selection_reviews.csv

include ../../generic.make
