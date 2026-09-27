AUDIT_DOCUMENTS := 52fc332fdf068895488f 6338e5a87d96ff5af282 444e99affa94f415b846 \
	2ed657ccf1fdddca5f64 6fe3cf5bed536f014268 f74ef50149c43d9dec52 \
	454495b3c2b022fa8752 8cc1f566a6b3da4f05b1 efc2d0fe06402abe9198 \
	22929526d2ff38ceca2e 7c333cc492149fda31c5 aa80d94c3ebc73595465 \
	4b026df915da0008d83f 972066b72c66c8244bb9 b6a2ecdd4db3af500b69 \
	5663059d8a5882b9c972 d7f0f0f503caae417e85 1818edbd692337c3eeeb \
	c27f7af95a119ee12e69 02c18c0a3b85fce07355

all: ../output/cpc_statement_report_measures.csv ../output/cpc_statement_measure_audit_sensitivity.csv \
	../output/cpc_statement_measure_human_agreement.csv \
	../output/cpc_statement_full_run_human_agreement.csv ../output/cpc_statement_full_run_retest.csv \
	../output/cpc_statement_full_run_length.csv

../%/cpc_statement_report_measures.csv ../%/cpc_statement_measure_audit_sensitivity.csv ../%/cpc_statement_measure_human_agreement.csv: \
	build_cpc_statement_report_measures.py cpc_statement_measures.py ../input/ulurp_cpc_statements.csv ../input/ulurp_cpc_human_coding.csv \
	../input/cpc_statement_audit_assessments.csv $(foreach doc,$(AUDIT_DOCUMENTS),../input/$(doc)_inventory.json) | ../output ../report
	python3 $<

# The full run is still in progress; these outputs are a snapshot of its completed reports.
../%/cpc_statement_full_run_human_agreement.csv ../%/cpc_statement_full_run_retest.csv ../%/cpc_statement_full_run_length.csv: \
	check_full_run_measures.py cpc_statement_measures.py ../input/ulurp_cpc_statements_full_sol_high_20260927.csv \
	../input/ulurp_cpc_statement_status_full_sol_high_20260927.csv ../input/ulurp_cpc_statements.csv \
	../input/ulurp_cpc_human_coding.csv ../input/ulurp_cpc_reading_roster.csv | ../output ../report
	python3 $<

../input/ulurp_cpc_statements_full_sol_high_20260927.csv: ../../../extract_ulurp_cpc_statements/output/ulurp_cpc_statements_full_sol_high_20260927.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_statement_status_full_sol_high_20260927.csv: ../../../extract_ulurp_cpc_statements/output/ulurp_cpc_statement_status_full_sol_high_20260927.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_reading_roster.csv: ../../../build_ulurp_cpc_reading_text/output/ulurp_cpc_reading_roster.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_statements.csv: ../../../extract_ulurp_cpc_statements/output/ulurp_cpc_statements_first100_sol_high_20260926.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_human_coding.csv: ../../../summarize_text_cpc_trends/output/ulurp_cpc_human_coding.csv | ../input
	ln -sf $< $@

../input/cpc_statement_audit_assessments.csv: ../../audit_ulurp_cpc_statements/output/cpc_statement_audit_assessments.csv | ../input
	ln -sf $< $@

../input/%_inventory.json: ../../../../data_raw/cpc_statement_extraction/first100_sol_high_20260926/audit/source_inventories/%_attempt1.json | ../input
	ln -sf $< $@

link-inputs: ../input/ulurp_cpc_statements.csv ../input/ulurp_cpc_statements_full_sol_high_20260927.csv \
	../input/ulurp_cpc_statement_status_full_sol_high_20260927.csv ../input/ulurp_cpc_reading_roster.csv ../input/ulurp_cpc_human_coding.csv ../input/cpc_statement_audit_assessments.csv \
	$(foreach doc,$(AUDIT_DOCUMENTS),../input/$(doc)_inventory.json)

include ../../generic.make
