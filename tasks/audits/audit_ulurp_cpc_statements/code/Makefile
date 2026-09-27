AUDIT_SEED := cpc-statements-audit20-20260926
AUDIT_DOCUMENTS := 52fc332fdf068895488f 6338e5a87d96ff5af282 444e99affa94f415b846 \
	2ed657ccf1fdddca5f64 6fe3cf5bed536f014268 f74ef50149c43d9dec52 \
	454495b3c2b022fa8752 8cc1f566a6b3da4f05b1 efc2d0fe06402abe9198 \
	22929526d2ff38ceca2e 7c333cc492149fda31c5 aa80d94c3ebc73595465 \
	4b026df915da0008d83f 972066b72c66c8244bb9 b6a2ecdd4db3af500b69 \
	5663059d8a5882b9c972 d7f0f0f503caae417e85 1818edbd692337c3eeeb \
	c27f7af95a119ee12e69 02c18c0a3b85fce07355
HUMAN_DOCUMENTS := 454495b3c2b022fa8752 aa80d94c3ebc73595465 4b026df915da0008d83f \
	972066b72c66c8244bb9 b6a2ecdd4db3af500b69 1818edbd692337c3eeeb \
	c27f7af95a119ee12e69 02c18c0a3b85fce07355
HUMAN_REVISED_DOCUMENTS := 454495b3c2b022fa8752 aa80d94c3ebc73595465 \
	4b026df915da0008d83f 972066b72c66c8244bb9

all: ../output/cpc_statement_audit_sample.csv ../output/cpc_statement_audit_summary.csv \
	../output/cpc_statement_audit_assessments.csv ../output/cpc_statement_audit_findings.csv \
	../output/cpc_statement_human_comparison.csv

../output/cpc_statement_human_comparison.csv: build_human_statement_audit.py Makefile \
	../output/cpc_statement_audit_sample.csv ../input/human_reference.csv \
	$(foreach doc,$(HUMAN_DOCUMENTS),../input/$(doc)_human_comparison.json ../input/$(doc)_extraction.json) | ../output ../report
	python3 $<

../%/cpc_statement_audit_summary.csv ../%/cpc_statement_audit_assessments.csv ../%/cpc_statement_audit_findings.csv: \
	build_statement_audit.py Makefile ../output/cpc_statement_audit_sample.csv \
	../input/statement_schema.json ../input/ulurp_cpc_reading_segments.csv \
	$(foreach doc,$(AUDIT_DOCUMENTS),../input/$(doc)_inventory.json ../input/$(doc)_extraction.json ../input/$(doc)_comparison.json) | ../output ../report
	python3 $<

../output/cpc_statement_audit_sample.csv: select_statement_audit.py Makefile \
	../input/first100_sample.csv ../input/first100_prompts.csv | ../output ../report
	python3 $< $(AUDIT_SEED)

../input/first100_sample.csv: ../../../../data_raw/cpc_statement_extraction/first100_sol_high_20260926/sample.csv | ../input
	ln -sf $< $@

../input/first100_prompts.csv: ../../../../data_raw/cpc_statement_extraction/first100_sol_high_20260926/prompts.csv | ../input
	ln -sf $< $@

../input/statement_schema.json: ../../../../data_raw/cpc_statement_extraction/first100_sol_high_20260926/statement_schema.json | ../input
	ln -sf $< $@

../input/ulurp_cpc_reading_segments.csv: ../../../build_ulurp_cpc_reading_text/output/ulurp_cpc_reading_segments.csv | ../input
	ln -sf $< $@

../input/%_inventory.json: ../../../../data_raw/cpc_statement_extraction/first100_sol_high_20260926/audit/source_inventories/%_attempt1.json | ../input
	ln -sf $< $@

../input/%_extraction.json: ../../../../data_raw/cpc_statement_extraction/first100_sol_high_20260926/audit/extraction_reference/%.json | ../input
	ln -sf $< $@

../input/%_comparison.json: ../../../../data_raw/cpc_statement_extraction/first100_sol_high_20260926/audit/comparisons/%_attempt1.json | ../input
	ln -sf $< $@

../input/human_reference.csv: ../../../../data_raw/cpc_statement_extraction/first100_sol_high_20260926/human_reference.csv | ../input
	ln -sf $< $@

../input/%_human_comparison.json: ../../../../data_raw/cpc_statement_extraction/first100_sol_high_20260926/audit/human_comparisons/%_attempt1.json | ../input
	ln -sf $< $@

$(foreach doc,$(HUMAN_REVISED_DOCUMENTS),../input/$(doc)_human_comparison.json): ../input/%_human_comparison.json: \
	../../../../data_raw/cpc_statement_extraction/first100_sol_high_20260926/audit/human_comparisons/%_attempt2.json | ../input
	ln -sf $< $@

link-inputs: ../input/first100_sample.csv ../input/first100_prompts.csv \
	../input/statement_schema.json ../input/ulurp_cpc_reading_segments.csv \
	$(foreach doc,$(AUDIT_DOCUMENTS),../input/$(doc)_inventory.json ../input/$(doc)_extraction.json ../input/$(doc)_comparison.json) \
	../input/human_reference.csv $(foreach doc,$(HUMAN_DOCUMENTS),../input/$(doc)_human_comparison.json)

include ../../generic.make
