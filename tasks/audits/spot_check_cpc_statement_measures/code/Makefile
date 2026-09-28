all: ../output/cpc_spot_check_items.csv ../output/cpc_spot_check_review.xlsx ../output/cpc_spot_check_scores.csv \
	../output/cpc_row_judge_packets.csv

# cpc_spot_check_sample.csv is frozen: select_spot_check.py drew it once from the
# full run's September 27 snapshot (seed cpc-spot-check-20260927).
# The first pass was acquired outside Make (see README); its folder is frozen.
# jacob_spot_check_review.csv holds the saved review answers. The workbook is precious:
# a build refused over unsaved answers must not delete it.
.PRECIOUS: ../output/cpc_spot_check_review.xlsx
../%/cpc_spot_check_items.csv ../%/cpc_spot_check_review.xlsx: build_spot_check_workbook.py ../../../_lib/cpc_statement_measures.py \
	cpc_spot_check_sample.csv jacob_spot_check_review.csv spot_check_codebook.md ../input/first_pass ../input/ulurp_cpc_reading_segments.csv \
	../input/cpc_statement_full_run_report_measures.csv ../input/ulurp_cpc_statements_full_sol_high_20260927.csv | ../output ../report
	python3 $<

# Row judging was acquired outside Make (see README); its folder is frozen.
../output/cpc_spot_check_scores.csv: score_spot_check.py ../output/cpc_spot_check_items.csv jacob_spot_check_review.csv \
	../input/row_judging ../input/ulurp_cpc_reading_segments.csv | ../output ../report
	python3 $<

../output/cpc_row_judge_packets.csv: write_judge_packets.py cpc_spot_check_sample.csv \
	../input/ulurp_cpc_statements_full_sol_high_20260927.csv ../input/ulurp_cpc_reading_segments.csv | ../output ../report ../temp
	python3 $<

../input/row_judging: ../../../../data_raw/cpc_spot_check_row_judging/claude_opus_5_5_20260928 | ../input
	ln -sfn $< $@

../input/first_pass: ../../../../data_raw/cpc_spot_check_first_pass/claude_opus_5_5_20260927 | ../input
	ln -sfn $< $@

../input/ulurp_cpc_statements_full_sol_high_20260927.csv: ../../../extract_ulurp_cpc_statements/output/ulurp_cpc_statements_full_sol_high_20260927.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_statement_status_full_sol_high_20260927.csv: ../../../extract_ulurp_cpc_statements/output/ulurp_cpc_statement_status_full_sol_high_20260927.csv | ../input
	ln -sf $< $@

../input/cpc_statement_full_run_report_measures.csv: ../../summarize_cpc_statement_report_measures/output/cpc_statement_full_run_report_measures.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_reading_roster.csv: ../../../build_ulurp_cpc_reading_text/output/ulurp_cpc_reading_roster.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_human_coding.csv: ../../../summarize_text_cpc_trends/output/ulurp_cpc_human_coding.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_statement_sample.csv: ../../../extract_ulurp_cpc_statements/output/ulurp_cpc_statement_sample.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_reading_segments.csv: ../../../build_ulurp_cpc_reading_text/output/ulurp_cpc_reading_segments.csv | ../input
	ln -sf $< $@

link-inputs: ../input/first_pass ../input/row_judging ../input/ulurp_cpc_statements_full_sol_high_20260927.csv \
	../input/ulurp_cpc_statement_status_full_sol_high_20260927.csv ../input/cpc_statement_full_run_report_measures.csv \
	../input/ulurp_cpc_reading_roster.csv ../input/ulurp_cpc_human_coding.csv ../input/ulurp_cpc_statement_sample.csv \
	../input/ulurp_cpc_reading_segments.csv

include ../../generic.make
