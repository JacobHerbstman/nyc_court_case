SHELL := bash

all: ../output/member_deference_nonapproval_geography_recovery_summary.csv \
	../output/member_deference_nonapproval_geography_recovery_unresolved.csv \
	../output/member_deference_nonapproval_application_crosswalk.csv \
	../output/member_deference_nonapproval_title_location_candidates.csv \
	../output/member_deference_nonapproval_geography_review_queue.csv \
	../output/member_deference_nonapproval_geography_review_batches.md \
	../output/member_deference_nonapproval_geography_recovery_qc.csv \
	../output/member_deference_nonapproval_geography_chatgpt_review_responses.csv \
	../output/member_deference_nonapproval_geography_chatgpt_review_responses.md

../output/member_deference_nonapproval_geography_recovery_summary.csv: audit_member_deference_nonapproval_geography_recovery.py \
	../input/member_deference_nonapproval_geography_recovery.csv \
	../input/member_deference_final_action_vote_queue.csv | ../output
	python3 $<

../output/member_deference_nonapproval_geography_recovery_unresolved.csv: ../output/member_deference_nonapproval_geography_recovery_summary.csv
	@test -s "$@"

../output/member_deference_nonapproval_application_crosswalk.csv: ../output/member_deference_nonapproval_geography_recovery_summary.csv
	@test -s "$@"

../output/member_deference_nonapproval_title_location_candidates.csv: ../output/member_deference_nonapproval_geography_recovery_summary.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_review_queue.csv: ../output/member_deference_nonapproval_geography_recovery_summary.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_review_batches.md: ../output/member_deference_nonapproval_geography_recovery_summary.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_recovery_qc.csv: ../output/member_deference_nonapproval_geography_recovery_summary.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_chatgpt_review_responses.csv: member_deference_nonapproval_geography_chatgpt_review_responses.csv | ../output
	cp $< $@

../output/member_deference_nonapproval_geography_chatgpt_review_responses.md: member_deference_nonapproval_geography_chatgpt_review_responses.md | ../output
	cp $< $@

../input/member_deference_nonapproval_geography_recovery.csv: ../../../recover_member_deference_nonapproval_geography/output/member_deference_nonapproval_geography_recovery.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_final_action_vote_queue.csv: ../../../build_member_deference_vote_panel/output/member_deference_final_action_vote_queue.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/member_deference_nonapproval_geography_recovery.csv \
	../input/member_deference_final_action_vote_queue.csv

include ../../generic.make
