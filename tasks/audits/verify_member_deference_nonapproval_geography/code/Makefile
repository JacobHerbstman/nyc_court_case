SHELL := bash

all: ../output/member_deference_nonapproval_geography_official_verification.csv \
	../output/member_deference_nonapproval_geography_official_sources.csv \
	../output/member_deference_nonapproval_geography_conservative_queue.csv \
	../output/member_deference_nonapproval_geography_conservative_summary.csv \
	../output/member_deference_nonapproval_geography_second_pass_review_queue.csv \
	../output/member_deference_nonapproval_geography_second_pass_review_batches.md \
	../output/member_deference_nonapproval_geography_official_verification_summary.csv \
	../output/member_deference_nonapproval_geography_official_verification_qc.csv

../output/member_deference_nonapproval_geography_official_verification.csv: verify_member_deference_nonapproval_geography.py \
	../input/member_deference_final_action_vote_queue.csv \
	../input/member_deference_nonapproval_geography_recovery.csv \
	../input/member_deference_nonapproval_geography_review_queue.csv \
	../input/member_deference_nonapproval_geography_chatgpt_review_responses.csv \
	../input/member_deference_matter_universe.csv \
	../input/dcp_mappluto_current_25v4.parquet \
	../input/council_member_roster_master.csv | ../output
	python3 $<

../output/member_deference_nonapproval_geography_official_sources.csv: ../output/member_deference_nonapproval_geography_official_verification.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_conservative_queue.csv: ../output/member_deference_nonapproval_geography_official_verification.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_conservative_summary.csv: ../output/member_deference_nonapproval_geography_official_verification.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_second_pass_review_queue.csv: ../output/member_deference_nonapproval_geography_official_verification.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_second_pass_review_batches.md: ../output/member_deference_nonapproval_geography_official_verification.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_official_verification_summary.csv: ../output/member_deference_nonapproval_geography_official_verification.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_official_verification_qc.csv: ../output/member_deference_nonapproval_geography_official_verification.csv
	@test -s "$@"

../input/member_deference_final_action_vote_queue.csv: ../../build_member_deference_vote_panel/output/member_deference_final_action_vote_queue.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_nonapproval_geography_recovery.csv: ../../recover_member_deference_nonapproval_geography/output/member_deference_nonapproval_geography_recovery.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_nonapproval_geography_review_queue.csv: ../../recover_member_deference_nonapproval_geography/output/member_deference_nonapproval_geography_review_queue.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_nonapproval_geography_chatgpt_review_responses.csv: ../../recover_member_deference_nonapproval_geography/output/member_deference_nonapproval_geography_chatgpt_review_responses.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_matter_universe.csv: ../../build_member_deference_vote_panel/output/member_deference_matter_universe.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_mappluto_current_25v4.parquet: ../../../stage_mappluto_lots/output/dcp_mappluto_current_25v4.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_member_roster_master.csv: ../../../stage_council_member_roster/output/council_member_roster_master.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/member_deference_final_action_vote_queue.csv \
	../input/member_deference_nonapproval_geography_recovery.csv \
	../input/member_deference_nonapproval_geography_review_queue.csv \
	../input/member_deference_nonapproval_geography_chatgpt_review_responses.csv \
	../input/member_deference_matter_universe.csv \
	../input/dcp_mappluto_current_25v4.parquet \
	../input/council_member_roster_master.csv

include ../../../generic.make
