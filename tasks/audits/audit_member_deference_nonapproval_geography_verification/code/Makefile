SHELL := bash

all: ../output/member_deference_nonapproval_geography_official_sources.csv \
	../output/member_deference_nonapproval_geography_conservative_summary.csv \
	../output/member_deference_nonapproval_geography_second_pass_review_queue.csv \
	../output/member_deference_nonapproval_geography_second_pass_review_batches.md \
	../output/member_deference_nonapproval_geography_official_verification_summary.csv \
	../output/member_deference_nonapproval_geography_official_verification_qc.csv \
	../output/member_deference_nonapproval_geography_second_pass_chatgpt_responses.csv \
	../output/member_deference_nonapproval_geography_second_pass_chatgpt_responses.md

../output/member_deference_nonapproval_geography_official_sources.csv: audit_member_deference_nonapproval_geography_verification.py \
	../input/member_deference_nonapproval_geography_official_verification.csv \
	../input/member_deference_nonapproval_geography_conservative_queue.csv | ../output
	python3 $<

../output/member_deference_nonapproval_geography_conservative_summary.csv: ../output/member_deference_nonapproval_geography_official_sources.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_second_pass_review_queue.csv: ../output/member_deference_nonapproval_geography_official_sources.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_second_pass_review_batches.md: ../output/member_deference_nonapproval_geography_official_sources.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_official_verification_summary.csv: ../output/member_deference_nonapproval_geography_official_sources.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_official_verification_qc.csv: ../output/member_deference_nonapproval_geography_official_sources.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_second_pass_chatgpt_responses.csv: member_deference_nonapproval_geography_second_pass_chatgpt_responses.csv | ../output
	cp $< $@

../output/member_deference_nonapproval_geography_second_pass_chatgpt_responses.md: member_deference_nonapproval_geography_second_pass_chatgpt_responses.md | ../output
	cp $< $@

../input/member_deference_nonapproval_geography_official_verification.csv: ../../../verify_member_deference_nonapproval_geography/output/member_deference_nonapproval_geography_official_verification.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_nonapproval_geography_conservative_queue.csv: ../../../verify_member_deference_nonapproval_geography/output/member_deference_nonapproval_geography_conservative_queue.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/member_deference_nonapproval_geography_official_verification.csv \
	../input/member_deference_nonapproval_geography_conservative_queue.csv

include ../../generic.make
