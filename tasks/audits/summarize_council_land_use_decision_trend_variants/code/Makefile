SHELL := bash

all: ../output/council_land_use_decision_trends_year.csv \
	../output/council_land_use_decision_trends_long.csv \
	../output/council_land_use_local_member_vote_trends_year.csv \
	../output/council_land_use_local_member_rollcall_adoption_position_year.csv \
	../output/council_land_use_local_member_rollcall_adoption_position_event_year.csv \
	../output/council_land_use_local_member_rollcall_adoption_position_period.csv \
	../output/council_land_use_pre_post_2002_summary.csv \
	../output/council_land_use_action_category_summary.csv \
	../output/council_land_use_action_category_period.csv \
	../output/council_land_use_action_code_summary.csv \
	../output/council_land_use_zmzr_vs_docket_year.csv \
	../output/council_land_use_decision_volume_trends.pdf \
	../output/council_land_use_nonapproval_trends.pdf \
	../output/council_land_use_vote_coverage_trends.pdf \
	../output/council_land_use_local_member_vote_trends.pdf \
	../output/council_land_use_approval_over_local_member_objection_rolling.pdf \
	../output/council_land_use_approval_over_local_member_objection_rolling4.pdf \
	../output/council_land_use_approval_over_local_member_objection_rolling4_clean.pdf \
	../output/council_land_use_approval_over_local_member_objection_rolling5_with_raw_clean.pdf \
	../output/council_land_use_approval_over_local_member_objection_rolling5_clean.pdf \
	../output/council_land_use_unanimous_vote_share_rolling.pdf \
	../output/council_land_use_matter_volume_rolling.pdf \
	../output/council_land_use_zmzr_vs_docket_raw.pdf \
	../output/council_land_use_zmzr_vs_docket_rolling3.pdf \
	../output/council_land_use_adoption_by_local_member_rollcall_position_rolling3.pdf \
	../output/council_land_use_adoption_by_local_member_rollcall_position_rolling5_exclude2025.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_exclude2025.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling4.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling4_clean.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_clean.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling3_with_raw_clean.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling4_with_raw_clean.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling5_with_raw_clean.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling5_clean.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_event_rolling5_with_raw_clean.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_event_rolling5_clean.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_event_count_rolling5_with_raw_clean.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_event_count_rolling5_clean.pdf

../output/council_land_use_decision_trends_year.csv: summarize_council_land_use_decision_trend_variants.R \
	../input/council_land_use_decision_panel.csv \
	../input/council_land_use_local_member_votes.csv \
	../input/council_land_use_decision_universe_validation_summary.csv | ../output
	Rscript $<

../output/council_land_use_decision_trends_long.csv: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_local_member_vote_trends_year.csv: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_local_member_rollcall_adoption_position_year.csv: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_local_member_rollcall_adoption_position_event_year.csv: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_local_member_rollcall_adoption_position_period.csv: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_pre_post_2002_summary.csv: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_action_category_summary.csv: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_action_category_period.csv: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_action_code_summary.csv: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_zmzr_vs_docket_year.csv: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_decision_volume_trends.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_nonapproval_trends.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_vote_coverage_trends.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_local_member_vote_trends.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_approval_over_local_member_objection_rolling.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_approval_over_local_member_objection_rolling4.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_approval_over_local_member_objection_rolling4_clean.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_approval_over_local_member_objection_rolling5_with_raw_clean.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_approval_over_local_member_objection_rolling5_clean.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_unanimous_vote_share_rolling.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_matter_volume_rolling.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_zmzr_vs_docket_raw.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_zmzr_vs_docket_rolling3.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_by_local_member_rollcall_position_rolling3.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_by_local_member_rollcall_position_rolling5_exclude2025.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_exclude2025.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling4.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling4_clean.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_clean.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling3_with_raw_clean.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling4_with_raw_clean.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling5_with_raw_clean.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling5_clean.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_over_local_member_rollcall_opposition_event_rolling5_with_raw_clean.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_over_local_member_rollcall_opposition_event_rolling5_clean.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_over_local_member_rollcall_opposition_event_count_rolling5_with_raw_clean.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../output/council_land_use_adoption_over_local_member_rollcall_opposition_event_count_rolling5_clean.pdf: ../output/council_land_use_decision_trends_year.csv
	@test -s "$@"

../input/council_land_use_decision_panel.csv: ../../../build_council_land_use_decision_panel/output/council_land_use_decision_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_land_use_local_member_votes.csv: ../../../build_council_land_use_decision_panel/output/council_land_use_local_member_votes.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_land_use_decision_universe_validation_summary.csv: ../../validate_council_land_use_decision_universe/output/council_land_use_decision_universe_validation_summary.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/council_land_use_decision_panel.csv \
	../input/council_land_use_local_member_votes.csv \
	../input/council_land_use_decision_universe_validation_summary.csv

include ../../generic.make
