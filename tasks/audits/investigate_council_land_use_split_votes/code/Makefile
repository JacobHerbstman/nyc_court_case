SHELL := bash

RECALL_YEARS := 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025

all: ../output/council_land_use_split_vote_annual_decomposition.csv \
	../output/council_land_use_split_vote_dissent_size_year.csv \
	../output/council_land_use_split_vote_local_member_alignment_year.csv \
	../output/council_land_use_split_vote_matter_type_year.csv \
	../output/council_land_use_split_vote_roll_call_signature_year.csv \
	../output/council_land_use_split_vote_top_roll_call_signatures.csv \
	../output/council_land_use_missing_geography_roll_call_repair_queue.csv \
	../output/council_land_use_split_vote_top_matter_examples.csv \
	../output/council_land_use_split_vote_nonaffirmative_member_rows.csv \
	../output/council_land_use_split_vote_nonaffirmative_member_summary.csv \
	../output/council_land_use_split_vote_member_match_qc.csv \
	../output/council_land_use_split_vote_unmatched_member_vote_actions.csv \
	../output/council_land_use_local_member_no_abstain_roll_call_events.csv \
	../output/council_land_use_local_member_no_abstain_matter_events.csv \
	../output/council_land_use_local_member_no_abstain_roll_call_rate_year.csv \
	../output/council_land_use_local_member_no_abstain_roll_call_rate_rolling_3_year.csv \
	../output/council_land_use_local_member_no_abstain_roll_call_rate_rolling_year.csv \
	../output/council_land_use_roll_call_multi_district_year.csv \
	../output/council_land_use_roll_call_multi_district_rolling_year.csv \
	../output/council_land_use_local_member_no_abstain_rolling_year.csv \
	../output/council_land_use_local_member_no_abstain_roll_call_year.csv \
	../output/council_land_use_local_member_no_abstain_roll_call_rolling_year.csv \
	../output/council_land_use_seed_overrule_case_overlap.csv \
	../output/council_land_use_nonlocal_dissent_matter_rows.csv \
	../output/council_land_use_nonlocal_dissent_year.csv \
	../output/council_land_use_nonlocal_dissent_rolling_year.csv \
	../output/council_land_use_nonlocal_dissent_size_distribution.csv \
	../output/council_land_use_nonlocal_dissent_member_period_summary.csv \
	../output/council_land_use_nonlocal_dissent_member_concentration.csv \
	../output/council_land_use_nonlocal_dissent_pivotality_period.csv \
	../output/council_land_use_nonlocal_dissent_action_category_period.csv \
	../output/council_land_use_unanimity_composition_decomposition.csv \
	../output/council_land_use_final_member_vote_rows_with_local_status.csv \
	../output/council_land_use_split_vote_decomposition.pdf \
	../output/council_land_use_split_vote_dissent_size.pdf \
	../output/council_land_use_split_vote_local_member_alignment.pdf \
	../output/council_land_use_local_member_no_abstain_roll_call_rate_trends.pdf \
	../output/council_land_use_local_member_no_abstain_roll_call_rate_rolling_3_trends.pdf \
	../output/council_land_use_local_member_no_abstain_roll_call_rate_rolling_trends.pdf \
	../output/council_land_use_roll_call_multi_district_rolling_trends.pdf \
	../output/council_land_use_local_member_no_abstain_trends.pdf \
	../output/council_land_use_local_member_no_abstain_rolling_trends.pdf \
	../output/council_land_use_local_member_no_abstain_roll_call_trends.pdf \
	../output/council_land_use_local_member_no_abstain_roll_call_rolling_trends.pdf \
	../output/council_land_use_nonlocal_dissent_trends.pdf \
	../output/council_land_use_nonlocal_dissent_vote_type_trends.pdf \
	../output/council_land_use_nonlocal_dissent_pivotality.pdf

../output/council_land_use_split_vote_annual_decomposition.csv: investigate_council_land_use_split_votes.R \
	../input/council_land_use_decision_panel.csv \
	../input/council_land_use_decision_universe_validation_summary.csv \
	../input/member_deference_nonapproval_member_votes.csv \
	../input/charter_overrule_seed_cases.csv \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_member_votes.csv) | ../output
	Rscript $<

../output/council_land_use_split_vote_dissent_size_year.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_split_vote_local_member_alignment_year.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_split_vote_matter_type_year.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_split_vote_roll_call_signature_year.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_split_vote_top_roll_call_signatures.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_missing_geography_roll_call_repair_queue.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_split_vote_top_matter_examples.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_split_vote_nonaffirmative_member_rows.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_split_vote_nonaffirmative_member_summary.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_split_vote_member_match_qc.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_split_vote_unmatched_member_vote_actions.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_local_member_no_abstain_roll_call_events.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_local_member_no_abstain_matter_events.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_local_member_no_abstain_roll_call_rate_year.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_local_member_no_abstain_roll_call_rate_rolling_3_year.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_local_member_no_abstain_roll_call_rate_rolling_year.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_roll_call_multi_district_year.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_roll_call_multi_district_rolling_year.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_local_member_no_abstain_rolling_year.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_local_member_no_abstain_roll_call_year.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_local_member_no_abstain_roll_call_rolling_year.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_seed_overrule_case_overlap.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_nonlocal_dissent_matter_rows.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_nonlocal_dissent_year.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_nonlocal_dissent_rolling_year.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_nonlocal_dissent_size_distribution.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_nonlocal_dissent_member_period_summary.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_nonlocal_dissent_member_concentration.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_nonlocal_dissent_pivotality_period.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_nonlocal_dissent_action_category_period.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_unanimity_composition_decomposition.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_final_member_vote_rows_with_local_status.csv: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_split_vote_decomposition.pdf: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_split_vote_dissent_size.pdf: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_split_vote_local_member_alignment.pdf: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_local_member_no_abstain_roll_call_rate_trends.pdf: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_local_member_no_abstain_roll_call_rate_rolling_3_trends.pdf: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_local_member_no_abstain_roll_call_rate_rolling_trends.pdf: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_roll_call_multi_district_rolling_trends.pdf: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_local_member_no_abstain_trends.pdf: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_local_member_no_abstain_rolling_trends.pdf: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_local_member_no_abstain_roll_call_trends.pdf: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_local_member_no_abstain_roll_call_rolling_trends.pdf: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_nonlocal_dissent_trends.pdf: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_nonlocal_dissent_vote_type_trends.pdf: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../output/council_land_use_nonlocal_dissent_pivotality.pdf: ../output/council_land_use_split_vote_annual_decomposition.csv
	@test -s "$@"

../input/council_land_use_decision_panel.csv: ../../../build_council_land_use_decision_panel/output/council_land_use_decision_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_land_use_decision_universe_validation_summary.csv: ../../../validate_council_land_use_decision_universe/output/council_land_use_decision_universe_validation_summary.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_nonapproval_member_votes.csv: ../../../fetch_member_deference_nonapproval_action_votes/output/member_deference_nonapproval_member_votes.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/charter_overrule_seed_cases.csv: ../../build_member_deference_overrule_candidates/code/charter_overrule_seed_cases.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_member_votes.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_member_votes.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/council_land_use_decision_panel.csv \
	../input/council_land_use_decision_universe_validation_summary.csv \
	../input/member_deference_nonapproval_member_votes.csv \
	../input/charter_overrule_seed_cases.csv \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_member_votes.csv)

include ../../generic.make
