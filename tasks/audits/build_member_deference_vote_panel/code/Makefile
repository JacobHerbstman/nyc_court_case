SHELL := bash

RECALL_YEARS := 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025

.PRECIOUS: $(foreach year,$(RECALL_YEARS),../../../fetch_council_land_use_records/output/legistar_$(year)_broad_recall_action_details.csv)
.PRECIOUS: $(foreach year,$(RECALL_YEARS),../../../fetch_council_land_use_records/output/legistar_$(year)_broad_recall_member_votes.csv)
.PRECIOUS: $(foreach year,$(RECALL_YEARS),../../../fetch_council_land_use_records/output/legistar_$(year)_broad_recall_matter_index.csv)
.PRECIOUS: $(foreach year,$(RECALL_YEARS),../../../fetch_council_land_use_records/output/legistar_$(year)_broad_recall_history_events.csv)
.SECONDARY: $(foreach year,$(RECALL_YEARS),../../../fetch_council_land_use_records/output/legistar_$(year)_broad_recall_action_details.csv)
.SECONDARY: $(foreach year,$(RECALL_YEARS),../../../fetch_council_land_use_records/output/legistar_$(year)_broad_recall_member_votes.csv)
.SECONDARY: $(foreach year,$(RECALL_YEARS),../../../fetch_council_land_use_records/output/legistar_$(year)_broad_recall_matter_index.csv)
.SECONDARY: $(foreach year,$(RECALL_YEARS),../../../fetch_council_land_use_records/output/legistar_$(year)_broad_recall_history_events.csv)

all: ../output/member_deference_vote_panel.csv \
	../output/member_deference_vote_panel_summary.csv \
	../output/member_deference_vote_panel_qc.csv \
	../output/member_deference_vote_panel_exception_candidates.csv \
	../output/member_deference_vote_panel_unresolved.csv \
	../output/member_deference_matter_universe.csv \
	../output/member_deference_matter_universe_summary.csv \
	../output/member_deference_filed_matter_audit.csv \
	../output/member_deference_final_action_vote_queue.csv \
	../output/member_deference_final_action_vote_queue_summary.csv

../output/member_deference_vote_panel.csv: build_member_deference_vote_panel.py \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_action_details.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_member_votes.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_matter_index.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_history_events.csv) \
	../input/council_member_roster_master.csv \
	../input/zap_project_data.parquet \
	../input/council_land_use_ai_geography_accepted_repairs.csv | ../output
	python3 $<

../output/member_deference_vote_panel_summary.csv: ../output/member_deference_vote_panel.csv
	@test -s "$@"

../output/member_deference_vote_panel_qc.csv: ../output/member_deference_vote_panel.csv
	@test -s "$@"

../output/member_deference_vote_panel_exception_candidates.csv: ../output/member_deference_vote_panel.csv
	@test -s "$@"

../output/member_deference_vote_panel_unresolved.csv: ../output/member_deference_vote_panel.csv
	@test -s "$@"

../output/member_deference_matter_universe.csv: ../output/member_deference_vote_panel.csv
	@test -s "$@"

../output/member_deference_matter_universe_summary.csv: ../output/member_deference_vote_panel.csv
	@test -s "$@"

../output/member_deference_filed_matter_audit.csv: ../output/member_deference_vote_panel.csv
	@test -s "$@"

../output/member_deference_final_action_vote_queue.csv: ../output/member_deference_vote_panel.csv
	@test -s "$@"

../output/member_deference_final_action_vote_queue_summary.csv: ../output/member_deference_vote_panel.csv
	@test -s "$@"

../input/legistar_%_broad_recall_action_details.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_action_details.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_member_votes.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_member_votes.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_matter_index.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_matter_index.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_history_events.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_history_events.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_member_roster_master.csv: ../../../stage_council_member_roster/output/council_member_roster_master.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_data.parquet: ../../../stage_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_land_use_ai_geography_accepted_repairs.csv: ../../../stage_council_land_use_ai_geography_repairs/output/council_land_use_ai_geography_accepted_repairs.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/council_member_roster_master.csv \
	../input/zap_project_data.parquet \
	../input/council_land_use_ai_geography_accepted_repairs.csv \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_action_details.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_member_votes.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_matter_index.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_history_events.csv)

include ../../../generic.make
