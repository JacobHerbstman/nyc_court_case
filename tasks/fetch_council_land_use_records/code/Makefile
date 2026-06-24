SHELL := bash
include ../../shell_functions.make

RECALL_YEARS := 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025

all: ../output/council_land_use_fetch_files.csv \
	$(foreach year,$(RECALL_YEARS),../output/legistar_$(year)_broad_recall_matter_index.csv) \
	$(foreach year,$(RECALL_YEARS),../output/legistar_$(year)_broad_recall_history_events.csv) \
	$(foreach year,$(RECALL_YEARS),../output/legistar_$(year)_broad_recall_detail_files.csv) \
	$(foreach year,$(RECALL_YEARS),../output/legistar_$(year)_broad_recall_action_details.csv) \
	$(foreach year,$(RECALL_YEARS),../output/legistar_$(year)_broad_recall_member_votes.csv) \
	$(foreach year,$(RECALL_YEARS),../output/legistar_$(year)_broad_recall_split_vote_signals.csv)

../output/council_land_use_fetch_files.csv: fetch_council_land_use_records.R council_land_use_seed_sources.csv ../../_lib/source_pipeline_utils.R | ../output
	$(R) $<

../output/legistar_%_broad_recall_matter_index.csv: fetch_legistar_broad_recall.py | ../output
	python3 $< $*

../output/legistar_%_broad_recall_history_events.csv: ../output/legistar_%_broad_recall_matter_index.csv
	@test -s "$@"

../output/legistar_%_broad_recall_detail_files.csv: ../output/legistar_%_broad_recall_matter_index.csv
	@test -s "$@"

../output/legistar_%_broad_recall_count_check.csv: ../output/legistar_%_broad_recall_matter_index.csv
	@test -s "$@"

../output/legistar_%_broad_recall_qc.csv: ../output/legistar_%_broad_recall_matter_index.csv
	@test -s "$@"

../output/legistar_%_broad_recall_action_details.csv: fetch_legistar_action_votes.py \
	../output/legistar_%_broad_recall_history_events.csv \
	../output/legistar_%_broad_recall_matter_index.csv | ../output
	python3 $< $*

../output/legistar_%_broad_recall_member_votes.csv: ../output/legistar_%_broad_recall_action_details.csv
	@test -s "$@"

../output/legistar_%_broad_recall_action_vote_qc.csv: ../output/legistar_%_broad_recall_action_details.csv
	@test -s "$@"

../output/legistar_%_broad_recall_split_vote_signals.csv: ../output/legistar_%_broad_recall_action_details.csv
	@test -s "$@"

link-inputs:

include ../../generic.make
