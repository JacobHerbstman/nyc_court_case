SHELL := bash

RECALL_YEARS := 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025

all: ../output/council_land_use_decision_universe_validation_summary.csv \
	../output/council_land_use_decision_universe_year.csv \
	../output/council_land_use_decision_universe_fetch_query_year.csv \
	../output/council_land_use_decision_universe_disposition_year.csv \
	../output/council_land_use_decision_universe_vote_source_year.csv

../output/council_land_use_decision_universe_validation_summary.csv: validate_council_land_use_decision_universe.py \
	../input/council_land_use_decision_panel.csv \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_count_check.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_qc.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_action_vote_qc.csv) | ../output
	python3 $<

../output/council_land_use_decision_universe_year.csv: ../output/council_land_use_decision_universe_validation_summary.csv
	@test -s "$@"

../output/council_land_use_decision_universe_fetch_query_year.csv: ../output/council_land_use_decision_universe_validation_summary.csv
	@test -s "$@"

../output/council_land_use_decision_universe_disposition_year.csv: ../output/council_land_use_decision_universe_validation_summary.csv
	@test -s "$@"

../output/council_land_use_decision_universe_vote_source_year.csv: ../output/council_land_use_decision_universe_validation_summary.csv
	@test -s "$@"

../input/council_land_use_decision_panel.csv: ../../build_council_land_use_decision_panel/output/council_land_use_decision_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_count_check.csv: ../../fetch_council_land_use_records/output/legistar_%_broad_recall_count_check.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_qc.csv: ../../fetch_council_land_use_records/output/legistar_%_broad_recall_qc.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_action_vote_qc.csv: ../../fetch_council_land_use_records/output/legistar_%_broad_recall_action_vote_qc.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/council_land_use_decision_panel.csv \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_count_check.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_qc.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_action_vote_qc.csv)

include ../../generic.make
