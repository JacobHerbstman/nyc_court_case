SHELL := bash

RECALL_YEARS := 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025

all: ../output/council_land_use_decision_universe_validation_summary.csv

../output/council_land_use_decision_universe_validation_summary.csv: validate_council_land_use_decision_universe.py \
	../input/council_land_use_decision_panel.csv \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_matter_index.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_history_events.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_action_details.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_member_votes.csv) | ../output
	python3 $<

../input/council_land_use_decision_panel.csv: ../../../build_council_land_use_decision_panel/output/council_land_use_decision_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_matter_index.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_matter_index.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_history_events.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_history_events.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_action_details.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_action_details.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_member_votes.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_member_votes.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/council_land_use_decision_panel.csv \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_matter_index.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_history_events.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_action_details.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_member_votes.csv)

include ../../generic.make
