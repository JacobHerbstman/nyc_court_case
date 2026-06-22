SHELL := bash

RECALL_YEARS := 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025

all: ../output/council_land_use_outside_current_type_candidates.csv \
	../output/legistar_matter_type_lookup.csv \
	../output/legistar_all_type_query_rows.csv \
	../output/legistar_all_type_matter_index.csv \
	../output/legistar_all_type_page_fetches.csv \
	../output/council_land_use_matter_type_query_summary.csv \
	../output/council_land_use_matter_type_cap_suspects.csv \
	../output/council_land_use_outside_current_type_candidate_summary.csv \
	../output/council_land_use_outside_current_type_candidate_triage.csv \
	../output/council_land_use_outside_current_type_candidate_triage_summary.csv \
	../output/council_land_use_outside_current_type_candidate_triage_samples.csv \
	../output/council_land_use_adjacent_current_recall_crosswalk.csv \
	../output/council_land_use_matter_type_universe_audit_qc.csv \
	../output/research_understanding_checklist.md

../output/council_land_use_outside_current_type_candidates.csv: audit_council_land_use_matter_type_universe.py \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_matter_index.csv) | ../output ../temp
	python3 -v -S $< 2> ../temp/python_verbose_import_trace.log

../output/legistar_matter_type_lookup.csv: | ../output/council_land_use_outside_current_type_candidates.csv
	@test -s "$@"

../output/legistar_all_type_query_rows.csv: | ../output/council_land_use_outside_current_type_candidates.csv
	@test -s "$@"

../output/legistar_all_type_matter_index.csv: | ../output/council_land_use_outside_current_type_candidates.csv
	@test -s "$@"

../output/legistar_all_type_page_fetches.csv: | ../output/council_land_use_outside_current_type_candidates.csv
	@test -s "$@"

../output/council_land_use_matter_type_query_summary.csv: | ../output/council_land_use_outside_current_type_candidates.csv
	@test -s "$@"

../output/council_land_use_matter_type_cap_suspects.csv: build_cap_suspects.py ../output/council_land_use_matter_type_query_summary.csv | ../output
	python3 -S $<

../output/council_land_use_outside_current_type_candidate_summary.csv: | ../output/council_land_use_outside_current_type_candidates.csv
	@test -s "$@"

../output/council_land_use_outside_current_type_candidate_triage.csv: triage_outside_current_candidates.py \
	../output/council_land_use_outside_current_type_candidates.csv \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_matter_index.csv) | ../output
	python3 -S $<

../output/council_land_use_outside_current_type_candidate_triage_summary.csv: ../output/council_land_use_outside_current_type_candidate_triage.csv
	@test -s "$@"

../output/council_land_use_outside_current_type_candidate_triage_samples.csv: ../output/council_land_use_outside_current_type_candidate_triage.csv
	@test -s "$@"

../output/council_land_use_adjacent_current_recall_crosswalk.csv: ../output/council_land_use_outside_current_type_candidate_triage.csv
	@test -s "$@"

../output/council_land_use_matter_type_universe_audit_qc.csv: | ../output/council_land_use_outside_current_type_candidates.csv
	@test -s "$@"

../output/research_understanding_checklist.md: | ../output/council_land_use_outside_current_type_candidates.csv
	@test -s "$@"

../input/legistar_%_broad_recall_matter_index.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_matter_index.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: $(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_matter_index.csv)

include ../../../generic.make
