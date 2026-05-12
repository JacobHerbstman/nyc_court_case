SHELL := bash
include ../../shell_functions.make

all: ../output/council_land_use_fetch_files.csv \
	../output/council_land_use_fetch_qc.csv \
	../output/legistar_2001_broad_recall_matter_index.csv \
	../output/legistar_2001_broad_recall_detail_files.csv \
	../output/legistar_2001_broad_recall_history_events.csv \
	../output/legistar_2001_broad_recall_page_fetches.csv \
	../output/legistar_2001_broad_recall_count_check.csv \
	../output/legistar_2001_broad_recall_qc.csv

../output/council_land_use_fetch_files.csv: fetch_council_land_use_records.R council_land_use_seed_sources.csv ../../_lib/source_pipeline_utils.R | ../output
	$(R) $<

../output/council_land_use_fetch_qc.csv: ../output/council_land_use_fetch_files.csv
	@test -s "$@"

../output/legistar_2001_broad_recall_matter_index.csv: fetch_legistar_2001_broad_recall.py | ../output
	python3 $<

../output/legistar_2001_broad_recall_detail_files.csv: ../output/legistar_2001_broad_recall_matter_index.csv
	@test -s "$@"

../output/legistar_2001_broad_recall_history_events.csv: ../output/legistar_2001_broad_recall_matter_index.csv
	@test -s "$@"

../output/legistar_2001_broad_recall_page_fetches.csv: ../output/legistar_2001_broad_recall_matter_index.csv
	@test -s "$@"

../output/legistar_2001_broad_recall_count_check.csv: ../output/legistar_2001_broad_recall_matter_index.csv
	@test -s "$@"

../output/legistar_2001_broad_recall_qc.csv: ../output/legistar_2001_broad_recall_matter_index.csv
	@test -s "$@"

include ../../generic.make
