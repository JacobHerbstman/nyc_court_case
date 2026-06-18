SHELL := bash

RECALL_YEARS := 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025
FETCH_MODE := download
REPORT_FETCH_SCOPE := all_matched
ATTACHMENT_FETCH_MODE := candidate_attachments

all: ../output/ulurp_modification_council_document_links.csv \
	../output/ulurp_modification_council_document_text.csv \
	../output/ulurp_modification_council_document_snippets.csv \
	../output/ulurp_modification_council_document_fetch_failures.csv \
	../output/ulurp_modification_council_document_qc.csv

../output/ulurp_modification_council_document_links.csv: fetch_council_modification_documents.py \
	../input/ulurp_modification_project_spine.csv \
	../input/ulurp_modification_project_matter_crosswalk.csv \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_detail_files.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_action_details.csv) | ../output ../temp
	python3 $< "$(RECALL_YEARS)" "$(FETCH_MODE)" "$(REPORT_FETCH_SCOPE)" "$(ATTACHMENT_FETCH_MODE)"

../output/ulurp_modification_council_document_text.csv: ../output/ulurp_modification_council_document_links.csv
	@test -s "$@"

../output/ulurp_modification_council_document_snippets.csv: ../output/ulurp_modification_council_document_links.csv
	@test -s "$@"

../output/ulurp_modification_council_document_fetch_failures.csv: ../output/ulurp_modification_council_document_links.csv
	@test -s "$@"

../output/ulurp_modification_council_document_qc.csv: ../output/ulurp_modification_council_document_links.csv
	@test -s "$@"

../input/ulurp_modification_project_spine.csv: ../../build_ulurp_modification_spine/output/ulurp_modification_project_spine.csv | ../input
	@target="$$(cd ../../build_ulurp_modification_spine/output && pwd)/ulurp_modification_project_spine.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_project_matter_crosswalk.csv: ../../build_ulurp_modification_spine/output/ulurp_modification_project_matter_crosswalk.csv | ../input
	@target="$$(cd ../../build_ulurp_modification_spine/output && pwd)/ulurp_modification_project_matter_crosswalk.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/legistar_%_broad_recall_detail_files.csv: ../../fetch_council_land_use_records/output/legistar_%_broad_recall_detail_files.csv | ../input
	@target="$$(cd ../../fetch_council_land_use_records/output && pwd)/legistar_$*_broad_recall_detail_files.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/legistar_%_broad_recall_action_details.csv: ../../fetch_council_land_use_records/output/legistar_%_broad_recall_action_details.csv | ../input
	@target="$$(cd ../../fetch_council_land_use_records/output && pwd)/legistar_$*_broad_recall_action_details.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

link-inputs: ../input/ulurp_modification_project_spine.csv \
	../input/ulurp_modification_project_matter_crosswalk.csv \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_detail_files.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_action_details.csv)

include ../../generic.make
