SHELL := bash

RECALL_YEARS := 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025
FETCH_MODE := download
REPORT_FETCH_SCOPE := all_matched
ATTACHMENT_FETCH_MODE := candidate_attachments
FULL_TEXT_EXPORT := write_full_text

all: ../output/ulurp_modification_council_document_text.csv

../output/ulurp_modification_council_document_text.csv: ../../../fetch_council_modification_documents/code/fetch_council_modification_documents.py \
	../input/ulurp_modification_project_spine.csv \
	../input/ulurp_modification_project_matter_crosswalk.csv \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_detail_files.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_action_details.csv) | ../output ../temp
	python3 $< "$(RECALL_YEARS)" "$(FETCH_MODE)" "$(REPORT_FETCH_SCOPE)" "$(ATTACHMENT_FETCH_MODE)" "$(FULL_TEXT_EXPORT)"

../input/ulurp_modification_project_spine.csv: ../../../build_ulurp_modification_spine/output/ulurp_modification_project_spine.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_modification_project_matter_crosswalk.csv: ../../../build_ulurp_modification_spine/output/ulurp_modification_project_matter_crosswalk.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_detail_files.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_detail_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_action_details.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_action_details.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_modification_project_spine.csv \
	../input/ulurp_modification_project_matter_crosswalk.csv \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_detail_files.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_action_details.csv)

include ../../generic.make
