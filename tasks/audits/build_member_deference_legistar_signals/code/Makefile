SHELL := bash

RECALL_YEARS := 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010

all: $(foreach year,$(RECALL_YEARS),../output/legistar_$(year)_member_override_matter_signals.csv) \
	$(foreach year,$(RECALL_YEARS),../output/legistar_$(year)_member_override_summary.csv) \
	$(foreach year,$(RECALL_YEARS),../output/legistar_$(year)_member_override_qc.csv)

../output/legistar_%_member_override_matter_signals.csv: build_member_deference_legistar_signals.py \
	../input/legistar_%_broad_recall_action_details.csv \
	../input/legistar_%_broad_recall_matter_index.csv \
	../input/council_member_roster_master.csv | ../output
	python3 $< $*

../output/legistar_%_member_override_summary.csv: ../output/legistar_%_member_override_matter_signals.csv
	@test -s "$@"

../output/legistar_%_member_override_qc.csv: ../output/legistar_%_member_override_matter_signals.csv
	@test -s "$@"

../input/legistar_%_broad_recall_action_details.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_action_details.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_matter_index.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_matter_index.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_member_roster_master.csv: ../../../build_council_member_roster/output/council_member_roster_master.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/council_member_roster_master.csv \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_action_details.csv) \
	$(foreach year,$(RECALL_YEARS),../input/legistar_$(year)_broad_recall_matter_index.csv)

include ../../generic.make
