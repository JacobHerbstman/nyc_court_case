PILOT_YEARS := 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010

all: ../output/member_deference_pilot_1998_2010_bundle_candidates.csv \
	../output/member_deference_pilot_1998_2010_audit_sheet.csv \
	../output/member_deference_pilot_1998_2010_matter_crosswalk.csv \
	../output/member_deference_pilot_1998_2010_bundle_bridge_review.csv \
	../output/member_deference_pilot_1998_2010_pressure_candidate_audit_targets.csv \
	../output/member_deference_pilot_1998_2010_pressure_candidate_series.csv \
	../output/member_deference_pilot_1998_2010_summary.csv \
	../output/member_deference_pilot_1998_2010_qc.csv

../output/member_deference_pilot_1998_2010_bundle_candidates.csv: build_member_deference_pilot_audit.py \
	$(foreach year,$(PILOT_YEARS),../input/legistar_$(year)_broad_recall_matter_index.csv) \
	$(foreach year,$(PILOT_YEARS),../input/legistar_$(year)_broad_recall_history_events.csv) \
	$(foreach year,$(PILOT_YEARS),../input/legistar_$(year)_broad_recall_action_details.csv) \
	$(foreach year,$(PILOT_YEARS),../input/legistar_$(year)_member_override_matter_signals.csv) \
	../input/council_member_roster_master.csv | ../output
	python3 $<

../output/member_deference_pilot_1998_2010_audit_sheet.csv: ../output/member_deference_pilot_1998_2010_bundle_candidates.csv
	@test -s "$@"

../output/member_deference_pilot_1998_2010_matter_crosswalk.csv: ../output/member_deference_pilot_1998_2010_bundle_candidates.csv
	@test -s "$@"

../output/member_deference_pilot_1998_2010_bundle_bridge_review.csv: ../output/member_deference_pilot_1998_2010_bundle_candidates.csv
	@test -s "$@"

../output/member_deference_pilot_1998_2010_pressure_candidate_audit_targets.csv: ../output/member_deference_pilot_1998_2010_bundle_candidates.csv
	@test -s "$@"

../output/member_deference_pilot_1998_2010_pressure_candidate_series.csv: ../output/member_deference_pilot_1998_2010_bundle_candidates.csv
	@test -s "$@"

../output/member_deference_pilot_1998_2010_summary.csv: ../output/member_deference_pilot_1998_2010_bundle_candidates.csv
	@test -s "$@"

../output/member_deference_pilot_1998_2010_qc.csv: ../output/member_deference_pilot_1998_2010_bundle_candidates.csv
	@test -s "$@"

../input/legistar_%_broad_recall_matter_index.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_matter_index.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_history_events.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_history_events.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_broad_recall_action_details.csv: ../../../fetch_council_land_use_records/output/legistar_%_broad_recall_action_details.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/legistar_%_member_override_matter_signals.csv: ../../../build_member_deference_legistar_signals/output/legistar_%_member_override_matter_signals.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_member_roster_master.csv: ../../../stage_council_member_roster/output/council_member_roster_master.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: $(foreach year,$(PILOT_YEARS),../input/legistar_$(year)_broad_recall_matter_index.csv) \
	$(foreach year,$(PILOT_YEARS),../input/legistar_$(year)_broad_recall_history_events.csv) \
	$(foreach year,$(PILOT_YEARS),../input/legistar_$(year)_broad_recall_action_details.csv) \
	$(foreach year,$(PILOT_YEARS),../input/legistar_$(year)_member_override_matter_signals.csv) \
	../input/council_member_roster_master.csv

include ../../generic.make
