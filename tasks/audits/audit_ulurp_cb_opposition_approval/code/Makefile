BREAK_YEAR := 2002
TRANSITION_END_YEAR := 2009
START_YEAR := 1975
END_YEAR := 2025
VALIDATION_CASES_PER_PERIOD := 15

all: ../output/ulurp_cb_opposition_approval_summary.csv \
	../output/ulurp_cb_opposition_approval.pdf \
	../output/ulurp_cb_opposition_approval_cases.csv \
	../output/ulurp_cb_opposition_transition.csv \
	../output/ulurp_cb_opposition_transition.pdf

../output/ulurp_cb_opposition_approval_summary.csv: audit_ulurp_cb_opposition_approval.R \
	../input/ulurp_cpc_text_labels.csv \
	../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_community_district_corrections.csv \
	../input/zap_project_bbl.parquet \
	../input/mappluto_current_lot_lookup.parquet \
	../input/cd_homeownership_1990_measure.csv \
	../input/ulurp_cpc_training_labels_jacob.csv \
	../input/council_land_use_decision_panel.csv | ../output
	Rscript --vanilla $< $(BREAK_YEAR) $(TRANSITION_END_YEAR) $(START_YEAR) $(END_YEAR) $(VALIDATION_CASES_PER_PERIOD)

../output/ulurp_cb_opposition_approval.pdf: ../output/ulurp_cb_opposition_approval_summary.csv
	@test -s "$@"

../output/ulurp_cb_opposition_approval_cases.csv: ../output/ulurp_cb_opposition_approval_summary.csv
	@test -s "$@"

../output/ulurp_cb_opposition_transition.csv: ../output/ulurp_cb_opposition_approval_summary.csv
	@test -s "$@"

../output/ulurp_cb_opposition_transition.pdf: ../output/ulurp_cb_opposition_approval_summary.csv
	@test -s "$@"

../input/ulurp_cpc_text_labels.csv: ../../../summarize_text_cpc_trends/output/ulurp_cpc_text_labels.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_report_manifest.csv: ../../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_community_district_corrections.csv: ../../../record_ulurp_cpc_source_corrections/output/ulurp_cpc_community_district_corrections.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../../build_zap_datasets/output/zap_project_bbl.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/mappluto_current_lot_lookup.parquet: ../../../build_mappluto_current_lookup/output/mappluto_current_lot_lookup.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_homeownership_1990_measure.csv: ../../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_training_labels_jacob.csv: ../../../record_ulurp_cpc_training_labels/output/ulurp_cpc_training_labels_jacob.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_land_use_decision_panel.csv: ../../../build_council_land_use_decision_panel/output/council_land_use_decision_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_cpc_text_labels.csv \
	../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_community_district_corrections.csv \
	../input/zap_project_bbl.parquet \
	../input/mappluto_current_lot_lookup.parquet \
	../input/cd_homeownership_1990_measure.csv \
	../input/ulurp_cpc_training_labels_jacob.csv \
	../input/council_land_use_decision_panel.csv

include ../../../generic.make
