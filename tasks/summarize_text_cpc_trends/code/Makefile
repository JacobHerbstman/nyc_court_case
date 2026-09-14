START_YEAR := 1975
END_YEAR := 2025
BOILERPLATE_DOC_SHARE := 0.05
RULE_CONTEXT_WORDS := 135
MOVING_WINDOW_YEARS := 3
MINIMUM_DOCUMENTS_PER_MOVING_WINDOW := 20

all: ../output/ulurp_cpc_text_labels.csv \
	../output/ulurp_cpc_text_signal_trends.pdf \
	../output/ulurp_cpc_text_signal_homeowner_tercile_trends.pdf

../output/ulurp_cpc_text_signal_trends.pdf: plot_ulurp_cpc_text_signal_trends.R \
	../output/ulurp_cpc_text_labels.csv | ../output
	Rscript --vanilla $< $(START_YEAR) $(END_YEAR) $(MOVING_WINDOW_YEARS) $(MINIMUM_DOCUMENTS_PER_MOVING_WINDOW)

../output/ulurp_cpc_text_signal_homeowner_tercile_trends.pdf: plot_ulurp_cpc_text_signal_homeowner_terciles.R \
	../output/ulurp_cpc_text_labels.csv \
	../input/ulurp_cpc_community_district_corrections.csv \
	../input/zap_project_bbl.parquet \
	../input/mappluto_current_lot_lookup.parquet \
	../input/cd_homeownership_1990_measure.csv | ../output
	Rscript --vanilla $< $(START_YEAR) $(END_YEAR) $(MOVING_WINDOW_YEARS) $(MINIMUM_DOCUMENTS_PER_MOVING_WINDOW)

../output/ulurp_cpc_text_labels.csv: build_ulurp_cpc_text_labels.py Makefile \
	../../_lib/cpc_counts.py ../../_lib/data_reports.py \
	../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_narrative_boundary_exceptions.csv | ../output ../report
	python3 $< $(START_YEAR) $(END_YEAR) $(BOILERPLATE_DOC_SHARE) $(RULE_CONTEXT_WORDS)

../input/ulurp_cpc_report_manifest.csv: ../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	ln -sf "$<" "$@"

../input/ulurp_cpc_narrative_boundary_exceptions.csv: ../../record_ulurp_cpc_source_corrections/output/ulurp_cpc_narrative_boundary_exceptions.csv | ../input
	ln -sf "$<" "$@"

../input/ulurp_cpc_community_district_corrections.csv: ../../record_ulurp_cpc_source_corrections/output/ulurp_cpc_community_district_corrections.csv | ../input
	ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../build_zap_datasets/output/zap_project_bbl.parquet | ../input
	ln -sf "$<" "$@"

../input/mappluto_current_lot_lookup.parquet: ../../build_mappluto_current_lookup/output/mappluto_current_lot_lookup.parquet | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_narrative_boundary_exceptions.csv \
	../input/ulurp_cpc_community_district_corrections.csv \
	../input/zap_project_bbl.parquet \
	../input/mappluto_current_lot_lookup.parquet \
	../input/cd_homeownership_1990_measure.csv

include ../../generic.make
