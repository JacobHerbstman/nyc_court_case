START_YEAR := 1975
END_YEAR := 2025
BOILERPLATE_DOC_SHARE := 0.05
RULE_CONTEXT_WORDS := 135
MOVING_WINDOW_YEARS := 3
MINIMUM_DOCUMENTS_PER_MOVING_WINDOW := 20

all: ../output/ulurp_cpc_text_labels.csv \
	../output/ulurp_cpc_text_signal_trends.pdf \
	../output/ulurp_cpc_text_signal_homeowner_tercile_trends.pdf

../output/ulurp_cpc_text_labels.csv: build_ulurp_cpc_text_labels.py \
	ulurp_cpc_text_label_rules.py \
	../temp/ulurp_cpc_narrative_manifest.csv | ../output
	python3 $< $(START_YEAR) $(END_YEAR) $(BOILERPLATE_DOC_SHARE) $(RULE_CONTEXT_WORDS)

../output/ulurp_cpc_text_signal_trends.pdf: plot_ulurp_cpc_text_signal_trends.R \
	../output/ulurp_cpc_text_labels.csv | ../output
	Rscript --vanilla $< $(START_YEAR) $(END_YEAR) $(MOVING_WINDOW_YEARS) $(MINIMUM_DOCUMENTS_PER_MOVING_WINDOW)

../output/ulurp_cpc_text_signal_homeowner_tercile_trends.pdf: plot_ulurp_cpc_text_signal_homeowner_terciles.R \
	../output/ulurp_cpc_text_labels.csv \
	../input/ulurp_cpc_community_district_corrections.csv \
	../input/zap_project_bbl.parquet \
	../input/ccdist2010_mappluto_bbl_lookup.parquet \
	../input/dcp_mappluto_current_25v4.parquet \
	../input/ccdist2010_homeownership_1990_measure.csv | ../output
	Rscript --vanilla $< $(START_YEAR) $(END_YEAR) $(MOVING_WINDOW_YEARS) $(MINIMUM_DOCUMENTS_PER_MOVING_WINDOW)

../temp/ulurp_cpc_narrative_manifest.csv: build_ulurp_cpc_narratives.py \
	../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_narrative_boundary_decisions.csv | ../temp
	python3 $< $(START_YEAR) $(END_YEAR)

../input/ulurp_cpc_report_manifest.csv: ../../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_narrative_boundary_decisions.csv: ../../record_ulurp_cpc_source_validation/output/official_ulurp_cpc_narrative_boundary_exceptions.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_community_district_corrections.csv: ../../record_ulurp_cpc_text_analysis_decisions/output/ulurp_cpc_community_district_corrections.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../../build_zap_datasets/output/zap_project_bbl.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_mappluto_bbl_lookup.parquet: ../../build_ccd2010_mappluto_bbl_lookup/output/ccdist2010_mappluto_bbl_lookup.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_mappluto_current_25v4.parquet: ../../build_mappluto_lots/output/dcp_mappluto_current_25v4.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_homeownership_1990_measure.csv: ../../../build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_narrative_boundary_decisions.csv \
	../input/ulurp_cpc_community_district_corrections.csv \
	../input/zap_project_bbl.parquet \
	../input/ccdist2010_mappluto_bbl_lookup.parquet \
	../input/dcp_mappluto_current_25v4.parquet \
	../input/ccdist2010_homeownership_1990_measure.csv

include ../../generic.make
