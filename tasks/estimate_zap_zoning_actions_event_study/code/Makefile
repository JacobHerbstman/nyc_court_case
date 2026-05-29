all: ../output/zap_zoning_actions_event_coefficients_5yr_bins.csv \
	../output/zap_zoning_actions_event_coefficients_5yr_bins.pdf \
	../output/zap_zoning_actions_event_design_panel.csv \
	../output/zap_zoning_actions_long_difference_estimates.csv \
	../output/zap_zoning_actions_long_difference.tex

../output/zap_zoning_actions_event_coefficients_5yr_bins.csv: estimate_zap_zoning_actions_event_study.R ../../_lib/source_pipeline_utils.R ../input/zap_zoning_map_special_permit_project_classification.csv ../input/zap_zoning_map_special_permit_project_ccd2010_fractional.csv ../input/ccdist2010_homeownership_1990_measure.csv | ../output
	Rscript --vanilla $<

../output/zap_zoning_actions_event_coefficients_5yr_bins.pdf: ../output/zap_zoning_actions_event_coefficients_5yr_bins.csv
	@test -s "$@"

../output/zap_zoning_actions_event_design_panel.csv: ../output/zap_zoning_actions_event_coefficients_5yr_bins.csv
	@test -s "$@"

../output/zap_zoning_actions_long_difference_estimates.csv: ../output/zap_zoning_actions_event_coefficients_5yr_bins.csv
	@test -s "$@"

../output/zap_zoning_actions_long_difference.tex: ../output/zap_zoning_actions_event_coefficients_5yr_bins.csv
	@test -s "$@"

../input/zap_zoning_map_special_permit_project_classification.csv: ../../summarize_zap_zoning_actions_by_decade/output/zap_zoning_map_special_permit_project_classification.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_zoning_map_special_permit_project_ccd2010_fractional.csv: ../../summarize_zap_zoning_actions_by_decade/output/zap_zoning_map_special_permit_project_ccd2010_fractional.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_homeownership_1990_measure.csv: ../../build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_zoning_map_special_permit_project_classification.csv ../input/zap_zoning_map_special_permit_project_ccd2010_fractional.csv ../input/ccdist2010_homeownership_1990_measure.csv

include ../../generic.make
