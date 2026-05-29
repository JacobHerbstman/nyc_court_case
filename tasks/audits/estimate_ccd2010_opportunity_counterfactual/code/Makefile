all: ../output/ccdist2010_opportunity_counterfactual_panel.csv \
	../output/ccdist2010_opportunity_counterfactual_summary.csv \
	../output/ccdist2010_opportunity_counterfactual_model_summary.csv \
	../output/ccdist2010_opportunity_counterfactual_main_treatment_coefficients.csv \
	../output/ccdist2010_opportunity_counterfactual_two_by_two.csv

../output/ccdist2010_opportunity_counterfactual_panel.csv: estimate_ccd2010_opportunity_counterfactual.R ../../../_lib/source_pipeline_utils.R ../input/ccdist2010_redevelopment_potential.csv ../input/ccdist2010_homeownership_long_units_series.csv | ../output
	Rscript --vanilla $<

../output/ccdist2010_opportunity_counterfactual_summary.csv: ../output/ccdist2010_opportunity_counterfactual_panel.csv
	@test -s "$@"

../output/ccdist2010_opportunity_counterfactual_model_summary.csv: ../output/ccdist2010_opportunity_counterfactual_panel.csv
	@test -s "$@"

../output/ccdist2010_opportunity_counterfactual_main_treatment_coefficients.csv: ../output/ccdist2010_opportunity_counterfactual_panel.csv
	@test -s "$@"

../output/ccdist2010_opportunity_counterfactual_two_by_two.csv: ../output/ccdist2010_opportunity_counterfactual_panel.csv
	@test -s "$@"

../input/ccdist2010_redevelopment_potential.csv: ../../../build_ccd2010_redevelopment_potential/output/ccdist2010_redevelopment_potential.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_homeownership_long_units_series.csv: ../../../build_ccd2010_homeownership_long_units_series/output/ccdist2010_homeownership_long_units_series.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ccdist2010_redevelopment_potential.csv ../input/ccdist2010_homeownership_long_units_series.csv

include ../../generic.make
