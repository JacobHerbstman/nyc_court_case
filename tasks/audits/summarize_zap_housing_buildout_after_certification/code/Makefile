all: ../output/zap_housing_buildout_project_audited.csv ../output/zap_housing_buildout_cohort_summary.csv ../output/zap_housing_buildout_delay_coefficients.csv ../output/zap_housing_buildout_plots.pdf

../output/zap_housing_buildout_project_audited.csv: summarize_zap_housing_buildout_after_certification.R ../../../_lib/source_pipeline_utils.R ../input/zap_housing_hdb_project_summary.csv ../input/zap_housing_hdb_link_candidates.csv | ../output
	Rscript --vanilla $<

../output/zap_housing_buildout_cohort_summary.csv ../output/zap_housing_buildout_delay_coefficients.csv ../output/zap_housing_buildout_plots.pdf: ../output/zap_housing_buildout_project_audited.csv
	@test -s "$@"

../input/zap_housing_hdb_project_summary.csv: ../../build_zap_housing_hdb_link/output/zap_housing_hdb_project_summary.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_housing_hdb_link_candidates.csv: ../../build_zap_housing_hdb_link/output/zap_housing_hdb_link_candidates.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_housing_hdb_project_summary.csv ../input/zap_housing_hdb_link_candidates.csv

include ../../generic.make
