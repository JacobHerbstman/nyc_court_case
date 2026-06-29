R := Rscript

all: ../output/cd_homeownership_validation_decision_table.csv ../output/cd_homeownership_validation_memo.md

../input/cd_homeownership_exact_decadal_validation_tercile.csv: ../../build_cd_homeownership_exact_decadal_validation/output/cd_homeownership_exact_decadal_validation_tercile.csv | ../input
	ln -sf "../../build_cd_homeownership_exact_decadal_validation/output/cd_homeownership_exact_decadal_validation_tercile.csv" "$@"

../input/cd_homeownership_exact_decadal_validation_comparison.csv: ../../build_cd_homeownership_exact_decadal_validation/output/cd_homeownership_exact_decadal_validation_comparison.csv | ../input
	ln -sf "../../build_cd_homeownership_exact_decadal_validation/output/cd_homeownership_exact_decadal_validation_comparison.csv" "$@"

../input/cd_homeownership_proxy_overlap_metrics.csv: ../../summarize_cd_homeownership_proxy_overlap_validation/output/cd_homeownership_proxy_overlap_metrics.csv | ../input
	ln -sf "../../summarize_cd_homeownership_proxy_overlap_validation/output/cd_homeownership_proxy_overlap_metrics.csv" "$@"

../input/cd_homeownership_long_units_sensitivity_summary.csv: ../../summarize_cd_homeownership_long_units_sensitivity/output/cd_homeownership_long_units_sensitivity_summary.csv | ../input
	ln -sf "../../summarize_cd_homeownership_long_units_sensitivity/output/cd_homeownership_long_units_sensitivity_summary.csv" "$@"

../output/cd_homeownership_validation_decision_table.csv: write_cd_homeownership_validation_memo.R ../input/cd_homeownership_exact_decadal_validation_tercile.csv ../input/cd_homeownership_exact_decadal_validation_comparison.csv ../input/cd_homeownership_proxy_overlap_metrics.csv ../input/cd_homeownership_long_units_sensitivity_summary.csv | ../output
	$(R) $< ../input/cd_homeownership_exact_decadal_validation_tercile.csv ../input/cd_homeownership_exact_decadal_validation_comparison.csv ../input/cd_homeownership_proxy_overlap_metrics.csv ../input/cd_homeownership_long_units_sensitivity_summary.csv ../output/cd_homeownership_validation_decision_table.csv ../output/cd_homeownership_validation_memo.md

../output/cd_homeownership_validation_memo.md: ../output/cd_homeownership_validation_decision_table.csv

../input ../output:
	mkdir -p $@

link-inputs: ../input/cd_homeownership_exact_decadal_validation_tercile.csv ../input/cd_homeownership_exact_decadal_validation_comparison.csv ../input/cd_homeownership_proxy_overlap_metrics.csv ../input/cd_homeownership_long_units_sensitivity_summary.csv
