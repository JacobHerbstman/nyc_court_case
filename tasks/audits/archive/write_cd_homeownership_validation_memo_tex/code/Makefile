R := Rscript --vanilla

all: ../output/cd_homeownership_validation_memo.pdf

../input/cd_homeownership_validation_decision_table.csv: ../../write_cd_homeownership_validation_memo/output/cd_homeownership_validation_decision_table.csv | ../input
	ln -sf "../../write_cd_homeownership_validation_memo/output/cd_homeownership_validation_decision_table.csv" "$@"

../input/cd_homeownership_exact_decadal_validation_comparison.csv: ../../build_cd_homeownership_exact_decadal_validation/output/cd_homeownership_exact_decadal_validation_comparison.csv | ../input
	ln -sf "../../build_cd_homeownership_exact_decadal_validation/output/cd_homeownership_exact_decadal_validation_comparison.csv" "$@"

../input/cd_homeownership_proxy_overlap_metrics.csv: ../../summarize_cd_homeownership_proxy_overlap_validation/output/cd_homeownership_proxy_overlap_metrics.csv | ../input
	ln -sf "../../summarize_cd_homeownership_proxy_overlap_validation/output/cd_homeownership_proxy_overlap_metrics.csv" "$@"

../input/cd_homeownership_long_units_sensitivity_summary.csv: ../../summarize_cd_homeownership_long_units_sensitivity/output/cd_homeownership_long_units_sensitivity_summary.csv | ../input
	ln -sf "../../summarize_cd_homeownership_long_units_sensitivity/output/cd_homeownership_long_units_sensitivity_summary.csv" "$@"

../input/cd_homeownership_exact_decadal_validation_plot.pdf: ../../build_cd_homeownership_exact_decadal_validation/output/cd_homeownership_exact_decadal_validation_plot.pdf | ../input
	ln -sf "../../build_cd_homeownership_exact_decadal_validation/output/cd_homeownership_exact_decadal_validation_plot.pdf" "$@"

../input/cd_homeownership_proxy_overlap_plots.pdf: ../../summarize_cd_homeownership_proxy_overlap_validation/output/cd_homeownership_proxy_overlap_plots.pdf | ../input
	ln -sf "../../summarize_cd_homeownership_proxy_overlap_validation/output/cd_homeownership_proxy_overlap_plots.pdf" "$@"

../input/cd_homeownership_long_units_sensitivity_plots.pdf: ../../summarize_cd_homeownership_long_units_sensitivity/output/cd_homeownership_long_units_sensitivity_plots.pdf | ../input
	ln -sf "../../summarize_cd_homeownership_long_units_sensitivity/output/cd_homeownership_long_units_sensitivity_plots.pdf" "$@"

../output/cd_homeownership_validation_memo.tex: write_cd_homeownership_validation_memo_tex.R ../input/cd_homeownership_validation_decision_table.csv ../input/cd_homeownership_exact_decadal_validation_comparison.csv ../input/cd_homeownership_proxy_overlap_metrics.csv ../input/cd_homeownership_long_units_sensitivity_summary.csv ../input/cd_homeownership_exact_decadal_validation_plot.pdf ../input/cd_homeownership_proxy_overlap_plots.pdf ../input/cd_homeownership_long_units_sensitivity_plots.pdf | ../output
	$(R) $< ../input/cd_homeownership_validation_decision_table.csv ../input/cd_homeownership_exact_decadal_validation_comparison.csv ../input/cd_homeownership_proxy_overlap_metrics.csv ../input/cd_homeownership_long_units_sensitivity_summary.csv ../output/cd_homeownership_validation_memo.tex

../output/cd_homeownership_validation_memo.pdf: ../output/cd_homeownership_validation_memo.tex ../input/cd_homeownership_exact_decadal_validation_plot.pdf ../input/cd_homeownership_proxy_overlap_plots.pdf ../input/cd_homeownership_long_units_sensitivity_plots.pdf
	cd ../output && pdflatex -interaction=nonstopmode -halt-on-error cd_homeownership_validation_memo.tex

../input ../output:
	mkdir -p $@

link-inputs: ../input/cd_homeownership_validation_decision_table.csv ../input/cd_homeownership_exact_decadal_validation_comparison.csv ../input/cd_homeownership_proxy_overlap_metrics.csv ../input/cd_homeownership_long_units_sensitivity_summary.csv ../input/cd_homeownership_exact_decadal_validation_plot.pdf ../input/cd_homeownership_proxy_overlap_plots.pdf ../input/cd_homeownership_long_units_sensitivity_plots.pdf
