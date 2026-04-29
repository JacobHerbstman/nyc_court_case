SHELL := bash
include ../../shell_functions.make

all: ../output/zap_ulurp_redev_memo.pdf

../input/zap_ulurp_redev_project_base.csv: ../../build_zap_ulurp_redev_base/output/zap_ulurp_redev_project_base.csv | ../input
	ln -sf "$<" "$@"

../input/zap_ulurp_redev_project_base_qc.csv: ../../build_zap_ulurp_redev_base/output/zap_ulurp_redev_project_base_qc.csv | ../input
	ln -sf "$<" "$@"

../input/zap_ulurp_redev_2x2_era_summary.csv: ../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_2x2_era_summary.csv | ../input
	ln -sf "$<" "$@"

../input/zap_ulurp_redev_model_summary.csv: ../../estimate_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_model_summary.csv | ../input
	ln -sf "$<" "$@"

../input/zap_ulurp_redev_nested_diagnostics.csv: ../../estimate_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_nested_diagnostics.csv | ../input
	ln -sf "$<" "$@"

../input/zap_ulurp_redev_plots.pdf: ../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_plots.pdf | ../input
	ln -sf "$<" "$@"

../input/zap_ulurp_redev_coefficients.pdf: ../../estimate_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_coefficients.pdf | ../input
	ln -sf "$<" "$@"

../output/zap_ulurp_redev_memo.tex: write_zap_ulurp_redev_memo.R ../input/zap_ulurp_redev_project_base.csv ../input/zap_ulurp_redev_project_base_qc.csv ../input/zap_ulurp_redev_2x2_era_summary.csv ../input/zap_ulurp_redev_model_summary.csv ../input/zap_ulurp_redev_nested_diagnostics.csv | ../output
	$(R) $< ../input/zap_ulurp_redev_project_base.csv ../input/zap_ulurp_redev_project_base_qc.csv ../input/zap_ulurp_redev_2x2_era_summary.csv ../input/zap_ulurp_redev_model_summary.csv ../input/zap_ulurp_redev_nested_diagnostics.csv ../output/zap_ulurp_redev_memo.md ../output/zap_ulurp_redev_memo.tex

../output/zap_ulurp_redev_memo.pdf: ../output/zap_ulurp_redev_memo.tex ../input/zap_ulurp_redev_plots.pdf ../input/zap_ulurp_redev_coefficients.pdf
	cd ../output && pdflatex -interaction=nonstopmode -halt-on-error zap_ulurp_redev_memo.tex

link-inputs: ../input/zap_ulurp_redev_project_base.csv ../input/zap_ulurp_redev_project_base_qc.csv ../input/zap_ulurp_redev_2x2_era_summary.csv ../input/zap_ulurp_redev_model_summary.csv ../input/zap_ulurp_redev_nested_diagnostics.csv ../input/zap_ulurp_redev_plots.pdf ../input/zap_ulurp_redev_coefficients.pdf

include ../../generic.make
