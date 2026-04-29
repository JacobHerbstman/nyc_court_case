SHELL := bash
include ../../shell_functions.make

all: ../output/brooklyn_homeownership_case_study_memo.pdf

../input/brooklyn_homeownership_case_study_cd_summary.csv: ../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_cd_summary.csv | ../input
	ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_era_outcomes.csv: ../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_era_outcomes.csv | ../input
	ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_block_regressions.csv: ../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_block_regressions.csv | ../input
	ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_block_diagnostics.csv: ../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_block_diagnostics.csv | ../input
	ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_leave_one_cd_out.csv: ../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_leave_one_cd_out.csv | ../input
	ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_size_bin_summary.csv: ../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_size_bin_summary.csv | ../input
	ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_zap_summary.csv: ../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_zap_summary.csv | ../input
	ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_zap_block_regressions.csv: ../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_zap_block_regressions.csv | ../input
	ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_qc.csv: ../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_qc.csv | ../input
	ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_plots.pdf: ../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_plots.pdf | ../input
	ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_control_flip_plots.pdf: ../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_control_flip_plots.pdf | ../input
	ln -sf "$<" "$@"

../output/brooklyn_homeownership_case_study_memo.tex: write_brooklyn_homeownership_case_study_memo.R ../input/brooklyn_homeownership_case_study_cd_summary.csv ../input/brooklyn_homeownership_case_study_era_outcomes.csv ../input/brooklyn_homeownership_case_study_block_regressions.csv ../input/brooklyn_homeownership_case_study_block_diagnostics.csv ../input/brooklyn_homeownership_case_study_leave_one_cd_out.csv ../input/brooklyn_homeownership_case_study_size_bin_summary.csv ../input/brooklyn_homeownership_case_study_zap_summary.csv ../input/brooklyn_homeownership_case_study_zap_block_regressions.csv ../input/brooklyn_homeownership_case_study_qc.csv | ../output
	$(R) $< ../input/brooklyn_homeownership_case_study_cd_summary.csv ../input/brooklyn_homeownership_case_study_era_outcomes.csv ../input/brooklyn_homeownership_case_study_block_regressions.csv ../input/brooklyn_homeownership_case_study_block_diagnostics.csv ../input/brooklyn_homeownership_case_study_leave_one_cd_out.csv ../input/brooklyn_homeownership_case_study_size_bin_summary.csv ../input/brooklyn_homeownership_case_study_zap_summary.csv ../input/brooklyn_homeownership_case_study_zap_block_regressions.csv ../input/brooklyn_homeownership_case_study_qc.csv ../output/brooklyn_homeownership_case_study_memo.md ../output/brooklyn_homeownership_case_study_memo.tex

../output/brooklyn_homeownership_case_study_memo.pdf: ../output/brooklyn_homeownership_case_study_memo.tex ../input/brooklyn_homeownership_case_study_plots.pdf ../input/brooklyn_homeownership_case_study_control_flip_plots.pdf
	cd ../output && pdflatex -interaction=nonstopmode -halt-on-error brooklyn_homeownership_case_study_memo.tex

link-inputs: ../input/brooklyn_homeownership_case_study_cd_summary.csv ../input/brooklyn_homeownership_case_study_era_outcomes.csv ../input/brooklyn_homeownership_case_study_block_regressions.csv ../input/brooklyn_homeownership_case_study_block_diagnostics.csv ../input/brooklyn_homeownership_case_study_leave_one_cd_out.csv ../input/brooklyn_homeownership_case_study_size_bin_summary.csv ../input/brooklyn_homeownership_case_study_zap_summary.csv ../input/brooklyn_homeownership_case_study_zap_block_regressions.csv ../input/brooklyn_homeownership_case_study_qc.csv ../input/brooklyn_homeownership_case_study_plots.pdf ../input/brooklyn_homeownership_case_study_control_flip_plots.pdf

include ../../generic.make
