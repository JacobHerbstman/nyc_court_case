all: ../output/brooklyn_homeownership_case_study_memo.pdf

../output/brooklyn_homeownership_case_study_memo.pdf: ../output/brooklyn_homeownership_case_study_memo.tex ../input/brooklyn_homeownership_case_study_plots.pdf ../input/brooklyn_homeownership_case_study_control_flip_plots.pdf
	cd ../output && pdflatex -interaction=nonstopmode -halt-on-error brooklyn_homeownership_case_study_memo.tex

../output/brooklyn_homeownership_case_study_memo.tex: write_brooklyn_homeownership_case_study_memo.R ../../../_lib/source_pipeline_utils.R ../input/brooklyn_homeownership_case_study_cd_summary.csv ../input/brooklyn_homeownership_case_study_era_outcomes.csv ../input/brooklyn_homeownership_case_study_block_regressions.csv ../input/brooklyn_homeownership_case_study_block_diagnostics.csv ../input/brooklyn_homeownership_case_study_leave_one_cd_out.csv ../input/brooklyn_homeownership_case_study_size_bin_summary.csv ../input/brooklyn_homeownership_case_study_zap_summary.csv ../input/brooklyn_homeownership_case_study_zap_block_regressions.csv ../input/brooklyn_homeownership_case_study_qc.csv | ../output
	Rscript --vanilla $<

../input/brooklyn_homeownership_case_study_cd_summary.csv: ../../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_cd_summary.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_era_outcomes.csv: ../../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_era_outcomes.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_block_regressions.csv: ../../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_block_regressions.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_block_diagnostics.csv: ../../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_block_diagnostics.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_leave_one_cd_out.csv: ../../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_leave_one_cd_out.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_size_bin_summary.csv: ../../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_size_bin_summary.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_zap_summary.csv: ../../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_zap_summary.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_zap_block_regressions.csv: ../../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_zap_block_regressions.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_qc.csv: ../../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_qc.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_plots.pdf: ../../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_plots.pdf | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/brooklyn_homeownership_case_study_control_flip_plots.pdf: ../../../summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_control_flip_plots.pdf | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/brooklyn_homeownership_case_study_cd_summary.csv ../input/brooklyn_homeownership_case_study_era_outcomes.csv ../input/brooklyn_homeownership_case_study_block_regressions.csv ../input/brooklyn_homeownership_case_study_block_diagnostics.csv ../input/brooklyn_homeownership_case_study_leave_one_cd_out.csv ../input/brooklyn_homeownership_case_study_size_bin_summary.csv ../input/brooklyn_homeownership_case_study_zap_summary.csv ../input/brooklyn_homeownership_case_study_zap_block_regressions.csv ../input/brooklyn_homeownership_case_study_qc.csv ../input/brooklyn_homeownership_case_study_plots.pdf ../input/brooklyn_homeownership_case_study_control_flip_plots.pdf

include ../../generic.make
