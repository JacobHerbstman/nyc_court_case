R := Rscript --vanilla

all: ../output/cd_homeownership_next_stage_memo.pdf

../input/cd_homeownership_long_units_anatomy_top_cd.csv: ../../summarize_cd_homeownership_long_units_anatomy/output/cd_homeownership_long_units_anatomy_top_cd.csv | ../input
	ln -sf "../../summarize_cd_homeownership_long_units_anatomy/output/cd_homeownership_long_units_anatomy_top_cd.csv" "$@"

../input/cd_homeownership_long_units_borough_era_shares.csv: ../../summarize_cd_homeownership_long_units_borough_details/output/cd_homeownership_long_units_borough_era_shares.csv | ../input
	ln -sf "../../summarize_cd_homeownership_long_units_borough_details/output/cd_homeownership_long_units_borough_era_shares.csv" "$@"

../input/cd_homeownership_long_units_borough_overlap_error.csv: ../../summarize_cd_homeownership_long_units_borough_details/output/cd_homeownership_long_units_borough_overlap_error.csv | ../input
	ln -sf "../../summarize_cd_homeownership_long_units_borough_details/output/cd_homeownership_long_units_borough_overlap_error.csv" "$@"

../input/cd_homeownership_long_units_exact_era_splice_total_units.csv: ../../summarize_cd_homeownership_long_units_borough_details/output/cd_homeownership_long_units_exact_era_splice_total_units.csv | ../input
	ln -sf "../../summarize_cd_homeownership_long_units_borough_details/output/cd_homeownership_long_units_exact_era_splice_total_units.csv" "$@"

../input/cd_homeownership_long_units_level_era.csv: ../../summarize_cd_homeownership_long_units_levels_developability/output/cd_homeownership_long_units_level_era.csv | ../input
	ln -sf "../../summarize_cd_homeownership_long_units_levels_developability/output/cd_homeownership_long_units_level_era.csv" "$@"

../input/cd_homeownership_long_units_levels_developability_qc.csv: ../../summarize_cd_homeownership_long_units_levels_developability/output/cd_homeownership_long_units_levels_developability_qc.csv | ../input
	ln -sf "../../summarize_cd_homeownership_long_units_levels_developability/output/cd_homeownership_long_units_levels_developability_qc.csv" "$@"

../input/cd_homeownership_total_units_tercile.pdf: ../../write_cd_homeownership_gpt_pro_packet_tex/output/cd_homeownership_total_units_tercile.pdf | ../input
	ln -sf "../../write_cd_homeownership_gpt_pro_packet_tex/output/cd_homeownership_total_units_tercile.pdf" "$@"

../input/cd_homeownership_units_50_plus_tercile.pdf: ../../write_cd_homeownership_gpt_pro_packet_tex/output/cd_homeownership_units_50_plus_tercile.pdf | ../input
	ln -sf "../../write_cd_homeownership_gpt_pro_packet_tex/output/cd_homeownership_units_50_plus_tercile.pdf" "$@"

../input/cd_homeownership_exact_decadal_validation_plot.pdf: ../../build_cd_homeownership_exact_decadal_validation/output/cd_homeownership_exact_decadal_validation_plot.pdf | ../input
	ln -sf "../../build_cd_homeownership_exact_decadal_validation/output/cd_homeownership_exact_decadal_validation_plot.pdf" "$@"

../input/cd_homeownership_long_units_borough_plots.pdf: ../../summarize_cd_homeownership_long_units_borough_details/output/cd_homeownership_long_units_borough_plots.pdf | ../input
	ln -sf "../../summarize_cd_homeownership_long_units_borough_details/output/cd_homeownership_long_units_borough_plots.pdf" "$@"

../input/cd_homeownership_long_units_levels_developability_plots.pdf: ../../summarize_cd_homeownership_long_units_levels_developability/output/cd_homeownership_long_units_levels_developability_plots.pdf | ../input
	ln -sf "../../summarize_cd_homeownership_long_units_levels_developability/output/cd_homeownership_long_units_levels_developability_plots.pdf" "$@"

../output/cd_homeownership_next_stage_memo.tex: write_cd_homeownership_next_stage_memo_tex.R ../input/cd_homeownership_long_units_anatomy_top_cd.csv ../input/cd_homeownership_long_units_borough_era_shares.csv ../input/cd_homeownership_long_units_borough_overlap_error.csv ../input/cd_homeownership_long_units_exact_era_splice_total_units.csv ../input/cd_homeownership_long_units_level_era.csv ../input/cd_homeownership_long_units_levels_developability_qc.csv | ../output
	$(R) $< ../input/cd_homeownership_long_units_anatomy_top_cd.csv ../input/cd_homeownership_long_units_borough_era_shares.csv ../input/cd_homeownership_long_units_borough_overlap_error.csv ../input/cd_homeownership_long_units_exact_era_splice_total_units.csv ../input/cd_homeownership_long_units_level_era.csv ../input/cd_homeownership_long_units_levels_developability_qc.csv ../output/cd_homeownership_next_stage_memo.tex

../output/cd_homeownership_next_stage_memo.pdf: ../output/cd_homeownership_next_stage_memo.tex ../input/cd_homeownership_total_units_tercile.pdf ../input/cd_homeownership_units_50_plus_tercile.pdf ../input/cd_homeownership_exact_decadal_validation_plot.pdf ../input/cd_homeownership_long_units_borough_plots.pdf ../input/cd_homeownership_long_units_levels_developability_plots.pdf
	cd ../output && pdflatex -interaction=nonstopmode -halt-on-error cd_homeownership_next_stage_memo.tex

../input ../output:
	mkdir -p $@

link-inputs: ../input/cd_homeownership_long_units_anatomy_top_cd.csv ../input/cd_homeownership_long_units_borough_era_shares.csv ../input/cd_homeownership_long_units_borough_overlap_error.csv ../input/cd_homeownership_long_units_exact_era_splice_total_units.csv ../input/cd_homeownership_long_units_level_era.csv ../input/cd_homeownership_long_units_levels_developability_qc.csv ../input/cd_homeownership_total_units_tercile.pdf ../input/cd_homeownership_units_50_plus_tercile.pdf ../input/cd_homeownership_exact_decadal_validation_plot.pdf ../input/cd_homeownership_long_units_borough_plots.pdf ../input/cd_homeownership_long_units_levels_developability_plots.pdf
