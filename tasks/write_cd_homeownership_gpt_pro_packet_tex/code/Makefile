R := Rscript --vanilla

all: ../output/cd_homeownership_gpt_pro_packet.pdf

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv" "$@"

../input/cd_homeownership_1990_measure_comparison_summary.csv: ../../compare_cd_homeownership_1990_measures/output/cd_homeownership_1990_measure_comparison_summary.csv | ../input
	ln -sf "../../compare_cd_homeownership_1990_measures/output/cd_homeownership_1990_measure_comparison_summary.csv" "$@"

../input/mappluto_construction_proxy_validation_summary.csv: ../../summarize_mappluto_construction_proxy/output/mappluto_construction_proxy_validation_summary.csv | ../input
	ln -sf "../../summarize_mappluto_construction_proxy/output/mappluto_construction_proxy_validation_summary.csv" "$@"

../input/mappluto_construction_proxy_vs_decadal.csv: ../../summarize_mappluto_construction_proxy/output/mappluto_construction_proxy_vs_decadal.csv | ../input
	ln -sf "../../summarize_mappluto_construction_proxy/output/mappluto_construction_proxy_vs_decadal.csv" "$@"

../input/cd_homeownership_long_units_city_year.csv: ../../summarize_cd_homeownership_long_units_series/output/cd_homeownership_long_units_city_year.csv | ../input
	ln -sf "../../summarize_cd_homeownership_long_units_series/output/cd_homeownership_long_units_city_year.csv" "$@"

../input/cd_homeownership_long_units_tercile_year.csv: ../../summarize_cd_homeownership_long_units_series/output/cd_homeownership_long_units_tercile_year.csv | ../input
	ln -sf "../../summarize_cd_homeownership_long_units_series/output/cd_homeownership_long_units_tercile_year.csv" "$@"

../input/cd_homeownership_long_units_tercile_era.csv: ../../summarize_cd_homeownership_long_units_series/output/cd_homeownership_long_units_tercile_era.csv | ../input
	ln -sf "../../summarize_cd_homeownership_long_units_series/output/cd_homeownership_long_units_tercile_era.csv" "$@"

../output/cd_homeownership_gpt_pro_packet.tex: write_cd_homeownership_gpt_pro_packet_tex.R ../input/cd_homeownership_1990_measure.csv ../input/cd_homeownership_1990_measure_comparison_summary.csv ../input/mappluto_construction_proxy_validation_summary.csv ../input/mappluto_construction_proxy_vs_decadal.csv ../input/cd_homeownership_long_units_city_year.csv ../input/cd_homeownership_long_units_tercile_year.csv ../input/cd_homeownership_long_units_tercile_era.csv | ../output
	$(R) $< ../input/cd_homeownership_1990_measure.csv ../input/cd_homeownership_1990_measure_comparison_summary.csv ../input/mappluto_construction_proxy_validation_summary.csv ../input/mappluto_construction_proxy_vs_decadal.csv ../input/cd_homeownership_long_units_city_year.csv ../input/cd_homeownership_long_units_tercile_year.csv ../input/cd_homeownership_long_units_tercile_era.csv ../output/cd_homeownership_gpt_pro_packet.tex ../output/cd_homeownership_total_units_tercile.pdf ../output/cd_homeownership_units_50_plus_tercile.pdf ../output/cd_homeownership_long_city_totals.pdf ../output/cd_homeownership_proxy_validation_scatter_1990s.pdf

../output/cd_homeownership_total_units_tercile.pdf: ../output/cd_homeownership_gpt_pro_packet.tex

../output/cd_homeownership_units_50_plus_tercile.pdf: ../output/cd_homeownership_gpt_pro_packet.tex

../output/cd_homeownership_long_city_totals.pdf: ../output/cd_homeownership_gpt_pro_packet.tex

../output/cd_homeownership_proxy_validation_scatter_1990s.pdf: ../output/cd_homeownership_gpt_pro_packet.tex

../output/cd_homeownership_gpt_pro_packet.pdf: ../output/cd_homeownership_gpt_pro_packet.tex ../output/cd_homeownership_total_units_tercile.pdf ../output/cd_homeownership_units_50_plus_tercile.pdf ../output/cd_homeownership_long_city_totals.pdf ../output/cd_homeownership_proxy_validation_scatter_1990s.pdf
	cd ../output && pdflatex -interaction=nonstopmode -halt-on-error cd_homeownership_gpt_pro_packet.tex

link-inputs: ../input/cd_homeownership_1990_measure.csv ../input/cd_homeownership_1990_measure_comparison_summary.csv ../input/mappluto_construction_proxy_validation_summary.csv ../input/mappluto_construction_proxy_vs_decadal.csv ../input/cd_homeownership_long_units_city_year.csv ../input/cd_homeownership_long_units_tercile_year.csv ../input/cd_homeownership_long_units_tercile_era.csv
