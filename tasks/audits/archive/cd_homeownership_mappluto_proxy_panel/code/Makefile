R := Rscript --vanilla

all: ../output/cd_homeownership_mappluto_proxy_panel.csv ../output/cd_homeownership_mappluto_proxy_panel_qc.csv

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv" "$@"

../input/cd_baseline_1990_controls.csv: ../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	ln -sf "../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv" "$@"

../input/mappluto_construction_proxy_cd_year.csv: ../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv | ../input
	ln -sf "../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv" "$@"

../output/cd_homeownership_mappluto_proxy_panel.csv: build_cd_homeownership_mappluto_proxy_panel.R ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/mappluto_construction_proxy_cd_year.csv | ../output
	$(R) $< ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/mappluto_construction_proxy_cd_year.csv ../output/cd_homeownership_mappluto_proxy_panel.csv ../output/cd_homeownership_mappluto_proxy_panel_qc.csv

../output/cd_homeownership_mappluto_proxy_panel_qc.csv: ../output/cd_homeownership_mappluto_proxy_panel.csv

link-inputs: ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/mappluto_construction_proxy_cd_year.csv
