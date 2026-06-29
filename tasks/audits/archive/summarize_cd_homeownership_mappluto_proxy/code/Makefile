R := Rscript --vanilla

all: ../output/cd_homeownership_mappluto_proxy_tercile_year.csv ../output/cd_homeownership_mappluto_proxy_tercile_era.csv ../output/cd_homeownership_mappluto_proxy_summary_qc.csv ../output/cd_homeownership_mappluto_proxy_plots.pdf

../input/cd_homeownership_mappluto_proxy_panel.csv: ../../cd_homeownership_mappluto_proxy_panel/output/cd_homeownership_mappluto_proxy_panel.csv | ../input
	ln -sf "../../cd_homeownership_mappluto_proxy_panel/output/cd_homeownership_mappluto_proxy_panel.csv" "$@"

../output/cd_homeownership_mappluto_proxy_tercile_year.csv: summarize_cd_homeownership_mappluto_proxy.R ../input/cd_homeownership_mappluto_proxy_panel.csv | ../output
	$(R) $< ../input/cd_homeownership_mappluto_proxy_panel.csv ../output/cd_homeownership_mappluto_proxy_tercile_year.csv ../output/cd_homeownership_mappluto_proxy_tercile_era.csv ../output/cd_homeownership_mappluto_proxy_summary_qc.csv ../output/cd_homeownership_mappluto_proxy_plots.pdf

../output/cd_homeownership_mappluto_proxy_tercile_era.csv: ../output/cd_homeownership_mappluto_proxy_tercile_year.csv

../output/cd_homeownership_mappluto_proxy_summary_qc.csv: ../output/cd_homeownership_mappluto_proxy_tercile_year.csv

../output/cd_homeownership_mappluto_proxy_plots.pdf: ../output/cd_homeownership_mappluto_proxy_tercile_year.csv

link-inputs: ../input/cd_homeownership_mappluto_proxy_panel.csv
