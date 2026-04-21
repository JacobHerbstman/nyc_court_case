R := Rscript --vanilla

all: ../output/cd_homeownership_mappluto_proxy_era_interactions.csv ../output/cd_homeownership_mappluto_proxy_model_summary.csv

../input/cd_homeownership_mappluto_proxy_panel.csv: ../../cd_homeownership_mappluto_proxy_panel/output/cd_homeownership_mappluto_proxy_panel.csv | ../input
	ln -sf "../../cd_homeownership_mappluto_proxy_panel/output/cd_homeownership_mappluto_proxy_panel.csv" "$@"

../output/cd_homeownership_mappluto_proxy_era_interactions.csv: estimate_cd_homeownership_mappluto_proxy.R ../input/cd_homeownership_mappluto_proxy_panel.csv | ../output
	$(R) $< ../input/cd_homeownership_mappluto_proxy_panel.csv ../output/cd_homeownership_mappluto_proxy_era_interactions.csv ../output/cd_homeownership_mappluto_proxy_model_summary.csv

../output/cd_homeownership_mappluto_proxy_model_summary.csv: ../output/cd_homeownership_mappluto_proxy_era_interactions.csv

link-inputs: ../input/cd_homeownership_mappluto_proxy_panel.csv
