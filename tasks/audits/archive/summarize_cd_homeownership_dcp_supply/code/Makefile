SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_dcp_supply_tercile_year.csv ../output/cd_homeownership_dcp_supply_tercile_five_year.csv ../output/cd_homeownership_dcp_supply_proxy_split_five_year.csv ../output/cd_homeownership_dcp_supply_summary_qc.csv ../output/cd_homeownership_dcp_supply_plots.pdf

../input/cd_homeownership_dcp_supply_panel.csv: ../../cd_homeownership_dcp_supply_panel/output/cd_homeownership_dcp_supply_panel.csv | ../input
	ln -sf "$<" "$@"

../output/cd_homeownership_dcp_supply_tercile_year.csv: summarize_cd_homeownership_dcp_supply.R ../input/cd_homeownership_dcp_supply_panel.csv | ../output
	$(R) $< ../input/cd_homeownership_dcp_supply_panel.csv ../output/cd_homeownership_dcp_supply_tercile_year.csv ../output/cd_homeownership_dcp_supply_tercile_five_year.csv ../output/cd_homeownership_dcp_supply_proxy_split_five_year.csv ../output/cd_homeownership_dcp_supply_summary_qc.csv ../output/cd_homeownership_dcp_supply_plots.pdf

../output/cd_homeownership_dcp_supply_tercile_five_year.csv: ../output/cd_homeownership_dcp_supply_tercile_year.csv

../output/cd_homeownership_dcp_supply_proxy_split_five_year.csv: ../output/cd_homeownership_dcp_supply_tercile_year.csv

../output/cd_homeownership_dcp_supply_summary_qc.csv: ../output/cd_homeownership_dcp_supply_tercile_year.csv

../output/cd_homeownership_dcp_supply_plots.pdf: ../output/cd_homeownership_dcp_supply_tercile_year.csv

link-inputs: ../input/cd_homeownership_dcp_supply_panel.csv

include ../../generic.make
