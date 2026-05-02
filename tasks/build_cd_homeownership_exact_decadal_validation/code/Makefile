R := Rscript

all: ../output/cd_homeownership_exact_decadal_validation_cd.csv ../output/cd_homeownership_exact_decadal_validation_tercile.csv ../output/cd_homeownership_exact_decadal_validation_comparison.csv ../output/cd_homeownership_exact_decadal_validation_borough.csv ../output/cd_homeownership_exact_decadal_validation_qc.csv ../output/cd_homeownership_exact_decadal_validation_plot.pdf

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv" || ln -sf "../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv" "$@"

../input/dcp_cd_profiles_1990_2000_files.csv: ../../stage_dcp_cd_profiles_1990_2000/output/dcp_cd_profiles_1990_2000_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/mappluto_construction_proxy_cd_year.csv: ../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv | ../input
	@test "$$(readlink "$@")" = "../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv" || ln -sf "../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv" "$@"

../output/cd_homeownership_exact_decadal_validation_cd.csv: build_cd_homeownership_exact_decadal_validation.R ../input/cd_homeownership_1990_measure.csv ../input/dcp_cd_profiles_1990_2000_files.csv ../input/mappluto_construction_proxy_cd_year.csv | ../output
	$(R) $< ../input/cd_homeownership_1990_measure.csv ../input/dcp_cd_profiles_1990_2000_files.csv ../input/mappluto_construction_proxy_cd_year.csv ../output/cd_homeownership_exact_decadal_validation_cd.csv ../output/cd_homeownership_exact_decadal_validation_tercile.csv ../output/cd_homeownership_exact_decadal_validation_comparison.csv ../output/cd_homeownership_exact_decadal_validation_borough.csv ../output/cd_homeownership_exact_decadal_validation_qc.csv ../output/cd_homeownership_exact_decadal_validation_plot.pdf

../output/cd_homeownership_exact_decadal_validation_tercile.csv: ../output/cd_homeownership_exact_decadal_validation_cd.csv

../output/cd_homeownership_exact_decadal_validation_comparison.csv: ../output/cd_homeownership_exact_decadal_validation_cd.csv

../output/cd_homeownership_exact_decadal_validation_borough.csv: ../output/cd_homeownership_exact_decadal_validation_cd.csv

../output/cd_homeownership_exact_decadal_validation_qc.csv: ../output/cd_homeownership_exact_decadal_validation_cd.csv

../output/cd_homeownership_exact_decadal_validation_plot.pdf: ../output/cd_homeownership_exact_decadal_validation_cd.csv

link-inputs: ../input/cd_homeownership_1990_measure.csv ../input/dcp_cd_profiles_1990_2000_files.csv ../input/mappluto_construction_proxy_cd_year.csv

include ../../generic.make
