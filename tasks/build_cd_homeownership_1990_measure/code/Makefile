all: ../output/cd_homeownership_1990_measure.csv \
	../output/cd_homeownership_1990_map.pdf

../%/cd_homeownership_1990_measure.csv ../%/cd_homeownership_1990_map.pdf: build_cd_homeownership_1990_measure.R Makefile ../../_lib/data_reports.R ../input/dcp_cd_profiles_1990_2000_20260501.parquet ../input/dcp_boundary_community_districts_20260501.parquet | ../output ../report
	Rscript --vanilla $<

../input/dcp_cd_profiles_1990_2000_20260501.parquet: ../../build_dcp_cd_profiles_1990_2000/output/dcp_cd_profiles_1990_2000_20260501.parquet | ../input
	ln -sf "$<" "$@"

../input/dcp_boundary_community_districts_20260501.parquet: ../../build_dcp_boundaries/output/dcp_boundary_community_districts_20260501.parquet | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/dcp_cd_profiles_1990_2000_20260501.parquet ../input/dcp_boundary_community_districts_20260501.parquet

include ../../generic.make
