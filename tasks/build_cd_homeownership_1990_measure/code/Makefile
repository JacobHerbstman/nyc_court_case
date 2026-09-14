all: ../output/cd_homeownership_1990_measure.csv \
	../output/cd_homeownership_1990_map.pdf

../output/cd_homeownership_1990_measure.csv: build_cd_homeownership_1990_measure.R ../../_lib/source_pipeline_utils.R ../input/dcp_cd_profiles_1990_2000_files.csv ../input/dcp_boundary_community_districts_20260501.parquet | ../output
	Rscript --vanilla $<

../output/cd_homeownership_1990_map.pdf: ../output/cd_homeownership_1990_measure.csv
	@test -s "$@"

../input/dcp_cd_profiles_1990_2000_files.csv: ../../build_dcp_cd_profiles_1990_2000/output/dcp_cd_profiles_1990_2000_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_boundary_community_districts_20260501.parquet: ../../build_dcp_boundaries/output/dcp_boundary_community_districts_20260501.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/dcp_cd_profiles_1990_2000_files.csv ../input/dcp_boundary_community_districts_20260501.parquet

include ../../generic.make
