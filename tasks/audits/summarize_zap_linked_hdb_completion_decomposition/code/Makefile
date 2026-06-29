all: ../output/zap_linked_hdb_50plus_cd_year.csv ../output/zap_linked_hdb_50plus_tercile_trends.pdf ../output/zap_linked_hdb_50plus_coefficients.csv ../output/zap_linked_hdb_50plus_summary.csv

../output/zap_linked_hdb_50plus_cd_year.csv: summarize_zap_linked_hdb_completion_decomposition.R ../input/dcp_housing_database_project_level_25q4.parquet ../input/zap_housing_hdb_link_candidates.csv ../input/cd_redevelopment_potential_baseline.csv | ../output
	Rscript --vanilla $<

../output/zap_linked_hdb_50plus_tercile_trends.pdf: ../output/zap_linked_hdb_50plus_cd_year.csv
	@test -s "$@"

../output/zap_linked_hdb_50plus_coefficients.csv: ../output/zap_linked_hdb_50plus_cd_year.csv
	@test -s "$@"

../output/zap_linked_hdb_50plus_summary.csv: ../output/zap_linked_hdb_50plus_cd_year.csv
	@test -s "$@"

../input/dcp_housing_database_project_level_25q4.parquet: ../../../build_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_housing_hdb_link_candidates.csv: ../../build_zap_housing_hdb_link/output/zap_housing_hdb_link_candidates.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_redevelopment_potential_baseline.csv: ../../redevelopment_potential_first_pass/output/cd_redevelopment_potential_baseline.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/dcp_housing_database_project_level_25q4.parquet ../input/zap_housing_hdb_link_candidates.csv ../input/cd_redevelopment_potential_baseline.csv

include ../../generic.make
