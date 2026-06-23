SHELL := bash
include ../../shell_functions.make

all: ../output/citywide_ulurp_application_year.csv \
	../output/citywide_ulurp_application_trends.pdf

../output/citywide_ulurp_application_year.csv: summarize_citywide_ulurp_application_trends.R \
	../../_lib/source_pipeline_utils.R \
	../input/zap_project_data.parquet | ../output
	$(R) $<

../output/citywide_ulurp_application_trends.pdf: ../output/citywide_ulurp_application_year.csv
	@test -s "$@"

../input/zap_project_data.parquet: ../../stage_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_data.parquet

include ../../generic.make
