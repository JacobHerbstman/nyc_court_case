all: ../output/citywide_ulurp_application_validation_year.csv

../output/citywide_ulurp_application_validation_year.csv: summarize_citywide_ulurp_application_validation.R \
	../input/zap_project_data.parquet | ../output
	Rscript $<

../input/zap_project_data.parquet: ../../../build_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_data.parquet

../input ../output:
	mkdir -p $@
