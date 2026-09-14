all: ../output/zap_project_universe.csv

../output/zap_project_universe.csv: build_zap_project_universe.py source_snapshot_sha256.json ../../_lib/data_reports.py ../input/zap_project_data.csv ../input/zap_project_metadata.json ../input/zap_project_count.json ../input/zap_bbl.csv ../input/zap_bbl_metadata.json ../input/zap_bbl_count.json ../input/zapprojects_datadictionary.xlsx | ../output ../report
	python3 $< > build_zap_project_universe.log 2>&1

../input/zap_project_data.csv: ../../../data_raw/dcp_zap_full_export/20260914/zap_project_data.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_metadata.json: ../../../data_raw/dcp_zap_full_export/20260914/zap_project_metadata.json | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_count.json: ../../../data_raw/dcp_zap_full_export/20260914/zap_project_count.json | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_bbl.csv: ../../../data_raw/dcp_zap_full_export/20260914/zap_bbl.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_bbl_metadata.json: ../../../data_raw/dcp_zap_full_export/20260914/zap_bbl_metadata.json | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_bbl_count.json: ../../../data_raw/dcp_zap_full_export/20260914/zap_bbl_count.json | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zapprojects_datadictionary.xlsx: ../../../data_raw/dcp_zap_full_export/20260914/zapprojects_datadictionary.xlsx | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_data.csv ../input/zap_project_metadata.json ../input/zap_project_count.json ../input/zap_bbl.csv ../input/zap_bbl_metadata.json ../input/zap_bbl_count.json ../input/zapprojects_datadictionary.xlsx

../report:
	mkdir -p $@

include sources.make
include ../../generic.make
