WORKERS := 4

all: ../output/zap_missing_project_details.jsonl

../output/zap_missing_project_details.jsonl: fetch_zap_missing_project_details.py ../input/zap_project_universe.csv Makefile | ../output ../temp ../report ../../../data_raw/dcp_zap_project_details/20260914
	python3 $< $(WORKERS) > fetch_zap_missing_project_details.log 2>&1

../input/zap_project_universe.csv: ../../build_zap_project_universe/output/zap_project_universe.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_universe.csv

../report ../../../data_raw/dcp_zap_project_details/20260914:
	mkdir -p $@

include ../../generic.make
