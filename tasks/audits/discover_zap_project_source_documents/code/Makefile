all: ../output/zap_project_source_document_links.csv \
	../output/zap_project_source_project_summary.csv \
	../output/zap_project_source_docket_text.csv \
	../output/zap_project_source_fetch_failures.csv \
	../output/zap_project_source_discovery_qc.csv

../output/zap_project_source_document_links.csv: discover_zap_project_source_documents.py ../input/zap_rezoning_direction_text_candidate_queue.csv | ../output ../temp
	python3 $<

../output/zap_project_source_project_summary.csv \
../output/zap_project_source_docket_text.csv \
../output/zap_project_source_fetch_failures.csv \
../output/zap_project_source_discovery_qc.csv: ../output/zap_project_source_document_links.csv
	@test -s "$@"

../input/zap_rezoning_direction_text_candidate_queue.csv: ../../summarize_zap_rezoning_direction_scope/output/zap_rezoning_direction_text_candidate_queue.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_rezoning_direction_text_candidate_queue.csv

include ../../generic.make
