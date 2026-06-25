all: ../output/zap_rezoning_source_document_index.csv \
	../output/zap_rezoning_source_text_snippets.csv \
	../output/zap_rezoning_chatgpt_review_packet.csv \
	../output/zap_rezoning_chatgpt_review_batch_001.md \
	../output/zap_rezoning_chatgpt_review_batch_manifest.csv \
	../output/zap_rezoning_provisional_best_direction_project_labels.csv \
	../output/zap_rezoning_provisional_best_direction_city_year.csv \
	../output/zap_rezoning_provisional_best_direction_period.csv \
	../output/zap_rezoning_provisional_best_direction_tercile_year.csv \
	../output/zap_rezoning_provisional_up_down_scope_summary.csv \
	../output/zap_rezoning_provisional_up_down_scope_by_tercile.csv \
	../output/zap_rezoning_provisional_up_down_top_scope_projects.csv \
	../output/zap_rezoning_provisional_best_direction_city_trends.pdf \
	../output/zap_rezoning_provisional_up_down_tercile_trends.pdf \
	../output/zap_rezoning_provisional_up_down_scope_by_tercile.pdf \
	../output/zap_rezoning_provisional_best_direction_qc.csv \
	../output/zap_rezoning_source_text_qc.csv

../output/zap_rezoning_chatgpt_review_packet.csv: extract_zap_rezoning_source_text.py ../input/zap_project_source_document_links.csv ../input/zap_project_source_docket_text.csv ../input/zap_rezoning_direction_text_candidate_queue.csv | ../output ../temp/source_pdfs ../temp/source_text
	python3 $<

../output/zap_rezoning_source_document_index.csv \
../output/zap_rezoning_source_text_snippets.csv \
../output/zap_rezoning_chatgpt_review_batch_001.md \
../output/zap_rezoning_source_text_qc.csv: ../output/zap_rezoning_chatgpt_review_packet.csv
	@test -s "$@"

../output/zap_rezoning_chatgpt_review_batch_manifest.csv: build_chatgpt_review_batches.py ../output/zap_rezoning_chatgpt_review_packet.csv | ../output/chatgpt_batches
	python3 $<

../output/zap_rezoning_chatgpt_second_review_batch_manifest.csv: build_chatgpt_second_review_batches.py ../output/zap_rezoning_chatgpt_manual_review_queue.csv | ../output/chatgpt_second_review_batches
	python3 $<

../output/zap_rezoning_provisional_best_direction_project_labels.csv: plot_provisional_zap_rezoning_direction_trends.R ../input/zap_rezoning_direction_project_classification.csv ../input/zap_project_bbl.parquet ../input/ccdist2010_mappluto_bbl_lookup.parquet ../input/dcp_mappluto_current_25v4.parquet ../input/ccdist2010_homeownership_1990_measure.csv ../output/zap_rezoning_chatgpt_manual_review_queue.csv ../output/zap_rezoning_chatgpt_second_manual_review_queue.csv zap_rezoning_human_manual_review_verdicts.csv | ../output
	Rscript --vanilla $<

../output/zap_rezoning_provisional_best_direction_city_year.csv \
../output/zap_rezoning_provisional_best_direction_period.csv \
../output/zap_rezoning_provisional_best_direction_tercile_year.csv \
../output/zap_rezoning_provisional_up_down_scope_summary.csv \
../output/zap_rezoning_provisional_up_down_scope_by_tercile.csv \
../output/zap_rezoning_provisional_up_down_top_scope_projects.csv \
../output/zap_rezoning_provisional_best_direction_city_trends.pdf \
../output/zap_rezoning_provisional_up_down_tercile_trends.pdf \
../output/zap_rezoning_provisional_up_down_scope_by_tercile.pdf \
../output/zap_rezoning_provisional_best_direction_qc.csv: ../output/zap_rezoning_provisional_best_direction_project_labels.csv
	@test -s "$@"

../input/zap_project_source_document_links.csv: ../../discover_zap_project_source_documents/output/zap_project_source_document_links.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_source_docket_text.csv: ../../discover_zap_project_source_documents/output/zap_project_source_docket_text.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_rezoning_direction_text_candidate_queue.csv: ../../summarize_zap_rezoning_direction_scope/output/zap_rezoning_direction_text_candidate_queue.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_rezoning_direction_project_classification.csv: ../../summarize_zap_rezoning_direction_scope/output/zap_rezoning_direction_project_classification.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../../build_zap_datasets/output/zap_project_bbl.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_mappluto_bbl_lookup.parquet: ../../../build_ccd2010_mappluto_construction_proxy/output/ccdist2010_mappluto_bbl_lookup.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_mappluto_current_25v4.parquet: ../../../build_mappluto_lots/output/dcp_mappluto_current_25v4.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_homeownership_1990_measure.csv: ../../../build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../output/chatgpt_batches ../output/chatgpt_second_review_batches ../temp/source_pdfs ../temp/source_text:
	mkdir -p $@

link-inputs: ../input/zap_project_source_document_links.csv ../input/zap_project_source_docket_text.csv ../input/zap_rezoning_direction_text_candidate_queue.csv ../input/zap_rezoning_direction_project_classification.csv ../input/zap_project_bbl.parquet ../input/ccdist2010_mappluto_bbl_lookup.parquet ../input/dcp_mappluto_current_25v4.parquet ../input/ccdist2010_homeownership_1990_measure.csv

include ../../generic.make
