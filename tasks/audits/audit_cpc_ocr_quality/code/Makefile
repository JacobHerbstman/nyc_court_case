SAMPLE_SEED := cpc-ocr-quality-20260927
OCR_DPI := 300
VISION_THRESHOLD := 0.80
WORKERS := 6

all: ../output/cpc_ocr_sample.csv ../output/cpc_ocr_page_texts.csv ../output/cpc_ocr_report_comparison.csv \
	../output/cpc_ocr_page_comparison.csv ../output/cpc_ocr_excerpts.md

../output/cpc_ocr_sample.csv: select_ocr_sample.py ../input/ulurp_cpc_statement_status_full_sol_high_20260927.csv \
	../input/ulurp_cpc_reading_roster.csv ../input/ulurp_cpc_human_coding.csv | ../output ../report
	python3 $< $(SAMPLE_SEED)

../output/cpc_ocr_page_texts.csv: reocr_sample_pages.py ocr_text_quality.py ../output/cpc_ocr_sample.csv \
	../input/ulurp_cpc_statement_status_full_sol_high_20260927.csv ../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_reading_segments.csv ../input/cpc_report_pdfs | ../output ../report ../temp
	python3 $< $(OCR_DPI) $(VISION_THRESHOLD) $(WORKERS)

# Vision transcriptions were acquired outside Make (see README); the folder is frozen.
../%/cpc_ocr_report_comparison.csv ../%/cpc_ocr_page_comparison.csv ../%/cpc_ocr_excerpts.md: compare_ocr_versions.py ocr_text_quality.py \
	../output/cpc_ocr_sample.csv ../output/cpc_ocr_page_texts.csv ../input/ulurp_cpc_human_coding.csv \
	../input/vision_transcriptions | ../output ../report
	python3 $<

../input/ulurp_cpc_statement_status_full_sol_high_20260927.csv: ../../../extract_ulurp_cpc_statements/output/ulurp_cpc_statement_status_full_sol_high_20260927.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_reading_roster.csv: ../../../build_ulurp_cpc_reading_text/output/ulurp_cpc_reading_roster.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_human_coding.csv: ../../../summarize_text_cpc_trends/output/ulurp_cpc_human_coding.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_report_manifest.csv: ../../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_reading_segments.csv: ../../../build_ulurp_cpc_reading_text/output/ulurp_cpc_reading_segments.csv | ../input
	ln -sf $< $@

../input/vision_transcriptions: ../../../../data_raw/cpc_ocr_quality_vision/claude_opus_5_5_20260927 | ../input
	ln -sfn $< $@

# The downloaded PDFs are frozen; link the folder rather than 11,208 files.
../input/cpc_report_pdfs: ../../../build_ulurp_cpc_report_corpus/output/cpc_report_pdfs | ../input
	ln -sfn $< $@

link-inputs: ../input/ulurp_cpc_statement_status_full_sol_high_20260927.csv ../input/ulurp_cpc_reading_roster.csv \
	../input/ulurp_cpc_human_coding.csv ../input/ulurp_cpc_report_manifest.csv ../input/ulurp_cpc_reading_segments.csv \
	../input/cpc_report_pdfs ../input/vision_transcriptions

include ../../generic.make
