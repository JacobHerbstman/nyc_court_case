OCR_DPI = 150
OCR_PAGE_TIMEOUT_SECONDS = 60

all: ../output/flushing_commons_cpc_ocr_manifest.csv \
	../output/flushing_commons_C100206PPQ_ocr.txt \
	../output/flushing_commons_C100207ZMQ_ocr.txt \
	../output/flushing_commons_C100208ZSQ_ocr.txt \
	../output/flushing_commons_C100209ZSQ_ocr.txt \
	../output/flushing_commons_C100212ZSQ_ocr.txt

../output/flushing_commons_cpc_ocr_manifest.csv: ocr_ulurp_flushing_commons_cpc_reports.py \
	../input/100206_P2010Q0132_C100206PPQ_f1ec804c.pdf \
	../input/100207_P2010Q0132_C100207ZMQ_db53b2c6.pdf \
	../input/100208_P2010Q0132_C100208ZSQ_2776d16b.pdf \
	../input/100209_P2010Q0132_C100209ZSQ_c571d786.pdf \
	../input/100212_P2010Q0132_C100212ZSQ_cf25c4b5.pdf | ../output
	python3 $< $(OCR_DPI) $(OCR_PAGE_TIMEOUT_SECONDS)

../output/flushing_commons_C100206PPQ_ocr.txt \
../output/flushing_commons_C100207ZMQ_ocr.txt \
../output/flushing_commons_C100208ZSQ_ocr.txt \
../output/flushing_commons_C100209ZSQ_ocr.txt \
../output/flushing_commons_C100212ZSQ_ocr.txt: ../output/flushing_commons_cpc_ocr_manifest.csv
	@test -s "$@"

../input/100206_P2010Q0132_C100206PPQ_f1ec804c.pdf: ../../../build_ulurp_cpc_report_corpus/output/cpc_report_pdfs/100206_P2010Q0132_C100206PPQ_f1ec804c.pdf | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/100207_P2010Q0132_C100207ZMQ_db53b2c6.pdf: ../../../build_ulurp_cpc_report_corpus/output/cpc_report_pdfs/100207_P2010Q0132_C100207ZMQ_db53b2c6.pdf | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/100208_P2010Q0132_C100208ZSQ_2776d16b.pdf: ../../../build_ulurp_cpc_report_corpus/output/cpc_report_pdfs/100208_P2010Q0132_C100208ZSQ_2776d16b.pdf | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/100209_P2010Q0132_C100209ZSQ_c571d786.pdf: ../../../build_ulurp_cpc_report_corpus/output/cpc_report_pdfs/100209_P2010Q0132_C100209ZSQ_c571d786.pdf | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/100212_P2010Q0132_C100212ZSQ_cf25c4b5.pdf: ../../../build_ulurp_cpc_report_corpus/output/cpc_report_pdfs/100212_P2010Q0132_C100212ZSQ_cf25c4b5.pdf | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/100206_P2010Q0132_C100206PPQ_f1ec804c.pdf \
	../input/100207_P2010Q0132_C100207ZMQ_db53b2c6.pdf \
	../input/100208_P2010Q0132_C100208ZSQ_2776d16b.pdf \
	../input/100209_P2010Q0132_C100209ZSQ_c571d786.pdf \
	../input/100212_P2010Q0132_C100212ZSQ_cf25c4b5.pdf

include ../../generic.make
