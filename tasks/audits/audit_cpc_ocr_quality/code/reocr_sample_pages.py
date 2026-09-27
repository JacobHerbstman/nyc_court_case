"""Re-OCR every page the reader received for the sampled reports.

For each page: (a) the production reading text, and (b) Tesseract at OCR_DPI with
--psm 3 on every page, including pages that already have an embedded text layer.
Page images are kept in temp/ for the vision transcription step. Pages go to that
step when their (a) text looks garbled (dictionary share below VISION_THRESHOLD),
or when the reader's notes name the page's segment in a sentence about garbled or
illegible text. Only (a) and the notes decide this, never (b).
"""
# Interactive use: cd to tasks/audits/audit_cpc_ocr_quality/code, then
# python3 reocr_sample_pages.py 300 0.80 6
import csv
import re
import subprocess
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, '../../../_lib')
from data_reports import save_csv
from ocr_text_quality import dictionary_share

ocr_dpi, vision_threshold, workers = int(sys.argv[1]), float(sys.argv[2]), int(sys.argv[3])
FLAG = re.compile(r'garbl|illegib|unreadable|\bOCR\b', re.I)
csv.field_size_limit(10**9)

with open('../output/cpc_ocr_sample.csv') as f:
    sample = {r['document_id']: r for r in csv.DictReader(f)}
with open('../input/ulurp_cpc_statement_status_full_sol_high_20260927.csv') as f:
    notes = {r['document_id']: r['reading_notes'] for r in csv.DictReader(f) if r['document_id'] in sample}
with open('../input/ulurp_cpc_report_manifest.csv') as f:
    pdf_name = {r['document_id']: Path(r['local_pdf_path']).name for r in csv.DictReader(f)}

# Production text per page: the reading segments for that PDF page, in order.
pages = defaultdict(list)
with open('../input/ulurp_cpc_reading_segments.csv') as f:
    for r in csv.DictReader(f):
        if r['document_id'] in sample:
            pages[r['document_id'], r['source_document_id'], int(r['pdf_page'])].append(r)
assert {doc for doc, _, _ in pages} == set(sample)

# Segments the notes call garbled or illegible, including ranges such as s0015–s0016.
noted = defaultdict(set)
for doc, text in notes.items():
    for sentence in re.split(r'(?<=[.;])\s+', text):
        if FLAG.search(sentence):
            for a, b in re.findall(r's(\d{4})(?:\s*[–-]\s*s(\d{4}))?', sentence):
                noted[doc].update(f's{i:04d}' for i in range(int(a), int(b or a) + 1))

images = Path('../temp/page_images')
images.mkdir(parents=True, exist_ok=True)


def reocr(key):
    doc, source_doc, page = key
    stem = images / f'{source_doc}_p{page:03d}'
    started = time.time()
    subprocess.run(['pdftoppm', '-f', str(page), '-l', str(page), '-r', str(ocr_dpi), '-png', '-singlefile',
                    f'../input/cpc_report_pdfs/{pdf_name[source_doc]}', str(stem)], check=True, capture_output=True)
    text = subprocess.run(['tesseract', f'{stem}.png', 'stdout', '--psm', '3'], check=True,
                          capture_output=True, text=True).stdout
    return key, ' '.join(text.split()), time.time() - started


with ThreadPoolExecutor(max_workers=workers) as pool:
    results = list(pool.map(reocr, sorted(pages)))

rows = []
for (doc, source_doc, page), text_b, seconds in results:
    segments = pages[doc, source_doc, page]
    text_a = ' '.join(s['text'] for s in segments)
    share_a = dictionary_share(text_a)
    reasons = []
    if share_a is None or share_a < vision_threshold:
        reasons.append('garbled_text_a')
    if {s['segment_id'] for s in segments} & noted[doc]:
        reasons.append('reader_note')
    rows.append(dict(document_id=doc, year=sample[doc]['year'], source_document_id=source_doc, pdf_page=page,
                     segment_ids=';'.join(s['segment_id'] for s in segments), page_scope=segments[0]['page_scope'],
                     text_a=text_a, text_b=text_b, seconds_b=f'{seconds:.1f}', vision=int(bool(reasons)),
                     vision_reason=';'.join(reasons), image=f'{source_doc}_p{page:03d}.png'))
save_csv(rows, list(rows[0]), '../output/cpc_ocr_page_texts.csv', key=['document_id', 'source_document_id', 'pdf_page'])
print(f'{len(rows)} pages; {sum(r["vision"] for r in rows)} for vision; '
      f'{sum(float(r["seconds_b"]) for r in rows) / len(rows):.1f} s per page to render and OCR')
