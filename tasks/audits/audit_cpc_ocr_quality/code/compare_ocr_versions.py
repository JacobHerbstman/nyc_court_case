"""Compare production text with the two re-OCR versions on the sampled reports.

Versions of each report:
- a: production reading text;
- b_all: Tesseract 300 DPI --psm 3 on every page;
- b_flagged: b on the pages sent to vision, a elsewhere;
- c_flagged: vision transcription on those pages, a elsewhere.

Outputs:
- cpc_ocr_report_comparison.csv: per report and version, whether the human-coded
  community board vote tally and CPC hearing speaker tallies can be found in the text,
  and the dictionary share.
- cpc_ocr_page_comparison.csv: per page, dictionary share by version and which
  sections (board recommendation, hearing, consideration) the page carries.
- cpc_ocr_excerpts.md: side-by-side excerpts from the worst production pages.
"""
# Interactive use: cd to tasks/audits/audit_cpc_ocr_quality/code, then
# python3 compare_ocr_versions.py
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, '../../../_lib')
from data_reports import save_csv
from ocr_text_quality import alphabetic_words, dictionary_share

csv.field_size_limit(10**9)
NUMBER_WORDS = {w: i for i, w in enumerate(
    'zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen '
    'seventeen eighteen nineteen twenty'.split())}
NUMBER = re.compile(r'\b(\d{1,3}|' + '|'.join(NUMBER_WORDS) + r')\b', re.I)
NONE = re.compile(r'\b(none|no one|nobody|no speakers?|no other)\b', re.I)
TALLIES = {'cb_votes': (re.compile(r'\bvot(e|es|ed|ing)\b', re.I), 'cb_support_votes', 'cb_opposition_votes'),
           'speakers': (re.compile(r'\bspeak(er|ers|ing)?\b|\bspoke\b', re.I), 'cpc_support_speakers', 'cpc_opposition_speakers')}
SECTIONS = {'board_recommendation': re.compile(r'community board.{0,300}(recommend|vote)', re.I | re.S),
            'hearing': re.compile(r'public hearing', re.I),
            'consideration': re.compile(r'\bconsideration\b', re.I)}


def found(text, keyword, support, opposition):
    """Both human-coded numbers appear within 250 characters of a vote or speaker mention."""
    for m in keyword.finditer(text):
        window = text[max(0, m.start() - 250): m.end() + 250]
        numbers = {int(n) if n.isdigit() else NUMBER_WORDS[n.lower()] for n in NUMBER.findall(window)}
        if NONE.search(window):
            numbers.add(0)
        if support in numbers and (opposition is None or opposition in numbers):
            return True
    return False


with open('../output/cpc_ocr_sample.csv') as f:
    sample = {r['document_id']: r for r in csv.DictReader(f)}
with open('../output/cpc_ocr_page_texts.csv') as f:
    pages = list(csv.DictReader(f))
vision_dir = Path('../input/vision_transcriptions')
for p in pages:
    p['text_c'] = ''
    if p['vision'] == '1':
        p['text_c'] = ' '.join((vision_dir / p['image'].replace('.png', '.txt')).read_text().split())
human = defaultdict(dict)
with open('../input/ulurp_cpc_human_coding.csv') as f:
    for r in csv.DictReader(f):
        if r['source_document_id'] in sample and r['human_value'] != '' and r['field'].endswith(('_votes', '_speakers')):
            human[r['source_document_id']][r['field']] = int(r['human_value'])


def version_text(p, version):
    if version == 'a':
        return p['text_a']
    if version == 'b_all':
        return p['text_b']
    replacement = p['text_b'] if version == 'b_flagged' else p['text_c']
    return replacement if p['vision'] == '1' else p['text_a']


VERSIONS = ('a', 'b_all', 'b_flagged', 'c_flagged')
by_report = defaultdict(list)
for p in pages:
    by_report[p['document_id']].append(p)

report_rows = []
for doc, doc_pages in sorted(by_report.items()):
    for version in VERSIONS:
        text = ' '.join(version_text(p, version) for p in doc_pages)
        row = dict(document_id=doc, year=sample[doc]['year'], sample_cell=sample[doc]['sample_cell'], version=version,
                   words=alphabetic_words(text), dictionary_share=f'{dictionary_share(text) or 0:.3f}')
        for tally, (keyword, support_field, opposition_field) in TALLIES.items():
            support, opposition = human[doc].get(support_field), human[doc].get(opposition_field)
            row[f'{tally}_human'] = '' if support is None else f'{support}-{"" if opposition is None else opposition}'
            row[f'{tally}_found'] = '' if support is None else str(int(found(text, keyword, support, opposition)))
        report_rows.append(row)
save_csv(report_rows, list(report_rows[0]), '../output/cpc_ocr_report_comparison.csv', key=['document_id', 'version'])

page_rows = []
for p in pages:
    texts = {v: version_text(p, v) for v in ('a', 'b_all', 'c_flagged')}
    union = ' '.join(texts.values())
    page_rows.append(dict(document_id=p['document_id'], year=p['year'], source_document_id=p['source_document_id'],
                          pdf_page=p['pdf_page'], vision=p['vision'], vision_reason=p['vision_reason'],
                          words_a=alphabetic_words(texts['a']), words_b=alphabetic_words(texts['b_all']),
                          share_a=f'{dictionary_share(texts["a"]) or 0:.3f}',
                          share_b=f'{dictionary_share(texts["b_all"]) or 0:.3f}',
                          share_c=f'{dictionary_share(p["text_c"]) or 0:.3f}' if p['vision'] == '1' else '',
                          sections=';'.join(s for s, pattern in SECTIONS.items() if pattern.search(union)),
                          seconds_b=p['seconds_b']))
save_csv(page_rows, list(page_rows[0]), '../output/cpc_ocr_page_comparison.csv',
         key=['document_id', 'source_document_id', 'pdf_page'])

# Worst production pages that carry a board, hearing or consideration section.
worst = sorted((p for p in pages if p['vision'] == '1' and alphabetic_words(p['text_a']) >= 40),
               key=lambda p: (dictionary_share(p['text_a']) or 0))
worst = [p for p in worst if any(s.search(p['text_b'] + ' ' + p['text_c']) for s in SECTIONS.values())][:5]
lines = ['# Worst production pages: excerpts', '',
         'First 700 characters of each version; `a` is what readers see now.', '']
for p in worst:
    lines += [f"## {p['document_id']}, {p['year']}, source {p['source_document_id']} page {p['pdf_page']}", '']
    for label, text in (('a (production)', p['text_a']), ('b (Tesseract 300 DPI, psm 3)', p['text_b']),
                        ('c (vision)', p['text_c'])):
        lines += [f'**{label}**, dictionary share {dictionary_share(text) or 0:.2f}:', '', '> ' + text[:700], '']
Path('../output/cpc_ocr_excerpts.md').write_text('\n'.join(lines) + '\n')
