"""Draw the OCR-quality sample once, before any re-OCR, and freeze it in code/.

The run keeps completing reports, so redrawing later would give a different sample;
the script refuses to overwrite the frozen table. Delete it deliberately to redraw.

From reports the full statement run has completed:
- flagged: the reader's notes mention garbled, illegible, unreadable or OCR text;
- human tallies: a human code exists for community board votes or CPC hearing
  speaker counts.
Selection, in seeded random order within each cell, excluding reports over 60 pages:
- all flagged pre-1990 reports with human tallies (15 after the page cap);
- 9 flagged 1990-or-later reports with human tallies;
- 5 flagged pre-1990 reports without human tallies;
- 5 unflagged controls with human tallies, 2 pre-1990 and 3 from 1990 on.
"""
# Interactive use: cd to tasks/audits/audit_cpc_ocr_quality/code, then
# python3 select_ocr_sample.py cpc-ocr-quality-20260927
import csv
import hashlib
import re
import sys
from pathlib import Path

sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

seed = sys.argv[1]
if Path('cpc_ocr_sample.csv').exists():
    sys.exit('cpc_ocr_sample.csv is the frozen sample; delete it deliberately to redraw.')
FLAG = re.compile(r'garbl|illegib|unreadable|\bOCR\b', re.I)
TALLY_FIELDS = {'cb_support_votes', 'cb_opposition_votes', 'cpc_support_speakers', 'cpc_opposition_speakers'}

# The run was paused on September 27; the status snapshot's hash ties the sample to it.
# Rebuilding after the run resumes would draw from a larger pool.
status_path = '../input/ulurp_cpc_statement_status_full_sol_high_20260927.csv'
snapshot = hashlib.sha256(open(status_path, 'rb').read()).hexdigest()
with open(status_path) as f:
    complete = {r['document_id']: r for r in csv.DictReader(f) if r['status'] == 'complete'}
with open('../input/ulurp_cpc_reading_roster.csv') as f:
    roster = {r['document_id']: r for r in csv.DictReader(f)}
with open('../input/ulurp_cpc_human_coding.csv') as f:
    tallied = {r['source_document_id'] for r in csv.DictReader(f) if r['field'] in TALLY_FIELDS and r['human_value'] != ''}

candidates = []
for doc, status in complete.items():
    r = roster[doc]
    if int(r['segment_count']) > 60:
        continue
    candidates.append(dict(document_id=doc, application_number=r['application_number'], year=r['year'],
                           pages=r['segment_count'], flagged=int(bool(FLAG.search(status['reading_notes']))),
                           human_tallies=int(doc in tallied), pre1990=int(int(r['year']) < 1990),
                           rank=hashlib.sha256(f'{seed}|{doc}'.encode()).hexdigest(), status_snapshot_sha256=snapshot,
                           reading_notes=status['reading_notes']))
candidates.sort(key=lambda r: r['rank'])

cells = [('flagged_pre1990_human', 1, 1, 1, None), ('flagged_1990on_human', 1, 1, 0, 9),
         ('flagged_pre1990_no_human', 1, 0, 1, 5), ('control_pre1990_human', 0, 1, 1, 2),
         ('control_1990on_human', 0, 1, 0, 3)]
sample = []
for cell, flagged, human, pre1990, n in cells:
    pool = [r for r in candidates if (r['flagged'], r['human_tallies'], r['pre1990']) == (flagged, human, pre1990)]
    sample += [dict(r, sample_cell=cell) for r in (pool if n is None else pool[:n])]
assert len(sample) == len({r["document_id"] for r in sample})
fields = ['document_id', 'application_number', 'year', 'pages', 'flagged', 'human_tallies', 'pre1990', 'sample_cell', 'rank', 'status_snapshot_sha256',
          'reading_notes']
save_csv(sample, fields, '../code/cpc_ocr_sample.csv', key=['document_id'])
print({cell: sum(r['sample_cell'] == cell for r in sample) for cell, *_ in cells})
