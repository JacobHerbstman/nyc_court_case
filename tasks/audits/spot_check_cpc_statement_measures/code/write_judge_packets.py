"""Write packets for judging the spot-check measures from Sol's statement rows.

Three packets per sampled report:
- rows_only: the full run's statement rows for the report;
- rows_hearing: the same rows plus the report's hearing pages, meaning in-scope
  segments that mention a public hearing, appearances or speakers (companion reports
  in the bundle included);
- rows_hearing_slim: the hearing pages with a compact table of only the row fields
  the measures use (no quotes, notes or field names on every row), about a third of
  the size.
A model then answers the codebook from the packet alone, without the PDF.
"""
# Interactive use: cd to tasks/audits/spot_check_cpc_statement_measures/code, then
# python3 write_judge_packets.py
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

csv.field_size_limit(10**9)
HEARING = re.compile(r'public hearing|hearing was (duly )?held|appearances?|speakers? (in|who)|spoke in', re.I)
SLIM_FIELDS = ['statement_id', 'actor_name', 'actor_roles', 'project_team', 'statement_type', 'stance_on_project',
               'component', 'votes', 'response', 'response_statement_ids', 'stage', 'summary']
FIELDS = ['statement_id', 'segment_ids', 'application', 'actor_name', 'actor_roles', 'office_or_affiliation',
          'project_team', 'speaks_for', 'statement_type', 'certainty', 'stance_on_project', 'component', 'votes',
          'response', 'response_statement_ids', 'stage', 'timing_note', 'summary', 'quote', 'note']

with open('cpc_spot_check_sample.csv') as f:
    sample = list(csv.DictReader(f))
docs = {r['document_id'] for r in sample}
rows = defaultdict(list)
with open('../input/ulurp_cpc_statements_full_sol_high_20260927.csv') as f:
    for r in csv.DictReader(f):
        if r['document_id'] in docs:
            rows[r['document_id']].append(r)
hearing = defaultdict(list)
with open('../input/ulurp_cpc_reading_segments.csv') as f:
    for r in csv.DictReader(f):
        if r['document_id'] in docs and r['page_scope'] == 'in_scope' and HEARING.search(r['text']):
            hearing[r['document_id']].append(r)

manifest = []
for s in sample:
    doc = s['document_id']
    header = (f"document_id: {doc}\napplication_number: {s['application_number']}\nyear: {s['year']}\n"
              f"project_name: {s['project_name']}\n")
    statement_rows = '\n'.join(' | '.join(f'{k}={r[k]}' for k in FIELDS if r[k]) for r in rows[doc])
    texts = {'rows_only': f'{header}\n## Statement rows\n\n{statement_rows}\n'}
    pages = '\n\n'.join(f"=== {r['segment_id']} | source {r['source_application_number']} | PDF page {r['pdf_page']} ===\n"
                        f"{r['text']}" for r in hearing[doc])
    texts['rows_hearing'] = texts['rows_only'] + f'\n## Hearing pages\n\n{pages}\n'
    slim_rows = '\n'.join(' | '.join(r[k] for k in SLIM_FIELDS) for r in rows[doc])
    texts['rows_hearing_slim'] = (f"{header}\n## Statement rows\n\nColumns: {' | '.join(SLIM_FIELDS)}\n\n{slim_rows}\n"
                                  f"\n## Hearing pages\n\n{pages}\n")
    for variant, text in texts.items():
        path = Path(f'../temp/judge_packets/{variant}/{doc}.txt')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        manifest.append(dict(document_id=doc, variant=variant, statement_rows=len(rows[doc]),
                             hearing_segments=len(hearing[doc]) if variant != 'rows_only' else 0,
                             characters=len(text), packet=str(path)))
save_csv(manifest, list(manifest[0]), '../output/cpc_row_judge_packets.csv', key=['document_id', 'variant'])
print({v: sum(m['characters'] for m in manifest if m['variant'] == v) for v in texts})
