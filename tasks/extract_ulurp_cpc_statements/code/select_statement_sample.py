"""Freeze the first 100 statement readings: controls, long reports and fresh reports."""
import csv
import hashlib
import sys
from collections import defaultdict

sys.path.insert(0, '../../_lib')
from data_reports import save_csv

seed = sys.argv[1]
with open('../input/ulurp_cpc_reading_roster.csv') as f:
    roster = list(csv.DictReader(f))
assert len(roster) == len({r['document_id'] for r in roster})
assert all(r['preparation_status'] == 'ready' for r in roster)
with open('../input/ulurp_cpc_human_coding.csv') as f:
    human = list(csv.DictReader(f))
# Several coder/field rows can identify the same narrative; use membership, not a join.
coded = {doc.strip() for r in human for doc in r['represented_narrative_ids'].split(';') if doc.strip()}
rank = lambda r: hashlib.sha256((seed + '|' + r['document_id']).encode()).hexdigest()
roster.sort(key=rank)
selected = {}
known = {'C 190403 ZMX', 'C 160064 ZMX', 'C 170452 ZSM', 'C 180085 ZMQ'}
for r in roster:
    if r['application_number'] in known:
        selected[r['document_id']] = 'known_attachment_repair'
assert len(selected) == 4
for r in roster:
    if len(selected) < 20 and r['document_id'] in coded and r['document_id'] not in selected:
        selected[r['document_id']] = 'human_coded_control'
assert len(selected) == 20

fresh = [r for r in roster if r['document_id'] not in coded and r['document_id'] not in selected]
very_long = [r for r in fresh if int(r['segment_characters']) > 400000][:2]
long = [r for r in fresh if 100000 < int(r['segment_characters']) <= 400000][:4]
assert len(very_long) == 2 and len(long) == 4
for r in very_long + long:
    selected[r['document_id']] = 'long_report_stress_case'

# Round-robin across decade and presence of unresolved pages, seeded within each cell.
cells = defaultdict(list)
for r in fresh:
    if r['document_id'] not in selected:
        cells[int(r['year']) // 10 * 10, int(r['unresolved_pages']) > 0].append(r)
while len(selected) < 100:
    for cell in sorted(cells):
        if cells[cell] and len(selected) < 100:
            r = cells[cell].pop(0)
            selected[r['document_id']] = 'fresh_decade_scope_sample'

rows = []
for r in roster:
    if r['document_id'] in selected:
        rows.append(dict(document_id=r['document_id'], application_number=r['application_number'],
            project_name=r['project_name'], year=r['year'], selection_reason=selected[r['document_id']],
            human_coded=int(r['document_id'] in coded), unresolved_pages=r['unresolved_pages'],
            segment_count=r['segment_count'], segment_characters=r['segment_characters'], sample_seed=seed))
assert len(rows) == 100
save_csv(rows, list(rows[0]), '../output/ulurp_cpc_statement_sample.csv', ['document_id'])
print(f'Selected {len(rows)} reports, including {sum(r["human_coded"] for r in rows)} with prior human coding.')
