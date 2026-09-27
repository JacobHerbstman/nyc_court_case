"""Draw the spot-check sample of full-run reports once and freeze it in code/.

The run keeps completing reports, so redrawing later would give a different sample;
the script refuses to overwrite the frozen table. Delete it deliberately to redraw.

Candidates are reports the full run has completed, of 40 pages or fewer, with no
earlier human coding. Reports in the first 100 are excluded. Selection, in seeded
random order within each cell:
- 15 where the derived council member position is not none;
- 15 more where the derived civic group position is not none;
- 5 where the reading text mentions a council member but the derived position is
  none, to catch omissions;
- 10 from the rest.
"""
# Interactive use: cd to tasks/audits/spot_check_cpc_statement_measures/code, then
# python3 select_spot_check.py cpc-spot-check-20260927
import csv
import hashlib
import re
import sys
from pathlib import Path

sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

seed = sys.argv[1]
if Path('cpc_spot_check_sample.csv').exists():
    sys.exit('cpc_spot_check_sample.csv is the frozen sample; delete it deliberately to redraw.')
COUNCIL_MEMBER = re.compile(r'council\s*(member|man|woman)', re.I)
csv.field_size_limit(10**9)

# The run is in progress; the status snapshot's hash ties the sample to it.
status_path = '../input/ulurp_cpc_statement_status_full_sol_high_20260927.csv'
snapshot = hashlib.sha256(open(status_path, 'rb').read()).hexdigest()
with open(status_path) as f:
    complete = {r['document_id'] for r in csv.DictReader(f) if r['status'] == 'complete'}
with open('../input/cpc_statement_full_run_report_measures.csv') as f:
    derived = {}
    for r in csv.DictReader(f):
        derived.setdefault(r['document_id'], {})[r['measure']] = r['value']
with open('../input/ulurp_cpc_reading_roster.csv') as f:
    roster = {r['document_id']: r for r in csv.DictReader(f)}
with open('../input/ulurp_cpc_human_coding.csv') as f:
    coded = {r['source_document_id'] for r in csv.DictReader(f)}
with open('../input/ulurp_cpc_statement_sample.csv') as f:
    first100 = {r['document_id'] for r in csv.DictReader(f)}
mentions = set()
with open('../input/ulurp_cpc_reading_segments.csv') as f:
    for r in csv.DictReader(f):
        if r['document_id'] in complete and COUNCIL_MEMBER.search(r['text']):
            mentions.add(r['document_id'])

candidates = sorted((doc for doc in complete & set(derived)
                     if doc not in coded | first100 and int(roster[doc]['segment_count']) <= 40),
                    key=lambda doc: hashlib.sha256(f'{seed}|{doc}'.encode()).hexdigest())
cells = [('council_derived', lambda d: derived[d]['councilmember_position'] != 'none_or_procedural', 15),
         ('civic_derived', lambda d: derived[d]['civic_group_position'] != 'none_or_procedural', 15),
         ('council_mentioned_not_derived', lambda d: d in mentions and derived[d]['councilmember_position'] == 'none_or_procedural', 5),
         ('random_other', lambda d: True, 10)]
chosen = {}
for cell, rule, n in cells:
    for doc in [d for d in candidates if d not in chosen and rule(d)][:n]:
        chosen[doc] = cell
rows = [dict(document_id=doc, application_number=roster[doc]['application_number'], year=roster[doc]['year'],
             project_name=roster[doc]['project_name'], pages=roster[doc]['segment_count'], sample_cell=cell,
             status_snapshot_sha256=snapshot) for doc, cell in chosen.items()]
assert len(rows) == 45
save_csv(rows, list(rows[0]), '../code/cpc_spot_check_sample.csv', key=['document_id'])
print({cell: sum(r['sample_cell'] == cell for r in rows) for cell, _, _ in cells})
