"""Choose twenty source audits without reading extraction answers or label values."""
import csv
import hashlib
import sys
from collections import defaultdict

sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

seed = sys.argv[1]
with open('../input/first100_sample.csv') as f:
    sample = list(csv.DictReader(f))
with open('../input/first100_prompts.csv') as f:
    prompts = list(csv.DictReader(f))
assert len(sample) == len({r['document_id'] for r in sample}) == 100
assert len(prompts) == len({(r['document_id'], r['part']) for r in prompts}) == 103
assert {r['document_id'] for r in sample} == {r['document_id'] for r in prompts}

part_numbers = defaultdict(list)
for r in prompts:
    part_numbers[r['document_id']].append(int(r['part']))
for r in prompts:
    assert sorted(part_numbers[r['document_id']]) == list(range(1, int(r['parts']) + 1))
sample.sort(key=lambda r: hashlib.sha256((seed + '|' + r['document_id']).encode()).hexdigest())

selected = {}
for r in sample:
    if r['selection_reason'] == 'known_attachment_repair':
        selected[r['document_id']] = 'repaired_attachment_control'
assert len(selected) == 4
for r in sample:
    if len(part_numbers[r['document_id']]) > 1:
        assert r['document_id'] not in selected
        selected[r['document_id']] = 'split_bundle'
assert len(selected) == 6

controls = [r for r in sample if r['human_coded'] == '1' and r['document_id'] not in selected][:4]
assert len(controls) == 4
for r in controls:
    selected[r['document_id']] = 'additional_human_control'
long = [r for r in sample if r['selection_reason'] == 'long_report_stress_case'
        and r['document_id'] not in selected][:2]
assert len(long) == 2
for r in long:
    selected[r['document_id']] = 'long_single_packet'

fresh = [r for r in sample if r['human_coded'] == '0' and r['document_id'] not in selected]
# One further fresh report per decade, then balance unresolved-page coverage.
decades = sorted({int(r['year']) // 10 * 10 for r in fresh})
assert len(decades) == 6
for decade in decades:
    r = next(r for r in fresh if int(r['year']) // 10 * 10 == decade)
    selected[r['document_id']] = 'fresh_decade_coverage'
for unresolved in [False, True]:
    r = next(r for r in fresh if r['document_id'] not in selected
             and (int(r['unresolved_pages']) > 0) == unresolved)
    selected[r['document_id']] = 'fresh_page_scope_coverage'
assert len(selected) == 20

rows = []
for r in sample:
    if r['document_id'] in selected:
        rows.append(dict(audit_order=len(rows) + 1, document_id=r['document_id'],
            application_number=r['application_number'], project_name=r['project_name'], year=r['year'],
            human_coded=r['human_coded'], unresolved_pages=r['unresolved_pages'],
            segment_characters=r['segment_characters'], parts=len(part_numbers[r['document_id']]),
            audit_reason=selected[r['document_id']], audit_seed=seed))
save_csv(rows, list(rows[0]), '../output/cpc_statement_audit_sample.csv', ['document_id'])
print(f'Frozen {len(rows)} audits: {sum(r["human_coded"] == "1" for r in rows)} human-coded, '
      f'{sum(r["parts"] > 1 for r in rows)} split bundles, '
      f'{sum(int(r["unresolved_pages"]) > 0 for r in rows)} with unresolved pages.')
