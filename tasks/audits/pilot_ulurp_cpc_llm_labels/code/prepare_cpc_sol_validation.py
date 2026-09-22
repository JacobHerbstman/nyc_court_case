#!/usr/bin/env python3
"""Select development reports using original human labels, then retain full text."""
import csv
import hashlib
import json
import sys
from pathlib import Path
sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

version = sys.argv[1] if len(sys.argv) == 2 else 'v2'
assert version in {'v2', 'actors_v2', 'v3'} and len(sys.argv) <= 2
if version == 'v2':
    design = json.loads(Path('../input/cpc_sol_validation_design_v2.json').read_text())
elif version == 'actors_v2':
    design = json.loads(Path('../input/cpc_sol_actor_design_v2.json').read_text())
else:
    design = json.loads(Path('../input/cpc_sol_expansion_design_v3.json').read_text())
with open('../input/cpc_jev_roster_corpus_v3.csv') as f:
    roster_rows = list(csv.DictReader(f))
roster = {r['document_id']: r for r in roster_rows}
assert len(roster) == len(roster_rows)
with open('../input/ulurp_cpc_human_coding.csv') as f:
    humans = list(csv.DictReader(f))
human = {(r['source_document_id'], r['field']): r for r in humans}
assert len(human) == len(humans)
excluded = set(design['excluded_source_document_ids'])
eligibility = []
for doc in sorted({r['source_document_id'] for r in humans}):
    r = roster.get(doc, {})
    source_ids = set(r.get('source_document_ids', '').split(';'))
    values = {h['field']: h['human_value'] for h in humans if h['source_document_id'] == doc
        and h['human_status'] in {'human_agreement', 'single_human_coder'}
        and (h['jacob_coding_complete'] == '1' or h['tyler_coding_complete'] == '1')}
    reason = ('missing_current_focal' if not r else 'source_not_ready' if r['preparation_status'] != 'ready'
        else 'prior_pilot_source' if source_ids & excluded else 'no_completed_human' if not values else 'eligible')
    council, civic = values.get('councilmember_position', ''), values.get('civic_group_position', '')
    eligibility.append(dict(document_id=doc, application_number=r.get('application_number', ''),
        reason=reason, council=council, civic=civic, issue_positive=int(any(values.get(f) == '1' for f in design['legacy_topics'])),
        source_parts=int(r.get('request_count', '0')), rank=hashlib.sha256(f'{design["seed"]}|{doc}'.encode()).hexdigest(), selected_group=''))
    if version in {'actors_v2', 'v3'}:
        h = human.get((doc, 'civic_group_position'), {})
        eligibility[-1].update(earlier_development=int(bool(source_ids & set(design['earlier_development_source_ids']))),
            civic_both=int(any(h.get(f'{coder}_coding_complete') == '1' and h.get(f'{coder}_value') == 'both'
                for coder in ['jacob', 'tyler'])))
selected, used = [], set()
for group, count in design['strata']:
    candidates = [r for r in eligibility if r['reason'] == 'eligible' and not r['selected_group'] and
        (group == 'council_support' and r['council'] == 'support_or_request' or
         group == 'civic_support' and r['civic'] == 'support_or_request' or
         group == 'civic_opposition' and r['civic'] == 'opposition' or
         group == 'long_issue' and r['source_parts'] >= 2 and r['issue_positive'] == 1 and r['council'] == r['civic'] == 'none_or_procedural' or
         group == 'long_issue_any_actor' and r['source_parts'] >= 2 and r['issue_positive'] == 1 or
         group == 'actor_negative' and r['council'] == r['civic'] == 'none_or_procedural' or
         group == 'council_opposition' and r['council'] == 'opposition' or
         group == 'civic_both' and r['civic_both'] == 1 or
         group == 'fresh_civic_opposition' and r['earlier_development'] == 0 and r['civic'] == 'opposition' or
         group == 'fresh_actor_negative' and r['earlier_development'] == 0 and r['council'] == r['civic'] == 'none_or_procedural' or
         group == 'random_remainder')]
    added = 0
    for r in sorted(candidates, key=lambda r: r['rank']):
        sources = set(roster[r['document_id']]['source_document_ids'].split(';'))
        if sources & used:
            continue
        r['selected_group'] = group
        selected.append(r)
        used.update(sources)
        added += 1
        if added == count:
            break
    assert added == count, f'Insufficient disjoint candidates for {group}'
assert len(selected) == sum(count for group, count in design['strata'])
selected_ids = {r['document_id'] for r in selected}
pages = {doc: [] for doc in selected_ids}
with open('../input/cpc_jev_segments_corpus_v3.csv') as f:
    for r in csv.DictReader(f):
        if r['document_id'] in selected_ids:
            pages[r['document_id']].append(r)
packets = {}
for doc in sorted(selected_ids):
    assert pages[doc] and len({p['segment_id'] for p in pages[doc]}) == len(pages[doc])
    packets[doc] = dict(document_id=doc, application_number=roster[doc]['application_number'],
        project_name=roster[doc]['project_name'], segments=pages[doc])
readers = list('abc') if version != 'v3' else [f'{n:02d}' for n in range(1, 11)]
assigned = {reader: [] for reader in readers}
loads = {reader: 0 for reader in readers}
sample = []
for r in sorted(selected, key=lambda r: -sum(len(p['text']) for p in pages[r['document_id']])):
    doc = r['document_id']
    reader = min((a for a in readers if len(assigned[a]) < 2), key=lambda a: (loads[a], a))
    assigned[reader].append(packets[doc])
    size = sum(len(p['text']) for p in pages[doc])
    loads[reader] += size
    sample.append(dict(document_id=doc, application_number=roster[doc]['application_number'],
        project_name=roster[doc]['project_name'], year=roster[doc]['year'], reader=reader,
        selection_group=r['selected_group'], source_parts=r['source_parts'], segments=len(pages[doc]), source_characters=size,
        source_document_ids=roster[doc]['source_document_ids'],
        packet_sha256=hashlib.sha256(json.dumps(packets[doc], sort_keys=True, ensure_ascii=False).encode()).hexdigest()))
references = [r for r in humans if r['source_document_id'] in selected_ids]
if version == 'v2':
    for reader in 'abc':
        Path(f'../output/cpc_sol_validation_packets_{reader}_v2.json').write_text(json.dumps(assigned[reader], indent=2, ensure_ascii=False)+'\n')
    save_csv(sample, list(sample[0]), '../output/cpc_sol_validation_sample_v2.csv', ['document_id'])
    save_csv(eligibility, list(eligibility[0]), '../output/cpc_sol_validation_selection_v2.csv', ['document_id'])
    save_csv(references, list(references[0]), '../output/cpc_sol_validation_humans_v2.csv', ['source_document_id', 'field'])
elif version == 'actors_v2':
    for reader in 'abc':
        Path(f'../output/cpc_sol_actor_packets_{reader}_v2.json').write_text(json.dumps(assigned[reader], indent=2, ensure_ascii=False)+'\n')
    for r in sample:
        r['earlier_development'] = next(e['earlier_development'] for e in selected if e['document_id'] == r['document_id'])
    save_csv(sample, list(sample[0]), '../output/cpc_sol_actor_sample_v2.csv', ['document_id'])
    save_csv(eligibility, list(eligibility[0]), '../output/cpc_sol_actor_selection_v2.csv', ['document_id'])
    save_csv(references, list(references[0]), '../output/cpc_sol_actor_humans_v2.csv', ['source_document_id', 'field'])
else:
    for reader in readers:
        Path(f'../output/cpc_sol_expansion_packets_{reader}_v3.json').write_text(json.dumps(assigned[reader], indent=2, ensure_ascii=False)+'\n')
    for r in sample:
        r['earlier_development'] = next(e['earlier_development'] for e in selected if e['document_id'] == r['document_id'])
    save_csv(sample, list(sample[0]), '../output/cpc_sol_expansion_sample_v3.csv', ['document_id'])
    save_csv(eligibility, list(eligibility[0]), '../output/cpc_sol_expansion_selection_v3.csv', ['document_id'])
    save_csv(references, list(references[0]), '../output/cpc_sol_expansion_humans_v3.csv', ['source_document_id', 'field'])
print(f'Selected {len(selected)} reports from {sum(r["reason"] == "eligible" for r in eligibility)} eligible human-coded reports; all text retained. No inference.')
