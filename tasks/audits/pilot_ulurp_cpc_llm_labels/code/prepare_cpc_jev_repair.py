#!/usr/bin/env python3
"""Freeze a bounded comparison of repaired sources and candidate questions."""
import copy
import csv
import hashlib
import json
import sys
from pathlib import Path
sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

# Run from tasks/audits/pilot_ulurp_cpc_llm_labels/code.
fresh_count, seed = int(sys.argv[1]), sys.argv[2]
with open('../input/cpc_jev_roster_corpus_v3.csv') as f:
    roster = list(csv.DictReader(f))
with open('../input/cpc_jev_labels_corpus_v2.csv') as f:
    old = {r['document_id']: r for r in csv.DictReader(f)}
with open('../input/cpc_jev_usage_corpus_v2.csv') as f:
    successes = {r['request_sha256']: r for r in csv.DictReader(f) if r['status'] == 'successful'}
with open('../input/ulurp_cpc_human_coding.csv') as f:
    excluded = {r['source_document_id'] for r in csv.DictReader(f)}
quality = json.loads(Path('../input/cpc_jev_quality_selection.json').read_text())
excluded |= {r['document_id'] for r in quality['sample']}
for reader in 'abc':
    for line in Path(f'../input/cpc_jev_corpus_reader_{reader}.jsonl').read_text().splitlines():
        excluded.add(json.loads(line)['document_id'])
candidate = json.loads(Path('../input/cpc_jev_codebook_candidate_v2.json').read_text())
assert len(roster) == len({r['document_id'] for r in roster})
by_app = {r['application_number']: r for r in roster}
known = {
    'C 230091 ZMQ': 'known_error', 'C 180346 PSX': 'known_error',
    'C 010691 ZSM': 'known_error', 'C 920197 PPQ': 'known_error',
    'C 900038 PLQ': 'known_error',
    'C 820666 HDK': 'positive_control', 'C 990218 ZSK': 'positive_control',
}
selected = [dict(by_app[app], test_group=group) for app, group in known.items()]
assert all(r['preparation_status'] == 'ready' for r in selected)
used_sources = {s for r in selected for s in r['source_document_ids'].split(';')}
eligible = [r for r in roster if r['preparation_status'] == 'ready'
            and int(r['request_count']) <= 2
            and old.get(r['document_id'], {}).get('extraction_status') == 'complete'
            and not set(r['source_document_ids'].split(';')) & excluded]
fresh = []
for r in sorted(eligible, key=lambda r: hashlib.sha256(f'{seed}|{r["document_id"]}'.encode()).hexdigest()):
    if set(r['source_document_ids'].split(';')) & used_sources:
        continue
    fresh.append(dict(r, test_group='fresh'))
    used_sources.update(r['source_document_ids'].split(';'))
    if len(fresh) == fresh_count:
        break
assert len(fresh) == fresh_count
selected += fresh
sample = [{k: r[k] for k in ('document_id', 'application_number', 'project_name', 'year',
          'test_group', 'request_count', 'source_document_ids', 'excluded_unrelated_pages')}
          | {'selection_seed': seed, 'fresh_eligible_reports': len(eligible)} for r in selected]
selected_ids = {r['document_id'] for r in sample}
packets, plan = [], []
with open('../input/cpc_jev_requests_corpus_v3.jsonl') as f:
    for line in f:
        original = json.loads(line)
        if original['document_id'] not in selected_ids:
            continue
        for condition in ('original', 'candidate'):
            body = copy.deepcopy(original['body'])
            if condition == 'candidate':
                options = next(q['criteria'] for q in body['questions'].values() if q['type'] == 'choice')
                questions = {}
                for field, rule in candidate['questions'].items():
                    questions[field] = dict(type=rule['type'], instructions=rule['instructions'],
                        criteria=options if rule['type'] == 'choice' else rule['criteria'])
                    if rule['type'] == 'noul' and rule['evidence']:
                        questions['page__' + field] = dict(type='choice',
                            instructions='Which supplied page best supports a YES answer to this question? Select none if no page does. ' + rule['instructions'],
                            criteria=options)
                body['questions'] = questions
            digest = hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            if condition == 'original':
                assert digest == original['request_sha256']
            reuse = successes.get(digest)
            request_id = condition + '__' + original['request_id']
            report = body['state']['report']
            plan.append(dict(request_id=request_id, document_id=original['document_id'],
                condition=condition, part=report['part'], parts=report['parts'], request_sha256=digest,
                acquisition='reuse_saved' if reuse else 'one_new_attempt',
                saved_request_id=reuse['request_id'] if reuse else '',
                base_request_id=original['request_id']))
            if not reuse:
                packets.append(dict(request_id=request_id, document_id=original['document_id'],
                    request_sha256=digest, body=body))
assert len(plan) == 2 * sum(int(r['request_count']) for r in sample)
assert len({p['request_sha256'] for p in packets}) == len(packets)
assert all(p['body']['model'] == 'typesafe-ai/jev' for p in packets)
# Keep related conditions adjacent; no identical request is resubmitted.
packets.sort(key=lambda p: (next(i for i, r in enumerate(sample) if r['document_id'] == p['document_id']),
                          p['body']['state']['report']['part'], p['request_id'].startswith('candidate')))
with open('../output/cpc_jev_requests_v12.jsonl', 'w') as f:
    for p in packets:
        f.write(json.dumps(p, ensure_ascii=False) + '\n')
save_csv(sample, list(sample[0]), '../output/cpc_jev_sample_v12.csv', ['document_id'])
save_csv(plan, list(plan[0]), '../output/cpc_jev_plan_v12.csv', ['request_id'])
print(f'Frozen {len(sample)} reports: {len(known)} development cases and {len(fresh)} fresh reports. '
      f'{len(packets)} new requests; {len(plan)-len(packets)} exact saved requests reused. No inference.')
