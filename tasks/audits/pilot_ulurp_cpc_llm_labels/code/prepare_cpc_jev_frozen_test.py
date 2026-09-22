#!/usr/bin/env python3
"""Select the fixed short/long comparison and preserve complete source packets."""
import copy
import csv
import hashlib
import json
import sys
from pathlib import Path
sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

fresh_per_stratum, seed = int(sys.argv[1]), sys.argv[2]
assert fresh_per_stratum == 5, 'The approved experiment has ten fresh reports.'
if len(sys.argv) == 4:
    version = sys.argv[3]
    assert version in {'v15', 'v16'}
    prior = 'v13' if version == 'v15' else 'v15'
    # Recover failed calls on the original sample; never select new reports.
    packets = [json.loads(s) for s in Path(f'../input/cpc_jev_frozen_plan_{prior}.jsonl').read_text().splitlines()]
    latest, pending = {}, set()
    for line in Path(f'../input/cpc_jev_responses_{prior}.jsonl').read_text().splitlines():
        r = json.loads(line)
        key = r['request_id'], r['attempt_number']
        if r['event'] == 'started':
            pending.add(key)
        else:
            pending.remove(key)
            latest[r['request_id']] = r
    assert not pending
    new_requests, plan = [], []
    for p in packets:
        response = p['reused_response'] or latest[p['request_id']]
        assert response['request_sha256'] == p['request_sha256']
        if response['valid_response']:
            p['reused_response'] = response
        else:
            assert response['http_status'] in {502, 503, 504, 529}
            p['reused_response'] = None
            new_requests.append({k: v for k, v in p.items() if k != 'reused_response'})
        plan.append(dict(request_id=p['request_id'], document_id=p['document_id'], condition=p['condition'],
            request_sha256=p['request_sha256'], acquisition='reuse_saved' if p['reused_response'] else 'retry_received_server_failure'))
    assert len(packets) == 94 and len(new_requests) == (62 if version == 'v15' else 59)
    Path(f'../output/cpc_jev_all_requests_{version}.jsonl').write_text(''.join(json.dumps(p, ensure_ascii=False) + '\n' for p in packets))
    Path(f'../output/cpc_jev_requests_{version}.jsonl').write_text(''.join(json.dumps(p, ensure_ascii=False) + '\n' for p in new_requests))
    save_csv(plan, list(plan[0]), f'../output/cpc_jev_plan_{version}.csv', ['request_id'])
    print(f'Same twenty reports: {len(packets) - len(new_requests)} exact successes reused; one new attempt scheduled for each of {len(new_requests)} received server failures.')
    sys.exit(0)
with open('../input/cpc_jev_roster_corpus_v3.csv') as f:
    roster = list(csv.DictReader(f))
by_doc = {r['document_id']: r for r in roster}
by_app = {r['application_number']: r for r in roster}
assert len(by_doc) == len(roster)
with open('../input/ulurp_cpc_human_coding.csv') as f:
    excluded = {r['source_document_id'] for r in csv.DictReader(f)}
prior_versions = ['v1', 'v2', 'v3_retrieve', 'v3_verify', 'v4', 'v5', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
for version in prior_versions:
    for line in Path(f'../input/cpc_jev_requests_{version}.jsonl').read_text().splitlines():
        packet = json.loads(line)
        excluded.add(packet['document_id'])
for reader in 'abc':
    for line in Path(f'../input/cpc_jev_corpus_reader_{reader}.jsonl').read_text().splitlines():
        excluded.add(json.loads(line)['document_id'])
excluded.update(r['document_id'] for r in json.loads(Path('../input/cpc_jev_quality_selection.json').read_text())['sample'])
with open('../output/cpc_jev_sample_v12.csv') as f:
    excluded.update(r['document_id'] for r in csv.DictReader(f))
for doc in list(excluded):
    excluded.update(by_doc.get(doc, {}).get('source_document_ids', '').split(';'))
known = ['C 180346 PSX', 'C 230091 ZMQ', 'C 010691 ZSM', 'C 900038 PLQ',
         'C 830464 ZMX', 'C 050480 ZMX', 'C 810488 PPM', 'C 820666 HDK',
         'C 800193 HPK', 'C 990218 ZSK']
selected = [dict(by_app[app], test_group='control') for app in known]
used = {s for r in selected for s in r['source_document_ids'].split(';')}
eligibility = []
for stratum in ('short', 'long'):
    eligible = [r for r in roster if r['preparation_status'] == 'ready'
                and (int(r['request_count']) <= 2) == (stratum == 'short')
                and not set(r['source_document_ids'].split(';')) & excluded]
    count = 0
    for r in sorted(eligible, key=lambda r: hashlib.sha256(f'{seed}|{r["document_id"]}'.encode()).hexdigest()):
        sources = set(r['source_document_ids'].split(';'))
        if sources & used:
            continue
        selected.append(dict(r, test_group='fresh'))
        used.update(sources)
        count += 1
        if count == fresh_per_stratum:
            break
    assert count == fresh_per_stratum
    eligibility.append(dict(stratum=stratum, eligible=len(eligible), selected=count, selection_seed=seed))
sample = [dict((k, r[k]) for k in ('document_id', 'application_number', 'project_name', 'year',
          'test_group', 'request_count', 'source_document_ids', 'preparation_status', 'excluded_unrelated_pages'))
          | dict(stratum='short' if int(r['request_count']) <= 2 else 'long', selection_seed=seed)
          for r in selected]
assert len(sample) == 20 and len({r['document_id'] for r in sample}) == 20
selected_ids = {r['document_id'] for r in sample}
books = {'baseline': json.loads(Path('../input/cpc_jev_codebook_candidate_v2.json').read_text()),
         'revised': json.loads(Path('cpc_jev_codebook_v13.json').read_text())}
saved = {}
for line in Path('../input/cpc_jev_responses_v12.jsonl').read_text().splitlines():
    r = json.loads(line)
    if r['event'] == 'received' and r['valid_response']:
        assert r['request_sha256'] not in saved
        saved[r['request_sha256']] = r
packets, plan, new_requests = [], [], []
with open('../input/cpc_jev_requests_corpus_v3.jsonl') as f:
    for line in f:
        original = json.loads(line)
        if original['document_id'] not in selected_ids:
            continue
        for condition, book in books.items():
            body = copy.deepcopy(original['body'])
            options = next(q['criteria'] for q in body['questions'].values() if q['type'] == 'choice')
            questions = {}
            for field, rule in book['questions'].items():
                questions[field] = dict(type=rule['type'], instructions=rule['instructions'],
                    criteria=rule.get('criteria', options))
                if rule['evidence'] and (rule['type'] == 'noul' or condition == 'revised'):
                    evidence_options = dict(options)
                    if condition == 'revised':
                        evidence_options.update(ambiguous='Relevant wording is present but cannot be interpreted determinately.',
                            context_missing='Potential evidence requires unavailable or cross-part context.')
                    questions['page__' + field] = dict(type='choice',
                        instructions='Which supplied page best establishes a YES answer to the following substantive question? Select none if no supplied page does. ' + rule['instructions'],
                        criteria=evidence_options)
                    if condition == 'baseline':
                        questions['page__' + field]['instructions'] = 'Which supplied page best supports a YES answer to this question? Select none if no page does. ' + rule['instructions']
            body['questions'] = questions
            digest = hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            reuse = saved.get(digest)
            packet = dict(request_id=condition + '__' + original['request_id'], document_id=original['document_id'],
                request_sha256=digest, body=body, condition=condition,
                reused_response=reuse, base_request_id=original['request_id'])
            packets.append(packet)
            if not reuse:
                new_requests.append({k: v for k, v in packet.items() if k != 'reused_response'})
            plan.append(dict(request_id=packet['request_id'], document_id=packet['document_id'], condition=condition,
                part=body['state']['report']['part'], parts=body['state']['report']['parts'],
                request_sha256=digest, acquisition='reuse_saved' if reuse else 'new',
                full_request_characters=len(json.dumps(body, ensure_ascii=False)),
                saved_request_id=reuse['request_id'] if reuse else ''))
assert len(plan) == 2 * sum(int(r['request_count']) for r in sample if r['preparation_status'] == 'ready')
assert len({p['request_sha256'] for p in packets}) == len(packets)
assert max(r['full_request_characters'] for r in plan) < 150000, 'Inspect full request size before acquisition.'
order = {r['document_id']: i for i, r in enumerate(sample)}
for rows, destination in [(packets, '../output/cpc_jev_all_requests_v13.jsonl'), (new_requests, '../output/cpc_jev_requests_v13.jsonl')]:
    rows.sort(key=lambda p: (order[p['document_id']], p['body']['state']['report']['part'], p['condition']))
    Path(destination).write_text(''.join(json.dumps(p, ensure_ascii=False) + '\n' for p in rows))
with open('../input/cpc_jev_segments_corpus_v3.csv') as f:
    segments = [r for r in csv.DictReader(f) if r['document_id'] in selected_ids]
assert len({(r['document_id'], r['segment_id']) for r in segments}) == len(segments)
save_csv(sample, list(sample[0]), '../output/cpc_jev_sample_v13.csv', ['document_id'])
save_csv(plan, list(plan[0]), '../output/cpc_jev_plan_v13.csv', ['request_id'])
save_csv(segments, list(segments[0]), '../output/cpc_jev_segments_v13.csv', ['document_id', 'segment_id'])
save_csv(eligibility, list(eligibility[0]), '../output/cpc_jev_selection_v13.csv', ['stratum'])
print(f'Frozen 20 reports; {len(new_requests)} new screening requests and {len(plan)-len(new_requests)} exact reuses.')
print('Fresh eligibility and selection:', eligibility)
print('Largest complete request:', max(r['full_request_characters'] for r in plan), 'characters, not a model-token count.')
