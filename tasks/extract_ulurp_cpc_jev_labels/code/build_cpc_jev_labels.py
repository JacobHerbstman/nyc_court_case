#!/usr/bin/env python3
"""Build provisional labels from saved responses; retain the complete corpus."""
import csv
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
sys.path.insert(0,'../../_lib')
from data_reports import save_csv
threshold = float(sys.argv[1])
assert 0 < threshold < 1
book = json.loads(Path('cpc_jev_codebook_v1.json').read_text())
with open('../output/cpc_jev_roster_v2.csv') as f:
    roster = list(csv.DictReader(f))
with open('../output/cpc_jev_request_index_v2.csv') as f:
    index = list(csv.DictReader(f))
with open('../output/cpc_jev_segments_v2.csv') as f:
    segments = {(r['request_id'],r['segment_id']):r for r in csv.DictReader(f)}
with open('../input/cpc_jev_report_manifest_v2.csv') as f:
    manifest = list(csv.DictReader(f))
with open('../input/cpc_jev_narrative_sources_v2.csv') as f:
    links = list(csv.DictReader(f))
with open('../input/cpc_jev_completed_requests_v1.jsonl') as f:
    carried = [json.loads(line) for line in f]
with open('../input/cpc_jev_completed_segments_v1.csv') as f:
    carried_segments = {(r['request_id'],r['segment_id']):r for r in csv.DictReader(f)}
carried_docs = {r['document_id'] for r in carried}
assert len(roster)==len({r['document_id'] for r in roster})
assert len(index)==len({r['request_id'] for r in index})
assert len(manifest)==len({r['document_id'] for r in manifest})
assert len(links)==len({(r['document_id'],r['source_document_id']) for r in links})
# Only the three fully completed first-vintage reports are eligible for reuse.
requests = {r['request_id']:dict(r,vintage='v2') for r in index if r['document_id'] not in carried_docs}
for p in carried:
    assert p['body']['state']['report']['parts']==1
    requests[p['request_id']] = dict(request_id=p['request_id'],document_id=p['document_id'],request_sha256=p['request_sha256'],part=1,parts=1,vintage='v1')
received, started = {}, {}
for vintage, path in [('v1','../input/cpc_jev_responses_v1.jsonl'),('v2','../input/cpc_jev_responses_v2.jsonl')]:
    with open(path) as f:
        for line in f:
            r = json.loads(line)
            if r['request_id'] not in requests or requests[r['request_id']]['vintage']!=vintage:
                assert vintage=='v1', 'Unexpected newer request on a carried report.'
                continue
            assert r['request_sha256']==requests[r['request_id']]['request_sha256']
            attempt = r.get('attempt', 1)
            assert 1 <= attempt <= 6
            target = started if r['event']=='started' else received
            key = (r['request_id'], attempt)
            assert r['event'] in {'started','received'} and key not in target
            target[key] = r
assert received.keys() <= started.keys()
history = defaultdict(list)
attempt_counts = defaultdict(int)
latest_attempts = defaultdict(int)
unknown_requests = {rid for rid, number in started.keys() - received.keys()}
for request_id, attempt in started:
    attempt_counts[request_id] += 1
    latest_attempts[request_id] = max(latest_attempts[request_id], attempt)
for (request_id, attempt), r in sorted(received.items()):
    if attempt > 1:
        prior = received.get((request_id, attempt - 1), {})
        assert not prior.get('valid_response') and prior.get('http_status') in {429, 502, 503, 504, 529}, 'Recovery did not follow a recorded temporary error.'
    history[request_id].append(dict(r, attempt=attempt))
for request_id, observations in history.items():
    valid = [r for r in observations if r.get('valid_response')]
    assert len(valid) <= 1, 'A successful request was submitted again.'
    if valid:
        assert latest_attempts[request_id] == valid[0]['attempt'], 'Attempt after a successful answer.'
answers, usage, grouped = [], [], defaultdict(list)
for request_id, p in requests.items():
    observations = history[request_id]
    valid = [r for r in observations if r.get('valid_response')]
    r = valid[0] if valid else observations[-1] if observations else {}
    status = 'successful' if valid else 'failed' if r else 'outcome_unknown' if attempt_counts[request_id] else 'not_attempted'
    if request_id in unknown_requests:
        status = 'outcome_unknown'
        r = {}
    try:
        payload = json.loads(r.get('raw_response','{}'))
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload,dict):
        payload = {}
    if r.get('http_status') == 0 and payload.get('outcome_unknown'):
        status = 'outcome_unknown'
    result = payload.get('answers',{}) if status=='successful' else {}
    if status=='successful':
        expected = set(book['questions']) | {'page__'+f for f,q in book['questions'].items() if q['type']=='noul' and q['evidence']}
        assert set(result)==expected, 'Successful response has missing or extra questions.'
    usage.append(dict(request_id=request_id,document_id=p['document_id'],vintage=p['vintage'],status=status,http_status=r.get('http_status',''),
        input_tokens=payload.get('usage',{}).get('input_tokens'),output_tokens=payload.get('usage',{}).get('output_tokens'),request_sha256=p['request_sha256'],
        selected_attempt=r.get('attempt', ''),attempt_count=attempt_counts[request_id]))
    grouped[p['document_id']].append(usage[-1])
    lookup = carried_segments if p['vintage']=='v1' else segments
    for field, rule in book['questions'].items():
        a = result.get(field,{})
        selected = a.get('choice','') if rule['type']=='choice' else result.get('page__'+field,{}).get('choice','')
        probability = a.get('noul') if rule['type']=='noul' else None
        if a:
            assert a['type']==rule['type']
            if rule['type']=='noul':
                assert type(probability) in (int,float) and math.isfinite(probability) and 0 <= probability <= 1
            else:
                assert selected, 'Missing selected evidence.'
        value = ('' if not a else str(int(selected!='none'))) if rule['type']=='choice' else '' if probability is None or probability==threshold else str(int(probability>threshold))
        evidence = lookup.get((request_id,selected),{})
        assert selected in {'','none'} or evidence, 'Selected evidence absent from supplied source.'
        answers.append(dict(document_id=p['document_id'],request_id=request_id,part=p['part'],field=field,value=value,probability_yes=probability,
            answer_status=status if not a else 'threshold_tie' if value=='' else 'answered',vintage=p['vintage'],request_sha256=p['request_sha256'],attempt=r.get('attempt', ''),
            selected_segment_id=selected,source_document_id=evidence.get('source_document_id',''),source_application_number=evidence.get('source_application_number',''),
            pdf_page=evidence.get('pdf_page',''),page_start=evidence.get('page_start',''),page_end=evidence.get('page_end',''),public_pdf_url=evidence.get('public_pdf_url',''),
            evidence_text=evidence.get('text',''),source_text_sha256=evidence.get('source_text_sha256',''),
            evidence_disagrees=int(rule['type']=='noul' and selected!='' and value!='' and (selected!='none')!=(value=='1'))))


def union(values):
    return '1' if '1' in values else '0' if values and all(v=='0' for v in values) else ''


by_doc = defaultdict(list)
for r in answers:
    by_doc[r['document_id']].append(r)
labels = []
for row in roster:
    doc = row['document_id']
    values = {field:union([r['value'] for r in by_doc[doc] if r['field']==field]) for field in book['questions']}
    statuses = [r['status'] for r in grouped[doc]]
    status = 'complete' if statuses and all(s=='successful' for s in statuses) else 'partial' if 'successful' in statuses else 'failed' if 'failed' in statuses else 'outcome_unknown' if 'outcome_unknown' in statuses else 'not_attempted' if statuses else 'no_usable_text'
    result = dict(row,extraction_status=status,observation_vintage='v1_carried' if doc in carried_docs else 'v2',
        successful_parts=statuses.count('successful'),evidence_conflicts=sum(r['evidence_disagrees'] for r in by_doc[doc]),**values)
    result['stage_conflicts'] = sum(values[t+'__discussed']=='0' and '1' in [values[t+'__concern_request'],values[t+'__adopted']] for t in {f.split('__')[0] for f in values if f.endswith('__discussed')})
    for legacy, topics in book['legacy_topics'].items():
        result[legacy+'__concern_request'] = union([values[t+'__concern_request'] for t in topics])
        result[legacy+'__review_issue'] = union([values[t+'__'+stage] for t in topics for stage in ['concern_request','adopted']])
    result['council_involvement'] = union([values[f] for f in values if f.startswith('council__')])
    for actor, positive_fields in [('council',['support','requested_project','requested_provision','motivating_concern']),('civic',['support','request'])]:
        opposition = values[actor+'__opposition']
        positive = union([values[actor+'__'+f] for f in positive_fields])
        result[actor+'member_position' if actor=='council' else 'civic_group_position'] = 'opposition' if opposition=='1' else 'support_or_request' if opposition=='0' and positive=='1' else 'none_or_procedural' if opposition==positive=='0' else ''
    labels.append(result)
# Explicit set aggregation preserves every source without an expanding join.
represented = defaultdict(set)
context = defaultdict(set)
for link in links:
    context[link['source_document_id']].add(link['document_id'])
    if link['represented_application_flag']=='TRUE':
        represented[link['source_document_id']].add(link['document_id'])
label = {r['document_id']:r for r in labels}
coverage = []
for m in manifest:
    docs = sorted(represented[m['document_id']])
    statuses = [label[d]['extraction_status'] for d in docs]
    coverage.append(dict(document_id=m['document_id'],application_number=m['application_number'],corpus_role=m['corpus_role'],source_usable=m['source_usable'],
        represented_narrative_ids=';'.join(docs),context_narrative_ids=';'.join(sorted(context[m['document_id']])),
        coverage_status='no_usable_text' if m['source_usable']!='TRUE' else 'represented' if docs else 'context_only' if context[m['document_id']] else 'unrepresented_source',
        complete_narrative_count=statuses.count('complete'),represented_narrative_count=len(docs)))
save_csv(labels,list(labels[0]),'../output/cpc_jev_labels_v2.csv',['document_id'])
successful_labels = [r for r in labels if r['extraction_status'] == 'complete']
save_csv(successful_labels,list(labels[0]),'../output/cpc_jev_successful_labels_v2.csv',['document_id'])
save_csv(answers,list(answers[0]),'../output/cpc_jev_answers_v2.csv',['request_id','field'])
save_csv(usage,list(usage[0]),'../output/cpc_jev_usage_v2.csv',['request_id'])
save_csv(coverage,list(coverage[0]),'../output/cpc_jev_source_coverage_v2.csv',['document_id'])
print(f"Saved {len(successful_labels)} successful narratives for use; retained {len(labels)} narratives and {len(coverage)} sources for coverage. Labels remain provisional.")
