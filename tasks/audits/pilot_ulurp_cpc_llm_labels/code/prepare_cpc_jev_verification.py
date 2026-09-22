#!/usr/bin/env python3
"""Apply the frozen routing rule to distinct, focused evidence-window questions."""
import csv
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path
sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

book = json.loads(Path('cpc_jev_codebook_v13.json').read_text())
version = sys.argv[1] if len(sys.argv) == 2 else 'v13'
assert version in {'v13', 'v14', 'v15', 'v16'}
if version == 'v14':
    # Correct the transmitted definition on the identical nine frozen windows.
    requests = []
    for line in Path('../input/cpc_jev_requests_v13_verify.jsonl').read_text().splitlines():
        packet = json.loads(line)
        original_sha = packet['request_sha256']
        assert hashlib.sha256(json.dumps(packet['body'], sort_keys=True, ensure_ascii=False).encode()).hexdigest() == original_sha
        for name, question in packet['body']['questions'].items():
            field = name.removeprefix('basis__')
            rule = book['questions'][field]
            assert rule['instructions'] in question['instructions']
            question['instructions'] += '\nOriginal substantive coding rules defining this predicate:\n' + '\n'.join(
                answer + ': ' + definition for answer, definition in rule['criteria'].items())
            assert all(definition in question['instructions'] for definition in rule['criteria'].values())
        packet['request_sha256'] = hashlib.sha256(json.dumps(packet['body'], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        assert packet['request_sha256'] != original_sha
        packet['prior_request_sha256'] = original_sha
        requests.append(packet)
    assert len(requests) == 9 and len({p['request_sha256'] for p in requests}) == 9
    Path('../output/cpc_jev_requests_v14_verify.jsonl').write_text(''.join(json.dumps(p, ensure_ascii=False) + '\n' for p in requests))
    print('Prepared nine corrected evidence requests; same windows, fields, screening answers and references. No inference.')
    sys.exit(0)
baseline = json.loads(Path('../input/cpc_jev_codebook_candidate_v2.json').read_text())
rules = json.loads(Path('cpc_jev_verification_v13.json').read_text())
packets = [json.loads(s) for s in Path(f'../output/cpc_jev_all_requests_{version}.jsonl').read_text().splitlines()]
assert Path(f'../output/cpc_jev_all_requests_{version}.jsonl').read_bytes() == Path(f'../input/cpc_jev_frozen_plan_{version}.jsonl').read_bytes()
with open('../output/cpc_jev_segments_v13.csv') as f:
    segments = list(csv.DictReader(f))
lookup = {(r['document_id'], r['segment_id']): r for r in segments}
sources = defaultdict(list)
for r in segments:
    sources[r['document_id'], r['source_document_id']].append(r)
for rows in sources.values():
    rows.sort(key=lambda r: (int(r['pdf_page']), int(r['page_start'])))
latest, pending = {}, set()
for line in Path(f'../input/cpc_jev_responses_{version}.jsonl').read_text().splitlines():
    r = json.loads(line)
    key = r['request_id'], r['attempt_number']
    if r['event'] == 'started':
        assert key not in pending
        assert not latest.get(r['request_id'], {}).get('valid_response')
        pending.add(key)
    else:
        assert key in pending
        pending.remove(key)
        latest[r['request_id']] = r
assert not pending, 'Wait for screening acquisition to finish.'
answers, usage, windows = [], [], {}
for p in packets:
    doc, request_id, condition = p['document_id'], p['request_id'], p['condition']
    r = p['reused_response'] or latest.get(request_id)
    assert r is not None, 'Every screening request must receive its scheduled attempt before verification is prepared.'
    assert r['request_sha256'] == p['request_sha256']
    valid = r['valid_response']
    response = json.loads(r['raw_response'])['answers'] if valid else {}
    fields = book['questions'] if condition == 'revised' else baseline['questions']
    usage.append(dict(document_id=doc, request_id=request_id, condition=condition,
        part=p['body']['state']['report']['part'], parts=p['body']['state']['report']['parts'],
        status='reused' if p['reused_response'] else 'successful' if valid else 'failed',
        request_sha256=p['request_sha256'], http_status=r['http_status'], attempt=r['attempt_number']))
    for field, rule in fields.items():
        answer = response.get(field, {})
        selected = response.get('page__' + field, {}).get('choice', '')
        probability, value, state = '', '', 'service_missing'
        if valid and rule['type'] == 'noul':
            probability = answer['noul']
            value = '' if probability == .5 else str(int(probability > .5))
            state = 'threshold_tie' if value == '' else 'answered'
        elif valid and condition == 'baseline':
            selected = answer['choice']
            value, state = str(int(selected != 'none')), 'answered'
        elif valid:
            state = answer['choice']
            value = {'yes': '1', 'no': '0'}.get(state, '')
        source = lookup.get((doc, selected), {})
        assert selected in {'', 'none', 'ambiguous', 'context_missing'} or source
        conflict = bool(valid and selected and value and (value == '1') != bool(source))
        route = (condition == 'revised' and field in book['target_fields'] and valid and
                 (value == '1' or conflict or value == '' or selected in {'ambiguous', 'context_missing'}))
        verification_id = ''
        if route and source:
            same_source = sources[doc, source['source_document_id']]
            i = next(i for i, s in enumerate(same_source) if s['segment_id'] == selected)
            context = same_source[max(0, i-1):i+2]
            window_key = doc, tuple(s['segment_id'] for s in context)
            if window_key not in windows:
                digest = hashlib.sha256('|'.join(window_key[1]).encode()).hexdigest()[:10]
                windows[window_key] = dict(request_id=f'verify__{doc}__{digest}', document_id=doc,
                    report=p['body']['state']['report'], context=context, fields=set())
            window = windows[window_key]
            window['fields'].add(field)
            verification_id = window['request_id']
        answers.append(dict(document_id=doc, condition=condition, request_id=request_id,
            part=p['body']['state']['report']['part'], field=field, value=value, answer_state=state,
            probability_yes=probability, selected_segment_id=selected, evidence_conflict=int(conflict),
            verification_required=int(route), verification_request_id=verification_id,
            source_document_id=source.get('source_document_id', ''), pdf_page=source.get('pdf_page', ''),
            evidence_text=source.get('text', ''), status='successful' if valid else 'failed'))
requests, index = [], []
for window in windows.values():
    questions = {}
    for field in sorted(window['fields']):
        # Historical v13 omitted the original answer criteria; preserve for replay, not production reuse.
        questions[field] = dict(type='choice', instructions=rules['instructions'] + ' ' + book['questions'][field]['instructions'], criteria=rules['criteria'])
        if field.endswith('__specific_obligation'):
            questions['basis__' + field] = dict(type='choice', instructions=rules['basis_instructions'] + ' ' + book['questions'][field]['instructions'], criteria=rules['basis_criteria'])
        if version in {'v15', 'v16'}:
            definitions = '\nOriginal substantive coding rules defining this predicate:\n' + '\n'.join(
                answer + ': ' + definition for answer, definition in book['questions'][field]['criteria'].items())
            questions[field]['instructions'] += definitions
            if field.endswith('__specific_obligation'):
                questions['basis__' + field]['instructions'] += definitions
            assert all(d in questions[field]['instructions'] for d in book['questions'][field]['criteria'].values())
        index.append(dict(verification_request_id=window['request_id'], document_id=window['document_id'],
            field=field, segment_ids=';'.join(s['segment_id'] for s in window['context']),
            source_document_id=window['context'][0]['source_document_id'],
            page_first=window['context'][0]['pdf_page'], page_last=window['context'][-1]['pdf_page']))
    report = {k: v for k, v in window['report'].items() if k != 'text'}
    report['text'] = '\n\n'.join(f"Source {s['source_application_number']}, page {s['pdf_page']}, segment {s['segment_id']}\n{s['text']}" for s in window['context'])
    report['context_scope'] = 'Selected evidence and adjacent retained segments from the same PDF. This is not the full report; missing references must stay unresolved.'
    body = dict(model='typesafe-ai/jev', state=dict(report=report), questions=questions)
    requests.append(dict(request_id=window['request_id'], document_id=window['document_id'],
        request_sha256=hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest(), body=body))
assert len(requests) <= 200, 'Frozen verification request cap exceeded; inspect without silently dropping windows.'
assert len({p['request_sha256'] for p in requests}) == len(requests)
if version in {'v15', 'v16'}:
    saved = {}
    for line in Path('../input/cpc_jev_responses_v14_verify.jsonl').read_text().splitlines():
        r = json.loads(line)
        if r['event'] == 'received' and r['valid_response']:
            assert r['request_sha256'] not in saved
            saved[r['request_sha256']] = r
    assert len(saved) == 9
    for p in requests:
        p['reused_response'] = saved.get(p['request_sha256'])
    Path(f'../output/cpc_jev_all_requests_{version}_verify.jsonl').write_text(''.join(json.dumps(p, ensure_ascii=False) + '\n' for p in requests))
    print(f'Reused {sum(bool(p["reused_response"]) for p in requests)} exact corrected verification successes.')
    requests = [{k: v for k, v in p.items() if k != 'reused_response'} for p in requests if not p['reused_response']]
Path(f'../output/cpc_jev_requests_{version}_verify.jsonl').write_text(''.join(json.dumps(p, ensure_ascii=False) + '\n' for p in requests))
save_csv(answers, list(answers[0]), f'../output/cpc_jev_screening_{version}.csv', ['request_id', 'field'])
save_csv(usage, list(usage[0]), f'../output/cpc_jev_usage_{version}.csv', ['request_id'])
save_csv(index, ['verification_request_id', 'document_id', 'field', 'segment_ids', 'source_document_id', 'page_first', 'page_last'], f'../output/cpc_jev_verification_index_{version}.csv', ['verification_request_id', 'field'])
print(f'Prepared {len(requests)} new focused requests covering {len(index)} total predicates; no inference.')
