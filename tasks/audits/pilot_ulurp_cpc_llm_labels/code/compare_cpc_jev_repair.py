#!/usr/bin/env python3
"""Compare fixed sources and revised questions without changing production labels."""
import csv
import hashlib
import json
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

threshold = float(sys.argv[1])
with open('../output/cpc_jev_sample_v12.csv') as f:
    sample = {r['document_id']: r for r in csv.DictReader(f)}
with open('../output/cpc_jev_plan_v12.csv') as f:
    plan = list(csv.DictReader(f))
with open('cpc_jev_repair_expectations_v12.csv') as f:
    references = list(csv.DictReader(f))
with open('../input/cpc_jev_labels_corpus_v2.csv') as f:
    historical = {r['document_id']: r for r in csv.DictReader(f)}
old_answers = defaultdict(list)
with open('../input/cpc_jev_answers_corpus_v2.csv') as f:
    for r in csv.DictReader(f):
        if r['document_id'] in sample:
            old_answers[r['request_sha256']].append(r)
segments = {}
with open('../input/cpc_jev_segments_corpus_v3.csv') as f:
    for r in csv.DictReader(f):
        if r['document_id'] in sample:
            segments[r['document_id'], r['segment_id']] = r
prepared = Path('../output/cpc_jev_requests_v12.jsonl').read_bytes()
assert prepared == Path('../input/cpc_jev_requests_v12.jsonl').read_bytes()
packets = {r['request_id']: r for line in prepared.decode().splitlines() if (r := json.loads(line))}
expected = {r['request_id']: r for r in plan}
assert len(expected) == len(plan)
journal = {}
for line in Path('../input/cpc_jev_responses_v12.jsonl').read_text().splitlines():
    r = json.loads(line)
    assert r['request_sha256'] == expected[r['request_id']]['request_sha256']
    assert r['attempt_number'] in {1, 2}
    key = r['request_id'], r['attempt_number']
    if r['event'] == 'started':
        assert key not in journal
        if r['attempt_number'] == 2:
            previous = journal[r['request_id'], 1]
            assert previous['http_status'] in {502, 503, 504, 529} and not previous['valid_response']
        journal[key] = dict(r, status='unfinished')
    else:
        assert r['event'] == 'received' and journal[key]['status'] == 'unfinished'
        journal[key].update(r, status='successful' if r['valid_response'] else 'failed')
assert all(r['status'] != 'unfinished' for r in journal.values())
attempts = {}
for (request_id, attempt), r in sorted(journal.items()):
    assert attempts.get(request_id, {}).get('status') != 'successful'
    attempts[request_id] = r
books = {condition: json.loads(Path(path).read_text()) for condition, path in [
    ('original', '../input/cpc_jev_codebook_corpus_v1.json'),
    ('candidate', '../input/cpc_jev_codebook_candidate_v2.json')]}
answers, usage = [], []
for p in plan:
    request_id, doc = p['request_id'], p['document_id']
    r = attempts.get(request_id, {})
    reused = p['acquisition'] == 'reuse_saved'
    status = 'reused' if reused else r.get('status', 'not_attempted')
    usage.append(dict(p, status=status, http_status=r.get('http_status', ''), attempt=r.get('attempt_number', '')))
    values = {}
    if reused:
        assert old_answers[p['request_sha256']]
        for a in old_answers[p['request_sha256']]:
            assert a['document_id'] == doc
            values[a['field']] = (a['value'], a['probability_yes'], a['selected_segment_id'])
    elif status == 'successful':
        packet = packets[request_id]
        assert hashlib.sha256(json.dumps(packet['body'], sort_keys=True, ensure_ascii=False).encode()).hexdigest() == p['request_sha256']
        payload = json.loads(r['raw_response'])['answers']
        assert set(payload) == set(packet['body']['questions'])
        for field, rule in books[p['condition']]['questions'].items():
            a = payload[field]
            if rule['type'] == 'noul':
                probability = a['noul']
                value = '' if probability == threshold else str(int(probability > threshold))
                page = payload.get('page__' + field, {}).get('choice', '')
            else:
                probability = ''
                page = a['choice']
                value = str(int(page != 'none'))
            values[field] = value, probability, page
    for field in books[p['condition']]['questions']:
        value, probability, page = values.get(field, ('', '', ''))
        source = segments.get((doc, page), {})
        assert page in {'', 'none'} or source, (doc, page)
        answers.append(dict(document_id=doc, condition=p['condition'], request_id=request_id,
            part=p['part'], field=field, value=value, probability_yes=probability,
            selected_segment_id=page, source_document_id=source.get('source_document_id', ''),
            pdf_page=source.get('pdf_page', ''), evidence_text=source.get('text', ''),
            evidence_conflict=int((value == '1' and page == 'none') or (value == '0' and page not in {'', 'none'})),
            status=status, request_sha256=p['request_sha256']))
completed = {(doc, condition): all(r['status'] in {'successful', 'reused'} for r in usage
             if r['document_id'] == doc and r['condition'] == condition)
             for doc in sample for condition in books}
grouped = defaultdict(list)
for r in answers:
    grouped[r['document_id'], r['condition'], r['field']].append(r['value'])
labels = {}
for key, values in grouped.items():
    labels[key] = ('' if not completed[key[:2]] else '1' if '1' in values
                   else '0' if all(v == '0' for v in values) else '')
for reader in 'ab':
    for line in Path(f'../input/cpc_jev_repair_reference_{reader}.jsonl').read_text().splitlines():
        r = json.loads(line)
        assert sample[r['document_id']]['test_group'] == 'fresh'
        assert r['field'] in books['candidate']['questions']
        assert str(r['value']) in {'0', '1', 'unclear'}
        for e in r['evidence']:
            text = ' '.join(s['text'] for s in segments.values() if s['document_id'] == r['document_id']
                            and s['source_document_id'] == e['source_document_id'] and s['pdf_page'] == str(e['pdf_page']))
            assert ' '.join(e['quote'].split()) in ' '.join(text.split()), (r['document_id'], r['field'])
        references.append(dict(document_id=r['document_id'], application_number=sample[r['document_id']]['application_number'],
            test_group='fresh', field=r['field'], value=str(r['value']), reason=r['reason'],
            evidence_json=json.dumps(r['evidence'], ensure_ascii=False), reviewer=r['reader']))
assert len(references) == len({(r['document_id'], r['field']) for r in references})
assert sum(r['test_group'] == 'fresh' for r in references) == 4 * 33
comparison = []
for r in references:
    doc, field = r['document_id'], r['field']
    row = dict(r, historical=historical[doc].get(field, ''),
        corrected_original=labels.get((doc, 'original', field), ''),
        corrected_candidate=labels.get((doc, 'candidate', field), ''),
        original_complete=int(completed[doc, 'original']), candidate_complete=int(completed[doc, 'candidate']))
    for name in ('historical', 'corrected_original', 'corrected_candidate'):
        row[name + '_match'] = int(row[name] == r['value']) if row[name] in {'0', '1'} and r['value'] in {'0', '1'} else ''
    comparison.append(row)
metrics = []
for group in ('known_error', 'positive_control', 'fresh'):
    rows = [r for r in comparison if r['test_group'] == group and r['field'] in books['original']['questions']]
    paired = [r for r in rows if all(r[k + '_match'] != '' for k in ('historical', 'corrected_original', 'corrected_candidate'))]
    for condition in ('historical', 'corrected_original', 'corrected_candidate'):
        answered = [r for r in rows if r[condition + '_match'] != '']
        metrics.append(dict(test_group=group, condition=condition, common_fields=len(rows),
            available=len(answered), matches=sum(r[condition + '_match'] for r in answered),
            paired_available=len(paired), paired_matches=sum(r[condition + '_match'] for r in paired),
            paired_positive=sum(r['value'] == '1' for r in paired),
            paired_positive_found=sum(r['value'] == r[condition] == '1' for r in paired),
            paired_wrong_positive=sum(r['value'] == '0' and r[condition] == '1' for r in paired),
            paired_wrong_negative=sum(r['value'] == '1' and r[condition] == '0' for r in paired)))
save_csv(answers, list(answers[0]), '../output/cpc_jev_answers_v12.csv', ['request_id', 'field'])
save_csv(usage, list(usage[0]), '../output/cpc_jev_usage_v12.csv', ['request_id'])
save_csv(comparison, list(comparison[0]), '../output/cpc_jev_comparison_v12.csv', ['document_id', 'field'])
save_csv(metrics, list(metrics[0]), '../output/cpc_jev_agreement_v12.csv', ['test_group', 'condition'])
credits = [json.loads(line) for line in Path('../input/cpc_jev_credits_v12.jsonl').read_text().splitlines()]
assert credits[-1]['phase'] == 'after'
spent = Decimal(str(credits[-1]['total_used'])) - Decimal(str(credits[0]['total_used']))
lines = ['# Jev after source and question repair', '',
    'Eleven reports: seven selected development cases and four fresh reports selected by a fixed seed from completed reports with resolved source scope and at most two parts. The fresh reports exclude the human-coded and prior source-audit reports; two Sol-medium readers coded them without seeing Jev answers. These are fallible AI reference judgments, not human gold labels or population accuracy.', '',
    'Historical means the saved old run. Corrected original uses repaired text and the original questions. Corrected candidate changes only the questions on that same repaired text. Exactly matching successful requests are reused. Successful answers are never repeated; received server failures may receive one recovery attempt. Reports with a failed part are unavailable in the comparison. Exact threshold ties remain missing.', '',
    f'{sum(r["status"] == "successful" for r in usage)} new requests succeeded, {sum(r["status"] == "failed" for r in usage)} failed, and {sum(r["status"] == "not_attempted" for r in usage)} remain unattempted; {sum(r["status"] == "reused" for r in usage)} were reused. Observed account usage increased by ${spent}; final balance ${credits[-1]["balance"]}.', '',
    '| Sample | Condition | Available matches | Same-field paired matches | Positives found, paired | False positives, paired |',
    '|---|---|---:|---:|---:|---:|']
for r in metrics:
    lines.append(f'| {r["test_group"].replace("_", " ")} | {r["condition"].replace("_", " ")} | {r["matches"]}/{r["available"]} | {r["paired_matches"]}/{r["paired_available"]} | {r["paired_positive_found"]}/{r["paired_positive"]} | {r["paired_wrong_positive"]} |')
lines += ['', 'The new Council review-concern question is excluded from the common-field totals because it was not asked previously. It is reported separately below. Field outcomes within a report are correlated, and the sample is small.', '',
    '| Development case | Field | Reference | Historical | Corrected original | Corrected candidate |', '|---|---|---:|---:|---:|---:|']
for r in comparison:
    if r['test_group'] == 'fresh':
        continue
    lines.append('| ' + ' | '.join(str(r[k]).replace('_', ' ') or 'unavailable' for k in ('application_number', 'field', 'value', 'historical', 'corrected_original', 'corrected_candidate')) + ' |')
lines += ['', 'Full answers, supporting pages, disagreements, unavailable outcomes and source references are retained in the adjacent CSVs. This test does not restart bulk acquisition or replace production labels.', '']
Path('../output/cpc_jev_findings_v12.md').write_text('\n'.join(lines))
print(f'Compared {len(comparison)} reference fields; observed spend ${spent}. Bulk acquisition remains paused.')
