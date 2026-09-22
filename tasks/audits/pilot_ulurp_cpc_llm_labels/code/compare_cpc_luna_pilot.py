#!/usr/bin/env python3
"""Validate frozen Luna readings and compare with unchanged diagnostic references."""
import csv
import hashlib
import json
import sys
from pathlib import Path
sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

with open('../output/cpc_luna_sample_v1.csv') as f:
    sample = {r['document_id']: r for r in csv.DictReader(f)}
book = json.loads(Path('../input/cpc_luna_codebook_v1.json').read_text())
with open('../input/cpc_jev_comparison_v16.csv') as f:
    previous = [r for r in csv.DictReader(f) if r['document_id'] in sample]
reference = {(r['document_id'], r['field']): r for r in previous}
assert len(reference) == len(previous)
actor_roles = {'applicant', 'community_board', 'council_member', 'council_body', 'cpc', 'independent_civic', 'other', 'unclear'}
statuses = {'concern', 'request', 'applicant_promise', 'adopted_requirement', 'incorporated_change', 'proposal_description',
            'support', 'opposition', 'main_approval', 'no_impact', 'unconfirmed_rule', 'unclear'}
topics = {f.split('__')[0] for f in book['questions']} - {'council', 'civic'}
labels, evidence, comparison = [], [], []
seen = set()
for reader in 'abc':
    packets = json.loads(Path(f'../input/cpc_luna_packets_{reader}_v1.json').read_text())
    readings = [json.loads(s) for s in Path(f'../input/cpc_luna_reader_{reader}_v1.jsonl').read_text().splitlines()]
    packets = {p['document_id']: p for p in packets}
    assert len(readings) == len({r['document_id'] for r in readings})
    assert set(packets) == {r['document_id'] for r in readings}
    for reading in readings:
        doc = reading['document_id']
        assert doc not in seen and sample[doc]['reader'] == reader
        seen.add(doc)
        packet = packets[doc]
        assert hashlib.sha256(json.dumps(packet, sort_keys=True, ensure_ascii=False).encode()).hexdigest() == sample[doc]['packet_sha256']
        pages = {r['segment_id']: r['text'] for r in packet['segments']}
        assert len(pages) == len(packet['segments'])
        assert reading['application_number'] == sample[doc]['application_number']
        assert reading['reader'] == 'luna_medium_' + reader
        assert reading['source_scope'] in {'clear', 'concern'}
        assert len(reading['read_segment_ids']) == len(set(reading['read_segment_ids'])) == len(pages)
        assert set(reading['read_segment_ids']) == set(pages)
        assert set(reading['fields']) == set(book['questions'])
        for field, answer in reading['fields'].items():
            assert answer['value'] in {'1', '0', 'unresolved'}
            assert set(answer['segments']) <= set(pages)
            quote_found = bool(answer['quote']) and any(answer['quote'] in pages[s] for s in answer['segments'])
            row = dict(document_id=doc, application_number=sample[doc]['application_number'], reader=reader,
                field=field, value=answer['value'], segments=';'.join(answer['segments']), quote=answer['quote'],
                reason=answer['reason'], quote_found=int(quote_found), source_scope=reading['source_scope'],
                scope_reason=reading['scope_reason'])
            labels.append(row)
            old = reference.get((doc, field))
            if old:
                comparison.append(dict(row, reference=old['reference'], reference_origin=old['reference_origin'],
                    reference_reason=old['reference_reason'], jev_baseline=old['baseline'],
                    jev_revised=old['revised_verified'], jev_baseline_complete=old['baseline_complete'],
                    jev_revised_complete=old['revised_complete']))
        assert len(reading['evidence_records']) <= 12
        for number, event in enumerate(reading['evidence_records'], 1):
            assert event['actor_role'] in actor_roles and event['status'] in statuses
            assert set(event['topics']) <= topics and set(event['segments']) <= set(pages)
            found = bool(event['quote']) and any(event['quote'] in pages[s] for s in event['segments'])
            evidence.append(dict(document_id=doc, application_number=sample[doc]['application_number'], record=number,
                actor=event['actor'], actor_role=event['actor_role'], status=event['status'], topics=';'.join(event['topics']),
                segments=';'.join(event['segments']), quote=event['quote'], quote_found=int(found), reason=event['reason']))
assert seen == set(sample) and len(labels) == 4 * 36
metrics = []
for group in ['all', 'council', 'civic', 'topics']:
    selected = [r for r in comparison if r['reference'] in {'0', '1'} and
        (group == 'all' or r['field'].split('__')[0] == group or group == 'topics' and r['field'].split('__')[0] in topics)]
    for condition in ['value', 'jev_baseline', 'jev_revised']:
        available = [r for r in selected if condition == 'value' or r[condition + '_complete'] == '1']
        metrics.append(dict(group=group, condition=condition, reference_fields=len(selected), available=len(available),
            service_missing=len(selected)-len(available),
            positive=sum(r['reference'] == '1' for r in available),
            negative=sum(r['reference'] == '0' for r in available),
            true_positive=sum(r['reference'] == r[condition] == '1' for r in available),
            false_positive=sum(r['reference'] == '0' and r[condition] == '1' for r in available),
            false_negative=sum(r['reference'] == '1' and r[condition] == '0' for r in available),
            true_negative=sum(r['reference'] == r[condition] == '0' for r in available),
            unresolved=sum(r[condition] not in {'0', '1'} for r in available)))
save_csv(labels, list(labels[0]), '../output/cpc_luna_labels_v1.csv', ['document_id', 'field'])
save_csv(evidence, list(evidence[0]), '../output/cpc_luna_evidence_v1.csv', ['document_id', 'record'])
save_csv(comparison, list(comparison[0]), '../output/cpc_luna_comparison_v1.csv', ['document_id', 'field'])
save_csv(metrics, list(metrics[0]), '../output/cpc_luna_agreement_v1.csv', ['group', 'condition'])
names = {'value': 'Luna', 'jev_baseline': 'Jev old', 'jev_revised': 'Jev revised'}
lines = ['# Four-report Luna reading pilot', '',
    'Three GPT-6 Luna subagents at medium reasoning read four complete reports and retained companions. They received the unchanged 36-field codebook and no prior answers. These are selected development cases, not a random sample or holdout. References are unchanged earlier AI/manager source readings, not definitive human gold.', '',
    f'All four reports returned: {len(labels)} field judgments and {len(evidence)} attributed evidence records. '
    f'Positive labels with exact supporting quotes: {sum(r["value"] == "1" and r["quote_found"] for r in labels)}/{sum(r["value"] == "1" for r in labels)}. '
    f'Exact evidence-record quotes: {sum(r["quote_found"] for r in evidence)}/{len(evidence)}. '
    f'Unresolved Luna judgments across all fields: {sum(r["value"] == "unresolved" for r in labels)}. '
    f'Unresolved reference fields excluded from scoring: {sum(r["reference"] not in {"0", "1"} for r in comparison)}.', '',
    f'Only {len(comparison)}/{len(labels)} judgments have an existing reference. The unresolved Luna judgment has no reference and is outside the scored set. '
    'Commerce Avenue and ASPCA contribute 36 reference fields each; Sunset Park contributes four and Whitestone Lanes three. '
    'The 79 report-level reference decisions differ from the earlier 20 passage checks.', '',
    'The table scores only available fields. Coverage and source samples differ for Jev because of failed calls; these are not like-for-like overall accuracy rates. Unresolved answers remain in the available denominator. A matching quotation establishes provenance, not correctness of the interpretation.', '',
    '| Reading | Available | Positive hits | False positives | False negatives | Unresolved | Missing |',
    '|---|---:|---:|---:|---:|---:|---:|']
for r in metrics:
    if r['group'] == 'all':
        lines.append(f'| {names[r["condition"]]} | {r["available"]}/{r["reference_fields"]} | {r["true_positive"]}/{r["positive"]} | {r["false_positive"]}/{r["negative"]} | {r["false_negative"]} | {r["unresolved"]} | {r["service_missing"]} |')
lines += ['', 'Restricting each comparison to the same available reference fields gives:', '',
    '| Available Jev condition | Shared fields | Luna matches | Jev matches |', '|---|---:|---:|---:|']
for condition in ['jev_baseline', 'jev_revised']:
    paired = [r for r in comparison if r['reference'] in {'0', '1'} and r[condition + '_complete'] == '1']
    lines.append(f'| {names[condition]} | {len(paired)} | {sum(r["value"] == r["reference"] for r in paired)} | {sum(r[condition] == r["reference"] for r in paired)} |')
lines += ['', 'This compares reading procedures, not an isolated model substitution: Luna reads complete packets and returns quotations; Jev used different question batches, retrieval and verification. The old Jev commitment fields also used broader adopted-change wording. These small, selected comparisons do not establish general superiority.', '',
    'Council fields match 13/13 existing references, but only one is positive. Civic fields match 7/8; Luna misses the explicitly supportive homeowners association in Whitestone Lanes. This is insufficient positive coverage to validate either actor family.']
lines += ['', 'These Codex readings use account usage rather than the Vercel credits. No new Jev/API calls were made, and no production labels were changed. Raw packets, prompt, model request, agent identifiers and readings are preserved; exact rerun determinism is not claimed.', '']
Path('../output/cpc_luna_findings_v1.md').write_text('\n'.join(lines))
print(f'Validated four readings and compared {len(comparison)} reference fields.')
