#!/usr/bin/env python3
"""Compare frozen source readings with screening and evidence-verified results."""
import csv
import hashlib
import json
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

version = sys.argv[1] if len(sys.argv) == 2 else 'v13'
assert version in {'v13', 'v14', 'v15', 'v16'}
screen_version = version if version in {'v15', 'v16'} else 'v13'
book = json.loads(Path('cpc_jev_codebook_v13.json').read_text())
with open('../output/cpc_jev_sample_v13.csv') as f:
    sample = {r['document_id']: r for r in csv.DictReader(f)}
with open(f'../output/cpc_jev_screening_{screen_version}.csv') as f:
    screening = list(csv.DictReader(f))
with open(f'../output/cpc_jev_usage_{screen_version}.csv') as f:
    usage = list(csv.DictReader(f))
with open(f'../output/cpc_jev_verification_index_{screen_version}.csv') as f:
    index = list(csv.DictReader(f))
with open('../output/cpc_jev_segments_v13.csv') as f:
    source = {(r['document_id'], r['segment_id']): r for r in csv.DictReader(f)}
with open('../input/cpc_jev_control_expectations_v13.csv') as f:
    references = list(csv.DictReader(f))
evidence_readings = [json.loads(s) for s in Path('../input/cpc_jev_evidence_reference_v13.jsonl').read_text().splitlines()]
evidence_lookup = {(r['verification_request_id'], r['field']): r for r in evidence_readings}
assert len(evidence_lookup) == len(evidence_readings)
audit_index = index
if version in {'v15', 'v16'}:
    with open('../output/cpc_jev_verification_index_v13.csv') as f:
        audit_index = list(csv.DictReader(f))
selected_evidence = sorted(audit_index, key=lambda r: hashlib.sha256(
    ('jev-v13-window-audit-20260922|' + r['verification_request_id'] + '|' + r['field']).encode()).hexdigest())[:20]
assert set(evidence_lookup) == {(r['verification_request_id'], r['field']) for r in selected_evidence}
for row in selected_evidence:
    r = evidence_lookup[row['verification_request_id'], row['field']]
    assert r['answer'] in {'established', 'not_established', 'ambiguous', 'context_missing'}
    assert r['basis'] in {'', 'applicant_undertaking', 'imposed_condition', 'both', 'neither', 'unclear'}
    assert not r['quote'] or any(r['quote'] in source[row['document_id'], s]['text'] for s in row['segment_ids'].split(';'))
readings = defaultdict(list)
for reader in 'abc':
    for line in Path(f'../input/cpc_jev_reference_{reader}_v13.jsonl').read_text().splitlines():
        r = json.loads(line)
        assert sample[r['document_id']]['test_group'] == 'fresh'
        assert set(r['fields']) == set(book['questions'])
        for field, answer in r['fields'].items():
            assert answer['value'] in {'1', '0', 'unresolved'}
            assert all((r['document_id'], s) in source for s in answer['segments'])
            assert not answer['quote'] or any(answer['quote'] in source[r['document_id'], s]['text'] for s in answer['segments'])
            readings[r['document_id'], field].append(dict(answer, reader=reader, source_scope=r['source_scope']))
assert len(readings) == 10 * len(book['questions'])
for (doc, field), rows in readings.items():
    values = {r['value'] for r in rows}
    value = next(iter(values)) if len(values) == 1 and all(r['source_scope'] == 'clear' for r in rows) else 'unresolved'
    references.append(dict(document_id=doc, application_number=sample[doc]['application_number'], field=field,
        value=value, segments=';'.join(sorted({s for r in rows for s in r['segments']})),
        reason=' | '.join(r['reader'] + ': ' + r['reason'] for r in rows),
        reference_origin='blinded_sol_medium_consensus' if len(rows) > 1 else 'blinded_sol_medium_single'))
assert len({(r['document_id'], r['field']) for r in references}) == len(references)

verified = {}
pending = set()
for line in Path(f'../input/cpc_jev_responses_{version}_verify.jsonl').read_text().splitlines():
    r = json.loads(line)
    key = r['request_id'], r['attempt_number']
    if r['event'] == 'started':
        assert not verified.get(r['request_id'], {}).get('valid_response')
        pending.add(key)
    else:
        assert key in pending
        pending.remove(key)
        verified[r['request_id']] = r
assert not pending
assert Path(f'../output/cpc_jev_requests_{version}_verify.jsonl').read_bytes() == Path(f'../input/cpc_jev_requests_{version}_verify.jsonl').read_bytes()
verification_requests = {r['request_id']: r for s in Path(f'../output/cpc_jev_requests_{version}_verify.jsonl').read_text().splitlines() if (r := json.loads(s))}
if version in {'v15', 'v16'}:
    all_requests = [json.loads(s) for s in Path(f'../output/cpc_jev_all_requests_{version}_verify.jsonl').read_text().splitlines()]
    for p in all_requests:
        if p['reused_response']:
            assert p['request_id'] not in verified
            assert p['reused_response']['request_sha256'] == p['request_sha256']
            verified[p['request_id']] = p['reused_response']
    verification_requests = {p['request_id']: p for p in all_requests}
assert set(verified) == set(verification_requests), 'Every scheduled verification must finish before comparison.'
prior_checks = {}
if version == 'v14':
    with open('../output/cpc_jev_verification_v13.csv') as f:
        prior_checks = {(r['verification_request_id'], r['field']): r for r in csv.DictReader(f)}
checks, check_lookup = [], {}
for row in index:
    request_id, field = row['verification_request_id'], row['field']
    r = verified.get(request_id, {})
    if r:
        assert r['request_sha256'] == verification_requests[request_id]['request_sha256']
    valid = r.get('valid_response', False)
    payload = json.loads(r['raw_response'])['answers'] if valid else {}
    result = payload.get(field, {}).get('choice', '')
    basis = payload.get('basis__' + field, {}).get('choice', '')
    value = '1' if result == 'established' else '0' if result == 'not_established' else ''
    if field.endswith('__specific_obligation') and value == '1' and basis not in {'applicant_undertaking', 'imposed_condition', 'both'}:
        value = ''
    evidence = evidence_lookup.get((request_id, field), {})
    check = dict(row, status='successful' if valid else 'failed' if r else 'not_attempted',
        answer=result, basis=basis, value=value, http_status=r.get('http_status', ''), attempt=r.get('attempt_number', ''),
        evidence_reference=evidence.get('answer', ''), evidence_reference_basis=evidence.get('basis', ''),
        evidence_reference_quote=evidence.get('quote', ''), evidence_reference_reason=evidence.get('reason', ''))
    if version == 'v14':
        old = prior_checks[request_id, field]
        check.update(prior_answer=old['answer'], prior_basis=old['basis'], prior_value=old['value'])
    checks.append(check)
    check_lookup[request_id, field] = check

final_parts = []
for row in screening:
    if row['condition'] != 'revised':
        continue
    value, reason = row['value'], row['answer_state']
    verification_complete = True
    if row['verification_required'] == '1':
        check = check_lookup.get((row['verification_request_id'], row['field']))
        if check is None:
            value, reason = '', 'no_evidence_anchor'
        elif check['status'] != 'successful':
            value, reason, verification_complete = '', 'verification_service_missing', False
        elif check['value'] == '1':
            value, reason = '1', 'evidence_established'
        elif check['value'] == '0' and row['value'] == '0' and row['selected_segment_id'] not in {'ambiguous', 'context_missing'}:
            value, reason = '0', 'negative_with_rejected_anchor'
        else:
            value, reason = '', 'rejected_positive_anchor' if check['value'] == '0' else 'verification_unresolved'
    final_parts.append(dict(row, final_value=value, final_reason=reason, verification_complete=int(verification_complete)))


def union(values):
    return '1' if '1' in values else '0' if values and all(v == '0' for v in values) else ''


complete = {(doc, condition): sample[doc]['preparation_status'] == 'ready' and
            len(rows := [r for r in usage if r['document_id'] == doc and r['condition'] == condition]) == int(sample[doc]['request_count']) and
            all(r['status'] in {'successful', 'reused'} for r in rows)
            for doc in sample for condition in ('baseline', 'revised')}
raw = defaultdict(list)
final = defaultdict(list)
for r in screening:
    raw[r['document_id'], r['condition'], r['field']].append(r['value'])
for r in final_parts:
    final[r['document_id'], r['field']].append(r)
labels = []
lookup = {}
for doc, report in sample.items():
    for field, rule in book['questions'].items():
        old_field = rule['baseline_field']
        baseline_value = union(raw[doc, 'baseline', old_field]) if old_field and complete[doc, 'baseline'] else ''
        screen_value = union(raw[doc, 'revised', field]) if complete[doc, 'revised'] else ''
        final_value = union([r['final_value'] for r in final[doc, field]]) if complete[doc, 'revised'] else ''
        row = dict(document_id=doc, application_number=report['application_number'], test_group=report['test_group'], stratum=report['stratum'],
            field=field, baseline_field=old_field, baseline=baseline_value, revised_screen=screen_value, revised_verified=final_value,
            baseline_complete=int(complete[doc, 'baseline']), revised_complete=int(complete[doc, 'revised']),
            verification_complete=int(all(r['verification_complete'] for r in final[doc, field])),
            target=int(field in book['target_fields']),
            definition_comparison='new_goal' if not old_field else 'changed_construct' if field.endswith('__specific_obligation') else
                'clarified_definition' if field in book['target_fields'] else 'unchanged')
        labels.append(row)
        lookup[doc, field] = row
comparison = []
for r in references:
    row = lookup[r['document_id'], r['field']]
    comparison.append(dict(row, reference=r['value'], reference_segments=r['segments'], reference_reason=r['reason'],
        reference_origin=r['reference_origin'],
        paired_eligible=int(row['baseline_complete'] and row['revised_complete'] and row['verification_complete'] and r['value'] in {'0', '1'}),
        paired_change='unchanged' if row['baseline'] == row['revised_verified'] else
            'became_unresolved' if row['revised_verified'] == '' else
            'corrected' if row['revised_verified'] == r['value'] else
            'new_error' if row['baseline'] == r['value'] else 'other_change',
        reader_disagreement=int(len({a['value'] for a in readings.get((r['document_id'], r['field']), [])}) > 1)))
metrics = []
for group in ('control', 'fresh'):
    for stratum in ('short', 'long', 'all'):
        for family in ('displacement', 'preservation', 'obligations', 'council', 'civic', 'all_targets'):
            rows = [r for r in comparison if r['test_group'] == group and (stratum == 'all' or r['stratum'] == stratum) and r['target'] and
                    (family == 'all_targets' or
                     family == 'obligations' and r['field'].endswith('__specific_obligation') or
                     family == 'displacement' and r['field'].startswith('displacement__') or
                     family == 'preservation' and r['field'].startswith(('neighborhood_character__', 'scale_density_design__', 'historic_preservation__')) or
                     r['field'].startswith(family + '__'))]
            for condition in ('baseline', 'revised_screen', 'revised_verified'):
                for comparison_type in ('mapped_existing', 'new_goals'):
                    for analysis_set in ('paired', 'available', 'end_to_end'):
                        eligible = [r for r in rows if r['reference'] in {'0', '1'} and
                                    (analysis_set == 'end_to_end' or
                                     analysis_set == 'paired' and r['paired_eligible'] or
                                     analysis_set == 'available' and (r['baseline_complete'] if condition == 'baseline' else
                                         r['revised_complete'] and (condition != 'revised_verified' or r['verification_complete']))) and
                                    bool(r['baseline_field']) == (comparison_type == 'mapped_existing')]
                        if not eligible or (condition == 'baseline' and comparison_type == 'new_goals'):
                            continue
                        service_missing = [r for r in eligible if r[condition] == '' and
                            (not r['baseline_complete'] if condition == 'baseline' else
                             not r['revised_complete'] or (condition == 'revised_verified' and not r['verification_complete']))]
                        tp = sum(r['reference'] == r[condition] == '1' for r in eligible)
                        fp = sum(r['reference'] == '0' and r[condition] == '1' for r in eligible)
                        positives = sum(r['reference'] == '1' for r in eligible)
                        metrics.append(dict(test_group=group, stratum=stratum, family=family, comparison_type=comparison_type,
                            condition=condition, analysis_set=analysis_set, eligible=len(eligible), reference_positive=positives,
                            reference_negative=sum(r['reference'] == '0' for r in eligible), true_positive=tp, false_positive=fp,
                            false_negative=sum(r['reference'] == '1' and r[condition] == '0' for r in eligible),
                            true_negative=sum(r['reference'] == r[condition] == '0' for r in eligible),
                            unresolved_positive=sum(r['reference'] == '1' and r[condition] == '' for r in eligible),
                            unresolved_negative=sum(r['reference'] == '0' and r[condition] == '' for r in eligible),
                            service_missing_positive=sum(r['reference'] == '1' for r in service_missing),
                            service_missing_negative=sum(r['reference'] == '0' for r in service_missing),
                            precision=tp / (tp + fp) if tp + fp else '', positive_yield=tp / positives if positives else '',
                            resolved=sum(r[condition] in {'0', '1'} for r in eligible)))
coverage = []
for doc, row in sample.items():
    doc_checks = [r for r in checks if r['document_id'] == doc]
    coverage.append(dict(row, baseline_complete=int(complete[doc, 'baseline']), revised_complete=int(complete[doc, 'revised']),
        verification_requests=len({r['verification_request_id'] for r in doc_checks}),
        verification_failed_requests=len({r['verification_request_id'] for r in doc_checks if r['status'] != 'successful'}),
        reference_fields=sum(r['document_id'] == doc for r in comparison),
        unresolved_reference_fields=sum(r['document_id'] == doc and r['reference'] == 'unresolved' for r in comparison)))
assert set(evidence_lookup) <= set(check_lookup)
save_csv(checks, ['verification_request_id', 'document_id', 'field', 'segment_ids', 'source_document_id', 'page_first', 'page_last', 'status', 'answer', 'basis', 'value', 'http_status', 'attempt', 'evidence_reference', 'evidence_reference_basis', 'evidence_reference_quote', 'evidence_reference_reason'] + (['prior_answer', 'prior_basis', 'prior_value'] if version == 'v14' else []), f'../output/cpc_jev_verification_{version}.csv', ['verification_request_id', 'field'])
save_csv(final_parts, list(final_parts[0]), f'../output/cpc_jev_verified_parts_{version}.csv', ['request_id', 'field'])
save_csv(labels, list(labels[0]), f'../output/cpc_jev_labels_{version}.csv', ['document_id', 'field'])
save_csv(comparison, list(comparison[0]), f'../output/cpc_jev_comparison_{version}.csv', ['document_id', 'field'])
metric_fields = ['test_group', 'stratum', 'family', 'comparison_type', 'condition', 'analysis_set', 'eligible', 'reference_positive', 'reference_negative', 'true_positive', 'false_positive', 'false_negative', 'true_negative', 'unresolved_positive', 'unresolved_negative', 'service_missing_positive', 'service_missing_negative', 'precision', 'positive_yield', 'resolved']
save_csv(metrics, metric_fields, f'../output/cpc_jev_agreement_{version}.csv', metric_fields[:6])
save_csv(coverage, list(coverage[0]), f'../output/cpc_jev_coverage_{version}.csv', ['document_id'])
credits = [json.loads(s) for s in Path(f'../input/cpc_jev_credits_{screen_version}.jsonl').read_text().splitlines()]
verification_credits = [json.loads(s) for s in Path(f'../input/cpc_jev_credits_{version}_verify.jsonl').read_text().splitlines()]
assert credits[-1]['phase'] == verification_credits[-1]['phase'] == 'after'
spent = Decimal(str(verification_credits[-1]['total_used'])) - Decimal(str((verification_credits if version == 'v14' else credits)[0]['total_used']))
condition_names = {'baseline': 'Baseline', 'revised_screen': 'Revised', 'revised_verified': 'With check'}
endpoint_names = {'mapped_existing': 'Existing', 'new_goals': 'New goals'}
recovery_note = []
if version in {'v15', 'v16'}:
    recovery_note = [f'Screening recovery: {sum(r["status"] == "reused" for r in usage)} prior successes reused; '
        f'{sum(r["status"] == "successful" for r in usage)}/{sum(r["status"] != "reused" for r in usage)} additional requests succeeded; '
        f'{sum(r["status"] == "failed" for r in usage)} remain failed. '
        f'Evidence verification reused {sum(bool(p["reused_response"]) for p in verification_requests.values())} exact corrected successes '
        f'and required {sum(not p["reused_response"] for p in verification_requests.values())} new requests.', '']
lines = ['# Frozen Jev comparison', '',
    ('Implementation limitation discovered after comparison: the v13 verifier transmitted each field\'s instructions but omitted its original answer criteria. Those criteria contain substantive exclusions, including Community Boards and government agencies from independent civic organizations. The archived requests are preserved. Verifier results describe this defective implementation; they do not fairly evaluate the fully intended predicate. No production adoption is supported.' if version == 'v13' else 'Corrected v14 verification transmits each original substantive answer criterion in addition to its instruction text. Only those missing rules change: the nine source windows, question batches, screening answers and frozen source readings are identical. This is a repair test on previously inspected material, not new holdout validation. The twenty-report sample stays fixed; unavailable screening reports remain unavailable.' if version == 'v14' else f'Recovery {version} retains the exact original twenty reports and screening questions. It reuses all original successes and adds one new attempt on each prior screening server failure. Evidence checks use the complete v14 rules and reuse exact successful v14 requests. This run changes availability; the separate v14 comparison isolates the prompt-construction repair. No reference labels are changed.'), '',
    'Twenty fixed reports: ten selected controls and ten fresh reports (five short, five long). Fresh references are blinded Sol-medium readings; two reports have independent second readings. Disagreements between readers remain unresolved. These are fallible source readings, not human gold or a population accuracy estimate.', '',
    'Baseline uses the prior candidate questions on identical repaired sources. Revised screening narrows definitions and separates obligation existence from page selection. Revised verified applies one prespecified evidence-window check. Original human labels and production Jev labels are unchanged. Main-proposal approval is separate from a specific undertaking; new preservation-goal fields have no old equivalent.', '',
    f'Complete screening reports: baseline {sum(r["baseline_complete"] for r in coverage)}/20; revised {sum(r["revised_complete"] for r in coverage)}/20. '
    f'Focused verification: {sum(r["valid_response"] for r in verified.values())}/{len(verification_requests)} requests succeeded. '
    f'Observed spend ${spent}; remaining account balance ${verification_credits[-1]["balance"]}.', '',
    *recovery_note,
    'Paired rows below require all screening parts in both conditions and required verification calls to succeed, with a determinate reference. Semantic unresolved answers stay in the positive/negative denominators. Service failures remain separately reported missing observations. The mapped existing totals include explicitly changed obligation definitions and therefore measure performance against the revised target, not unchanged-construct accuracy.', '',
    '| Sample | Endpoint set | Condition | Positive hits / reference positives | False positives / reference negatives | Resolved / eligible |',
    '|---|---|---|---:|---:|---:|']
for r in metrics:
    if r['stratum'] == 'all' and r['family'] == 'all_targets' and r['analysis_set'] == 'paired':
        lines.append(f'| {r["test_group"]} | {endpoint_names[r["comparison_type"]]} | {condition_names[r["condition"]]} | {r["true_positive"]}/{r["reference_positive"]} | {r["false_positive"]}/{r["reference_negative"]} | {r["resolved"]}/{r["eligible"]} |')
lines += ['', 'Available-case accuracy uses each condition\'s completed reports and field checks separately. The reports can differ across conditions, so these rows do not establish a before/after improvement.', '',
    '| Sample | Endpoint set | Condition | Positive hits / available positives | False positives / available negatives | Resolved / eligible |',
    '|---|---|---|---:|---:|---:|']
for r in metrics:
    if r['stratum'] == 'all' and r['family'] == 'all_targets' and r['analysis_set'] == 'available':
        lines.append(f'| {r["test_group"]} | {endpoint_names[r["comparison_type"]]} | {condition_names[r["condition"]]} | {r["true_positive"]}/{r["reference_positive"]} | {r["false_positive"]}/{r["reference_negative"]} | {r["resolved"]}/{r["eligible"]} |')
lines += ['', 'End-to-end yield retains every determinate reference in the fixed sample, including reports with server failures. Unresolved includes service missing; the separate service column identifies that component. These denominators describe this deliberately mixed test sample, not population rates.', '',
    '| Sample | Endpoint set | Condition | Positive hits / all positives | False positives | Unresolved / all fields | Service missing |',
    '|---|---|---|---:|---:|---:|---:|']
for r in metrics:
    if r['stratum'] == 'all' and r['family'] == 'all_targets' and r['analysis_set'] == 'end_to_end':
        lines.append(f'| {r["test_group"]} | {endpoint_names[r["comparison_type"]]} | {condition_names[r["condition"]]} | {r["true_positive"]}/{r["reference_positive"]} | {r["false_positive"]} | {r["unresolved_positive"] + r["unresolved_negative"]}/{r["eligible"]} | {r["service_missing_positive"] + r["service_missing_negative"]} |')
paired = [r for r in comparison if r['paired_eligible'] and r['target'] and r['baseline_field']]
lines += ['', f'Paired mapped target judgments: {len(paired)}. Corrected: {sum(r["paired_change"] == "corrected" for r in paired)}; '
    f'new errors from a previously correct answer: {sum(r["paired_change"] == "new_error" for r in paired)}; '
    f'became unresolved: {sum(r["paired_change"] == "became_unresolved" for r in paired)}. '
    f'Reference fields unresolved before comparison: {sum(r["reference"] == "unresolved" for r in comparison)}; '
    f'fields with disagreement between the two blinded readers: {sum(r["reader_disagreement"] for r in comparison)}.']
audited = [r for r in checks if r['evidence_reference']]
audited_complete = [r for r in audited if r['status'] == 'successful' and r['evidence_reference'] in {'established', 'not_established'}]
lines += ['', f'Blinded evidence-window audit: {len(audited)} prespecified window/field pairs; '
    f'{sum(r["evidence_reference"] == "established" for r in audited)} establish the claim, '
    f'{sum(r["evidence_reference"] == "not_established" for r in audited)} do not, '
    f'{sum(r["evidence_reference"] in {"ambiguous", "context_missing"} for r in audited)} remain unclear. '
    f'Among {len(audited_complete)} determinate audited pairs with a returned verification, '
    f'{sum(r["answer"] == r["evidence_reference"] for r in audited_complete)} agree with the blinded reader; '
    f'{sum(r["answer"] == "established" and r["evidence_reference"] == "not_established" for r in audited_complete)} falsely establish the claim. '
    'This checks excerpt entailment, separately from report-wide labels; it is a fallible Sol-medium comparison.']
if version in {'v14', 'v15', 'v16'}:
    correction_audited = audited_complete
    if version in {'v15', 'v16'}:
        with open('../output/cpc_jev_verification_v14.csv') as f:
            correction_audited = [r for r in csv.DictReader(f) if r['status'] == 'successful' and r['evidence_reference'] in {'established', 'not_established'}]
    lines += ['', 'The isolated v14 correction holds the twenty audited passage/field pairs fixed and uses only pairs with a returned corrected check. The reader judgments were frozen before either the correction or its results.', '',
        '| Check | Agrees / available pairs | False establishments | False rejections | Unresolved |',
        '|---|---:|---:|---:|---:|']
    for label, key in [('Original incomplete rules', 'prior_answer'), ('Corrected complete rules', 'answer')]:
        lines.append(f'| {label} | {sum(r[key] == r["evidence_reference"] for r in correction_audited)}/{len(correction_audited)} | '
            f'{sum(r[key] == "established" and r["evidence_reference"] == "not_established" for r in correction_audited)} | '
            f'{sum(r[key] == "not_established" and r["evidence_reference"] == "established" for r in correction_audited)} | '
            f'{sum(r[key] in {"ambiguous", "context_missing"} for r in correction_audited)} |')
if version in {'v14', 'v15', 'v16'}:
    lines += ['', 'Requiring a qualifying commitment basis can leave an apparently positive predicate unresolved. The table below evaluates the usable check after that requirement, on the same twenty audited pairs.', '',
        '| Check | Correct / available pairs | Positive claims confirmed | False positives | False negatives | Unresolved |',
        '|---|---:|---:|---:|---:|---:|']
    for label, key in [('Original incomplete rules', 'prior_value'), ('Corrected complete rules', 'value')]:
        lines.append(f'| {label} | {sum(r[key] == ("1" if r["evidence_reference"] == "established" else "0") for r in correction_audited)}/{len(correction_audited)} | '
            f'{sum(r[key] == "1" and r["evidence_reference"] == "established" for r in correction_audited)} | '
            f'{sum(r[key] == "1" and r["evidence_reference"] == "not_established" for r in correction_audited)} | '
            f'{sum(r[key] == "0" and r["evidence_reference"] == "established" for r in correction_audited)} | '
            f'{sum(r[key] == "" for r in correction_audited)} |')
lines += ['', '| Application | Sample | Parts | Baseline done | Revised done | Checks failed |', '|---|---|---:|---:|---:|---:|']
for r in coverage:
    lines.append(f'| {r["application_number"].replace(" ", "")} | {r["test_group"]} | {r["request_count"]} | {r["baseline_complete"]} | {r["revised_complete"]} | {r["verification_failed_requests"]} |')
lines += ['', 'A zero reference denominator means that detection or false-positive performance is not estimable for that cell. Family and short/long metrics, exact answers, evidence windows, unresolved reference readings and processing failures are retained in the adjacent datasets. A rejected positive evidence window remains unresolved rather than becoming a report-wide negative. No bulk processing has restarted.', '']
Path(f'../output/cpc_jev_findings_{version}.md').write_text('\n'.join(lines))
print(f'Compared {len(comparison)} reference fields. Observed usage increase ${spent}.')
