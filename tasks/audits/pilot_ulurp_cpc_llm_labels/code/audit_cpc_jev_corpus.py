#!/usr/bin/env python3
"""Audit saved corpus answers; preserve missing results and all ZAP projects."""
import csv
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

with open('../input/cpc_jev_labels_corpus_v2.csv') as f:
    labels = list(csv.DictReader(f))
with open('../input/cpc_jev_successful_labels_corpus_v2.csv') as f:
    successful_labels = list(csv.DictReader(f))
with open('../input/cpc_jev_usage_corpus_v2.csv') as f:
    usage = list(csv.DictReader(f))
with open('../input/cpc_jev_source_coverage_corpus_v2.csv') as f:
    sources = list(csv.DictReader(f))
with open('../input/cpc_jev_reconciled_coding_corpus_v2.csv') as f:
    humans = list(csv.DictReader(f))
with open('../input/zap_project_coverage_corpus.csv') as f:
    projects = list(csv.DictReader(f))
book = json.loads(Path('../input/cpc_jev_codebook_corpus_v1.json').read_text())
packets = [json.loads(line) for line in Path('../output/cpc_jev_corpus_audit_packets_v1.jsonl').read_text().splitlines()]
assert len(labels) == len({r['document_id'] for r in labels})
assert len(sources) == len({r['document_id'] for r in sources})
assert len(projects) == len({r['project_id'] for r in projects})
assert len(humans) == len({(r['source_document_id'], r['field']) for r in humans})
label = {r['document_id']: r for r in labels}
usable = {r['document_id']: r for r in successful_labels}
assert len(usable) == len(successful_labels)
assert set(usable) == {r['document_id'] for r in labels if r['extraction_status'] == 'complete'}
source = {r['document_id']: r for r in sources}
packet = {r['document_id']: r for r in packets}
assert set(packet) == {r['document_id'] for r in labels if r['random_audit'] == '1'}

# The human benchmark is distinct from the random, blinded AI source audit.
comparison = []
for h in humans:
    doc, field = h['source_document_id'], h['field']
    assert doc in label, 'Human source is absent from the narrative roster.'
    r = label[doc]
    modeled_field = field + '__review_issue' if field in book['legacy_topics'] else field
    modeled = usable.get(doc, {}).get(modeled_field, '')
    comparison.append(dict(h, jev_field=modeled_field, jev_value=modeled,
        jev_concern_request=usable.get(doc, {}).get(field + '__concern_request', ''),
        jev_extraction_status=r['extraction_status'],
        comparison_status='not_measured' if modeled_field not in r else 'model_unavailable' if modeled == '' else 'reference_unavailable' if h['reconciled_value'] == '' else 'comparable',
        agrees='' if modeled == '' or h['reconciled_value'] == '' else int(modeled == h['reconciled_value'])))

audit = []
for reader in 'abc':
    for line in Path(f'../input/cpc_jev_corpus_reader_{reader}.jsonl').read_text().splitlines():
        r = json.loads(line)
        p = packet[r['document_id']]
        assert p['reader'] == r['reader'] == reader
        assert r['field'] in book['questions'] and r['value'] in (0, 1, None)
        assert r['model'] == 'gpt-5.6-sol' and r['reasoning'] == 'medium'
        assert r['value'] != 1 or r['evidence'], 'Positive source judgment lacks evidence.'
        for e in r['evidence']:
            assert e['quote'] and any(e['quote'] in s['text'] for s in p['source_segments']
                if s['source_document_id'] == e['source_document_id'] and int(s['pdf_page']) == int(e['pdf_page'])), 'Audit quotation is not in the supplied source.'
        jev = usable.get(r['document_id'], {}).get(r['field'], '')
        audit.append(dict(document_id=r['document_id'], application_number=p['application_number'],
            field=r['field'], reader=reader, model=r['model'], reasoning=r['reasoning'],
            reference_value=r['value'], jev_value=jev, extraction_status=label[r['document_id']]['extraction_status'],
            comparable=int(r['value'] is not None and jev != ''),
            agrees='' if r['value'] is None or jev == '' else int(str(r['value']) == jev),
            reference_reason=r['reason'], reference_evidence_json=json.dumps(r['evidence'], ensure_ascii=False)))
assert len(audit) == len(packet) * len(book['questions'])
assert len(audit) == len({(r['document_id'], r['field']) for r in audit})

metrics = []
for reference, rows, fields in [('blinded_sol_random_audit', audit, list(book['questions'])),
        ('existing_reconciled_benchmark', comparison, list(book['legacy_topics']) + ['councilmember_position', 'civic_group_position'])]:
    for field in fields:
        selected = [r for r in rows if r['field'] == field]
        paired = [(str(r['reference_value'] if reference == 'blinded_sol_random_audit' else r['reconciled_value']), r['jev_value'])
                  for r in selected if (r['comparable'] if reference == 'blinded_sol_random_audit' else r['comparison_status'] == 'comparable')]
        positive = [p for p in paired if p[0] == '1' or p[0] in {'support_or_request', 'opposition'}]
        negative = [p for p in paired if p[0] in {'0', 'none_or_procedural'}]
        metrics.append(dict(reference=reference, field=field, reference_rows=len(selected), comparable=len(paired), unavailable=len(selected)-len(paired),
            agreements=sum(a == b for a, b in paired), reference_positive=len(positive),
            positive_detected=sum(b not in {'0', 'none_or_procedural'} for a, b in positive),
            positive_exact_matches=sum(a == b for a, b in positive), reference_negative=len(negative),
            false_positives=sum(b not in {'0', 'none_or_procedural'} for a, b in negative)))

# Link to the complete ZAP spine by sets, never by a row-expanding join.
project_rows = []
for p in projects:
    linked = {s.strip() for s in re.split('[;|]', p['linked_cpc_document_ids']) if s.strip()}
    assert len(linked) == int(p['linked_cpc_report_count'])
    retained = linked & source.keys()
    represented = {doc for sid in retained for doc in source[sid]['represented_narrative_ids'].split(';') if doc}
    complete = {doc for doc in represented if label[doc]['extraction_status'] == 'complete'}
    status = 'no_linked_cpc_report' if not linked else 'no_represented_narrative' if not represented else 'all_represented_narratives_complete' if complete == represented else 'extraction_incomplete'
    project_rows.append(dict(p, retained_cpc_source_count=len(retained),
        cpc_sources_outside_narrative_corpus=';'.join(sorted(linked - retained)),
        retained_cpc_sources_without_text=sum(source[sid]['coverage_status'] == 'no_usable_text' for sid in retained),
        retained_cpc_sources_context_only=sum(source[sid]['coverage_status'] == 'context_only' for sid in retained),
        retained_cpc_sources_unrepresented=sum(source[sid]['coverage_status'] == 'unrepresented_source' for sid in retained),
        jev_narrative_ids=';'.join(sorted(represented)), jev_narrative_count=len(represented),
        jev_complete_narrative_count=len(complete), jev_coverage_status=status))

# The targeted queue is a diagnostic sample and is not part of random-audit rates.
queue = []
random_ids = set(packet)
ranked = sorted((r for r in successful_labels if r['document_id'] not in random_ids),
                key=lambda r: hashlib.sha256(('cpc-jev-targeted-v1:' + r['document_id']).encode()).hexdigest())
for reason, candidates in [
        ('binary_evidence_conflict', [r for r in ranked if int(r['evidence_conflicts'])]),
        ('stage_conflict', [r for r in ranked if int(r['stage_conflicts'])]),
        ('positive_council_or_civic', [r for r in ranked if r['council_involvement'] == '1' or r['civic_group_position'] in {'opposition', 'support_or_request'}]),
        ('positive_topic_concern', [r for r in ranked if any(r[t + '__concern_request'] == '1' for t in book['legacy_topics'])])]:
    for r in candidates[:6]:
        queue.append(dict(document_id=r['document_id'], application_number=r['application_number'], reason=reason,
            extraction_status=r['extraction_status'], review_status='pending_source_review'))

save_csv(comparison, list(comparison[0]), '../output/cpc_jev_corpus_human_comparison_v2.csv', ['source_document_id', 'field'])
save_csv(audit, list(audit[0]), '../output/cpc_jev_corpus_source_audit_v2.csv', ['document_id', 'field'])
save_csv(metrics, list(metrics[0]), '../output/cpc_jev_corpus_agreement_v2.csv', ['reference', 'field'])
save_csv(project_rows, list(project_rows[0]), '../output/cpc_jev_corpus_zap_coverage_v2.csv', ['project_id'])
save_csv(queue, ['document_id', 'application_number', 'reason', 'extraction_status', 'review_status'], '../output/cpc_jev_corpus_review_queue_v2.csv', ['document_id', 'reason'])

counts = Counter(r['extraction_status'] for r in labels)
request_counts = Counter(r['status'] for r in usage)
withdrawn = [r for r in project_rows if r['withdrawn_or_terminated'].lower() == 'true']
credit = [json.loads(line) for line in Path('../input/cpc_jev_credits_corpus_v2.jsonl').read_text().splitlines()]
charge = float(credit[-1]['total_used']) - float(credit[0]['total_used'])
lines = ['# Corpus processing and audit snapshot', '',
    f"The saved response snapshot retains all {len(labels):,} narratives: " + ', '.join(f'{n:,} {s}' for s, n in sorted(counts.items())) + '.', '',
    f"The working dataset contains only the {len(successful_labels):,} reports for which every requested part returned a valid response. Partial reports and failed or unattempted reports are excluded from usable results and comparisons. They remain in the coverage table and audit denominators. Success means the API response worked, not that every judgment is correct; exact probability ties still remain missing.", '',
    f"There are {len(usage):,} planned requests after carrying forward three complete reports. " + ', '.join(f'{n:,} {s}' for s, n in sorted(request_counts.items())) + '.', '',
    f"The corpus ledger records {sum(int(r['attempt_count']) for r in usage):,} attempts, including {sum(r['status'] == 'successful' and int(r['selected_attempt'] or 0) > 1 for r in usage):,} requests recovered after temporary service errors. Every attempt is archived; the first valid answer is kept and successful requests are never submitted again.", '',
    f"The last saved account check ({credit[-1]['checked_at']}) shows ${charge:.2f} additional account usage and ${float(credit[-1]['balance']):.2f} remaining. Account usage is shared; it is not a per-request invoice.", '',
    f"All {len(sources):,} CPC source records and {len(project_rows):,} ZAP projects remain in their coverage tables. Of {len(withdrawn):,} withdrawn/terminated ZAP projects, {sum(int(r['linked_cpc_report_count']) > 0 for r in withdrawn):,} have a linked CPC report and {sum(int(r['jev_narrative_count']) > 0 for r in withdrawn):,} have a represented narrative. Missing narratives and failed API calls are not coded as absence of a concern.", '',
    '## Random source audit', '',
    f'Three Sol-medium readers independently coded {len(packet)} preselected random reports without seeing Jev answers or old human coding. These are AI source judgments, not adjudicated human truth. Exact quotations were checked against source pages. Each field below has its own available denominator; unavailable cases remain in the audit.', '',
    '| Field | Paired | Match | Positive hits | False + |',
    '|---|---:|---:|---:|---:|']
for m in metrics:
    if m['reference'] == 'blinded_sol_random_audit':
        display = m['field'].replace('__', ': ').replace('_', ' ')
        for long, short in [('neighborhood character', 'character'), ('scale density design', 'scale/design'), ('historic preservation', 'historic'), ('infrastructure services', 'services'), ('environment open space', 'environment'), ('concern request', 'concern/request')]:
            display = display.replace(long, short)
        lines.append(f"| {display} | {m['comparable']} / 24 | {m['agreements']} | {m['positive_detected']} / {m['reference_positive']} | {m['false_positives']} |")
lines += ['', '## Existing human benchmark', '',
    'All 340 previously coded reports remain in the comparison, with Jacob, Tyler, and the working reconciled reference preserved separately. This is a convenience benchmark used during development, not an untouched test set. Topic comparisons use concern/request OR adopted commitment; concern-only values are also saved. Fields outside the frozen 32 questions are explicitly marked not measured.', '',
    '| Field | Paired | Match | Positive hits | False + |',
    '|---|---:|---:|---:|---:|']
for m in metrics:
    if m['reference'] == 'existing_reconciled_benchmark':
        lines.append(f"| {m['field'].replace('_', ' ')} | {m['comparable']} / 340 | {m['agreements']} | {m['positive_detected']} / {m['reference_positive']} | {m['false_positives']} |")
lines += ['', '## Limits and remaining review', '',
    f"The targeted queue contains {len(queue)} report/reason pairs, flagged for source review separately from the random audit. Its contents can grow/change as the run proceeds. No targeted-review agreement rate is claimed.", '',
    'Long documents retain every text segment but are judged in separate parts. One positive part establishes a provisional positive; a negative requires all parts to answer negatively. Missing parts and exact 0.5 ties remain missing unless another part supplies a positive. A positive evidence selection and a yes/no judgment may disagree; both are retained.', '',
    'Adopted commitments are measured provisionally using the general commitment question from the earlier topic pilot. This field does not establish a revision after certification, or that opposition caused a change. Council involvement includes concerns motivating the rezoning; explicit endorsement remains a separate field. These definitions should not be collapsed into causal claims.', '']
Path('../output/cpc_jev_corpus_findings_v2.md').write_text('\n'.join(lines))
print(f'Audit snapshot saved: {len(audit)} random source judgments, {len(comparison)} preserved human rows, {len(project_rows)} ZAP projects.')
