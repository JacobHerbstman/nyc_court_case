#!/usr/bin/env python3
"""Validate saved Sol readings against Luna or the original human coding."""
import csv
import hashlib
import json
import sys
from pathlib import Path
sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

version = sys.argv[1] if len(sys.argv) == 2 else 'v1'
assert version in {'v1', 'v2', 'v3'} and len(sys.argv) <= 2
if version == 'v1':
    with open('../output/cpc_luna_labels_v1.csv') as f:
        luna_rows = list(csv.DictReader(f))
    luna = {(r['document_id'], r['field']): r for r in luna_rows}
    assert len(luna) == len(luna_rows) == 144
    with open('../output/cpc_luna_comparison_v1.csv') as f:
        reference_rows = list(csv.DictReader(f))
    reference = {(r['document_id'], r['field']): r for r in reference_rows}
    assert len(reference) == len(reference_rows) == 79
    with open('../output/cpc_luna_sample_v1.csv') as f:
        sample_rows = list(csv.DictReader(f))
    book = json.loads(Path('../input/cpc_sol_codebook_v1.json').read_text())
elif version == 'v2':
    with open('../output/cpc_sol_validation_sample_v2.csv') as f:
        sample_rows = list(csv.DictReader(f))
    book = json.loads(Path('../input/cpc_sol_validation_codebook_v2.json').read_text())
else:
    with open('../output/cpc_sol_expansion_sample_v3.csv') as f:
        sample_rows = list(csv.DictReader(f))
    book = json.loads(Path('../input/cpc_sol_expansion_codebook_v3.json').read_text())
sample = {r['document_id']: r for r in sample_rows}
assert len(sample) == len(sample_rows) == {'v1': 4, 'v2': 6, 'v3': 20}[version]
original_book = json.loads(Path('../input/cpc_luna_codebook_v1.json').read_text())
if version == 'v3':
    assert book['questions'] == {k: v for k, v in original_book['questions'].items()
        if not k.startswith(('council__', 'civic__'))}
else:
    assert book == original_book
actor_roles = {'applicant', 'community_board', 'council_member', 'council_body', 'cpc', 'independent_civic', 'other', 'unclear'}
statuses = {'concern', 'request', 'applicant_promise', 'adopted_requirement', 'incorporated_change', 'proposal_description',
            'support', 'opposition', 'main_approval', 'no_impact', 'unconfirmed_rule', 'unclear'}
topics = {f.split('__')[0] for f in book['questions']} - {'council', 'civic'}
labels, evidence, comparison = [], [], []
seen = set()
source_segments = {}
for reader in (list('abc') if version != 'v3' else [f'{n:02d}' for n in range(1, 11)]):
    if version == 'v1':
        packets = json.loads(Path(f'../input/cpc_sol_packets_{reader}_v1.json').read_text())
        readings = [json.loads(s) for s in Path(f'../input/cpc_sol_reader_{reader}_v1.jsonl').read_text().splitlines()]
    elif version == 'v2':
        packets = json.loads(Path(f'../input/cpc_sol_validation_packets_{reader}_v2.json').read_text())
        readings = [json.loads(s) for s in Path(f'../input/cpc_sol_validation_reader_{reader}_v2.jsonl').read_text().splitlines()]
    else:
        packets = json.loads(Path(f'../input/cpc_sol_expansion_packets_{reader}_v3.json').read_text())
        readings = [json.loads(s) for s in Path(f'../input/cpc_sol_expansion_topics_{reader}_v3.jsonl').read_text().splitlines()]
    assert len(packets) == len({p['document_id'] for p in packets})
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
        source_segments.update({(doc, r['segment_id']): r for r in packet['segments']})
        assert len(pages) == len(packet['segments'])
        assert reading['application_number'] == sample[doc]['application_number']
        assert reading['reader'] == 'sol_medium_' + reader
        assert reading['source_scope'] in {'clear', 'concern'}
        assert len(reading['read_segment_ids']) == len(set(reading['read_segment_ids'])) == len(pages)
        assert set(reading['read_segment_ids']) == set(pages)
        assert set(reading['fields']) == set(book['questions'])
        for field, answer in reading['fields'].items():
            raw_value = answer['value']
            if version == 'v3':
                answer['value'] = {'yes': '1', 'no': '0'}.get(raw_value, raw_value)
            assert answer['value'] in {'1', '0', 'unresolved'}
            assert set(answer['segments']) <= set(pages)
            found = bool(answer['quote']) and any(answer['quote'] in pages[s] for s in answer['segments'])
            row = dict(document_id=doc, application_number=sample[doc]['application_number'], reader=reader,
                field=field, value=answer['value'], segments=';'.join(answer['segments']), quote=answer['quote'],
                reason=answer['reason'], quote_found=int(found), source_scope=reading['source_scope'], scope_reason=reading['scope_reason'])
            if version == 'v3':
                row.update(raw_value=raw_value, value_normalized=int(raw_value != answer['value']))
            labels.append(row)
            if version == 'v1':
                other, old = luna[(doc, field)], reference.get((doc, field), {})
                comparison.append(dict(row, luna=other['value'], luna_quote=other['quote'], luna_reason=other['reason'],
                    luna_segments=other['segments'], luna_quote_found=other['quote_found'],
                    reference=old.get('reference', ''), reference_origin=old.get('reference_origin', ''),
                    reference_reason=old.get('reference_reason', ''), models_agree=int(answer['value'] == other['value'])))
        assert len(reading['evidence_records']) <= 12
        for number, event in enumerate(reading['evidence_records'], 1):
            assert event['actor_role'] in actor_roles and event['status'] in statuses
            assert set(event['topics']) <= topics and set(event['segments']) <= set(pages)
            found = bool(event['quote']) and any(event['quote'] in pages[s] for s in event['segments'])
            evidence.append(dict(document_id=doc, application_number=sample[doc]['application_number'], record=number,
                actor=event['actor'], actor_role=event['actor_role'], status=event['status'], topics=';'.join(event['topics']),
                segments=';'.join(event['segments']), quote=event['quote'], quote_found=int(found), reason=event['reason']))
assert seen == set(sample) and len(labels) == len(sample) * len(book['questions'])
if version in {'v2', 'v3'}:
    if version == 'v2':
        design = json.loads(Path('../input/cpc_sol_validation_design_v2.json').read_text())
        with open('../output/cpc_sol_validation_humans_v2.csv') as f:
            human_rows = list(csv.DictReader(f))
    else:
        design = json.loads(Path('../input/cpc_sol_expansion_design_v3.json').read_text())
        with open('../output/cpc_sol_expansion_humans_v3.csv') as f:
            human_rows = list(csv.DictReader(f))
    humans = {(r['source_document_id'], r['field']): r for r in human_rows}
    assert len(humans) == len(human_rows)
    values = {(r['document_id'], r['field']): r['value'] for r in labels}
    assert len(values) == len(labels)
    for doc, report in sample.items():
        for field in list(design['legacy_topics']) + ([] if version == 'v3' else ['councilmember_position', 'civic_group_position']):
            h = humans[(doc, field)]
            if field in design['legacy_topics']:
                concern = [values[doc, t+'__concern_request'] for t in design['legacy_topics'][field]]
                obligation = [values[doc, t+'__specific_obligation'] for t in design['legacy_topics'][field]]
                components = {'concern_only': concern, 'concern_or_obligation': concern+obligation}
                predictions = {variant: '1' if '1' in vv else '0' if set(vv) == {'0'} else 'unresolved'
                               for variant, vv in components.items()}
                allowed = {'0', '1'}
            else:
                actor = 'council' if field == 'councilmember_position' else 'civic'
                support_fields = ['support', 'requested_project', 'requested_provision'] if actor == 'council' else ['support', 'request']
                support = [values[doc, actor+'__'+f] for f in support_fields]
                opposition = values[doc, actor+'__opposition']
                position = ('opposition' if opposition == '1' else 'unresolved' if opposition == 'unresolved' else
                    'support_or_request' if '1' in support else 'none_or_procedural' if set(support) == {'0'} else 'unresolved')
                actor_values = [v for (d, f), v in values.items() if d == doc and f.startswith(actor+'__')]
                involvement = '1' if '1' in actor_values else '0' if set(actor_values) == {'0'} else 'unresolved'
                predictions = {'position': position, 'involvement': involvement}
                allowed = {'support_or_request', 'opposition', 'none_or_procedural'}
            for variant, prediction in predictions.items():
                references = {'original': h['human_value'] if h['human_status'] in {'human_agreement', 'single_human_coder'}
                    and (h['jacob_coding_complete'] == '1' or h['tyler_coding_complete'] == '1') else '',
                    'jacob': h['jacob_value'] if h['jacob_coding_complete'] == '1' else '',
                    'tyler': h['tyler_value'] if h['tyler_coding_complete'] == '1' else ''}
                references = {k: v if v in allowed else '' for k, v in references.items()}
                if variant == 'involvement':
                    references = {k: ('0' if v == 'none_or_procedural' else '1') if v else '' for k, v in references.items()}
                comparison.append(dict(document_id=doc, application_number=report['application_number'],
                    selection_group=report['selection_group'], field=field, variant=variant, value=prediction,
                    original_reference=references['original'], jacob_reference=references['jacob'], tyler_reference=references['tyler'],
                    human_status=h['human_status'], original_raw=h['human_value'], jacob_raw=h['jacob_value'], tyler_raw=h['tyler_value'],
                    jacob_complete=h['jacob_coding_complete'], tyler_complete=h['tyler_coding_complete'],
                    jacob_notes=h['jacob_evidence_summary'], tyler_notes=h['tyler_evidence_summary'],
                    coding_source_check=h['coding_source_check']))
    metrics = []
    for field, variant in sorted({(r['field'], r['variant']) for r in comparison}):
        selected = [r for r in comparison if r['field'] == field and r['variant'] == variant]
        for reference in ['original', 'jacob', 'tyler']:
            column = reference+'_reference'
            scored = [r for r in selected if r[column]]
            negative = {'0', 'none_or_procedural'}
            metrics.append(dict(field=field, variant=variant, reference=reference, reports=len(selected), compared=len(scored),
                reference_missing=len(selected)-len(scored), matches=sum(r['value'] == r[column] for r in scored),
                reference_positive=sum(r[column] not in negative for r in scored),
                positive_detected=sum(r[column] not in negative and r['value'] not in negative | {'unresolved'} for r in scored),
                false_positive=sum(r[column] in negative and r['value'] not in negative | {'unresolved'} for r in scored),
                false_negative=sum(r[column] not in negative and r['value'] in negative for r in scored),
                unresolved=sum(r['value'] == 'unresolved' for r in scored)))
    if version == 'v3':
        save_csv(labels, list(labels[0]), '../output/cpc_sol_expansion_labels_v3.csv', ['document_id', 'field'])
        save_csv(evidence, list(evidence[0]), '../output/cpc_sol_expansion_evidence_v3.csv', ['document_id', 'record'])
        save_csv(comparison, list(comparison[0]), '../output/cpc_sol_expansion_comparison_v3.csv', ['document_id', 'field', 'variant'])
        save_csv(metrics, list(metrics[0]), '../output/cpc_sol_expansion_agreement_v3.csv', ['field', 'variant', 'reference'])
        with open('../input/cpc_sol_source_page_review_v3.csv') as f:
            page_review = list(csv.DictReader(f))
        expected_sources = {(doc, s['source_document_id']) for (doc, segment), s in source_segments.items()}
        assert len(page_review) == len(expected_sources)
        assert {(r['document_id'], r['source_document_id']) for r in page_review} == expected_sources
        for r in page_review:
            supplied = {int(s['pdf_page']) for (doc, segment), s in source_segments.items()
                if doc == r['document_id'] and s['source_document_id'] == r['source_document_id']}
            assert len(supplied) == int(r['supplied_pages'])
            assert r['missing_pages'] == ';'.join(map(str, sorted(set(range(1, int(r['pdf_pages'])+1)) - supplied)))
        save_csv(page_review, list(page_review[0]), '../output/cpc_sol_source_page_review_v3.csv',
                 ['document_id', 'source_document_id'])
        with open('../input/cpc_sol_expansion_source_review_v3.csv') as f:
            source_review = list(csv.DictReader(f))
        for r in source_review:
            segment = source_segments[r['document_id'], r['segment_id']]
            assert r['quote'] and r['quote'] in segment['text']
            r.update(quote_found=1, source_document_id=segment['source_document_id'],
                source_application_number=segment['source_application_number'], pdf_page=segment['pdf_page'])
        save_csv(source_review, list(source_review[0]), '../output/cpc_sol_expansion_source_review_v3.csv', ['finding_id'])
        lines = ['# Twenty-report topic expansion', '',
            'Twenty targeted human-coded reports, including nine long reports, receive a separate topic reading '
            'and actor inventory. The 27 topic definitions are unchanged; actor questions are omitted from this '
            'pass. Earlier September 22 Sol/Luna sources are excluded, older development history is flagged, '
            'and bulk Jev use is not excluded. This is not a representative holdout.', '',
            f'Returned {len(labels)} topic judgments and {len(evidence)} selected evidence records. '
            f'Positive quotes match {sum(r["value"] == "1" and r["quote_found"] for r in labels)}/'
            f'{sum(r["value"] == "1" for r in labels)}. Evidence quotes match '
            f'{sum(r["quote_found"] for r in evidence)}/{len(evidence)}. '
            f'Unresolved labels: {sum(r["value"] == "unresolved" for r in labels)}. '
            f'Explicit yes/no-to-1/0 format normalizations: {sum(r["value_normalized"] for r in labels)}. '
            'Exact quotations establish source presence, not correct interpretation or exhaustive coverage.', '',
            'The table compares completed, nonconflicting original human labels. Two mappings approximate the '
            'older broad issue definitions. Original Jacob and Tyler comparisons remain separate in the CSV; '
            'mismatches may reflect human errors or definition differences as well as model errors.', '',
            '| Issue | Mapping | Matches | Positive hits | False + | Unresolved |',
            '|---|---|---:|---:|---:|---:|']
        for r in metrics:
            if r['reference'] == 'original':
                variant = r['variant'].replace('concern_or_obligation', 'concern or obligation').replace('concern_only', 'concern only')
                lines.append(f'| {r["field"].replace("_", " ")} | {variant} | {r["matches"]}/{r["compared"]} | '
                    f'{r["positive_detected"]}/{r["reference_positive"]} | {r["false_positive"]} | {r["unresolved"]} |')
        primary = [r for r in metrics if r['reference'] == 'original' and r['variant'] == 'concern_or_obligation']
        lines += ['', f'The broad concern-or-obligation mapping matches {sum(r["matches"] for r in primary)}/'
            f'{sum(r["compared"] for r in primary)} available report/issue references, detecting '
            f'{sum(r["positive_detected"] for r in primary)}/{sum(r["reference_positive"] for r in primary)} '
            'reference positives. It does not validate every discussed/concern/obligation stage judgment.', '']
        lines += ['## Source coverage check', '',
            'A post-reading check compares supplied page numbers with all 28 source PDFs. Four focal reports '
            'have visually confirmed missing scanned recommendation attachments: Bronx Special Districts, '
            '3276 Jerome Avenue, 19 East 72nd Street and Variety Boys and Girls Club. A fifth source omits '
            'a zoning-map page. The OCR helper stops at the first CPC resolution; full supplied-segment '
            'coverage therefore does not establish full PDF coverage. The frozen packets and readings are '
            'not repaired mid-test. Missing attachments can affect both topic and actor results.', '',
            '## Selected source review', '',
            'The following are manager judgments after seeing the answers, not independent gold labels or '
            'an exhaustive audit of all 540 fields. Quotes are verified against the frozen packets. Original '
            'answers, human codes and reported agreement scores are unchanged.', '']
        for r in source_review:
            field_label = r['field'].replace('__', ': ').replace('_', ' ')
            lines += [f"**{r['application_number']} ({field_label}).** {r['assessment']}", '']
        Path('../output/cpc_sol_expansion_findings_v3.md').write_text('\n'.join(lines))
        print('Validated twenty topic readings and compared five issue families against original human coding.')
        sys.exit(0)
    save_csv(labels, list(labels[0]), '../output/cpc_sol_validation_labels_v2.csv', ['document_id', 'field'])
    save_csv(evidence, list(evidence[0]), '../output/cpc_sol_validation_evidence_v2.csv', ['document_id', 'record'])
    save_csv(comparison, list(comparison[0]), '../output/cpc_sol_validation_comparison_v2.csv', ['document_id', 'field', 'variant'])
    save_csv(metrics, list(metrics[0]), '../output/cpc_sol_validation_agreement_v2.csv', ['field', 'variant', 'reference'])
    lines = ['# Six additional Sol-medium readings', '',
        'Six targeted human-coded reports, including Council and civic examples and two long packets, were selected before reading. Earlier pilot/source-audit bundles are excluded; earlier bulk Jev use and human analysis are not. This is a development test, not a representative or untouched holdout.', '',
        f'All six reports returned {len(labels)} judgments and {len(evidence)} selected evidence records. '
        f'Positive quotes match {sum(r["value"] == "1" and r["quote_found"] for r in labels)}/{sum(r["value"] == "1" for r in labels)}; '
        f'evidence quotes match {sum(r["quote_found"] for r in evidence)}/{len(evidence)}. '
        f'Unresolved detailed judgments: {sum(r["value"] == "unresolved" for r in labels)}. Quote matching checks provenance, not interpretation.', '',
        'The table compares complete nonconflicting original human labels. Original Jacob and Tyler comparisons remain separate in the CSV. Topic rows show two approximations to the old broader issue definitions. Actor involvement includes concerns and does not establish explicit support. Disagreements may reflect definitions or human errors, not just model errors.', '',
        '| Field | Mapping | Matches | Positive hits | False + | Unresolved |', '|---|---|---:|---:|---:|---:|']
    for r in metrics:
        if r['reference'] == 'original':
            field = r['field'].replace('_', ' ').replace(' position', '')
            variant = r['variant'].replace('concern_or_obligation', 'concern or obligation').replace('concern_only', 'concern only')
            lines.append(f'| {field} | {variant} | {r["matches"]}/{r["compared"]} | {r["positive_detected"]}/{r["reference_positive"]} | {r["false_positive"]} | {r["unresolved"]} |')
    primary = [r for r in metrics if r['reference'] == 'original' and r['variant'] in {'position', 'concern_or_obligation'}]
    lines += ['', f'Across one selected mapping per family (topic concern-or-obligation and actor position), matches are {sum(r["matches"] for r in primary)}/{sum(r["compared"] for r in primary)}. '
        'Three of the 42 possible report/family comparisons have unavailable or conflicting references. '
        'A combined actor position can hide an omitted supporting or opposing organization; it does not validate every detailed stance or an exhaustive actor inventory.', '',
        'The 36-field codebook is unchanged; four reader checks address actor coverage, original-proposal undertakings, topic scope and evidence consistency. Different reports prevent attributing any score difference to these clarifications. Prompts, hashes and raw answers are preserved. Jev and bulk processing remain paused; no API calls or production relabeling occurred.', '']
    Path('../output/cpc_sol_validation_findings_v2.md').write_text('\n'.join(lines))
    print('Validated six Sol readings and compared five topic families plus two actor families with original human coding.')
    sys.exit(0)

assert len(comparison) == 144
metrics = []
for group in ['all', 'council', 'civic', 'topics']:
    selected = [r for r in comparison if group == 'all' or r['field'].split('__')[0] == group or
                group == 'topics' and r['field'].split('__')[0] in topics]
    scored = [r for r in selected if r['reference'] in {'0', '1'}]
    for model, column in [('sol', 'value'), ('luna', 'luna')]:
        metrics.append(dict(group=group, model=model, fields=len(selected), reference_fields=len(scored),
            unreferenced=len(selected)-len(scored), positive=sum(r['reference'] == '1' for r in scored),
            negative=sum(r['reference'] == '0' for r in scored),
            true_positive=sum(r['reference'] == r[column] == '1' for r in scored),
            false_positive=sum(r['reference'] == '0' and r[column] == '1' for r in scored),
            false_negative=sum(r['reference'] == '1' and r[column] == '0' for r in scored),
            true_negative=sum(r['reference'] == r[column] == '0' for r in scored),
            unresolved_scored=sum(r[column] not in {'0', '1'} for r in scored),
            unresolved_all=sum(r[column] not in {'0', '1'} for r in selected)))
save_csv(labels, list(labels[0]), '../output/cpc_sol_labels_v1.csv', ['document_id', 'field'])
save_csv(evidence, list(evidence[0]), '../output/cpc_sol_evidence_v1.csv', ['document_id', 'record'])
save_csv(comparison, list(comparison[0]), '../output/cpc_sol_luna_comparison_v1.csv', ['document_id', 'field'])
save_csv(metrics, list(metrics[0]), '../output/cpc_sol_luna_agreement_v1.csv', ['group', 'model'])
lines = ['\\newpage', '', '# Sol and Luna on the same four reports', '',
    'Three GPT-6 Sol readers at medium received the identical full source packets and 36-field codebook used by Luna. Instructions differ only in model and reader names. Readers received no prior answers or targeted hints. These are known development cases, not fresh validation.', '',
    f'Both readings returned all 144 report-field judgments. Sol supplied {len(evidence)} evidence records. '
    f'Exact quotes accompany {sum(r["value"] == "1" and r["quote_found"] for r in labels)}/{sum(r["value"] == "1" for r in labels)} Sol positives '
    f'and {sum(r["quote_found"] for r in evidence)}/{len(evidence)} supplementary records. Quote validity does not establish interpretive correctness.', '',
    'The same 79 earlier AI/manager references score both models; 65 fields lack a reference. The references are fallible and remain unchanged. Commerce Avenue and ASPCA contribute 36 each; Sunset Park four and Whitestone three. All reported errors below are disagreements with those references.', '',
    '| Model | Matches | Positive hits | False positives | False negatives | Unresolved scored / all |',
    '|---|---:|---:|---:|---:|---:|']
for r in metrics:
    if r['group'] == 'all':
        lines.append(f'| {r["model"].title()} | {r["true_positive"]+r["true_negative"]}/{r["reference_fields"]} | {r["true_positive"]}/{r["positive"]} | {r["false_positive"]}/{r["negative"]} | {r["false_negative"]} | {r["unresolved_scored"]} / {r["unresolved_all"]} |')
scored = [r for r in comparison if r['reference'] in {'0', '1'}]
lines += ['', f'Models agree on {sum(r["models_agree"] for r in comparison)}/144 fields, counting shared unresolved answers as agreement. '
    f'Within the scored set, Sol matches where Luna disagrees on {sum(r["value"] == r["reference"] != r["luna"] for r in scored)} fields; '
    f'Luna matches where Sol disagrees on {sum(r["luna"] == r["reference"] != r["value"] for r in scored)}.', '',
    '| Group | Model | Matches | Positive hits | Unresolved scored |', '|---|---|---:|---:|---:|']
for r in metrics:
    if r['group'] != 'all':
        lines.append(f'| {r["group"].title()} | {r["model"].title()} | {r["true_positive"]+r["true_negative"]}/{r["reference_fields"]} | {r["true_positive"]}/{r["positive"]} | {r["unresolved_scored"]} |')
lines += ['', 'Both are single readings under Codex, not repeated trials or API benchmarks. Model settings, source/prompt hashes and raw readings are preserved; exact rerun determinism and per-run dollar cost are unavailable. No Jev/API calls or production-label changes were made.', '']
Path('../output/cpc_sol_luna_findings_v1.md').write_text('\n'.join(lines))
print('Validated four Sol readings and compared all 144 fields with Luna, including 79 unchanged references.')
