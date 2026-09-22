#!/usr/bin/env python3
"""Check the running corpus against old coding and new, blinded source readings."""
import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

with open('../input/cpc_jev_labels_corpus_v2.csv') as f:
    labels = list(csv.DictReader(f))
with open('../output/cpc_jev_corpus_human_comparison_v2.csv') as f:
    humans = list(csv.DictReader(f))
with open('../output/cpc_jev_corpus_source_audit_v2.csv') as f:
    random_audit = list(csv.DictReader(f))
with open('../input/cpc_jev_narrative_sources_corpus_v2.csv') as f:
    links = list(csv.DictReader(f))
book = json.loads(Path('../input/cpc_jev_codebook_corpus_v1.json').read_text())
selection = json.loads(Path('../input/cpc_jev_quality_selection.json').read_text())
checks = json.loads(Path('../input/cpc_jev_quality_source_checks.json').read_text())
label = {r['document_id']: r for r in labels}
assert len(label) == len(labels)
complete = {d: r for d, r in label.items() if r['extraction_status'] == 'complete'}
selected = {r['document_id']: r for r in selection['sample']}
assert len(selected) == len(selection['sample']) == 12
assert set(selected) <= complete.keys()

# Reproduce the frozen positive-sample selection; do not use later completions.
human_ids = {r['source_document_id'] for r in humans}
ranked = sorted((label[d] for d in selection['population_document_ids'] if d not in human_ids and label[d]['random_audit'] != '1'),
    key=lambda r: hashlib.sha256((selection['selection_seed'] + r['document_id']).encode()).hexdigest())
chosen = {}
for group in selection['strata']:
    candidates = [r for r in ranked if r['document_id'] not in chosen]
    if group == 'council_positive':
        candidates = [r for r in candidates if r['council_involvement'] == '1']
    elif group == 'civic_positive':
        candidates = [r for r in candidates if r['civic_group_position'] in {'support_or_request', 'opposition'}]
    elif group == 'character_adopted_positive':
        candidates = [r for r in candidates if r['neighborhood_character__adopted'] == '1']
    elif group == 'affordability_positive':
        candidates = [r for r in candidates if '1' in [r['affordability__concern_request'], r['affordability__adopted']]]
    else:
        raise AssertionError('Unknown selection group')
    for r in candidates[:selection['reports_per_stratum']]:
        chosen[r['document_id']] = group
assert chosen == {d: r['selection_group'] for d, r in selected.items()}

# Verify every new quotation against the full source segments, not Jev evidence.
review_ids = set(selected) | {r['document_id'] for r in checks}
segments = defaultdict(list)
with open('../input/cpc_jev_segments_corpus_v2.csv') as f:
    for r in csv.DictReader(f):
        if r['document_id'] in review_ids:
            segments[r['document_id']].append(r)
source_rows = []
for doc, r in selected.items():
    packet = dict(document_id=doc, application_number=label[doc]['application_number'], project_name=label[doc]['project_name'], source_segments=segments[doc])
    assert hashlib.sha256(json.dumps(packet, ensure_ascii=False).encode()).hexdigest() == r['packet_sha256']
for reader in 'de':
    with open(f'../input/cpc_jev_quality_reader_{reader}.jsonl') as f:
        for line in f:
            r = json.loads(line)
            doc = r['document_id']
            assert selected[doc]['reader'] == r['reader'] == reader
            assert r['field'] in book['questions'] and r['value'] in (0, 1, None)
            assert r['model'] == 'gpt-5.6-sol' and r['reasoning'] == 'medium'
            assert r['value'] != 1 or r['evidence']
            for e in r['evidence']:
                assert e['quote'] and any(e['quote'] in s['text'] for s in segments[doc]
                    if s['source_document_id'] == e['source_document_id'] and int(s['pdf_page']) == int(e['pdf_page']))
            source_rows.append(dict(document_id=doc, application_number=label[doc]['application_number'],
                selection_group=selected[doc]['selection_group'], field=r['field'], reference_value=r['value'],
                jev_value=complete[doc][r['field']], reason=r['reason'], reader=reader,
                evidence_json=json.dumps(r['evidence'], ensure_ascii=False)))
assert len(source_rows) == len({(r['document_id'], r['field']) for r in source_rows}) == 384
for r in checks:
    for e in r['evidence']:
        assert e['quote'] and any(e['quote'] in s['text'] for s in segments[r['document_id']]
            if s['source_document_id'] == e['source_document_id'] and int(s['pdf_page']) == int(e['pdf_page']))

# Exact agreement and positive detection are separate; negatives cannot hide misses.
metrics = []
for field in list(book['legacy_topics']) + ['councilmember_position', 'civic_group_position']:
    for variant in ['concern_or_adopted', 'concern_only'] if field in book['legacy_topics'] else ['position']:
        column = 'jev_concern_request' if variant == 'concern_only' else 'jev_value'
        rows = [r for r in humans if r['field'] == field and r['comparison_status'] == 'comparable' and r[column] != '']
        pairs = [(r['reconciled_value'], r[column]) for r in rows]
        negative = {'0', 'none_or_procedural'}
        metrics.append(dict(reference='existing_humans', field=field, variant=variant, compared=len(pairs),
            agreements=sum(a == b for a, b in pairs), reference_positive=sum(a not in negative for a, b in pairs),
            detected_positive=sum(a not in negative and b not in negative for a, b in pairs),
            exact_positive=sum(a == b and a not in negative for a, b in pairs),
            model_positive=sum(b not in negative for a, b in pairs),
            false_positive=sum(a in negative and b not in negative for a, b in pairs)))
for reference, rows in [('random_sol', random_audit), ('targeted_sol', source_rows)]:
    for field in book['questions']:
        pairs = [(str(r['reference_value']), r['jev_value']) for r in rows
            if r['field'] == field and r['reference_value'] not in ('', None) and r['jev_value'] != '']
        metrics.append(dict(reference=reference, field=field, variant='frozen_question', compared=len(pairs),
            agreements=sum(a == b for a, b in pairs), reference_positive=sum(a == '1' for a, b in pairs),
            detected_positive=sum(a == b == '1' for a, b in pairs), exact_positive=sum(a == b == '1' for a, b in pairs),
            model_positive=sum(b == '1' for a, b in pairs), false_positive=sum(a == '0' and b == '1' for a, b in pairs)))

# These flags locate ambiguity; they are not source-adjudicated error rates.
diagnostics = []
# A shared generic title can connect separate projects. Flag for source review,
# not automatic exclusion: different ZAP IDs do not alone prove a bad link.
focal = {r['document_id']: r for r in links if r['link_role'] == 'focal_report'}
link_checks = []
for r in links:
    if r['link_role'] != 'context_companion' or r['text_included_flag'] != 'TRUE':
        continue
    f = focal[r['document_id']]
    focal_zap = set(f['source_zap_project_ids'].split('; ')) - {''}
    other_zap = set(r['source_zap_project_ids'].split('; ')) - {''}
    if (f['source_project_name'] and f['source_project_name'] == r['source_project_name']
        and f['source_vote_date'] == r['source_vote_date'] and focal_zap and other_zap and not focal_zap & other_zap):
        link_checks.append(dict(document_id=r['document_id'], application_number=r['narrative_application_number'],
            source_document_id=r['source_document_id'], source_application_number=r['source_application_number'],
            shared_name=r['source_project_name'], vote_date=r['source_vote_date'],
            focal_zap_ids=f['source_zap_project_ids'], companion_zap_ids=r['source_zap_project_ids'],
            focal_district=f['source_analysis_community_district'], companion_district=r['source_analysis_community_district'],
            extraction_status=label[r['document_id']]['extraction_status']))
assert len(focal) == len(labels)
for size in ['1', '2-3', '4+']:
    rows = [r for r in labels if ('1' if int(r['request_count']) == 1 else '2-3' if int(r['request_count']) <= 3 else '4+') == size]
    diagnostics.append(dict(kind='completion_by_parts', field=size, denominator=len(rows), count=sum(r['extraction_status'] == 'complete' for r in rows)))
for field in ['evidence_conflicts', 'stage_conflicts']:
    diagnostics.append(dict(kind='report_flag', field=field, denominator=len(complete), count=sum(int(r[field]) > 0 for r in complete.values())))
evidence_counts = defaultdict(Counter)
with open('../input/cpc_jev_answers_corpus_v2.csv') as f:
    for r in csv.DictReader(f):
        rule = book['questions'][r['field']]
        if r['document_id'] not in complete or rule['type'] != 'noul' or not rule['evidence'] or r['value'] not in {'0', '1'}:
            continue
        counts = evidence_counts[r['field']]
        counts['yes' if r['value'] == '1' else 'no'] += 1
        counts['yes_without_page'] += r['value'] == '1' and r['selected_segment_id'] == 'none'
        counts['no_with_page'] += r['value'] == '0' and r['selected_segment_id'] not in {'', 'none'}
for field, counts in evidence_counts.items():
    for flag, base in [('yes_without_page', 'yes'), ('no_with_page', 'no')]:
        diagnostics.append(dict(kind=flag, field=field, denominator=counts[base], count=counts[flag]))

save_csv(metrics, list(metrics[0]), '../output/cpc_jev_quality_metrics_v1.csv', ['reference', 'field', 'variant'])
save_csv(source_rows, list(source_rows[0]), '../output/cpc_jev_quality_source_checks_v1.csv', ['document_id', 'field'])
save_csv(diagnostics, list(diagnostics[0]), '../output/cpc_jev_quality_diagnostics_v1.csv', ['kind', 'field'])
save_csv(link_checks, list(link_checks[0]), '../output/cpc_jev_quality_link_checks_v1.csv', ['document_id', 'source_document_id'])
lines = ['# Mid-run quality evidence', '',
    f'This build contains {len(complete):,} completed reports. The human comparison covers {len({r["source_document_id"] for r in humans if r["comparison_status"] == "comparable"})} previously coded reports; field denominators vary because references or threshold ties can be missing. These development-era, imperfect labels measure agreement, not verified accuracy.', '',
    '| Field | Current match | Concern-only match | Current positive hits |', '|---|---:|---:|---:|']
for field in list(book['legacy_topics']) + ['councilmember_position', 'civic_group_position']:
    mm = [m for m in metrics if m['reference'] == 'existing_humans' and m['field'] == field]
    broad = mm[0]
    concern = next((m for m in mm if m['variant'] == 'concern_only'), None)
    lines.append(f"| {field.replace('_', ' ')} | {broad['agreements']}/{broad['compared']} | {str(concern['agreements']) + '/' + str(concern['compared']) if concern else 'n/a'} | {broad['detected_positive']}/{broad['reference_positive']} |")
lines += ['', 'Current topic comparisons combine a concern/request OR an adopted commitment. Concern-only is a diagnostic using already saved answers; no labels or questions were changed. Council and civic positive hits mean presence of either support/request or opposition; exact position can still disagree.', '',
    '# Fresh source readings', '',
    'Twelve new reports were selected from completed reports outside the old human benchmark and the original 24-report random audit. Three positive reports were selected for each signal below. Two Sol-medium readers read complete source packets without Jev answers or selection-group labels. These are additional AI judgments, not human adjudications or an overall accuracy estimate.', '',
    '| Selected positive signal | Blinded reader also positive | Unresolved |', '|---|---:|---:|']
source_lookup = {(r['document_id'], r['field']): r['reference_value'] for r in source_rows}
for group in selection['strata']:
    values = []
    for doc, selected_row in selected.items():
        if selected_row['selection_group'] != group:
            continue
        fields = [f for f in book['questions'] if f.startswith('council__')] if group == 'council_positive' else [f for f in book['questions'] if f.startswith('civic__')] if group == 'civic_positive' else ['neighborhood_character__adopted'] if group == 'character_adopted_positive' else ['affordability__concern_request', 'affordability__adopted']
        vv = [source_lookup[doc, f] for f in fields]
        values.append(1 if 1 in vv else 0 if all(v == 0 for v in vv) else None)
    lines.append(f"| {group.replace('_', ' ')} | {values.count(1)}/{len(values) - values.count(None)} | {values.count(None)} |")
lines += ['', '# Direct source checks', '', 'The following are primary-agent source assessments, not human adjudications.', '']
for r in checks:
    lines += [f"**{label[r['document_id']]['application_number']} — {r['field'].replace('__', ': ').replace('_', ' ')}.** {r['reason']} Source: [PDF page {r['evidence'][0]['pdf_page']}]({r['public_pdf_url']}#page={r['evidence'][0]['pdf_page']}). Assessment: {r['assessment'].replace('_', ' ')}.", '']
suspect_ids = {r['document_id'] for r in link_checks}
cop_ids = {r['document_id'] for r in link_checks if r['shared_name'] == 'C-O-P'}
lines += ['# Source-link screening', '',
    f'{len(suspect_ids):,} reports have an included context companion with the same nonempty title and vote date but different, nonoverlapping recorded ZAP project IDs; {len(suspect_ids & complete.keys()):,} are completed. Within this screen, {len(cop_ids):,} reports ({len(cop_ids & complete.keys()):,} completed) share the generic C-O-P title. These are candidates for checking, not confirmed errors. Separate ZAP IDs can sometimes belong to genuinely related actions.', '',
    'C 920197 PPQ establishes a real scope error: its Queens parcels inherit civic opposition from a Brooklyn C-O-P report. The source-link producer connects same-title/same-date groups, and the request producer includes the full PDFs. The Queens PDF itself also contains unrelated Manhattan testimony after its own report and local recommendations. Both cross-document links and within-PDF application boundaries need attention. Retaining all source material should not imply assigning every passage to every application.', '',
    '# Coverage and internal consistency', '', '| Report parts | Completed |', '|---|---:|']
for r in diagnostics:
    if r['kind'] == 'completion_by_parts':
        lines.append(f"| {r['field']} | {r['count']}/{r['denominator']} |")
for r in diagnostics:
    if r['kind'] == 'report_flag':
        lines += ['', f"{r['count']:,}/{r['denominator']:,} completed reports have at least one {r['field'].replace('_', ' ')} flag. This is a review flag, not an established report error."]
positive_calls = sum(r['denominator'] for r in diagnostics if r['kind'] == 'yes_without_page')
positive_no_page = sum(r['count'] for r in diagnostics if r['kind'] == 'yes_without_page')
negative_calls = sum(r['denominator'] for r in diagnostics if r['kind'] == 'no_with_page')
negative_with_page = sum(r['count'] for r in diagnostics if r['kind'] == 'no_with_page')
lines += ['', f'At the individual request/question level, {positive_no_page:,}/{positive_calls:,} positive yes/no answers have no selected supporting page, while {negative_with_page:,}/{negative_calls:,} negative answers nonetheless select a page. A candidate evidence page is not an independent verification of the label. A report contains many questions, so report-level flags can be common even when the per-answer conflict share is much smaller.']
lines += ['']
Path('../output/cpc_jev_quality_findings_v1.md').write_text('\n'.join(lines))
print(f'Quality audit saved: {len(complete)} completed reports, 384 fresh source judgments, {len(checks)} targeted source checks. No extraction labels changed.')
