#!/usr/bin/env python3
"""Validate saved actor inventories; preserve separate support/opposition."""
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
    with open('../output/cpc_sol_validation_sample_v2.csv') as f:
        sample_rows = list(csv.DictReader(f))
    with open('../output/cpc_sol_validation_labels_v2.csv') as f:
        prior_rows = list(csv.DictReader(f))
    prior = {(r['document_id'], r['field']): r for r in prior_rows}
    assert len(prior) == len(prior_rows)
    with open('../input/cpc_sol_actor_checks_v1.csv') as f:
        checks = list(csv.DictReader(f))
    assert len({(r['document_id'], r['field']) for r in checks}) == len(checks) == 8
elif version == 'v2':
    with open('../output/cpc_sol_actor_sample_v2.csv') as f:
        sample_rows = list(csv.DictReader(f))
    with open('../output/cpc_sol_actor_humans_v2.csv') as f:
        human_rows = list(csv.DictReader(f))
    humans = {(r['source_document_id'], r['field']): r for r in human_rows}
    assert len(humans) == len(human_rows)
else:
    with open('../output/cpc_sol_expansion_sample_v3.csv') as f:
        sample_rows = list(csv.DictReader(f))
    with open('../output/cpc_sol_expansion_humans_v3.csv') as f:
        human_rows = list(csv.DictReader(f))
    humans = {(r['source_document_id'], r['field']): r for r in human_rows}
    assert len(humans) == len(human_rows)
sample = {r['document_id']: r for r in sample_rows}
assert len(sample) == len(sample_rows)

roles = {'applicant', 'community_board', 'council_member', 'council_body',
         'government', 'independent_civic', 'resident', 'unclear', 'project_team'}
statements, summaries = [], []
seen = set()
source_segments = {}
for reader in ('ab' if version == 'v1' else 'abc' if version == 'v2' else [f'{n:02d}' for n in range(1, 11)]):
    if version == 'v1':
        packets = [json.loads(Path(f'../input/cpc_sol_actor_packet_{reader}_v1.json').read_text())]
        readings = [json.loads(Path(f'../input/cpc_sol_actor_reader_{reader}_v1.json').read_text())]
    elif version == 'v2':
        packets = json.loads(Path(f'../input/cpc_sol_actor_packets_{reader}_v2.json').read_text())
        readings = [json.loads(line) for line in Path(f'../input/cpc_sol_actor_reader_{reader}_v2.jsonl').read_text().splitlines()]
    else:
        packets = json.loads(Path(f'../input/cpc_sol_expansion_packets_{reader}_v3.json').read_text())
        readings = [json.loads(line) for line in Path(f'../input/cpc_sol_expansion_actors_{reader}_v3.jsonl').read_text().splitlines()]
    packets_by_id = {p['document_id']: p for p in packets}
    assert len(packets_by_id) == len(packets) == len(readings)
    assert set(packets_by_id) == {r['document_id'] for r in readings}
    for reading in readings:
        packet = packets_by_id[reading['document_id']]
        doc = packet['document_id']
        assert doc not in seen and reading['document_id'] == doc
        seen.add(doc)
        assert reading['application_number'] == packet['application_number']
        packet_hash = hashlib.sha256(json.dumps(packet, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        assert packet_hash == sample[doc]['packet_sha256']
        segments = {s['segment_id']: s for s in packet['segments']}
        source_segments.update({(doc, key): value for key, value in segments.items()})
        assert len(segments) == len(packet['segments'])
        assert len(reading['read_segment_ids']) == len(set(reading['read_segment_ids'])) == len(segments)
        assert set(reading['read_segment_ids']) == set(segments)
        assert len({a['actor'].casefold() for a in reading['actors']}) == len(reading['actors'])
        for number, actor in enumerate(reading['actors'], 1):
            assert actor['actor'].strip() and actor['role'] in roles and actor['statements']
            for claim_number, claim in enumerate(actor['statements'], 1):
                assert claim['kind'] in {'support', 'opposition', 'request', 'concern'}
                assert claim['summary'].strip() and claim['quote'].strip()
                segment = segments[claim['segment_id']]
                found = int(claim['quote'] in segment['text'])
                statements.append(dict(document_id=doc, application_number=packet['application_number'],
                    actor_number=number, actor=actor['actor'], role=actor['role'], statement_number=claim_number,
                    kind=claim['kind'], summary=claim['summary'], segment_id=claim['segment_id'],
                    quote=claim['quote'], quote_found=found, source_document_id=segment['source_document_id'],
                    source_application_number=segment['source_application_number'], source_role=segment['source_role'],
                    pdf_page=segment['pdf_page'], source_text_sha256=segment['source_text_sha256'],
                    public_pdf_url=segment['public_pdf_url']))
assert seen == ({r['document_id'] for r in checks} if version == 'v1' else set(sample))

if version == 'v1':
    # Zero means no qualifying statement extracted, not a validated absence of all evidence.
    # Requests retain their object in the inventory; no legacy request label is inferred.
    for check in checks:
        doc, field = check['document_id'], check['field']
        family, kind = field.split('__')
        role = {'council': 'council_member', 'civic': 'independent_civic'}[family]
        matches = [r for r in statements if r['document_id'] == doc and r['role'] == role and r['kind'] == kind]
        value = str(int(bool(matches)))
        previous = prior[doc, field]['value']
        summaries.append(dict(check, prior_v2=previous, actor_pass=value,
            prior_matches=int(previous == check['expected']), actor_pass_matches=int(value == check['expected']),
            actors='; '.join(dict.fromkeys(r['actor'] for r in matches)),
            segments='; '.join(dict.fromkeys(r['segment_id'] for r in matches))))

    save_csv(statements, list(statements[0]), '../output/cpc_sol_actor_statements_v1.csv',
             ['document_id', 'actor_number', 'statement_number'])
    save_csv(summaries, list(summaries[0]), '../output/cpc_sol_actor_comparison_v1.csv', ['document_id', 'field'])

    lines = ['# Separate Sol actor pass: two known reports', '',
        'One fresh GPT-6 Sol medium reader per report; no topic questions or repeat calls.', '',
        f"The readers return {len(set((r['document_id'], r['actor_number']) for r in statements))} actor/office records and "
        f"{len(statements)} statements. {sum(r['quote_found'] for r in statements)}/{len(statements)} quotations occur "
        'verbatim in their cited segments. Both readers report reading every supplied segment (50 and 8).', '',
        f"Targeted source checks: previous v2 matches {sum(r['prior_matches'] for r in summaries)}/8; "
        f"the separate actor pass matches {sum(r['actor_pass_matches'] for r in summaries)}/8. "
        'These are prespecified checks on known development reports, not an accuracy estimate or an independent actor census.', '',
        '| Application | Signal | Prior v2 | Actor pass | Source check |',
        '|---|---|---:|---:|---:|']
    for r in summaries:
        lines.append(f"| {r['application_number']} | {r['field'].replace('__', ' ').replace('_', ' ')} | "
                     f"{r['prior_v2']} | {r['actor_pass']} | {r['expected']} |")
    lines += ['', 'Support and opposition remain separate indicators. Requests and concerns remain attributed statements; '
        'an approval request is not converted into a legacy substantive-change request. Zero means no qualifying '
        'statement was extracted. Review the inventory for attribution, omitted actors and request objects; quote '
        'matches do not establish those. Prior topic and actor labels are unchanged.', '']
    Path('../output/cpc_sol_actor_findings_v1.md').write_text('\n'.join(lines))
else:
    # Preserve both stances. Legacy support/request is a broad approximation only.
    for doc, report in sample.items():
        for family, role, field in [('council', 'council_member', 'councilmember_position'),
                                    ('civic', 'independent_civic', 'civic_group_position')]:
            claims = [r for r in statements if r['document_id'] == doc and r['role'] == role]
            kinds = {r['kind'] for r in claims}
            position = ('both' if {'support', 'opposition'} <= kinds else 'opposition' if 'opposition' in kinds
                else 'support_or_request' if kinds & {'support', 'request'} else 'none_or_procedural')
            human = humans[doc, field]
            row = dict(document_id=doc, application_number=report['application_number'], project_name=report['project_name'],
                earlier_development=report['earlier_development'], selection_group=report['selection_group'], family=family,
                support=int('support' in kinds), opposition=int('opposition' in kinds), request=int('request' in kinds),
                concern=int('concern' in kinds), model_position=position, human_status=human['human_status'],
                human_value=human['human_value'], actors='; '.join(dict.fromkeys(r['actor'] for r in claims)),
                segments='; '.join(dict.fromkeys(r['segment_id'] for r in claims)))
            for coder in ['jacob', 'tyler']:
                value = human[f'{coder}_value']
                available = human[f'{coder}_coding_complete'] == '1' and value in {
                    'both', 'opposition', 'support_or_request', 'none_or_procedural'}
                row.update({f'{coder}_value': value, f'{coder}_complete': human[f'{coder}_coding_complete'],
                    f'{coder}_compared': int(available), f'{coder}_matches': int(value == position) if available else ''})
            if version == 'v3':
                row['involvement'] = int(bool(kinds))
                for coder in ['jacob', 'tyler']:
                    available = row[f'{coder}_compared']
                    reference = int(row[f'{coder}_value'] != 'none_or_procedural') if available else ''
                    row[f'{coder}_involvement'] = reference
                    row[f'{coder}_involvement_matches'] = int(row['involvement'] == reference) if available else ''
            summaries.append(row)
    assert len(summaries) == len(sample) * 2
    if version == 'v3':
        assert len(sample) == 20
        save_csv(statements, list(statements[0]), '../output/cpc_sol_actor_statements_v3.csv',
                 ['document_id', 'actor_number', 'statement_number'])
        save_csv(summaries, list(summaries[0]), '../output/cpc_sol_actor_comparison_v3.csv', ['document_id', 'family'])
        with open('../input/cpc_sol_control_expectations_v3.csv') as f:
            expectations = list(csv.DictReader(f))
        controls = []
        # These two frozen readings test changed instructions, not identical retries.
        for phase, control_readings, control_packets in [
            ('first_clarification', [json.loads(s) for s in Path('../input/cpc_sol_control_reader_v3.jsonl').read_text().splitlines()],
             json.loads(Path('../input/cpc_sol_controls_v3.json').read_text())),
            ('explicit_retention', [json.loads(s) for s in Path('../input/cpc_sol_operator_control_reader_v3.jsonl').read_text().splitlines()],
             json.loads(Path('../input/cpc_sol_operator_control_v3.json').read_text()))]:
            control_sources = {p['document_id']: p for p in control_packets}
            assert set(control_sources) == {r['document_id'] for r in control_readings}
            for reading in control_readings:
                doc = reading['document_id']
                source = {s['segment_id']: s['text'] for s in control_sources[doc]['segments']}
                assert len(reading['read_segment_ids']) == len(set(reading['read_segment_ids'])) == len(source)
                assert set(reading['read_segment_ids']) == set(source)
                for actor in reading['actors']:
                    assert actor['role'] in roles
                    for claim in actor['statements']:
                        assert claim['quote'] and claim['quote'] in source[claim['segment_id']]
                for check in [c for c in expectations if c['document_id'] == doc]:
                    if check['finding_id'] == 'bartow_operator':
                        selected = [a for a in reading['actors'] if 'animal care centers' in a['actor'].lower()]
                        passed = bool(selected) and all(a['role'] == 'project_team' for a in selected)
                    elif check['finding_id'] == 'bartow_signatures':
                        selected = [a for a in reading['actors'] if 'advocates' in a['actor'].lower()]
                        passed = bool(selected) and all(a['role'] in {'resident', 'unclear'} for a in selected)
                    elif check['finding_id'] == 'bartow_relocation':
                        selected = [a for a in reading['actors'] if 'community board 10' in a['actor'].lower()]
                        kinds = {s['kind'] for a in selected for s in a['statements']}
                        passed = {'opposition', 'request'} <= kinds and 'support' not in kinds
                    else:
                        assert check['finding_id'] == 'flatbush_companion'
                        selected = [a for a in reading['actors'] if any(s['kind'] == 'opposition'
                            and s['segment_id'] == 's0003' for s in a['statements'])]
                        passed = bool(selected) and all(a['role'] == 'unclear' for a in selected)
                    controls.append(dict(phase=phase, finding_id=check['finding_id'], document_id=doc,
                        application_number=reading['application_number'], passed=int(passed),
                        actors='; '.join(a['actor'] for a in selected), roles='; '.join(a['role'] for a in selected),
                        expectation=check['assessment']))
        save_csv(controls, list(controls[0]), '../output/cpc_sol_actor_controls_v3.csv', ['phase', 'finding_id'])
        lines = ['# Twenty-report actor expansion', '',
            'Each report receives the short clarified actor instruction separately from the topic questions. '
            'The inventory retains project-team actors, independent organizations and individuals separately, '
            'and preserves paired-report evidence. Support and opposition remain separate indicators.', '',
            f'Returned {len(sample)} reports, {len(set((r["document_id"], r["actor_number"]) for r in statements))} '
            f'actor/office records and {len(statements)} statements. '
            f'{sum(r["quote_found"] for r in statements)}/{len(statements)} quotations match exactly. '
            'Each reader declares every supplied segment read; neither check establishes exhaustive extraction.', '',
            '| Earlier development | Coder | Category matches |', '|---|---|---:|']
        for old in ['0', '1']:
            for coder in ['jacob', 'tyler']:
                rows = [r for r in summaries if r['earlier_development'] == old and r[f'{coder}_compared']]
                lines.append(f'| {"Yes" if old == "1" else "No"} | {coder.title()} | '
                    f'{sum(r[f"{coder}_matches"] for r in rows)}/{len(rows)} |')
        lines += ['', 'A separate involvement indicator counts any attributed support, opposition, request or '
            'concern. This preserves the user-approved interpretation that a Council concern prompting the '
            'proposal is involvement. It does not turn that concern into overall support. This additional '
            'diagnostic mapping leaves the frozen stance comparison unchanged.', '',
            '| Actor family | Coder | Stance matches | Involvement matches |', '|---|---|---:|---:|']
        for family in ['council', 'civic']:
            for coder in ['jacob', 'tyler']:
                selected = [r for r in summaries if r['family'] == family and r[f'{coder}_compared']]
                lines.append(f'| {family} | {coder.title()} | {sum(r[f"{coder}_matches"] for r in selected)}/{len(selected)} | '
                    f'{sum(r[f"{coder}_involvement_matches"] for r in selected)}/{len(selected)} |')
        council = [r for r in summaries if r['family'] == 'council'
            and r['human_status'] in {'human_agreement', 'single_human_coder'}
            and (r['jacob_compared'] or r['tyler_compared'])]
        positive = [r for r in council if r['human_value'] != 'none_or_procedural']
        lines += ['', f'Against available nonconflicting original references, Council involvement matches '
            f'{sum(r["involvement"] == int(r["human_value"] != "none_or_procedural") for r in council)}/{len(council)} '
            f'reports, including {sum(r["involvement"] for r in positive)}/{len(positive)} positives. '
            'These comparisons count each report once. They do not validate overall stance or completeness '
            'of the underlying PDFs.', '']
        lines += ['', 'Comparisons retain each original coder and the mixed category both. Agreement does not '
            'validate every actor or request. Concerns, membership in an organization and official organizational '
            'positions may differ. The sample is selected for validation, not population accuracy estimation.', '',
            '## Clarification controls', '',
            '| Instruction | Prior issue | Passed |', '|---|---|---:|']
        for r in controls:
            lines.append(f'| {r["phase"].replace("_", " ")} | {r["finding_id"].replace("_", " ")} | {r["passed"]} |')
        lines += ['', 'The first instruction ambiguously said to exclude project actors and the reader omitted the '
            'operator. The refined instruction explicitly retains the operator as project_team and was rechecked '
            'on Bartow. Flatbush uses the earlier control reading; its paired-report instruction did not change. '
            'Both attempts are preserved. Controls are known cases, separate from the twenty new reports.', '',
            'Raw readings and original human codes remain unchanged. No repeated completed readings, API use, '
            'corpus-scale processing or production relabeling occurred.', '']
        Path('../output/cpc_sol_actor_findings_v3.md').write_text('\n'.join(lines))
        sys.exit(0)
    save_csv(statements, list(statements[0]), '../output/cpc_sol_actor_statements_v2.csv',
             ['document_id', 'actor_number', 'statement_number'])
    save_csv(summaries, list(summaries[0]), '../output/cpc_sol_actor_comparison_v2.csv', ['document_id', 'family'])
    with open('../input/cpc_sol_actor_source_review_v2.csv') as f:
        review = list(csv.DictReader(f))
    for row in review:
        source = source_segments[row['document_id'], row['segment_id']]
        assert row['quote'] and row['quote'] in source['text']
        row.update(quote_found=1, source_document_id=source['source_document_id'],
            source_application_number=source['source_application_number'], pdf_page=source['pdf_page'])
    save_csv(review, list(review[0]), '../output/cpc_sol_actor_review_v2.csv', ['finding_id'])
    lines = ['# Actor inventories on six additional reports', '',
        'Three fresh GPT-6 Sol medium readers, two reports each. The short actor prompt is unchanged. '
        'The readers receive all retained text and no previous answers. No topic questions, repeat calls or API use.', '',
        f"Returned {len(seen)} reports, {len(set((r['document_id'], r['actor_number']) for r in statements))} actor/office "
        f"records and {len(statements)} statements. {sum(r['quote_found'] for r in statements)}/{len(statements)} "
        'quotations match their cited segments exactly. Every report lists all supplied segment IDs as read.', '',
        'Three reports avoid the earlier pilot/source-audit sources; three stress cases overlap older development '
        'work. Earlier bulk Jev use and original human coding are not excluded. These are targeted development '
        'reports, not a representative holdout. Quotes and coverage declarations do not establish completeness.', '',
        '| Earlier development | Coder | Category matches |', '|---|---|---:|']
    for old in ['0', '1']:
        for coder in ['jacob', 'tyler']:
            rows = [r for r in summaries if r['earlier_development'] == old and r[f'{coder}_compared']]
            lines.append(f"| {'Yes' if old == '1' else 'No'} | {coder.title()} | "
                         f"{sum(r[f'{coder}_matches'] for r in rows)}/{len(rows)} |")
    lines += ['', 'Each comparison uses a completed original coder with a recognized category; disagreements between '
        'coders remain visible. Both means explicit support and opposition coexist. Otherwise opposition takes '
        'precedence over support/request for the broad legacy comparison. Approval requests and proposal-change '
        'requests remain distinct in the underlying statements. A zero indicator does not prove absence.', '',
        '| Application | Actor family | Extracted category | Jacob | Tyler |', '|---|---|---|---|---|']
    display = {'support_or_request': 'support/request', 'none_or_procedural': 'none/procedural', '': '--'}
    for r in summaries:
        values = [display.get(r[k], r[k]) for k in ['model_position', 'jacob_value', 'tyler_value']]
        lines.append(f"| {r['application_number']} | {r['family']} | {' | '.join(values)} |")
    lines += ['', 'This comparison uses broad historical categories and cannot validate every actor or statement. '
        'Source review is required for mismatches and potentially missing positions. Original human labels and '
        'earlier model results are unchanged. Jev and bulk processing remain paused.', '']
    lines += ['## Source review after reading', '',
        'These observations are manager judgments made after seeing the readings, not independent gold labels. '
        'The saved review table includes exact quotations and source application IDs. No scores or original answers '
        'are revised in response to these observations.', '']
    for row in review:
        lines += [f"**{row['application_number']}: {row['actor']}.** {row['model_observation']} {row['assessment']}", '']
    Path('../output/cpc_sol_actor_findings_v2.md').write_text('\n'.join(lines))
