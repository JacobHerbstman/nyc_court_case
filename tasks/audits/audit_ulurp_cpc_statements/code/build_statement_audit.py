"""Validate frozen source comparisons and save statement- and report-level audit tables."""
import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict

import jsonschema

sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

with open('../output/cpc_statement_audit_sample.csv') as f:
    sample = list(csv.DictReader(f))
assert len(sample) == len({r['document_id'] for r in sample}) == 20
with open('../input/statement_schema.json') as f:
    schema = json.load(f)
csv.field_size_limit(10**8)
source = defaultdict(dict)
with open('../input/ulurp_cpc_reading_segments.csv') as f:
    for r in csv.DictReader(f):
        if r['document_id'] in {s['document_id'] for s in sample}:
            assert r['segment_id'] not in source[r['document_id']]
            source[r['document_id']][r['segment_id']] = ' '.join(r['text'].split())

summaries, assessments, findings = [], [], []
for selected in sample:
    doc = selected['document_id']
    with open(f'../input/{doc}_inventory.json', 'rb') as f:
        inventory_bytes = f.read()
    with open(f'../input/{doc}_extraction.json', 'rb') as f:
        extraction_bytes = f.read()
    inventory = json.loads(inventory_bytes)
    extraction = json.loads(extraction_bytes)
    with open(f'../input/{doc}_comparison.json') as f:
        comparison = json.load(f)
    assert comparison['document_id'] == doc
    assert comparison['inventory_sha256'] == hashlib.sha256(inventory_bytes).hexdigest()
    assert comparison['extraction_sha256'] == hashlib.sha256(extraction_bytes).hexdigest()
    assert len(comparison['source_segments_read']) == len(set(comparison['source_segments_read']))
    assert set(comparison['source_segments_read']) == set(source[doc])
    for answer in [inventory, extraction]:
        jsonschema.Draft202012Validator(schema).validate(answer)
        assert answer['document_id'] == doc
        assert set(answer['segments_read']) == set(source[doc])
        ids = {r['statement_id'] for r in answer['statements']}
        assert len(ids) == len(answer['statements'])
        for r in answer['statements']:
            assert set(r['segment_ids']) <= set(source[doc])
            assert any(' '.join(r['quote'].split()) in source[doc][s] for s in r['segment_ids'])
            assert set(r['response_statement_ids']) <= ids
    inventory_ids = {r['statement_id'] for r in inventory['statements']}
    extraction_ids = {r['statement_id'] for r in extraction['statements']}
    issues = {r['issue_id']: r for r in comparison['issues']}
    assert len(issues) == len(comparison['issues'])
    for issue in issues.values():
        assert issue['classification'] in {'clear_error', 'unclear', 'source_limit', 'granularity', 'inventory_omission'}
        assert set(issue['categories']) <= {'omission', 'unsupported', 'application', 'actor', 'project_team', 'topic', 'request_adoption', 'stage_position'}
        assert set(issue['inventory_statement_ids']) <= inventory_ids
        assert set(issue['extraction_statement_ids']) <= extraction_ids
        assert issue['segment_ids'] and set(issue['segment_ids']) <= set(source[doc])
        quote = ' '.join(issue['quote'].split())
        assert quote and any(quote in source[doc][s] for s in issue['segment_ids'])
        assert issue['explanation'].strip()
        findings.append(dict(document_id=doc, issue_id=issue['issue_id'],
            classification=issue['classification'], categories=json.dumps(issue['categories']),
            inventory_statement_ids=json.dumps(issue['inventory_statement_ids']),
            extraction_statement_ids=json.dumps(issue['extraction_statement_ids']),
            segment_ids=json.dumps(issue['segment_ids']), quote=issue['quote'], explanation=issue['explanation']))
    counts = {}
    for side, expected_ids, options in [
            ('inventory', inventory_ids, {'covered', 'partly_covered', 'omitted', 'unclear'}),
            ('extraction', extraction_ids, {'supported', 'clear_error', 'unclear', 'duplicate'})]:
        rows = comparison[f'{side}_assessments']
        assert len(rows) == len({r['statement_id'] for r in rows})
        assert {r['statement_id'] for r in rows} == expected_ids
        counts[side] = Counter(r['assessment'] for r in rows)
        for r in rows:
            assert r['assessment'] in options
            assert set(r['issue_ids']) <= set(issues)
            if r['assessment'] in {'omitted', 'partly_covered', 'clear_error', 'unclear', 'duplicate'}:
                assert r['issue_ids'], (doc, side, r['statement_id'])
            if r['assessment'] == 'clear_error':
                assert any(issues[i]['classification'] == 'clear_error' for i in r['issue_ids'])
            for issue_id in r['issue_ids']:
                assert r['statement_id'] in issues[issue_id][f'{side}_statement_ids']
            matches = r.get('extraction_statement_ids', [])
            assert set(matches) <= extraction_ids
            assessments.append(dict(document_id=doc, side=side, statement_id=r['statement_id'],
                assessment=r['assessment'], extraction_statement_ids=json.dumps(matches), issue_ids=json.dumps(r['issue_ids'])))
    summaries.append(dict(document_id=doc, application_number=selected['application_number'],
        project_name=selected['project_name'], audit_reason=selected['audit_reason'],
        human_coded=int(selected['human_coded']), unresolved_pages=int(selected['unresolved_pages']),
        segments_assessed=len(source[doc]), inventory_statements=len(inventory_ids),
        extracted_statements=len(extraction_ids),
        inventory_covered=counts['inventory']['covered'],
        inventory_partly_covered=counts['inventory']['partly_covered'],
        inventory_omitted=counts['inventory']['omitted'], inventory_unclear=counts['inventory']['unclear'],
        clear_omission_rows=sum(r['assessment'] in {'omitted', 'partly_covered'}
            and any(issues[i]['classification'] == 'clear_error' and 'omission' in issues[i]['categories']
                for i in r['issue_ids']) for r in comparison['inventory_assessments']),
        extraction_supported=counts['extraction']['supported'],
        extraction_clear_error=counts['extraction']['clear_error'],
        extraction_unclear=counts['extraction']['unclear'], extraction_duplicate=counts['extraction']['duplicate'],
        clear_error_issues=sum(i['classification'] == 'clear_error' for i in issues.values()),
        unresolved_issues=sum(i['classification'] == 'unclear' for i in issues.values()),
        source_limit_issues=sum(i['classification'] == 'source_limit' for i in issues.values()),
        review_notes=comparison['review_notes']))

save_csv(summaries, list(summaries[0]), '../output/cpc_statement_audit_summary.csv', ['document_id'])
save_csv(assessments, list(assessments[0]), '../output/cpc_statement_audit_assessments.csv', ['document_id', 'side', 'statement_id'])
save_csv(findings, ['document_id', 'issue_id', 'classification', 'categories', 'inventory_statement_ids',
    'extraction_statement_ids', 'segment_ids', 'quote', 'explanation'],
    '../output/cpc_statement_audit_findings.csv', ['document_id', 'issue_id'])
print(f'Saved source comparisons for {len(summaries)} reports; this stress sample is not population accuracy.')
