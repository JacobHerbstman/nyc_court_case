"""Preserve source-based comparisons with the older, imperfect human labels."""
import csv
import json
import re
import sys

sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

with open('../output/cpc_statement_audit_sample.csv') as f:
    sample = [r for r in csv.DictReader(f) if r['human_coded'] == '1']
assert len(sample) == 8
with open('../input/human_reference.csv') as f:
    reference = list(csv.DictReader(f))

rows = []
for selected in sample:
    doc = selected['document_id']
    expected = {(r['source_document_id'], r['field']): r for r in reference
                if doc in re.split(r'[;|,\s]+', r['represented_narrative_ids'])}
    matching = [r for r in reference if doc in re.split(r'[;|,\s]+', r['represented_narrative_ids'])]
    assert len(expected) == len(matching) and expected
    with open(f'../input/{doc}_human_comparison.json') as f:
        answer = json.load(f)
    with open(f'../input/{doc}_extraction.json') as f:
        extraction = json.load(f)
    statement_ids = {r['statement_id'] for r in extraction['statements']}
    assert answer['document_id'] == doc
    keys = [(r['source_document_id'], r['field']) for r in answer['rows']]
    assert len(keys) == len(set(keys)) and set(keys) == set(expected)
    for r in answer['rows']:
        original = expected[r['source_document_id'], r['field']]
        for field in ['human_value', 'human_status', 'jacob_value', 'tyler_value']:
            assert str(r[field]) == original[field], (doc, r['field'], field)
        assert isinstance(r['extraction_value'], str) and r['extraction_value'].strip()
        assert r['comparison'] in {'agree', 'disagree', 'unclear', 'not_comparable', 'human_disagreement'}
        assert set(r['extraction_statement_ids']) <= statement_ids
        if r['comparison'] in {'agree', 'disagree'}:
            assert original['human_value'] and r['extraction_value'] not in {'unclear', 'not_comparable'}
            assert (original['human_value'] == r['extraction_value']) == (r['comparison'] == 'agree')
        assert r['explanation'].strip()
        rows.append(dict(document_id=doc, source_document_id=r['source_document_id'], field=r['field'],
            human_value=r['human_value'], human_status=r['human_status'], jacob_value=r['jacob_value'],
            tyler_value=r['tyler_value'], extraction_value=r['extraction_value'], comparison=r['comparison'],
            extraction_statement_ids=json.dumps(r['extraction_statement_ids']), explanation=r['explanation'],
            review_notes=answer['review_notes']))
save_csv(rows, list(rows[0]), '../output/cpc_statement_human_comparison.csv',
         ['document_id', 'source_document_id', 'field'])
print(f'Saved {len(rows)} human-reference comparisons across eight reports; labels are not gold standards.')
