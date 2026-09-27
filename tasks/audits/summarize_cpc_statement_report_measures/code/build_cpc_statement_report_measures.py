"""Derive report-level CPC measures from statement rows, then test them.

Three outputs:
1. One row per report and measure for the 100 read reports.
2. For the 20 audited reports, each measure recomputed after applying the
   source audit (drop rows judged clear errors, add inventory statements the
   extraction missed or only partly captured, and the inventory counterparts of
   erroneous rows), to see whether row-level errors move report-level measures.
3. Agreement with the earlier human codes on the 20 human-coded reports.
"""
import csv
import json
import sys
from collections import defaultdict

sys.path.insert(0, '../../../_lib')
from data_reports import save_csv
from cpc_statement_measures import measures, parse, speaker_counts

csv.field_size_limit(10**8)

statements = defaultdict(dict)
with open('../input/ulurp_cpc_statements.csv') as f:
    for r in csv.DictReader(f):
        statements[r['document_id']][int(r['statement_id'])] = parse(r)
assert len(statements) == 100

report_rows = []
for doc, rows in sorted(statements.items()):
    for measure, value in {**measures(rows.values()), **speaker_counts(rows.values())}.items():
        report_rows.append({'document_id': doc, 'measure': measure, 'value': value})
save_csv(report_rows, ['document_id', 'measure', 'value'], '../output/cpc_statement_report_measures.csv',
         key=['document_id', 'measure'])

# Audit sensitivity: rebuild each audited report's rows from the source audit.
assessments = defaultdict(dict)
with open('../input/cpc_statement_audit_assessments.csv') as f:
    for r in csv.DictReader(f):
        assessments[r['document_id'], r['side']][int(r['statement_id'])] = (r['assessment'], json.loads(r['extraction_statement_ids']))
audited = sorted({doc for doc, _ in assessments})
assert len(audited) == 20

sensitivity_rows = []
for doc in audited:
    extraction = assessments[doc, 'extraction']
    assert set(extraction) == set(statements[doc]), doc
    with open(f'../input/{doc}_inventory.json') as f:
        inventory = {s['statement_id']: parse(s) for s in json.load(f)['statements']}
    errors = {i for i, (assessment, _) in extraction.items() if assessment == 'clear_error'}
    unclear = {i for i, (assessment, _) in extraction.items() if assessment == 'unclear'}
    added = [inventory[i] for i, (assessment, matched) in assessments[doc, 'inventory'].items()
             if assessment in {'omitted', 'partly_covered'} or set(matched) & errors]
    kept = [r for i, r in statements[doc].items() if i not in errors]
    versions = {
        'original': measures(statements[doc].values()),
        'audit_corrected': measures(kept + added),
        'audit_corrected_drop_unclear': measures([r for i, r in statements[doc].items() if i not in errors | unclear] + added),
    }
    for measure, value in versions['original'].items():
        sensitivity_rows.append({'document_id': doc, 'measure': measure, 'original': value,
                                 'audit_corrected': versions['audit_corrected'][measure],
                                 'audit_corrected_drop_unclear': versions['audit_corrected_drop_unclear'][measure],
                                 'clear_error_rows': len(errors), 'unclear_rows': len(unclear), 'inventory_rows_added': len(added)})
save_csv(sensitivity_rows, list(sensitivity_rows[0]), '../output/cpc_statement_measure_audit_sensitivity.csv',
         key=['document_id', 'measure'])

# Agreement with earlier human codes, for fields defined in both.
derived = {(r['document_id'], r['measure']): r['value'] for r in report_rows}
corrected = {(r['document_id'], r['measure']): r['audit_corrected'] for r in sensitivity_rows}
human_rows = []
with open('../input/ulurp_cpc_human_coding.csv') as f:
    for r in csv.DictReader(f):
        key = (r['source_document_id'], r['field'])
        if key not in derived or derived[key] == '' or r['human_value'] == '':
            continue
        human_rows.append({'document_id': key[0], 'measure': key[1], 'human_value': r['human_value'],
                           'human_status': r['human_status'], 'derived_value': derived[key],
                           'agree': str(int(derived[key] == r['human_value'])),
                           'audit_corrected_value': corrected.get(key, ''),
                           'audit_corrected_agree': str(int(corrected[key] == r['human_value'])) if key in corrected else ''})
assert len({r['document_id'] for r in human_rows}) == 20
save_csv(human_rows, list(human_rows[0]), '../output/cpc_statement_measure_human_agreement.csv',
         key=['document_id', 'measure'])
