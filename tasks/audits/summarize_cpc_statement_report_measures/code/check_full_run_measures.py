"""Check report-level measures on the full run's completed reports so far.

Four outputs:
0. Report-level measures for every completed report.
1. Agreement with the earlier human codes on every completed human-coded report.
2. Test-retest: the first 100 reports were read twice, on September 26 and again
   in the full run, under instructions that differ only by the procedural field.
3. Statement rows against report length, to spot reports read too thinly.
"""
# Interactive use: cd to tasks/audits/summarize_cpc_statement_report_measures/code, then
# python3 check_full_run_measures.py
import csv
import sys
from collections import defaultdict

sys.path.insert(0, '../../../_lib')
from data_reports import save_csv
from cpc_statement_measures import measures, parse, speaker_counts

csv.field_size_limit(10**8)


def report_measures(path):
    rows = defaultdict(list)
    with open(path) as f:
        for r in csv.DictReader(f):
            rows[r['document_id']].append(parse(r))
    return {doc: {**measures(rs), **speaker_counts(rs)} for doc, rs in rows.items()}, rows


full, full_rows = report_measures('../input/ulurp_cpc_statements_full_sol_high_20260927.csv')
first100, _ = report_measures('../input/ulurp_cpc_statements.csv')
with open('../input/ulurp_cpc_statement_status_full_sol_high_20260927.csv') as f:
    complete = {r['document_id'] for r in csv.DictReader(f) if r['status'] == 'complete'}
assert set(full) == complete

measure_rows = [{'document_id': doc, 'measure': m, 'value': v} for doc in sorted(full) for m, v in full[doc].items()]
save_csv(measure_rows, ['document_id', 'measure', 'value'], '../output/cpc_statement_full_run_report_measures.csv',
         key=['document_id', 'measure'])

human_rows = []
with open('../input/ulurp_cpc_human_coding.csv') as f:
    for r in csv.DictReader(f):
        doc, measure = r['source_document_id'], r['field']
        if doc not in full or measure not in full[doc] or full[doc][measure] == '' or r['human_value'] == '':
            continue
        derived = full[doc][measure]
        human_rows.append({'document_id': doc, 'measure': measure, 'human_value': r['human_value'],
                           'human_status': r['human_status'], 'derived_value': derived,
                           'agree': str(int(derived == r['human_value'])),
                           'absolute_difference': str(abs(int(derived) - int(r['human_value']))) if measure.endswith('_speakers') else ''})
save_csv(human_rows, list(human_rows[0]), '../output/cpc_statement_full_run_human_agreement.csv', key=['document_id', 'measure'])

retest_rows = [{'document_id': doc, 'measure': measure, 'first100_value': value, 'full_run_value': full[doc][measure],
                'agree': str(int(value == full[doc][measure]))}
               for doc in sorted(set(first100) & set(full)) for measure, value in first100[doc].items()
               if value != '' and full[doc][measure] != '']
save_csv(retest_rows, list(retest_rows[0]), '../output/cpc_statement_full_run_retest.csv', key=['document_id', 'measure'])

length_rows = []
with open('../input/ulurp_cpc_reading_roster.csv') as f:
    for r in csv.DictReader(f):
        if r['document_id'] in full:
            n = len(full_rows[r['document_id']])
            length_rows.append({'document_id': r['document_id'], 'year': r['year'], 'characters': r['segment_characters'],
                                'unresolved_segments': r['unresolved_segments'], 'statement_rows': str(n),
                                'rows_per_10k_characters': f"{n / int(r['segment_characters']) * 10**4:.2f}"})
save_csv(length_rows, list(length_rows[0]), '../output/cpc_statement_full_run_length.csv', key=['document_id'])
