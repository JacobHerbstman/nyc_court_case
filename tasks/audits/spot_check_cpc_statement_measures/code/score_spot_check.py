"""Score the first pass and the derived measures against the reviewed values.

The review covers every item where the two differ, and a seeded sample of 25 of
the items where they agree. By measure, it reports how often each method matches
the reviewed value in both groups. Overall accuracy on all 315 items weights each
group by its size: disagreements are fully reviewed; for agreements, the sampled
share where the reviewed value differs from the shared value is assumed to hold
for all of them.
"""
# Interactive use: cd to tasks/audits/spot_check_cpc_statement_measures/code, then
# python3 score_spot_check.py
import csv
import sys

sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

with open('../output/cpc_spot_check_items.csv') as f:
    items = {(r['document_id'], r['measure']): r for r in csv.DictReader(f)}
with open('jacob_spot_check_review.csv') as f:
    review = {(r['document_id'], r['measure']): r for r in csv.DictReader(f)}
assert set(review) == {k for k, r in items.items() if r['priority'] == '1'}

rows = []
for measure in sorted({m for _, m in items}):
    for group, match in (('disagreement', '0'), ('agreement_sample', '1')):
        keys = [k for k in review if k[1] == measure and items[k]['first_pass_matches_derived'] == match]
        rows.append(dict(measure=measure, group=group, reviewed=len(keys),
                         first_pass_correct=sum(items[k]['first_pass_value'] == review[k]['reviewed_value'] for k in keys),
                         derived_correct=sum(items[k]['derived_value'] == review[k]['reviewed_value'] for k in keys)))

agreements = [k for k, r in items.items() if r['first_pass_matches_derived'] == '1']
sampled = [k for k in agreements if k in review]
agreement_error = sum(items[k]['first_pass_value'] != review[k]['reviewed_value'] for k in sampled) / len(sampled)
disagreements = [k for k, r in items.items() if r['first_pass_matches_derived'] == '0']
for method in ('first_pass', 'derived'):
    correct = (len(agreements) * (1 - agreement_error)
               + sum(items[k][f'{method}_value'] == review[k]['reviewed_value'] for k in disagreements))
    rows.append(dict(measure='all', group=f'estimated_accuracy_{method}', reviewed=len(items),
                     first_pass_correct=f'{correct / len(items):.3f}' if method == 'first_pass' else '',
                     derived_correct=f'{correct / len(items):.3f}' if method == 'derived' else ''))
save_csv(rows, list(rows[0]), '../output/cpc_spot_check_scores.csv', key=['measure', 'group'])
