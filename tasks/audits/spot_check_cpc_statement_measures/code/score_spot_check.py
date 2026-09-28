"""Score the first pass and the derived measures against the reviewed values.

The reference for each of the 315 items is the reviewed value where Jacob (or a page
check) reviewed it. Elsewhere it is the first-pass value. Those unreviewed items all
agreed with the derived value at review time, and all 25 sampled agreements were
confirmed. Derived values are re-read from the current rules, so this rescores them
after each rule change. `review_group` in the review table keeps the original strata.
"""
# Interactive use: cd to tasks/audits/spot_check_cpc_statement_measures/code, then
# python3 score_spot_check.py
import csv
import sys

sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

with open('../output/cpc_spot_check_items.csv') as f:
    items = list(csv.DictReader(f))
with open('jacob_spot_check_review.csv') as f:
    review = {(r['document_id'], r['measure']): r for r in csv.DictReader(f)}
assert review.keys() <= {(i['document_id'], i['measure']) for i in items}

rows = []
for measure in sorted({i['measure'] for i in items}) + ['all']:
    group = [i for i in items if measure in ('all', i['measure'])]
    reference = [review[i['document_id'], i['measure']]['reviewed_value'] if (i['document_id'], i['measure']) in review
                 else i['first_pass_value'] for i in group]
    rows.append(dict(measure=measure, items=len(group), reviewed=sum((i['document_id'], i['measure']) in review for i in group),
                     first_pass_correct=sum(i['first_pass_value'] == ref for i, ref in zip(group, reference)),
                     derived_correct=sum(i['derived_value'] == ref for i, ref in zip(group, reference)),
                     derived_blank=sum(i['derived_value'] == '' and ref != '' for i, ref in zip(group, reference))))
save_csv(rows, list(rows[0]), '../output/cpc_spot_check_scores.csv', key=['measure'])
