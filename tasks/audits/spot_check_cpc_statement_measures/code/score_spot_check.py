"""Score the first pass, the derived measures and the row-judging versions against the
reviewed values.

The reference for each of the 315 items is the reviewed value where Jacob (or a page
check) reviewed it. Elsewhere it is the first-pass value. Those unreviewed items all
agreed with the derived value at review time, and all 25 sampled agreements were
confirmed. Derived values are re-read from the current rules, so this rescores them
after each rule change. `review_group` in the review table keeps the original strata.

Row judging (data_raw/cpc_spot_check_row_judging/) answers the same codebook from Sol's
statement rows, without the PDF: `rows_only`, and `rows_hearing` with the report's
hearing pages added. Their values and quotes are validated here. Unreviewed items take
the first pass as the reference, which slightly favors the first pass.
"""
# Interactive use: cd to tasks/audits/spot_check_cpc_statement_measures/code, then
# python3 score_spot_check.py
import csv
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

with open('../output/cpc_spot_check_items.csv') as f:
    items = list(csv.DictReader(f))
with open('jacob_spot_check_review.csv') as f:
    review = {(r['document_id'], r['measure']): r for r in csv.DictReader(f)}
assert review.keys() <= {(i['document_id'], i['measure']) for i in items}
csv.field_size_limit(10**9)
docs = {i['document_id'] for i in items}
segments = {}
with open('../input/ulurp_cpc_reading_segments.csv') as f:
    for r in csv.DictReader(f):
        if r['document_id'] in docs:
            segments[r['document_id'], r['segment_id']] = ' '.join(r['text'].split())
VARIANTS = ('rows_only', 'rows_hearing')


def quoted(quote, text):
    """Every piece of a quote, split at ellipses, appears in the segment in order."""
    start = 0
    for piece in (' '.join(p.split()) for p in re.split(r'\.\.\.|\u2026', quote)):
        if piece:
            start = text.find(piece, start)
            if start < 0:
                return False
            start += len(piece)
    return True

judged = {}
for variant in VARIANTS:
    for doc in docs:
        answer = json.loads(Path(f'../input/row_judging/{variant}/{doc}.json').read_text())
        assert answer['document_id'] == doc
        for i in (i for i in items if i['document_id'] == doc):
            a = answer['measures'][i['measure']]
            value = str(a['value'])
            if i['measure'].endswith('_speakers'):
                assert value == '' or value.isdigit(), (variant, doc, i['measure'], value)
            elif i['measure'].endswith('_position'):
                assert value in {'support_or_request', 'opposition', 'none_or_procedural'}, (variant, doc, value)
            else:
                assert value in {'0', '1'}, (variant, doc, i['measure'], value)
            if a.get('quote'):
                assert variant == 'rows_hearing' and quoted(a['quote'], segments[doc, a['segment_id']]), (variant, doc)
            judged[variant, doc, i['measure']] = value

rows = []
for measure in sorted({i['measure'] for i in items}) + ['all']:
    group = [i for i in items if measure in ('all', i['measure'])]
    reference = [review[i['document_id'], i['measure']]['reviewed_value'] if (i['document_id'], i['measure']) in review
                 else i['first_pass_value'] for i in group]
    rows.append(dict(measure=measure, items=len(group), reviewed=sum((i['document_id'], i['measure']) in review for i in group),
                     first_pass_correct=sum(i['first_pass_value'] == ref for i, ref in zip(group, reference)),
                     derived_correct=sum(i['derived_value'] == ref for i, ref in zip(group, reference)),
                     **{f'{v}_correct': sum(judged[v, i['document_id'], i['measure']] == ref for i, ref in zip(group, reference))
                        for v in VARIANTS},
                     derived_blank=sum(i['derived_value'] == '' and ref != '' for i, ref in zip(group, reference))))
save_csv(rows, list(rows[0]), '../output/cpc_spot_check_scores.csv', key=['measure'])
