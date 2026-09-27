"""Order the full roster for one reading run.

The first-100 sample goes first, so its new readings can be compared with the
September 26 run. The remaining single-packet reports follow in seeded random
order, so any stopping point is a random sample of them. Reports too long for one
packet go last: they need a whole-report reconciliation after their parts.
"""
# Interactive use: cd to tasks/extract_ulurp_cpc_statements/code, then
# python3 order_statement_run.py cpc-statements-full-20260927 400000
import csv
import hashlib
import sys

sys.path.insert(0, '../../_lib')
from data_reports import save_csv
from cpc_statement_packets import packet_parts, read_inputs

seed, max_characters = sys.argv[1], int(sys.argv[2])
with open('../output/ulurp_cpc_statement_sample.csv') as f:
    first100 = [r['document_id'] for r in csv.DictReader(f)]
with open('../input/ulurp_cpc_reading_roster.csv') as f:
    all_ids = [r['document_id'] for r in csv.DictReader(f)]
assert len(first100) == 100 and set(first100) <= set(all_ids)
roster, segments = read_inputs(set(all_ids))
parts = {doc: packet_parts(roster[doc], segments[doc], max_characters)[0]['parts'] for doc in all_ids}

rest = sorted((doc for doc in all_ids if doc not in set(first100)),
              key=lambda doc: hashlib.sha256((seed + '|' + doc).encode()).hexdigest())
order = ([(doc, 'first100') for doc in first100]
         + [(doc, 'single_packet') for doc in rest if parts[doc] == 1]
         + [(doc, 'split_report') for doc in rest if parts[doc] > 1])
rows = [dict(document_id=doc, run_order=i, order_group=group, parts=parts[doc]) for i, (doc, group) in enumerate(order, 1)]
assert len(rows) == len(all_ids)
save_csv(rows, ['document_id', 'run_order', 'order_group', 'parts'], '../output/ulurp_cpc_statement_run_order.csv', ['document_id'])
print({g: sum(r['order_group'] == g for r in rows) for g in ('first100', 'single_packet', 'split_report')})
