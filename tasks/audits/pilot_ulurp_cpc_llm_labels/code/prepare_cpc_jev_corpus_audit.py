#!/usr/bin/env python3
"""Prepare the preselected random source audit, blinded to all model/human labels."""
import csv
import json
from collections import defaultdict
with open('../input/cpc_jev_roster_corpus_v1.csv') as f:
    roster = [r for r in csv.DictReader(f) if r['random_audit']=='1']
assert len(roster)==len({r['document_id'] for r in roster})
selected = {r['document_id'] for r in roster}
pages = defaultdict(list)
with open('../input/cpc_jev_segments_corpus_v1.csv') as f:
    for r in csv.DictReader(f):
        if r['document_id'] in selected:
            pages[r['document_id']].append(r)
with open('../output/cpc_jev_corpus_audit_packets_v1.jsonl','w') as f:
    for i,r in enumerate(roster):
        f.write(json.dumps(dict(document_id=r['document_id'],application_number=r['application_number'],reader='abc'[i%3],source_segments=pages[r['document_id']]),ensure_ascii=False)+'\n')
print(f'{len(roster)} random reports prepared without Jev answers or human references.')
