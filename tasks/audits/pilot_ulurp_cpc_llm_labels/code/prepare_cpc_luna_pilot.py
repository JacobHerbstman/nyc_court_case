#!/usr/bin/env python3
"""Prepare four complete reports for a small blinded Luna reading pilot."""
import csv
import hashlib
import json
import sys
from pathlib import Path
sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

with open('../output/cpc_jev_sample_v13.csv') as f:
    reports = {r['document_id']: r for r in csv.DictReader(f)}
with open('../output/cpc_jev_segments_v13.csv') as f:
    segments = list(csv.DictReader(f))
assert len({(r['document_id'], r['segment_id']) for r in segments}) == len(segments)
assignments = {
    'a': ['18bfb1ad1fab83a2555e', '28cc037f9056f9cd9141'],
    'b': ['e70fca539711f235a7ab'],
    'c': ['a89b9ba39beede21a028']}
sample = []
for reader, documents in assignments.items():
    packets = []
    for doc in documents:
        report = reports[doc]
        pages = [r for r in segments if r['document_id'] == doc]
        assert pages and report['preparation_status'] == 'ready'
        packet = dict(document_id=doc, application_number=report['application_number'],
            project_name=report['project_name'], segments=pages)
        packets.append(packet)
        sample.append(dict(document_id=doc, application_number=report['application_number'],
            project_name=report['project_name'], reader=reader, segments=len(pages),
            source_characters=sum(len(r['text']) for r in pages),
            packet_sha256=hashlib.sha256(json.dumps(packet, sort_keys=True, ensure_ascii=False).encode()).hexdigest(),
            requested_model='gpt-6-luna', reasoning='medium'))
    Path(f'../output/cpc_luna_packets_{reader}_v1.json').write_text(json.dumps(packets, indent=2, ensure_ascii=False) + '\n')
assert len(sample) == len({r['document_id'] for r in sample}) == 4
save_csv(sample, list(sample[0]), '../output/cpc_luna_sample_v1.csv', ['document_id'])
print('Prepared four complete reports for three blinded Luna readers. No inference.')
