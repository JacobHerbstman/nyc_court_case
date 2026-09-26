#!/usr/bin/env python3
"""Turn saved statement answers for one run into a statement table and a coverage table.

No model calls. Every narrative on the roster gets a status row; statements come
only from narratives whose parts all have valid answers.
"""
# Interactive use: cd to tasks/extract_ulurp_cpc_statements/code, then
# python3 build_ulurp_cpc_statements.py pilot_20260926
import csv
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, '../../_lib')
from data_reports import save_csv
from cpc_statement_packets import SCHEMA, packet_parts, read_inputs, validate_answer

run_id = sys.argv[1]
run_dir = Path('../../../data_raw/cpc_statement_extraction') / run_id
settings = json.loads((run_dir / 'run.json').read_text())
attempts = [json.loads(line) for line in (run_dir / 'attempts.jsonl').read_text().splitlines()]

with open('../input/ulurp_cpc_reading_roster.csv') as f:
    roster_rows = list(csv.DictReader(f))
attempted = {a['document_id'] for a in attempts}
roster, segments = read_inputs(attempted)
meta = {(s['document_id'], s['segment_id']): s for doc in segments for s in segments[doc]}

by_part = defaultdict(list)
for a in attempts:
    by_part[a['document_id'], a['part']].append(a)

fields = list(SCHEMA['properties']['statements']['items']['properties'])
statements, status = [], []
for narrative in roster_rows:
    doc = narrative['document_id']
    row = dict(document_id=doc, application_number=narrative['application_number'], status='not_run',
               parts='', valid_parts=0, attempts=0, statement_rows=0,
               input_tokens=0, cached_input_tokens=0, output_tokens=0)
    if doc in attempted:
        parts = packet_parts(narrative, segments[doc], settings['max_packet_characters'])
        doc_rows = []
        for part in parts:
            tries = by_part[doc, part['part']]
            assert all(a['packet_sha256'] == hashlib.sha256(part['text'].encode()).hexdigest() for a in tries), \
                f'{doc} part {part["part"]}: current packet differs from the one that was sent'
            for a in tries:
                row['attempts'] += 1
                for k in ['input_tokens', 'cached_input_tokens', 'output_tokens']:
                    row[k] += int(a[k] or 0)
            valid = [a for a in tries if a['validation_status'] == 'valid']
            if not valid:
                continue
            a = valid[-1]
            raw = (run_dir / 'responses' / f"{doc}_part{a['part']}_attempt{a['attempt']}.json").read_text()
            answer, errors = validate_answer(raw, doc, part['segments'])
            assert answer, f'{doc} part {a["part"]}: saved valid answer no longer validates: {errors[:3]}'
            row['valid_parts'] += 1
            for s in answer['statements']:
                cited = [meta[doc, i] for i in s['segment_ids']]
                out = dict(document_id=doc, focal_application_number=narrative['application_number'],
                           part=part['part'])
                out.update({k: ';'.join(map(str, s[k])) if isinstance(s[k], list) else s[k] for k in fields})
                out.update(cited_pdf_pages=';'.join(c['pdf_page'] for c in cited),
                           cited_source_applications=';'.join(sorted({c['source_application_number'] for c in cited})),
                           cited_page_scopes=';'.join(sorted({c['page_scope'] for c in cited})))
                doc_rows.append(out)
        row['parts'] = len(parts)
        row['status'] = 'complete' if row['valid_parts'] == len(parts) else 'failed'
        if row['status'] == 'complete':
            statements.extend(doc_rows)
            row['statement_rows'] = len(doc_rows)
    status.append(row)

columns = ['document_id', 'focal_application_number', 'part'] + fields + \
          ['cited_pdf_pages', 'cited_source_applications', 'cited_page_scopes']
save_csv(statements, columns, '../output/ulurp_cpc_statements.csv', ['document_id', 'part', 'statement_id'])
save_csv(status, list(status[0]), '../output/ulurp_cpc_statement_status.csv', ['document_id'])
counts = defaultdict(int)
for r in status:
    counts[r['status']] += 1
print(f'{run_id}: {dict(counts)}; {len(statements)} statement rows.')
