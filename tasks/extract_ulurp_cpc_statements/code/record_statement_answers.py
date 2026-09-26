#!/usr/bin/env python3
"""Validate answers written by Codex app readers and log them in attempts.jsonl.

Each responses/<document_id>_part<k>_attempt<a>.json not yet logged is checked
with the same rules as run_statement_extraction.py and appended as one attempt.
Answer files are never edited. Prints the status of every prepared part.
"""
# Interactive use: cd to tasks/extract_ulurp_cpc_statements/code, then
# python3 record_statement_answers.py pilot_app_20260926
import csv
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from cpc_statement_packets import packet_parts, read_inputs, validate_answer

run_id = sys.argv[1]
run_dir = Path('../../../data_raw/cpc_statement_extraction') / run_id
settings = json.loads((run_dir / 'run.json').read_text())
with (run_dir / 'prompts.csv').open() as f:
    manifest = {(r['document_id'], int(r['part'])): r for r in csv.DictReader(f)}
attempts_file = run_dir / 'attempts.jsonl'
logged = [json.loads(line) for line in attempts_file.read_text().splitlines()] if attempts_file.exists() else []
logged_keys = {(a['document_id'], a['part'], a['attempt']) for a in logged}

roster, segments = read_inputs({doc for doc, _ in manifest})
parts = {}
for doc in {doc for doc, _ in manifest}:
    for part in packet_parts(roster[doc], segments[doc], settings['max_packet_characters']):
        assert hashlib.sha256(part['text'].encode()).hexdigest() == manifest[doc, part['part']]['packet_sha256'], \
            f'{doc} part {part["part"]}: reading text changed since prompts were prepared'
        parts[doc, part['part']] = part

new = []
for path in sorted((run_dir / 'responses').glob('*.json')):
    m = re.fullmatch(r'(\w+)_part(\d+)_attempt(\d+)\.json', path.name)
    assert m and (m.group(1), int(m.group(2))) in parts, f'Unexpected answer file {path.name}'
    doc, k, attempt = m.group(1), int(m.group(2)), int(m.group(3))
    if (doc, k, attempt) in logged_keys:
        continue
    answer, errors = validate_answer(path.read_text(), doc, parts[doc, k]['segments'])
    new.append(dict(document_id=doc, part=k, parts=parts[doc, k]['parts'], attempt=attempt, route=settings['route'],
                    model=settings['model'], reasoning=settings['reasoning'],
                    packet_sha256=manifest[doc, k]['packet_sha256'], prompt_sha256=manifest[doc, k]['prompt_sha256'],
                    instruction_sha256=settings['instruction_sha256'], schema_sha256=settings['schema_sha256'],
                    started_at='', ended_at=datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(timespec='seconds'),
                    exit_code='', input_tokens='', cached_input_tokens='', output_tokens='',
                    validation_status='valid' if answer else 'invalid', validation_errors=errors[:20],
                    statement_rows=len(answer['statements']) if answer else ''))
with attempts_file.open('a') as f:
    for record in new:
        f.write(json.dumps(record) + '\n')

for (doc, k), r in sorted(manifest.items()):
    tries = [a for a in logged + new if a['document_id'] == doc and a['part'] == k]
    status = 'valid' if any(a['validation_status'] == 'valid' for a in tries) else \
             'invalid' if tries else 'no answer yet'
    rows = next((a['statement_rows'] for a in tries if a['validation_status'] == 'valid'), '')
    print(f"{doc} part {k}/{r['parts']}: {status}; attempts {len(tries)}; rows {rows}")
    for a in tries:
        if a['validation_status'] != 'valid':
            for error in a['validation_errors'][:5]:
                print(f"    attempt {a['attempt']}: {error}")
print(f'Logged {len(new)} new answer files.')
