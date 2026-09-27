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
for name, field in [('statement_instructions.md', 'instruction_sha256'), ('statement_schema.json', 'schema_sha256')]:
    assert hashlib.sha256(Path(name).read_bytes()).hexdigest() == settings[field], f'{name} changed; use a new run.'
with (run_dir / 'prompts.csv').open() as f:
    manifest = {(r['document_id'], int(r['part'])): r for r in csv.DictReader(f)}
attempts_file = run_dir / 'attempts.jsonl'
logged = [json.loads(line) for line in attempts_file.read_text().splitlines()] if attempts_file.exists() else []
logged_keys = {(a['document_id'], a['part'], a['attempt']) for a in logged}
schema = json.loads((run_dir / 'statement_schema.json').read_text())

roster, segments = read_inputs({doc for doc, _ in manifest})
parts = {}
for doc in {doc for doc, _ in manifest}:
    for part in packet_parts(roster[doc], segments[doc], settings['max_packet_characters']):
        assert hashlib.sha256(part['text'].encode()).hexdigest() == manifest[doc, part['part']]['packet_sha256'], \
            f'{doc} part {part["part"]}: reading text changed since prompts were prepared'
        parts[doc, part['part']] = part
    if len([key for key in manifest if key[0] == doc]) > 1:
        # Part 0 is the whole-report reconciliation of split readings.
        parts[doc, 0] = packet_parts(roster[doc], segments[doc], 10**9)[0]

new = []
for path in sorted((run_dir / 'responses').glob('*.json')):
    m = re.fullmatch(r'(\w+)_part(\d+)_attempt(\d+)\.json', path.name)
    assert m and (m.group(1), int(m.group(2))) in parts, f'Unexpected answer file {path.name}'
    doc, k, attempt = m.group(1), int(m.group(2)), int(m.group(3))
    if (doc, k, attempt) in logged_keys:
        record = next(a for a in logged if (a['document_id'], a['part'], a['attempt']) == (doc, k, attempt))
        if record.get('response_sha256'):
            assert hashlib.sha256(path.read_bytes()).hexdigest() == record['response_sha256'], f'Logged answer changed: {path}'
        continue
    answer, errors = validate_answer(path.read_text(), doc, parts[doc, k]['segments'], schema)
    if answer and answer['application_number'] != roster[doc]['application_number']:
        errors.append('Focal application_number does not match the roster.')
        answer = None
    new.append(dict(document_id=doc, part=k, parts=parts[doc, k]['parts'], attempt=attempt, route=settings['route'],
                    model=settings['model'], reasoning=settings['reasoning'],
                    packet_sha256=hashlib.sha256(parts[doc, k]['text'].encode()).hexdigest(),
                    prompt_sha256=manifest[doc, k]['prompt_sha256'] if k else hashlib.sha256((run_dir / 'whole_report_review.md').read_bytes()).hexdigest(),
                    instruction_sha256=settings['instruction_sha256'], schema_sha256=settings['schema_sha256'],
                    response_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
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
for (doc, k) in parts:
    if k == 0:
        reviewed = any(a['document_id'] == doc and a['part'] == 0 and a['validation_status'] == 'valid' for a in logged + new)
        print(f'{doc}: whole-report reconciliation {"valid" if reviewed else "still required"}')
