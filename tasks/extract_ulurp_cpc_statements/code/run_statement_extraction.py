#!/usr/bin/env python3
"""Send CPC reading packets to the model through `codex exec` on the ChatGPT plan.

Each narrative is split into parts only if it exceeds MAX_PACKET_CHARACTERS. Every
attempt is appended to attempts.jsonl and its answer saved unedited. A part with a
valid saved answer is never sent again. Invalid answers and timeouts get one retry;
a usage limit or any Codex error stops the whole run. Rerunning resumes.

Run from tasks/extract_ulurp_cpc_statements/code (normally through `make acquire`).
"""
import hashlib
import csv
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from cpc_statement_packets import build_prompt, packet_parts, read_inputs, validate_answer

model, reasoning, max_characters, run_id, document_id_file, workers, timeout_seconds = sys.argv[1:8]
max_characters, workers, timeout_seconds = int(max_characters), int(workers), int(timeout_seconds)
MAX_ATTEMPTS = 2
LIMIT_PATTERN = re.compile(r'usage limit|rate limit|rate_limit|too many requests|quota', re.I)

sha = lambda text: hashlib.sha256(text.encode()).hexdigest()
now = lambda: datetime.now(timezone.utc).isoformat(timespec='seconds')
env = {k: v for k, v in os.environ.items() if k not in {'OPENAI_API_KEY', 'AI_GATEWAY_API_KEY'}}

login = subprocess.run(['codex', 'login', 'status'], capture_output=True, text=True, env=env, cwd=tempfile.gettempdir())
if 'Logged in using ChatGPT' not in login.stdout + login.stderr:
    sys.exit('Codex is not logged in with ChatGPT; refusing to run (nothing may bill the API).')

instructions = Path('statement_instructions.md').read_text()
schema_path = Path('statement_schema.json').resolve()
schema = json.loads(schema_path.read_text())
run_dir = Path('../../../data_raw/cpc_statement_extraction') / run_id
(run_dir / 'responses').mkdir(parents=True, exist_ok=True)
(run_dir / 'events').mkdir(exist_ok=True)

settings = dict(model=model, reasoning=reasoning, max_packet_characters=max_characters,
                instruction_sha256=sha(instructions), schema_sha256=sha(schema_path.read_text()))
run_file = run_dir / 'run.json'
if run_file.exists():
    recorded = json.loads(run_file.read_text())
    changed = [k for k in settings if recorded[k] != settings[k]]
    if changed:
        sys.exit(f'Run {run_id} was started with different {changed}; use a new RUN_ID.')
else:
    version = subprocess.run(['codex', '--version'], capture_output=True, text=True, env=env, cwd=tempfile.gettempdir()).stdout.strip()
    commit = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True).stdout.strip()
    run_file.write_text(json.dumps(dict(settings, run_id=run_id, codex_version=version, git_commit=commit,
                                        started_at=now()), indent=2) + '\n')
    (run_dir / 'statement_instructions.md').write_text(instructions)
    (run_dir / 'statement_schema.json').write_text(schema_path.read_text())

attempts_file = run_dir / 'attempts.jsonl'
history = [json.loads(line) for line in attempts_file.read_text().splitlines()] if attempts_file.exists() else []

if Path(document_id_file).suffix == '.csv':
    with open(document_id_file) as f:
        document_ids = [r['document_id'] for r in csv.DictReader(f)]
else:
    document_ids = [line.strip() for line in Path(document_id_file).read_text().splitlines() if line.strip()]
assert len(document_ids) == len(set(document_ids)), 'Duplicate requested documents.'
roster, segments = read_inputs(set(document_ids))
queue = []
for doc in document_ids:
    for part in packet_parts(roster[doc], segments[doc], max_characters):
        prior = [a for a in history if a['document_id'] == doc and a['part'] == part['part']]
        if any(a['packet_sha256'] != sha(part['text']) or a['parts'] != part['parts'] for a in prior):
            sys.exit(f'Packet for {doc} part {part["part"]} changed since earlier attempts; use a new RUN_ID.')
        counted = [a for a in prior if a['validation_status'] != 'usage_limit']
        if any(a['validation_status'] == 'valid' for a in prior) or len(counted) >= MAX_ATTEMPTS:
            continue
        queue.append((doc, part, len(counted)))
print(f'{len(queue)} parts to send ({len(document_ids)} narratives requested).', flush=True)

lock, stop = threading.Lock(), threading.Event()


def last_usage(event_lines):
    usage = {}
    for line in event_lines:
        try:
            stack = [json.loads(line)]
        except json.JSONDecodeError:
            continue
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                if 'input_tokens' in item and 'output_tokens' in item:
                    usage = item
                stack.extend(item.values())
            elif isinstance(item, list):
                stack.extend(item)
    return usage


def send(doc, part, done_attempts):
    prompt = build_prompt(instructions, part['text'])
    for attempt in range(done_attempts + 1, MAX_ATTEMPTS + 1):
        if stop.is_set():
            return
        stem = f"{doc}_part{part['part']}_attempt{attempt}"
        response = (run_dir / 'responses' / f'{stem}.json').resolve()
        record = dict(document_id=doc, part=part['part'], parts=part['parts'], attempt=attempt, model=model,
                      reasoning=reasoning, packet_sha256=sha(part['text']), prompt_sha256=sha(prompt),
                      instruction_sha256=settings['instruction_sha256'], schema_sha256=settings['schema_sha256'],
                      started_at=now())
        with tempfile.TemporaryDirectory() as empty:
            try:
                result = subprocess.run(
                    ['codex', 'exec', '--model', model, '-c', f'model_reasoning_effort="{reasoning}"',
                     '--sandbox', 'read-only', '--skip-git-repo-check', '--ephemeral',
                     '--output-schema', str(schema_path), '--json', '-o', str(response), '-'],
                    input=prompt, capture_output=True, text=True, cwd=empty, env=env, timeout=timeout_seconds)
                exit_code, stdout, stderr = result.returncode, result.stdout, result.stderr
            except subprocess.TimeoutExpired as e:
                exit_code, stdout, stderr = 'timeout', e.stdout or '', e.stderr or ''
                stdout, stderr = [x.decode() if isinstance(x, bytes) else x for x in (stdout, stderr)]
        (run_dir / 'events' / f'{stem}.jsonl').write_text(stdout)
        usage = last_usage(stdout.splitlines())
        record.update(ended_at=now(), exit_code=exit_code, input_tokens=usage.get('input_tokens', ''),
                      cached_input_tokens=usage.get('cached_input_tokens', ''),
                      output_tokens=usage.get('output_tokens', ''), stderr=stderr[-2000:])
        if exit_code == 0 and response.exists():
            answer, errors = validate_answer(response.read_text(), doc, part['segments'], schema)
            record.update(validation_status='valid' if answer else 'invalid', validation_errors=errors[:20],
                          statement_rows=len(answer['statements']) if answer else '')
        elif exit_code != 'timeout' and LIMIT_PATTERN.search(stderr + stdout):
            record.update(validation_status='usage_limit', validation_errors=[stderr[-500:]])
            stop.set()
        elif exit_code == 'timeout':
            record.update(validation_status='timeout', validation_errors=[])
        else:
            record.update(validation_status='codex_error', validation_errors=[stderr[-500:]])
            stop.set()
        with lock:
            with attempts_file.open('a') as f:
                f.write(json.dumps(record) + '\n')
            print(f"{doc} part {part['part']}/{part['parts']} attempt {attempt}: {record['validation_status']} "
                  f"{record.get('statement_rows', '')} rows; tokens in {record['input_tokens']} "
                  f"out {record['output_tokens']}", flush=True)
            for error in record['validation_errors'][:5]:
                print('    ' + str(error).replace('\n', ' ')[:300], flush=True)
        if record['validation_status'] in {'valid', 'usage_limit', 'codex_error'}:
            return


with ThreadPoolExecutor(max_workers=workers) as pool:
    for future in [pool.submit(send, *item) for item in queue]:
        future.result()
if stop.is_set():
    sys.exit('Stopped early (usage limit or Codex error); see attempts.jsonl. Rerun the same command to resume.')
