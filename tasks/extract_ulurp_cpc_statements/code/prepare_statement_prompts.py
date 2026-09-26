#!/usr/bin/env python3
"""Write one prompt file per narrative part for readers run from the Codex app.

The app route replaces run_statement_extraction.py: app subagents read these
files and save answers to the same run folder, and record_statement_answers.py
validates and logs them. Long segment lines are wrapped so file readers do not
truncate them; validation ignores whitespace. No model calls.
"""
# Interactive use: cd to tasks/extract_ulurp_cpc_statements/code, then
# python3 prepare_statement_prompts.py pilot_app_20260926 gpt-6-sol high 400000 pilot_documents.txt
import csv
import hashlib
import json
import subprocess
import sys
import textwrap
from pathlib import Path

from cpc_statement_packets import packet_parts, read_inputs

run_id, model, reasoning, max_characters, document_id_file = sys.argv[1:6]
max_characters = int(max_characters)
sha = lambda text: hashlib.sha256(text.encode()).hexdigest()

instructions = Path('statement_instructions.md').read_text()
settings = dict(route='app_subagents', model=model, reasoning=reasoning, max_packet_characters=max_characters,
                instruction_sha256=sha(instructions), schema_sha256=sha(Path('statement_schema.json').read_text()))
run_dir = Path('../../../data_raw/cpc_statement_extraction') / run_id
run_file = run_dir / 'run.json'
if run_file.exists():
    recorded = json.loads(run_file.read_text())
    changed = [k for k in settings if recorded.get(k) != settings[k]]
    if changed:
        sys.exit(f'Run {run_id} was prepared with different {changed}; use a new RUN_ID.')
else:
    (run_dir / 'prompts').mkdir(parents=True)
    (run_dir / 'responses').mkdir()
    commit = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True).stdout.strip()
    run_file.write_text(json.dumps(dict(settings, run_id=run_id, git_commit=commit), indent=2) + '\n')

document_ids = [line.strip() for line in Path(document_id_file).read_text().splitlines() if line.strip()]
roster, segments = read_inputs(set(document_ids))
manifest = []
for doc in document_ids:
    for part in packet_parts(roster[doc], segments[doc], max_characters):
        wrapped = '\n'.join(w for line in part['text'].split('\n')
                            for w in (textwrap.wrap(line, 160, break_long_words=False, break_on_hyphens=False) or ['']))
        prompt = instructions + '\n\n## Report\n\n' + wrapped
        path = run_dir / 'prompts' / f"{doc}_part{part['part']}.txt"
        if path.exists() and path.read_text() != prompt:
            sys.exit(f'{path} exists with different text; use a new RUN_ID.')
        path.write_text(prompt)
        manifest.append(dict(document_id=doc, part=part['part'], parts=part['parts'], prompt_file=path.name,
                             answer_file=f"{doc}_part{part['part']}_attempt1.json", segments=len(part['segments']),
                             packet_sha256=sha(part['text']), prompt_sha256=sha(prompt)))
with (run_dir / 'prompts.csv').open('w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=list(manifest[0]))
    writer.writeheader()
    writer.writerows(manifest)
print(f'{run_id}: {len(manifest)} prompt files for {len(document_ids)} narratives in {run_dir}/prompts')
