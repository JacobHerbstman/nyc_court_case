"""Render CPC reading packets and validate statement answers.

Shared by run_statement_extraction.py (model calls) and
build_ulurp_cpc_statements.py (saved answers only).
"""
import csv
import json
from collections import defaultdict
from pathlib import Path

import jsonschema

PROCEDURAL_TYPES = {'request', 'commitment', 'requirement', 'modification', 'decision'}
WRAPPER = ('You are a non-interactive reader. Do not run commands, read files, browse or use tools; '
           'everything you need is in this message. Follow the instruction below and return only the JSON object.')


def read_inputs(document_ids):
    """Roster rows and ordered segments for the requested narratives."""
    with open('../input/ulurp_cpc_reading_roster.csv') as f:
        roster = {r['document_id']: r for r in csv.DictReader(f)}
    missing = set(document_ids) - set(roster)
    assert not missing, f'Unknown document IDs: {sorted(missing)[:5]}'
    segments = defaultdict(list)
    csv.field_size_limit(10**8)
    with open('../input/ulurp_cpc_reading_segments.csv') as f:
        for r in csv.DictReader(f):
            if r['document_id'] in document_ids:
                segments[r['document_id']].append(r)
    for doc in segments:
        segments[doc].sort(key=lambda r: r['segment_id'])
    return roster, segments


def packet_parts(narrative, segments, max_characters):
    """Split a narrative's segments into packets within max_characters, at segment boundaries."""
    apps = ', '.join(sorted({s['source_application_number'] for s in segments}))
    blocks = [f"=== segment {s['segment_id']} | source {s['source_application_number']} ({s['source_role']}) "
              f"| PDF page {s['pdf_page']} | page_scope {s['page_scope']} ===\n{s['text']}\n" for s in segments]
    header_room = 400 + len(narrative['project_name']) + len(apps)
    groups, group, size = [], [], 0
    for segment, block in zip(segments, blocks):
        if group and size + len(block) + 1 > max_characters - header_room:
            groups.append(group)
            group, size = [], 0
        group.append((segment, block))
        size += len(block) + 1
    if group:
        groups.append(group)
    parts = []
    for k, group in enumerate(groups, 1):
        header = [f"document_id: {narrative['document_id']}", f"application_number: {narrative['application_number']}",
                  f"project_name: {narrative['project_name']}", f"bundle_applications: {apps}",
                  f"segments: {len(group)}"]
        if len(groups) > 1:
            header.append(f'part: {k} of {len(groups)}')
        text = '\n'.join(header) + '\n\n' + '\n'.join(block for _, block in group)
        parts.append(dict(part=k, parts=len(groups), text=text, segments={s['segment_id']: s['text'] for s, _ in group}))
    assert sum(len(p['segments']) for p in parts) == len(segments)
    return parts


def build_prompt(instructions, packet_text):
    return WRAPPER + '\n\n' + instructions + '\n\n## Report\n\n' + packet_text


def validate_answer(raw, document_id, part_segments, schema):
    """Return (answer or None, list of errors)."""
    try:
        answer = json.loads(raw)
    except json.JSONDecodeError as e:
        return None, [f'invalid JSON: {e}']
    errors = [f'schema: {e.message[:200]}' for e in jsonschema.Draft202012Validator(schema).iter_errors(answer)]
    if errors:
        return None, errors
    norm = lambda s: ' '.join(s.split())
    texts = {k: norm(v) for k, v in part_segments.items()}
    rows = answer['statements']
    ids = [r['statement_id'] for r in rows]
    if answer['document_id'] != document_id:
        errors.append(f"document_id {answer['document_id']} does not match {document_id}")
    if len(ids) != len(set(ids)):
        errors.append('duplicate statement_id')
    for r in rows:
        if not norm(r['quote']):
            errors.append(f"row {r['statement_id']}: empty quotation")
        unknown = [s for s in r['segment_ids'] if s not in texts]
        if unknown:
            errors.append(f"row {r['statement_id']}: unknown segments {unknown}")
        elif not any(norm(r['quote']) in texts[s] for s in r['segment_ids']):
            errors.append(f"row {r['statement_id']}: quote not found in cited segments")
        if any(i not in ids for i in r['response_statement_ids']):
            errors.append(f"row {r['statement_id']}: response_statement_ids refer to missing rows")
        if r['statement_id'] in r['response_statement_ids']:
            errors.append(f"row {r['statement_id']}: a statement cannot be its own response")
        if r['statement_type'] not in {'concern', 'request'} and (r['response'] != 'not_applicable' or r['response_statement_ids']):
            errors.append(f"row {r['statement_id']}: response fields apply only to concerns and requests")
        if r['statement_type'] in {'concern', 'request'} and r['response'] == 'not_applicable':
            errors.append(f"row {r['statement_id']}: concern or request needs a response status")
        if r['response'] in {'adopted', 'partly_adopted', 'rejected', 'addressed_otherwise'} and not r['response_statement_ids']:
            errors.append(f"row {r['statement_id']}: recorded response needs supporting statement IDs")
        if (r['statement_type'] in {'commitment', 'requirement', 'modification'}) == (r['certainty'] == 'not_applicable'):
            errors.append(f"row {r['statement_id']}: certainty does not match statement type")
        if r.get('procedural_action', 'none') != 'none' and r['statement_type'] not in PROCEDURAL_TYPES:
            errors.append(f"row {r['statement_id']}: procedural_action applies only to requests, commitments, requirements, modifications and decisions")
    if set(answer['segments_read']) != set(texts):
        errors.append(f"segments_read differs from supplied segments ({len(set(texts) - set(answer['segments_read']))} missing)")
    return (answer if not errors else None), errors
