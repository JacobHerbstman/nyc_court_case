#!/usr/bin/env python3
"""Prepare corrected source packets; keep ambiguous attachments out of acquisition."""
import csv
import hashlib
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
sys.path.insert(0, '../../_lib')
from data_reports import save_csv
from cpc_narratives import FILING_PARAGRAPH, COMMISSION_SIGNATURE, narrative_boundary
from cpc_pages import scope_pages

# Run from tasks/extract_ulurp_cpc_jev_labels/code.
max_state_characters, random_audit_count = map(int, sys.argv[1:3])
audit_seed = sys.argv[3]
book = json.loads(Path('cpc_jev_codebook_v1.json').read_text())
with open('../input/ulurp_cpc_text_labels.csv') as f:
    narratives = list(csv.DictReader(f))
with open('../input/ulurp_cpc_narrative_sources.csv') as f:
    links = list(csv.DictReader(f))
with open('../input/ulurp_cpc_report_manifest.csv') as f:
    manifest_rows = list(csv.DictReader(f))
with open('../input/ulurp_cpc_page_scope_reviews.csv') as f:
    reviews = list(csv.DictReader(f))
with open('../input/cpc_jev_responses_v2.jsonl') as f:
    token_limit_documents = {r['document_id'] for line in f if (r := json.loads(line)).get('http_status') == 400
                             and 'max_tokens_exceeded' in r.get('raw_response', '')}
assert len(narratives) == len({r['document_id'] for r in narratives})
assert len(links) == len({(r['document_id'], r['source_document_id']) for r in links})
assert len(manifest_rows) == len({r['document_id'] for r in manifest_rows})
manifest = {r['document_id']: r for r in manifest_rows}
page_reviews = {}
for r in reviews:
    assert r['scope'] in {'in_scope', 'out_of_scope'}
    for page in range(int(r['first_page']), int(r['last_page']) + 1):
        assert (r['source_document_id'], page) not in page_reviews
        page_reviews[r['source_document_id'], page] = r
sources = defaultdict(list)
for r in links:
    if r['text_included_flag'] == 'TRUE':
        sources[r['document_id']].append(r)
# Repeated narrative prefixes can have different attachments. Keep each distinct
# full text once, even when the regex producer needs only one copy of the prefix.
for r in links:
    if (r['link_role'] == 'repeated_narrative' and r['source_text_sha256']
        and r['source_text_sha256'] not in {s['source_text_sha256'] for s in sources[r['document_id']]}):
        sources[r['document_id']].append(r)
audit_order = sorted((r['document_id'] for r in narratives), key=lambda d: hashlib.sha256(f'{audit_seed}|{d}'.encode()).hexdigest())
audit_ids = set(audit_order[:random_audit_count])
# Audited reports run first; their selection never depends on model answers.
narratives.sort(key=lambda r: (r['document_id'] not in audit_ids, audit_order.index(r['document_id'])))
roster, segments, request_index, page_scope = [], [], [], []
applied_reviews = set()
with open('../output/cpc_jev_requests_v3.jsonl', 'w') as output:
    for narrative in narratives:
        doc = narrative['document_id']
        state_limit = min(max_state_characters, 22000) if doc in token_limit_documents else max_state_characters
        pieces = []
        scoped = []
        allowed_applications = sorted({r['source_application_number'] for r in sources[doc]})
        for link in sorted(sources[doc], key=lambda r: (r['source_document_id'] != doc, r['source_document_id'])):
            m = manifest[link['source_document_id']]
            text = (Path('../../build_ulurp_cpc_report_corpus/code') / m['local_text_path']).read_text()
            assert hashlib.sha256(text.encode()).hexdigest() == link['source_text_sha256']
            boundary, method = narrative_boundary(text)
            endings = [match for pattern in (FILING_PARAGRAPH, COMMISSION_SIGNATURE)
                       if (match := pattern.search(text, boundary))]
            core_end = text.count('\f', 0, min(match.end() for match in endings)) + 1 if endings else 0
            if core_end == 0:
                core_end = text.count('\f', 0, boundary) + 1 if method != 'full_text_no_boundary_found' else 0
            borough = {'K': 'brooklyn', 'Q': 'queens', 'M': 'manhattan', 'X': 'bronx', 'R': 'staten island'}.get(m['application_number'][-1], '')
            pages = text.split('\f')
            if pages and not pages[-1].strip():
                pages.pop()
            rows = scope_pages(pages, m['application_number'], allowed_applications, core_end, borough)
            if not core_end:
                first_text = next((r for r in rows if r['scope'] != 'blank'), None)
                if first_text:
                    first_text.update(scope='unresolved', reason='Main-report end is not established; source boundary needs review.')
            for row in rows:
                page, page_text = row['page'], row['text']
                review = page_reviews.get((m['document_id'], page))
                if review:
                    assert review['source_text_sha256'] == link['source_text_sha256']
                    row.update(scope=review['scope'], reason='recorded_source_review: ' + review['reason'])
                    applied_reviews.add((m['document_id'], page))
                scoped.append(dict(document_id=doc, source_document_id=m['document_id'],
                    source_application_number=m['application_number'], pdf_page=page, scope=row['scope'],
                    scope_reason=row['reason'], block_start_page=row['block_start_page'],
                    source_text_sha256=link['source_text_sha256'], public_pdf_url=m['resolved_pdf_url'],
                    text_characters=len(page_text), low_text=int(len(re.findall(r'\w+', page_text)) < 10)))
                if row['scope'] != 'in_scope':
                    continue
                page_text = ' '.join(page_text.split())
                start, part = 0, 0
                while start < len(page_text):
                    end = min(start + 12000, len(page_text))
                    if end < len(page_text):
                        end = page_text.rfind(' ', start + 1, end) if ' ' in page_text[start + 1:end] else end
                    part += 1
                    pieces.append(dict(document_id=doc, segment_id=f's{len(pieces)+1:04d}',
                        source_document_id=link['source_document_id'], source_application_number=link['source_application_number'],
                        source_role=link['link_role'], pdf_page=page, page_start=start, page_end=end,
                        source_text_sha256=link['source_text_sha256'], public_pdf_url=m['resolved_pdf_url'], text=page_text[start:end]))
                    start = end
        page_scope.extend(scoped)
        unresolved_pages = sum(r['scope'] == 'unresolved' for r in scoped)
        groups, group, size = [], [], 0
        for piece in pieces:
            rendered = f"Source {piece['source_application_number']} ({piece['source_role']}), page ID {piece['segment_id']}\n{piece['text']}"
            if group and (size + len(json.dumps(rendered, ensure_ascii=False)) + 2 > state_limit - 1500 or len(group)>=20):
                groups.append(group)
                group, size = [], 0
            group.append(piece)
            size += len(json.dumps(rendered, ensure_ascii=False)) + 2
        if group:
            groups.append(group)
        # Keep the report on the roster; never infer absence from omitted uncertain pages.
        if unresolved_pages:
            groups = []
        for part, group in enumerate(groups, 1):
            state = dict(report=dict(application_number=narrative['application_number'], project_name=narrative['project_name'],
                part=part, parts=len(groups), text='\n\n'.join(f"Source {p['source_application_number']} ({p['source_role']}), page ID {p['segment_id']}\n{p['text']}" for p in group)))
            assert len(json.dumps(state, ensure_ascii=False)) <= state_limit
            options = {p['segment_id']: None for p in group}
            options['none'] = 'No supplied passage establishes this particular topic or position.'
            questions = {}
            for field, rule in book['questions'].items():
                questions[field] = dict(type=rule['type'], instructions=rule['instructions'], criteria=options if rule['type']=='choice' else rule['criteria'])
                if rule['type']=='noul' and rule['evidence']:
                    questions['page__'+field] = dict(type='choice', instructions='Which supplied page best supports a YES answer to this question? Select none if no page does. '+rule['instructions'], criteria=options)
            body = dict(model='typesafe-ai/jev', state=state, questions=questions)
            request_id = f'{doc}_part{part:03d}'
            digest = hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            output.write(json.dumps(dict(request_id=request_id, document_id=doc, request_sha256=digest, body=body), ensure_ascii=False)+'\n')
            request_index.append(dict(request_id=request_id, document_id=doc, part=part, parts=len(groups), request_sha256=digest, state_characters=len(json.dumps(state, ensure_ascii=False))))
            segments.extend(dict(p, request_id=request_id) for p in group)
        roster.append(dict(document_id=doc, application_number=narrative['application_number'], year=narrative['year'],
            project_name=narrative['project_name'], zap_project_ids=narrative['zap_project_ids'],
            state_character_limit=state_limit,
            preparation_status='needs_scope_review' if unresolved_pages else 'ready' if groups else 'no_usable_text',
            unresolved_pages=unresolved_pages, excluded_unrelated_pages=sum(r['scope'] == 'out_of_scope' for r in scoped),
            request_count=len(groups), split_context=int(len(groups)>1),
            random_audit=int(doc in audit_ids), source_document_ids=';'.join(r['source_document_id'] for r in sources[doc])))
assert {r['document_id'] for r in roster} == {r['document_id'] for r in narratives}
assert applied_reviews == page_reviews.keys(), 'A recorded page decision was not applied.'
save_csv(roster, list(roster[0]), '../output/cpc_jev_roster_v3.csv', ['document_id'])
save_csv(segments, list(segments[0]), '../output/cpc_jev_segments_v3.csv', ['document_id','segment_id'])
save_csv(request_index, list(request_index[0]), '../output/cpc_jev_request_index_v3.csv', ['request_id'])
save_csv(page_scope, list(page_scope[0]), '../output/cpc_jev_page_scope_v3.csv', ['document_id','source_document_id','pdf_page'])
print(f'Prepared {len(roster)} narratives, {len(request_index)} requests. {sum(r["preparation_status"] == "needs_scope_review" for r in roster)} reports held for scope review. No API calls made.')
