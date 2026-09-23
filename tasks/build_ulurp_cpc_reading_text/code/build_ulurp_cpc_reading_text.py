#!/usr/bin/env python3
"""Decide which PDF pages each CPC narrative's reader receives; split them into segments.

Pages that cannot be attributed to the focal application are still supplied, flagged
`unresolved`; the reader records the application behind each statement."""
import csv
import hashlib
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, '../../_lib')
from data_reports import save_csv
from cpc_narratives import FILING_PARAGRAPH, COMMISSION_SIGNATURE, narrative_boundary
from cpc_pages import scope_pages

# Run from tasks/build_ulurp_cpc_reading_text/code.
MAX_SEGMENT_CHARACTERS = 12000
BOROUGHS = {'K': 'brooklyn', 'Q': 'queens', 'M': 'manhattan', 'X': 'bronx', 'R': 'staten island'}

with open('../input/ulurp_cpc_text_labels.csv') as f:
    narratives = list(csv.DictReader(f))
with open('../input/ulurp_cpc_narrative_sources.csv') as f:
    links = list(csv.DictReader(f))
with open('../input/ulurp_cpc_report_manifest.csv') as f:
    manifest_rows = list(csv.DictReader(f))
with open('../input/ulurp_cpc_page_scope_reviews.csv') as f:
    reviews = list(csv.DictReader(f))
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
# full text once, even when the narrative producer needs only one copy of the prefix.
for r in links:
    if (r['link_role'] == 'repeated_narrative' and r['source_text_sha256']
        and r['source_text_sha256'] not in {s['source_text_sha256'] for s in sources[r['document_id']]}):
        sources[r['document_id']].append(r)

roster, segments, page_scope = [], [], []
applied_reviews = set()
for narrative in narratives:
    doc = narrative['document_id']
    pieces, scoped = [], []
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
        pages = text.split('\f')
        if pages and not pages[-1].strip():
            pages.pop()
        rows = scope_pages(pages, m['application_number'], allowed_applications, core_end,
                           BOROUGHS.get(m['application_number'][-1], ''))
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
            if row['scope'] not in {'in_scope', 'unresolved'}:
                continue
            page_text = ' '.join(page_text.split())
            start = 0
            while start < len(page_text):
                end = min(start + MAX_SEGMENT_CHARACTERS, len(page_text))
                if end < len(page_text):
                    end = page_text.rfind(' ', start + 1, end) if ' ' in page_text[start + 1:end] else end
                pieces.append(dict(document_id=doc, segment_id=f's{len(pieces)+1:04d}',
                    source_document_id=link['source_document_id'], source_application_number=link['source_application_number'],
                    source_role=link['link_role'], pdf_page=page, page_scope=row['scope'], page_start=start, page_end=end,
                    source_text_sha256=link['source_text_sha256'], public_pdf_url=m['resolved_pdf_url'], text=page_text[start:end]))
                start = end
    page_scope.extend(scoped)
    unresolved_pages = sum(r['scope'] == 'unresolved' for r in scoped)
    status = 'ready' if pieces else 'no_usable_text'
    segments.extend(pieces)
    roster.append(dict(document_id=doc, application_number=narrative['application_number'], year=narrative['year'],
        project_name=narrative['project_name'], zap_project_ids=narrative['zap_project_ids'],
        preparation_status=status, unresolved_pages=unresolved_pages,
        excluded_unrelated_pages=sum(r['scope'] == 'out_of_scope' for r in scoped),
        segment_count=len(pieces), unresolved_segments=sum(p['page_scope'] == 'unresolved' for p in pieces),
        segment_characters=sum(len(p['text']) for p in pieces),
        source_document_ids=';'.join(r['source_document_id'] for r in sources[doc])))

assert {r['document_id'] for r in roster} == {r['document_id'] for r in narratives}
assert applied_reviews == page_reviews.keys(), 'A recorded page decision was not applied.'
save_csv(roster, list(roster[0]), '../output/ulurp_cpc_reading_roster.csv', ['document_id'])
save_csv(page_scope, list(page_scope[0]), '../output/ulurp_cpc_page_scope.csv', ['document_id', 'source_document_id', 'pdf_page'])
save_csv(segments, list(segments[0]), '../output/ulurp_cpc_reading_segments.csv', ['document_id', 'segment_id'])
print(f'{len(roster)} narratives: {sum(r["preparation_status"] == "ready" for r in roster)} ready, '
      f'{sum(r["unresolved_pages"] > 0 for r in roster)} include unattributed pages.')
