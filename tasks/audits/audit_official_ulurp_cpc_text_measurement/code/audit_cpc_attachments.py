"""Measure the OCR repair against the preserved pre-repair corpus."""
import csv
import hashlib
import re
import sys
from pathlib import Path

sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

with open('../input/cpc_attachment_manifest_before.csv') as f:
    before_rows = list(csv.DictReader(f))
with open('../input/official_ulurp_cpc_report_manifest.csv') as f:
    after_rows = list(csv.DictReader(f))
with open('../input/cpc_attachment_pages_before.csv') as f:
    old_pages = list(csv.DictReader(f))
with open('../input/cpc_attachment_roster_before.csv') as f:
    roster = list(csv.DictReader(f))
with open('../input/cpc_attachment_pilot_sample.csv') as f:
    pilot = list(csv.DictReader(f))

before = {r['document_id']: r for r in before_rows}
after = {r['document_id']: r for r in after_rows}
old = {(r['document_id'], int(r['pdf_page'])): r for r in old_pages}
assert len(before) == len(before_rows) == len(after) == len(after_rows)
assert set(before) == set(after)
assert len(old) == len(old_pages)
assert len(roster) == len({r['document_id'] for r in roster})
assert len(pilot) == len({r['document_id'] for r in pilot}) == 20

pages, reports = [], []
for doc, source in after.items():
    prior = before[doc]
    for field in ['application_number', 'resolved_pdf_url', 'pdf_page_count', 'source_usable']:
        assert source[field] == prior[field], (doc, field)
    supplied = []
    if source['local_text_path']:
        text = (Path('../../../build_ulurp_cpc_report_corpus/code') / source['local_text_path']).read_text()
        count = int(source['pdf_page_count'])
        parts = text.split('\f')[:count]
        parts += [''] * (count - len(parts))
        repaired = {int(n.strip()) for n in source['partial_ocr_pages'].split(';') if n.strip()}
        for page, content in enumerate(parts, 1):
            previous = old[doc, page]
            words = len(re.findall(r"[A-Za-z0-9$]+(?:[-'][A-Za-z0-9]+)?", content))
            digest = hashlib.sha256(content.encode()).hexdigest()
            candidate = int(previous['after_resolution']) == 1 and int(previous['words']) < 50
            row = dict(document_id=doc, application_number=source['application_number'], pdf_page=page,
                after_prior_resolution=int(previous['after_resolution']), words_before=int(previous['words']),
                words_after=words, blank_before=int(previous['blank']), blank_after=int(not content.strip()),
                candidate=int(candidate), changed=int(digest != previous['text_sha256']),
                ocr_applied=int(page in repaired), words_added=words-int(previous['words']),
                recovered_50_words=int(candidate and words >= 50),
                recommendation_marker=int(candidate and words >= 50 and bool(re.search(
                    r'community[\s/\w,-]{0,40}board[\s\w,-]{0,80}recommend|borough[\s\w,-]{0,35}president|council\s*member',
                    content, re.I))), before_page_sha256=previous['text_sha256'], after_page_sha256=digest)
            pages.append(row)
            supplied.append(row)
    candidates = [p for p in supplied if p['candidate']]
    reports.append(dict(document_id=doc, application_number=source['application_number'],
        project_name=source['official_project_name'], year=source['official_vote_year'], corpus_role=source['corpus_role'],
        source_usable=source['source_usable'], pdf_pages=source['pdf_page_count'],
        candidate_pages=len(candidates), blank_candidate_pages=sum(p['blank_before'] for p in candidates),
        changed_candidate_pages=sum(p['changed'] for p in candidates),
        recovered_50_word_pages=sum(p['recovered_50_words'] for p in candidates),
        recommendation_marker_pages=sum(p['recommendation_marker'] for p in candidates),
        candidate_words_added=sum(max(0, p['words_added']) for p in candidates),
        remaining_short_candidate_pages=sum(p['words_after'] < 50 for p in candidates),
        changed_other_pages=sum(p['changed'] for p in supplied if not p['candidate']),
        pilot_focal=int(any(r['document_id'] == doc for r in pilot)),
        public_pdf_url=source['resolved_pdf_url']))
assert len(pages) == len(old_pages)
assert {(p['document_id'], p['pdf_page']) for p in pages} == set(old)

candidate_docs = {r['document_id'] for r in reports if r['candidate_pages']}
recovered_docs = {r['document_id'] for r in reports if r['recovered_50_word_pages']}
marker_docs = {r['document_id'] for r in reports if r['recommendation_marker_pages']}
affected_bundles = [r for r in roster if set(r['source_document_ids'].split(';')) & candidate_docs]
recovered_bundles = [r for r in roster if set(r['source_document_ids'].split(';')) & recovered_docs]
summary = [
    dict(metric='source_reports', value=len(reports), unit='source reports', meaning='All preserved corpus source rows, including known unavailable sources.'),
    dict(metric='usable_source_reports', value=sum(r['source_usable'] == 'TRUE' for r in reports), unit='source reports', meaning='Reports with a usable cached PDF and text.'),
    dict(metric='candidate_source_reports', value=len(candidate_docs), unit='source reports', meaning='At least one page after the prior resolution had fewer than 50 extracted words; includes maps and blanks.'),
    dict(metric='blank_candidate_source_reports', value=sum(r['blank_candidate_pages'] > 0 for r in reports), unit='source reports', meaning='At least one candidate page had no extracted text.'),
    dict(metric='candidate_pages', value=sum(r['candidate_pages'] for r in reports), unit='PDF pages', meaning='All short pages after the prior resolution.'),
    dict(metric='recovered_source_reports', value=len(recovered_docs), unit='source reports', meaning='At least one candidate page now has 50 or more extracted words; this is recovered text, not a manual content classification.'),
    dict(metric='recovered_pages', value=sum(r['recovered_50_word_pages'] for r in reports), unit='PDF pages', meaning='Candidate pages now containing at least 50 words, including maps, forms and OCR noise from images.'),
    dict(metric='recommendation_marker_source_reports', value=len(marker_docs), unit='source reports', meaning='Recovered text contains a board recommendation, Borough President or Council-member marker; automatic screening only.'),
    dict(metric='narrative_bundles_before', value=len(roster), unit='focal narrative bundles', meaning='The pre-repair processing roster; several source reports can describe one project review.'),
    dict(metric='candidate_narrative_bundles', value=len(affected_bundles), unit='focal narrative bundles', meaning='At least one previously linked source has candidate pages.'),
    dict(metric='recovered_narrative_bundles', value=len(recovered_bundles), unit='focal narrative bundles', meaning='At least one previously linked source has a page recovered to 50 words.'),
]
save_csv(pages, list(pages[0]), '../output/cpc_attachment_pages.csv', ['document_id', 'pdf_page'])
save_csv(reports, list(reports[0]), '../output/cpc_attachment_reports.csv', ['document_id'])
save_csv(summary, list(summary[0]), '../output/cpc_attachment_summary.csv', ['metric'])

lines = ['# CPC attachment OCR repair', '',
    f'The preserved corpus contains {len(reports):,} source reports. Before repair, {len(candidate_docs):,} '
    f'had at least one short page after the main resolution, including '
    f'{sum(r["blank_candidate_pages"] > 0 for r in reports):,} with an entirely empty extracted page. '
    'Short or empty text is a screen: some pages are maps or blank sheets, not missing narrative.', '',
    f'After OCR, {len(recovered_docs):,} source reports have at least one formerly short page with 50 or more words, '
    f'covering {sum(r["recovered_50_word_pages"] for r in reports):,} pages. '
    f'{len(marker_docs):,} source reports contain an automatically detected recommendation/official marker in '
    'recovered text. These are not manually verified counts of substantive attachments.', '',
    f'The former processing roster contains {len(roster):,} focal narrative bundles. '
    f'{len(affected_bundles):,} link to a candidate source; {len(recovered_bundles):,} link to a source with '
    'newly recovered text. Source-report counts must not be interpreted as distinct project counts.', '',
    '| Known pilot report | Previously short pages | Pages recovered to 50 words | Added words |',
    '|---|---:|---:|---:|']
for app in ['C 190403 ZMX', 'C 160064 ZMX', 'C 170452 ZSM', 'C 180085 ZMQ']:
    r = next(r for r in reports if r['application_number'] == app)
    assert r['recovered_50_word_pages'] > 0, app
    lines.append(f'| {app} | {r["candidate_pages"]} | {r["recovered_50_word_pages"]} | {r["candidate_words_added"]:,} |')
lines += ['', 'The repair checks every short page, including attachments after the resolution. The original '
    'PDFs are reused and the pre-repair text is archived. Report identifiers and source availability are unchanged. '
    'A page remaining short is retained and flagged; it is not automatically treated as missing or discarded. '
    'OCR remains imperfect: photographs can produce spurious words, and text above the threshold can still contain errors. The word-count screen is not a measure of reading accuracy.', '',
    'The completed Sol trial and raw model answers remain frozen. This run stops after OCR and coverage '
    'measurement, as requested: no new model test, coding run, or reading-packet rebuild was performed. '
    'Application attribution and coding updates remain separate follow-up work.', '']
Path('../output/cpc_attachment_findings.md').write_text('\n'.join(lines))
print(f'OCR repair: {len(candidate_docs)} candidate sources; {len(recovered_docs)} with pages recovered to 50 words.')
