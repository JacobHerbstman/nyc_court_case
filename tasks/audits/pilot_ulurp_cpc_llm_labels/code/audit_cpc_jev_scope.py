#!/usr/bin/env python3
"""Compare repaired source packets with the preserved, paused extraction."""
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
sys.path.insert(0, '../../../_lib')
from data_reports import save_csv
from cpc_pages import scope_pages
from cpc_narratives import project_review_key

# An unidentified attachment, blank gap, or incidental citation cannot prove scope.
focal = 'C 111111 ABC'
for text in ['unidentified attachment', 'This unrelated application cites application C 111111 ABC in its history.', 'NEW ATTACHMENT\n' + 'x' * 1300 + ' C 111111 ABC ' + 'y' * 800]:
    assert scope_pages(['core', text], focal, [], 1, 'queens')[1]['scope'] == 'unresolved'
assert [r['scope'] for r in scope_pages(['core', '', 'continuation'], focal, [], 1, 'queens')] == ['in_scope', 'blank', 'unresolved']
assert scope_pages(['Recommendation report\n(111111 ABC)'], focal, [], 0, 'queens')[0]['scope'] == 'in_scope'
assert scope_pages(['Manhattan Borough President recommendation C 111111 ABC'], focal, [], 0, 'queens')[0]['scope'] == 'unresolved'
assert scope_pages(['Recommendation report C 111112 ABC'], focal, [], 0, 'queens')[0]['scope'] == 'unresolved'
assert not project_review_key('2000-01-01', '', 'C-O-P')
assert project_review_key('2000-01-01', 'a', 'C-O-P') != project_review_key('2000-01-01', 'b', 'C-O-P')
assert project_review_key('2000-01-01', 'a;b', 'same name') == project_review_key('2000-01-01', 'b;a', 'Same Name')

with open('../input/cpc_jev_labels_corpus_v2.csv') as f:
    old = {r['document_id']: r for r in csv.DictReader(f)}
with open('../input/cpc_jev_usage_corpus_v2.csv') as f:
    usage = list(csv.DictReader(f))
with open('../input/cpc_jev_narrative_sources_corpus_v2.csv') as f:
    old_links = list(csv.DictReader(f))
with open('../input/cpc_jev_repaired_narrative_sources_v3.csv') as f:
    new_links = list(csv.DictReader(f))
with open('../input/cpc_jev_roster_corpus_v3.csv') as f:
    new = list(csv.DictReader(f))
with open('../input/cpc_jev_request_index_corpus_v3.csv') as f:
    requests = list(csv.DictReader(f))
with open('../input/cpc_jev_page_scope_corpus_v3.csv') as f:
    pages = list(csv.DictReader(f))
assert len(new) == len({r['document_id'] for r in new})
assert set(old) <= {r['document_id'] for r in new}, 'An old narrative disappeared.'
assert len(new_links) == len({(r['document_id'], r['source_document_id']) for r in new_links})
assert len(requests) == len({r['request_id'] for r in requests})
assert len(pages) == len({(r['document_id'], r['source_document_id'], r['pdf_page']) for r in pages})
successful = {r['request_sha256'] for r in usage if r['status'] == 'successful'}
requests_by_doc = defaultdict(list)
for r in requests:
    requests_by_doc[r['document_id']].append(r)
before, after = defaultdict(set), defaultdict(set)
for rows, grouped in [(old_links, before), (new_links, after)]:
    for r in rows:
        if r['text_included_flag'] == 'TRUE':
            grouped[r['document_id']].add(r['source_document_id'])
for r in new:
    after[r['document_id']] = set(r['source_document_ids'].split(';')) - {''}

link_changes = []
for doc in sorted(before.keys() | after.keys()):
    for source in sorted(before[doc] ^ after[doc]):
        link_changes.append(dict(document_id=doc, source_document_id=source,
            change='added' if source in after[doc] else 'removed',
            old_complete=int(old.get(doc, {}).get('extraction_status') == 'complete')))
plan = []
for r in new:
    doc = r['document_id']
    packet = requests_by_doc[doc]
    reused = sum(p['request_sha256'] in successful for p in packet)
    assert len(packet) == int(r['request_count'])
    assert not packet or r['preparation_status'] == 'ready'
    status = ('held_for_scope_review' if r['preparation_status'] == 'needs_scope_review'
              else 'no_usable_text' if not packet
              else 'all_requests_reusable' if reused == len(packet)
              else 'some_requests_reusable' if reused else 'new_requests_required')
    plan.append(dict(document_id=doc, application_number=r['application_number'],
        old_complete=int(old.get(doc, {}).get('extraction_status') == 'complete'),
        old_roster_member=int(doc in old), source_bundle_changed=int(before[doc] != after[doc]),
        preparation_status=r['preparation_status'], recovery_status=status,
        required_requests=len(packet), reusable_successful_requests=reused,
        new_requests_required=len(packet)-reused, unresolved_pages=r['unresolved_pages'],
        excluded_unrelated_pages=r['excluded_unrelated_pages']))

# Regression examples check a false link and independently evidenced companions.
app = {r['application_number']: r['document_id'] for r in new}
source_app = {r['source_document_id']: r['source_application_number'] for r in new_links}
assert {source_app[s] for s in after[app['C 920197 PPQ']]} == {'C 920197 PPQ'}
for focal, expected in [
    ('C 870321 HDK', {'C 870320 HUK', 'C 870321 HDK', 'C 870371 ZMK'}),
    ('C 910238 ZMK', {'C 910170 HUK', 'C 910171 HDK', 'C 910237 MMK', 'C 910238 ZMK'}),
    ('C 910119 HUK', {'C 920049 HDK'}),
]:
    assert expected <= {source_app[s] for s in after[app[focal]]}, focal
queens = [r for r in pages if r['document_id'] == app['C 920197 PPQ']]
assert all(r['scope'] == ('in_scope' if int(r['pdf_page']) <= 6 else 'out_of_scope') for r in queens)
bay_ridge = [r for r in pages if r['document_id'] == app['C 990218 ZSK'] and r['source_document_id'] == app['C 990218 ZSK']]
assert next(r for r in bay_ridge if r['pdf_page'] == '74')['scope'] == 'in_scope'
wooten = [r for r in pages if r['document_id'] == app['C 910119 HUK']]
assert next(r for r in wooten if source_app[r['source_document_id']] == 'C 920049 HDK' and r['pdf_page'] == '11')['scope'] == 'in_scope'
assert all(r['scope'] == 'out_of_scope' for r in wooten
           if r['source_document_id'] == app['C 910119 HUK'] and 24 <= int(r['pdf_page']) <= 27)
assert all(r['preparation_status'] != 'ready' or r['unresolved_pages'] == '0' for r in new)
for r in new_links:
    path = r['relationship_path'].split('; ')
    assert path[0] == r['document_id'] and path[-1] == r['source_document_id']
    assert r['relationship_basis'] and 'project_name' not in r['relationship_basis']
old_represented = {r['source_document_id'] for r in old_links if r['represented_application_flag'] == 'TRUE'}
new_represented = {r['source_document_id'] for r in new_links if r['represented_application_flag'] == 'TRUE'}
assert old_represented <= new_represented, 'Previously represented applications disappeared.'

save_csv(plan, list(plan[0]), '../output/cpc_jev_recovery_plan_v3.csv', ['document_id'])
save_csv(link_changes, ['document_id', 'source_document_id', 'change', 'old_complete'],
    '../output/cpc_jev_source_link_changes_v3.csv', ['document_id', 'source_document_id'])
counts = Counter(r['recovery_status'] for r in plan)
complete_counts = Counter(r['recovery_status'] for r in plan if r['old_complete'])
scope_counts = Counter(r['scope'] for r in pages)
unresolved_sources = {r['source_document_id'] for r in pages if r['scope'] == 'unresolved'}
old_complete = sum(r['extraction_status'] == 'complete' for r in old.values())
lines = ['# Source-attribution repair', '',
    f'The old run remains paused with {old_complete:,} complete reports. Its requests, responses, source inputs and codebook are preserved. No API calls were made for this repair.', '',
    f'The corrected narrative roster has {len(new):,} rows, compared with {len(old):,} before. Every previously represented source application remains represented. A title and date alone no longer join sources or justify collapsing an application into a designated lead. Separate ZAP records can still be linked by explicit same-date application references, exact narrative identity or recorded source decisions.', '',
    f'{sum(r["change"] == "removed" for r in link_changes):,} old included source links were removed and {sum(r["change"] == "added" for r in link_changes):,} added. These are source-to-narrative links, not deleted PDFs or projects.', '',
    '| Packet disposition | All corrected narratives | Previously complete |', '|---|---:|---:|']
for status in ['all_requests_reusable', 'some_requests_reusable', 'new_requests_required', 'held_for_scope_review', 'no_usable_text']:
    lines.append(f'| {status.replace("_", " ")} | {counts[status]:,} | {complete_counts[status]:,} |')
lines += ['', f'The prepared queue has {len(requests):,} requests; {sum(r["reusable_successful_requests"] for r in plan):,} have an exactly matching successful request hash from the old run. Reuse is a byte-equivalent input check, not validation of the substantive answer. No corrected labels or new answers have been invented.', '',
    '# Attachment coverage', '',
    f'{len(unresolved_sources):,} distinct source PDFs contain unresolved page scope. Every report that would use one of those unresolved pages is held outside the prepared queue, with an explicit needs-scope-review status. It remains on the complete roster. These are unresolved application boundaries, not reports coded as having no opposition.', '',
    '| Page-to-narrative assignment | Count |', '|---|---:|']
for scope, count in sorted(scope_counts.items()):
    lines.append(f'| {scope.replace("_", " ")} | {count:,} |')
lines += ['', '# Checked cases', '',
    'C 920197 PPQ now uses only its own source PDF. Pages 1–6 remain; pages 7–11 concern unrelated Manhattan applications and cannot enter its questions. The unrelated Brooklyn civic opposition is absent from its packet.', '',
    'The Freeman Street and Brownsville related-action reports remain connected. The unreadable Brownsville map remains unresolved. C 920049 HDK remains available to C 910119 HUK, preserving the Councilmember Wooten passage on page 11. The unrelated Saratoga Square statement appended to C 910119 HUK, pages 24–27, is excluded. The Bay Ridge attachment page 74 remains in scope; the entire report is still held if any other page remains unresolved.', '',
    'The remaining repair work is a focused review of unresolved attachment blocks and continued validation of Council/civic actor attribution and adopted-versus-requested commitments. No estimated improvement in Jev accuracy is claimed before running a separate validation sample. Jev remains paused.', '']
Path('../output/cpc_jev_scope_findings_v3.md').write_text('\n'.join(lines))
print(f'Source repair checked: {len(new)} narratives; {dict(counts)}. No API calls.')
