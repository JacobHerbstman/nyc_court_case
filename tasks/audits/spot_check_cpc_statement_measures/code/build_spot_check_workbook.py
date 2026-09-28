"""Build the spot-check review: first-pass codes beside the derived measures.

For each sampled report and measure:
- the first-pass value, quote and note (blind Claude coding from source text,
  data_raw/cpc_spot_check_first_pass/);
- the value derived from the full run's statement rows, with the rows behind it.
Every first-pass quote is checked against its cited segment. Jacob reviews a
priority set: every item where the first pass and the derived value differ, plus
25 seeded agreements (15 council member or civic group, 10 other) to check they are
not both wrong. The rest are on a separate, optional sheet. Jacob's answers are then saved as a committed table
before any rebuild, and the script refuses to overwrite a workbook that has answers.
"""
# Interactive use: cd to tasks/audits/spot_check_cpc_statement_measures/code, then
# python3 build_spot_check_workbook.py
import csv
import hashlib
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill

sys.path.insert(0, '../../../_lib')
from cpc_statement_measures import CIVIC_ROLES, asks, is_local, opposes, parse
from data_reports import save_csv

csv.field_size_limit(10**9)
MEASURES = ['councilmember_position', 'civic_group_position', 'bp_request_or_opposition', 'cb_request_or_opposition',
            'substantial_local_opposition', 'cpc_support_speakers', 'cpc_opposition_speakers']
CATEGORIES = {'support_or_request', 'opposition', 'none_or_procedural'}
REVIEW = '../output/cpc_spot_check_review.xlsx'
REVIEW_COLUMNS = ['jacob_agrees', 'jacob_value', 'jacob_note']


def evidence_rows(rows, measure):
    """The statement rows each derived measure reads, as in cpc_statement_measures."""
    local = [r for r in rows if is_local(r)]
    if measure == 'councilmember_position':
        return [r for r in local if 'council_member' in r['actor_roles']]
    if measure == 'civic_group_position':
        return [r for r in local if r['actor_roles'] & CIVIC_ROLES]
    if measure in ('bp_request_or_opposition', 'cb_request_or_opposition'):
        role = 'borough_president' if measure.startswith('bp') else 'community_board'
        return [r for r in local if role in r['actor_roles'] and (asks(r) or opposes(r))]
    if measure == 'substantial_local_opposition':
        return [r for r in local if opposes(r)]
    stances = {'support', 'conditional_support'} if 'support' in measure else {'oppose'}
    return [r for r in rows if r['stage'] == 'cpc_hearing' and r['stance_on_project'] in stances]


# Saved answers (Jacob's review, plus page checks by Claude) are filled back into the
# workbook. A workbook holding answers not yet saved to code/ is never overwritten.
saved = {}
if Path('jacob_spot_check_review.csv').exists():
    with open('jacob_spot_check_review.csv') as f:
        saved = {(r['document_id'], r['measure']): r for r in csv.DictReader(f)}
if Path(REVIEW).exists():
    for sheet in load_workbook(REVIEW).worksheets[1:3]:
        header = [c.value for c in sheet[1]]
        for row in sheet.iter_rows(min_row=2, values_only=True):
            d = dict(zip(header, row))
            answer = str(d.get('jacob_agrees') or '').strip().upper()
            key = (d['document_id'], d['measure'])
            if answer and (key not in saved or saved[key]['agrees'] != answer):
                sys.exit(f'{REVIEW} has review answers not saved in code/jacob_spot_check_review.csv.')

with open('cpc_spot_check_sample.csv') as f:
    sample = list(csv.DictReader(f))
docs = {r['document_id'] for r in sample}
segments = {}
with open('../input/ulurp_cpc_reading_segments.csv') as f:
    for r in csv.DictReader(f):
        if r['document_id'] in docs:
            segments[r['document_id'], r['segment_id']] = r
derived = defaultdict(dict)
with open('../input/cpc_statement_full_run_report_measures.csv') as f:
    for r in csv.DictReader(f):
        if r['document_id'] in docs:
            derived[r['document_id']][r['measure']] = r['value']
statements = defaultdict(list)
with open('../input/ulurp_cpc_statements_full_sol_high_20260927.csv') as f:
    for r in csv.DictReader(f):
        if r['document_id'] in docs and r['application'] != 'other_project':
            statements[r['document_id']].append(parse(r))
rules = dict(re.findall(r'^## (\w+)\n(.*?)(?=^## |\Z)', Path('spot_check_codebook.md').read_text(), re.M | re.S))

items = []
for s in sample:
    doc = s['document_id']
    first = json.loads(Path(f'../input/first_pass/{doc}.json').read_text())
    assert first['document_id'] == doc and set(first['measures']) == set(MEASURES), doc
    pdf_url = next(v['public_pdf_url'] for (d, _), v in sorted(segments.items()) if d == doc)
    for measure in MEASURES:
        f = first['measures'][measure]
        value = f['value']
        if measure.endswith('_speakers'):
            assert value == '' or value.isdigit(), (doc, measure, value)
        elif measure.endswith('_position'):
            assert value in CATEGORIES, (doc, measure, value)
        else:
            assert value in {'0', '1'}, (doc, measure, value)
        page = quote_pdf_url = ''
        if f['quote']:
            segment = segments[doc, f['segment_id']]
            assert ' '.join(f['quote'].split()) in ' '.join(segment['text'].split()), (doc, measure)
            page = f"{segment['source_application_number']} p.{segment['pdf_page']}"
            quote_pdf_url = segment['public_pdf_url']
        rows = evidence_rows(statements[doc], measure)
        items.append(dict(
            document_id=doc, application_number=s['application_number'], year=s['year'], project_name=s['project_name'],
            sample_cell=s['sample_cell'], pdf_url=pdf_url, measure=measure, first_pass_value=value,
            first_pass_quote=f['quote'], first_pass_page=page, quote_pdf_url=quote_pdf_url, first_pass_note=f['note'],
            derived_value=derived[doc][measure],
            derived_evidence='\n'.join(f"[{r['statement_type']}/{r['stance_on_project']}] {r['actor_name']}: {r['summary']}"
                                       for r in rows[:4]) + (f'\n(+{len(rows) - 4} more)' if len(rows) > 4 else ''),
            first_pass_matches_derived=str(int(value == derived[doc][measure]))))
rank = lambda i: hashlib.sha256(f"cpc-spot-check-priority|{i['document_id']}|{i['measure']}".encode()).hexdigest()
agreements = sorted((i for i in items if i['first_pass_matches_derived'] == '1'), key=rank)
actors = [i for i in agreements if i['measure'] in ('councilmember_position', 'civic_group_position')][:15]
others = [i for i in agreements if i['measure'] not in ('councilmember_position', 'civic_group_position')][:10]
checked = {(i['document_id'], i['measure']) for i in actors + others}
for i in items:
    i['priority'] = str(int(i['first_pass_matches_derived'] == '0' or (i['document_id'], i['measure']) in checked))
save_csv(items, list(items[0]), '../output/cpc_spot_check_items.csv', key=['document_id', 'measure'])

book = Workbook()
guide = book.active
guide.title = 'Instructions'
for line in [
        'Spot check of CPC report measures: 45 reports x 7 measures (315 items).',
        'START HERE: the Review sheet has the priority items only. The Other items sheet is optional.',
        'Review = every item where the first pass and the derived value differ (shaded), plus 25 random agreements.',
        'first_pass_value is a blind Claude coding from the report text, with its quote and page.',
        'derived_value is what the full statement run produces; derived_evidence shows the rows behind it.',
        'Rows where the two differ are shaded. Rows are grouped by report; pdf_url opens the report.',
        'quote_pdf_url opens the PDF the quote comes from; it can be a companion report in the same bundle.',
        'For each row, fill jacob_agrees with Y or N for the FIRST-PASS value; if N, put the right value in jacob_value.',
        'Values: positions support_or_request / opposition / none_or_procedural; BP, CB and opposition 1/0;',
        'speaker counts an integer, or blank when the report gives no exact count.',
        'Definitions are on the Codebook sheet. Save the file in place when done.']:
    guide.append([line])
guide.column_dimensions['A'].width = 120
columns = ['document_id', 'year', 'project_name', 'pdf_url', 'measure', 'first_pass_value', 'first_pass_quote',
           'first_pass_page', 'quote_pdf_url', 'first_pass_note', 'derived_value', 'derived_evidence'] + REVIEW_COLUMNS
shade = PatternFill('solid', fgColor='FCE4D6')
widths = dict(document_id=12, year=6, project_name=24, pdf_url=14, measure=26, first_pass_value=16, first_pass_quote=50,
              first_pass_page=14, quote_pdf_url=14, first_pass_note=40, derived_value=16, derived_evidence=60,
              jacob_agrees=12, jacob_value=16, jacob_note=30)
for title, keep in (('Review', '1'), ('Other items', '0')):
    sheet = book.create_sheet(title)
    sheet.append(columns)
    for item in (i for i in items if i['priority'] == keep):
        answer = saved.get((item['document_id'], item['measure']))
        filled = dict(jacob_agrees=answer['agrees'], jacob_value=answer['reviewed_value'] if answer['agrees'] == 'N' else '',
                      jacob_note=answer['note'] if answer['reviewer'] == 'jacob' else f"(Claude page check) {answer['note']}") if answer else {}
        sheet.append([filled.get(c, item.get(c, '')) for c in columns])
        if item['first_pass_matches_derived'] == '0':
            for cell in sheet[sheet.max_row]:
                cell.fill = shade
    for cell in sheet[1]:
        cell.font = Font(bold=True)
    for i, c in enumerate(columns, 1):
        sheet.column_dimensions[sheet.cell(1, i).column_letter].width = widths[c]
    for row in sheet.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical='top')
    sheet.freeze_panes = 'F2'
codebook = book.create_sheet('Codebook')
for measure in MEASURES:
    codebook.append([measure, ' '.join(rules[measure].split())])
codebook.column_dimensions['A'].width = 30
codebook.column_dimensions['B'].width = 140
for row in codebook.iter_rows():
    for cell in row:
        cell.alignment = Alignment(wrap_text=True, vertical='top')
book.save(REVIEW)
print(f"{len(items)} items; first pass matches derived on {sum(i['first_pass_matches_derived'] == '1' for i in items)}; "
      f"{sum(i['priority'] == '1' for i in items)} priority items")
