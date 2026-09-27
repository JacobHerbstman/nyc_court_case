"""Derive report-level CPC measures from statement rows, then test them.

Three outputs:
1. One row per report and measure for the 100 read reports.
2. For the 20 audited reports, each measure recomputed after applying the
   source audit (drop rows judged clear errors, add inventory statements the
   extraction missed or only partly captured, and the inventory counterparts of
   erroneous rows), to see whether row-level errors move report-level measures.
3. Agreement with the earlier human codes on the 20 human-coded reports.
"""
import csv
import json
import sys
from collections import defaultdict

sys.path.insert(0, '../../../_lib')
from data_reports import save_csv

csv.field_size_limit(10**8)

# Actors counted as local: community boards, borough presidents and boards,
# elected officials, organizations, residents and unidentified hearing speakers,
# excluding anyone on the project team.
LOCAL_ROLES = {'community_board', 'borough_board', 'borough_president', 'council_member',
               'other_elected_official', 'civic_organization', 'business_or_trade_group',
               'labor_union', 'resident', 'unidentified_speaker'}
INSTITUTIONAL_LOCAL_ROLES = LOCAL_ROLES - {'resident', 'unidentified_speaker'}
CIVIC_ROLES = {'civic_organization', 'business_or_trade_group', 'labor_union'}
ISSUE_TOPICS = {
    'affordability_displacement': {'affordability', 'displacement'},
    'environment_open_space': {'environment_open_space'},
    'infrastructure_services': {'infrastructure_services'},
    'traffic_parking': {'traffic_parking'},
    'scale_character_preservation': {'scale_density_design', 'neighborhood_character', 'historic_preservation'},
}


def is_local(r):
    return bool(r['actor_roles'] & LOCAL_ROLES) and r['project_team'] != 'yes'


def asks(r):
    return r['statement_type'] == 'request' or r['stance_on_project'] == 'conditional_support'


def opposes(r):
    return r['stance_on_project'] in {'oppose', 'mixed'}


def objects(r):
    return opposes(r) or r['statement_type'] in {'request', 'concern'}


def position(rows):
    if any(opposes(r) for r in rows):
        return 'opposition'
    if any(r['stance_on_project'] in {'support', 'conditional_support'} or r['statement_type'] in {'request', 'position'}
           for r in rows):
        return 'support_or_request'
    return 'none_or_procedural'


def measures(rows):
    """Report-level measures, named as in the human codebook where one exists."""
    rows = [r for r in rows if r['application'] != 'other_project']
    local = [r for r in rows if is_local(r)]
    opposing_individuals = {r['actor_name'] for r in local if opposes(r) and not r['actor_roles'] & INSTITUTIONAL_LOCAL_ROLES}
    cpc_approves = any('cpc' in r['actor_roles'] and r['statement_type'] == 'decision'
                       and r['stance_on_project'] in {'support', 'conditional_support'} for r in rows)
    out = {
        'substantial_local_opposition': any(opposes(r) and r['actor_roles'] & INSTITUTIONAL_LOCAL_ROLES for r in local)
        or len(opposing_individuals) >= 2,
        'local_request_condition': any(asks(r) for r in local),
        'revision_or_concession': any(r['statement_type'] in {'modification', 'commitment'} and r['topics'] - {'process'}
                                      for r in rows),
        'procedural_response': any(r['statement_type'] in {'modification', 'commitment', 'requirement'} and 'process' in r['topics']
                                   for r in rows),
        'explicit_local_response': any(objects(r) and r['response'] in {'adopted', 'partly_adopted'} for r in local),
        'approved_unresolved_objection': cpc_approves and any(objects(r) and r['response'] == 'rejected' for r in local),
        'cb_request_or_opposition': any((asks(r) or opposes(r)) for r in local if 'community_board' in r['actor_roles']),
        'bp_request_or_opposition': any((asks(r) or opposes(r)) for r in local if 'borough_president' in r['actor_roles']),
    }
    out = {k: str(int(v)) for k, v in out.items()}
    out['councilmember_position'] = position([r for r in local if 'council_member' in r['actor_roles']])
    out['civic_group_position'] = position([r for r in local if r['actor_roles'] & CIVIC_ROLES and r['speaks_for'] == 'organization'])
    # An issue counts when a local actor raises it: opposes, objects or asks for
    # something about it. Mentions only in the project description or in CPC's
    # own findings (e.g. routine environmental review) do not count.
    for name, topics in ISSUE_TOPICS.items():
        out[name] = str(int(any(r['topics'] & topics for r in local if objects(r))))
    return out


def parse(r):
    """Set-valued roles and topics; the CSV joins them with ';', the JSON uses lists."""
    r = dict(r)
    for field in ('actor_roles', 'topics'):
        value = r[field]
        r[field] = set(value) if isinstance(value, list) else {v for v in value.split(';') if v}
    return r


statements = defaultdict(dict)
with open('../input/ulurp_cpc_statements.csv') as f:
    for r in csv.DictReader(f):
        statements[r['document_id']][int(r['statement_id'])] = parse(r)
assert len(statements) == 100

report_rows = []
for doc, rows in sorted(statements.items()):
    for measure, value in measures(rows.values()).items():
        report_rows.append({'document_id': doc, 'measure': measure, 'value': value})
save_csv(report_rows, ['document_id', 'measure', 'value'], '../output/cpc_statement_report_measures.csv',
         key=['document_id', 'measure'])

# Audit sensitivity: rebuild each audited report's rows from the source audit.
assessments = defaultdict(dict)
with open('../input/cpc_statement_audit_assessments.csv') as f:
    for r in csv.DictReader(f):
        assessments[r['document_id'], r['side']][int(r['statement_id'])] = (r['assessment'], json.loads(r['extraction_statement_ids']))
audited = sorted({doc for doc, _ in assessments})
assert len(audited) == 20

sensitivity_rows = []
for doc in audited:
    extraction = assessments[doc, 'extraction']
    assert set(extraction) == set(statements[doc]), doc
    with open(f'../input/{doc}_inventory.json') as f:
        inventory = {s['statement_id']: parse(s) for s in json.load(f)['statements']}
    errors = {i for i, (assessment, _) in extraction.items() if assessment == 'clear_error'}
    unclear = {i for i, (assessment, _) in extraction.items() if assessment == 'unclear'}
    added = [inventory[i] for i, (assessment, matched) in assessments[doc, 'inventory'].items()
             if assessment in {'omitted', 'partly_covered'} or set(matched) & errors]
    kept = [r for i, r in statements[doc].items() if i not in errors]
    versions = {
        'original': measures(statements[doc].values()),
        'audit_corrected': measures(kept + added),
        'audit_corrected_drop_unclear': measures([r for i, r in statements[doc].items() if i not in errors | unclear] + added),
    }
    for measure, value in versions['original'].items():
        sensitivity_rows.append({'document_id': doc, 'measure': measure, 'original': value,
                                 'audit_corrected': versions['audit_corrected'][measure],
                                 'audit_corrected_drop_unclear': versions['audit_corrected_drop_unclear'][measure],
                                 'clear_error_rows': len(errors), 'unclear_rows': len(unclear), 'inventory_rows_added': len(added)})
save_csv(sensitivity_rows, list(sensitivity_rows[0]), '../output/cpc_statement_measure_audit_sensitivity.csv',
         key=['document_id', 'measure'])

# Agreement with earlier human codes, for fields defined in both.
derived = {(r['document_id'], r['measure']): r['value'] for r in report_rows}
corrected = {(r['document_id'], r['measure']): r['audit_corrected'] for r in sensitivity_rows}
human_rows = []
with open('../input/ulurp_cpc_human_coding.csv') as f:
    for r in csv.DictReader(f):
        key = (r['source_document_id'], r['field'])
        if key not in derived or r['human_value'] == '':
            continue
        human_rows.append({'document_id': key[0], 'measure': key[1], 'human_value': r['human_value'],
                           'human_status': r['human_status'], 'derived_value': derived[key],
                           'agree': str(int(derived[key] == r['human_value'])),
                           'audit_corrected_value': corrected.get(key, ''),
                           'audit_corrected_agree': str(int(corrected[key] == r['human_value'])) if key in corrected else ''})
assert len({r['document_id'] for r in human_rows}) == 20
save_csv(human_rows, list(human_rows[0]), '../output/cpc_statement_measure_human_agreement.csv',
         key=['document_id', 'measure'])
