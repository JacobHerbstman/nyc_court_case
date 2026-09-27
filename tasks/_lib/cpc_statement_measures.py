"""Report-level CPC measures derived from statement rows.

Shared by tasks/audits/summarize_cpc_statement_report_measures (first-100 audit and
full-run checks) and tasks/audits/spot_check_cpc_statement_measures.
"""
import re

# Actors counted as local: community boards, borough presidents and boards,
# elected officials, organizations and institutions, residents and unidentified
# hearing speakers, excluding anyone on the project team.
LOCAL_ROLES = {'community_board', 'borough_board', 'borough_president', 'council_member',
               'other_elected_official', 'civic_organization', 'business_or_trade_group',
               'labor_union', 'institution', 'resident', 'unidentified_speaker'}
INSTITUTIONAL_LOCAL_ROLES = LOCAL_ROLES - {'resident', 'unidentified_speaker'}
# Named organizations, including their officers even when the reader marks them as
# speaking for themselves ("President of the Block Association").
CIVIC_ROLES = {'civic_organization', 'business_or_trade_group', 'labor_union', 'institution'}
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
    """Codebook actor position: opposing all or part of the proposal is opposition, so an
    objection or concern counts; otherwise support, a request or a commitment is support."""
    if any(opposes(r) or r['statement_type'] == 'concern' for r in rows):
        return 'opposition'
    if any(r['stance_on_project'] in {'support', 'conditional_support'}
           or r['statement_type'] in {'request', 'position', 'commitment'} for r in rows):
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
        'explicit_local_response': any(objects(r) and r['response'] in {'adopted', 'partly_adopted'} for r in local),
        'approved_unresolved_objection': cpc_approves and any(objects(r) and r['response'] == 'rejected' for r in local),
        'cb_request_or_opposition': any((asks(r) or opposes(r)) for r in local if 'community_board' in r['actor_roles']),
        'bp_request_or_opposition': any((asks(r) or opposes(r)) for r in local if 'borough_president' in r['actor_roles']),
    }
    out = {k: str(int(v)) for k, v in out.items()}
    # Procedural response: a local request or concern is answered by a row the reader
    # marked as a study, monitoring or reporting, task force, or outreach/consultation.
    # Only runs from September 27 have procedural_action; earlier runs leave it blank.
    if rows and 'procedural_action' in rows[0]:
        by_id = {r['statement_id']: r for r in rows}
        out['procedural_response'] = str(int(any(
            by_id[i]['procedural_action'] not in {'none', 'other_procedural'}
            for r in local if objects(r) for i in r['response_statement_ids'] if i in by_id)))
    else:
        out['procedural_response'] = ''
    out['councilmember_position'] = position([r for r in local if 'council_member' in r['actor_roles']])
    out['civic_group_position'] = position([r for r in local if r['actor_roles'] & CIVIC_ROLES])
    # An issue counts when a local actor raises it: opposes, objects or asks for
    # something about it. Mentions only in the project description or in CPC's
    # own findings (e.g. routine environmental review) do not count.
    for name, topics in ISSUE_TOPICS.items():
        out[name] = str(int(any(r['topics'] & topics for r in local if objects(r))))
    return out


def parse(r):
    """Set-valued roles and topics and a list of response IDs; the CSV joins lists with ';'."""
    r = dict(r)
    r['statement_id'] = str(r['statement_id'])
    ids = r['response_statement_ids']
    r['response_statement_ids'] = [str(i) for i in ids] if isinstance(ids, list) else [i for i in ids.split(';') if i]
    for field in ('actor_roles', 'topics'):
        value = r[field]
        r[field] = set(value) if isinstance(value, list) else {v for v in value.split(';') if v}
    return r


NUMBER_WORDS = {w: i for i, w in enumerate(
    'zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen '
    'sixteen seventeen eighteen nineteen twenty'.split())}
# A number followed within three words by a noun for people: "six speakers", "Five
# applicant-team representatives", "three members of the Board". Not "36 letters".
HEADCOUNT = re.compile(r'\b(\d+|' + '|'.join(NUMBER_WORDS) + r')\s+(?:[\w-]+\s+){0,3}?'
                       r'(speakers?|people|persons|individuals|residents|members|representatives)\b', re.I)
# Rows standing for all speakers on one side ("six speakers", "Speakers in favor"),
# as opposed to a named person or group.
GENERIC_SPEAKERS = re.compile(r'\b(speakers?|people|persons|individuals)\b', re.I)


def headcount(r):
    """People a row stands for when it states a number, else None."""
    for text in (r['actor_name'], r['summary']):
        m = HEADCOUNT.search(text)
        if m:
            value = m.group(1).lower()
            return int(value) if value.isdigit() else NUMBER_WORDS[value]
    return None


def speaker_counts(rows):
    """Support and opposition speakers at the CPC hearing, or '' when not recoverable.

    Reports usually give a tally ("six speakers in favor") and then describe some of
    the speakers, who also get their own rows. For each side, take the larger of the
    stated tally and the number of individually described speakers (a named group
    with a stated number counts that many). If the only tally row gives no number,
    the count is unknown.
    """
    out = {}
    for side, stances in (('support', {'support', 'conditional_support'}), ('opposition', {'oppose'})):
        hearing = [r for r in rows if r['stage'] == 'cpc_hearing' and r['stance_on_project'] in stances
                   and not r['actor_roles'] & {'cpc', 'city_planning_staff'} and r['application'] != 'other_project']
        tally_rows = [r for r in hearing if GENERIC_SPEAKERS.search(r['actor_name'])]
        tallies = [n for n in map(headcount, tally_rows) if n is not None]
        named = {r['actor_name']: headcount(r) or 1 for r in hearing if r not in tally_rows}
        if tally_rows and not tallies and not named:
            out[f'cpc_{side}_speakers'] = ''
        else:
            out[f'cpc_{side}_speakers'] = str(max([sum(named.values())] + tallies))
    return out
