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
# Civic groups (Jacob's review, September 28): civic, neighborhood, tenant and community
# organizations and unions, including their officers and unnamed community groups that
# clearly take a side. Businesses count only as associations (a merchants association,
# a chamber of commerce); individual businesses, facility operators and institutions
# do not.
CIVIC_ROLES = {'civic_organization', 'labor_union'}
BUSINESS_ASSOCIATION = re.compile(r'associat|chamber|alliance|coalition|council|merchants|improvement district|\bbid\b|'
                                  r'partnership|federation|league|society|committee|union|club', re.I)


def civic_group(r):
    if 'operator_or_occupant' in r['actor_roles']:
        return False
    return bool(r['actor_roles'] & CIVIC_ROLES) or (
        'business_or_trade_group' in r['actor_roles'] and bool(BUSINESS_ASSOCIATION.search(r['actor_name'])))
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
    out['civic_group_position'] = position([r for r in local if civic_group(r)])
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
TENS = {'twenty': 20, 'thirty': 30, 'forty': 40, 'fifty': 50}
NUMBER = r'(\d+|(?:twenty|thirty|forty|fifty)-\w+|' + '|'.join(NUMBER_WORDS) + r')'
# A stated hearing tally: "six speakers", "13 appearances", "Twenty-nine speakers".
TALLY = re.compile(NUMBER + r'\s+(?:[\w-]+\s+){0,2}?(speakers?|people|persons|individuals|appearances)\b', re.I)
# A named group with a number: "Five members of local art organizations". Not "36 letters".
HEADCOUNT = re.compile(NUMBER + r'\s+(?:[\w-]+\s+){0,3}?'
                       r'(speakers?|people|persons|individuals|residents|members|representatives)\b', re.I)
# Rows standing for all speakers on one side ("six speakers", "Speakers in favor", "29"),
# as opposed to a named person or group.
GENERIC_SPEAKERS = re.compile(r'^\W*(\d+|' + '|'.join(NUMBER_WORDS) + r'|several|many|other|a number of|those|the|all|few)?'
                              r'\b.{0,25}\b(speakers?|people|persons|individuals|appearances)\b|^\W*\d+\W*$', re.I)
# Plural groups with no number: their size is unknown.
PLURAL = re.compile(r'\b(representatives|members|residents|speakers|owners|officials|people|individuals|persons|others|'
                    r'neighbors|tenants|groups)\b', re.I)


def number(text):
    text = text.lower()
    if text.isdigit():
        return int(text)
    if '-' in text:
        tens, ones = text.split('-', 1)
        return TENS[tens] + NUMBER_WORDS.get(ones, 0)
    return NUMBER_WORDS[text]


def first_number(pattern, r):
    for text in (r['actor_name'], r['summary']):
        m = pattern.search(text)
        if m:
            return number(m.group(1))
    return None


def speaker_key(name):
    """Collapse name variants of one speaker ("the representative of the applicant",
    "The applicant's representative")."""
    name = re.sub(r"[\u2019']s\b", '', name.lower())
    name = re.sub(r'\b(the|a|an|of|from|for|representatives?|spokes\w+|speaking)\b', ' ', name)
    return ' '.join(re.sub(r'[^a-z0-9 ]', ' ', name).split()[:4])


def speaker_counts(rows):
    """Support and opposition speakers at the CPC hearing, or '' when not recoverable.

    Uses the position rows of hearing speakers (their findings and concerns repeat the
    same people). Stated tallies ("six speakers in favor") win; tallies on different
    hearing dates are added. Otherwise count the distinct named speakers, a named group
    with a number counting that many. A tally with no number, or a plural group with no
    number ("neighborhood residents"), makes the count unknown.
    """
    out = {}
    for side, stances in (('support', {'support', 'conditional_support'}), ('opposition', {'oppose'})):
        hearing = [r for r in rows if r['stage'] == 'cpc_hearing' and r['stance_on_project'] in stances
                   and r['statement_type'] == 'position' and not r['actor_roles'] & {'cpc', 'city_planning_staff'}
                   and r['application'] != 'other_project']
        tallies, named, unknown = {}, {}, False
        for r in hearing:
            if GENERIC_SPEAKERS.search(r['actor_name']):
                n = first_number(TALLY, r)
                if n is None and re.fullmatch(r'\W*\d+\W*', r['actor_name']):
                    n = int(re.sub(r'\D', '', r['actor_name']))
                if n is None:
                    unknown = True
                else:
                    tallies[n, r['timing_note']] = n
            else:
                n = first_number(HEADCOUNT, r)
                if n is None and PLURAL.search(r['actor_name']):
                    unknown = True
                named[speaker_key(r['actor_name'])] = n or 1
        if tallies:
            out[f'cpc_{side}_speakers'] = str(sum(tallies.values()))
        elif unknown:
            out[f'cpc_{side}_speakers'] = ''
        else:
            out[f'cpc_{side}_speakers'] = str(sum(named.values()))
    return out
