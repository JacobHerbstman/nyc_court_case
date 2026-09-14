#!/usr/bin/env python3

import csv
import hashlib
import math
import re
import sys
from collections import defaultdict
from pathlib import Path

RESOLUTION_SECTION_HEADING = re.compile(
    r"(?im)^[ \t\f]*RESOLUTION[ \t]*:?[ \t]*$"
)
CPC_RESOLVED_HEADING = re.compile(
    r"(?im)^[ \t\f]*RESOLVED(?:[ \t]*,|[ \t]+BY\b|[ \t]+THAT\b)"
    r"(?=[\s\S]{0,300}?\bCITY[ \t\r\n]+PLANNING[ \t\r\n]+COMMISSION\b).*$"
)
FILING_PARAGRAPH = re.compile(
    r"(?is)(?:the[ \t\r\n]+(?:above|foregoing)[ \t\r\n]+resol\w*|"
    r"the[ \t\r\n]+resol\w*[ \t\r\n]*\([^)]{1,80}\))"
    r".{0,1600}?(?:is[ \t\r\n]+)?(?:hereby[ \t\r\n]+|herewith[ \t\r\n]+)?"
    r"(?:filed|fuled|tiled|ffled)"
)
ANCHOR_HEADING = re.compile(
    r"(?im)^[ \t\f]*(?:CONSIDERATION|FINDINGS(?:[ \t]+AND[ \t]+(?:APPROVAL|RECOMMENDATIONS?))?|"
    r"UNIFORM[ \t]+LAND[ \t]+USE[ \t]+REVIEW(?:[ \t]+PROCEDURE)?)[ \t]*:?\s*$"
)
PAGE_HEADER = re.compile(
    r"(?i)^\s*(?:page\s+)?\d+\s+(?:C\s*)?\d{6}(?:\s*\([A-Z]\))?\s*[A-Z]{2,4}\s*$"
)
COMMISSION_SIGNATURE = re.compile(
    r"(?im)^[ \t\f]*[A-Z][A-Za-z.'-]+(?:[ \t]+[A-Z][A-Za-z.'-]+){1,5},?[ \t]+"
    r"(?:Chair|Chairman|Chairperson|Vice[- ]?Chairman|Vice[- ]?Chairperson)\b.*$",
    re.IGNORECASE | re.MULTILINE,
)
ADOPTED_RESOLUTION = re.compile(
    r"(?is)(?:city[ \t\r\n]+planning[ \t\r\n]+commission|the[ \t\r\n]+commission)"
    r".{0,260}?(?:adopts?|adopted).{0,80}?(?:following[ \t\r\n]+)?resol\w*"
)
MANUAL_EXCLUSION_METHODS = {
    "exclude_incomplete_source",
    "exclude_supplemental_statement_without_main_report",
    "exclude_related_action_covered_by_companion",
}

SECTION_ORDER = [
    "background",
    "environmental_review",
    "ulurp",
    "community_board",
    "borough_president",
    "cpc_hearing",
    "consideration_findings",
    "resolution",
    "unsectioned",
]

SECTION_LABELS = {
    "background": "background",
    "project_description": "background",
    "description_of_project": "background",
    "environmental_review": "environmental_review",
    "environmental_assessment": "environmental_review",
    "environmental_impact": "environmental_review",
    "ceqr": "environmental_review",
    "uniform_land_use_review": "ulurp",
    "ulurp": "ulurp",
    "community_board": "community_board",
    "community_board_public_hearing": "community_board",
    "community_board_recommendation": "community_board",
    "community_board_review": "community_board",
    "community_board_action": "community_board",
    "borough_president": "borough_president",
    "borough_president_recommendation": "borough_president",
    "borough_president_review": "borough_president",
    "borough_president_public_hearing": "borough_president",
    "city_planning_commission_public_hearing": "cpc_hearing",
    "cpc_public_hearing": "cpc_hearing",
    "summary_of_public_hearing": "cpc_hearing",
    "public_hearing": "cpc_hearing",
    "consideration": "consideration_findings",
    "consideration_by_the_city_planning_commission": "consideration_findings",
    "findings": "consideration_findings",
    "commission_findings": "consideration_findings",
    "land_use_considerations": "consideration_findings",
    "other_considerations": "consideration_findings",
    "resolution": "resolution",
    "resolved": "resolution",
}

HEADING_PATTERNS = [
    ("background", r"BACKGROUND(?: AND DESCRIPTION)?"),
    ("project_description", r"PROJECT DESCRIPTION"),
    ("description_of_project", r"DESCRIPTION OF PROJECT"),
    ("environmental_review", r"ENVIRONMENTAL REVIEW"),
    ("environmental_assessment", r"ENVIRONMENTAL ASSESSMENT"),
    ("environmental_impact", r"ENVIRONMENTAL IMPACT"),
    ("ceqr", r"CEQR"),
    ("uniform_land_use_review", r"UNIFORM LAND USE REVIEW(?: PROCEDURE)?"),
    ("ulurp", r"ULURP"),
    ("community_board_public_hearing", r"COMMUNITY BOARD PUBLIC HEARING"),
    ("community_board_recommendation", r"COMMUNITY BOARD RECOMMENDATION"),
    ("community_board_review", r"COMMUNITY BOARD REVIEW"),
    ("community_board_action", r"COMMUNITY BOARD ACTION"),
    ("community_board", r"COMMUNITY BOARD"),
    ("borough_president_recommendation", r"BOROUGH PRESIDENT(?:'S)? RECOMMENDATION"),
    ("borough_president_review", r"BOROUGH PRESIDENT(?:'S)? REVIEW"),
    ("borough_president_public_hearing", r"BOROUGH PRESIDENT(?:'S)? PUBLIC HEARING"),
    ("borough_president", r"BOROUGH PRESIDENT"),
    ("city_planning_commission_public_hearing", r"CITY PLANNING COMMISSION PUBLIC HEARING"),
    ("cpc_public_hearing", r"CPC PUBLIC HEARING"),
    ("summary_of_public_hearing", r"SUMMARY OF (?:THE )?PUBLIC HEARING"),
    ("public_hearing", r"PUBLIC HEARING"),
    (
        "consideration_by_the_city_planning_commission",
        r"CONSIDERATION BY THE CITY PLANNING COMMISSION",
    ),
    ("consideration", r"CONSIDERATION"),
    ("commission_findings", r"COMMISSION FINDINGS"),
    ("land_use_considerations", r"LAND USE CONSIDERATIONS"),
    ("other_considerations", r"OTHER CONSIDERATIONS"),
    ("findings", r"FINDINGS"),
    ("resolution", r"RESOLUTION"),
    ("resolved", r"RESOLVED"),
]

MONTH_PATTERN = (
    r"january|february|march|april|may|june|july|august|september|"
    r"october|november|december"
)

APPLICATION_REFERENCE = re.compile(
    r"(?<![A-Z0-9])(?:[CN][ \t]*)?\d{6}(?:[ \t]*\([A-Z]\)|[A-Z])?"
    r"[ \t]*[A-Z]{2,4}[A-Z](?![A-Z0-9])",
    re.IGNORECASE,
)
ANY_RESOLVED_HEADING = re.compile(
    r"(?im)^[ \t\f]*RESOLVED(?:[ \t]*,|[ \t]+BY\b|[ \t]+THAT\b).*$"
)

NUMBER_WORDS = {
    "no": 0,
    "none": 0,
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
    "thirty": 30,
    "forty": 40,
    "fifty": 50,
    "sixty": 60,
}
NUMBER_TOKEN = (
    r"(?:\d{1,3}|no|none|zero|one|two|three|four|five|six|seven|eight|nine|ten|"
    r"eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|"
    r"twenty(?:[- ](?:one|two|three|four|five|six|seven|eight|nine))?|"
    r"thirty(?:[- ](?:one|two|three|four|five|six|seven|eight|nine))?|"
    r"forty(?:[- ](?:one|two|three|four|five|six|seven|eight|nine))?|"
    r"fifty(?:[- ](?:one|two|three|four|five|six|seven|eight|nine))?|sixty)"
)

NO_APPEARANCES = re.compile(
    r"\b(?:there (?:was|were) )?no appearances?\b|"
    r"\bthere were no speakers (?:on (?:this|the) application|and the hearing was closed)\b",
    re.IGNORECASE,
)
NO_OTHER_SPEAKERS = re.compile(
    r"\bthere were no other speakers\b|\bno other speakers appeared\b",
    re.IGNORECASE,
)
SUPPORT_TERM = r"(?:in favor|in support|spoke in favor|testified in favor|supporting the (?:application|proposal|project))"
OPPOSITION_TERM = r"(?:in opposition|spoke against|testified against|against the (?:application|proposal|project|proposed [a-z -]+)|opposing the (?:application|proposal|project))"

REVIEW_ACTION = re.compile(
    r"\b(?:oppos\w*|object\w*|disapprov\w*|concern\w*|request\w*|condition\w*|"
    r"recommend\w*|support\w*|urge\w*|ask\w*|testif\w*|spoke|speaker\w*|"
    r"impact\w*|effect\w*|mitigat\w*|address\w*|respond\w*|appropriate|adequate|"
    r"should|must|would|could)\b",
    re.IGNORECASE,
)
OPPOSITION = re.compile(
    r"\b(?:oppos\w*|object\w*|disapprov\w*|unfavorable|against the (?:application|"
    r"proposal|project)|speaker\w* in opposition|testif\w* against)\b",
    re.IGNORECASE,
)
NO_OPPOSITION = re.compile(
    r"\b(?:no|none|zero) (?:speakers? )?(?:in opposition|opposed|against)\b|"
    r"\bwithout opposition\b|\bno objections?\b",
    re.IGNORECASE,
)
SUBSTANTIVE_REQUEST = re.compile(
    r"\b(?:request\w*|condition\w*|provided that|subject to|with the following "
    r"(?:conditions?|modifications?)|urge\w*|ask\w*|call\w* for|should|must|"
    r"recommend\w* disapproval|disapprov\w*|reject\w*|delay\w*|alternative)\b",
    re.IGNORECASE,
)
MINOR_OR_PROCEDURAL_REQUEST = re.compile(
    r"\b(?:referred to|for information and review|waived? (?:its )?public hearing|"
    r"recommendation to follow|attach additional sheets?|administrative correction)\b",
    re.IGNORECASE,
)
REVISION_OR_CONCESSION = re.compile(
    r"\b(?:(?:in response to (?:the )?(?:concerns?|comments?|requests?|objections?)|"
    r"at the request of|as requested by|after (?:the )?(?:public hearing|meeting)|"
    r"subsequent to (?:the )?(?:public hearing|community board review))"
    r".{0,180}(?:applicant|developer|owner|agency|proposal|application|plans?|design|project)"
    r".{0,120}(?:agreed|committed|revised|modified|changed|reduced|removed|eliminated|"
    r"withdrew|scaled back)|(?:proposal|application|plans?|design) (?:was|were|has been) "
    r"(?:revised|modified|changed|reduced|amended)|agreed to (?:provide|fund|construct|"
    r"maintain|limit|reduce|remove|retain)|committed to (?:provide|fund|construct|"
    r"maintain|limit|reduce|remove|retain))\b",
    re.IGNORECASE,
)
MECHANICAL_REVISION = re.compile(
    r"\b(?:modifications specifically granted|except for (?:the )?modifications|"
    r"modification of (?:use|bulk|height and setback) regulations|last date revised|"
    r"zoning resolution,? as amended|amended urban renewal plan|revised negative declaration|"
    r"revised environmental assessment statement|pursuant to (?:the )?revised .{0,50} text|"
    r"application (?:requests?|seeks?) (?:a )?modification)\b",
    re.IGNORECASE,
)
PROCEDURAL_RESPONSE = re.compile(
    r"\b(?:study|task force|working group|monitor\w*|reporting requirements?|"
    r"outreach program|future (?:meeting\w*|consult\w*)|continued (?:consult\w*|"
    r"coordination|communication)|advisory committee|investigat\w*|evaluat\w*|"
    r"reapply|reapplication|refer\w* .{0,100}(?:request|issue|concern))\b",
    re.IGNORECASE,
)
EXPLICIT_RESPONSE_LINK = re.compile(
    r"\b(?:in response to (?:the )?(?:concerns?|comments?|requests?|objections?)|"
    r"at the request of|as requested by|to address (?:the )?(?:concerns?|requests?|objections?)|"
    r"following (?:concerns?|requests?|objections?) (?:raised|expressed) by)\b",
    re.IGNORECASE,
)
RESPONSE_ACTION = re.compile(
    r"\b(?:revis\w*|modif\w*|chang\w*|reduc\w*|remov\w*|eliminat\w*|agree\w*|"
    r"commit\w*|condition\w*|study|monitor\w*|report\w*|outreach|consult\w*|"
    r"task force|working group|advisory committee|reject\w*|declin\w*|defer\w*|"
    r"refus\w*|not warranted|outside (?:the )?scope|beyond (?:the )?scope)\b",
    re.IGNORECASE,
)
UNRESOLVED_RESPONSE = re.compile(
    r"\b(?:the commission (?:does|did) not (?:agree|believe|find|support)|"
    r"the commission (?:disagrees|declines|rejects)|(?:declined|failed|refused) to "
    r"(?:adopt|accept|include|modify|change|grant)|not warranted|not appropriate|"
    r"not persuaded|cannot support|cannot be accommodated|not feasible|"
    r"outside (?:the )?scope|beyond (?:the )?scope|would not be appropriate|"
    r"remain(?:s|ed)? unmitigated|could not be mitigated|nevertheless|nonetheless|"
    r"despite (?:the )?(?:opposition|objection|disapproval|concerns?))\b",
    re.IGNORECASE,
)

DISPOSITION_DENIED = re.compile(
    r"\b(?:be (?:and )?hereby is disapproved|application (?:is|was) (?:hereby )?"
    r"(?:disapproved|denied)|commission (?:therefore )?(?:denies|denied|disapproves|"
    r"disapproved)|application .{0,500}? (?:is|be) (?:hereby )?(?:denied|disapproved))\b",
    re.IGNORECASE,
)
DISPOSITION_APPROVED = re.compile(
    r"\b(?:be (?:and )?(?:is )?hereby approved|application (?:is|was) (?:hereby )?approved|"
    r"commission (?:therefore )?(?:approves|approved|grants|granted)|"
    r"(?:special permit|authorization|application) .{0,500}? (?:is |be )?(?:hereby )?granted|"
    r"(?:special permit|application) .{0,300}? warrants approval|"
    r"zoning resolution .{0,500} is (?:hereby |further )?amended|"
    r"zoning map .{0,500} is (?:hereby |further )?amended|"
    r"adopted (?:the )?(?:following )?resolution|"
    r"resolution .{0,500}? duly adopted by the city planning commission)\b",
    re.IGNORECASE,
)
CHANGE_ACTION = re.compile(
    r"\b(?:revis\w*|amend\w*|modif\w*|chang\w*|reduc\w*|decreas\w*|"
    r"remov\w*|eliminat\w*|withdraw\w*|delet\w*|limit\w*|relocat\w*|"
    r"redesign\w*|substitut\w*|narrow\w*|scaled? back|cut|retain\w*|"
    r"add(?:ed|ing)?|provid\w*|fund\w*|construct\w*|maintain\w*)\b",
    re.IGNORECASE,
)
CHANGE_SUBJECT = re.compile(
    r"\b(?:applicant|developer|owner|agency|department|proposal|application|"
    r"project|plans?|design|development|commission|city planning|dcp)\b",
    re.IGNORECASE,
)
CHANGE_STAGE = re.compile(
    r"\b(?:as originally (?:filed|proposed|submitted)|original(?:ly)? proposal|"
    r"proposal now|during (?:the )?(?:review|ulurp)|following (?:the )?(?:public hearing|"
    r"community board review|commission review)|after (?:the )?(?:public hearing|"
    r"community board review|commission review)|subsequent to (?:the )?(?:public hearing|"
    r"community board review|commission review)|"
    r"prior to approval|"
    r"since (?:the )?(?:application|hearing|submission))\b",
    re.IGNORECASE,
)
COMMITMENT_ACTION = re.compile(
    r"\b(?:agree\w*|commit\w*|assur\w*|undert(?:ake|ook|aken)|promis\w*|"
    r"execut\w*|enter\w* into)\b",
    re.IGNORECASE,
)
SUBSTANTIVE_COMMITMENT = re.compile(
    r"\b(?:provide|fund|construct|maintain|limit|reduce|remove|retain|relocate|"
    r"redesign|monitor|report|repair|improve|restrict|prohibit|preserve|protect)\w*\b",
    re.IGNORECASE,
)
TRIAL_OR_REAPPLICATION = re.compile(
    r"\b(?:trial period|temporary (?:approval|permit)|limited to (?:a |one |two |three )?"
    r"(?:year|years)|reapply|reapplication|future review)\b",
    re.IGNORECASE,
)
LOCAL_RESPONSE_REFERENCE = re.compile(
    r"\b(?:these|those|such|the foregoing) (?:concerns?|comments?|requests?|objections?)|"
    r"\b(?:community board|borough president|council ?member|councilmember|"
    r"civic (?:group|association)|local community|residents?)'?s? "
    r"(?:concerns?|comments?|requests?|objections?|recommendations?)\b|"
    r"\b(?:the|this|that) (?:request|condition|recommendation|objection)\b|"
    r"\b(?:concerns?|comments?|requests?|objections?|recommendations?) "
    r"(?:raised|expressed|made|submitted) by\b",
    re.IGNORECASE,
)

COUNCIL_ACTOR = re.compile(r"\b(?:council ?member|councilmember)\b", re.IGNORECASE)
COMMUNITY_BOARD_ACTOR = re.compile(
    r"\b(?:community(?: planning)? board(?: no\.?| number| #)?\s*\d*|the board)\b",
    re.IGNORECASE,
)
BOROUGH_PRESIDENT_ACTOR = re.compile(r"\bborough president\b", re.IGNORECASE)
OTHER_LOCAL_ACTOR = re.compile(
    r"\b(?:residents?|neighbou?rs?|community members?|local (?:groups?|organizations?|"
    r"residents?|community)|community (?:wishes|concerns?|requests?)|opponents?|"
    r"members? of the public)\b",
    re.IGNORECASE,
)
NONLOCAL_ACTOR = re.compile(
    r"\b(?:applicant|application|developer|owner|city planning commission|the commission|"
    r"department|agency|administration)\b",
    re.IGNORECASE,
)
ADJACENT_EVENT_LINK = re.compile(
    r"^(?:he|she|they|it|this|these|those|the board|the borough president|"
    r"the council ?member|the group|the organization|the association|the applicant|"
    r"the commission|in response|as a result|subsequent(?:ly)?|following (?:these|those))\b",
    re.IGNORECASE,
)
COUNCIL_PROCEDURE = re.compile(
    r"\b(?:filed with|referred to|transmitted to).{0,100}\b(?:city council|office of the speaker)\b|"
    r"\bpursuant to section 197-d\b",
    re.IGNORECASE,
)
CIVIC_ACTOR = re.compile(
    r"\b(?:civic association|tenant association|neighbou?rhood association|"
    r"community organization|community group|block association|business improvement district|"
    r"chamber of commerce|coalition|conservancy|preservation league|society)\b",
    re.IGNORECASE,
)
SUPPORT_POSITION = re.compile(
    r"\b(?:support\w*|in favor|recommend\w* approval|urge\w* approval)\b",
    re.IGNORECASE,
)
REQUEST_POSITION = re.compile(
    r"\b(?:request\w*|condition\w*|recommend\w*|urge\w*|ask\w*|should|must)\b",
    re.IGNORECASE,
)

ISSUE_PATTERNS = {
    "affordability_displacement": re.compile(
        r"\b(?:affordab\w*|displac\w*|gentrif\w*|tenant protection|harassment|"
        r"housing access|rent burden|permanent affordability)\b",
        re.IGNORECASE,
    ),
    "traffic_parking": re.compile(
        r"\b(?:traffic|parking|loading|trucks?|congestion|curb use|vehicular)\b",
        re.IGNORECASE,
    ),
    "scale_character_preservation": re.compile(
        r"\b(?:scale|height|density|bulk|design|shadows?|neighbou?rhood character|"
        r"out of character|contextual|landmarks?|preserv\w*|historic)\b",
        re.IGNORECASE,
    ),
    "infrastructure_services": re.compile(
        r"\b(?:schools?|sewers?|transit|subways?|sanitation|utilities|public facilities|"
        r"service capacity|water supply|drainage|emergency services)\b",
        re.IGNORECASE,
    ),
    "environment_open_space": re.compile(
        r"\b(?:environmental (?:effects?|impacts?|concerns?)|remediation|contaminat\w*|"
        r"water quality|parks?|waterfront access|open space|flood\w*|resilien\w*|"
        r"air quality|noise impacts?)\b",
        re.IGNORECASE,
    ),
}

BINARY_SIGNAL_FIELDS = [
    "substantial_local_opposition",
    "local_request_condition",
    "revision_or_concession",
    "procedural_response",
    "explicit_local_response",
    "approved_unresolved_objection",
    "cb_opposition",
    "cb_request_or_opposition",
    "bp_request_or_opposition",
    "affordability_displacement",
    "traffic_parking",
    "scale_character_preservation",
    "infrastructure_services",
    "environment_open_space",
    "restrictive_declaration",
    "points_of_agreement",
]

POSITION_FIELDS = ["councilmember_position", "civic_group_position"]
COUNT_FIELDS = [
    "cpc_support_speakers",
    "cpc_opposition_speakers",
    "cb_support_votes",
    "cb_opposition_votes",
]


def clean_text(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def application_key(value):
    compact = re.sub(r"[^A-Z0-9]", "", clean_text(value).upper())
    if re.match(r"^[CN]\d{6}", compact):
        compact = compact[1:]
    return compact


def resolve_task_path(raw_path, manifest_real_path):
    if not clean_text(raw_path):
        return None
    path = Path(clean_text(raw_path))
    if path.is_absolute():
        return path
    return manifest_real_path.parent.parent / "code" / path


def narrative_boundary(text):
    anchor_matches = list(ANCHOR_HEADING.finditer(text))
    anchor = (
        anchor_matches[0].start()
        if anchor_matches and anchor_matches[0].start() < 0.75 * len(text)
        else min(500, len(text))
    )
    resolved_matches = [
        match for match in ANY_RESOLVED_HEADING.finditer(text) if match.start() > anchor
    ]
    cpc_resolved_matches = [
        match for match in CPC_RESOLVED_HEADING.finditer(text) if match.start() > anchor
    ]
    if resolved_matches and cpc_resolved_matches:
        first_resolved = resolved_matches[0]
        first_cpc_resolved = cpc_resolved_matches[0]
        if first_resolved.start() == first_cpc_resolved.start():
            return first_resolved.start(), "resolution_heading"
        resolution_headings = [
            match
            for match in RESOLUTION_SECTION_HEADING.finditer(text)
            if first_resolved.start() < match.start() < first_cpc_resolved.start()
        ]
        if resolution_headings:
            return resolution_headings[-1].start(), "cpc_resolution_after_quoted_resolution"
        return first_cpc_resolved.start(), "cpc_resolution_after_quoted_resolution"
    if resolved_matches:
        return resolved_matches[0].start(), "resolution_heading_fallback"

    for pattern, method in (
        (FILING_PARAGRAPH, "filing_paragraph"),
        (ADOPTED_RESOLUTION, "adopted_resolution_paragraph"),
        (COMMISSION_SIGNATURE, "commission_signature"),
    ):
        matches = [match for match in pattern.finditer(text) if match.start() > anchor]
        if matches:
            return matches[0].start(), method
    return len(text), "full_text_no_boundary_found"


def cpc_disposition(text, narrative_end):
    decision_text = normalize_whitespace(text[max(0, narrative_end - 20000) :])
    if not decision_text:
        decision_text = normalize_whitespace(text[-12000:])
    filing_match = FILING_PARAGRAPH.search(decision_text)
    if filing_match:
        decision_text = decision_text[: filing_match.start()]
    signature_match = COMMISSION_SIGNATURE.search(decision_text)
    if signature_match:
        decision_text = decision_text[: signature_match.start()]

    partial = bool(
        re.search(
            r"\bapproved in part.{0,300}disapproved in part|"
            r"\bdisapproved in part.{0,300}approved in part",
            decision_text,
            re.IGNORECASE,
        )
    )
    denied = bool(DISPOSITION_DENIED.search(decision_text))
    approved = bool(DISPOSITION_APPROVED.search(decision_text))
    if partial:
        return "partial"
    if denied:
        return "denied"
    if approved:
        return "approved"
    return "unknown"


def normalize_narrative(text):
    kept_lines = []
    for line in text.replace("\f", "\n").splitlines():
        stripped = line.strip()
        if not stripped or re.fullmatch(r"[_\-]{10,}", stripped):
            continue
        if PAGE_HEADER.fullmatch(stripped):
            continue
        kept_lines.append(stripped)
    return re.sub(r"\s+", " ", " ".join(kept_lines)).strip().lower()


def normalized_project_name(value):
    normalized = re.sub(r"[^a-z0-9]+", " ", clean_text(value).lower()).strip()
    return re.sub(r"\bsize\s+\d+(?:\s+\d+)?\s+mb\b", "", normalized).strip()

def as_int(value):
    if value in ("", None):
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def as_float(value):
    try:
        return float(value)
    except ValueError:
        return None


def normalize_whitespace(text):
    return re.sub(r"\s+", " ", text).strip()


def normalize_heading_key(text):
    text = re.sub(r"[^A-Z0-9 ]+", "", text.upper())
    text = re.sub(r"\s+", " ", text).strip()
    return text.replace(" ", "_").lower()


def detect_heading(line):
    stripped = normalize_whitespace(line)
    if not stripped:
        return None, ""

    uppercase_share = 0
    letters = re.findall(r"[A-Za-z]", stripped)
    if letters:
        uppercase_share = sum(letter.isupper() for letter in letters) / len(letters)

    for heading_key, heading_pattern in HEADING_PATTERNS:
        match = re.match(rf"^({heading_pattern})(?:\s*[:\-]\s*)?(.*)$", stripped, re.IGNORECASE)
        if not match:
            continue

        remainder = match.group(2).strip()
        whole_line_key = normalize_heading_key(stripped)
        section = SECTION_LABELS[heading_key]
        if whole_line_key == heading_key or uppercase_share >= 0.75:
            return section, remainder

        if heading_key in {"uniform_land_use_review", "ulurp"} and len(remainder.split()) >= 4:
            return section, remainder

        if heading_key in {
            "community_board_public_hearing",
            "community_board_recommendation",
            "community_board_review",
            "borough_president_recommendation",
            "borough_president_review",
            "borough_president_public_hearing",
            "city_planning_commission_public_hearing",
            "cpc_public_hearing",
        }:
            return section, remainder

    return None, ""


def parse_sections(text):
    parts = defaultdict(list)
    current_section = "unsectioned"

    text = re.sub(r"-\s*\n\s*", "", text)
    for line in text.splitlines():
        section, remainder = detect_heading(line)
        if section is not None:
            current_section = section
            if remainder:
                parts[current_section].append(remainder)
            continue
        stripped = normalize_whitespace(line)
        if re.search(
            r"\b(?:certif\w*|refer\w*)\b.{0,140}\bcommunity(?: planning)?(?: board)?\b|"
            r"^(?:on .{0,60},? )?(?:the )?community(?: planning)? board\b.{0,120}"
            r"\b(?:held|reviewed|voted|recommended|adopted|waived|issued|approved|"
            r"disapproved|opposed|requested|considered)\b",
            stripped,
            re.IGNORECASE,
        ):
            current_section = "community_board"
        elif re.search(
            r"^(?:the )?(?:application|proposal|project)?.{0,80}\bconsidered by "
            r"(?:the )?(?:[A-Za-z]+ )?borough president\b|"
            r"^(?:the )?(?:[A-Za-z]+ )?borough president\b.{0,100}"
            r"\b(?:held|considered|recommended|approved|disapproved|opposed|requested)\b",
            stripped,
            re.IGNORECASE,
        ):
            current_section = "borough_president"
        elif re.search(
            r"^(?:on .{0,80},? )?(?:the )?(?:city planning )?commission\b.{0,140}"
            r"\b(?:scheduled|held)\b.{0,80}\bpublic hearing\b",
            stripped,
            re.IGNORECASE,
        ):
            current_section = "cpc_hearing"
        elif re.search(
            r"^(?:the )?(?:city planning )?commission (?:therefore )?"
            r"(?:believes|considers|finds|has carefully considered|hereby makes)\b",
            stripped,
            re.IGNORECASE,
        ):
            current_section = "consideration_findings"
        parts[current_section].append(line)

    return {
        section: normalize_whitespace("\n".join(parts.get(section, [])))
        for section in SECTION_ORDER
    }


def split_sentences(text):
    text = normalize_whitespace(text)
    if not text:
        return []

    pieces = re.split(r"(?<=[.!?;])\s+(?=[A-Z0-9\"'(\[])|(?:\n\s*){2,}", text)
    sentences = []
    for piece in pieces:
        piece = normalize_whitespace(piece)
        if not piece:
            continue
        if len(piece) > 1200:
            sentences.extend(split_long_sentence(piece))
        else:
            sentences.append(piece)
    return sentences


def split_long_sentence(sentence):
    chunks = []
    words = sentence.split()
    for start in range(0, len(words), 80):
        chunk = " ".join(words[start : start + 80]).strip()
        if chunk:
            chunks.append(chunk)
    return chunks


def word_count(text):
    return len(re.findall(r"[A-Za-z0-9$]+(?:[-'][A-Za-z0-9]+)?", text))


def normalize_sentence_for_boilerplate(sentence):
    text = sentence.lower()
    text = re.sub(rf"\b(?:{MONTH_PATTERN})\s+\d{{1,2}},?\s+\d{{4}}\b", " <date> ", text)
    text = re.sub(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", " <date> ", text)
    text = re.sub(r"\b[cnm]\s?\d{6}\s?[a-z]{2,4}\b", " <appno> ", text)
    text = re.sub(r"\bp\d{4}[a-z]\d{4}\b", " <projectid> ", text)
    text = re.sub(r"\b\d+(?:\.\d+)?\b", " <num> ", text)
    text = re.sub(r"[^a-z0-9<>$ ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_council_filing_boilerplate(sentence):
    lower_sentence = sentence.lower()
    return (
        "197-d" in lower_sentence
        and "council" in lower_sentence
        and ("filed" in lower_sentence or "referred" in lower_sentence)
    )


def is_special_permit_modification_boilerplate(sentence):
    lower_sentence = sentence.lower()
    return (
        "modifications specifically granted" in lower_sentence
        or "except for modifications" in lower_sentence
        or "modifications herein granted" in lower_sentence
        or "special permit modifications" in lower_sentence
        or "modification of use or bulk regulations" in lower_sentence
        or "modifications of use or bulk regulations" in lower_sentence
    )


def sentence_rule_text(document_sentences, index, context_words):
    start = max(0, index - 1)
    end = min(len(document_sentences), index + 2)
    context = " ".join(
        sentence["sentence"] for sentence in document_sentences[start:end]
    )
    context = " ".join(context.split()[:context_words])
    return normalize_whitespace(context)


def parse_number(value):
    value = clean_text(value).lower().replace("-", " ")
    if value.isdigit():
        return int(value)
    if value in NUMBER_WORDS:
        return NUMBER_WORDS[value]
    parts = value.split()
    if len(parts) == 2 and parts[0] in NUMBER_WORDS and parts[1] in NUMBER_WORDS:
        return NUMBER_WORDS[parts[0]] + NUMBER_WORDS[parts[1]]
    return None


def revision_is_positive(text, section):
    if MECHANICAL_REVISION.search(text):
        return False
    if REVISION_OR_CONCESSION.search(text):
        return True
    if CHANGE_ACTION.search(text) and CHANGE_SUBJECT.search(text) and CHANGE_STAGE.search(text):
        return True
    if section in {"cpc_hearing", "consideration_findings"} and (
        COMMITMENT_ACTION.search(text)
        and SUBSTANTIVE_COMMITMENT.search(text)
        and re.search(r"\b(?:applicant|developer|owner|agency|department)\b", text, re.IGNORECASE)
    ):
        return True
    if section in {"cpc_hearing", "consideration_findings"} and (
        re.search(r"\brestrictive declaration\b", text, re.IGNORECASE)
        and (COMMITMENT_ACTION.search(text) or SUBSTANTIVE_COMMITMENT.search(text))
    ):
        return True
    return bool(
        section == "consideration_findings"
        and TRIAL_OR_REAPPLICATION.search(text)
        and CHANGE_SUBJECT.search(text)
    )


def extract_cpc_speaker_counts(text):
    text = normalize_whitespace(text)
    if not text:
        return None, None

    hearing_blocks = re.split(
        r"(?=\b(?:the )?(?:continued )?hearing was duly held\b)",
        text,
        flags=re.IGNORECASE,
    )
    if len(hearing_blocks) > 1:
        hearing_blocks = hearing_blocks[1:]
    else:
        hearing_blocks = [text]

    support_total = 0
    opposition_total = 0
    support_found = False
    opposition_found = False
    pair_patterns = [
        re.compile(
            rf"\b(?:there (?:was|were) )?{NUMBER_TOKEN} (?:speakers?|appearances?)\s*[:,;]?\s*"
            rf"(?P<support>{NUMBER_TOKEN})(?: speakers?|appearances?)?\s+(?:were )?{SUPPORT_TERM}"
            rf".{{0,120}}?(?P<opposition>{NUMBER_TOKEN})(?: (?:speakers?|appearances?))?\s+"
            rf"(?:were )?{OPPOSITION_TERM}",
            re.IGNORECASE,
        ),
        re.compile(
            rf"\b(?P<support>{NUMBER_TOKEN})(?: speakers?|appearances?)?\s+(?:were )?{SUPPORT_TERM}"
            rf".{{0,180}}?\b(?P<opposition>{NUMBER_TOKEN})(?: (?:speakers?|appearances?))?\s+"
            rf"(?:were )?{OPPOSITION_TERM}",
            re.IGNORECASE,
        ),
        re.compile(
            rf"\b(?P<opposition>{NUMBER_TOKEN})(?: speakers?|appearances?)?\s+(?:were )?{OPPOSITION_TERM}"
            rf".{{0,180}}?\b(?P<support>{NUMBER_TOKEN})(?: (?:speakers?|appearances?))?\s+"
            rf"(?:were )?{SUPPORT_TERM}",
            re.IGNORECASE,
        ),
    ]
    single_patterns = {
        "support": re.compile(
            rf"\b(?:there (?:was|were) )?(?P<count>{NUMBER_TOKEN})(?: speakers?|appearances?)?\s+"
            rf"(?:were )?{SUPPORT_TERM}",
            re.IGNORECASE,
        ),
        "opposition": re.compile(
            rf"\b(?:there (?:was|were) )?(?P<count>{NUMBER_TOKEN})(?: speakers?|appearances?)?\s+"
            rf"(?:were )?{OPPOSITION_TERM}",
            re.IGNORECASE,
        ),
    }

    for block in hearing_blocks:
        if NO_APPEARANCES.search(block):
            support_found = True
            opposition_found = True
            continue

        pair_matches = [match for pattern in pair_patterns for match in pattern.finditer(block)]
        pair_match = min(pair_matches, key=lambda match: match.start()) if pair_matches else None
        block_support = None
        block_opposition = None
        if pair_match:
            block_support = parse_number(pair_match.group("support"))
            block_opposition = parse_number(pair_match.group("opposition"))
        else:
            support_match = single_patterns["support"].search(block)
            opposition_match = single_patterns["opposition"].search(block)
            if support_match:
                block_support = parse_number(support_match.group("count"))
            if opposition_match:
                block_opposition = parse_number(opposition_match.group("count"))
            if block_support is None and re.search(
                rf"\b(?:the |a |an )?(?:applicant|owner|attorney|representative|speaker)"
                rf"(?: for the applicant)?.{{0,100}}?{SUPPORT_TERM}",
                block,
                re.IGNORECASE,
            ):
                block_support = 1
            if block_opposition is None and re.search(
                rf"\b(?:the |a |an )?(?:applicant|owner|attorney|representative|speaker)"
                rf".{{0,100}}?{OPPOSITION_TERM}",
                block,
                re.IGNORECASE,
            ):
                block_opposition = 1

        if block_support is None and re.search(
            rf"\b(?:sole|only) (?:appearance|speaker).{{0,100}}?{SUPPORT_TERM}|"
            rf"\b(?:applicant|owner|attorney).{{0,80}}?appeared.{{0,40}}?{SUPPORT_TERM}",
            block,
            re.IGNORECASE,
        ):
            block_support = 1

        if block_support is None and re.search(
            r"\b(?:no|none|zero) (?:speakers? )?(?:in favor|in support)\b",
            block,
            re.IGNORECASE,
        ):
            block_support = 0
        if block_opposition is None and re.search(
            r"\b(?:no|none|zero) (?:speakers? )?(?:in opposition|opposed)\b",
            block,
            re.IGNORECASE,
        ):
            block_opposition = 0
        if NO_OTHER_SPEAKERS.search(block):
            if block_support is not None and block_opposition is None:
                block_opposition = 0
            if block_opposition is not None and block_support is None:
                block_support = 0
        if re.search(r"\bthe hearing was closed\b", block, re.IGNORECASE):
            if block_support is not None and block_opposition is None:
                block_opposition = 0
            if block_opposition is not None and block_support is None:
                block_support = 0

        if block_support is not None:
            support_total += block_support
            support_found = True
        if block_opposition is not None:
            opposition_total += block_opposition
            opposition_found = True

    return support_total if support_found else None, opposition_total if opposition_found else None


def extract_cb_vote_counts(text):
    text = normalize_whitespace(text)
    if not text:
        return None, None

    vote_patterns = [
        re.compile(
            rf"\b(?:by a vote of |the vote was |voted )?(?P<support>{NUMBER_TOKEN})"
            rf"(?: members?)?(?: voting)? (?:in favor|for|supporting).{{0,80}}?"
            rf"(?P<opposition>{NUMBER_TOKEN})(?: members?)?(?: voting)? "
            rf"(?:against|opposed|in opposition)",
            re.IGNORECASE,
        ),
        re.compile(
            rf"\b(?P<opposition>{NUMBER_TOKEN})(?: members?)? (?:voting )?"
            rf"(?:against|opposed|in opposition).{{0,80}}?(?P<support>{NUMBER_TOKEN})"
            rf"(?: members?)? (?:voting )?(?:in favor|for|supporting)",
            re.IGNORECASE,
        ),
        re.compile(
            rf"#?\s*in favor\s*:?\s*(?P<support>{NUMBER_TOKEN}).{{0,80}}?"
            rf"#?\s*(?:against|opposed)\s*:?\s*(?P<opposition>{NUMBER_TOKEN})",
            re.IGNORECASE,
        ),
        re.compile(
            rf"\b(?:by (?:a )?(?:vote of )?|vote(?:d| was)?|voting)\s*"
            rf"(?P<first>{NUMBER_TOKEN})\s*(?:to|-|/)\s*(?P<second>{NUMBER_TOKEN})\b",
            re.IGNORECASE,
        ),
    ]
    stance_pattern = re.compile(
        r"\b(?:recommend\w* |resolution (?:recommending )?|motion to )?"
        r"(?P<stance>disapprov\w*|unfavorable|denial|reject\w*|approv\w*|favorable)\b",
        re.IGNORECASE,
    )

    vote_matches = sorted(
        (match for pattern in vote_patterns for match in pattern.finditer(text)),
        key=lambda match: (match.start(), -(match.end() - match.start())),
    )
    accepted_matches = []
    for match in vote_matches:
        if any(
            match.start() < prior.end() and prior.start() < match.end()
            for prior in accepted_matches
        ):
            continue
        accepted_matches.append(match)

    support_total = 0
    opposition_total = 0
    found = False
    for match in accepted_matches:
        groups = match.groupdict()
        if "support" in groups:
            support = parse_number(groups["support"])
            opposition = parse_number(groups["opposition"])
        else:
            support = parse_number(groups["first"])
            opposition = parse_number(groups["second"])

        context_start = max(0, match.start() - 220)
        context_end = min(len(text), match.end() + 260)
        context = text[context_start:context_end]
        stances = list(stance_pattern.finditer(context))
        if stances:
            match_center = match.start() - context_start
            stance = min(
                stances,
                key=lambda stance_match: abs(stance_match.start() - match_center),
            ).group("stance").lower()
            if stance.startswith(("disapprov", "reject")) or stance in {"unfavorable", "denial"}:
                support, opposition = opposition, support

        support_total += support
        opposition_total += opposition
        found = True

    if found:
        return support_total, opposition_total

    support_only = list(
        re.finditer(
            rf"\b(?P<support>{NUMBER_TOKEN})(?: board members?)? voting in favor\b",
            text,
            re.IGNORECASE,
        )
    )
    if support_only and re.search(r"\bunanimous\w*\b", text, re.IGNORECASE):
        return sum(parse_number(match.group("support")) for match in support_only), 0

    if re.search(r"\bunanimously (?:approved|recommended approval)\b", text, re.IGNORECASE):
        return None, 0
    if re.search(r"\bunanimously (?:disapproved|recommended disapproval)\b", text, re.IGNORECASE):
        return 0, None
    return None, None


def actor_position(contexts, actor_pattern):
    actor_contexts = [context for context in contexts if actor_pattern.search(context)]
    if actor_pattern is COUNCIL_ACTOR:
        actor_contexts = [context for context in actor_contexts if not COUNCIL_PROCEDURE.search(context)]
    if any(OPPOSITION.search(context) and not NO_OPPOSITION.search(context) for context in actor_contexts):
        return "opposition"
    if any(SUPPORT_POSITION.search(context) or REQUEST_POSITION.search(context) for context in actor_contexts):
        return "support_or_request"
    return "none_or_procedural"


def issue_is_positive(issue_name, events):
    for event in events:
        if issue_name not in event["issues"]:
            continue
        if event["section"] in {"community_board", "borough_president", "cpc_hearing"}:
            if event["review_action"] and (event["actors"] or event["linked"]):
                return True
        elif event["section"] == "consideration_findings":
            if event["explicit_response"] or event["unresolved"]:
                return True
            if event["review_action"]:
                return True
    return False


if len(sys.argv) != 5:
    raise SystemExit(
        "Usage: build_ulurp_cpc_text_labels.py "
        "START_YEAR END_YEAR BOILERPLATE_DOC_SHARE RULE_CONTEXT_WORDS"
    )

start_year = as_int(sys.argv[1])
end_year = as_int(sys.argv[2])
boilerplate_doc_share = as_float(sys.argv[3])
rule_context_words = as_int(sys.argv[4])

if start_year is None or end_year is None:
    raise SystemExit("START_YEAR and END_YEAR must be integers.")
if end_year < start_year:
    raise SystemExit("END_YEAR must be greater than or equal to START_YEAR.")
if boilerplate_doc_share is None or not 0 < boilerplate_doc_share < 1:
    raise SystemExit("BOILERPLATE_DOC_SHARE must be between 0 and 1.")
if rule_context_words is None or rule_context_words < 1:
    raise SystemExit("RULE_CONTEXT_WORDS must be a positive integer.")

corpus_manifest_real_path = Path("../input/ulurp_cpc_report_manifest.csv").resolve()
with Path("../input/ulurp_cpc_report_manifest.csv").open(
    newline="", encoding="utf-8"
) as input_file:
    source_rows = [
        row
        for row in csv.DictReader(input_file)
        if start_year <= int(row["official_vote_year"]) <= end_year
    ]

with Path("../input/ulurp_cpc_narrative_boundary_exceptions.csv").open(
    newline="", encoding="utf-8"
) as input_file:
    boundary_exception_rows = list(csv.DictReader(input_file))
boundary_exceptions = {
    row["application_number"]: row for row in boundary_exception_rows
}
if len(boundary_exceptions) != len(boundary_exception_rows):
    raise RuntimeError("Narrative-boundary exceptions are not unique by application number.")
if len(source_rows) != len({row["document_id"] for row in source_rows}):
    raise RuntimeError("Official corpus manifest is not unique by document_id.")

candidate_rows = []
applied_boundary_exceptions = set()
for source_row in source_rows:
    text_path = resolve_task_path(
        source_row["local_text_path"], corpus_manifest_real_path
    )
    source_usable = source_row["source_usable"] == "TRUE"
    if source_usable and (
        source_row["text_status"] != "text_extracted"
        or text_path is None
        or not text_path.is_file()
    ):
        raise RuntimeError(f"Missing readable text for {source_row['application_number']}.")

    if source_usable:
        text_stat = text_path.stat()
        if text_stat.st_size < 100 or (
            getattr(text_stat, "st_blocks", 1) == 0 and text_stat.st_size > 0
        ):
            raise RuntimeError(
                f"Unreadable text for analysis narrative {source_row['application_number']}."
            )
        full_text = text_path.read_text(encoding="utf-8", errors="replace")
        source_text_hash = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
        boundary_exception = boundary_exceptions.get(
            source_row["application_number"], {}
        )
        if boundary_exception:
            if boundary_exception["source_text_sha256"] != source_text_hash:
                raise RuntimeError(
                    f"Stale narrative-boundary decision for {source_row['application_number']}."
                )
            applied_boundary_exceptions.add(source_row["application_number"])
            boundary_method = boundary_exception["boundary_decision"]
            if boundary_method in MANUAL_EXCLUSION_METHODS:
                narrative_end = 0
            else:
                narrative_end = int(boundary_exception["narrative_end_char"])
                if not 0 < narrative_end <= len(full_text):
                    raise RuntimeError(
                        f"Invalid manual narrative boundary for {source_row['application_number']}."
                    )
            narrative_text = full_text[:narrative_end]
        else:
            narrative_end, boundary_method = narrative_boundary(full_text)
            narrative_text = full_text[:narrative_end]
        disposition = cpc_disposition(full_text, narrative_end)
        cb_attachment_flag = bool(
            re.search(
                r"COMMUNITY/?BOROUGH BOARD RECOMMENDATION|"
                r"Please attach any further explanation of the recommendation",
                full_text,
                re.IGNORECASE,
            )
        )
        normalized_text = normalize_narrative(narrative_text)
        narrative_word_count = len(re.findall(r"\b[\w'-]+\b", normalized_text))
        narrative_hash = hashlib.sha256(normalized_text.encode("utf-8")).hexdigest()
    else:
        boundary_exception = {}
        source_text_hash = ""
        boundary_method = "documented_source_unavailable"
        narrative_word_count = 0
        narrative_hash = ""
        narrative_text = ""
        disposition = "unknown"
        text_path = None
        cb_attachment_flag = False

    project_name_key = normalized_project_name(source_row["official_project_name"])
    lead_group_key = (
        f"{source_row['official_vote_date']}|{project_name_key}"
        if source_usable and project_name_key
        else ""
    )
    candidate_rows.append(
        {
            "document_id": source_row["document_id"],
            "application_number": source_row["application_number"],
            "action_code": source_row["action_code"],
            "corpus_role": source_row["corpus_role"],
            "project_name": source_row["official_project_name"],
            "community_district": source_row["official_community_district"],
            "year": int(source_row["official_vote_year"]),
            "zap_project_ids": source_row["zap_project_ids"],
            "official_vote_date": source_row["official_vote_date"],
            "official_lead_report_flag": source_row["official_lead_report_flag"],
            "source_text_sha256": source_text_hash,
            "narrative_boundary_method": boundary_method,
            "narrative_word_count": narrative_word_count,
            "narrative_sha256": narrative_hash,
            "lead_group_key": lead_group_key,
            "analysis_non_pp_flag": str(source_row["action_code"] != "PP").upper(),
            "analysis_zm_zr_zs_flag": str(
                source_row["action_code"] in {"ZM", "ZR", "ZS"}
            ).upper(),
            "cpc_disposition": disposition,
            "source_text_path": str(text_path) if text_path is not None else "",
            "cb_attachment_flag": cb_attachment_flag,
            "manual_companion_application": boundary_exception.get(
                "analysis_narrative_representative_application", ""
            ),
            "text": narrative_text,
        }
    )

unapplied_boundary_exceptions = set(boundary_exceptions) - applied_boundary_exceptions
if unapplied_boundary_exceptions:
    raise RuntimeError(
        "Unapplied narrative-boundary exceptions: "
        + "; ".join(sorted(unapplied_boundary_exceptions))
    )

lead_groups = defaultdict(list)
for row in candidate_rows:
    if row["lead_group_key"]:
        lead_groups[row["lead_group_key"]].append(row)

related_to_lead = set()
for group_rows in lead_groups.values():
    lead_rows = [row for row in group_rows if row["official_lead_report_flag"] == "TRUE"]
    if len(group_rows) > 1 and lead_rows:
        certified_group_rows = [
            row for row in group_rows if row["corpus_role"] == "certified_ulurp_report"
        ]
        group_non_pp_flag = any(row["action_code"] != "PP" for row in certified_group_rows)
        group_zm_zr_zs_flag = any(
            row["action_code"] in {"ZM", "ZR", "ZS"}
            for row in certified_group_rows
        )
        for row in group_rows:
            row["analysis_non_pp_flag"] = str(group_non_pp_flag).upper()
            row["analysis_zm_zr_zs_flag"] = str(group_zm_zr_zs_flag).upper()
            if row["official_lead_report_flag"] != "TRUE":
                related_to_lead.add(row["document_id"])

for row in candidate_rows:
    if row["manual_companion_application"]:
        related_to_lead.add(row["document_id"])

rows_by_application = {
    application_key(row["application_number"]): row for row in candidate_rows
}
if len(rows_by_application) != len(candidate_rows):
    raise RuntimeError("Analysis source rows are not unique by application number.")

companion_neighbors = defaultdict(set)
for row in candidate_rows:
    for match in APPLICATION_REFERENCE.finditer(row["text"]):
        companion = rows_by_application.get(application_key(match.group(0)))
        if (
            companion is None
            or companion["document_id"] == row["document_id"]
            or companion["official_vote_date"] != row["official_vote_date"]
        ):
            continue
        companion_neighbors[row["document_id"]].add(companion["document_id"])
        companion_neighbors[companion["document_id"]].add(row["document_id"])

rows_by_zap_project = defaultdict(list)
for row in candidate_rows:
    for project_id in row["zap_project_ids"].split("; "):
        if project_id:
            rows_by_zap_project[(row["official_vote_date"], project_id)].append(row)
for group_rows in rows_by_zap_project.values():
    for row in group_rows[1:]:
        companion_neighbors[group_rows[0]["document_id"]].add(row["document_id"])
        companion_neighbors[row["document_id"]].add(group_rows[0]["document_id"])

for group_rows in lead_groups.values():
    for row in group_rows[1:]:
        companion_neighbors[group_rows[0]["document_id"]].add(row["document_id"])
        companion_neighbors[row["document_id"]].add(group_rows[0]["document_id"])

rows_by_narrative = defaultdict(list)
for row in candidate_rows:
    if row["narrative_sha256"]:
        rows_by_narrative[row["narrative_sha256"]].append(row)
for group_rows in rows_by_narrative.values():
    for row in group_rows[1:]:
        companion_neighbors[group_rows[0]["document_id"]].add(row["document_id"])
        companion_neighbors[row["document_id"]].add(group_rows[0]["document_id"])

for row in candidate_rows:
    companion = rows_by_application.get(
        application_key(row["manual_companion_application"])
    )
    if companion is not None:
        companion_neighbors[row["document_id"]].add(companion["document_id"])
        companion_neighbors[companion["document_id"]].add(row["document_id"])

rows_by_document_id = {row["document_id"]: row for row in candidate_rows}
companion_components = {}
unassigned_document_ids = set(rows_by_document_id)
while unassigned_document_ids:
    first_document_id = min(unassigned_document_ids)
    component = {first_document_id}
    pending_document_ids = [first_document_id]
    while pending_document_ids:
        document_id = pending_document_ids.pop()
        new_document_ids = companion_neighbors[document_id] - component
        component.update(new_document_ids)
        pending_document_ids.extend(new_document_ids)
    for document_id in component:
        companion_components[document_id] = component
    unassigned_document_ids -= component

eligible_rows = [
    row
    for row in candidate_rows
    if row["document_id"] not in related_to_lead
    and row["narrative_boundary_method"] != "full_text_no_boundary_found"
    and row["narrative_boundary_method"] not in MANUAL_EXCLUSION_METHODS
    and row["narrative_word_count"] >= 100
]
exact_groups = defaultdict(list)
for row in eligible_rows:
    exact_groups[row["narrative_sha256"]].append(row)

documents = []
for group_rows in exact_groups.values():
    group_rows.sort(
        key=lambda row: (
            row["official_lead_report_flag"] != "TRUE",
            row["application_number"],
        )
    )
    document = group_rows[0]
    component_rows = [
        rows_by_document_id[document_id]
        for document_id in companion_components[document["document_id"]]
    ]
    component_rows.sort(
        key=lambda row: (
            row["document_id"] != document["document_id"],
            row["official_lead_report_flag"] != "TRUE",
            row["application_number"],
        )
    )
    analysis_rows = []
    included_narratives = set()
    for row in component_rows:
        if (
            row["narrative_boundary_method"] == "full_text_no_boundary_found"
            or row["narrative_boundary_method"] in MANUAL_EXCLUSION_METHODS
            or row["narrative_word_count"] < 100
            or row["narrative_sha256"] in included_narratives
        ):
            continue
        analysis_rows.append(row)
        included_narratives.add(row["narrative_sha256"])
    document["analysis_text"] = "\n\n".join(row["text"] for row in analysis_rows)
    document["analysis_text_sha256"] = hashlib.sha256(
        normalize_narrative(document["analysis_text"]).encode("utf-8")
    ).hexdigest()
    document["analysis_word_count"] = word_count(document["analysis_text"])
    document["companion_application_numbers"] = "; ".join(
        row["application_number"]
        for row in analysis_rows
        if row["document_id"] != document["document_id"]
    )
    document["decade"] = f"{document['year'] // 10 * 10}s"
    documents.append(document)

print(
    f"Built {len(documents)} analysis narratives from {len(candidate_rows)} report rows."
)

if len(documents) != len({row["document_id"] for row in documents}):
    raise RuntimeError("Analysis narratives are not unique by document_id.")

sentence_rows = []
sentence_doc_ids = defaultdict(set)
document_section_sentences = defaultdict(list)
for document in documents:
    for section, section_text in parse_sections(document["analysis_text"]).items():
        for sentence in split_sentences(section_text):
            words = word_count(sentence)
            if words == 0:
                continue
            sentence_position = len(
                document_section_sentences[(document["document_id"], section)]
            )
            normalized_sentence = normalize_sentence_for_boilerplate(sentence)
            row = {
                "document_id": document["document_id"],
                "section": section,
                "sentence_position": sentence_position,
                "sentence": sentence,
                "normalized_sentence": normalized_sentence,
            }
            sentence_rows.append(row)
            document_section_sentences[
                (document["document_id"], section)
            ].append(row)

            if words >= 6 and normalized_sentence:
                sentence_doc_ids[normalized_sentence].add(document["document_id"])

minimum_boilerplate_documents = max(
    2,
    math.floor(len(documents) * boilerplate_doc_share) + 1,
)
boilerplate_sentences = {
    normalized_sentence
    for normalized_sentence, document_ids in sentence_doc_ids.items()
    if len(document_ids) >= minimum_boilerplate_documents
}

document_measurements = {}
for document in documents:
    document_id = document["document_id"]
    context_rows = []
    section_contexts = defaultdict(list)

    for section in SECTION_ORDER:
        section_rows = document_section_sentences[(document_id, section)]
        for row in section_rows:
            if (
                row["normalized_sentence"] in boilerplate_sentences
                or is_council_filing_boilerplate(row["sentence"])
                or is_special_permit_modification_boilerplate(row["sentence"])
            ):
                continue
            context = sentence_rule_text(
                section_rows,
                row["sentence_position"],
                rule_context_words,
            )
            context_row = {
                "section": section,
                "sentence_position": row["sentence_position"],
                "sentence": row["sentence"],
                "context": context,
            }
            context_rows.append(context_row)
            section_contexts[section].append(context)

    event_units = [
        {"section": row["section"], "text": row["sentence"], "linked": False}
        for row in context_rows
    ]
    for section in SECTION_ORDER:
        section_event_rows = [
            row for row in context_rows if row["section"] == section
        ]
        for first_row, second_row in zip(section_event_rows, section_event_rows[1:]):
            if (
                second_row["sentence_position"] == first_row["sentence_position"] + 1
                and ADJACENT_EVENT_LINK.search(second_row["sentence"])
            ):
                event_units.append(
                    {
                        "section": section,
                        "text": first_row["sentence"] + " " + second_row["sentence"],
                        "linked": True,
                    }
                )

    events = []
    for unit in event_units:
        sentence = unit["text"]
        actors = set()
        if COMMUNITY_BOARD_ACTOR.search(sentence):
            actors.add("community_board")
        if BOROUGH_PRESIDENT_ACTOR.search(sentence):
            actors.add("borough_president")
        if COUNCIL_ACTOR.search(sentence) and not COUNCIL_PROCEDURE.search(sentence):
            actors.add("councilmember")
        if CIVIC_ACTOR.search(sentence):
            actors.add("civic_group")
        if OTHER_LOCAL_ACTOR.search(sentence) and not NONLOCAL_ACTOR.search(sentence):
            actors.add("other_local")
        if unit["section"] == "community_board":
            actors.add("community_board")
        elif unit["section"] == "borough_president":
            actors.add("borough_president")

        opposition = bool(OPPOSITION.search(sentence) and not NO_OPPOSITION.search(sentence))
        support = bool(SUPPORT_POSITION.search(sentence) and not opposition)
        request = bool(
            SUBSTANTIVE_REQUEST.search(sentence)
            and not MINOR_OR_PROCEDURAL_REQUEST.search(sentence)
        )
        issues = {
            field
            for field, issue_pattern in ISSUE_PATTERNS.items()
            if issue_pattern.search(sentence)
        }
        review_action = bool(REVIEW_ACTION.search(sentence))
        response_action = bool(RESPONSE_ACTION.search(sentence))
        explicit_response = bool(
            response_action
            and (
                EXPLICIT_RESPONSE_LINK.search(sentence)
                or LOCAL_RESPONSE_REFERENCE.search(sentence)
                or (
                    unit["section"] == "consideration_findings"
                    and review_action
                    and issues
                )
            )
        )
        unresolved = bool(UNRESOLVED_RESPONSE.search(sentence))
        revision = revision_is_positive(sentence, unit["section"])
        procedural = bool(
            PROCEDURAL_RESPONSE.search(sentence)
            and (
                explicit_response
                or re.search(
                    r"\b(?:applicant|agency|department|commission)\b.{0,120}"
                    r"\b(?:agreed|committed|will|shall|required|directed|referred)\b",
                    sentence,
                    re.IGNORECASE,
                )
                or unit["section"] == "consideration_findings"
            )
        )
        if actors or revision or procedural or unresolved or (issues and review_action):
            events.append(
                {
                    "section": unit["section"],
                    "text": sentence,
                    "actors": actors,
                    "opposition": opposition,
                    "support": support,
                    "request": request,
                    "revision": revision,
                    "procedural": procedural,
                    "review_action": review_action,
                    "response_action": response_action,
                    "explicit_response": explicit_response,
                    "unresolved": unresolved,
                    "issues": issues,
                    "linked": unit["linked"],
                }
            )

    bundled_community_board_text = " ".join(
        row["sentence"]
        for row in document_section_sentences[(document_id, "community_board")]
    )
    bundled_cpc_hearing_text = " ".join(
        row["sentence"]
        for row in document_section_sentences[(document_id, "cpc_hearing")]
    )
    primary_sections = parse_sections(document["text"])
    cpc_support_speakers, cpc_opposition_speakers = extract_cpc_speaker_counts(
        primary_sections["cpc_hearing"]
    )
    bundled_support_speakers, bundled_opposition_speakers = extract_cpc_speaker_counts(
        bundled_cpc_hearing_text
    )
    if cpc_support_speakers is None:
        cpc_support_speakers = bundled_support_speakers
    if cpc_opposition_speakers is None:
        cpc_opposition_speakers = bundled_opposition_speakers
    if cpc_support_speakers is None and cpc_opposition_speakers is None:
        fallback_match = re.search(
            r"(?is)\b(?:city planning commission|the commission)\b.{0,120}"
            r"scheduled.{0,120}(?:public )?hearing(?P<hearing_text>.*)$",
            document["text"],
        )
        if fallback_match:
            cpc_support_speakers, cpc_opposition_speakers = extract_cpc_speaker_counts(
                fallback_match.group("hearing_text")
            )
    cb_support_votes, cb_opposition_votes = extract_cb_vote_counts(
        primary_sections["community_board"]
    )
    bundled_cb_support_votes, bundled_cb_opposition_votes = extract_cb_vote_counts(
        bundled_community_board_text
    )
    if cb_support_votes is None:
        cb_support_votes = bundled_cb_support_votes
    if cb_opposition_votes is None:
        cb_opposition_votes = bundled_cb_opposition_votes
    if cb_support_votes is None and cb_opposition_votes is None:
        cpc_match = re.search(
            r"(?is)\b(?:city planning commission|the commission)\b.{0,120}"
            r"scheduled.{0,120}(?:public )?hearing",
            document["text"],
        )
        before_cpc = document["text"][: cpc_match.start()] if cpc_match else document["text"]
        board_matches = list(
            re.finditer(
                r"(?is)\bcommunity board\b.{0,160}\b(?:held|voted|adopted|recommended|approved|disapproved)\b",
                before_cpc,
            )
        )
        if board_matches:
            board_start = board_matches[0].start()
            cb_support_votes, cb_opposition_votes = extract_cb_vote_counts(
                before_cpc[board_start:]
            )
    if (
        cb_support_votes is None
        and cb_opposition_votes is None
        and document["cb_attachment_flag"]
    ):
        source_cb_support_votes, source_cb_opposition_votes = extract_cb_vote_counts(
            Path(document["source_text_path"]).read_text(
                encoding="utf-8",
                errors="replace",
            )
        )
        cb_support_votes = source_cb_support_votes
        cb_opposition_votes = source_cb_opposition_votes

    local_actor_names = {
        "community_board",
        "borough_president",
        "councilmember",
        "civic_group",
        "other_local",
    }
    review_contexts = [
        row["context"]
        for row in context_rows
        if row["section"]
        in {
            "community_board",
            "borough_president",
            "cpc_hearing",
            "consideration_findings",
        }
    ]
    single_events = [event for event in events if not event["linked"]]
    cb_events = [event for event in single_events if "community_board" in event["actors"]]
    council_events = [event for event in single_events if "councilmember" in event["actors"]]
    local_events = [
        event for event in single_events if event["actors"] & local_actor_names
    ]
    local_issues = set().union(
        *(
            event["issues"]
            for event in local_events
            if event["request"] or event["opposition"]
        )
    ) if any(event["request"] or event["opposition"] for event in local_events) else set()

    cb_request = any(event["request"] or event["opposition"] for event in cb_events)
    bp_request = any(
        SUBSTANTIVE_REQUEST.search(context)
        and not MINOR_OR_PROCEDURAL_REQUEST.search(context)
        for context in section_contexts["borough_president"]
    )
    if any(event["opposition"] for event in council_events):
        council_position = "opposition"
    elif any(event["support"] or event["request"] for event in council_events):
        council_position = "support_or_request"
    else:
        council_position = "none_or_procedural"
    civic_position = actor_position(review_contexts, CIVIC_ACTOR)

    revision_or_concession = any(event["revision"] for event in events)
    explicit_local_response = any(
        event["explicit_response"]
        and (
            event["actors"] & local_actor_names
            or bool(event["issues"] & local_issues)
        )
        for event in events
        if event["section"] in {
            "background",
            "community_board",
            "borough_president",
            "cpc_hearing",
            "consideration_findings",
        }
    )
    procedural_response = any(event["procedural"] for event in events)

    if cb_support_votes is not None and cb_opposition_votes is not None:
        cb_opposition = cb_opposition_votes > cb_support_votes
    else:
        cb_opposition = any(
            re.search(
                r"\b(?:recommend\w* disapproval|disapprov\w* (?:the )?(?:application|proposal|project)|"
                r"unfavorable recommendation|opposed (?:the )?(?:application|proposal|project))\b",
                context,
                re.IGNORECASE,
            )
            for context in section_contexts["community_board"]
        )
    bp_opposition = any(
        re.search(
            r"\b(?:recommend\w* disapproval|disapprov\w* (?:the )?(?:application|proposal|project)|"
            r"unfavorable recommendation|opposed (?:the )?(?:application|proposal|project))\b",
            context,
            re.IGNORECASE,
        )
        for context in section_contexts["borough_president"]
    )
    substantial_local_opposition = (
        cb_opposition
        or bp_opposition
        or council_position == "opposition"
        or civic_position == "opposition"
        or (
            cpc_opposition_speakers is not None
            and cpc_opposition_speakers >= 10
        )
    )
    local_request_condition = (
        cb_request
        or bp_request
        or any(event["request"] or event["opposition"] for event in local_events)
    )
    approved_unresolved_objection = (
        document["cpc_disposition"] in {"approved", "partial"}
        and substantial_local_opposition
    )

    measurements = {
        "substantial_local_opposition": int(substantial_local_opposition),
        "local_request_condition": int(local_request_condition),
        "revision_or_concession": int(revision_or_concession),
        "procedural_response": int(procedural_response),
        "explicit_local_response": int(explicit_local_response),
        "approved_unresolved_objection": int(approved_unresolved_objection),
        "cb_opposition": int(cb_opposition),
        "cb_request_or_opposition": int(cb_request or cb_opposition),
        "bp_request_or_opposition": int(bp_request or bp_opposition),
        "councilmember_position": council_position,
        "civic_group_position": civic_position,
        "cpc_support_speakers": cpc_support_speakers,
        "cpc_opposition_speakers": cpc_opposition_speakers,
        "cb_support_votes": cb_support_votes,
        "cb_opposition_votes": cb_opposition_votes,
        "restrictive_declaration": int(
            any(re.search(r"\brestrictive declaration\b", context, re.IGNORECASE) for context in review_contexts)
        ),
        "points_of_agreement": int(
            any(re.search(r"\bpoints of agreement\b", context, re.IGNORECASE) for context in review_contexts)
        ),
    }
    for field in ISSUE_PATTERNS:
        measurements[field] = int(issue_is_positive(field, events))

    document_measurements[document_id] = measurements

fieldnames = [
    "document_id",
    "application_number",
    "action_code",
    "project_name",
    "community_district",
    "year",
    "decade",
    "source_text_sha256",
    "narrative_sha256",
    "narrative_word_count",
    "analysis_text_sha256",
    "analysis_word_count",
    "companion_application_numbers",
    "narrative_boundary_method",
    "zap_project_ids",
    "analysis_non_pp_flag",
    "analysis_zm_zr_zs_flag",
    "cpc_disposition",
    *BINARY_SIGNAL_FIELDS,
    *POSITION_FIELDS,
    *COUNT_FIELDS,
]
with Path("../output/ulurp_cpc_text_labels.csv").open(
    "w",
    newline="",
    encoding="utf-8",
) as output_file:
    writer = csv.DictWriter(
        output_file,
        fieldnames=fieldnames,
        lineterminator="\n",
    )
    writer.writeheader()
    for document in sorted(
        documents,
        key=lambda row: (row["year"], row["document_id"]),
    ):
        writer.writerow(
            {
                "document_id": document["document_id"],
                "application_number": document["application_number"],
                "action_code": document["action_code"],
                "project_name": document["project_name"],
                "community_district": document["community_district"],
                "year": document["year"],
                "decade": document["decade"],
                "source_text_sha256": document["source_text_sha256"],
                "narrative_sha256": document["narrative_sha256"],
                "narrative_word_count": document["narrative_word_count"],
                "analysis_text_sha256": document["analysis_text_sha256"],
                "analysis_word_count": document["analysis_word_count"],
                "companion_application_numbers": document[
                    "companion_application_numbers"
                ],
                "narrative_boundary_method": document[
                    "narrative_boundary_method"
                ],
                "zap_project_ids": document["zap_project_ids"],
                "analysis_non_pp_flag": document["analysis_non_pp_flag"],
                "analysis_zm_zr_zs_flag": document[
                    "analysis_zm_zr_zs_flag"
                ],
                "cpc_disposition": document["cpc_disposition"],
                **document_measurements[document["document_id"]],
            }
        )

print(f"Wrote deterministic text labels for {len(documents)} CPC narratives.")
