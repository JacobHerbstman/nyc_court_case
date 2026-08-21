#!/usr/bin/env python3

import csv
import hashlib
import math
import re
import sys
from collections import defaultdict
from pathlib import Path

RESOLUTION_HEADING = re.compile(
    r"(?im)^[ \t\f]*RESOLVED(?:[ \t]*,|[ \t]+BY\b|[ \t]+THAT\b).*$"
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
    "borough_president": "borough_president",
    "borough_president_recommendation": "borough_president",
    "borough_president_review": "borough_president",
    "borough_president_public_hearing": "borough_president",
    "city_planning_commission_public_hearing": "cpc_hearing",
    "cpc_public_hearing": "cpc_hearing",
    "public_hearing": "cpc_hearing",
    "consideration": "consideration_findings",
    "consideration_by_the_city_planning_commission": "consideration_findings",
    "findings": "consideration_findings",
    "commission_findings": "consideration_findings",
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
    ("community_board", r"COMMUNITY BOARD"),
    ("borough_president_recommendation", r"BOROUGH PRESIDENT(?:'S)? RECOMMENDATION"),
    ("borough_president_review", r"BOROUGH PRESIDENT(?:'S)? REVIEW"),
    ("borough_president_public_hearing", r"BOROUGH PRESIDENT(?:'S)? PUBLIC HEARING"),
    ("borough_president", r"BOROUGH PRESIDENT"),
    ("city_planning_commission_public_hearing", r"CITY PLANNING COMMISSION PUBLIC HEARING"),
    ("cpc_public_hearing", r"CPC PUBLIC HEARING"),
    ("public_hearing", r"PUBLIC HEARING"),
    (
        "consideration_by_the_city_planning_commission",
        r"CONSIDERATION BY THE CITY PLANNING COMMISSION",
    ),
    ("consideration", r"CONSIDERATION"),
    ("commission_findings", r"COMMISSION FINDINGS"),
    ("findings", r"FINDINGS"),
    ("resolution", r"RESOLUTION"),
    ("resolved", r"RESOLVED"),
]

MONTH_PATTERN = (
    r"january|february|march|april|may|june|july|august|september|"
    r"october|november|december"
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
    r"recommend\w* disapproval|disapprov\w*)\b",
    re.IGNORECASE,
)
MINOR_OR_PROCEDURAL_REQUEST = re.compile(
    r"\b(?:referred to|for information and review|waived? (?:its )?public hearing|"
    r"recommendation to follow|attach additional sheets?|administrative correction)\b",
    re.IGNORECASE,
)
REVISION_OR_CONCESSION = re.compile(
    r"\b(?:in response to (?:the )?(?:concerns?|comments?|requests?|objections?)|"
    r"at the request of|as requested by|after (?:the )?(?:public hearing|meeting)|"
    r"subsequent to (?:the )?(?:public hearing|community board review)|"
    r"applicant\w* .{0,100}(?:agreed|committed|revised|modified|changed|reduced|"
    r"eliminated|withdrew|scaled back)|(?:proposal|application|plans?|design) (?:was|were|has been) "
    r"(?:revised|modified|changed|reduced|amended)|agreed to (?:provide|fund|construct|"
    r"maintain|limit|reduce|remove|retain)|committed to (?:provide|fund|construct|"
    r"maintain|limit|reduce|remove|retain))\b",
    re.IGNORECASE,
)
MECHANICAL_REVISION = re.compile(
    r"\b(?:modifications specifically granted|except for (?:the )?modifications|"
    r"modification of (?:use|bulk|height and setback) regulations|last date revised|"
    r"zoning resolution,? as amended|amended urban renewal plan|revised negative declaration|"
    r"revised environmental assessment statement|application (?:requests?|seeks?) (?:a )?modification)\b",
    re.IGNORECASE,
)
PROCEDURAL_RESPONSE = re.compile(
    r"\b(?:study|task force|working group|monitor\w*|report\w*|outreach|consult\w*|"
    r"future meeting\w*|continued coordination|advisory committee)\b",
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
    r"task force|working group|advisory committee)\b",
    re.IGNORECASE,
)
UNRESOLVED_RESPONSE = re.compile(
    r"\b(?:the commission (?:does|did) not (?:agree|believe|find|support)|"
    r"the commission (?:disagrees|declines|rejects)|not warranted|not appropriate|"
    r"not persuaded|cannot support|would not be appropriate|nevertheless|nonetheless|"
    r"despite (?:the )?(?:opposition|objection|disapproval|concerns?))\b",
    re.IGNORECASE,
)

COUNCIL_ACTOR = re.compile(r"\b(?:council ?member|councilmember)\b", re.IGNORECASE)
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
    for pattern, method in (
        (RESOLUTION_HEADING, "resolution_heading"),
        (FILING_PARAGRAPH, "filing_paragraph"),
        (ADOPTED_RESOLUTION, "adopted_resolution_paragraph"),
        (COMMISSION_SIGNATURE, "commission_signature"),
    ):
        matches = [match for match in pattern.finditer(text) if match.start() > anchor]
        if matches:
            return matches[0].start(), method
    return len(text), "full_text_no_boundary_found"


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
            rf"\b(?:there (?:was|were) )?{NUMBER_TOKEN} speakers?\s*[:,]?\s*"
            rf"(?P<support>{NUMBER_TOKEN})(?: speakers?)? {SUPPORT_TERM}.{{0,120}}?"
            rf"(?P<opposition>{NUMBER_TOKEN})(?: speakers?)? {OPPOSITION_TERM}",
            re.IGNORECASE,
        ),
        re.compile(
            rf"\b(?P<support>{NUMBER_TOKEN}) (?:speakers?|appearances?).{{0,220}}?{SUPPORT_TERM}"
            rf".{{0,220}}?\b(?P<opposition>{NUMBER_TOKEN})(?: (?:speakers?|appearances?))?.{{0,80}}?{OPPOSITION_TERM}",
            re.IGNORECASE,
        ),
        re.compile(
            rf"\b(?P<opposition>{NUMBER_TOKEN}) (?:speakers?|appearances?).{{0,220}}?{OPPOSITION_TERM}"
            rf".{{0,220}}?\b(?P<support>{NUMBER_TOKEN})(?: (?:speakers?|appearances?))?.{{0,80}}?{SUPPORT_TERM}",
            re.IGNORECASE,
        ),
    ]
    single_patterns = {
        "support": re.compile(
            rf"\b(?:there (?:was|were) )?(?P<count>{NUMBER_TOKEN}) (?:speakers?|appearances?)"
            rf".{{0,220}}?{SUPPORT_TERM}",
            re.IGNORECASE,
        ),
        "opposition": re.compile(
            rf"\b(?:there (?:was|were) )?(?P<count>{NUMBER_TOKEN}) (?:speakers?|appearances?)"
            rf".{{0,220}}?{OPPOSITION_TERM}",
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
                rf"\b(?:a|an) (?:representative|speaker).{{0,220}}?{SUPPORT_TERM}",
                block,
                re.IGNORECASE,
            ):
                block_support = 1
            if block_opposition is None and re.search(
                rf"\b(?:a|an) (?:representative|speaker).{{0,220}}?{OPPOSITION_TERM}",
                block,
                re.IGNORECASE,
            ):
                block_opposition = 1

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

    explicit_patterns = [
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
    ]
    explicit_matches = [match for pattern in explicit_patterns for match in pattern.finditer(text)]
    explicit_match = min(explicit_matches, key=lambda match: match.start()) if explicit_matches else None
    if explicit_match:
        support = parse_number(explicit_match.group("support"))
        opposition = parse_number(explicit_match.group("opposition"))
        context_start = max(0, explicit_match.start() - 180)
        context_end = min(len(text), explicit_match.end() + 320)
        context = text[context_start:context_end]
        stance_matches = list(
            re.finditer(
                r"\b(?:recommend\w* |resolution )?(?P<stance>disapprov\w*|unfavorable|approv\w*|favorable)\b",
                context,
                re.IGNORECASE,
            )
        )
        if stance_matches:
            match_center = explicit_match.start() - context_start
            nearest_stance = min(
                stance_matches,
                key=lambda match: abs(match.start() - match_center),
            ).group("stance").lower()
            if (nearest_stance.startswith("disapprov") or nearest_stance == "unfavorable") and support >= opposition:
                support, opposition = opposition, support
        return support, opposition

    unlabeled_pattern = re.compile(
        rf"\b(?:by a vote of|vote(?:d| was)?|voting)\s+"
        rf"(?P<first>{NUMBER_TOKEN})\s*(?:to|-|/)\s*(?P<second>{NUMBER_TOKEN})\b",
        re.IGNORECASE,
    )
    for match in unlabeled_pattern.finditer(text):
        context = text[max(0, match.start() - 180) : min(len(text), match.end() + 240)]
        first = parse_number(match.group("first"))
        second = parse_number(match.group("second"))
        stance_matches = list(
            re.finditer(
                r"\b(?:recommend\w* |resolution )?(?P<stance>disapprov\w*|unfavorable|approv\w*|favorable)\b",
                context,
                re.IGNORECASE,
            )
        )
        if stance_matches:
            match_center = match.start() - max(0, match.start() - 180)
            nearest_stance = min(
                stance_matches,
                key=lambda stance_match: abs(stance_match.start() - match_center),
            ).group("stance").lower()
            if nearest_stance.startswith("disapprov") or nearest_stance == "unfavorable":
                return second, first
            return first, second

    support_only = re.search(
        rf"\b(?P<support>{NUMBER_TOKEN})(?: board members?)? voting in favor\b",
        text,
        re.IGNORECASE,
    )
    if support_only and re.search(r"\bunanimous\w*\b", text, re.IGNORECASE):
        return parse_number(support_only.group("support")), 0

    labeled_fields = re.search(
        rf"\b(?:voting |#\s*)?in favor\s*:?\s*(?P<support>{NUMBER_TOKEN}).{{0,100}}?"
        rf"\b(?:voting |#\s*)?against\s*:?\s*(?P<opposition>{NUMBER_TOKEN})",
        text,
        re.IGNORECASE,
    )
    if labeled_fields:
        return (
            parse_number(labeled_fields.group("support")),
            parse_number(labeled_fields.group("opposition")),
        )

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


def issue_is_positive(issue_pattern, context_rows):
    for row in context_rows:
        if not issue_pattern.search(row["sentence"]):
            continue
        context = row["context"]
        if row["section"] in {"community_board", "borough_president", "cpc_hearing"}:
            if REVIEW_ACTION.search(context):
                return True
        elif row["section"] == "consideration_findings" and REVIEW_ACTION.search(context):
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
    for section, section_text in parse_sections(document["text"]).items():
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
                "sentence": row["sentence"],
                "context": context,
            }
            context_rows.append(context_row)
            section_contexts[section].append(context)

    community_board_text = " ".join(
        row["sentence"]
        for row in document_section_sentences[(document_id, "community_board")]
    )
    cpc_hearing_text = " ".join(
        row["sentence"]
        for row in document_section_sentences[(document_id, "cpc_hearing")]
    )
    cpc_support_speakers, cpc_opposition_speakers = extract_cpc_speaker_counts(
        cpc_hearing_text
    )
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
        community_board_text
    )
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

    local_contexts = (
        section_contexts["community_board"]
        + section_contexts["borough_president"]
        + section_contexts["cpc_hearing"]
    )
    review_contexts = local_contexts + section_contexts["consideration_findings"]
    cb_request = any(
        SUBSTANTIVE_REQUEST.search(context)
        and not MINOR_OR_PROCEDURAL_REQUEST.search(context)
        for context in section_contexts["community_board"]
    )
    bp_request = any(
        SUBSTANTIVE_REQUEST.search(context)
        and not MINOR_OR_PROCEDURAL_REQUEST.search(context)
        for context in section_contexts["borough_president"]
    )
    council_position = actor_position(review_contexts, COUNCIL_ACTOR)
    civic_position = actor_position(review_contexts, CIVIC_ACTOR)

    revision_or_concession = any(
        REVISION_OR_CONCESSION.search(context)
        and not MECHANICAL_REVISION.search(context)
        for context in review_contexts
    )
    explicit_local_response = any(
        EXPLICIT_RESPONSE_LINK.search(context) and RESPONSE_ACTION.search(context)
        for context in review_contexts
    )
    procedural_response = any(
        PROCEDURAL_RESPONSE.search(context)
        and (
            EXPLICIT_RESPONSE_LINK.search(context)
            or re.search(
                r"\b(?:applicant|agency|commission)\b.{0,100}\b(?:agreed|committed|will|shall)\b",
                context,
                re.IGNORECASE,
            )
        )
        for context in review_contexts
    )

    cb_opposition = (
        cb_support_votes is not None
        and cb_opposition_votes is not None
        and cb_opposition_votes > cb_support_votes
    ) or any(
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
        or any(
            SUBSTANTIVE_REQUEST.search(context)
            and re.search(
                r"\b(?:community board|borough president|council ?member|councilmember|"
                r"civic association|tenant association|neighbou?rhood association|"
                r"community organization|community group|residents?|speakers?)\b",
                context,
                re.IGNORECASE,
            )
            and not MINOR_OR_PROCEDURAL_REQUEST.search(context)
            for context in local_contexts
        )
    )
    approved_unresolved_objection = (
        substantial_local_opposition
        and any(
            UNRESOLVED_RESPONSE.search(context)
            for context in section_contexts["consideration_findings"]
        )
    )

    measurements = {
        "substantial_local_opposition": int(substantial_local_opposition),
        "local_request_condition": int(local_request_condition),
        "revision_or_concession": int(revision_or_concession),
        "procedural_response": int(procedural_response),
        "explicit_local_response": int(explicit_local_response),
        "approved_unresolved_objection": int(approved_unresolved_objection),
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
    for field, issue_pattern in ISSUE_PATTERNS.items():
        measurements[field] = int(issue_is_positive(issue_pattern, context_rows))

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
    "narrative_boundary_method",
    "zap_project_ids",
    "analysis_non_pp_flag",
    "analysis_zm_zr_zs_flag",
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
                "narrative_boundary_method": document[
                    "narrative_boundary_method"
                ],
                "zap_project_ids": document["zap_project_ids"],
                "analysis_non_pp_flag": document["analysis_non_pp_flag"],
                "analysis_zm_zr_zs_flag": document[
                    "analysis_zm_zr_zs_flag"
                ],
                **document_measurements[document["document_id"]],
            }
        )

print(f"Wrote deterministic text labels for {len(documents)} CPC narratives.")
