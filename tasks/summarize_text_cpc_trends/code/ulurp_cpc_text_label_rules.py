#!/usr/bin/env python3

import re


OPPOSITION_CONTEXT_PATTERN = re.compile(
    r"\b(oppos(?:e|ed|es|ition)|object(?:ed|ion|ions)?|concern(?:ed|s)?|"
    r"testif(?:y|ied|ies).{0,80}\bagainst|against the application|"
    r"spoke against|speaker(?:s)? in opposition)\b",
    re.IGNORECASE,
)

NO_OPPOSITION_PATTERN = re.compile(
    r"\b(no speakers? (?:appeared |spoke |testified )?in opposition|"
    r"there (?:was|were) no (?:speaker|speakers|testimony) in opposition|"
    r"no one (?:appeared|spoke|testified) in opposition)\b",
    re.IGNORECASE,
)

SIGNAL_RULES = [
    (
        "revision_concession",
        re.compile(
            r"\b(revis(?:e|ed|ion|ions)|modif(?:y|ied|ication|ications)|amended|"
            r"changed|scaled back|reduced (?:in|the)|subsequent to certification|"
            r"applicant.{0,80}(?:agreed|committed|revised|modified|changed|reduced))\b",
            re.IGNORECASE,
        ),
    ),
    (
        "attribution_community_board",
        re.compile(
            r"\b(?:in response to|at the request of|as requested by|after meeting with|"
            r"in consultation with).{0,120}\bcommunity board\b|"
            r"\bcommunity board\b.{0,120}\b(?:request|concern|condition|recommend)",
            re.IGNORECASE,
        ),
    ),
    (
        "attribution_borough_president",
        re.compile(
            r"\b(?:in response to|at the request of|as requested by|after meeting with|"
            r"in consultation with).{0,120}\bborough president\b|"
            r"\bborough president\b.{0,120}\b(?:request|concern|condition|recommend)",
            re.IGNORECASE,
        ),
    ),
    (
        "attribution_council_member",
        re.compile(
            r"\b(?:in response to|at the request of|as requested by|after meeting with|"
            r"in consultation with).{0,120}\b(?:council member|councilmember|city council)\b|"
            r"\b(?:council member|councilmember|city council)\b.{0,120}\b"
            r"(?:request|concern|condition|recommend|support|oppos|revise|modify|meeting|met)",
            re.IGNORECASE,
        ),
    ),
    (
        "attribution_civic_group",
        re.compile(
            r"\b(?:in response to|at the request of|as requested by|after meeting with|"
            r"in consultation with).{0,120}\b(?:civic|association|community group|"
            r"community organization|tenant association|neighborhood association)\b|"
            r"\b(?:civic|association|community group|community organization|tenant association|"
            r"neighborhood association)\b.{0,120}\b(?:request|concern|condition|recommend|oppos)",
            re.IGNORECASE,
        ),
    ),
    (
        "attribution_applicant",
        re.compile(
            r"\bapplicant\b.{0,120}\b(?:request|concern|agreed|committed|revised|modified|"
            r"changed|reduced|met|meeting)",
            re.IGNORECASE,
        ),
    ),
    (
        "attribution_unspecified",
        re.compile(
            r"\b(?:in response to concerns|in response to comments|at the request of|"
            r"as requested by|after meeting with|in consultation with)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "conditions_commitments",
        re.compile(
            r"\b(terms and conditions|applicant shall|agreed to|committed to|commitment|"
            r"restrictive declaration|points of agreement|memorandum of understanding|"
            r"letter of intent|shall provide|shall be required)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "restrictive_declaration",
        re.compile(r"\brestrictive declaration\b", re.IGNORECASE),
    ),
    (
        "points_of_agreement",
        re.compile(r"\bpoints of agreement\b", re.IGNORECASE),
    ),
    (
        "dollar_terms",
        re.compile(r"\$\s?[0-9][0-9,]*(?:\.[0-9]+)?|\b[0-9]+(?:\.[0-9]+)? million dollars\b", re.IGNORECASE),
    ),
    (
        "substantive_council_member",
        re.compile(
            r"\b(?:council member|councilmember|city council|the council)\b.{0,120}\b"
            r"(?:request|concern|met|meeting|support|oppos|condition|revise|modify|reduce|"
            r"agreed|committed|recommended)|"
            r"\b(?:request|concern|met|meeting|support|oppos|condition|revise|modify|reduce|"
            r"agreed|committed|recommended).{0,120}\b"
            r"(?:council member|councilmember|city council|the council)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "community_board_disapproval",
        re.compile(
            r"\bcommunity board\b.{0,120}\b(?:disapprov|voted against|recommend(?:ed)? disapproval)|"
            r"\b(?:disapprov|voted against|recommend(?:ed)? disapproval).{0,120}\bcommunity board\b",
            re.IGNORECASE,
        ),
    ),
    (
        "community_board_conditioned_approval",
        re.compile(
            r"\bcommunity board\b.{0,120}\b(?:approv(?:ed|al)?.{0,40}(?:condition|provided that|subject to)|"
            r"condition(?:s|al)?.{0,40}approv)|"
            r"\b(?:condition(?:s|al)?.{0,40}approv|approv(?:ed|al)?.{0,40}(?:condition|provided that|subject to))"
            r".{0,120}\bcommunity board\b",
            re.IGNORECASE,
        ),
    ),
]

OPPOSITION_RULES = [
    (
        "opposition_any",
        re.compile(r".", re.IGNORECASE),
    ),
    (
        "opposition_traffic_parking",
        re.compile(r"\b(traffic|parking|congestion|truck|trucks|curb|loading)\b", re.IGNORECASE),
    ),
    (
        "opposition_scale_character",
        re.compile(
            r"\b(scale|bulk|height|density|out of character|neighborhood character|"
            r"context|too (?:large|tall|massive|dense)|light and air|shadow|shadows)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "opposition_displacement_affordability",
        re.compile(
            r"\b(displacement|displace|gentrification|luxury|affordab(?:le|ility)|"
            r"rent|tenant|harassment)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "opposition_infrastructure",
        re.compile(
            r"\b(school|schools|sewer|infrastructure|transit|subway|sanitation|"
            r"water|open space|park|parks)\b",
            re.IGNORECASE,
        ),
    ),
]

SIGNAL_FAMILIES = list(
    dict.fromkeys(
        signal_family
        for signal_family, _pattern in SIGNAL_RULES + OPPOSITION_RULES
    )
)

REVIEW_SECTIONS = {
    "ulurp",
    "community_board",
    "borough_president",
    "cpc_hearing",
    "consideration_findings",
    "unsectioned",
}
COMMUNITY_BOARD_SECTIONS = REVIEW_SECTIONS - {"borough_president"}
OPPOSITION_SECTIONS = REVIEW_SECTIONS - {"ulurp"}

SIGNAL_SECTION_ALLOWLIST = {
    "revision_concession": REVIEW_SECTIONS | {"background", "environmental_review"},
    "attribution_community_board": REVIEW_SECTIONS,
    "attribution_borough_president": REVIEW_SECTIONS,
    "attribution_council_member": REVIEW_SECTIONS,
    "attribution_civic_group": REVIEW_SECTIONS,
    "attribution_applicant": REVIEW_SECTIONS | {"background"},
    "attribution_unspecified": REVIEW_SECTIONS,
    "substantive_council_member": REVIEW_SECTIONS,
    "community_board_disapproval": COMMUNITY_BOARD_SECTIONS,
    "community_board_conditioned_approval": COMMUNITY_BOARD_SECTIONS,
    "opposition_any": OPPOSITION_SECTIONS,
    "opposition_traffic_parking": OPPOSITION_SECTIONS,
    "opposition_scale_character": OPPOSITION_SECTIONS,
    "opposition_displacement_affordability": OPPOSITION_SECTIONS,
    "opposition_infrastructure": OPPOSITION_SECTIONS,
}


NO_OPPOSITION = re.compile(
    r"\b(no opposition|there was no opposition|there were no speakers?.{0,40}opposition|"
    r"no one.{0,40}opposition|0 opposed|0 in opposition|0 opposition|none opposed|"
    r"none in opposition|without opposition|not (?:be )?opposed|would not be opposed|"
    r"no objection|as opposed to)\b",
    re.IGNORECASE,
)

PROCEDURAL_COUNCIL = re.compile(
    r"\b(transmitted to the city council|filed with.{0,80}city council|"
    r"referred to.{0,80}city council|approved by.{0,80}city council|"
    r"city council.{0,80}approved|pursuant to section 197-d|"
    r"office of the speaker.{0,80}city council)\b",
    re.IGNORECASE,
)

NON_GOVERNMENT_COUNCIL = re.compile(
    r"\b(parks council|planning council|joint planning council|"
    r"community council|tenant council|council on|district council of|"
    r"central labor council)\b",
    re.IGNORECASE,
)

SPECIAL_PERMIT_OR_DOCUMENT_REVISION = re.compile(
    r"\b(grant of a special permit.{0,120}modify|to modify.{0,80}requirements|"
    r"to modify .{0,80}regulations|"
    r"requires? modifications? of .{0,80}regulations|"
    r"requires? the modification of .{0,80}regulations|"
    r"requested .{0,40}bulk modifications|"
    r"bulk modifications shall have minimal adverse effects|"
    r"such bulk modifications|"
    r"modifications specifically granted|modifications herein granted|"
    r"except for the modifications|special permit modifications|"
    r"modifications? of use or bulk regulations|last date revised|latest revision|"
    r"application requests? a modification|"
    r"authorizations? for modification|"
    r"modification of existing topography|"
    r"modification of grades necessitated|"
    r"revised certificate of occupancy|"
    r"modified landmarks transfer|"
    r"waterfront revitalization program .{0,80}as amended|"
    r"application.{0,80}amendment of the zoning map|"
    r"application.{0,80}zoning map amendment|"
    r"amended .{0,40}urban renewal plan|"
    r"amended .{0,40}urban renewal project|"
    r"amended .{0,40}urban renewal area|"
    r"amended cpurp|"
    r"amended .{0,30}cpurp|"
    r"pursuant to .{0,60}amended cpurp|"
    r"prior action to map|"
    r"modified land use approvals|"
    r"modified bulk regulations|"
    r"special provisions for bulk modification|"
    r"this modification will allow|"
    r"revised .{0,30}alternative|"
    r"zoning resolution .{0,80}as subsequently amended.{0,80}further amended by changing|"
    r"housing new york:.{0,220}as modified in \d{4}|"
    r"appropriate modification of the city map|"
    r"declaration may be amended or cancelled|"
    r"amendment, modification, or cancellation of this declaration|"
    r"following any modification, amendment or cancellation|"
    r"amendment, modification and cancellation|"
    r"amendment modification and cancellation|"
    r"(?:housing|national housing|zoning|administrative|public housing) act .{0,80}as amended|"
    r"modification of a sewer easement|"
    r"approval without modification|"
    r"no significant effect.{0,80}modified as follows|"
    r"call for the modification of .{0,120}zoning resolution|"
    r"call for modification of .{0,80}(?:zr|mih|mandatory inclusionary housing)|"
    r"corrected, revised and maps furnished|"
    r"revised mitigation report|"
    r"budget and revised scope|"
    r"^revised city planning commission|"
    r"^revised[\\s\\-]+[ivx0-9]*\\s*city planning commission|"
    r"date revision|"
    r"grounds .{0,80}disapprove .{0,80}application for modification|"
    r"non-ulurp modification|"
    r"subsequently amended numerous times|"
    r"as originally proposed or as modified|"
    r"adopted a modification .{0,80}city master plan|"
    r"streets would be significantly changed|"
    r"views .{0,80}changed|"
    r"proposed interior modification|"
    r"revised hud guidelines|"
    r"revised new york city charter|"
    r"modify .{0,80}lottery community preference standards|"
    r"modification .{0,120}height and setback|"
    r"height .{0,80}setback .{0,80}modification|"
    r"modifications? of height (?:and setback )?regulations|"
    r"height and setback regulations|"
    r"use modifications shall have minimal adverse effects|"
    r"zoning ordinance could be modified|"
    r"propose modified or new guidelines|"
    r"exterior modification shall not alter|"
    r"administratively approve modifications to the declaration|"
    r"revised environmental assessment statement|"
    r"revised negative declaration|"
    r"modification to sign regulations|"
    r"modification of .{0,80}special permit to|"
    r"bulk modifications? on waterfront blocks|"
    r"authorization .{0,80}modification of waterfront visual corridor|"
    r"proposed use modifications would have minimal adverse effects|"
    r"dcp needs to modify the zr|"
    r"revised draft scope of work|"
    r"girder support system .{0,80}modified|"
    r"shall notify .{0,80}community board .{0,80}modification|"
    r"rezoning changed most|"
    r"would be changed from|"
    r"have become .{0,80}as .{0,40}changed|"
    r"needs .{0,80}changed|"
    r"revised scope of work|"
    r"no significant effect.{0,80}upon modification)\b",
    re.IGNORECASE,
)

CONCRETE_CONDITIONAL_APPROVAL = re.compile(
    r"\b(approve|approved|approval|recommend(?:ed)? approval).{0,80}"
    r"(with modifications?\s*/\s*conditions?|with conditions?|with the condition that|subject to|provided that|unless)\b|"
    r"\b(with modifications?\s*/\s*conditions?|with conditions?|with the condition that|subject to|provided that|unless).{0,80}"
    r"(approve|approved|approval|recommend(?:ed)? approval)\b",
    re.IGNORECASE,
)

CONDITION_DETAIL = re.compile(
    r"\b(condition|conditions|roof|repair|fencing|screening|landscap|parking|"
    r"hours|loading|access|traffic|mitigation|restrictive declaration|agreement|"
    r"memorandum|shall|must|require)\b",
    re.IGNORECASE,
)

FORM_ONLY_CONDITIONS = re.compile(
    r"\b(attach additional sheets?|recommendation attached|recommendation to follow|"
    r"explanation of recommendation[- ]modifications?/conditions?)\b",
    re.IGNORECASE,
)

CONCRETE_REVIEW_CHANGE = re.compile(
    r"\b(proposal was modified|application was modified|modified application|"
    r"modified special permit application|amended application|a-application|"
    r"project,? as amended|design change certification|changes? to the application include|"
    r"require modification and conditions|requires modification and conditions|"
    r"development plan was changed|"
    r"application as amended relative to|"
    r"zoning map, as revised, is appropriate|"
    r"zoning is modified to|"
    r"grant of .{0,80} as (?:modified|revised),? is appropriate|"
    r"modified two-way plan|modified 2-way plan|"
    r"apartment distribution has been changed|"
    r"subsequent modification was submitted|"
    r"revised site plan was submitted|"
    r"approvals were modified .{0,80}to accommodate|"
    r"modifications were approved .{0,80}to facilitate|"
    r"\b[cn]\s*\d{6}\s*\(a\)|"
    r"elimination of .{0,80}from the proposed|remove the .{0,80}from the proposed|"
    r"in response to concerns? raised by|in response to a comment received at the public hearing|"
    r"in response to these concerns|applicants? revised|applicant revised|"
    r"revised the applications?|modified the applications?|submitted .{0,60}\\(a\\)|"
    r"modified to seek)\b",
    re.IGNORECASE,
)

REVISION_TRUE = re.compile(
    r"\b(after the public hearing|subsequent to|in response to|at the request of|"
    r"as requested by|applicant.{0,100}(agreed|committed|revised|modified|changed|reduced)|"
    r"agreed to|committed to|scaled back|reduced|reducing|shrink|shrinking|"
    r"modified proposal|modified application|modification of the scope|"
    r"revised proposal|revised application|revised plans?)\b",
    re.IGNORECASE,
)

OPPOSITION_DIRECT = re.compile(
    r"\b(oppos(?:e|es|ed|ing|ition)|object(?:ed|ion|ions)?|against the application|"
    r"spoke against|testified against|speakers? in opposition)\b",
    re.IGNORECASE,
)

CONCERN_CONTEXT = re.compile(
    r"\b(community board|borough president|community|resident|residents|speaker|"
    r"civic|association|tenant|neighbors?|elected official|council member|"
    r"councilmember|city council)\b.{0,160}\b(concern|concerns|concerned)\b|"
    r"\b(concern|concerns|concerned)\b.{0,160}\b(community board|borough president|"
    r"community|resident|residents|speaker|civic|association|tenant|neighbors?|"
    r"elected official|council member|councilmember|city council)\b",
    re.IGNORECASE,
)

PUBLIC_CONCERN_CONTEXT = re.compile(
    r"\b(speakers?|participants?|testimony|testified|public hearing|community board|"
    r"\bcb\s?\d+|borough president|board|senator|assembly member|council member|"
    r"councilmember|resident|residents|community|neighbors?|public|civic|association)\b"
    r".{0,180}\b(concern|concerns|concerned)\b|"
    r"\b(concern|concerns|concerned)\b.{0,180}\b(speakers?|participants?|testimony|"
    r"testified|public hearing|community board|\bcb\s?\d+|borough president|board|"
    r"senator|assembly member|council member|councilmember|resident|residents|"
    r"community|neighbors?|public|civic|association)\b|"
    r"\bi have .{0,40}concerns?\b",
    re.IGNORECASE,
)

INTERNAL_CONCERN_CONTEXT = re.compile(
    r"\b(commissions?|commission\b|department|dcp staff|planning staff|agency|mta)\b.{0,120}"
    r"\b(concern|concerns|concerned)\b|"
    r"\b(concern|concerns|concerned)\b.{0,120}"
    r"\b(commissions?|commission\b|department|dcp staff|planning staff|agency|mta)\b|"
    r"\b(other environmental concerns|primary issues of concern analyzed for consistency)\b",
    re.IGNORECASE,
)

SECTION_OR_ACTOR_ATTRIBUTION = {
    "attribution_community_board": re.compile(
        r"\bcommunity board\b.{0,180}\b(request\w*|concern\w*|condition\w*|recommend\w*|"
        r"approv\w*|disapprov\w*|oppos\w*|support\w*|vot\w*|resolution)\b|"
        r"\b(request\w*|concern\w*|condition\w*|recommend\w*|approv\w*|"
        r"disapprov\w*|oppos\w*|support\w*|vot\w*|resolution)\b.{0,180}\bcommunity board\b",
        re.IGNORECASE,
    ),
    "attribution_borough_president": re.compile(
        r"\bborough president\b.{0,180}\b(request\w*|concern\w*|condition\w*|recommend\w*|"
        r"approv\w*|disapprov\w*|oppos\w*|support\w*|modif\w*|urg\w*|consult\w*)\b|"
        r"\b(request\w*|concern\w*|condition\w*|recommend\w*|approv\w*|"
        r"disapprov\w*|oppos\w*|support\w*|modif\w*|urg\w*|consult\w*)\b.{0,180}\bborough president\b",
        re.IGNORECASE,
    ),
    "attribution_council_member": re.compile(
        r"\b(council member|councilmember|local city council member|city council|"
        r"the council)\b.{0,180}\b(request\w*|concern\w*|condition\w*|recommend\w*|"
        r"support\w*|oppos\w*|modif\w*|consult\w*|meeting|met|urg\w*)\b|"
        r"\b(request\w*|concern\w*|condition\w*|recommend\w*|support\w*|"
        r"oppos\w*|modif\w*|consult\w*|meeting|met|urg\w*)\b.{0,180}\b(council member|councilmember|"
        r"local city council member|city council|the council)\b",
        re.IGNORECASE,
    ),
    "attribution_civic_group": re.compile(
        r"\b(civic|association|community organization|community group|tenant association|"
        r"neighborhood association)\b.{0,180}\b(request\w*|concern\w*|condition\w*|"
        r"recommend\w*|support\w*|oppos\w*|object\w*|consult\w*|determin\w*)\b|"
        r"\b(request\w*|concern\w*|condition\w*|recommend\w*|support\w*|"
        r"oppos\w*|object\w*|consult\w*|determin\w*)\b.{0,180}"
        r"\b(civic|association|community organization|community group|tenant association|"
        r"neighborhood association)\b",
        re.IGNORECASE,
    ),
    "attribution_applicant": re.compile(
        r"\bapplicant\b.{0,180}\b(agreed|committed|revised|modified|changed|"
        r"reduced|met with|meeting with|responded)\b|"
        r"\b(agreed|committed|revised|modified|changed|reduced|met with|"
        r"meeting with|responded)\b.{0,180}\bapplicant\b",
        re.IGNORECASE,
    ),
}

CONDITIONS_TRUE = re.compile(
    r"\b(applicant shall|restrictive declaration|terms and conditions|"
    r"points of agreement|memorandum of understanding|committed to|commitment|agreed to|"
    r"shall provide|shall be required|letter of intent)\b",
    re.IGNORECASE,
)

CONDITIONS_FALSE = re.compile(
    r"\b(shall conform to all applicable provisions of the zoning resolution|"
    r"except for the modifications)\b",
    re.IGNORECASE,
)


def clean_rule_text(rule_text):
    text = " ".join(rule_text.split())
    return re.sub(r"\bcommuni\s+ty\b", "community", text, flags=re.IGNORECASE)


def revision_signal_is_positive(text):
    if SPECIAL_PERMIT_OR_DOCUMENT_REVISION.search(text):
        return False
    if CONCRETE_REVIEW_CHANGE.search(text):
        return True
    if CONCRETE_CONDITIONAL_APPROVAL.search(text) and CONDITION_DETAIL.search(text):
        if FORM_ONLY_CONDITIONS.search(text) and not re.search(
            r"\b(roof|repair|fencing|screening|landscap|parking|hours|loading|access|"
            r"traffic|mitigation|restrictive declaration|agreement|memorandum|shall|must|require)\b",
            text,
            re.IGNORECASE,
        ):
            return False
        return True
    if REVISION_TRUE.search(text):
        return True
    return False


def council_signal_is_positive(text):
    if PROCEDURAL_COUNCIL.search(text):
        return False
    if re.search(
        r"\bbefore final city council approval\b|\bcity council phase\b|"
        r"\bcommunity board phase, borough president, city planning commission and city council\b",
        text,
        re.IGNORECASE,
    ):
        return False
    if re.search(
        r"\bcity council\b.{0,120}\b(obtain written commitments|should obtain|"
        r"must receive|modify the application|should modify|conditionally approve)\b|"
        r"\b(obtain written commitments|should obtain|must receive|modify the application|"
        r"should modify|conditionally approve)\b.{0,120}\bcity council\b|"
        r"\bcity council members\b.{0,120}\b(recognized|regular meetings|commitments)\b",
        text,
        re.IGNORECASE,
    ):
        return True
    if NON_GOVERNMENT_COUNCIL.search(text) and not re.search(
        r"\b(city council|council member|councilmember|the council)\b",
        text,
        re.IGNORECASE,
    ):
        return False
    if SECTION_OR_ACTOR_ATTRIBUTION["attribution_council_member"].search(text):
        return True
    return False


def opposition_signal_is_positive(text, signal_family, section):
    if NO_OPPOSITION.search(text):
        return False
    if OPPOSITION_DIRECT.search(text):
        return True
    if PUBLIC_CONCERN_CONTEXT.search(text):
        return True
    if INTERNAL_CONCERN_CONTEXT.search(text):
        return False
    if section in {"community_board", "borough_president"} and re.search(
        r"\b(concern|concerns|concerned)\b",
        text,
        re.IGNORECASE,
    ):
        return True
    if signal_family == "opposition_displacement_affordability" and re.search(
        r"\b(concern|concerns|concerned)\b.{0,220}\b(displacement|displace|"
        r"affordab(?:le|ility)|rent|tenant|soft sites?|permanent affordability)\b|"
        r"\b(displacement|displace|affordab(?:le|ility)|rent|tenant|soft sites?|"
        r"permanent affordability)\b.{0,220}\b(concern|concerns|concerned)\b",
        text,
        re.IGNORECASE,
    ):
        return True
    if signal_family == "opposition_infrastructure" and re.search(
        r"\b(concern|concerns|concerned)\b.{0,220}\b(sewer|water|overflow|"
        r"infrastructure|transit|subway|school|schools|open space|parks?)\b|"
        r"\b(sewer|water|overflow|infrastructure|transit|subway|school|schools|"
        r"open space|parks?)\b.{0,220}\b(concern|concerns|concerned)\b",
        text,
        re.IGNORECASE,
    ):
        return True
    if CONCERN_CONTEXT.search(text):
        return True
    return False


def attribution_signal_is_positive(text, signal_family):
    if signal_family == "attribution_council_member":
        return council_signal_is_positive(text)
    if signal_family == "attribution_unspecified":
        return re.search(
            r"\b(in response to concerns|in response to comments|at the request of|"
            r"as requested by|after meeting with|in consultation with)\b",
            text,
            re.IGNORECASE,
        ) is not None

    if signal_family == "attribution_applicant" and re.search(
        r"\bapplicant\b.{0,160}\b(filed|is filed|submitted|is being submitted|"
        r"request|seeks|is seeking|requested|requests|requesting|proposes|is proposing|"
        r"requested an? (amendment|approval|special permit|authorization))\b",
        text,
        re.IGNORECASE,
    ) and not re.search(
        r"\bapplicant\b.{0,180}\b(agreed|committed|revised|modified|changed|"
        r"responded|addressed|willing|restated their commitment)\b",
        text,
        re.IGNORECASE,
    ):
        return False
    if signal_family == "attribution_applicant" and re.search(
        r"\bapplicant'?s request\b|\bapplicant\b.{0,120}\bhas not met\b",
        text,
        re.IGNORECASE,
    ):
        return False
    if signal_family == "attribution_applicant" and re.search(
        r"\bapplicant\b.{0,180}\b(willing to comply|willing to return|"
        r"reiterated concerns|addressed these concerns|restated their commitment|continue .{0,60}working with|"
        r"shall|must|be required to|extend use|address community concerns)\b",
        text,
        re.IGNORECASE,
    ):
        return True

    pattern = SECTION_OR_ACTOR_ATTRIBUTION.get(signal_family)
    if pattern and pattern.search(text):
        return True

    return False


def conditions_signal_is_positive(text, signal_family):
    if signal_family in {"restrictive_declaration", "points_of_agreement", "dollar_terms"}:
        return True
    if CONDITIONS_FALSE.search(text):
        return False
    if CONDITIONS_TRUE.search(text):
        return True
    return False


def community_board_signal_is_positive(text, signal_family):
    if signal_family == "community_board_disapproval":
        if re.search(
            r"\b(city planning commission resolution|hereby disapproved)\b",
            text,
            re.IGNORECASE,
        ) and not re.search(
            r"\bcommunity board\b.{0,200}\b(disapprov\w*|recommend(?:ed)? disapproval|"
            r"voted against|oppos\w*)\b",
            text,
            re.IGNORECASE,
        ):
            return False
        return re.search(
            r"\bcommunity board\b.{0,220}\b(disapprov\w*|voted against|"
            r"recommend(?:ed)? disapproval|oppos\w*)\b|"
            r"\b(disapprov\w*|recommend(?:ed)? disapproval|oppos\w*)\b.{0,220}"
            r"\bcommunity board\b|"
            r"\bapplication was disapproved by both the community board\b|"
            r"\brecommendation .{0,120}\bdisapprove\b",
            text,
            re.IGNORECASE,
        ) is not None

    if signal_family == "community_board_conditioned_approval":
        return re.search(
            r"\bcommunity board\b.{0,260}\b(approv\w*|recommend\w*|voted in favor)"
            r".{0,140}\b(condition\w*|provided that|subject to|unless|with the following conditions)\b|"
            r"\bcommunity board\b.{0,220}\brequested .{0,80}\bconditions of its approval\b|"
            r"\bcommunity board\b.{0,260}\b(condition(?:al|ally)? approv\w*|approval with conditions)\b|"
            r"\bcondition(?:ed)? (?:our |its )?approval\b|"
            r"\bconditions? (?:were|was) approved by the community board\b|"
            r"\bapprove the application with the same conditions set forth by community board\b",
            text,
            re.IGNORECASE,
        ) is not None

    return False


def signal_is_positive(rule_text, section, signal_family):
    text = clean_rule_text(rule_text)

    if signal_family == "revision_concession":
        return revision_signal_is_positive(text)
    if signal_family == "substantive_council_member":
        return council_signal_is_positive(text)
    if signal_family.startswith("opposition_"):
        return opposition_signal_is_positive(text, signal_family, section)
    if signal_family.startswith("attribution_"):
        return attribution_signal_is_positive(text, signal_family)
    if signal_family in {
        "conditions_commitments",
        "restrictive_declaration",
        "points_of_agreement",
        "dollar_terms",
    }:
        return conditions_signal_is_positive(text, signal_family)
    if signal_family in {
        "community_board_disapproval",
        "community_board_conditioned_approval",
    }:
        return community_board_signal_is_positive(text, signal_family)

    return False
