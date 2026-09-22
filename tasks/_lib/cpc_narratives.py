"""Shared CPC narrative boundaries and exact-text normalization."""

import re


def project_review_key(vote_date, zap_project_ids, project_name):
    """A generic project title is not evidence that two applications belong together."""
    ids = sorted({value.strip() for value in zap_project_ids.split(';') if value.strip()})
    name = re.sub(r'[^a-z0-9]+', ' ', project_name.lower()).strip()
    name = re.sub(r'\bsize\s+\d+(?:\s+\d+)?\s+mb\b', '', name).strip()
    return vote_date + '|zap:' + ';'.join(ids) + '|name:' + name if vote_date and ids and name else ''

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

ANY_RESOLVED_HEADING = re.compile(
    r"(?im)^[ \t\f]*RESOLVED(?:[ \t]*,|[ \t]+BY\b|[ \t]+THAT\b).*$"
)

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
