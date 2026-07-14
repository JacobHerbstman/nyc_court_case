#!/usr/bin/env python3

import csv
import hashlib
import re
import sys
from collections import defaultdict
from pathlib import Path


# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/build_ulurp_cpc_text_analysis/code")
# start_year = 1975
# end_year = 2025


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
    if anchor_matches and anchor_matches[0].start() < 0.75 * len(text):
        anchor = anchor_matches[0].start()
    else:
        anchor = min(500, len(text))
    resolution_matches = [
        match for match in RESOLUTION_HEADING.finditer(text) if match.start() > anchor
    ]
    if resolution_matches:
        return resolution_matches[0].start(), "resolution_heading"

    filing_matches = [match for match in FILING_PARAGRAPH.finditer(text) if match.start() > anchor]
    if filing_matches:
        return filing_matches[0].start(), "filing_paragraph"

    adopted_resolution_matches = [
        match for match in ADOPTED_RESOLUTION.finditer(text) if match.start() > anchor
    ]
    if adopted_resolution_matches:
        return adopted_resolution_matches[0].start(), "adopted_resolution_paragraph"

    signature_matches = [
        match for match in COMMISSION_SIGNATURE.finditer(text) if match.start() > anchor
    ]
    if signature_matches:
        return signature_matches[0].start(), "commission_signature"

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


if len(sys.argv) != 3:
    raise RuntimeError(
        "Usage: python3 build_ulurp_cpc_narratives.py <start_year> <end_year>"
    )

start_year = int(sys.argv[1])
end_year = int(sys.argv[2])
if start_year > end_year:
    raise RuntimeError("Invalid text-measurement scalar arguments.")

manifest_real_path = Path("../input/ulurp_cpc_report_manifest.csv").resolve()
with Path("../input/ulurp_cpc_report_manifest.csv").open(
    newline="", encoding="utf-8"
) as input_file:
    source_rows = [
        row
        for row in csv.DictReader(input_file)
        if start_year <= int(row["official_vote_year"]) <= end_year
    ]

with Path("../input/ulurp_cpc_narrative_boundary_decisions.csv").open(
    newline="", encoding="utf-8"
) as input_file:
    boundary_exception_rows = list(csv.DictReader(input_file))
boundary_exceptions = {
    row["application_number"]: row
    for row in boundary_exception_rows
}
if len(boundary_exceptions) != len(boundary_exception_rows):
    raise RuntimeError("Narrative-boundary exceptions are not unique by application number.")

if len({row["document_id"] for row in source_rows}) != len(source_rows):
    raise RuntimeError("Official corpus manifest is not unique by document_id.")

rows = []
applied_boundary_exceptions = set()
for source_row in source_rows:
    text_path = resolve_task_path(source_row["local_text_path"], manifest_real_path)
    source_usable = source_row["source_usable"] == "TRUE"
    if source_usable and (
        source_row["text_status"] != "text_extracted"
        or text_path is None
        or not text_path.is_file()
    ):
        raise RuntimeError(f"Missing readable text for {source_row['application_number']}.")

    full_text = text_path.read_text(encoding="utf-8", errors="replace") if source_usable else ""
    source_text_hash = (
        hashlib.sha256(full_text.encode("utf-8")).hexdigest()
        if source_usable
        else ""
    )
    boundary_exception = boundary_exceptions.get(source_row["application_number"], {})
    if source_usable:
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
        narrative_end = 0
        boundary_method = "documented_source_unavailable"
        normalized_text = ""
        narrative_word_count = 0
        narrative_hash = ""
    project_name_key = normalized_project_name(source_row["official_project_name"])
    lead_group_key = (
        f"{source_row['official_vote_date']}|{project_name_key}"
        if source_usable and project_name_key
        else ""
    )

    rows.append(
        {
            "document_id": source_row["document_id"],
            "application_number": source_row["application_number"],
            "action_code": source_row["action_code"],
            "corpus_role": source_row["corpus_role"],
            "official_project_name": source_row["official_project_name"],
            "official_community_district": source_row["official_community_district"],
            "official_vote_date": source_row["official_vote_date"],
            "official_vote_year": source_row["official_vote_year"],
            "official_lead_report_flag": source_row["official_lead_report_flag"],
            "zap_project_ids": source_row["zap_project_ids"],
            "local_text_path": str(text_path) if text_path else "",
            "source_text_sha256": source_text_hash,
            "narrative_start_char": 0,
            "narrative_end_char": narrative_end,
            "narrative_boundary_method": boundary_method,
            "narrative_word_count": narrative_word_count,
            "narrative_sha256": narrative_hash,
            "lead_group_key": lead_group_key,
            "analysis_non_pp_flag": str(source_row["action_code"] != "PP").upper(),
            "analysis_zm_zr_zs_flag": str(source_row["action_code"] in {"ZM", "ZR", "ZS"}).upper(),
            "_manual_companion_application": boundary_exception.get(
                "analysis_narrative_representative_application", ""
            ),
        }
    )

unapplied_boundary_exceptions = set(boundary_exceptions) - applied_boundary_exceptions
if unapplied_boundary_exceptions:
    raise RuntimeError(
        "Unapplied narrative-boundary exceptions: "
        + "; ".join(sorted(unapplied_boundary_exceptions))
    )

lead_groups = defaultdict(list)
for row in rows:
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
            row["action_code"] in {"ZM", "ZR", "ZS"} for row in certified_group_rows
        )
        for row in group_rows:
            row["analysis_non_pp_flag"] = str(group_non_pp_flag).upper()
            row["analysis_zm_zr_zs_flag"] = str(group_zm_zr_zs_flag).upper()
            if row["official_lead_report_flag"] != "TRUE":
                related_to_lead.add(row["document_id"])

for row in rows:
    if row["_manual_companion_application"]:
        related_to_lead.add(row["document_id"])

eligible_rows = [
    row
    for row in rows
    if row["document_id"] not in related_to_lead
    and row["narrative_boundary_method"] != "full_text_no_boundary_found"
    and row["narrative_boundary_method"] not in MANUAL_EXCLUSION_METHODS
    and int(row["narrative_word_count"]) >= 100
]
exact_groups = defaultdict(list)
for row in eligible_rows:
    exact_groups[row["narrative_sha256"]].append(row)

representative_document_ids = set()
for group_rows in exact_groups.values():
    group_rows.sort(
        key=lambda row: (
            row["official_lead_report_flag"] != "TRUE",
            row["application_number"],
        )
    )
    representative_document_ids.add(group_rows[0]["document_id"])

manifest_fieldnames = [
    "document_id",
    "application_number",
    "action_code",
    "official_project_name",
    "official_community_district",
    "official_vote_year",
    "zap_project_ids",
    "local_text_path",
    "source_text_sha256",
    "narrative_start_char",
    "narrative_end_char",
    "narrative_boundary_method",
    "narrative_word_count",
    "narrative_sha256",
    "analysis_non_pp_flag",
    "analysis_zm_zr_zs_flag",
]
analysis_rows = [row for row in rows if row["document_id"] in representative_document_ids]
with Path("../temp/ulurp_cpc_narrative_manifest.csv").open(
    "w",
    newline="",
    encoding="utf-8",
) as output_file:
    writer = csv.DictWriter(
        output_file,
        fieldnames=manifest_fieldnames,
        extrasaction="ignore",
    )
    writer.writeheader()
    writer.writerows(analysis_rows)
print(
    f"Wrote {len(analysis_rows)} analysis narratives from {len(rows)} report rows."
)
