#!/usr/bin/env python3

import csv
import hashlib
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path


# setwd("tasks/audits/audit_official_ulurp_cpc_text_measurement/code")
# start_year = 1975
# end_year = 2025
# documents_per_decade = 100


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


def write_csv(rows, fieldnames, path):
    with Path(path).open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def narrative_boundary(text):
    anchor_matches = list(ANCHOR_HEADING.finditer(text))
    if anchor_matches and anchor_matches[0].start() < 0.75 * len(text):
        anchor = anchor_matches[0].start()
    else:
        anchor = min(500, len(text))
    resolution_matches = [match for match in RESOLUTION_HEADING.finditer(text) if match.start() > anchor]
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

    signature_matches = [match for match in COMMISSION_SIGNATURE.finditer(text) if match.start() > anchor]
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


def stable_hash(*values):
    return hashlib.sha256("|".join(str(value) for value in values).encode("utf-8")).hexdigest()


def words_at_edge(text, side, count=80):
    words = clean_text(text).split()
    if side == "tail":
        return " ".join(words[-count:])
    return " ".join(words[:count])


if len(sys.argv) != 4:
    raise RuntimeError(
        "Usage: python3 audit_official_ulurp_cpc_text_measurement.py "
        "<start_year> <end_year> <documents_per_decade>"
    )

start_year = int(sys.argv[1])
end_year = int(sys.argv[2])
documents_per_decade = int(sys.argv[3])
if start_year > end_year or documents_per_decade < 1:
    raise RuntimeError("Invalid text-measurement scalar arguments.")

manifest_real_path = Path("../input/official_ulurp_cpc_report_manifest.csv").resolve()
with Path("../input/official_ulurp_cpc_report_manifest.csv").open(
    newline="", encoding="utf-8"
) as input_file:
    source_rows = [
        row
        for row in csv.DictReader(input_file)
        if start_year <= int(row["official_vote_year"]) <= end_year
    ]

with Path("../input/official_ulurp_cpc_narrative_boundary_exceptions.csv").open(
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
    pdf_path = resolve_task_path(source_row["local_pdf_path"], manifest_real_path)
    source_usable = source_row["source_usable"] == "TRUE"
    if source_usable and (
        source_row["text_status"] != "text_extracted"
        or text_path is None
        or not text_path.is_file()
    ):
        raise RuntimeError(f"Missing readable text for {source_row['application_number']}.")

    full_text = text_path.read_text(encoding="utf-8", errors="replace") if source_usable else ""
    source_text_hash = hashlib.sha256(full_text.encode("utf-8")).hexdigest() if source_usable else ""
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
        excluded_text = full_text[narrative_end:]
        normalized_text = normalize_narrative(narrative_text)
        narrative_word_count = len(re.findall(r"\b[\w'-]+\b", normalized_text))
        narrative_hash = hashlib.sha256(normalized_text.encode("utf-8")).hexdigest()
    else:
        narrative_end = 0
        boundary_method = "documented_source_unavailable"
        narrative_text = ""
        excluded_text = ""
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
            "application_key": source_row["application_key"],
            "action_code": source_row["action_code"],
            "corpus_role": source_row["corpus_role"],
            "source_usable": source_row["source_usable"],
            "official_project_name": source_row["official_project_name"],
            "official_community_district": source_row["official_community_district"],
            "official_vote_date": source_row["official_vote_date"],
            "official_vote_year": source_row["official_vote_year"],
            "official_lead_report_flag": source_row["official_lead_report_flag"],
            "official_pdf_url": source_row["resolved_pdf_url"],
            "local_pdf_path": str(pdf_path) if pdf_path else "",
            "local_text_path": str(text_path) if text_path else "",
            "source_text_sha256": source_text_hash,
            "narrative_start_char": 0,
            "narrative_end_char": narrative_end,
            "full_text_char_count": len(full_text),
            "excluded_text_char_count": len(excluded_text),
            "narrative_boundary_method": boundary_method,
            "narrative_boundary_review_id": boundary_exception.get("boundary_review_id", ""),
            "narrative_boundary_review_reason": boundary_exception.get("boundary_reason", ""),
            "narrative_word_count": narrative_word_count,
            "narrative_sha256": narrative_hash,
            "lead_group_key": lead_group_key,
            "lead_group_size": 1,
            "exact_narrative_group_size": 1,
            "analysis_narrative_representative_application": "",
            "analysis_narrative_unit_flag": "FALSE",
            "analysis_narrative_unit_reason": "",
            "certified_report_flag": str(source_row["corpus_role"] == "certified_ulurp_report").upper(),
            "analysis_non_pp_flag": str(source_row["action_code"] != "PP").upper(),
            "analysis_zm_zr_zs_flag": str(source_row["action_code"] in {"ZM", "ZR", "ZS"}).upper(),
            "_manual_companion_application": boundary_exception.get(
                "analysis_narrative_representative_application", ""
            ),
            "_normalized_narrative": normalized_text,
            "_narrative_text": narrative_text,
            "_excluded_text": excluded_text,
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

related_to_lead = {}
for group_key, group_rows in lead_groups.items():
    lead_rows = [row for row in group_rows if row["official_lead_report_flag"] == "TRUE"]
    if len(group_rows) > 1 and lead_rows:
        certified_group_rows = [
            row for row in group_rows if row["corpus_role"] == "certified_ulurp_report"
        ]
        group_non_pp_flag = any(row["action_code"] != "PP" for row in certified_group_rows)
        group_zm_zr_zs_flag = any(
            row["action_code"] in {"ZM", "ZR", "ZS"} for row in certified_group_rows
        )
        lead_applications = "; ".join(
            sorted(row["application_number"] for row in lead_rows)
        )
        for row in group_rows:
            row["lead_group_size"] = len(group_rows)
            row["analysis_non_pp_flag"] = str(group_non_pp_flag).upper()
            row["analysis_zm_zr_zs_flag"] = str(group_zm_zr_zs_flag).upper()
            if row["official_lead_report_flag"] != "TRUE":
                related_to_lead[row["document_id"]] = lead_applications

for row in rows:
    if row["_manual_companion_application"]:
        related_to_lead[row["document_id"]] = row["_manual_companion_application"]

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

exact_representative = {}
for narrative_hash, group_rows in exact_groups.items():
    group_rows.sort(
        key=lambda row: (
            row["official_lead_report_flag"] != "TRUE",
            row["application_number"],
        )
    )
    representative = group_rows[0]["application_number"]
    for row in group_rows:
        row["exact_narrative_group_size"] = len(group_rows)
        exact_representative[row["document_id"]] = representative

for row in rows:
    if row["narrative_boundary_method"] == "documented_source_unavailable":
        row["analysis_narrative_unit_reason"] = "documented_source_unavailable"
        continue
    if row["narrative_boundary_method"] == "exclude_incomplete_source":
        row["analysis_narrative_unit_reason"] = "incomplete_source"
        continue
    if row["narrative_boundary_method"] == "exclude_supplemental_statement_without_main_report":
        row["analysis_narrative_unit_reason"] = "supplemental_statement_without_main_report"
        continue
    if row["narrative_boundary_method"] == "exclude_related_action_covered_by_companion":
        row["analysis_narrative_representative_application"] = row[
            "_manual_companion_application"
        ]
        row["analysis_narrative_unit_reason"] = "related_action_of_manual_companion"
        continue
    if row["narrative_boundary_method"] == "full_text_no_boundary_found":
        row["analysis_narrative_unit_reason"] = "unresolved_narrative_boundary"
        continue
    if int(row["narrative_word_count"]) < 100:
        row["analysis_narrative_unit_reason"] = "short_or_empty_narrative"
        continue
    if row["document_id"] in related_to_lead:
        row["analysis_narrative_representative_application"] = related_to_lead[row["document_id"]]
        row["analysis_narrative_unit_reason"] = "related_action_of_designated_lead"
        continue

    representative = exact_representative[row["document_id"]]
    row["analysis_narrative_representative_application"] = representative
    if row["application_number"] != representative:
        row["analysis_narrative_unit_reason"] = "exact_duplicate_narrative"
        continue

    row["analysis_narrative_unit_flag"] = "TRUE"
    row["analysis_narrative_unit_reason"] = "included_unique_narrative"

manifest_fieldnames = [
    "document_id",
    "application_number",
    "application_key",
    "action_code",
    "corpus_role",
    "source_usable",
    "official_project_name",
    "official_community_district",
    "official_vote_date",
    "official_vote_year",
    "official_lead_report_flag",
    "official_pdf_url",
    "local_pdf_path",
    "local_text_path",
    "source_text_sha256",
    "narrative_start_char",
    "narrative_end_char",
    "full_text_char_count",
    "excluded_text_char_count",
    "narrative_boundary_method",
    "narrative_boundary_review_id",
    "narrative_boundary_review_reason",
    "narrative_word_count",
    "narrative_sha256",
    "lead_group_key",
    "lead_group_size",
    "exact_narrative_group_size",
    "analysis_narrative_representative_application",
    "analysis_narrative_unit_flag",
    "analysis_narrative_unit_reason",
    "certified_report_flag",
    "analysis_non_pp_flag",
    "analysis_zm_zr_zs_flag",
]
write_csv(
    rows,
    manifest_fieldnames,
    "../output/official_ulurp_cpc_narrative_manifest.csv",
)

analysis_rows = [row for row in rows if row["analysis_narrative_unit_flag"] == "TRUE"]
sample_rows = []
by_decade = defaultdict(list)
for row in analysis_rows:
    decade = int(row["official_vote_year"]) // 10 * 10
    by_decade[decade].append(row)

for decade, decade_rows in sorted(by_decade.items()):
    decade_rows.sort(key=lambda row: stable_hash("boundary_sample", row["document_id"]))
    for sample_number, row in enumerate(decade_rows[:documents_per_decade], start=1):
        sample_rows.append(
            {
                "sample_id": f"{decade}s_{sample_number:03d}",
                "document_id": row["document_id"],
                "application_number": row["application_number"],
                "official_vote_year": row["official_vote_year"],
                "decade": f"{decade}s",
                "action_code": row["action_code"],
                "official_project_name": row["official_project_name"],
                "official_pdf_url": row["official_pdf_url"],
                "local_pdf_path": row["local_pdf_path"],
                "narrative_boundary_method": row["narrative_boundary_method"],
                "narrative_word_count": row["narrative_word_count"],
                "narrative_tail": words_at_edge(row["_narrative_text"], "tail"),
                "excluded_text_prefix": words_at_edge(row["_excluded_text"], "head"),
                "manual_boundary_correct": "",
                "manual_substantive_narrative": "",
                "manual_notes": "",
            }
        )

sample_fieldnames = list(sample_rows[0].keys())
write_csv(
    sample_rows,
    sample_fieldnames,
    "../output/official_ulurp_cpc_narrative_boundary_sample.csv",
)

boundary_counts = Counter(row["narrative_boundary_method"] for row in rows)
reason_counts = Counter(row["analysis_narrative_unit_reason"] for row in rows)
summary_rows = [
    {
        "metric": "official_source_rows",
        "value": len(rows),
        "note": "Corrected certified CPC reports plus separately identified related narrative leads.",
    },
    {
        "metric": "certified_ulurp_report_rows",
        "value": sum(row["corpus_role"] == "certified_ulurp_report" for row in rows),
        "note": "Certified C reports; supplemental N lead reports do not increase this count.",
    },
    {
        "metric": "related_project_narrative_lead_rows",
        "value": sum(row["corpus_role"] == "related_project_narrative_lead" for row in rows),
        "note": "N lead reports retained only to supply the project narrative for certified actions.",
    },
    {
        "metric": "documented_source_unavailable_rows",
        "value": reason_counts["documented_source_unavailable"],
        "note": "Certified action retained in the universe but excluded from text measurement.",
    },
    {
        "metric": "reports_with_comparable_boundary_and_100_words",
        "value": sum(
            row["narrative_boundary_method"] != "full_text_no_boundary_found"
            and row["narrative_boundary_method"] not in MANUAL_EXCLUSION_METHODS
            and int(row["narrative_word_count"]) >= 100
            for row in rows
        ),
        "note": "Reports suitable for narrative measurement before unit collapsing.",
    },
    {
        "metric": "analysis_narrative_units",
        "value": len(analysis_rows),
        "note": "Unique narratives after DCP lead-group and exact-text collapsing.",
    },
    {
        "metric": "designated_lead_related_actions_excluded",
        "value": reason_counts["related_action_of_designated_lead"],
        "note": "Non-lead actions sharing a named project and vote date with one DCP lead report.",
    },
    {
        "metric": "exact_duplicate_narratives_excluded",
        "value": reason_counts["exact_duplicate_narrative"],
        "note": "Exact normalized narrative duplicates among otherwise eligible reports.",
    },
    {
        "metric": "short_or_empty_narratives_excluded",
        "value": reason_counts["short_or_empty_narrative"],
        "note": "Fewer than 100 words before the resolution boundary.",
    },
    {
        "metric": "unresolved_narrative_boundaries_excluded",
        "value": reason_counts["unresolved_narrative_boundary"],
        "note": "Reports retained in the source universe but withheld from text measurement.",
    },
    {
        "metric": "manual_narrative_boundary_decisions",
        "value": len(applied_boundary_exceptions),
        "note": "Hash-locked report-specific decisions preserved in the record-only validation task.",
    },
    {
        "metric": "incomplete_sources_excluded",
        "value": reason_counts["incomplete_source"],
        "note": "Rendered source ends before CPC consideration or decision.",
    },
    {
        "metric": "supplemental_statements_without_main_report_excluded",
        "value": reason_counts["supplemental_statement_without_main_report"],
        "note": "Official PDF contains a commissioner statement but not the majority report.",
    },
    {
        "metric": "manual_companion_actions_excluded",
        "value": reason_counts["related_action_of_manual_companion"],
        "note": "Report explicitly delegates substantive analysis to the named companion report.",
    },
    {
        "metric": "resolution_heading_boundaries",
        "value": boundary_counts["resolution_heading"],
        "note": "Narrative ends at the first resolution or resolved heading after a review anchor.",
    },
    {
        "metric": "filing_paragraph_boundaries",
        "value": boundary_counts["filing_paragraph"],
        "note": "Fallback narrative boundary at the CPC filing paragraph.",
    },
    {
        "metric": "adopted_resolution_paragraph_boundaries",
        "value": boundary_counts["adopted_resolution_paragraph"],
        "note": "Fallback boundary where CPC introduces the resolution it adopted.",
    },
    {
        "metric": "commission_signature_boundaries",
        "value": boundary_counts["commission_signature"],
        "note": "Fallback narrative boundary at a CPC chair or vice-chair signature block.",
    },
    {
        "metric": "full_text_without_boundary",
        "value": boundary_counts["full_text_no_boundary_found"],
        "note": "Requires manual inspection before substantive measurement.",
    },
    {
        "metric": "boundary_sample_rows",
        "value": len(sample_rows),
        "note": "Deterministic sample capped at the requested number per vote decade.",
    },
]
for decade in sorted(by_decade):
    summary_rows.append(
        {
            "metric": f"analysis_narrative_units_{decade}s",
            "value": len(by_decade[decade]),
            "note": "Comparable narrative units by official CPC vote decade.",
        }
    )

write_csv(
    summary_rows,
    ["metric", "value", "note"],
    "../output/official_ulurp_cpc_text_measurement_summary.csv",
)

print(
    f"Wrote {len(rows)} report rows, {len(analysis_rows)} analysis narrative units, "
    f"and {len(sample_rows)} boundary sample rows."
)
