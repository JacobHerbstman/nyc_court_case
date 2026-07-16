#!/usr/bin/env python3

import csv
import hashlib
import math
import re
import sys
from collections import defaultdict
from pathlib import Path

from ulurp_cpc_text_label_rules import (
    NO_OPPOSITION_PATTERN,
    OPPOSITION_CONTEXT_PATTERN,
    OPPOSITION_RULES,
    SIGNAL_FAMILIES,
    SIGNAL_RULES,
    SIGNAL_SECTION_ALLOWLIST,
    signal_is_positive,
)


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
    "borough_president": "borough_president",
    "borough_president_recommendation": "borough_president",
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
    ("community_board", r"COMMUNITY BOARD"),
    ("borough_president_recommendation", r"BOROUGH PRESIDENT(?:'S)? RECOMMENDATION"),
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
            "borough_president_recommendation",
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


def opposition_context(sentence):
    if NO_OPPOSITION_PATTERN.search(sentence):
        return False
    return OPPOSITION_CONTEXT_PATTERN.search(sentence) is not None


def sentence_signals(sentence):
    suppressed_council = is_council_filing_boilerplate(sentence)
    suppressed_revision = is_special_permit_modification_boilerplate(sentence)

    signals = []
    for signal_family, pattern in SIGNAL_RULES:
        if signal_family == "revision_concession" and suppressed_revision:
            continue
        if (
            signal_family
            in {"substantive_council_member", "attribution_council_member"}
            and suppressed_council
        ):
            continue
        if pattern.search(sentence):
            signals.append(signal_family)

    if opposition_context(sentence):
        for signal_family, pattern in OPPOSITION_RULES:
            if pattern.search(sentence):
                signals.append(signal_family)

    return sorted(set(signals))


def sentence_rule_text(document_sentences, index, context_words):
    start = max(0, index - 1)
    end = min(len(document_sentences), index + 2)
    context = " ".join(
        sentence["sentence"] for sentence in document_sentences[start:end]
    )
    context = " ".join(context.split()[:context_words])
    return normalize_whitespace(document_sentences[index]["sentence"] + " " + context)


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

document_signals = defaultdict(set)
for row in sentence_rows:
    if (
        row["normalized_sentence"] in boilerplate_sentences
        or is_council_filing_boilerplate(row["sentence"])
        or is_special_permit_modification_boilerplate(row["sentence"])
    ):
        continue

    signals = [
        signal_family
        for signal_family in sentence_signals(row["sentence"])
        if row["section"]
        in SIGNAL_SECTION_ALLOWLIST.get(signal_family, SECTION_ORDER)
    ]
    for signal_family in signals:
        rule_text = sentence_rule_text(
            document_section_sentences[(row["document_id"], row["section"])],
            row["sentence_position"],
            rule_context_words,
        )
        if signal_is_positive(
            rule_text,
            row["section"],
            signal_family,
        ):
            document_signals[row["document_id"]].add(signal_family)

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
    *SIGNAL_FAMILIES,
]
with Path("../temp/ulurp_cpc_text_labels.csv").open(
    "w",
    newline="",
    encoding="utf-8",
) as output_file:
    writer = csv.DictWriter(output_file, fieldnames=fieldnames)
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
                **{
                    signal_family: int(
                        signal_family
                        in document_signals[document["document_id"]]
                    )
                    for signal_family in SIGNAL_FAMILIES
                },
            }
        )

print(f"Wrote deterministic text labels for {len(documents)} CPC narratives.")
