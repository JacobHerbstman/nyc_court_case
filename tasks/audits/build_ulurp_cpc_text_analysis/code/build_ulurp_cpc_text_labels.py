#!/usr/bin/env python3

import csv
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


# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/build_ulurp_cpc_text_analysis/code")
# start_year = 1975
# end_year = 2025
# boilerplate_doc_share = 0.05
# rule_context_words = 135


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

documents = []
manifest_real_path = Path("../temp/ulurp_cpc_narrative_manifest.csv").resolve()
with Path("../temp/ulurp_cpc_narrative_manifest.csv").open(
    newline="",
    encoding="utf-8",
) as manifest_file:
    for row in csv.DictReader(manifest_file):
        year = as_int(row.get("official_vote_year"))
        if year is None or year < start_year or year > end_year:
            continue

        raw_text_path = row.get("local_text_path", "")
        text_path = Path(raw_text_path) if raw_text_path else None
        if text_path is not None and not text_path.is_absolute():
            text_path = manifest_real_path.parent.parent / "code" / text_path
        if text_path is None or not text_path.exists():
            raise RuntimeError(
                f"Unreadable text for analysis narrative {row.get('application_number', '')}."
            )
        text_stat = text_path.stat()
        if (
            text_stat.st_size < 100
            or (
                getattr(text_stat, "st_blocks", 1) == 0
                and text_stat.st_size > 0
            )
        ):
            raise RuntimeError(
                f"Unreadable text for analysis narrative {row.get('application_number', '')}."
            )

        source_text = text_path.read_text(encoding="utf-8", errors="replace")
        narrative_start = as_int(row.get("narrative_start_char"))
        narrative_end = as_int(row.get("narrative_end_char"))
        if (
            narrative_start is None
            or narrative_end is None
            or narrative_end <= narrative_start
        ):
            raise RuntimeError(
                f"Invalid narrative boundary for {row.get('application_number', '')}."
            )
        text = source_text[narrative_start:narrative_end]
        if word_count(text) < 50:
            raise RuntimeError(
                f"Short analysis narrative for {row.get('application_number', '')}."
            )

        documents.append(
            {
                "document_id": row.get("document_id", ""),
                "project_name": row.get("official_project_name", ""),
                "application_number": row.get("application_number", ""),
                "action_code": row.get("action_code", ""),
                "community_district": row.get("official_community_district", ""),
                "year": year,
                "decade": f"{year // 10 * 10}s",
                "source_text_sha256": row.get("source_text_sha256", ""),
                "narrative_sha256": row.get("narrative_sha256", ""),
                "narrative_word_count": row.get("narrative_word_count", ""),
                "narrative_boundary_method": row.get(
                    "narrative_boundary_method",
                    "",
                ),
                "zap_project_ids": row.get("zap_project_ids", ""),
                "analysis_non_pp_flag": row.get("analysis_non_pp_flag", ""),
                "analysis_zm_zr_zs_flag": row.get(
                    "analysis_zm_zr_zs_flag",
                    "",
                ),
                "text": text,
            }
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
with Path("../output/ulurp_cpc_text_labels.csv").open(
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
