#!/usr/bin/env python3

import csv
import hashlib
import math
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

from label_ulurp_cpc_text_signal_kwic import label_row


# setwd("tasks/audits/audit_ulurp_cpc_text_signals/code")
# start_year = 1975
# end_year = 2025
# sample_documents_per_decade = 200
# boilerplate_doc_share = 0.05
# kwic_hits_per_rule = 50
# kwic_context_words = 45


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
    ("consideration_by_the_city_planning_commission", r"CONSIDERATION BY THE CITY PLANNING COMMISSION"),
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

SIGNAL_SECTION_ALLOWLIST = {
    "revision_concession": {
        "background",
        "environmental_review",
        "ulurp",
        "community_board",
        "borough_president",
        "cpc_hearing",
        "consideration_findings",
        "unsectioned",
    },
    "attribution_community_board": {
        "ulurp",
        "community_board",
        "borough_president",
        "cpc_hearing",
        "consideration_findings",
        "unsectioned",
    },
    "attribution_borough_president": {
        "ulurp",
        "community_board",
        "borough_president",
        "cpc_hearing",
        "consideration_findings",
        "unsectioned",
    },
    "attribution_council_member": {
        "ulurp",
        "community_board",
        "borough_president",
        "cpc_hearing",
        "consideration_findings",
        "unsectioned",
    },
    "attribution_civic_group": {
        "ulurp",
        "community_board",
        "borough_president",
        "cpc_hearing",
        "consideration_findings",
        "unsectioned",
    },
    "attribution_applicant": {
        "background",
        "ulurp",
        "community_board",
        "borough_president",
        "cpc_hearing",
        "consideration_findings",
        "unsectioned",
    },
    "attribution_unspecified": {
        "ulurp",
        "community_board",
        "borough_president",
        "cpc_hearing",
        "consideration_findings",
        "unsectioned",
    },
    "substantive_council_member": {
        "ulurp",
        "community_board",
        "borough_president",
        "cpc_hearing",
        "consideration_findings",
        "unsectioned",
    },
    "community_board_disapproval": {
        "ulurp",
        "community_board",
        "cpc_hearing",
        "consideration_findings",
        "unsectioned",
    },
    "community_board_conditioned_approval": {
        "ulurp",
        "community_board",
        "cpc_hearing",
        "consideration_findings",
        "unsectioned",
    },
    "opposition_any": {
        "community_board",
        "borough_president",
        "cpc_hearing",
        "consideration_findings",
        "unsectioned",
    },
    "opposition_traffic_parking": {
        "community_board",
        "borough_president",
        "cpc_hearing",
        "consideration_findings",
        "unsectioned",
    },
    "opposition_scale_character": {
        "community_board",
        "borough_president",
        "cpc_hearing",
        "consideration_findings",
        "unsectioned",
    },
    "opposition_displacement_affordability": {
        "community_board",
        "borough_president",
        "cpc_hearing",
        "consideration_findings",
        "unsectioned",
    },
    "opposition_infrastructure": {
        "community_board",
        "borough_president",
        "cpc_hearing",
        "consideration_findings",
        "unsectioned",
    },
}


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


def stable_hash(*values):
    return hashlib.sha256("|".join(str(value) for value in values).encode("utf-8")).hexdigest()


def decade_from_year(year):
    return f"{year // 10 * 10}s"


def resolve_task_path(raw_path, manifest_real_path):
    if not raw_path:
        return None
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return manifest_real_path.parent.parent / "code" / path


def readable_text_path(text_path):
    if text_path is None or not text_path.exists():
        return False
    stat_result = text_path.stat()
    if getattr(stat_result, "st_blocks", 1) == 0 and stat_result.st_size > 0:
        return False
    return stat_result.st_size >= 100


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

    return {section: normalize_whitespace("\n".join(parts.get(section, []))) for section in SECTION_ORDER}


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
    if is_council_filing_boilerplate(sentence):
        suppressed_council = True
    else:
        suppressed_council = False

    if is_special_permit_modification_boilerplate(sentence):
        suppressed_revision = True
    else:
        suppressed_revision = False

    signals = []
    for signal_family, pattern in SIGNAL_RULES:
        if signal_family in {"revision_concession"} and suppressed_revision:
            continue
        if signal_family in {"substantive_council_member", "attribution_council_member"} and suppressed_council:
            continue
        if pattern.search(sentence):
            signals.append(signal_family)

    if opposition_context(sentence):
        for signal_family, pattern in OPPOSITION_RULES:
            if pattern.search(sentence):
                signals.append(signal_family)

    return sorted(set(signals))


def trim_words(text, max_words):
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]).strip()


def sentence_context(document_sentences, index, context_words):
    start = max(0, index - 1)
    end = min(len(document_sentences), index + 2)
    context = " ".join(sentence["sentence"] for sentence in document_sentences[start:end])
    return trim_words(normalize_whitespace(context), context_words * 3)


def read_documents(start_year, end_year):
    documents = []
    manifest_real_path = Path("../input/official_ulurp_cpc_narrative_manifest.csv").resolve()

    with Path("../input/official_ulurp_cpc_narrative_manifest.csv").open(newline="", encoding="utf-8") as manifest_file:
        for row in csv.DictReader(manifest_file):
            year = as_int(row.get("official_vote_year"))
            if year is None or year < start_year or year > end_year:
                continue
            if row.get("analysis_narrative_unit_flag") != "TRUE":
                continue

            text_path = resolve_task_path(row.get("local_text_path", ""), manifest_real_path)
            if not readable_text_path(text_path):
                continue

            source_text = text_path.read_text(encoding="utf-8", errors="replace")
            narrative_start = as_int(row.get("narrative_start_char"))
            narrative_end = as_int(row.get("narrative_end_char"))
            if narrative_start is None or narrative_end is None or narrative_end <= narrative_start:
                continue
            text = source_text[narrative_start:narrative_end]
            if word_count(text) < 50:
                continue

            documents.append(
                {
                    "document_id": row.get("document_id", ""),
                    "project_id": row.get("lead_group_key", ""),
                    "project_name": row.get("official_project_name", ""),
                    "application_number": row.get("application_number", ""),
                    "parsed_action_code": row.get("action_code", ""),
                    "borough_name": "",
                    "community_district": row.get("official_community_district", ""),
                    "year": year,
                    "decade": decade_from_year(year),
                    "usable_text_status": "comparable_official_narrative",
                    "usable_text_source_type": "official_cpc_report",
                    "text": text,
                }
            )

    return documents


def build_sentence_rows(documents):
    sentence_rows = []
    sentence_doc_ids = defaultdict(set)
    sentence_examples = {}
    section_doc_words = defaultdict(int)
    document_section_sentences = defaultdict(list)

    for document in documents:
        for section, section_text in parse_sections(document["text"]).items():
            sentences = split_sentences(section_text)
            for sentence_index, sentence in enumerate(sentences):
                words = word_count(sentence)
                if words == 0:
                    continue
                sentence_position = len(document_section_sentences[(document["document_id"], section)])
                normalized_sentence = normalize_sentence_for_boilerplate(sentence)
                row = {
                    "document_id": document["document_id"],
                    "project_id": document["project_id"],
                    "project_name": document["project_name"],
                    "application_number": document["application_number"],
                    "parsed_action_code": document["parsed_action_code"],
                    "borough_name": document["borough_name"],
                    "community_district": document["community_district"],
                    "year": document["year"],
                    "decade": document["decade"],
                    "section": section,
                    "sentence_index": sentence_index,
                    "sentence_position": sentence_position,
                    "sentence": sentence,
                    "normalized_sentence": normalized_sentence,
                    "word_count": words,
                }
                sentence_rows.append(row)
                document_section_sentences[(document["document_id"], section)].append(row)

                if words >= 6 and normalized_sentence:
                    sentence_doc_ids[normalized_sentence].add(document["document_id"])
                    sentence_examples.setdefault(normalized_sentence, row)

    for row in sentence_rows:
        section_doc_words[(row["document_id"], row["section"])] += row["word_count"]

    return sentence_rows, sentence_doc_ids, sentence_examples, section_doc_words, document_section_sentences


def prepare_boilerplate_sentence_rows(sentence_doc_ids, sentence_examples, documents, boilerplate_doc_share):
    total_documents = len(documents)
    minimum_documents = max(2, math.ceil(total_documents * boilerplate_doc_share))
    rows = []

    for normalized_sentence, document_ids in sentence_doc_ids.items():
        document_count = len(document_ids)
        if document_count < minimum_documents:
            continue
        example = sentence_examples[normalized_sentence]
        rows.append(
            {
                "normalized_sentence": normalized_sentence,
                "document_count": document_count,
                "document_share": round(document_count / total_documents, 6),
                "example_year": example["year"],
                "example_application_number": example["application_number"],
                "example_section": example["section"],
                "example_sentence": example["sentence"],
            }
        )

    rows.sort(key=lambda row: (-row["document_count"], row["normalized_sentence"]))
    return rows, {row["normalized_sentence"] for row in rows}


def write_boilerplate_sentences(rows):
    with Path("../output/ulurp_cpc_text_boilerplate_sentences.csv").open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=[
                "normalized_sentence",
                "document_count",
                "document_share",
                "example_year",
                "example_application_number",
                "example_section",
                "example_sentence",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def prepare_signal_rows(sentence_rows, boilerplate_sentences, document_section_sentences, kwic_context_words):
    raw_signal_rows = []
    filtered_signal_rows = []
    for row in sentence_rows:
        is_boilerplate = (
            row["normalized_sentence"] in boilerplate_sentences
            or is_council_filing_boilerplate(row["sentence"])
            or is_special_permit_modification_boilerplate(row["sentence"])
        )
        row["is_boilerplate"] = is_boilerplate
        if is_boilerplate:
            row["signals"] = []
            continue
        row["signals"] = [
            signal_family
            for signal_family in sentence_signals(row["sentence"])
            if row["section"] in SIGNAL_SECTION_ALLOWLIST.get(signal_family, SECTION_ORDER)
        ]
        for signal_family in row["signals"]:
            context = sentence_context(
                document_section_sentences[(row["document_id"], row["section"])],
                row["sentence_position"],
                kwic_context_words,
            )
            signal_row = {**row, "signal_family": signal_family, "context": context}
            (
                signal_row["assistant_true_positive"],
                signal_row["assistant_confidence"],
                signal_row["assistant_reason"],
            ) = label_row(signal_row)
            raw_signal_rows.append(signal_row)
            if signal_row["assistant_true_positive"] == "1":
                filtered_signal_rows.append(signal_row)
    return raw_signal_rows, filtered_signal_rows


def aggregate_signal_rows(documents, sentence_rows, signal_rows, sample_document_ids=None, period_unit=None):
    if sample_document_ids is None:
        included_documents = {document["document_id"] for document in documents}
    else:
        included_documents = set(sample_document_ids)
    if period_unit is None:
        period_unit = "year" if sample_document_ids is None else "decade"

    words_by_period_section = Counter()
    docs_by_period_section = defaultdict(set)
    readable_docs_by_period = defaultdict(set)
    hits_by_period_section_signal = Counter()
    hit_docs_by_period_section_signal = defaultdict(set)

    for document in documents:
        if document["document_id"] not in included_documents:
            continue
        period = document[period_unit]
        readable_docs_by_period[period].add(document["document_id"])

    for row in sentence_rows:
        if row["document_id"] not in included_documents or row["is_boilerplate"]:
            continue
        period = row[period_unit]
        words_by_period_section[(period, row["section"])] += row["word_count"]
        words_by_period_section[(period, "all_sections")] += row["word_count"]
        docs_by_period_section[(period, row["section"])].add(row["document_id"])
        docs_by_period_section[(period, "all_sections")].add(row["document_id"])

    for row in signal_rows:
        if row["document_id"] not in included_documents:
            continue
        period = row[period_unit]
        for section in [row["section"], "all_sections"]:
            key = (period, section, row["signal_family"])
            hits_by_period_section_signal[key] += 1
            hit_docs_by_period_section_signal[key].add(row["document_id"])

    periods = sorted(readable_docs_by_period.keys())
    sections = ["all_sections"] + SECTION_ORDER
    signal_families = [signal_family for signal_family, _pattern in SIGNAL_RULES]
    signal_families.extend(signal_family for signal_family, _pattern in OPPOSITION_RULES)
    signal_families = sorted(set(signal_families))

    output_rows = []
    for period in periods:
        for section in sections:
            section_documents = len(docs_by_period_section[(period, section)])
            nonboilerplate_words = words_by_period_section[(period, section)]
            for signal_family in signal_families:
                key = (period, section, signal_family)
                hit_sentences = hits_by_period_section_signal[key]
                hit_documents = len(hit_docs_by_period_section_signal[key])
                if nonboilerplate_words > 0:
                    sentence_rate = hit_sentences / nonboilerplate_words * 1000
                else:
                    sentence_rate = 0
                if section_documents > 0:
                    document_share = hit_documents / section_documents
                else:
                    document_share = 0

                output_rows.append(
                    {
                        "period": period,
                        "section": section,
                        "signal_family": signal_family,
                        "readable_documents": len(readable_docs_by_period[period]),
                        "section_documents": section_documents,
                        "nonboilerplate_words": nonboilerplate_words,
                        "hit_sentences": hit_sentences,
                        "hit_documents": hit_documents,
                        "hit_sentences_per_1000_words": round(sentence_rate, 6),
                        "hit_document_share": round(document_share, 6),
                    }
                )

    return output_rows


def write_year_output(rows):
    with Path("../output/ulurp_cpc_text_signal_year.csv").open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=[
                "year",
                "section",
                "signal_family",
                "readable_documents",
                "section_documents",
                "nonboilerplate_words",
                "hit_sentences",
                "hit_documents",
                "hit_sentences_per_1000_words",
                "hit_document_share",
            ],
        )
        writer.writeheader()
        for row in rows:
            row = dict(row)
            row["year"] = row.pop("period")
            writer.writerow(row)


def write_document_output(documents, signal_rows):
    document_signals = defaultdict(set)
    for row in signal_rows:
        document_signals[row["document_id"]].add(row["signal_family"])

    fieldnames = [
        "document_id",
        "application_number",
        "action_code",
        "project_name",
        "community_district",
        "year",
        "decade",
        *SIGNAL_FAMILIES,
    ]

    with Path("../output/ulurp_cpc_text_signal_document.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        for document in sorted(documents, key=lambda row: (row["year"], row["document_id"])):
            writer.writerow(
                {
                    "document_id": document["document_id"],
                    "application_number": document["application_number"],
                    "action_code": document["parsed_action_code"],
                    "project_name": document["project_name"],
                    "community_district": document["community_district"],
                    "year": document["year"],
                    "decade": document["decade"],
                    **{
                        signal_family: int(
                            signal_family in document_signals[document["document_id"]]
                        )
                        for signal_family in SIGNAL_FAMILIES
                    },
                }
            )


def write_year_by_application_sample_output(documents, sentence_rows, signal_rows):
    application_samples = [
        ("all_reports", "All comparable official CPC narratives", lambda document: True),
        ("non_pp", "Comparable official CPC narratives excluding PP actions", lambda document: document["parsed_action_code"] != "PP"),
        (
            "zm_zr_zs",
            "Comparable official CPC narratives with ZM/ZR/ZS actions",
            lambda document: document["parsed_action_code"] in {"ZM", "ZR", "ZS"},
        ),
    ]

    output_rows = []
    for application_sample, application_sample_label, sample_filter in application_samples:
        sample_document_ids = {
            document["document_id"]
            for document in documents
            if sample_filter(document)
        }
        for row in aggregate_signal_rows(
            documents,
            sentence_rows,
            signal_rows,
            sample_document_ids,
            period_unit="year",
        ):
            row = dict(row)
            row["year"] = row.pop("period")
            row["application_sample"] = application_sample
            row["application_sample_label"] = application_sample_label
            output_rows.append(row)

    output_rows.sort(key=lambda row: (row["application_sample"], row["year"], row["section"], row["signal_family"]))
    with Path("../output/ulurp_cpc_text_signal_year_by_application_sample.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=[
                "application_sample",
                "application_sample_label",
                "year",
                "section",
                "signal_family",
                "readable_documents",
                "section_documents",
                "nonboilerplate_words",
                "hit_sentences",
                "hit_documents",
                "hit_sentences_per_1000_words",
                "hit_document_share",
            ],
        )
        writer.writeheader()
        writer.writerows(output_rows)


def select_decade_sample(documents, sample_documents_per_decade):
    documents_by_decade = defaultdict(list)
    for document in documents:
        documents_by_decade[document["decade"]].append(document)

    sample_ids = set()
    sample_counts = {}
    available_counts = {}
    for decade, decade_documents in documents_by_decade.items():
        available_counts[decade] = len(decade_documents)
        decade_documents = sorted(
            decade_documents,
            key=lambda document: stable_hash("decade_sample", decade, document["document_id"]),
        )
        selected = decade_documents[:sample_documents_per_decade]
        sample_counts[decade] = len(selected)
        sample_ids.update(document["document_id"] for document in selected)

    return sample_ids, sample_counts, available_counts


def write_decade_sample_output(rows, sample_counts, available_counts):
    with Path("../output/ulurp_cpc_text_signal_decade_sample.csv").open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=[
                "decade",
                "sample_documents",
                "available_documents",
                "section",
                "signal_family",
                "section_documents",
                "nonboilerplate_words",
                "hit_sentences",
                "hit_documents",
                "hit_sentences_per_1000_words",
                "hit_document_share",
            ],
        )
        writer.writeheader()
        for row in rows:
            row = dict(row)
            decade = row.pop("period")
            row["decade"] = decade
            row["sample_documents"] = sample_counts.get(decade, row.pop("readable_documents"))
            row["available_documents"] = available_counts.get(decade, "")
            row.pop("readable_documents", None)
            writer.writerow(row)


def write_kwic_sample(signal_rows, document_section_sentences, kwic_hits_per_rule, kwic_context_words):
    rows_by_signal = defaultdict(list)
    for row in signal_rows:
        rows_by_signal[row["signal_family"]].append(row)

    output_rows = []
    for signal_family, rows in rows_by_signal.items():
        rows = sorted(
            rows,
            key=lambda row: stable_hash(
                "kwic",
                signal_family,
                row["document_id"],
                row["section"],
                row["sentence_index"],
            ),
        )
        for row in rows[:kwic_hits_per_rule]:
            context = sentence_context(
                document_section_sentences[(row["document_id"], row["section"])],
                row["sentence_position"],
                kwic_context_words,
            )
            output_rows.append(
                {
                    "signal_family": signal_family,
                    "document_id": row["document_id"],
                    "project_id": row["project_id"],
                    "application_number": row["application_number"],
                    "project_name": row["project_name"],
                    "year": row["year"],
                    "decade": row["decade"],
                    "section": row["section"],
                    "sentence": row["sentence"],
                    "context": context,
                    "manual_true_positive": "",
                    "manual_reason": "",
                }
            )

    output_rows.sort(key=lambda row: (row["signal_family"], row["year"], row["document_id"]))
    with Path("../output/ulurp_cpc_text_signal_kwic_sample.csv").open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=[
                "signal_family",
                "document_id",
                "project_id",
                "application_number",
                "project_name",
                "year",
                "decade",
                "section",
                "sentence",
                "context",
                "manual_true_positive",
                "manual_reason",
            ],
        )
        writer.writeheader()
        writer.writerows(output_rows)


def main():
    if len(sys.argv) != 7:
        raise SystemExit(
            "Usage: audit_ulurp_cpc_text_signals.py "
            "START_YEAR END_YEAR SAMPLE_DOCUMENTS_PER_DECADE BOILERPLATE_DOC_SHARE "
            "KWIC_HITS_PER_RULE KWIC_CONTEXT_WORDS"
        )

    start_year = as_int(sys.argv[1])
    end_year = as_int(sys.argv[2])
    sample_documents_per_decade = as_int(sys.argv[3])
    boilerplate_doc_share = as_float(sys.argv[4])
    kwic_hits_per_rule = as_int(sys.argv[5])
    kwic_context_words = as_int(sys.argv[6])

    if start_year is None or end_year is None or sample_documents_per_decade is None:
        raise SystemExit("START_YEAR, END_YEAR, and SAMPLE_DOCUMENTS_PER_DECADE must be integers.")
    if end_year < start_year:
        raise SystemExit("END_YEAR must be greater than or equal to START_YEAR.")
    if boilerplate_doc_share is None or not 0 < boilerplate_doc_share < 1:
        raise SystemExit("BOILERPLATE_DOC_SHARE must be between 0 and 1.")
    if kwic_hits_per_rule is None or kwic_context_words is None:
        raise SystemExit("KWIC_HITS_PER_RULE and KWIC_CONTEXT_WORDS must be integers.")

    documents = read_documents(start_year, end_year)
    sentence_rows, sentence_doc_ids, sentence_examples, _section_doc_words, document_section_sentences = (
        build_sentence_rows(documents)
    )
    boilerplate_sentence_rows, boilerplate_sentences = prepare_boilerplate_sentence_rows(
        sentence_doc_ids,
        sentence_examples,
        documents,
        boilerplate_doc_share,
    )
    raw_signal_rows, filtered_signal_rows = prepare_signal_rows(
        sentence_rows,
        boilerplate_sentences,
        document_section_sentences,
        kwic_context_words,
    )

    write_year_output(aggregate_signal_rows(documents, sentence_rows, filtered_signal_rows))
    write_document_output(documents, filtered_signal_rows)
    write_year_by_application_sample_output(documents, sentence_rows, filtered_signal_rows)

    sample_ids, sample_counts, available_counts = select_decade_sample(
        documents,
        sample_documents_per_decade,
    )
    write_decade_sample_output(
        aggregate_signal_rows(documents, sentence_rows, filtered_signal_rows, sample_ids),
        sample_counts,
        available_counts,
    )
    write_kwic_sample(
        raw_signal_rows,
        document_section_sentences,
        kwic_hits_per_rule,
        kwic_context_words,
    )
    write_boilerplate_sentences(boilerplate_sentence_rows)


if __name__ == "__main__":
    main()
