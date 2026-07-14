#!/usr/bin/env python3

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_official_ulurp_cpc_corpus/code")
# start_year = 1975
# end_year = 2025
# documents_per_decade = 100

from __future__ import annotations

import csv
import calendar
import hashlib
import re
import statistics
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


PROJECT_STOPWORDS = {
    "and", "application", "avenue", "building", "city", "development", "for", "land", "new",
    "of", "project", "rezoning", "street", "the", "use", "york",
}
COUNCIL_C_PATTERN = re.compile(
    r"\bC\s+(\d{6}(?:\s*\(?[A-Z]\)?)?)\s+([A-Z]{3,5})\b",
    re.IGNORECASE,
)


def clean_text(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def stable_hash(*values):
    return hashlib.sha256("|".join(clean_text(value) for value in values).encode("utf-8")).hexdigest()


def indexed_application_key(value):
    return re.sub(r"[^A-Z0-9]", "", clean_text(value).upper())


def application_key(value):
    compact = indexed_application_key(value)
    if re.match(r"^[A-Z]\d{6}", compact):
        compact = compact[1:]
    return compact


def application_stem(value):
    match = re.match(r"^(?:[A-Z])?(\d{6})", indexed_application_key(value))
    return match.group(1) if match else ""


def base_application_key(value):
    key = application_key(value)
    match = re.match(r"^(\d{6})[A-Z]?([A-Z]{3,5})$", key)
    return f"{match.group(1)}{match.group(2)}" if match else key


def application_action_suffix(value):
    match = re.match(r"^\d{6}(?:[A-Z])?([A-Z]{3,5})$", application_key(value))
    return match.group(1) if match else ""


def project_key(value):
    return re.sub(r"[^a-z0-9]+", " ", clean_text(value).lower()).strip()


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as input_file:
        while True:
            block = input_file.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def resolve_task_path(raw_path, manifest_real_path):
    if not clean_text(raw_path):
        return None
    path = Path(clean_text(raw_path))
    if path.is_absolute():
        return path
    return manifest_real_path.parent.parent / "code" / path


def file_status(path, pdf=False):
    if path is None or not path.exists():
        return "missing"
    stat_result = path.stat()
    if getattr(stat_result, "st_blocks", 1) == 0 and stat_result.st_size > 0:
        return "dataless"
    if stat_result.st_size == 0:
        return "empty"
    if pdf:
        with path.open("rb") as input_file:
            if input_file.read(4) != b"%PDF":
                return "not_pdf"
    return "readable"


def word_count(text):
    return len(re.findall(r"[A-Za-z0-9$]+(?:[-'][A-Za-z0-9]+)?", text))


def application_range_contains_number(text, expected_stem):
    source = text[:5000].replace("\u2013", "-").replace("\u2014", "-")
    target = int(expected_stem)
    for match in re.finditer(r"(?<!\d)(\d{6})\s*-\s*([0-9]{1,6})(?!\d)", source):
        start = int(match.group(1))
        suffix = match.group(2)
        end = int(suffix if len(suffix) == 6 else match.group(1)[: 6 - len(suffix)] + suffix)
        if start <= target <= end and end - start <= 1000:
            return True
    return False


def application_range_contains(text, expected_stem, expected_action_suffix):
    source = text[:5000].replace("\u2013", "-").replace("\u2014", "-")
    target = int(expected_stem)
    for match in re.finditer(r"(?<!\d)(\d{6})\s*-\s*([0-9]{1,6})(?!\d)", source):
        start = int(match.group(1))
        suffix = match.group(2)
        end = int(suffix if len(suffix) == 6 else match.group(1)[: 6 - len(suffix)] + suffix)
        nearby_text = re.sub(r"[^A-Z0-9]", "", source[match.end(): match.end() + 80].upper())
        if start <= target <= end and end - start <= 1000 and expected_action_suffix in nearby_text:
            return True
    return False


def vote_date_in_header(text, vote_date):
    parsed_date = datetime.strptime(vote_date, "%m/%d/%Y")
    header = text[:3000]
    month_names = [calendar.month_name[parsed_date.month], calendar.month_abbr[parsed_date.month]]
    return any(
        re.search(
            rf"\b{re.escape(month_name)}\.?\s+0?{parsed_date.day}\s*,?\s+{parsed_date.year}\b",
            header,
            re.IGNORECASE,
        )
        for month_name in month_names
    ) or vote_date in header


def council_application_keys(title):
    return {
        application_key(f"C {match.group(1)} {match.group(2)}")
        for match in COUNCIL_C_PATTERN.finditer(clean_text(title))
    }


def project_token_overlap(project_name, text):
    tokens = {
        token.lower()
        for token in re.findall(r"[A-Za-z0-9]+", clean_text(project_name))
        if len(token) >= 4 and token.lower() not in PROJECT_STOPWORDS
    }
    if not tokens:
        return None
    text_lower = text.lower()
    return sum(token in text_lower for token in tokens) / len(tokens)


def percentile(values, probability):
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def write_csv(rows, fieldnames, path):
    with Path(path).open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main():
    if len(sys.argv) != 4:
        raise RuntimeError(
            "Usage: python3 audit_official_ulurp_cpc_corpus.py "
            "<start_year> <end_year> <documents_per_decade>"
        )
    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    documents_per_decade = int(sys.argv[3])
    if start_year > end_year or documents_per_decade < 1:
        raise RuntimeError("Invalid audit scalar arguments.")

    manifest_path = Path("../input/official_ulurp_cpc_report_manifest.csv")
    manifest_real_path = manifest_path.resolve()
    with manifest_path.open(newline="", encoding="utf-8") as input_file:
        manifest_rows = [
            row for row in csv.DictReader(input_file)
            if start_year <= int(row["official_vote_year"]) <= end_year
        ]
    with Path("../input/official_cpc_report_index.csv").open(newline="", encoding="utf-8") as input_file:
        official_index_rows = [
            row for row in csv.DictReader(input_file)
            if start_year <= int(row["vote_year"]) <= end_year
        ]
    with Path("../input/official_ulurp_cpc_source_corrections.csv").open(newline="", encoding="utf-8") as input_file:
        source_correction_rows = list(csv.DictReader(input_file))
    with Path("../input/official_ulurp_cpc_index_additions.csv").open(newline="", encoding="utf-8") as input_file:
        index_addition_rows = [
            row for row in csv.DictReader(input_file)
            if start_year <= int(row["vote_year"]) <= end_year
        ]
    with Path("../input/official_ulurp_cpc_external_reference_exclusions.csv").open(newline="", encoding="utf-8") as input_file:
        external_reference_exclusion_rows = list(csv.DictReader(input_file))
    with Path("../input/official_ulurp_cpc_source_exception_labels.csv").open(newline="", encoding="utf-8") as input_file:
        source_exception_label_rows = list(csv.DictReader(input_file))
    with Path("../input/official_ulurp_cpc_short_page_validation.csv").open(newline="", encoding="utf-8") as input_file:
        short_page_validation_rows = list(csv.DictReader(input_file))
    with Path("../input/ulurp_corpus_application_spine.csv").open(newline="", encoding="utf-8") as input_file:
        zap_application_rows = [
            row for row in csv.DictReader(input_file)
            if start_year <= int(row["corpus_reference_year"]) <= end_year
            and row["application_prefix"] == "C"
        ]
    with Path("../input/council_land_use_decision_panel.csv").open(newline="", encoding="utf-8") as input_file:
        council_rows = list(csv.DictReader(input_file))
    external_reference_exclusions = {
        application_key(row["external_application_number"]): row
        for row in external_reference_exclusion_rows
    }
    if len(external_reference_exclusions) != len(external_reference_exclusion_rows):
        raise RuntimeError("External reference exclusions must have unique application numbers.")
    if any(
        row["reference_source"] not in {"council", "zap"}
        for row in external_reference_exclusion_rows
    ):
        raise RuntimeError("External reference sources must be council or zap.")
    council_reference_exclusions = {
        key: row
        for key, row in external_reference_exclusions.items()
        if row["reference_source"] == "council"
    }
    zap_reference_exclusions = {
        key: row
        for key, row in external_reference_exclusions.items()
        if row["reference_source"] == "zap"
    }
    source_exception_labels = {}
    for row in source_exception_label_rows:
        application_numbers = [
            indexed_application_key(value)
            for value in row["application_numbers"].split(";")
            if clean_text(value)
        ]
        if len(application_numbers) != int(row["application_count"]):
            raise RuntimeError(f"Application count mismatch in {row['source_review_id']}.")
        for application_number in application_numbers:
            key = (row["text_sha256"], application_number)
            if key in source_exception_labels:
                raise RuntimeError(f"Duplicate source-exception decision for {application_number}.")
            source_exception_labels[key] = row
    short_page_validations = {
        (row["text_sha256"], row["short_page_numbers"], row["pdf_page_count"]): row
        for row in short_page_validation_rows
    }
    if len(short_page_validations) != len(short_page_validation_rows):
        raise RuntimeError("Short-page validations must be unique by text hash and page signature.")

    metrics_rows = []
    pdf_hash_groups = defaultdict(list)
    text_hash_groups = defaultdict(list)
    for row_number, row in enumerate(manifest_rows, start=1):
        pdf_path = resolve_task_path(row["local_pdf_path"], manifest_real_path)
        text_path = resolve_task_path(row["local_text_path"], manifest_real_path)
        pdf_status = file_status(pdf_path, pdf=True)
        text_status = file_status(text_path)
        pdf_hash = sha256_file(pdf_path) if pdf_status == "readable" else ""
        text_hash = sha256_file(text_path) if text_status == "readable" else ""
        if pdf_hash:
            pdf_hash_groups[pdf_hash].append(row)
        if text_hash:
            text_hash_groups[text_hash].append(row)

        text = text_path.read_text(encoding="utf-8", errors="replace") if text_status == "readable" else ""
        compact_text = re.sub(r"[^A-Z0-9]", "", text.upper())
        compact_header_text = re.sub(r"[^A-Z0-9]", "", text[:5000].upper())
        overlap = project_token_overlap(row["official_project_name"], text)
        expected_application_stem = application_stem(row["application_number"])
        expected_action_suffix = application_action_suffix(row["application_number"])
        expected_full_identifier = indexed_application_key(row["application_number"])
        first_page_text = text.split("\f", 1)[0]
        page_texts = text.split("\f")
        page_word_counts = [word_count(page_text) for page_text in page_texts]
        leading_image_only_page_run = 0
        if len(page_word_counts) >= 4 and page_word_counts[0] >= 20 and page_word_counts[1] < 20:
            run_end = 1
            while run_end < len(page_word_counts) and page_word_counts[run_end] < 20:
                run_end += 1
            if run_end - 1 >= 2 and sum(page_word_counts[run_end:]) >= 50:
                leading_image_only_page_run = run_end - 1
        partial_ocr_pages = [
            int(value)
            for value in clean_text(row.get("partial_ocr_pages")).split(";")
            if clean_text(value).isdigit()
        ]
        skipped_ocr_pages = [
            int(value)
            for value in clean_text(row.get("skipped_ocr_pages")).split(";")
            if clean_text(value).isdigit()
        ]
        short_text_pages = [
            int(value)
            for value in clean_text(row.get("short_text_pages_after_ocr")).split(";")
            if clean_text(value).isdigit()
        ]
        manifest_pdf_page_count = int(row["pdf_page_count"]) if clean_text(row["pdf_page_count"]) else 0
        stored_text_page_count = len(page_texts)
        if (
            stored_text_page_count == manifest_pdf_page_count + 1
            and not clean_text(page_texts[-1])
        ):
            stored_text_page_count -= 1
        ascii_character_share = sum(
            character in "\n\r\t\f" or 32 <= ord(character) <= 126
            for character in text
        ) / max(len(text), 1)
        metrics_rows.append(
            {
                "document_id": row["document_id"],
                "application_number": row["application_number"],
                "official_index_application_number": row["official_index_application_number"],
                "application_key": row["application_key"],
                "action_code": row["action_code"],
                "corpus_role": row["corpus_role"],
                "source_usable": row["source_usable"],
                "official_index_row_flag": row["official_index_row_flag"],
                "official_project_name": row["official_project_name"],
                "official_community_district": row["official_community_district"],
                "official_index_vote_date": row.get("official_index_vote_date", ""),
                "official_vote_date": row["official_vote_date"],
                "official_vote_year": row["official_vote_year"],
                "decade": f"{int(row['official_vote_year']) // 10 * 10}s",
                "official_lead_report_flag": row["official_lead_report_flag"],
                "official_pdf_url": row["official_pdf_url"],
                "resolved_pdf_url": row["resolved_pdf_url"],
                "source_correction_type": row["source_correction_type"],
                "source_correction_reason": row["source_correction_reason"],
                "zap_project_ids": row["zap_project_ids"],
                "pdf_source": row["pdf_source"],
                "text_method": row["text_method"],
                "pdf_file_status": pdf_status,
                "text_file_status": text_status,
                "pdf_size_bytes": pdf_path.stat().st_size if pdf_status == "readable" else "",
                "text_size_bytes": text_path.stat().st_size if text_status == "readable" else "",
                "pdf_sha256": pdf_hash,
                "text_sha256": text_hash,
                "text_word_count": word_count(text),
                "first_page_word_count": word_count(first_page_text),
                "partial_ocr_page_count": len(partial_ocr_pages),
                "interior_partial_ocr_page_count": sum(page_number > 1 for page_number in partial_ocr_pages),
                "skipped_ocr_page_count": len(skipped_ocr_pages),
                "short_text_page_count": len(short_text_pages),
                "short_text_page_numbers": "; ".join(str(page) for page in short_text_pages),
                "manifest_pdf_page_count": manifest_pdf_page_count,
                "stored_text_page_count": stored_text_page_count if text_status == "readable" else 0,
                "main_report_resolution_page": row.get("main_report_resolution_page", ""),
                "unresolved_leading_image_only_page_run": leading_image_only_page_run,
                "ascii_character_share": round(ascii_character_share, 6),
                "full_application_identifier_in_header": str(
                    expected_full_identifier in compact_header_text
                ).upper(),
                "application_stem_in_header": str(
                    expected_application_stem in compact_header_text
                ).upper(),
                "application_stem_in_text": str(expected_application_stem in compact_text).upper(),
                "application_range_in_header": str(
                    application_range_contains_number(text, expected_application_stem)
                ).upper(),
                "application_range_and_action_in_header": str(
                    application_range_contains(
                        text,
                        expected_application_stem,
                        expected_action_suffix,
                    )
                ).upper(),
                "official_vote_date_in_header": str(
                    vote_date_in_header(text, row["official_vote_date"])
                ).upper(),
                "cpc_phrase_in_text": str("CITY PLANNING COMMISSION" in text.upper()).upper(),
                "ulurp_phrase_in_text": str("UNIFORM LAND USE REVIEW" in text.upper()).upper(),
                "project_name_token_overlap": "" if overlap is None else round(overlap, 4),
                "replacement_character_count": text.count("\ufffd"),
                "local_pdf_path": str(pdf_path) if pdf_path else "",
                "local_text_path": str(text_path) if text_path else "",
            }
        )
        if row_number == 1 or row_number % 1000 == 0 or row_number == len(manifest_rows):
            print(f"Audited {row_number}/{len(manifest_rows)} official corpus files", flush=True)

    source_exception_rows = []
    for row in metrics_rows:
        if row["source_usable"] != "TRUE":
            row["source_identity_status"] = "documented_source_unavailable"
        elif row["full_application_identifier_in_header"] == "TRUE":
            row["source_identity_status"] = "exact_full_application_identifier"
            continue
        elif row["application_range_and_action_in_header"] == "TRUE":
            row["source_identity_status"] = "explicit_application_range_and_action"
            continue
        elif (
            row["application_stem_in_header"] == "TRUE"
            and row["official_vote_date_in_header"] == "TRUE"
            and row["cpc_phrase_in_text"] == "TRUE"
        ):
            row["source_identity_status"] = "application_stem_vote_date_and_cpc_header"
            continue
        elif (
            row["application_range_in_header"] == "TRUE"
            and row["official_vote_date_in_header"] == "TRUE"
            and row["cpc_phrase_in_text"] == "TRUE"
        ):
            row["source_identity_status"] = "application_range_vote_date_and_cpc_header"
            continue
        else:
            source_label = source_exception_labels.get(
                (row["text_sha256"], indexed_application_key(row["application_number"])),
                {},
            )
            if source_label.get("source_validation_decision") == "pass":
                row["source_identity_status"] = "reviewed_source_exception"
            else:
                row["source_identity_status"] = "unresolved_source_exception"
        source_label = source_exception_labels.get(
            (row["text_sha256"], indexed_application_key(row["application_number"])),
            {},
        )
        source_exception_rows.append(
            {
                "document_id": row["document_id"],
                "application_number": row["application_number"],
                "corpus_role": row["corpus_role"],
                "source_usable": row["source_usable"],
                "official_project_name": row["official_project_name"],
                "official_vote_date": row["official_vote_date"],
                "action_code": row["action_code"],
                "text_sha256": row["text_sha256"],
                "source_identity_status": row["source_identity_status"],
                "full_application_identifier_in_header": row["full_application_identifier_in_header"],
                "application_stem_in_header": row["application_stem_in_header"],
                "application_range_in_header": row["application_range_in_header"],
                "application_range_and_action_in_header": row["application_range_and_action_in_header"],
                "official_vote_date_in_header": row["official_vote_date_in_header"],
                "source_review_id": source_label.get("source_review_id", ""),
                "source_validation_method": source_label.get("source_validation_method", ""),
                "source_validation_reason": source_label.get("source_validation_reason", ""),
                "header_excerpt": source_label.get("header_excerpt", ""),
                "official_pdf_url": row["official_pdf_url"],
                "local_pdf_path": row["local_pdf_path"],
                "local_text_path": row["local_text_path"],
            }
        )
    write_csv(
        source_exception_rows,
        [
            "document_id", "application_number", "corpus_role", "source_usable",
            "official_project_name", "official_vote_date", "action_code", "text_sha256",
            "source_identity_status", "full_application_identifier_in_header",
            "application_stem_in_header", "application_range_in_header",
            "application_range_and_action_in_header", "official_vote_date_in_header", "source_review_id",
            "source_validation_method", "source_validation_reason", "header_excerpt",
            "official_pdf_url", "local_pdf_path", "local_text_path",
        ],
        "../output/official_ulurp_cpc_corpus_source_exceptions.csv",
    )

    duplicate_rows = []
    for content_type, hash_groups in (("pdf", pdf_hash_groups), ("text", text_hash_groups)):
        for content_hash, rows in hash_groups.items():
            if len(rows) < 2:
                continue
            duplicate_rows.append(
                {
                    "content_type": content_type,
                    "content_sha256": content_hash,
                    "row_count": len(rows),
                    "application_numbers": "; ".join(sorted(row["application_number"] for row in rows)),
                    "official_project_names": "; ".join(sorted({row["official_project_name"] for row in rows})),
                    "official_pdf_urls": "; ".join(sorted({row["official_pdf_url"] for row in rows})),
                }
            )
    duplicate_rows.sort(key=lambda row: (row["content_type"], -row["row_count"], row["content_sha256"]))
    write_csv(
        duplicate_rows,
        ["content_type", "content_sha256", "row_count", "application_numbers", "official_project_names", "official_pdf_urls"],
        "../output/official_ulurp_cpc_corpus_duplicate_content.csv",
    )

    by_year_rows = []
    rows_by_year = defaultdict(list)
    for row in metrics_rows:
        rows_by_year[int(row["official_vote_year"])].append(row)
    for year, rows in sorted(rows_by_year.items()):
        certified_rows = [row for row in rows if row["corpus_role"] == "certified_ulurp_report"]
        readable_certified_rows = [row for row in certified_rows if row["text_file_status"] == "readable"]
        narrative_lead_rows = [row for row in rows if row["corpus_role"] == "related_project_narrative_lead"]
        word_counts = [row["text_word_count"] for row in readable_certified_rows]
        overlaps = [
            float(row["project_name_token_overlap"])
            for row in readable_certified_rows
            if row["project_name_token_overlap"] != ""
        ]
        by_year_rows.append(
            {
                "official_vote_year": year,
                "certified_report_count": len(certified_rows),
                "readable_certified_report_count": len(readable_certified_rows),
                "related_narrative_lead_count": len(narrative_lead_rows),
                "official_index_omission_count": sum(row["official_index_row_flag"] == "FALSE" for row in rows),
                "recovered_pdf_count": sum("download" in row["pdf_source"] for row in rows),
                "ocr_text_count": sum("ocr" in row["text_method"] for row in readable_certified_rows),
                "full_application_identifier_in_header_share": round(
                    sum(row["full_application_identifier_in_header"] == "TRUE" for row in readable_certified_rows)
                    / len(readable_certified_rows),
                    4,
                ) if readable_certified_rows else "",
                "vote_date_in_header_share": round(
                    sum(row["official_vote_date_in_header"] == "TRUE" for row in readable_certified_rows)
                    / len(readable_certified_rows),
                    4,
                ) if readable_certified_rows else "",
                "cpc_phrase_in_text_share": round(sum(row["cpc_phrase_in_text"] == "TRUE" for row in readable_certified_rows) / len(readable_certified_rows), 4) if readable_certified_rows else "",
                "ulurp_phrase_in_text_share": round(sum(row["ulurp_phrase_in_text"] == "TRUE" for row in readable_certified_rows) / len(readable_certified_rows), 4) if readable_certified_rows else "",
                "text_word_count_p10": round(percentile(word_counts, 0.1), 1),
                "text_word_count_median": round(statistics.median(word_counts), 1),
                "text_word_count_p90": round(percentile(word_counts, 0.9), 1),
                "project_name_token_overlap_median": round(statistics.median(overlaps), 4) if overlaps else "",
            }
        )
    write_csv(
        by_year_rows,
        list(by_year_rows[0].keys()),
        "../output/official_ulurp_cpc_corpus_by_year.csv",
    )

    metrics_by_decade = defaultdict(list)
    for row in metrics_rows:
        if row["corpus_role"] == "certified_ulurp_report" and row["text_file_status"] == "readable":
            metrics_by_decade[row["decade"]].append(row)
    sample_rows = []
    for decade, rows in sorted(metrics_by_decade.items()):
        selected = sorted(rows, key=lambda row: stable_hash("official_document_sample", row["document_id"]))[:documents_per_decade]
        for row in selected:
            row["sample_scope"] = "certified_random_sample"
    for row in metrics_rows:
        if row["corpus_role"] == "related_project_narrative_lead" and row["text_file_status"] == "readable":
            row["sample_scope"] = "all_related_narrative_leads"

    sample_selection = sorted(
        [row for row in metrics_rows if row.get("sample_scope")],
        key=lambda row: (int(row["official_vote_year"]), row["sample_scope"], row["application_number"]),
    )
    for row in sample_selection:
        pdfinfo = subprocess.run(
            ["pdfinfo", row["local_pdf_path"]],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        page_match = re.search(r"^Pages:\s+([0-9]+)", pdfinfo.stdout, re.MULTILINE)
        fresh_text = subprocess.run(
            ["pdftotext", "-layout", "-enc", "UTF-8", row["local_pdf_path"], "-"],
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
        stored_text = Path(row["local_text_path"]).read_text(encoding="utf-8", errors="replace")
        normalized_stored_text = re.sub(r"\s+", " ", stored_text).strip()
        normalized_fresh_text = re.sub(r"\s+", " ", fresh_text.stdout).strip()
        sample_rows.append(
            {
                **row,
                "sample_scope": row["sample_scope"],
                "pdfinfo_status": "success" if pdfinfo.returncode == 0 else "failed",
                "pdf_page_count": page_match.group(1) if page_match else "",
                "fresh_pdftotext_status": "success" if fresh_text.returncode == 0 else "failed",
                "fresh_pdftotext_word_count": word_count(fresh_text.stdout),
                "stored_text_equals_fresh_pdftotext": str(normalized_stored_text == normalized_fresh_text).upper(),
                "manual_application_matches_pdf": "",
                "manual_project_matches_pdf": "",
                "manual_vote_date_matches_pdf": "",
                "manual_text_matches_pdf": "",
                "manual_readability": "",
                "manual_notes": "",
            }
        )
    write_csv(
        sample_rows,
        list(sample_rows[0].keys()),
        "../output/official_ulurp_cpc_corpus_document_sample.csv",
    )

    manifest_by_application_key = defaultdict(list)
    official_index_by_application_key = defaultdict(list)
    official_index_by_stem = defaultdict(list)
    for row in metrics_rows:
        manifest_by_application_key[application_key(row["application_number"])].append(row)
    for row in official_index_rows:
        official_index_by_application_key[application_key(row["application_number"])].append(row)
        official_index_by_stem[application_stem(row["application_number"])].append(row)
    index_addition_keys = {
        application_key(row["application_number"])
        for row in index_addition_rows
    }

    council_references = defaultdict(list)
    for row in council_rows:
        decision_date = clean_text(row.get("decision_date"))
        if decision_date:
            try:
                if datetime.strptime(decision_date, "%m/%d/%Y").year > end_year:
                    continue
            except ValueError:
                pass
        for key in council_application_keys(row.get("title")):
            council_references[key].append(row)

    council_benchmark_rows = []
    for key, rows in sorted(council_references.items()):
        manifest_matches = manifest_by_application_key.get(key, [])
        exact_index_matches = official_index_by_application_key.get(key, [])
        stem_index_matches = official_index_by_stem.get(key[:6], [])
        manifest_roles = {row["corpus_role"] for row in manifest_matches}
        external_review = council_reference_exclusions.get(key, {})
        if "certified_ulurp_report" in manifest_roles:
            match_status = "matched_certified_report"
        elif "related_project_narrative_lead" in manifest_roles:
            match_status = "matched_related_narrative_lead"
        elif external_review:
            match_status = f"reviewed_{external_review['review_decision']}"
        elif exact_index_matches:
            exact_prefixes = {
                indexed_application_key(row["application_number"])[0]
                for row in exact_index_matches
            }
            if "N" in exact_prefixes:
                match_status = "council_c_reference_to_official_n_matter"
            elif "C" in exact_prefixes:
                match_status = "indexed_c_record_excluded_after_source_review"
            else:
                match_status = "matched_other_official_cpc_matter"
        elif stem_index_matches:
            match_status = "same_stem_different_action_or_transcription"
        else:
            match_status = "no_official_index_stem"

        council_benchmark_rows.append(
            {
                "council_application_key": key,
                "council_application_stem": key[:6],
                "match_status": match_status,
                "recovered_index_omission_flag": str(key in index_addition_keys).upper(),
                "council_matter_count": len(rows),
                "council_matter_files": "; ".join(sorted({clean_text(row.get("matter_file")) for row in rows if clean_text(row.get("matter_file"))})),
                "council_decision_dates": "; ".join(sorted({clean_text(row.get("decision_date")) for row in rows if clean_text(row.get("decision_date"))})),
                "council_disposition_groups": "; ".join(sorted({clean_text(row.get("disposition_group")) for row in rows if clean_text(row.get("disposition_group"))})),
                "section_197_c_in_title": str(any(re.search(r"197\s*-?\s*c", clean_text(row.get("title")), re.IGNORECASE) for row in rows)).upper(),
                "ulurp_in_title": str(any("ULURP" in clean_text(row.get("title")).upper() for row in rows)).upper(),
                "manifest_application_numbers": "; ".join(sorted({row["application_number"] for row in manifest_matches})),
                "manifest_corpus_roles": "; ".join(sorted(manifest_roles)),
                "official_index_application_numbers": "; ".join(sorted({row["application_number"] for row in exact_index_matches})),
                "same_stem_official_index_application_numbers": "; ".join(sorted({row["application_number"] for row in stem_index_matches})),
                "external_review_decision": external_review.get("review_decision", ""),
                "external_review_canonical_application_number": external_review.get("canonical_application_number", ""),
                "external_review_reason": external_review.get("decision_reason", ""),
                "external_review_evidence_url": external_review.get("evidence_url", ""),
                "example_council_title": clean_text(rows[0].get("title")),
                "council_matter_urls": "; ".join(sorted({clean_text(row.get("matter_url")) for row in rows if clean_text(row.get("matter_url"))})),
            }
        )
    write_csv(
        council_benchmark_rows,
        list(council_benchmark_rows[0].keys()),
        "../output/official_ulurp_cpc_council_benchmark.csv",
    )

    certified_metrics_by_key = defaultdict(list)
    certified_metrics_by_base_key = defaultdict(list)
    certified_metrics_by_zap_project = defaultdict(list)
    for row in metrics_rows:
        if row["corpus_role"] != "certified_ulurp_report":
            continue
        certified_metrics_by_key[application_key(row["application_number"])].append(row)
        certified_metrics_by_base_key[base_application_key(row["application_number"])].append(row)
        for project_id in clean_text(row["zap_project_ids"]).split("; "):
            if project_id:
                certified_metrics_by_zap_project[project_id].append(row)

    source_correction_by_application_key = {
        application_key(row["raw_application_number"]): row
        for row in source_correction_rows
    }
    text_cache = {}
    zap_benchmark_rows = []
    for row in zap_application_rows:
        key = application_key(row["raw_application_number"])
        exact_matches = certified_metrics_by_key.get(key, [])
        base_matches = certified_metrics_by_base_key.get(base_application_key(key), [])
        project_matches = certified_metrics_by_zap_project.get(row["project_id"], [])
        source_correction = source_correction_by_application_key.get(key, {})
        external_review = zap_reference_exclusions.get(key, {})
        project_text_mentions_stem = False
        if project_matches:
            stem = application_stem(key)
            for match in project_matches:
                text_path = clean_text(match["local_text_path"])
                if not text_path:
                    continue
                if text_path not in text_cache:
                    text_cache[text_path] = Path(text_path).read_text(
                        encoding="utf-8",
                        errors="replace",
                    )
                if stem in re.sub(r"[^A-Z0-9]", "", text_cache[text_path].upper()):
                    project_text_mentions_stem = True
                    break

        if exact_matches:
            match_status = "exact_certified_report_match"
        elif source_correction.get("include_in_corpus") == "0":
            match_status = "reviewed_official_record_exclusion"
        elif external_review:
            match_status = f"reviewed_{external_review['review_decision']}"
        elif base_matches:
            match_status = "amendment_or_base_identifier_match"
        elif project_matches and project_text_mentions_stem:
            match_status = "same_project_report_mentions_application_stem"
        elif project_matches:
            match_status = "same_project_report_without_application_stem"
        elif row["project_status"] != "Complete" or row["public_status"] != "Completed":
            match_status = "not_final_by_end_year"
        else:
            match_status = "completed_zap_application_without_cpc_report_match"

        if match_status == "exact_certified_report_match":
            continue
        zap_benchmark_rows.append(
            {
                "project_id": row["project_id"],
                "raw_application_number": row["raw_application_number"],
                "application_key": key,
                "project_name": row["project_name"],
                "project_status": row["project_status"],
                "public_status": row["public_status"],
                "corpus_reference_date": row["corpus_reference_date"],
                "certified_referred_date": row["certified_referred_date"],
                "completed_date": row["completed_date"],
                "match_status": match_status,
                "official_application_numbers": "; ".join(
                    sorted({
                        match["application_number"]
                        for match in exact_matches + base_matches + project_matches
                    })
                ),
                "same_project_report_mentions_application_stem": str(
                    project_text_mentions_stem
                ).upper(),
                "source_correction_type": source_correction.get("correction_type", ""),
                "source_correction_reason": source_correction.get("correction_reason", ""),
                "external_review_decision": external_review.get("review_decision", ""),
                "external_review_canonical_application_number": external_review.get(
                    "canonical_application_number",
                    "",
                ),
                "external_review_reason": external_review.get("decision_reason", ""),
                "external_review_evidence_url": external_review.get("evidence_url", ""),
                "project_page_url": row["project_page_url"],
            }
        )
    zap_benchmark_rows.sort(
        key=lambda row: (
            row["match_status"],
            row["corpus_reference_date"],
            row["project_id"],
            row["raw_application_number"],
        )
    )
    write_csv(
        zap_benchmark_rows,
        list(zap_benchmark_rows[0].keys()),
        "../output/official_ulurp_cpc_zap_residual.csv",
    )

    status_counts = Counter()
    for row in metrics_rows:
        status_counts[("pdf", row["pdf_file_status"])] += 1
        status_counts[("text", row["text_file_status"])] += 1
    source_identity_counts = Counter(row["source_identity_status"] for row in metrics_rows)
    certified_metrics_rows = [row for row in metrics_rows if row["corpus_role"] == "certified_ulurp_report"]
    narrative_lead_metrics_rows = [row for row in metrics_rows if row["corpus_role"] == "related_project_narrative_lead"]
    usable_metrics_rows = [row for row in metrics_rows if row["source_usable"] == "TRUE"]
    short_page_group_keys = {
        (
            row["text_sha256"],
            row["short_text_page_numbers"],
            str(row["manifest_pdf_page_count"]),
        )
        for row in usable_metrics_rows
        if row["short_text_page_count"] > 0
    }
    unreviewed_short_page_groups = short_page_group_keys - set(short_page_validations)
    stale_short_page_validations = set(short_page_validations) - short_page_group_keys
    failed_short_page_validations = {
        key
        for key in short_page_group_keys & set(short_page_validations)
        if short_page_validations[key]["validation_decision"] != "pass_legitimately_sparse"
    }
    manifest_raw_index_keys = {
        indexed_application_key(row["official_index_application_number"])
        for row in metrics_rows
        if row["official_index_application_number"]
    }
    unapplied_source_corrections = 0
    for correction in source_correction_rows:
        raw_key = indexed_application_key(correction["raw_application_number"])
        should_be_included = correction["include_in_corpus"] == "1"
        unapplied_source_corrections += should_be_included != (raw_key in manifest_raw_index_keys)

    source_corrections = {
        indexed_application_key(row["raw_application_number"]): row
        for row in source_correction_rows
    }
    corrected_index_rows = []
    for row in official_index_rows:
        correction = source_corrections.get(indexed_application_key(row["application_number"]), {})
        corrected_index_rows.append(
            {
                **row,
                "canonical_application_number": correction.get("canonical_application_number") or row["application_number"],
                "canonical_vote_date": correction.get("canonical_vote_date") or row["vote_date"],
                "source_correction": correction,
            }
        )
    expected_index_certified_rows = [
        row for row in corrected_index_rows
        if re.match(r"^C\d{6}", indexed_application_key(row["canonical_application_number"]))
        and row["source_correction"].get("include_in_corpus", "1") == "1"
    ]
    expected_certified_project_votes = {
        (project_key(row["project_name"]), row["canonical_vote_date"])
        for row in expected_index_certified_rows
        if project_key(row["project_name"])
    }
    expected_index_narrative_leads = [
        row for row in corrected_index_rows
        if row["source_correction"].get("include_in_corpus") != "0"
        and re.match(r"^N\d{6}", indexed_application_key(row["canonical_application_number"]))
        and (
            row["source_correction"].get("corpus_role") == "related_project_narrative_lead"
            or (
                row["lead_report_flag"] == "TRUE"
                and (project_key(row["project_name"]), row["canonical_vote_date"]) in expected_certified_project_votes
            )
        )
    ]
    expected_certified_rows = len(expected_index_certified_rows) + sum(
        row["corpus_role"] == "certified_ulurp_report" for row in index_addition_rows
    )
    expected_narrative_leads = len(expected_index_narrative_leads) + sum(
        row["corpus_role"] == "related_project_narrative_lead" for row in index_addition_rows
    )
    raw_c_index_rows = sum(
        bool(re.match(r"^C\d{6}", indexed_application_key(row["application_number"])))
        for row in official_index_rows
    )
    council_status_counts = Counter(row["match_status"] for row in council_benchmark_rows)
    council_reference_keys = {row["council_application_key"] for row in council_benchmark_rows}
    zap_reference_keys = {
        application_key(row["raw_application_number"])
        for row in zap_application_rows
    }
    unapplied_external_reference_exclusions = 0
    for key, decision in external_reference_exclusions.items():
        expected_reference_keys = (
            council_reference_keys
            if decision["reference_source"] == "council"
            else zap_reference_keys
        )
        if key not in expected_reference_keys:
            unapplied_external_reference_exclusions += 1
        canonical_number = decision.get("canonical_application_number", "")
        if (
            canonical_number
            and indexed_application_key(canonical_number).startswith("C")
            and application_key(canonical_number) not in manifest_by_application_key
        ):
            unapplied_external_reference_exclusions += 1
    zap_status_counts = Counter(row["match_status"] for row in zap_benchmark_rows)
    council_no_stem_final_rows = [
        row for row in council_benchmark_rows
        if row["match_status"] == "no_official_index_stem"
        and any(
            disposition in {"adopted", "disapproved"}
            for disposition in row["council_disposition_groups"].split("; ")
        )
    ]
    summary_rows = [
        {"metric": "raw_official_c_prefixed_index_rows", "value": raw_c_index_rows, "status": "informational", "note": "Raw exact C-plus-six-digit rows returned by the official CPC Reports index before printed-report corrections."},
        {"metric": "source_correction_decision_rows", "value": len(source_correction_rows), "status": "informational", "note": "Preserved identifier, link, classification, duplicate, and availability decisions."},
        {"metric": "official_index_addition_rows", "value": len(index_addition_rows), "status": "informational", "note": "Verified CPC reports or certified actions recovered from official DCP, ZAP, and Council records but absent from the DCP search index."},
        {"metric": "index_omission_source_unavailable_rows", "value": sum(row.get("source_usable", "1") == "0" for row in index_addition_rows), "status": "informational", "note": "Externally verified certified actions absent from the DCP index whose authentic CPC report is not posted."},
        {"metric": "reviewed_external_reference_exclusions", "value": len(external_reference_exclusion_rows), "status": "informational", "note": "Council and ZAP references preserved as transcription errors, withdrawals, or matters outside CPC ULURP report scope."},
        {"metric": "unapplied_external_reference_exclusions", "value": unapplied_external_reference_exclusions, "status": "pass" if unapplied_external_reference_exclusions == 0 else "fail", "note": "Every external exclusion must match its Council or ZAP reference and every corrected certified C identifier must resolve to the corpus."},
        {"metric": "unapplied_source_correction_rows", "value": unapplied_source_corrections, "status": "pass" if unapplied_source_corrections == 0 else "fail", "note": "Every recorded inclusion or exclusion decision must agree with the built manifest."},
        {"metric": "certified_ulurp_report_rows", "value": len(certified_metrics_rows), "status": "pass" if len(certified_metrics_rows) == expected_certified_rows else "fail", "note": "Certified C reports after documented index exclusions and additions; supplemental N leads are not counted."},
        {"metric": "related_project_narrative_lead_rows", "value": len(narrative_lead_metrics_rows), "status": "pass" if len(narrative_lead_metrics_rows) == expected_narrative_leads else "fail", "note": "Separately identified N lead reports carrying narrative for paired certified applications."},
        {"metric": "unique_application_numbers", "value": len({row["application_number"] for row in metrics_rows}), "status": "pass" if len({row["application_number"] for row in metrics_rows}) == len(metrics_rows) else "fail", "note": "Canonical application numbers must identify one corpus row."},
        {"metric": "documented_source_unavailable_rows", "value": source_identity_counts["documented_source_unavailable"], "status": "informational", "note": "Certified actions retained in the universe but withheld from text analysis because the authentic report is unavailable."},
        {"metric": "readable_pdf_rows_among_usable_sources", "value": sum(row["pdf_file_status"] == "readable" for row in usable_metrics_rows), "status": "pass" if all(row["pdf_file_status"] == "readable" for row in usable_metrics_rows) else "fail", "note": "Every source declared usable must resolve to a real local PDF."},
        {"metric": "readable_text_rows_among_usable_sources", "value": sum(row["text_file_status"] == "readable" for row in usable_metrics_rows), "status": "pass" if all(row["text_file_status"] == "readable" for row in usable_metrics_rows) else "fail", "note": "Every source declared usable must resolve to real local extracted text."},
        {"metric": "dataless_pdf_rows", "value": status_counts[("pdf", "dataless")], "status": "pass" if status_counts[("pdf", "dataless")] == 0 else "fail", "note": "Cloud placeholders are not readable PDFs."},
        {"metric": "dataless_text_rows", "value": status_counts[("text", "dataless")], "status": "pass" if status_counts[("text", "dataless")] == 0 else "fail", "note": "Cloud placeholders are not readable text."},
        {"metric": "source_identity_exact_full_identifier_rows", "value": source_identity_counts["exact_full_application_identifier"], "status": "informational", "note": "Opening text contains the full prefix, number, action, and borough identifier."},
        {"metric": "source_identity_explicit_range_and_action_rows", "value": source_identity_counts["explicit_application_range_and_action"], "status": "informational", "note": "Grouped report range contains the number and is followed by the expected action code."},
        {"metric": "source_identity_reviewed_exception_rows", "value": source_identity_counts["reviewed_source_exception"], "status": "informational", "note": "OCR-damaged or historically formatted reports covered by preserved hash-level source review."},
        {"metric": "source_identity_unresolved_rows", "value": source_identity_counts["unresolved_source_exception"], "status": "pass" if source_identity_counts["unresolved_source_exception"] == 0 else "fail", "note": "Every row requires exact, range-and-action, reviewed, or documented-unavailable source evidence."},
        {"metric": "official_index_vote_date_not_found_in_opening_text", "value": sum(row["official_vote_date_in_header"] != "TRUE" for row in usable_metrics_rows), "status": "informational", "note": "The DCP index labels this field Vote Date. Missing or conflicting printed dates flag OCR and report-metadata disagreement but do not override the index date."},
        {"metric": "stored_text_first_page_under_20_words", "value": sum(row["first_page_word_count"] < 20 for row in usable_metrics_rows), "status": "pass" if all(row["first_page_word_count"] >= 20 for row in usable_metrics_rows) else "fail", "note": "Hybrid PDFs need first-page OCR rather than a whole-document word threshold."},
        {"metric": "partial_page_ocr_report_rows", "value": sum(row["partial_ocr_page_count"] > 0 for row in usable_metrics_rows), "status": "informational", "note": "Reports with one or more image-only pages repaired by targeted OCR."},
        {"metric": "report_pages_repaired_by_ocr", "value": sum(row["partial_ocr_page_count"] for row in usable_metrics_rows), "status": "informational", "note": "All sparse PDF pages where OCR recovered more text than the embedded layer."},
        {"metric": "interior_report_pages_repaired_by_ocr", "value": sum(row["interior_partial_ocr_page_count"] for row in usable_metrics_rows), "status": "informational", "note": "Image-only pages after page one recovered in otherwise hybrid PDFs."},
        {"metric": "ocr_page_timeouts_or_failures", "value": sum(row["skipped_ocr_page_count"] for row in usable_metrics_rows), "status": "pass" if all(row["skipped_ocr_page_count"] == 0 for row in usable_metrics_rows) else "fail", "note": "Every targeted page must render and complete OCR within the configured timeout."},
        {"metric": "stored_text_pdf_page_count_mismatches", "value": sum(row["stored_text_page_count"] != row["manifest_pdf_page_count"] for row in usable_metrics_rows), "status": "pass" if all(row["stored_text_page_count"] == row["manifest_pdf_page_count"] for row in usable_metrics_rows) else "fail", "note": "Stored text retains one form-feed segment per PDF page including sparse pages."},
        {"metric": "distinct_short_main_report_page_groups", "value": len(short_page_group_keys), "status": "informational", "note": "Distinct text-hash and page-signature combinations still below the OCR word threshold."},
        {"metric": "unreviewed_short_main_report_page_groups", "value": len(unreviewed_short_page_groups), "status": "pass" if not unreviewed_short_page_groups else "fail", "note": "Every still-short main-report page group requires a preserved rendered-page decision."},
        {"metric": "failed_short_main_report_page_validations", "value": len(failed_short_page_validations), "status": "pass" if not failed_short_page_validations else "fail", "note": "Reviewed sparse-page groups must be confirmed as legitimate non-prose or short pages."},
        {"metric": "stale_short_main_report_page_validations", "value": len(stale_short_page_validations), "status": "pass" if not stale_short_page_validations else "fail", "note": "Hash-locked sparse-page decisions must correspond to the current extraction."},
        {"metric": "unresolved_leading_image_only_report_runs", "value": sum(row["unresolved_leading_image_only_page_run"] > 0 for row in usable_metrics_rows), "status": "pass" if all(row["unresolved_leading_image_only_page_run"] == 0 for row in usable_metrics_rows) else "fail", "note": "A readable first page cannot be followed by multiple blank pages and then readable appendices."},
        {"metric": "stored_text_ascii_character_share_below_075", "value": sum(row["ascii_character_share"] < 0.75 for row in usable_metrics_rows), "status": "pass" if all(row["ascii_character_share"] >= 0.75 for row in usable_metrics_rows) else "fail", "note": "Severely corrupted embedded font maps are not usable extracted text."},
        {"metric": "pdf_duplicate_content_groups", "value": sum(row["content_type"] == "pdf" for row in duplicate_rows), "status": "informational", "note": "Reviewed grouped CPC reports; report rows remain in the source universe and narratives are collapsed before text measurement."},
        {"metric": "text_duplicate_content_groups", "value": sum(row["content_type"] == "text" for row in duplicate_rows), "status": "informational", "note": "Repeated grouped-report texts remain in the source universe and exact narratives are collapsed before text measurement."},
        {"metric": "certified_document_sample_rows", "value": sum(row["sample_scope"] == "certified_random_sample" for row in sample_rows), "status": "informational", "note": "Stable 100-per-decade source-reading sample of readable certified reports."},
        {"metric": "related_narrative_leads_in_document_sample", "value": sum(row["sample_scope"] == "all_related_narrative_leads" for row in sample_rows), "status": "pass" if sum(row["sample_scope"] == "all_related_narrative_leads" for row in sample_rows) == len(narrative_lead_metrics_rows) else "fail", "note": "Every supplemental narrative lead is included for source review."},
        {"metric": "sample_fresh_pdftotext_failures", "value": sum(row["fresh_pdftotext_status"] != "success" for row in sample_rows), "status": "pass" if all(row["fresh_pdftotext_status"] == "success" for row in sample_rows) else "fail", "note": "Independent fresh text extraction from sampled PDFs."},
        {"metric": "sample_stored_text_differs_from_fresh_pdftotext", "value": sum(row["stored_text_equals_fresh_pdftotext"] != "TRUE" for row in sample_rows), "status": "informational", "note": "Expected when stored text includes targeted or whole-document OCR."},
        {"metric": "unique_explicit_council_c_references", "value": len(council_benchmark_rows), "status": "informational", "note": "Independent Council Legistar references explicitly printed with a C prefix through the end year."},
        {"metric": "council_references_matched_to_certified_reports", "value": council_status_counts["matched_certified_report"], "status": "informational", "note": "Council references matched to the corrected certified-report corpus."},
        {"metric": "council_references_recovered_as_index_omissions", "value": sum(row["recovered_index_omission_flag"] == "TRUE" for row in council_benchmark_rows), "status": "informational", "note": "External benchmark references that led to verified additions absent from the DCP search index."},
        {"metric": "unreviewed_council_references_without_any_official_index_stem", "value": council_status_counts["no_official_index_stem"], "status": "pass" if council_status_counts["no_official_index_stem"] == 0 else "fail", "note": "Every Council reference without a DCP index stem must be an addition or have a preserved exclusion decision."},
        {"metric": "unreviewed_final_council_references_without_index_stem", "value": len(council_no_stem_final_rows), "status": "pass" if not council_no_stem_final_rows else "fail", "note": "No adopted or disapproved Council reference may remain unclassified."},
        {"metric": "zap_c_prefixed_application_rows", "value": len(zap_application_rows), "status": "informational", "note": "C-prefixed application rows in the production ZAP ULURP spine through the end year."},
        {"metric": "zap_exact_certified_report_matches", "value": len(zap_application_rows) - len(zap_benchmark_rows), "status": "informational", "note": "ZAP application identifiers matching corrected certified CPC source rows exactly."},
        {"metric": "zap_amendment_or_base_identifier_matches", "value": zap_status_counts["amendment_or_base_identifier_match"], "status": "informational", "note": "ZAP base identifiers covered by an A/B/C report variant with the same action code."},
        {"metric": "zap_same_project_report_mentions_application", "value": zap_status_counts["same_project_report_mentions_application_stem"], "status": "informational", "note": "Unmatched ZAP actions explicitly named in another certified report for the same project."},
        {"metric": "zap_reviewed_official_record_exclusions", "value": zap_status_counts["reviewed_official_record_exclusion"], "status": "informational", "note": "ZAP C rows whose corresponding official source was reviewed as a non-certified action or modification."},
        {"metric": "zap_reviewed_external_reference_exclusions", "value": sum(row["external_review_decision"] != "" for row in zap_benchmark_rows), "status": "informational", "note": "ZAP identifiers preserved as transcription errors or withdrawals after direct source review."},
        {"metric": "zap_not_final_by_end_year", "value": zap_status_counts["not_final_by_end_year"], "status": "informational", "note": "Applications still active or not completed by the corpus end year."},
        {"metric": "completed_zap_applications_without_cpc_report_match", "value": zap_status_counts["completed_zap_application_without_cpc_report_match"], "status": "warning", "note": "Completed ZAP application rows without a corrected CPC report match; these remain in the application universe but cannot supply report text."},
    ]
    write_csv(summary_rows, ["metric", "value", "status", "note"], "../output/official_ulurp_cpc_corpus_summary.csv")
    print("Wrote official ULURP CPC corpus audit outputs", flush=True)


if __name__ == "__main__":
    main()
