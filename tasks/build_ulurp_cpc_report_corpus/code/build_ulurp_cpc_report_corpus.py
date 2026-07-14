#!/usr/bin/env python3

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_ulurp_cpc_report_corpus/code")
# start_year = 1975
# end_year = 2025
# worker_count = 6
# ocr_dpi = 200
# ocr_page_timeout_seconds = 90
# minimum_embedded_page_words = 50

from __future__ import annotations

import csv
import hashlib
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd


DOWNLOAD_ATTEMPTS = 3
DOWNLOAD_TIMEOUT_SECONDS = 120
APPLICATION_PATTERN = re.compile(
    r"(?<![A-Z0-9])(?:[A-Z]\s*)?\d{6}(?:\s*\([A-Z]\)|[A-Z])?\s*[A-Z]{2,4}[A-Z](?![A-Z0-9])",
    re.IGNORECASE,
)
CPC_RESOLUTION_PATTERN = re.compile(
    r"(?im)^[ \t]*RESOLVED[,.]?[ \t]+BY[ \t]+THE[ \t]+CITY[ \t]+PLANNING[ \t]+COMMISSION\b"
)


def clean_text(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def application_key(value):
    compact = re.sub(r"[^A-Z0-9]", "", clean_text(value).upper())
    if re.match(r"^[A-Z]\d{6}", compact):
        compact = compact[1:]
    return compact


def indexed_application_key(value):
    return re.sub(r"[^A-Z0-9]", "", clean_text(value).upper())


def action_code(value):
    key = application_key(value)
    match = re.match(r"^\d{6}[A-Z]?([A-Z]{2,4})([A-Z])$", key)
    return match.group(1) if match else ""


def project_key(value):
    return re.sub(r"[^a-z0-9]+", " ", clean_text(value).lower()).strip()


def certified_identifier(value):
    return bool(re.match(r"^C\d{6}", indexed_application_key(value)))


def noticed_identifier(value):
    return bool(re.match(r"^N\d{6}", indexed_application_key(value)))


def safe_filename_part(value):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", clean_text(value)).strip("_")[:100] or "missing"


def readable_file(path, pdf=False, minimum_size=1):
    if path is None or not path.exists():
        return False
    stat_result = path.stat()
    if getattr(stat_result, "st_blocks", 1) == 0 and stat_result.st_size > 0:
        return False
    if stat_result.st_size < minimum_size:
        return False
    if pdf:
        with path.open("rb") as input_file:
            return input_file.read(4) == b"%PDF"
    return True


def text_word_count(text):
    return len(re.findall(r"[A-Za-z0-9$]+(?:[-'][A-Za-z0-9]+)?", text))


def extracted_text_is_usable(text):
    if text_word_count(text) < 50:
        return False
    ascii_character_share = sum(
        character in "\n\r\t\f" or 32 <= ord(character) <= 126
        for character in text
    ) / max(len(text), 1)
    return ascii_character_share >= 0.75


def read_csv(path):
    with Path(path).open(newline="", encoding="utf-8") as input_file:
        return list(csv.DictReader(input_file))


def download_pdf(url, output_path):
    if readable_file(output_path, pdf=True):
        return "downloaded_official_index", ""

    temp_path = Path(f"{output_path}.tmp")
    failures = []
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        temp_path.unlink(missing_ok=True)
        completed = subprocess.run(
            [
                "curl", "--silent", "--show-error", "--location",
                "--user-agent", "Mozilla/5.0",
                "--connect-timeout", "15", "--max-time", str(DOWNLOAD_TIMEOUT_SECONDS),
                "--output", str(temp_path), "--write-out", "%{http_code}", url,
            ],
            capture_output=True,
            text=True,
            timeout=DOWNLOAD_TIMEOUT_SECONDS + 10,
            check=False,
        )
        http_status = clean_text(completed.stdout)[-3:]
        if completed.returncode == 0 and http_status == "200" and readable_file(temp_path, pdf=True):
            os.replace(temp_path, output_path)
            return "downloaded_official_index", ""
        failures.append(
            f"attempt={attempt} curl_exit={completed.returncode} http_status={http_status or 'missing'} "
            f"stderr={clean_text(completed.stderr)}"
        )
        temp_path.unlink(missing_ok=True)
        if attempt < DOWNLOAD_ATTEMPTS:
            time.sleep(2 * attempt)
    return "download_failed", " | ".join(failures)


def extract_pdf_text(pdf_path):
    completed = subprocess.run(
        ["pdftotext", "-layout", "-enc", "UTF-8", str(pdf_path), "-"],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    if completed.returncode != 0:
        return "", clean_text(completed.stderr) or f"pdftotext exited {completed.returncode}"
    return completed.stdout, ""


def pdf_page_count(pdf_path):
    completed = subprocess.run(
        ["pdfinfo", str(pdf_path)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(clean_text(completed.stderr) or f"pdfinfo failed for {pdf_path}")
    match = re.search(r"^Pages:\s+([0-9]+)", completed.stdout, re.MULTILINE)
    if not match:
        raise RuntimeError(f"Could not read page count for {pdf_path}")
    return int(match.group(1))


def ocr_pdf_page(pdf_path, page_number, ocr_dpi, page_timeout_seconds, temp_dir):
    image_prefix = Path(temp_dir) / f"page_{page_number}"
    try:
        rendered = subprocess.run(
            [
                "pdftoppm", "-f", str(page_number), "-l", str(page_number),
                "-r", str(ocr_dpi), "-png", str(pdf_path), str(image_prefix),
            ],
            capture_output=True,
            text=True,
            timeout=page_timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return None
    if rendered.returncode != 0:
        raise RuntimeError(clean_text(rendered.stderr) or f"pdftoppm failed on page {page_number}")

    image_paths = sorted(Path(temp_dir).glob(f"page_{page_number}-*.png"))
    if not image_paths:
        return None
    try:
        recognized = subprocess.run(
            [
                "tesseract", str(image_paths[0]), "stdout", "--psm", "6",
                "--dpi", str(ocr_dpi),
            ],
            capture_output=True,
            text=True,
            timeout=page_timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return None
    if recognized.returncode != 0:
        raise RuntimeError(clean_text(recognized.stderr) or f"tesseract failed on page {page_number}")
    return recognized.stdout.strip()


def add_missing_report_page_ocr(
    pdf_path,
    extracted_text,
    ocr_dpi,
    page_timeout_seconds,
    minimum_embedded_page_words,
):
    page_count = pdf_page_count(pdf_path)
    pages = extracted_text.split("\f")[:page_count]
    pages.extend([""] * (page_count - len(pages)))
    main_report_resolution_page = next(
        (
            page_index + 1
            for page_index, page_text in enumerate(pages)
            if CPC_RESOLUTION_PATTERN.search(page_text)
        ),
        None,
    )
    candidate_page_indexes = [
        page_index
        for page_index, page_text in enumerate(pages)
        if text_word_count(page_text) < minimum_embedded_page_words
        and (
            main_report_resolution_page is None
            or page_index + 1 <= main_report_resolution_page
        )
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        repaired_page_numbers = []
        skipped_page_numbers = []
        for page_index in candidate_page_indexes:
            if (
                main_report_resolution_page is not None
                and page_index + 1 > main_report_resolution_page
            ):
                break
            page_text = ocr_pdf_page(
                pdf_path,
                page_index + 1,
                ocr_dpi,
                page_timeout_seconds,
                temp_dir,
            )
            if page_text is None:
                skipped_page_numbers.append(page_index + 1)
            elif text_word_count(page_text) > text_word_count(pages[page_index]):
                pages[page_index] = f"[PAGE {page_index + 1} OCR]\n{page_text}\n"
                repaired_page_numbers.append(page_index + 1)
            if CPC_RESOLUTION_PATTERN.search(page_text or ""):
                main_report_resolution_page = page_index + 1

    short_page_numbers = [
        page_index + 1
        for page_index, page_text in enumerate(pages)
        if text_word_count(page_text) < minimum_embedded_page_words
        and (
            main_report_resolution_page is None
            or page_index + 1 <= main_report_resolution_page
        )
    ]
    return (
        "\f".join(pages),
        repaired_page_numbers,
        skipped_page_numbers,
        short_page_numbers,
        main_report_resolution_page,
    )


def ocr_pdf(pdf_path, ocr_dpi, page_timeout_seconds):
    page_count = pdf_page_count(pdf_path)
    page_texts = []
    skipped_pages = []

    with tempfile.TemporaryDirectory() as temp_dir:
        for page_number in range(1, page_count + 1):
            page_text = ocr_pdf_page(
                pdf_path,
                page_number,
                ocr_dpi,
                page_timeout_seconds,
                temp_dir,
            )
            if page_text is None:
                skipped_pages.append(page_number)
                page_texts.append("")
                continue
            page_texts.append(f"[PAGE {page_number}]\n{page_text}")

    return "\f".join(page_texts), page_count, skipped_pages


def main():
    if len(sys.argv) != 7:
        raise RuntimeError(
            "Usage: python3 build_ulurp_cpc_report_corpus.py "
            "<start_year> <end_year> <worker_count> <ocr_dpi> "
            "<ocr_page_timeout_seconds> <minimum_embedded_page_words>"
        )

    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    worker_count = int(sys.argv[3])
    ocr_dpi = int(sys.argv[4])
    ocr_page_timeout_seconds = int(sys.argv[5])
    minimum_embedded_page_words = int(sys.argv[6])
    if (
        start_year > end_year
        or worker_count < 1
        or ocr_dpi < 72
        or ocr_page_timeout_seconds < 1
        or minimum_embedded_page_words < 1
    ):
        raise RuntimeError("Invalid corpus build scalar arguments.")

    official_index_rows = read_csv("../input/official_cpc_report_index.csv")
    correction_rows = read_csv("../input/ulurp_cpc_source_corrections.csv")
    source_corrections = {
        indexed_application_key(row["raw_application_number"]): row
        for row in correction_rows
    }
    if len(source_corrections) != len(correction_rows):
        raise RuntimeError("Source-correction application numbers must be unique.")
    index_additions = read_csv("../input/ulurp_cpc_index_additions.csv")

    indexed_keys = {
        indexed_application_key(row["application_number"])
        for row in official_index_rows
    }
    missing_correction_keys = sorted(set(source_corrections) - indexed_keys)
    if missing_correction_keys:
        raise RuntimeError(
            "Source corrections do not match the fetched CPC index: "
            + "; ".join(missing_correction_keys)
        )

    corrected_index_rows = []
    for row in official_index_rows:
        correction = source_corrections.get(indexed_application_key(row["application_number"]), {})
        corrected_row = dict(row)
        corrected_row["canonical_application_number"] = (
            correction.get("canonical_application_number") or row["application_number"]
        )
        corrected_row["canonical_vote_date"] = correction.get("canonical_vote_date") or row["vote_date"]
        corrected_row["source_correction"] = correction
        corrected_row["official_index_row_flag"] = "TRUE"
        corrected_index_rows.append(corrected_row)
    corrected_index_rows = [
        row for row in corrected_index_rows
        if start_year
        <= datetime.strptime(row["canonical_vote_date"], "%m/%d/%Y").year
        <= end_year
    ]

    certified_rows = [
        row for row in corrected_index_rows
        if certified_identifier(row["canonical_application_number"])
        and row["source_correction"].get("include_in_corpus", "1") == "1"
    ]
    certified_project_votes = {
        (project_key(row["project_name"]), row["canonical_vote_date"])
        for row in certified_rows
        if project_key(row["project_name"])
    }

    official_rows = []
    for row in corrected_index_rows:
        correction = row["source_correction"]
        if correction.get("include_in_corpus") == "0":
            continue
        if certified_identifier(row["canonical_application_number"]):
            row["corpus_role"] = correction.get("corpus_role") or "certified_ulurp_report"
            official_rows.append(row)
            continue
        related_by_exact_project_vote = (
            noticed_identifier(row["canonical_application_number"])
            and row["lead_report_flag"] == "TRUE"
            and (project_key(row["project_name"]), row["canonical_vote_date"]) in certified_project_votes
        )
        if correction.get("corpus_role") == "related_project_narrative_lead" or related_by_exact_project_vote:
            row["corpus_role"] = "related_project_narrative_lead"
            official_rows.append(row)

    for addition in index_additions:
        if not start_year <= int(addition["vote_year"]) <= end_year:
            continue
        official_rows.append(
            {
                **addition,
                "canonical_application_number": addition["application_number"],
                "canonical_vote_date": addition["vote_date"],
                "source_correction": {},
                "official_index_row_flag": "FALSE",
            }
        )

    official_rows.sort(
        key=lambda row: (
            datetime.strptime(row["canonical_vote_date"], "%m/%d/%Y"),
            indexed_application_key(row["canonical_application_number"]),
        )
    )
    canonical_identifiers = [
        indexed_application_key(row["canonical_application_number"])
        for row in official_rows
    ]
    if len(canonical_identifiers) != len(set(canonical_identifiers)):
        raise RuntimeError("Corrected CPC corpus application numbers must be unique.")

    zap_by_key = defaultdict(list)
    for project in pd.read_parquet("../input/zap_project_data.parquet").to_dict(orient="records"):
        for match in APPLICATION_PATTERN.finditer(clean_text(project.get("ulurp_numbers"))):
            zap_by_key[application_key(match.group(0))].append(project)

    previous_pdf_urls = {}
    if Path("../output/ulurp_cpc_report_manifest.csv").exists():
        previous_pdf_urls = {
            row["application_key"]: row["resolved_pdf_url"]
            for row in read_csv("../output/ulurp_cpc_report_manifest.csv")
        }

    def process_row(row_number, official_row):
        official_index_application_number = official_row["application_number"]
        correction = official_row["source_correction"]
        corrected_application_number = official_row["canonical_application_number"]
        key = application_key(corrected_application_number)
        source_keys = list(dict.fromkeys([
            key,
            application_key(official_index_application_number),
        ]))
        output_pdf_path = Path("../output/cpc_report_pdfs") / (
            f"{safe_filename_part(official_row['report_stem'])}_{safe_filename_part(key)}.pdf"
        )
        output_text_path = Path("../output/cpc_report_text") / (
            f"{safe_filename_part(official_row['report_stem'])}_{safe_filename_part(key)}.txt"
        )
        source_usable = (
            official_row.get("source_usable")
            or correction.get("source_usable", "1")
        ) == "1"
        resolved_pdf_url = (
            correction.get("resolved_pdf_url") or official_row["pdf_url"]
            if source_usable
            else correction.get("resolved_pdf_url", "")
        )
        if resolved_pdf_url and not resolved_pdf_url.lower().endswith(".pdf"):
            resolved_pdf_url = f"{resolved_pdf_url}.pdf"
        if source_usable and not resolved_pdf_url:
            raise RuntimeError(f"Usable source has no PDF URL: {corrected_application_number}")

        if previous_pdf_urls.get(key) != resolved_pdf_url:
            output_pdf_path.unlink(missing_ok=True)
            output_text_path.unlink(missing_ok=True)

        if not source_usable:
            pdf_path = None
            pdf_source = ""
            download_status = "known_source_unavailable"
            download_error = (
                official_row.get("source_unavailable_reason")
                or correction.get("correction_reason", "")
            )
        else:
            download_status, download_error = download_pdf(resolved_pdf_url, output_pdf_path)
            pdf_path = output_pdf_path if readable_file(output_pdf_path, pdf=True) else None
            if pdf_path:
                pdf_source = (
                    "official_cpc_index_download"
                    if official_row["official_index_row_flag"] == "TRUE"
                    else "verified_index_omission_download"
                )
            else:
                pdf_source = ""

        text = ""
        text_path = None
        text_method = ""
        text_error = ""
        page_count = ""
        skipped_ocr_pages = ""
        partial_ocr_pages = ""
        short_text_pages_after_ocr = ""
        main_report_resolution_page = ""

        if pdf_path is not None:
            page_count = pdf_page_count(pdf_path)
            fresh_text, fresh_text_error = extract_pdf_text(pdf_path)
            repaired_page_numbers = []
            skipped_page_numbers = []
            short_page_numbers = []
            if not fresh_text_error:
                (
                    fresh_text,
                    repaired_page_numbers,
                    skipped_page_numbers,
                    short_page_numbers,
                    main_report_resolution_page,
                ) = add_missing_report_page_ocr(
                    pdf_path,
                    fresh_text,
                    ocr_dpi,
                    ocr_page_timeout_seconds,
                    minimum_embedded_page_words,
                )
            if skipped_page_numbers:
                raise RuntimeError(
                    f"OCR did not complete for {corrected_application_number} pages "
                    + "; ".join(str(page) for page in skipped_page_numbers)
                )
            partial_ocr_pages = "; ".join(str(page) for page in repaired_page_numbers)
            skipped_ocr_pages = "; ".join(str(page) for page in skipped_page_numbers)
            short_text_pages_after_ocr = "; ".join(str(page) for page in short_page_numbers)

            if extracted_text_is_usable(fresh_text):
                text = fresh_text
                output_text_path.write_text(text, encoding="utf-8")
                text_path = output_text_path
                if repaired_page_numbers:
                    text_method = "partial_page_ocr"
                else:
                    text_method = "pdftotext"
            else:
                text_error = fresh_text_error

        if not text and pdf_path is not None:
            try:
                ocr_text, page_count, skipped_pages = ocr_pdf(
                    pdf_path,
                    ocr_dpi,
                    ocr_page_timeout_seconds,
                )
                if skipped_pages:
                    raise RuntimeError(
                        f"OCR did not complete for {corrected_application_number} pages "
                        + "; ".join(str(page) for page in skipped_pages)
                    )
                skipped_ocr_pages = "; ".join(str(page) for page in skipped_pages)
                short_text_pages_after_ocr = ""
                main_report_resolution_page = ""
                if text_word_count(ocr_text) >= 50:
                    text = ocr_text
                    output_text_path.write_text(text, encoding="utf-8")
                    text_path = output_text_path
                    text_method = "full_document_ocr"
                    text_error = ""
                else:
                    text_error = "OCR produced fewer than 50 words."
            except (RuntimeError, subprocess.TimeoutExpired) as error:
                text_error = clean_text(error)

        app_matches = [row for source_key in source_keys for row in zap_by_key.get(source_key, [])]
        project_ids = sorted({
            clean_text(row.get("project_id"))
            for row in app_matches
            if clean_text(row.get("project_id"))
        })
        project_names = sorted({
            clean_text(row.get("project_name"))
            for row in app_matches
            if clean_text(row.get("project_name"))
        })
        ulurp_groups = sorted({
            clean_text(row.get("ulurp_group"))
            for row in app_matches
            if clean_text(row.get("ulurp_group"))
        })

        return {
            "row_number": row_number,
            "application_number": corrected_application_number,
            "official_index_application_number": (
                official_index_application_number
                if official_row["official_index_row_flag"] == "TRUE"
                else ""
            ),
            "application_key": key,
            "action_code": action_code(corrected_application_number),
            "corpus_role": official_row["corpus_role"],
            "source_usable": str(source_usable).upper(),
            "official_index_row_flag": official_row["official_index_row_flag"],
            "official_project_name": official_row["project_name"],
            "official_community_district": official_row["community_district"],
            "official_index_vote_date": (
                official_row["vote_date"]
                if official_row["official_index_row_flag"] == "TRUE"
                else ""
            ),
            "official_vote_date": official_row["canonical_vote_date"],
            "official_vote_year": datetime.strptime(
                official_row["canonical_vote_date"], "%m/%d/%Y"
            ).year,
            "official_lead_report_flag": official_row["lead_report_flag"],
            "official_pdf_url": official_row["pdf_url"],
            "resolved_pdf_url": resolved_pdf_url,
            "source_correction_type": (
                correction.get("correction_type", "")
                if official_row["official_index_row_flag"] == "TRUE"
                else (
                    "index_omission_addition"
                    if source_usable
                    else "index_omission_source_unavailable"
                )
            ),
            "source_correction_reason": (
                correction.get("correction_reason", "")
                if official_row["official_index_row_flag"] == "TRUE"
                else official_row["index_omission_evidence"]
            ),
            "official_report_stem": official_row["report_stem"],
            "zap_project_ids": "; ".join(project_ids),
            "zap_project_names": "; ".join(project_names),
            "zap_ulurp_groups": "; ".join(ulurp_groups),
            "pdf_source": pdf_source,
            "download_status": download_status,
            "download_error": download_error,
            "local_pdf_path": str(pdf_path) if pdf_path else "",
            "text_method": text_method,
            "text_status": "text_extracted" if text_word_count(text) >= 50 else "text_unavailable",
            "text_error": text_error,
            "text_word_count": text_word_count(text),
            "text_char_count": len(text.strip()),
            "local_text_path": str(text_path) if text_path else "",
            "pdf_page_count": page_count,
            "partial_ocr_pages": partial_ocr_pages,
            "skipped_ocr_pages": skipped_ocr_pages,
            "short_text_pages_after_ocr": short_text_pages_after_ocr,
            "main_report_resolution_page": main_report_resolution_page or "",
            "document_id": hashlib.sha1(
                f"{corrected_application_number}|{official_row['canonical_vote_date']}|{resolved_pdf_url or official_row['pdf_url']}".encode("utf-8")
            ).hexdigest()[:20],
        }

    results = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(process_row, row_number, row)
            for row_number, row in enumerate(official_rows, start=1)
        ]
        for completed_count, future in enumerate(as_completed(futures), start=1):
            results.append(future.result())
            if completed_count == 1 or completed_count % 250 == 0 or completed_count == len(official_rows):
                print(f"Processed {completed_count}/{len(official_rows)} official ULURP CPC reports", flush=True)

    results.sort(key=lambda row: row.pop("row_number"))
    failed_sources = [
        row["application_number"]
        for row in results
        if row["source_usable"] == "TRUE" and row["text_status"] != "text_extracted"
    ]
    if failed_sources:
        raise RuntimeError(
            f"{len(failed_sources)} usable CPC sources lack extracted text: "
            + "; ".join(failed_sources[:20])
        )

    document_ids = [row["document_id"] for row in results]
    if len(document_ids) != len(set(document_ids)):
        raise RuntimeError("CPC corpus document identifiers must be unique.")

    fieldnames = list(results[0].keys())
    with Path("../output/ulurp_cpc_report_manifest.csv").open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    certified_count = sum(row["corpus_role"] == "certified_ulurp_report" for row in results)
    narrative_lead_count = sum(row["corpus_role"] == "related_project_narrative_lead" for row in results)
    unavailable_count = sum(row["text_status"] != "text_extracted" for row in results)
    print(
        f"Wrote {len(results)} CPC source rows: {certified_count} certified ULURP reports and "
        f"{narrative_lead_count} related project narrative leads; "
        f"{unavailable_count} lack usable text",
        flush=True,
    )


if __name__ == "__main__":
    main()
