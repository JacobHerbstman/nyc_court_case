#!/usr/bin/env python3

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/sample_ulurp_zoning_cpc_reports/code")
# sample_seed = 20260702
# documents_per_cohort = 10

from __future__ import annotations

import csv
import hashlib
import io
import os
import random
import re
import subprocess
import sys


TARGET_ACTION_CODES = {"ZM", "ZR", "ZS"}
CURL_CONNECT_TIMEOUT_SECONDS = 10
CURL_MAX_TIME_SECONDS = 120


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def stable_id(*parts: object) -> str:
    text = "||".join(clean_text(part) for part in parts)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:20]


def safe_filename_part(value: object) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", clean_text(value))[:80]
    return cleaned.strip("_") or "missing"


def write_csv_if_changed(rows: list[dict[str, object]], fieldnames: list[str], path: str) -> None:
    writer_buffer = io.StringIO()
    writer = csv.DictWriter(writer_buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    new_text = writer_buffer.getvalue()

    try:
        with open(path, "r", encoding="utf-8", newline="") as existing_file:
            old_text = existing_file.read()
    except FileNotFoundError:
        old_text = None

    if old_text != new_text:
        with open(path, "w", encoding="utf-8", newline="") as output_file:
            output_file.write(new_text)


def write_text_if_changed(text: str, path: str) -> None:
    try:
        with open(path, "r", encoding="utf-8") as existing_file:
            old_text = existing_file.read()
    except FileNotFoundError:
        old_text = None

    if old_text != text:
        with open(path, "w", encoding="utf-8") as output_file:
            output_file.write(text)


def parse_ulurp_action_rows(ulurp_numbers: str) -> list[dict[str, str]]:
    rows = []
    for raw_number in re.split(r"\s*;\s*", ulurp_numbers or ""):
        raw_number = clean_text(raw_number).upper()
        compact = re.sub(r"\s+", "", raw_number)
        action_match = re.search(r"([A-Z]{2})([MKQRX])$", compact)
        digit_match = re.search(r"\d{6}", compact)
        if not action_match or not digit_match:
            continue
        action_code = action_match.group(1)
        if action_code not in TARGET_ACTION_CODES:
            continue
        rows.append(
            {
                "raw_application_number": raw_number,
                "action_code": action_code,
                "cpc_report_stem": digit_match.group(0),
                "cpc_report_url": f"https://www.nyc.gov/assets/planning/download/pdf/about/cpc/{digit_match.group(0)}.pdf",
            }
        )
    return rows


def download_pdf(url: str, pdf_path: str) -> tuple[str, str]:
    completed = subprocess.run(
        [
            "curl",
            "--silent",
            "--show-error",
            "--location",
            "--fail",
            "--user-agent",
            "Mozilla/5.0",
            "--connect-timeout",
            str(CURL_CONNECT_TIMEOUT_SECONDS),
            "--max-time",
            str(CURL_MAX_TIME_SECONDS),
            "--output",
            pdf_path,
            url,
        ],
        capture_output=True,
        text=True,
        timeout=CURL_MAX_TIME_SECONDS + 10,
        check=False,
    )
    if completed.returncode != 0:
        return "download_failed", clean_text(completed.stderr) or f"curl exited {completed.returncode}"
    if not os.path.exists(pdf_path) or os.path.getsize(pdf_path) == 0:
        return "download_failed", "downloaded file is missing or empty"
    return "downloaded", ""


def extract_pdf_text(pdf_path: str, text_path: str) -> tuple[str, str, str]:
    completed = subprocess.run(
        ["pdftotext", "-layout", "-enc", "UTF-8", pdf_path, "-"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if completed.returncode != 0:
        write_text_if_changed("", text_path)
        return "", "text_extract_failed", clean_text(completed.stderr) or f"pdftotext exited {completed.returncode}"

    write_text_if_changed(completed.stdout, text_path)
    if clean_text(completed.stdout) == "":
        return completed.stdout, "empty_text", ""
    return completed.stdout, "text_extracted", ""


def read_sample_frame() -> list[dict[str, object]]:
    with open("../input/ulurp_corpus_project_spine.csv", "r", encoding="utf-8", newline="") as input_file:
        project_rows = list(csv.DictReader(input_file))

    rows = []
    for project_row in project_rows:
        if project_row.get("manual_sample_cohort") not in {"1990s", "early_mid_2000s", "2015_onward"}:
            continue
        action_rows = parse_ulurp_action_rows(project_row.get("ulurp_numbers", ""))
        if not action_rows:
            continue
        rows.append({**project_row, "target_action_rows": action_rows})
    return rows


if len(sys.argv) != 3:
    raise RuntimeError("Usage: python3 sample_ulurp_zoning_cpc_reports.py <sample_seed> <documents_per_cohort>")

sample_seed = int(sys.argv[1])
documents_per_cohort = int(sys.argv[2])
os.makedirs("../output/sample_documents", exist_ok=True)
if documents_per_cohort <= 0:
    raise RuntimeError("documents_per_cohort must be positive.")

sample_frame = read_sample_frame()
random_generator = random.Random(sample_seed)

index_rows: list[dict[str, object]] = []
text_rows: list[dict[str, object]] = []
failure_rows: list[dict[str, object]] = []

for cohort in ["1990s", "early_mid_2000s", "2015_onward"]:
    cohort_rows = [row for row in sample_frame if row["manual_sample_cohort"] == cohort]
    random_generator.shuffle(cohort_rows)

    selected_count = 0
    for project_row in cohort_rows:
        if selected_count >= documents_per_cohort:
            break
        target_action = project_row["target_action_rows"][0]
        sample_rank = len(index_rows) + 1
        filename_prefix = "_".join([
            f"{sample_rank:02d}",
            safe_filename_part(cohort),
            safe_filename_part(project_row.get("corpus_reference_year", "")),
            safe_filename_part(project_row.get("project_id", "")),
            safe_filename_part(target_action["action_code"]),
            safe_filename_part(target_action["raw_application_number"]),
            stable_id(project_row.get("project_id", ""), target_action["cpc_report_url"])[:8],
        ])
        pdf_path = f"../output/sample_documents/{filename_prefix}.pdf"
        text_path = f"../output/sample_documents/{filename_prefix}.txt"

        download_status, download_error = download_pdf(target_action["cpc_report_url"], pdf_path)
        if download_status != "downloaded":
            failure_rows.append(
                {
                    "cohort": cohort,
                    "project_id": project_row["project_id"],
                    "project_name": project_row.get("project_name", ""),
                    "corpus_reference_year": project_row.get("corpus_reference_year", ""),
                    "raw_application_number": target_action["raw_application_number"],
                    "action_code": target_action["action_code"],
                    "source_doc": target_action["cpc_report_url"],
                    "failure_reason": download_error,
                }
            )
            continue

        document_text, text_status, text_error = extract_pdf_text(pdf_path, text_path)
        selected_count += 1
        index_rows.append(
            {
                "sample_rank": sample_rank,
                "cohort": cohort,
                "cohort_sample_rank": selected_count,
                "sample_seed": sample_seed,
                "project_id": project_row["project_id"],
                "project_name": project_row.get("project_name", ""),
                "project_brief": project_row.get("project_brief", ""),
                "corpus_reference_year": project_row.get("corpus_reference_year", ""),
                "ulurp_numbers": project_row.get("ulurp_numbers", ""),
                "raw_application_number": target_action["raw_application_number"],
                "action_code": target_action["action_code"],
                "ceqr_number": project_row.get("ceqr_number", ""),
                "applicant_type": project_row.get("applicant_type", ""),
                "primary_applicant": project_row.get("primary_applicant", ""),
                "borough_name": project_row.get("borough_name", ""),
                "community_district": project_row.get("community_district", ""),
                "document_family": "cpc_report",
                "document_title": f"{target_action['raw_application_number']} CPC report",
                "source_doc": target_action["cpc_report_url"],
                "local_pdf_path": pdf_path,
                "local_text_path": text_path,
                "download_status": download_status,
                "text_status": text_status,
                "text_error": text_error,
                "text_char_count": len(clean_text(document_text)),
                "project_page_url": project_row.get("project_page_url", ""),
            }
        )
        text_rows.append(
            {
                "sample_rank": sample_rank,
                "cohort": cohort,
                "project_id": project_row["project_id"],
                "raw_application_number": target_action["raw_application_number"],
                "action_code": target_action["action_code"],
                "document_title": f"{target_action['raw_application_number']} CPC report",
                "source_doc": target_action["cpc_report_url"],
                "local_text_path": text_path,
                "text_status": text_status,
                "document_text": document_text,
            }
        )
        print(f"Selected {selected_count}/{documents_per_cohort} for {cohort}", flush=True)

qc_rows = [
    {
        "metric": "sample_seed",
        "value": sample_seed,
        "status": "pass",
        "note": "Fixed random seed used for project ordering within cohorts.",
    },
    {
        "metric": "documents_per_cohort",
        "value": documents_per_cohort,
        "status": "pass",
        "note": "Requested selected CPC report count per cohort.",
    },
    {
        "metric": "selected_document_count",
        "value": len(index_rows),
        "status": "pass" if len(index_rows) == documents_per_cohort * 3 else "fail",
        "note": "Downloaded ZM/ZR/ZS CPC reports selected across all cohorts.",
    },
    {
        "metric": "download_failure_count",
        "value": len(failure_rows),
        "status": "pass",
        "note": "Failed CPC report download attempts retained for audit.",
    },
]

for cohort in ["1990s", "early_mid_2000s", "2015_onward"]:
    selected_count = sum(1 for row in index_rows if row["cohort"] == cohort)
    frame_count = sum(1 for row in sample_frame if row["manual_sample_cohort"] == cohort)
    qc_rows.append(
        {
            "metric": f"selected_document_count_{cohort}",
            "value": selected_count,
            "status": "pass" if selected_count == documents_per_cohort else "fail",
            "note": f"Downloaded ZM/ZR/ZS CPC reports for {cohort}; sampling frame had {frame_count} projects.",
        }
    )

index_fieldnames = [
    "sample_rank",
    "cohort",
    "cohort_sample_rank",
    "sample_seed",
    "project_id",
    "project_name",
    "project_brief",
    "corpus_reference_year",
    "ulurp_numbers",
    "raw_application_number",
    "action_code",
    "ceqr_number",
    "applicant_type",
    "primary_applicant",
    "borough_name",
    "community_district",
    "document_family",
    "document_title",
    "source_doc",
    "local_pdf_path",
    "local_text_path",
    "download_status",
    "text_status",
    "text_error",
    "text_char_count",
    "project_page_url",
]

text_fieldnames = [
    "sample_rank",
    "cohort",
    "project_id",
    "raw_application_number",
    "action_code",
    "document_title",
    "source_doc",
    "local_text_path",
    "text_status",
    "document_text",
]

failure_fieldnames = [
    "cohort",
    "project_id",
    "project_name",
    "corpus_reference_year",
    "raw_application_number",
    "action_code",
    "source_doc",
    "failure_reason",
]

write_csv_if_changed(index_rows, index_fieldnames, "../output/ulurp_zoning_cpc_report_sample_index.csv")
write_csv_if_changed(text_rows, text_fieldnames, "../output/ulurp_zoning_cpc_report_sample_text.csv")
write_csv_if_changed(failure_rows, failure_fieldnames, "../output/ulurp_zoning_cpc_report_sample_fetch_failures.csv")
write_csv_if_changed(qc_rows, ["metric", "value", "status", "note"], "../output/ulurp_zoning_cpc_report_sample_qc.csv")

if any(row["status"] == "fail" for row in qc_rows):
    print("ULURP zoning CPC report sample QC failed.", file=sys.stderr)
    sys.exit(1)

print("Wrote ULURP zoning CPC report sample outputs to ../output")
