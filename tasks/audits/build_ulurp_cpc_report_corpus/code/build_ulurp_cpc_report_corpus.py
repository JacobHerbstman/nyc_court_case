#!/usr/bin/env python3

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/build_ulurp_cpc_report_corpus/code")
# start_year = 1975
# end_year = 2026
# report_limit = 0
# worker_count = 6

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse


CPC_REPORT_BASE_URL = "https://www.nyc.gov/assets/planning/download/pdf/about/cpc"
ZAP_API_HOST = "https://zap-api-production.herokuapp.com"
ZAP_PROJECT_INCLUDE = "actions"
CURL_CONNECT_TIMEOUT_SECONDS = 10
CURL_MAX_TIME_SECONDS = 120
DOWNLOAD_ATTEMPTS = 3
DOWNLOAD_RETRY_SLEEP_SECONDS = 2
API_FETCH_ATTEMPTS = 3
API_RETRY_SLEEP_SECONDS = 3
CURL_HTTP_STATUS_MARKER = "\n__HTTP_STATUS__:"
PROJECT_API_CACHE: dict[str, tuple[str, str, dict[str, object]]] = {}
KNOWN_TWO_LETTER_ACTION_CODES = {
    "BD",
    "CM",
    "EC",
    "HA",
    "HD",
    "HG",
    "HI",
    "HK",
    "HO",
    "HU",
    "LD",
    "MM",
    "PC",
    "PI",
    "PP",
    "PQ",
    "PX",
    "RC",
    "SC",
    "TC",
    "TL",
    "UC",
    "UD",
    "ZA",
    "ZC",
    "ZM",
    "ZR",
    "ZS",
}


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


def assert_unique_keys(rows: list[dict[str, str]], key_cols: list[str], name: str) -> None:
    seen: set[tuple[str, ...]] = set()
    duplicates: list[tuple[str, ...]] = []
    for row in rows:
        key = tuple(row.get(col, "") for col in key_cols)
        if key in seen:
            duplicates.append(key)
        seen.add(key)
    if duplicates:
        raise RuntimeError(f"{name} is not unique by {', '.join(key_cols)}.")


def parse_application_number(raw_application_number: str) -> dict[str, str]:
    raw_value = clean_text(raw_application_number).upper()
    compact = re.sub(r"\s+", "", raw_value)
    compact = re.sub(r"[^A-Z0-9()]", "", compact)
    digits_match = re.search(r"\d{6}", compact)
    if not digits_match:
        return {
            "base_report_stem": "",
            "candidate_report_stems": "",
            "parsed_action_code": "",
            "parsed_borough_code": "",
            "parsed_amendment_letter": "",
        }

    base_stem = digits_match.group(0)
    before_digits = compact[: digits_match.start()]
    after_digits = compact[digits_match.end():]
    after_digits = re.sub(r"^\(([A-Z])\)", r"\1", after_digits)

    parsed_action_code = ""
    parsed_borough_code = ""
    parsed_amendment_letter = ""

    tail_match = re.match(r"^([A-Z]*)([A-Z])$", after_digits)
    if tail_match:
        action_tail = tail_match.group(1)
        parsed_borough_code = tail_match.group(2)
        if len(action_tail) >= 2:
            parsed_action_code = action_tail[-2:]
            possible_amendment = action_tail[:-2]
            if len(possible_amendment) == 1 and parsed_action_code in KNOWN_TWO_LETTER_ACTION_CODES:
                parsed_amendment_letter = possible_amendment.lower()
            elif len(action_tail) == 3 and action_tail not in {"AHU", "AHA"} and parsed_action_code in KNOWN_TWO_LETTER_ACTION_CODES:
                parsed_amendment_letter = action_tail[0].lower()

    parenthetical_amendment = re.search(r"\(([A-Z])\)", compact)
    if parenthetical_amendment:
        parsed_amendment_letter = parenthetical_amendment.group(1).lower()

    candidate_stems = [base_stem]
    if parsed_amendment_letter:
        candidate_stems.append(f"{base_stem}{parsed_amendment_letter}")

    # A few migrated records omit the leading C/N/M prefix. The prefix does not
    # affect CPC report URLs, but retaining this flag helps audit parser oddities.
    return {
        "base_report_stem": base_stem,
        "candidate_report_stems": "; ".join(dict.fromkeys(candidate_stems)),
        "parsed_action_code": parsed_action_code,
        "parsed_borough_code": parsed_borough_code,
        "parsed_amendment_letter": parsed_amendment_letter,
        "parsed_application_prefix": before_digits if before_digits in {"C", "N", "M"} else "",
    }


def report_url(report_stem: str) -> str:
    return f"{CPC_REPORT_BASE_URL}/{report_stem}.pdf"


def sharepoint_server_relative_url(absolute_url: str) -> str:
    if not absolute_url:
        return ""
    parsed = urllib.parse.urlparse(absolute_url)
    return urllib.parse.unquote(parsed.path)


def comparable_ulurp_number(value: str) -> str:
    compact = re.sub(r"[^A-Za-z0-9]", "", value or "").upper()
    if re.match(r"^[CNMI]\d{6}", compact):
        return compact[1:]
    return compact


def fetch_project(project_id: str) -> tuple[str, str, dict[str, object]]:
    if project_id in PROJECT_API_CACHE:
        return PROJECT_API_CACHE[project_id]

    encoded_id = urllib.parse.quote(project_id)
    url = f"{ZAP_API_HOST}/projects/{encoded_id}?include={urllib.parse.quote(ZAP_PROJECT_INCLUDE, safe=',')}"
    result = ("not_attempted", "", {})
    for attempt in range(1, API_FETCH_ATTEMPTS + 1):
        completed = subprocess.run(
            [
                "curl",
                "--silent",
                "--show-error",
                "--location",
                "--connect-timeout",
                str(CURL_CONNECT_TIMEOUT_SECONDS),
                "--max-time",
                str(CURL_MAX_TIME_SECONDS),
                "--user-agent",
                "nyc-ulurp-corpus-research/0.1",
                "--write-out",
                f"{CURL_HTTP_STATUS_MARKER}%{{http_code}}",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=CURL_MAX_TIME_SECONDS + 10,
            check=False,
        )
        if CURL_HTTP_STATUS_MARKER not in completed.stdout:
            result = ("curl_error", clean_text(completed.stderr) or f"curl exited {completed.returncode}", {})
        else:
            response_text, http_status_text = completed.stdout.rsplit(CURL_HTTP_STATUS_MARKER, 1)
            http_status = clean_text(http_status_text)[:3]
            if completed.returncode != 0:
                result = (f"curl_error_{http_status}", clean_text(completed.stderr) or f"curl exited {completed.returncode}", {})
            elif http_status != "200":
                result = (f"http_{http_status}", clean_text(response_text)[:500], {})
            else:
                try:
                    result = ("success", "", json.loads(response_text))
                except json.JSONDecodeError as error:
                    result = ("json_error", clean_text(error), {})

        if result[0] == "success" or attempt == API_FETCH_ATTEMPTS or result[0] not in {"curl_error", "curl_error_000", "http_503", "http_502", "http_504", "http_500", "http_429"}:
            break
        time.sleep(API_RETRY_SLEEP_SECONDS)

    PROJECT_API_CACHE[project_id] = result
    return result


def zap_action_cpc_url(project_id: str, raw_application_number: str) -> tuple[str, str, str]:
    fetch_status, fetch_error, data = fetch_project(project_id)
    if fetch_status != "success":
        return "", fetch_status, fetch_error

    target_number = comparable_ulurp_number(raw_application_number)
    for row in data.get("included", []):
        if row.get("type") != "actions":
            continue
        attrs = row.get("attributes", {})
        ulurp_number = clean_text(attrs.get("dcp-ulurpnumber"))
        if comparable_ulurp_number(ulurp_number) != target_number:
            continue
        relative = sharepoint_server_relative_url(clean_text(attrs.get("dcp-spabsoluteurl")))
        parsed = parse_application_number(ulurp_number or raw_application_number)
        stem = parsed["base_report_stem"]
        if relative and stem:
            return f"{ZAP_API_HOST}/document/projectaction{urllib.parse.quote(relative)}/{stem}.pdf", fetch_status, ""

    return "", fetch_status, "matching ZAP action did not expose dcp-spabsoluteurl"


def is_pdf_file(path: str) -> bool:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return False
    with open(path, "rb") as input_file:
        return input_file.read(4) == b"%PDF"

def download_pdf_url(candidate_url: str, pdf_path: str) -> tuple[str, str]:
    failure_notes = []
    if is_pdf_file(pdf_path):
        return "downloaded", ""
    if os.path.exists(pdf_path):
        os.remove(pdf_path)

    temp_pdf_path = f"{pdf_path}.tmp"
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        if os.path.exists(temp_pdf_path):
            os.remove(temp_pdf_path)

        completed = subprocess.run(
            [
                "curl",
                "--silent",
                "--show-error",
                "--location",
                "--user-agent",
                "Mozilla/5.0",
                "--connect-timeout",
                str(CURL_CONNECT_TIMEOUT_SECONDS),
                "--max-time",
                str(CURL_MAX_TIME_SECONDS),
                "--output",
                temp_pdf_path,
                "--write-out",
                "%{http_code}",
                candidate_url,
            ],
            capture_output=True,
            text=True,
            timeout=CURL_MAX_TIME_SECONDS + 10,
            check=False,
        )

        http_status = clean_text(completed.stdout)[-3:]
        stderr = clean_text(completed.stderr)
        if completed.returncode == 0 and http_status == "200" and is_pdf_file(temp_pdf_path):
            os.replace(temp_pdf_path, pdf_path)
            return "downloaded", ""

        if os.path.exists(temp_pdf_path):
            os.remove(temp_pdf_path)

        failure_note = (
            f"{candidate_url} attempt {attempt}/{DOWNLOAD_ATTEMPTS} "
            f"curl_exit={completed.returncode} http_status={http_status or 'missing'}"
        )
        if stderr:
            failure_note = f"{failure_note} stderr={stderr}"
        if completed.returncode == 0 and http_status == "200":
            failure_note = f"{failure_note} non_pdf_or_empty_response"
        failure_notes.append(failure_note)

        should_retry = completed.returncode != 0 or http_status in {"000", "403", "408", "425", "429", "500", "502", "503", "504"}
        if attempt < DOWNLOAD_ATTEMPTS and should_retry:
            time.sleep(DOWNLOAD_RETRY_SLEEP_SECONDS)
        else:
            break

    return "download_failed", " | ".join(failure_notes)


def download_pdf(candidate_stems: list[str], pdf_path_for_stem) -> tuple[str, str, str, str, str]:
    failure_notes = []
    for report_stem in candidate_stems:
        if not report_stem:
            continue
        candidate_url = report_url(report_stem)
        pdf_path = pdf_path_for_stem(report_stem)
        download_status, download_error = download_pdf_url(candidate_url, pdf_path)
        if download_status == "downloaded":
            return "downloaded", "", report_stem, candidate_url, "nycgov_cpc_report"
        failure_notes.append(download_error)

    return "download_failed", " | ".join(failure_notes) or "all candidate CPC report URLs failed", "", "", ""


def extract_pdf_text(pdf_path: str, text_path: str) -> tuple[str, str, int]:
    if os.path.exists(text_path) and os.path.getsize(text_path) > 0:
        with open(text_path, "r", encoding="utf-8", errors="ignore") as input_file:
            text = input_file.read()
        return "text_extracted", "", len(clean_text(text))

    completed = subprocess.run(
        ["pdftotext", "-layout", "-enc", "UTF-8", pdf_path, "-"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if completed.returncode != 0:
        write_text_if_changed("", text_path)
        return "text_extract_failed", clean_text(completed.stderr) or f"pdftotext exited {completed.returncode}", 0

    write_text_if_changed(completed.stdout, text_path)
    text_char_count = len(clean_text(completed.stdout))
    if text_char_count == 0:
        return "empty_text", "", 0
    return "text_extracted", "", text_char_count


def read_csv_rows(path: str) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as input_file:
        return list(csv.DictReader(input_file))


def process_application_row(row_number: int, project_row: dict[str, str], application_row: dict[str, str]) -> dict[str, object]:
    parsed = parse_application_number(application_row["raw_application_number"])
    candidate_stems = [stem.strip() for stem in parsed["candidate_report_stems"].split(";") if stem.strip()]
    document_id = stable_id(project_row["project_id"], application_row["raw_application_number"], parsed["candidate_report_stems"])

    def pdf_path_for_stem(report_stem: str) -> str:
        return "../output/cpc_report_pdfs/" + "_".join([
            safe_filename_part(report_stem),
            safe_filename_part(project_row["project_id"]),
            safe_filename_part(application_row["raw_application_number"]),
            document_id[:8],
        ]) + ".pdf"

    download_status, download_error, downloaded_report_stem, source_doc, report_source_type = download_pdf(candidate_stems, pdf_path_for_stem)
    zap_url = ""
    zap_action_lookup_status = ""
    zap_action_lookup_error = ""
    if download_status != "downloaded":
        zap_url, zap_action_lookup_status, zap_action_lookup_error = zap_action_cpc_url(project_row["project_id"], application_row["raw_application_number"])
        if zap_url:
            zap_report_stem = parsed["base_report_stem"] or safe_filename_part(application_row["raw_application_number"])
            zap_pdf_path = pdf_path_for_stem(f"{zap_report_stem}_zap")
            zap_download_status, zap_download_error = download_pdf_url(zap_url, zap_pdf_path)
            if zap_download_status == "downloaded":
                download_status = "downloaded"
                download_error = ""
                downloaded_report_stem = zap_report_stem
                source_doc = zap_url
                report_source_type = "zap_action_cpc_report"
            else:
                download_error = " | ".join(part for part in [download_error, zap_download_error] if part)
    local_file_stem = downloaded_report_stem
    if report_source_type == "zap_action_cpc_report" and downloaded_report_stem:
        local_file_stem = f"{downloaded_report_stem}_zap"
    local_pdf_path = pdf_path_for_stem(local_file_stem) if local_file_stem else ""
    local_text_path = ""
    text_status = ""
    text_error = ""
    text_char_count = 0
    text_manifest_row = None
    failure_row = None

    if download_status == "downloaded":
        local_text_path = "../output/cpc_report_text/" + "_".join([
            safe_filename_part(local_file_stem),
            safe_filename_part(project_row["project_id"]),
            safe_filename_part(application_row["raw_application_number"]),
            document_id[:8],
        ]) + ".txt"
        text_status, text_error, text_char_count = extract_pdf_text(local_pdf_path, local_text_path)
        if text_status == "text_extracted":
            text_manifest_row = {
                "document_id": document_id,
                "project_id": project_row["project_id"],
                "raw_application_number": application_row["raw_application_number"],
            "downloaded_report_stem": downloaded_report_stem,
            "report_source_type": report_source_type,
            "source_doc": source_doc,
            "local_pdf_path": local_pdf_path,
                "local_text_path": local_text_path,
                "text_char_count": text_char_count,
            }

    if download_status != "downloaded" or text_status in {"text_extract_failed", "empty_text"}:
        failure_row = {
            "document_id": document_id,
            "project_id": project_row["project_id"],
            "project_name": project_row.get("project_name", ""),
            "corpus_reference_year": project_row["corpus_reference_year"],
            "raw_application_number": application_row["raw_application_number"],
            "candidate_report_stems": parsed["candidate_report_stems"],
            "stage": "download" if download_status != "downloaded" else "text_extract",
            "failure_reason": download_error or text_error or text_status,
            "candidate_urls": "; ".join([report_url(stem) for stem in candidate_stems] + ([zap_url] if zap_url else [])),
        }

    manifest_row = {
        "document_id": document_id,
        "project_id": project_row["project_id"],
        "project_name": project_row.get("project_name", ""),
        "corpus_reference_year": project_row["corpus_reference_year"],
        "corpus_reference_date": project_row["corpus_reference_date"],
        "raw_application_number": application_row["raw_application_number"],
        "application_key": application_row.get("application_key", ""),
        "application_prefix": application_row.get("application_prefix", ""),
        "application_digits": application_row.get("application_digits", ""),
        "parsed_action_code": parsed["parsed_action_code"],
        "parsed_borough_code": parsed["parsed_borough_code"],
        "parsed_amendment_letter": parsed["parsed_amendment_letter"],
        "base_report_stem": parsed["base_report_stem"],
        "candidate_report_stems": parsed["candidate_report_stems"],
        "downloaded_report_stem": downloaded_report_stem,
        "report_source_type": report_source_type,
        "source_doc": source_doc,
        "local_pdf_path": local_pdf_path,
        "local_text_path": local_text_path,
        "download_status": download_status,
        "download_error": download_error,
        "zap_action_lookup_status": zap_action_lookup_status,
        "zap_action_lookup_error": zap_action_lookup_error,
        "text_status": text_status,
        "text_error": text_error,
        "text_char_count": text_char_count,
        "ceqr_number": project_row.get("ceqr_number", ""),
        "actions": project_row.get("actions", ""),
        "applicant_type": project_row.get("applicant_type", ""),
        "primary_applicant": project_row.get("primary_applicant", ""),
        "borough_name": project_row.get("borough_name", ""),
        "community_district": project_row.get("community_district", ""),
        "project_page_url": project_row.get("project_page_url", ""),
    }

    return {
        "row_number": row_number,
        "manifest_row": manifest_row,
        "text_manifest_row": text_manifest_row,
        "failure_row": failure_row,
    }


def main() -> None:
    if len(sys.argv) != 5:
        raise RuntimeError("Usage: python3 build_ulurp_cpc_report_corpus.py <start_year> <end_year> <report_limit> <worker_count>")

    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    report_limit = int(sys.argv[3])
    worker_count = int(sys.argv[4])
    if start_year > end_year:
        raise RuntimeError("start_year cannot exceed end_year.")
    if worker_count < 1:
        raise RuntimeError("worker_count must be positive.")

    project_rows = read_csv_rows("../input/ulurp_corpus_project_spine.csv")
    application_rows = read_csv_rows("../input/ulurp_corpus_application_spine.csv")
    assert_unique_keys(project_rows, ["project_id"], "ULURP project spine")
    assert_unique_keys(application_rows, ["project_id", "raw_application_number"], "ULURP application spine")

    projects_by_id = {row["project_id"]: row for row in project_rows}
    selected_rows = []
    for application_row in application_rows:
        project_row = projects_by_id.get(application_row["project_id"])
        if project_row is None:
            raise RuntimeError(f"Application row has no project spine row: {application_row['project_id']}")
        corpus_reference_year = int(project_row["corpus_reference_year"])
        if start_year <= corpus_reference_year <= end_year:
            selected_rows.append((project_row, application_row))

    selected_rows.sort(key=lambda pair: (int(pair[0]["corpus_reference_year"]), pair[0]["project_id"], pair[1]["raw_application_number"]))
    if report_limit > 0:
        selected_rows = selected_rows[:report_limit]

    result_rows = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(process_application_row, row_number, project_row, application_row)
            for row_number, (project_row, application_row) in enumerate(selected_rows, start=1)
        ]
        for completed_count, future in enumerate(as_completed(futures), start=1):
            result_rows.append(future.result())
            if completed_count == 1 or completed_count % 100 == 0 or completed_count == len(selected_rows):
                print(f"Processed {completed_count}/{len(selected_rows)} CPC report application rows", flush=True)

    result_rows.sort(key=lambda row: int(row["row_number"]))
    manifest_rows = [row["manifest_row"] for row in result_rows]
    text_manifest_rows = [row["text_manifest_row"] for row in result_rows if row["text_manifest_row"] is not None]
    failure_rows = [row["failure_row"] for row in result_rows if row["failure_row"] is not None]

    selected_project_ids = {pair[0]["project_id"] for pair in selected_rows}
    source_project_count = sum(start_year <= int(row["corpus_reference_year"]) <= end_year for row in project_rows)
    missing_number_project_count = sum(
        start_year <= int(row["corpus_reference_year"]) <= end_year and str(row.get("has_ulurp_number", "")).upper() != "TRUE"
        for row in project_rows
    )
    downloaded_count = sum(row["download_status"] == "downloaded" for row in manifest_rows)
    text_count = sum(row["text_status"] == "text_extracted" for row in manifest_rows)

    qc_rows = [
        {
            "metric": "start_year",
            "value": start_year,
            "status": "pass",
            "note": "Lower year bound used for this CPC report corpus build.",
        },
        {
            "metric": "end_year",
            "value": end_year,
            "status": "pass",
            "note": "Upper year bound used for this CPC report corpus build.",
        },
        {
            "metric": "source_ulurp_project_count",
            "value": source_project_count,
            "status": "pass" if source_project_count > 0 else "fail",
            "note": "ULURP project spine rows in the requested year range.",
        },
        {
            "metric": "source_ulurp_project_missing_number_count",
            "value": missing_number_project_count,
            "status": "pass",
            "note": "ULURP project rows in range that cannot enter the CPC report URL build because ulurp_numbers is missing.",
        },
        {
            "metric": "attempted_application_report_count",
            "value": len(manifest_rows),
            "status": "pass" if len(manifest_rows) > 0 else "fail",
            "note": "Parsed ULURP application-number rows attempted for CPC report download.",
        },
        {
            "metric": "attempted_project_count",
            "value": len(selected_project_ids),
            "status": "pass" if selected_project_ids else "fail",
            "note": "Distinct projects represented by attempted application-number rows.",
        },
        {
            "metric": "downloaded_report_count",
            "value": downloaded_count,
            "status": "pass" if downloaded_count > 0 else "fail",
            "note": "Application rows with a downloaded CPC report PDF.",
        },
        {
            "metric": "text_extracted_report_count",
            "value": text_count,
            "status": "pass" if text_count > 0 else "fail",
            "note": "Application rows with nonempty extracted CPC report text.",
        },
        {
            "metric": "failed_application_report_count",
            "value": len(failure_rows),
            "status": "pass",
            "note": "Application rows with failed report download or failed/empty text extraction.",
        },
    ]

    manifest_fieldnames = [
        "document_id",
        "project_id",
        "project_name",
        "corpus_reference_year",
        "corpus_reference_date",
        "raw_application_number",
        "application_key",
        "application_prefix",
        "application_digits",
        "parsed_action_code",
        "parsed_borough_code",
        "parsed_amendment_letter",
        "base_report_stem",
        "candidate_report_stems",
        "downloaded_report_stem",
        "report_source_type",
        "source_doc",
        "local_pdf_path",
        "local_text_path",
        "download_status",
        "download_error",
        "zap_action_lookup_status",
        "zap_action_lookup_error",
        "text_status",
        "text_error",
        "text_char_count",
        "ceqr_number",
        "actions",
        "applicant_type",
        "primary_applicant",
        "borough_name",
        "community_district",
        "project_page_url",
    ]
    text_manifest_fieldnames = [
        "document_id",
        "project_id",
        "raw_application_number",
        "downloaded_report_stem",
        "report_source_type",
        "source_doc",
        "local_pdf_path",
        "local_text_path",
        "text_char_count",
    ]
    failure_fieldnames = [
        "document_id",
        "project_id",
        "project_name",
        "corpus_reference_year",
        "raw_application_number",
        "candidate_report_stems",
        "stage",
        "failure_reason",
        "candidate_urls",
    ]

    write_csv_if_changed(manifest_rows, manifest_fieldnames, "../output/ulurp_cpc_report_manifest.csv")
    write_csv_if_changed(text_manifest_rows, text_manifest_fieldnames, "../output/ulurp_cpc_report_text_manifest.csv")
    write_csv_if_changed(failure_rows, failure_fieldnames, "../output/ulurp_cpc_report_fetch_failures.csv")
    write_csv_if_changed(qc_rows, ["metric", "value", "status", "note"], "../output/ulurp_cpc_report_corpus_qc.csv")

    if any(row["status"] == "fail" for row in qc_rows):
        print("ULURP CPC report corpus QC failed.", file=sys.stderr)
        sys.exit(1)

    print("Wrote ULURP CPC report corpus outputs to ../output")


if __name__ == "__main__":
    main()
