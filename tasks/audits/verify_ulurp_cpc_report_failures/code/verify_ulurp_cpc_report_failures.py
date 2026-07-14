#!/usr/bin/env python3

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/verify_ulurp_cpc_report_failures/code")

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import subprocess
import time
import urllib.parse


ZAP_API_HOST = "https://zap-api-production.herokuapp.com"
ZAP_PROJECT_INCLUDE = "actions"
CURL_CONNECT_TIMEOUT_SECONDS = 10
CURL_MAX_TIME_SECONDS = 120
DOWNLOAD_ATTEMPTS = 2
DOWNLOAD_RETRY_SLEEP_SECONDS = 2
API_FETCH_ATTEMPTS = 3
API_RETRY_SLEEP_SECONDS = 3
HTTP_STATUS_MARKER = "\n__HTTP_STATUS__:"


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


def read_csv_rows(path: str) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as input_file:
        return list(csv.DictReader(input_file))


def is_pdf_file(path: str) -> bool:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return False
    with open(path, "rb") as input_file:
        return input_file.read(4) == b"%PDF"


def ulurp_pdf_stem(ulurp_number: str) -> str:
    compact = re.sub(r"\s+", "", ulurp_number or "")
    digits = re.search(r"\d{6}", compact)
    if not digits:
        return ""
    stem = digits.group(0)
    after_digits = compact[digits.end():]
    amendment = re.search(r"\(([A-Za-z])\)", after_digits)
    if amendment:
        stem = f"{stem}{amendment.group(1).lower()}"
    return stem


def comparable_ulurp_number(value: str) -> str:
    compact = re.sub(r"[^A-Za-z0-9]", "", value or "").upper()
    if re.match(r"^[CNMI]\d{6}", compact):
        return compact[1:]
    return compact


def split_candidate_urls(value: str) -> list[str]:
    return [clean_text(part) for part in re.split(r"\s*;\s*", value or "") if clean_text(part)]


def should_retry(http_status: str, curl_returncode: int) -> bool:
    return curl_returncode != 0 or http_status in {"000", "403", "408", "425", "429", "500", "502", "503", "504"}


def recheck_pdf_url(url: str, output_path: str) -> dict[str, object]:
    failure_notes = []
    if is_pdf_file(output_path):
        return {
            "pdf_found": True,
            "http_status": "cached",
            "curl_exit": 0,
            "failure_notes": "",
            "local_pdf_path": output_path,
        }
    if os.path.exists(output_path):
        os.remove(output_path)

    temp_path = f"{output_path}.tmp"
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        if os.path.exists(temp_path):
            os.remove(temp_path)

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
                temp_path,
                "--write-out",
                "%{http_code}",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=CURL_MAX_TIME_SECONDS + 10,
            check=False,
        )
        http_status = clean_text(completed.stdout)[-3:]
        stderr = clean_text(completed.stderr)
        if completed.returncode == 0 and http_status == "200" and is_pdf_file(temp_path):
            os.replace(temp_path, output_path)
            return {
                "pdf_found": True,
                "http_status": http_status,
                "curl_exit": completed.returncode,
                "failure_notes": "",
                "local_pdf_path": output_path,
            }

        if os.path.exists(temp_path):
            os.remove(temp_path)

        note = (
            f"attempt {attempt}/{DOWNLOAD_ATTEMPTS} "
            f"curl_exit={completed.returncode} http_status={http_status or 'missing'}"
        )
        if stderr:
            note = f"{note} stderr={stderr}"
        if completed.returncode == 0 and http_status == "200":
            note = f"{note} non_pdf_or_empty_response"
        failure_notes.append(note)

        if attempt < DOWNLOAD_ATTEMPTS and should_retry(http_status, completed.returncode):
            time.sleep(DOWNLOAD_RETRY_SLEEP_SECONDS)
        else:
            break

    return {
        "pdf_found": False,
        "http_status": http_status or "",
        "curl_exit": completed.returncode,
        "failure_notes": " | ".join(failure_notes),
        "local_pdf_path": "",
    }


def fetch_project(project_id: str) -> tuple[str, str, dict[str, object]]:
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
                f"{HTTP_STATUS_MARKER}%{{http_code}}",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=CURL_MAX_TIME_SECONDS + 10,
            check=False,
        )
        if HTTP_STATUS_MARKER not in completed.stdout:
            result = ("curl_error", clean_text(completed.stderr) or f"curl exited {completed.returncode}", {})
        else:
            response_text, http_status_text = completed.stdout.rsplit(HTTP_STATUS_MARKER, 1)
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

    return result


def sharepoint_server_relative_url(absolute_url: str) -> str:
    if not absolute_url:
        return ""
    parsed = urllib.parse.urlparse(absolute_url)
    if parsed.netloc.lower() != "nyco365.sharepoint.com":
        return urllib.parse.unquote(parsed.path)
    return urllib.parse.unquote(parsed.path)


def zap_action_cpc_url(data: dict[str, object], raw_application_number: str) -> str:
    target_number = comparable_ulurp_number(raw_application_number)
    for row in data.get("included", []):
        if row.get("type") != "actions":
            continue
        attrs = row.get("attributes", {})
        ulurp_number = clean_text(attrs.get("dcp-ulurpnumber"))
        if comparable_ulurp_number(ulurp_number) != target_number:
            continue
        relative = sharepoint_server_relative_url(clean_text(attrs.get("dcp-spabsoluteurl")))
        stem = ulurp_pdf_stem(ulurp_number or raw_application_number)
        if relative and stem:
            return f"{ZAP_API_HOST}/document/projectaction{urllib.parse.quote(relative)}/{stem}.pdf"
    return ""


def main() -> None:
    failure_rows = read_csv_rows("../input/ulurp_cpc_report_fetch_failures.csv")
    manifest_rows = read_csv_rows("../input/ulurp_cpc_report_manifest.csv")
    project_rows = read_csv_rows("../input/ulurp_corpus_project_spine.csv")
    manifest_by_document_id = {row["document_id"]: row for row in manifest_rows}
    projects_by_id = {row["project_id"]: row for row in project_rows}

    recheck_rows = []
    project_api_cache: dict[str, tuple[str, str, dict[str, object]]] = {}

    for row_number, failure_row in enumerate(failure_rows, start=1):
        manifest_row = manifest_by_document_id.get(failure_row["document_id"], {})
        project_row = projects_by_id.get(failure_row["project_id"], {})
        candidate_urls = split_candidate_urls(failure_row.get("candidate_urls", ""))

        official_url_results = []
        official_http_statuses = []
        official_pdf_found = False
        recovered_official_url = ""
        recovered_official_pdf_path = ""
        for candidate_url in candidate_urls:
            recovery_id = stable_id(failure_row["document_id"], candidate_url)
            output_path = "../output/recovered_cpc_report_pdfs/" + "_".join([
                safe_filename_part(failure_row.get("project_id", "")),
                safe_filename_part(failure_row.get("raw_application_number", "")),
                recovery_id[:8],
            ]) + ".pdf"
            result = recheck_pdf_url(candidate_url, output_path)
            official_http_statuses.append(str(result["http_status"]))
            official_url_results.append(
                f"{candidate_url} http_status={result['http_status']} "
                f"curl_exit={result['curl_exit']} pdf_found={result['pdf_found']}"
            )
            if result["failure_notes"]:
                official_url_results.append(str(result["failure_notes"]))
            if result["pdf_found"] and not official_pdf_found:
                official_pdf_found = True
                recovered_official_url = candidate_url
                recovered_official_pdf_path = str(result["local_pdf_path"])

        api_fetch_status = ""
        api_fetch_error = ""
        zap_url = ""
        zap_pdf_found = False
        zap_http_status = ""
        zap_pdf_path = ""
        if failure_row.get("project_id", "") not in project_api_cache:
            project_api_cache[failure_row.get("project_id", "")] = fetch_project(failure_row.get("project_id", ""))
        api_fetch_status, api_fetch_error, api_data = project_api_cache[failure_row.get("project_id", "")]
        if api_fetch_status == "success":
            zap_url = zap_action_cpc_url(api_data, failure_row.get("raw_application_number", ""))
            if zap_url:
                recovery_id = stable_id(failure_row["document_id"], zap_url)
                zap_output_path = "../output/recovered_cpc_report_pdfs/" + "_".join([
                    safe_filename_part(failure_row.get("project_id", "")),
                    safe_filename_part(failure_row.get("raw_application_number", "")),
                    "zap",
                    recovery_id[:8],
                ]) + ".pdf"
                zap_result = recheck_pdf_url(zap_url, zap_output_path)
                zap_pdf_found = bool(zap_result["pdf_found"])
                zap_http_status = str(zap_result["http_status"])
                zap_pdf_path = str(zap_result["local_pdf_path"])

        if failure_row.get("stage") == "text_extract":
            final_status = "downloaded_but_no_extractable_text"
        elif official_pdf_found:
            final_status = "recoverable_from_official_cpc_url"
        elif zap_pdf_found:
            final_status = "recoverable_from_zap_action_cpc_url"
        elif (
            candidate_urls
            and official_http_statuses
            and all(status == "404" for status in official_http_statuses)
            and api_fetch_status == "success"
            and not zap_pdf_found
        ):
            final_status = "confirmed_missing_from_checked_cpc_urls"
        else:
            final_status = "not_recovered_uncertain_status"

        recheck_rows.append(
            {
                "document_id": failure_row.get("document_id", ""),
                "project_id": failure_row.get("project_id", ""),
                "project_name": failure_row.get("project_name", ""),
                "corpus_reference_year": failure_row.get("corpus_reference_year", ""),
                "raw_application_number": failure_row.get("raw_application_number", ""),
                "stage": failure_row.get("stage", ""),
                "original_failure_reason": failure_row.get("failure_reason", ""),
                "candidate_urls": failure_row.get("candidate_urls", ""),
                "official_url_recheck_results": " | ".join(official_url_results),
                "official_pdf_found": official_pdf_found,
                "recovered_official_url": recovered_official_url,
                "recovered_official_pdf_path": recovered_official_pdf_path,
                "api_fetch_status": api_fetch_status,
                "api_fetch_error": api_fetch_error,
                "zap_action_cpc_url": zap_url,
                "zap_http_status": zap_http_status,
                "zap_pdf_found": zap_pdf_found,
                "zap_pdf_path": zap_pdf_path,
                "manifest_download_status": manifest_row.get("download_status", ""),
                "manifest_text_status": manifest_row.get("text_status", ""),
                "manifest_source_doc": manifest_row.get("source_doc", ""),
                "project_page_url": project_row.get("project_page_url", ""),
                "final_verification_status": final_status,
            }
        )

        if row_number == 1 or row_number % 100 == 0 or row_number == len(failure_rows):
            print(f"Rechecked {row_number}/{len(failure_rows)} CPC report failure rows", flush=True)

    status_counts: dict[tuple[str, str], int] = {}
    for row in recheck_rows:
        key = (str(row["corpus_reference_year"]), str(row["final_verification_status"]))
        status_counts[key] = status_counts.get(key, 0) + 1
    by_year_rows = [
        {
            "corpus_reference_year": year,
            "final_verification_status": status,
            "failure_count": count,
        }
        for (year, status), count in sorted(status_counts.items())
    ]

    recovered_count = sum(
        row["final_verification_status"] in {"recoverable_from_official_cpc_url", "recoverable_from_zap_action_cpc_url"}
        for row in recheck_rows
    )
    uncertain_count = sum(row["final_verification_status"] == "not_recovered_uncertain_status" for row in recheck_rows)
    qc_rows = [
        {
            "metric": "input_failure_count",
            "value": len(failure_rows),
            "status": "pass",
            "note": "Rows read from the CPC report corpus failure manifest.",
        },
        {
            "metric": "rechecked_failure_count",
            "value": len(recheck_rows),
            "status": "pass" if len(recheck_rows) == len(failure_rows) else "fail",
            "note": "Rows written by the independent failure verifier.",
        },
        {
            "metric": "recoverable_failure_count",
            "value": recovered_count,
            "status": "pass" if recovered_count == 0 else "fail",
            "note": "Rows where an official or ZAP action CPC PDF was recovered during verification.",
        },
        {
            "metric": "uncertain_failure_count",
            "value": uncertain_count,
            "status": "pass" if uncertain_count == 0 else "warn",
            "note": "Rows not recovered but not cleanly classified as checked-source missing or no extractable text.",
        },
    ]

    fieldnames = [
        "document_id",
        "project_id",
        "project_name",
        "corpus_reference_year",
        "raw_application_number",
        "stage",
        "original_failure_reason",
        "candidate_urls",
        "official_url_recheck_results",
        "official_pdf_found",
        "recovered_official_url",
        "recovered_official_pdf_path",
        "api_fetch_status",
        "api_fetch_error",
        "zap_action_cpc_url",
        "zap_http_status",
        "zap_pdf_found",
        "zap_pdf_path",
        "manifest_download_status",
        "manifest_text_status",
        "manifest_source_doc",
        "project_page_url",
        "final_verification_status",
    ]
    write_csv_if_changed(recheck_rows, fieldnames, "../output/ulurp_cpc_report_failure_recheck.csv")
    write_csv_if_changed(by_year_rows, ["corpus_reference_year", "final_verification_status", "failure_count"], "../output/ulurp_cpc_report_failure_recheck_by_year.csv")
    write_csv_if_changed(qc_rows, ["metric", "value", "status", "note"], "../output/ulurp_cpc_report_failure_recheck_qc.csv")

    if any(row["status"] == "fail" for row in qc_rows):
        print("ULURP CPC report failure verification found recoverable failures.", flush=True)

    print("Wrote ULURP CPC report failure verification outputs to ../output")


if __name__ == "__main__":
    main()
