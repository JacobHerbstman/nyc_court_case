#!/usr/bin/env python3

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/sample_ulurp_corpus_documents/code")
# sample_seed = 20260702
# documents_per_cohort = 10

from __future__ import annotations

import csv
import hashlib
import html
import io
import json
import os
import random
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse


ZAP_API_HOST = "https://zap-api-production.herokuapp.com"
ZAP_PROJECT_INCLUDE = (
    "actions,milestones,dispositions,dispositions.action,users,"
    "assignments.user,packages,artifacts"
)
CURL_CONNECT_TIMEOUT_SECONDS = 10
CURL_MAX_TIME_SECONDS = 120
CURL_HTTP_STATUS_MARKER = "\n__HTTP_STATUS__:"
REQUEST_SLEEP_SECONDS = 0.20


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


def ulurp_pdf_stem(ulurp_number: str) -> str:
    compact = re.sub(r"\s+", "", ulurp_number or "")
    digits = re.search(r"\d{6}", compact)
    if not digits:
        digits = re.search(r"\d+", compact)
    if not digits:
        return ""
    stem = digits.group(0)
    after = compact[digits.end():]
    amendment = re.search(r"\(([A-Za-z])\)", after)
    if amendment:
        stem = f"{stem}{amendment.group(1).lower()}"
    return stem


def split_ulurp_numbers(value: str) -> list[str]:
    return [
        clean_text(part).upper()
        for part in re.split(r"\s*;\s*", value or "")
        if clean_text(part)
    ]


def sharepoint_server_relative_url(absolute_url: str) -> str:
    if not absolute_url:
        return ""
    parsed = urllib.parse.urlparse(absolute_url)
    if parsed.netloc.lower() != "nyco365.sharepoint.com":
        return parsed.path
    return urllib.parse.unquote(parsed.path)


def cpc_report_url(ulurp_number: str, absolute_url: str) -> str:
    stem = ulurp_pdf_stem(ulurp_number)
    relative = sharepoint_server_relative_url(absolute_url)
    if not stem or not relative:
        return ""
    return f"{ZAP_API_HOST}/document/projectaction{urllib.parse.quote(relative)}/{stem}.pdf"


def nycgov_cpc_report_url(ulurp_number: str) -> str:
    stem = ulurp_pdf_stem(ulurp_number)
    if not stem:
        return ""
    return f"https://www.nyc.gov/assets/planning/download/pdf/about/cpc/{stem}.pdf"


def document_family(source_type: str, container_title: str, document_title: str, action_code: str = "") -> str:
    text = f"{source_type} {container_title} {document_title} {action_code}".upper()
    if source_type.startswith("cpc_report"):
        return "cpc_report"
    if "FILED LU" in text or "LAND USE APPLICATION" in text or re.search(r"\bAPPLICATION\b", text):
        return "land_use_application"
    if "PROJECT DESCRIPTION" in text:
        return "project_description"
    if "LAND USE" in text:
        return "land_use"
    if "FINAL ENVIRONMENTAL IMPACT" in text or "FEIS" in text:
        return "final_eis"
    if "DRAFT ENVIRONMENTAL IMPACT" in text or "DEIS" in text:
        return "draft_eis"
    if "ENVIRONMENTAL ASSESSMENT" in text or re.search(r"\bEAS\b", text):
        return "eas"
    if "TECHNICAL MEMO" in text:
        return "technical_memo"
    if "POINTS OF AGREEMENT" in text or re.search(r"\bPOA\b", text):
        return "points_of_agreement"
    if "RECOMMEND" in text or source_type == "recommendation_document":
        return "recommendation"
    if "ZONING" in text or action_code in {"ZM", "ZR"}:
        return "zoning_document"
    if source_type == "docket_description":
        return "docket_description"
    return "other_public_document"


def source_priority(row: dict[str, object]) -> int:
    family = str(row.get("document_family", ""))
    source_type = str(row.get("source_type", ""))
    title = str(row.get("document_title", "")).upper()
    action_code = str(row.get("action_code", ""))

    if family == "land_use_application":
        return 1
    if family in {"project_description", "land_use"}:
        return 2
    if source_type.startswith("cpc_report"):
        return 3
    if family in {"zoning_document", "points_of_agreement"}:
        return 4
    if family in {"final_eis", "draft_eis", "eas", "technical_memo"} and (
        "PROJECT DESCRIPTION" in title or "LAND USE" in title or "EXECUTIVE" in title
    ):
        return 5
    if family in {"final_eis", "draft_eis", "eas", "technical_memo"}:
        return 6
    if family == "recommendation":
        return 7
    if action_code in {"ZM", "ZR", "ZS", "HA", "HD", "HG", "MM", "PP", "PQ"}:
        return 8
    return 9


def fetch_project(project_id: str) -> tuple[str, int, dict[str, object]]:
    encoded_id = urllib.parse.quote(project_id)
    url = f"{ZAP_API_HOST}/projects/{encoded_id}?include={urllib.parse.quote(ZAP_PROJECT_INCLUDE, safe=',')}"
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
    if completed.returncode != 0:
        raise urllib.error.URLError(clean_text(completed.stderr) or f"curl exited {completed.returncode}")
    if CURL_HTTP_STATUS_MARKER not in completed.stdout:
        raise urllib.error.URLError("curl response did not include an HTTP status marker")

    response_text, http_status_text = completed.stdout.rsplit(CURL_HTTP_STATUS_MARKER, 1)
    http_status = int(http_status_text.strip()[:3])
    if http_status >= 400:
        raise urllib.error.HTTPError(url, http_status, clean_text(completed.stderr) or response_text[:500], None, None)
    return url, http_status, json.loads(response_text)


def included_rows(data: dict[str, object], record_type: str) -> list[dict[str, object]]:
    return [row for row in data.get("included", []) if row.get("type") == record_type]


def add_link(link_rows: list[dict[str, object]], row: dict[str, object]) -> None:
    row["document_family"] = document_family(
        str(row["source_type"]),
        str(row.get("source_container_title", "")),
        str(row.get("document_title", "")),
        str(row.get("action_code", "")),
    )
    row["source_priority"] = source_priority(row)
    link_rows.append(row)


def dedupe_links(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    deduped_rows = []
    seen = set()
    for row in rows:
        key = (row.get("project_id", ""), row.get("document_url", ""), row.get("document_title", ""))
        if key in seen:
            continue
        seen.add(key)
        deduped_rows.append(row)
    return deduped_rows


def fallback_cpc_links(project_row: dict[str, str], base_source_row: dict[str, object]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for ulurp_number in split_ulurp_numbers(project_row.get("ulurp_numbers", "")):
        fallback_url = nycgov_cpc_report_url(ulurp_number)
        if not fallback_url:
            continue
        action_match = re.search(r"([A-Z]{2})[A-Z]?$", re.sub(r"\s+", "", ulurp_number))
        add_link(
            rows,
            {
                **base_source_row,
                "source_type": "cpc_report_nycgov_fallback",
                "source_container_id": ulurp_number,
                "source_container_title": "CPC report fallback from ULURP number",
                "document_title": f"{ulurp_number} CPC report nyc.gov fallback",
                "document_url": fallback_url,
                "action_code": action_match.group(1) if action_match else "",
                "ulurp_number": ulurp_number,
                "document_created_at": "",
            },
        )
    return rows


def discover_links(project_row: dict[str, str]) -> tuple[list[dict[str, object]], str, str, str]:
    project_id = project_row["project_id"]
    project_page_url = f"https://zap.planning.nyc.gov/projects/{urllib.parse.quote(project_id)}"
    api_url = ""
    fetch_status = "success"
    fetch_error = ""
    data: dict[str, object] = {}

    try:
        api_url, _, data = fetch_project(project_id)
    except urllib.error.HTTPError as error:
        fetch_status = f"http_error_{error.code}"
        fetch_error = clean_text(getattr(error, "reason", str(error)))
    except urllib.error.URLError as error:
        fetch_status = "url_error"
        fetch_error = clean_text(error.reason)
    except (json.JSONDecodeError, TimeoutError, subprocess.TimeoutExpired) as error:
        fetch_status = "parse_or_timeout_error"
        fetch_error = clean_text(error)

    project_attrs = data.get("data", {}).get("attributes", {}) if data else {}
    project_name = clean_text(project_attrs.get("dcp-projectname")) or project_row.get("project_name", "")
    ceqr_number = clean_text(project_attrs.get("dcp-ceqrnumber")) or project_row.get("ceqr_number", "")
    base_source_row = {
        "cohort": project_row.get("manual_sample_cohort", ""),
        "project_id": project_id,
        "project_name": project_name,
        "corpus_reference_year": project_row.get("corpus_reference_year", ""),
        "api_url": api_url or f"{ZAP_API_HOST}/projects/{urllib.parse.quote(project_id)}",
        "project_page_url": project_page_url,
        "ceqr_number": ceqr_number,
        "fetch_status": fetch_status,
        "fetch_error": fetch_error,
    }

    link_rows = fallback_cpc_links(project_row, base_source_row)

    for action in included_rows(data, "actions"):
        attrs = action.get("attributes", {})
        ulurp_number = clean_text(attrs.get("dcp-ulurpnumber"))
        action_code = clean_text(attrs.get("dcp-action-value"))
        action_title = clean_text(attrs.get("dcp-name"))

        action_report = cpc_report_url(ulurp_number, attrs.get("dcp-spabsoluteurl"))
        if action_report:
            add_link(
                link_rows,
                {
                    **base_source_row,
                    "source_type": "cpc_report",
                    "source_container_id": action.get("id", ""),
                    "source_container_title": action_title,
                    "document_title": f"{ulurp_number} CPC report",
                    "document_url": action_report,
                    "action_code": action_code,
                    "ulurp_number": ulurp_number,
                    "document_created_at": "",
                },
            )

    for record_type, url_prefix, source_type in [
        ("packages", "/document/package", "public_package_document"),
        ("artifacts", "/document/artifact", "public_artifact_document"),
        ("dispositions", "/document/disposition", "recommendation_document"),
    ]:
        for record in included_rows(data, record_type):
            attrs = record.get("attributes", {})
            container_title = clean_text(attrs.get("dcp-name"))
            for document in attrs.get("documents") or []:
                server_relative_url = clean_text(document.get("serverRelativeUrl"))
                if not server_relative_url:
                    continue
                add_link(
                    link_rows,
                    {
                        **base_source_row,
                        "source_type": source_type,
                        "source_container_id": record.get("id", ""),
                        "source_container_title": container_title,
                        "document_title": clean_text(document.get("name")),
                        "document_url": f"{ZAP_API_HOST}{url_prefix}{urllib.parse.quote(server_relative_url)}",
                        "action_code": clean_text(attrs.get("dcp-projectaction-value")),
                        "ulurp_number": "",
                        "document_created_at": clean_text(document.get("timeCreated")),
                    },
                )

    return dedupe_links(link_rows), fetch_status, fetch_error, api_url


def download_document(url: str, output_path: str) -> tuple[str, str]:
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
            output_path,
            url,
        ],
        capture_output=True,
        text=True,
        timeout=CURL_MAX_TIME_SECONDS + 10,
        check=False,
    )
    if completed.returncode != 0:
        return "download_failed", clean_text(completed.stderr) or f"curl exited {completed.returncode}"
    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
        return "download_failed", "downloaded file is missing or empty"
    return "downloaded", ""


def extract_document_text(document_path: str, text_path: str) -> tuple[str, str, str]:
    with open(document_path, "rb") as input_file:
        head = input_file.read(8)

    if head.startswith(b"%PDF"):
        completed = subprocess.run(
            ["pdftotext", "-layout", "-enc", "UTF-8", document_path, "-"],
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

    with open(document_path, "rb") as input_file:
        raw_text = input_file.read().decode("utf-8", errors="ignore")
    text = clean_text(re.sub(r"<[^>]+>", " ", html.unescape(raw_text)))
    write_text_if_changed(text, text_path)
    if text == "":
        return text, "empty_text", "downloaded file was not PDF and had no readable text"
    return text, "plain_or_html_text_extracted", ""


def read_project_spine() -> list[dict[str, str]]:
    with open("../input/ulurp_corpus_project_spine.csv", "r", encoding="utf-8", newline="") as input_file:
        rows = list(csv.DictReader(input_file))

    return [
        row for row in rows
        if row.get("manual_sample_cohort") in {"1990s", "early_mid_2000s", "2015_onward"}
        and clean_text(row.get("ulurp_numbers", "")) != ""
    ]


if len(sys.argv) != 3:
    raise RuntimeError("Usage: python3 sample_ulurp_corpus_documents.py <sample_seed> <documents_per_cohort>")

sample_seed = int(sys.argv[1])
documents_per_cohort = int(sys.argv[2])
os.makedirs("../output/sample_documents", exist_ok=True)
if documents_per_cohort <= 0:
    raise RuntimeError("documents_per_cohort must be positive.")

project_rows = read_project_spine()
random_generator = random.Random(sample_seed)

index_rows: list[dict[str, object]] = []
text_rows: list[dict[str, object]] = []
failure_rows: list[dict[str, object]] = []
attempt_rows: list[dict[str, object]] = []

for cohort in ["1990s", "early_mid_2000s", "2015_onward"]:
    cohort_projects = [row for row in project_rows if row["manual_sample_cohort"] == cohort]
    random_generator.shuffle(cohort_projects)

    selected_in_cohort = 0
    for attempt_number, project_row in enumerate(cohort_projects, start=1):
        if selected_in_cohort >= documents_per_cohort:
            break
        if attempt_number > 1 or index_rows:
            time.sleep(REQUEST_SLEEP_SECONDS)

        links, fetch_status, fetch_error, api_url = discover_links(project_row)
        links.sort(key=lambda row: (int(row["source_priority"]), str(row.get("source_type", "")), str(row.get("document_title", ""))))
        downloadable_links = [
            link for link in links
            if str(link.get("source_type", "")) != "docket_description"
            and clean_text(link.get("document_url", "")) != ""
        ]

        attempt_rows.append(
            {
                "cohort": cohort,
                "project_id": project_row["project_id"],
                "attempt_number": attempt_number,
                "fetch_status": fetch_status,
                "candidate_link_count": len(downloadable_links),
            }
        )

        if not downloadable_links:
            failure_rows.append(
                {
                    "cohort": cohort,
                    "project_id": project_row["project_id"],
                    "project_name": project_row.get("project_name", ""),
                    "corpus_reference_year": project_row.get("corpus_reference_year", ""),
                    "stage": "link_discovery",
                    "source_doc": "",
                    "failure_reason": fetch_error or "no downloadable document links discovered",
                    "api_url": api_url,
                }
            )
            continue

        selected_link = None
        selected_pdf_path = ""
        selected_text_path = ""
        selected_download_status = ""
        selected_download_error = ""
        selected_text = ""
        selected_text_status = ""
        selected_text_error = ""

        for link in downloadable_links:
            document_id = stable_id(link.get("project_id", ""), link.get("document_url", ""), link.get("document_title", ""))
            filename_prefix = "_".join([
                f"{len(index_rows) + 1:02d}",
                safe_filename_part(cohort),
                safe_filename_part(project_row.get("corpus_reference_year", "")),
                safe_filename_part(project_row.get("project_id", "")),
                safe_filename_part(link.get("document_family", "")),
                safe_filename_part(link.get("ulurp_number", "") or link.get("document_title", "")),
                document_id[:8],
            ])
            pdf_path = f"../output/sample_documents/{filename_prefix}.pdf"
            text_path = f"../output/sample_documents/{filename_prefix}.txt"
            download_status, download_error = download_document(str(link["document_url"]), pdf_path)
            if download_status != "downloaded":
                failure_rows.append(
                    {
                        "cohort": cohort,
                        "project_id": project_row["project_id"],
                        "project_name": project_row.get("project_name", ""),
                        "corpus_reference_year": project_row.get("corpus_reference_year", ""),
                        "stage": "download",
                        "source_doc": link.get("document_url", ""),
                        "failure_reason": download_error,
                        "api_url": api_url,
                    }
                )
                continue

            selected_text, selected_text_status, selected_text_error = extract_document_text(pdf_path, text_path)
            selected_link = link
            selected_pdf_path = pdf_path
            selected_text_path = text_path
            selected_download_status = download_status
            selected_download_error = download_error
            break

        if selected_link is None:
            continue

        selected_in_cohort += 1
        sample_rank = len(index_rows) + 1
        index_rows.append(
            {
                "sample_rank": sample_rank,
                "cohort": cohort,
                "cohort_sample_rank": selected_in_cohort,
                "sample_seed": sample_seed,
                "project_id": project_row["project_id"],
                "project_name": project_row.get("project_name", ""),
                "project_brief": project_row.get("project_brief", ""),
                "corpus_reference_year": project_row.get("corpus_reference_year", ""),
                "ulurp_numbers": project_row.get("ulurp_numbers", ""),
                "actions": project_row.get("actions", ""),
                "ceqr_number": project_row.get("ceqr_number", ""),
                "applicant_type": project_row.get("applicant_type", ""),
                "primary_applicant": project_row.get("primary_applicant", ""),
                "borough_name": project_row.get("borough_name", ""),
                "community_district": project_row.get("community_district", ""),
                "source_type": selected_link.get("source_type", ""),
                "document_family": selected_link.get("document_family", ""),
                "source_priority": selected_link.get("source_priority", ""),
                "document_title": selected_link.get("document_title", ""),
                "source_doc": selected_link.get("document_url", ""),
                "action_code": selected_link.get("action_code", ""),
                "ulurp_number": selected_link.get("ulurp_number", ""),
                "document_created_at": selected_link.get("document_created_at", ""),
                "local_pdf_path": selected_pdf_path,
                "local_text_path": selected_text_path,
                "download_status": selected_download_status,
                "download_error": selected_download_error,
                "text_status": selected_text_status,
                "text_error": selected_text_error,
                "text_char_count": len(clean_text(selected_text)),
                "project_page_url": selected_link.get("project_page_url", ""),
                "api_url": selected_link.get("api_url", ""),
            }
        )
        text_rows.append(
            {
                "sample_rank": sample_rank,
                "cohort": cohort,
                "project_id": project_row["project_id"],
                "document_family": selected_link.get("document_family", ""),
                "document_title": selected_link.get("document_title", ""),
                "source_doc": selected_link.get("document_url", ""),
                "local_text_path": selected_text_path,
                "text_status": selected_text_status,
                "document_text": selected_text,
            }
        )

        print(f"Selected {selected_in_cohort}/{documents_per_cohort} for {cohort}", flush=True)

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
        "note": "Requested selected document count per cohort.",
    },
    {
        "metric": "selected_document_count",
        "value": len(index_rows),
        "status": "pass" if len(index_rows) == documents_per_cohort * 3 else "fail",
        "note": "Downloaded sample documents selected across all cohorts.",
    },
    {
        "metric": "download_failure_count",
        "value": sum(1 for row in failure_rows if row["stage"] == "download"),
        "status": "pass",
        "note": "Failed document download attempts retained for audit.",
    },
]

for cohort in ["1990s", "early_mid_2000s", "2015_onward"]:
    selected_count = sum(1 for row in index_rows if row["cohort"] == cohort)
    frame_count = sum(1 for row in project_rows if row["manual_sample_cohort"] == cohort)
    qc_rows.append(
        {
            "metric": f"selected_document_count_{cohort}",
            "value": selected_count,
            "status": "pass" if selected_count == documents_per_cohort else "fail",
            "note": f"Downloaded sample documents for {cohort}; sampling frame had {frame_count} projects with ULURP numbers.",
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
    "actions",
    "ceqr_number",
    "applicant_type",
    "primary_applicant",
    "borough_name",
    "community_district",
    "source_type",
    "document_family",
    "source_priority",
    "document_title",
    "source_doc",
    "action_code",
    "ulurp_number",
    "document_created_at",
    "local_pdf_path",
    "local_text_path",
    "download_status",
    "download_error",
    "text_status",
    "text_error",
    "text_char_count",
    "project_page_url",
    "api_url",
]

text_fieldnames = [
    "sample_rank",
    "cohort",
    "project_id",
    "document_family",
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
    "stage",
    "source_doc",
    "failure_reason",
    "api_url",
]

write_csv_if_changed(index_rows, index_fieldnames, "../output/ulurp_corpus_sample_document_index.csv")
write_csv_if_changed(text_rows, text_fieldnames, "../output/ulurp_corpus_sample_document_text.csv")
write_csv_if_changed(failure_rows, failure_fieldnames, "../output/ulurp_corpus_sample_document_fetch_failures.csv")
write_csv_if_changed(qc_rows, ["metric", "value", "status", "note"], "../output/ulurp_corpus_sample_document_qc.csv")

if any(row["status"] == "fail" for row in qc_rows):
    print("ULURP corpus sample document QC failed.", file=sys.stderr)
    sys.exit(1)

print("Wrote ULURP corpus sample document outputs to ../output")
