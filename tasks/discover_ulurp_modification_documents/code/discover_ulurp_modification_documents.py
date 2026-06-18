#!/usr/bin/env python3

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
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
REQUEST_SLEEP_SECONDS = 0.20
CURL_CONNECT_TIMEOUT_SECONDS = 10
PROJECT_FETCH_WALL_TIME_SECONDS = 35
CURL_HTTP_STATUS_MARKER = "\n__HTTP_STATUS__:"

KEYWORD_PATTERNS = [
    (
        "unit_quantity",
        1,
        re.compile(
            r"\b\d[\d,]*\s+(?:dwelling\s+)?units?\b|"
            r"\b\d[\d,]*\s+(?:homes?|apartments?)\b|"
            r"\b(?:dwelling|residential|affordable)\s+units?\b|"
            r"\bhousing units?\b|\bhomes?\b|\bapartments?\b",
            re.IGNORECASE,
        ),
    ),
    (
        "modification_signal",
        2,
        re.compile(
            r"\bmodified?\b|\bmodifications?\b|\brevised?\b|\breduced?\b|"
            r"\breduction\b|\bapproved with modifications\b|\bCity Council\b",
            re.IGNORECASE,
        ),
    ),
    (
        "affordability",
        3,
        re.compile(r"\baffordable\b|\bAMI\b|\binclusionary\b|\bMIH\b", re.IGNORECASE),
    ),
    (
        "height_or_bulk",
        4,
        re.compile(
            r"\bheight\b|\bstor(?:y|ies)\b|\bfloor area\b|\bFAR\b|"
            r"\bzoning floor area\b|\bsquare feet\b|\bgsf\b|\bzsf\b",
            re.IGNORECASE,
        ),
    ),
    ("parking", 5, re.compile(r"\bparking\b|\bspaces?\b|\bgarage\b", re.IGNORECASE)),
    (
        "cost_mitigation",
        6,
        re.compile(
            r"\bmitigation\b|\bcapital improvement\b|\binfrastructure\b|"
            r"\bsewer\b|\bstreet improvement\b|\btraffic mitigation\b|"
            r"\btransportation improvement\b|\bsidewalk\b|\bwater main\b",
            re.IGNORECASE,
        ),
    ),
    (
        "design",
        7,
        re.compile(r"\bdesign\b|\bopen space\b|\bsetback\b|\bfacade\b|\bbulk\b", re.IGNORECASE),
    ),
    (
        "local_benefit_commitment",
        8,
        re.compile(
            r"\bschool\b|\bpark\b|\btransit\b|\bjobs?\b|\blocal hiring\b|"
            r"\btenant\b|\bcommunity\b",
            re.IGNORECASE,
        ),
    ),
]


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def stable_id(*parts: object) -> str:
    text = "||".join(clean_text(part) for part in parts)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:20]


def truthy(value: object) -> bool:
    return str(value).strip().upper() in {"TRUE", "T", "1", "YES"}


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
    elif old_text is not None:
        os.utime(path, None)


def ulurp_pdf_stem(ulurp_number: str) -> str:
    if not ulurp_number:
        return ""
    compact = re.sub(r"\s+", "", ulurp_number)
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


def application_prefix(ulurp_number: str) -> str:
    compact = re.sub(r"\s+", "", ulurp_number or "").upper()
    match = re.match(r"^([CNM])", compact)
    return match.group(1) if match else ""


def a_application_flag(ulurp_number: str) -> bool:
    return bool(re.search(r"\(A\)", ulurp_number or "", flags=re.IGNORECASE))


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
    if source_type == "docket_description":
        return "docket_description"
    if "FINAL ENVIRONMENTAL IMPACT" in text or "FEIS" in text:
        return "final_eis"
    if "DRAFT ENVIRONMENTAL IMPACT" in text or "DEIS" in text or "PDEIS" in text:
        return "draft_eis"
    if "ENVIRONMENTAL ASSESSMENT" in text or re.search(r"\bEAS\b", text):
        return "eas"
    if "TECHNICAL MEMO" in text:
        return "technical_memo"
    if "POINTS OF AGREEMENT" in text or re.search(r"\bPOA\b", text):
        return "points_of_agreement"
    if "RESTRICTIVE DECLARATION" in text:
        return "restrictive_declaration"
    if "FILED LU" in text or "LAND USE APPLICATION" in text or "APPLICATION" in text:
        return "land_use_application"
    if "PROJECT DESCRIPTION" in text:
        return "project_description"
    if "LAND USE" in text:
        return "land_use"
    if "ZONING" in text or action_code in {"ZM", "ZR"}:
        return "zoning_document"
    if "SCOPE" in text:
        return "scope"
    if "MAP" in text:
        return "map"
    if "NOTICE" in text:
        return "notice"
    if "RECOMMEND" in text or source_type == "recommendation_document":
        return "recommendation"
    if source_type == "ceqr_access":
        return "ceqr_access"
    if source_type == "project_page":
        return "project_page"
    return "other_public_document"


def source_priority(source_type: str, family: str, ulurp_number: str, action_code: str, document_title: str) -> int:
    prefix = application_prefix(ulurp_number)
    if source_type.startswith("cpc_report") and prefix == "M":
        return 1
    if source_type.startswith("cpc_report") and a_application_flag(ulurp_number):
        return 2
    if source_type.startswith("cpc_report") and action_code in {"ZM", "ZR", "ZS", "HA", "HD", "HG", "PQ"}:
        return 3
    if family in {"points_of_agreement", "restrictive_declaration"}:
        return 4
    if family in {"land_use_application", "project_description", "land_use", "zoning_document"}:
        return 5
    if family in {"final_eis", "draft_eis", "eas", "technical_memo"}:
        return 6
    if family == "docket_description":
        return 7
    if source_type == "project_page":
        return 8
    if source_type == "ceqr_access":
        return 9
    return 10


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
            str(PROJECT_FETCH_WALL_TIME_SECONDS),
            "--user-agent",
            "nyc-ulurp-modification-research/0.1",
            "--write-out",
            f"{CURL_HTTP_STATUS_MARKER}%{{http_code}}",
            url,
        ],
        capture_output=True,
        text=True,
        timeout=PROJECT_FETCH_WALL_TIME_SECONDS + 5,
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
    row["application_prefix"] = application_prefix(str(row.get("ulurp_number", "")))
    row["a_application_flag"] = a_application_flag(str(row.get("ulurp_number", "")))
    row["m_report_candidate_flag"] = row["application_prefix"] == "M"
    row["source_priority"] = source_priority(
        str(row["source_type"]),
        str(row["document_family"]),
        str(row.get("ulurp_number", "")),
        str(row.get("action_code", "")),
        str(row.get("document_title", "")),
    )
    link_rows.append(row)


def dedupe_links(links: list[dict[str, object]]) -> list[dict[str, object]]:
    deduped_links = []
    seen_links = set()
    for link in links:
        key = (link["project_id"], link["source_type"], link["document_url"], link["document_title"])
        if key in seen_links:
            continue
        seen_links.add(key)
        deduped_links.append(link)
    return deduped_links


def selected_report_link(link: dict[str, object]) -> bool:
    if link.get("document_family") != "cpc_report":
        return False
    try:
        priority = int(link.get("source_priority") or 99)
    except ValueError:
        priority = 99
    return priority <= 3


def report_local_path(cache_id: str) -> str:
    return f"../temp/zap_documents/{cache_id}.bin"


def extract_downloaded_report_text(path: str) -> tuple[list[dict[str, str]], str]:
    with open(path, "rb") as input_file:
        header = input_file.read(8)

    if header.startswith(b"%PDF"):
        result = subprocess.run(
            ["pdftotext", "-layout", path, "-"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=45,
        )
        if result.returncode != 0:
            raise RuntimeError(clean_text(result.stderr))

        page_rows = []
        for page_number, page_text in enumerate(result.stdout.split("\f"), start=1):
            cleaned_page_text = clean_text(page_text)
            if cleaned_page_text:
                page_rows.append({"page": str(page_number), "document_text": cleaned_page_text})
        return page_rows, "pdftotext_layout"

    with open(path, "r", encoding="utf-8", errors="ignore") as input_file:
        raw_text = input_file.read()
    return [{"page": "NA_not_stated", "document_text": clean_text(re.sub(r"<[^>]+>", " ", raw_text))}], "plain_or_html_text"


def download_report_url(url: str, cache_id: str) -> tuple[str, list[dict[str, str]], str]:
    output_path = report_local_path(cache_id)
    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        page_rows, extraction_method = extract_downloaded_report_text(output_path)
        return output_path, page_rows, f"{extraction_method}_download_cached"

    result = subprocess.run(
        [
            "curl",
            "-sS",
            "-L",
            "--fail",
            "--max-time",
            "90",
            "--user-agent",
            "nyc-ulurp-modification-research/0.1",
            "-o",
            output_path,
            url,
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        if os.path.exists(output_path):
            os.unlink(output_path)
        raise RuntimeError(clean_text(result.stderr) or f"curl exited {result.returncode}")

    page_rows, extraction_method = extract_downloaded_report_text(output_path)
    return output_path, page_rows, f"{extraction_method}_download"


def snippet_window(text: str, start: int, end: int, width: int = 260) -> str:
    return clean_text(text[max(0, start - width) : min(len(text), end + width)])


def extract_report_snippets(text: str) -> list[dict[str, str]]:
    snippets: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for keyword_family, keyword_priority, pattern in KEYWORD_PATTERNS:
        family_count = 0
        for match in pattern.finditer(text):
            snippet = snippet_window(text, match.start(), match.end())
            key = (keyword_family, snippet)
            if key in seen:
                continue
            seen.add(key)
            snippets.append(
                {
                    "keyword_family": keyword_family,
                    "keyword_priority": str(keyword_priority),
                    "snippet": snippet,
                }
            )
            family_count += 1
            if family_count >= 6:
                break

    return snippets


if len(sys.argv) != 3:
    raise RuntimeError("Usage: python3 discover_ulurp_modification_documents.py <sample_mode> <discovery_limit>")

sample_mode = sys.argv[1]
discovery_limit = int(sys.argv[2])
if sample_mode not in {"full", "pilot"}:
    raise RuntimeError(f"Unsupported sample_mode: {sample_mode}")

with open("../input/ulurp_modification_project_spine.csv", "r", encoding="utf-8", newline="") as input_file:
    spine_rows = list(csv.DictReader(input_file))

spine_rows.sort(
    key=lambda row: (
        int(row.get("cert_year") or 9999),
        row.get("borough_name", ""),
        row.get("project_id", ""),
    )
)
if sample_mode == "pilot" and discovery_limit == 0:
    discovery_limit = 25
if discovery_limit > 0:
    spine_rows = spine_rows[:discovery_limit]

link_rows: list[dict[str, object]] = []
docket_rows: list[dict[str, object]] = []
summary_rows: list[dict[str, object]] = []
failure_rows: list[dict[str, object]] = []

for row_number, spine_row in enumerate(spine_rows, start=1):
    project_id = spine_row["project_id"]
    project_page_url = f"https://zap.planning.nyc.gov/projects/{urllib.parse.quote(project_id)}"
    api_url = ""
    fetch_status = "success"
    fetch_http_status = ""
    fetch_error = ""
    data: dict[str, object] = {}

    if row_number > 1:
        time.sleep(REQUEST_SLEEP_SECONDS)

    try:
        api_url, fetch_http_status, data = fetch_project(project_id)
    except urllib.error.HTTPError as error:
        fetch_status = "http_error"
        fetch_http_status = str(error.code)
        fetch_error = clean_text(getattr(error, "reason", str(error)))
    except urllib.error.URLError as error:
        fetch_status = "url_error"
        fetch_error = clean_text(error.reason)
    except (json.JSONDecodeError, TimeoutError, subprocess.TimeoutExpired) as error:
        fetch_status = "parse_or_timeout_error"
        fetch_error = clean_text(error)

    if row_number == 1 or row_number % 25 == 0 or row_number == len(spine_rows):
        print(f"Fetched {row_number}/{len(spine_rows)} ZAP projects", flush=True)

    project_attrs = data.get("data", {}).get("attributes", {}) if data else {}
    ceqr_number = clean_text(project_attrs.get("dcp-ceqrnumber")) or clean_text(spine_row.get("ceqr_leadagency", ""))
    project_name = clean_text(project_attrs.get("dcp-projectname")) or spine_row.get("project_name", "")
    project_link_rows: list[dict[str, object]] = []
    base_source_row = {
        "project_id": project_id,
        "project_name": project_name,
        "cert_year": spine_row.get("cert_year", ""),
        "stratum": spine_row.get("stratum", ""),
        "council_outcome": spine_row.get("council_outcome", ""),
        "queue_rank": row_number,
        "api_url": api_url,
        "ceqr_number": ceqr_number,
        "fetch_status": fetch_status,
        "fetch_http_status": fetch_http_status,
        "fetch_error": fetch_error,
    }

    add_link(
        project_link_rows,
        {
            **base_source_row,
            "source_type": "project_page",
            "source_container_id": project_id,
            "source_container_title": "ZAP project page",
            "document_title": "ZAP project page",
            "document_url": project_page_url,
            "action_code": "",
            "ulurp_number": "",
            "document_created_at": "",
        },
    )

    if ceqr_number:
        add_link(
            project_link_rows,
            {
                **base_source_row,
                "source_type": "ceqr_access",
                "source_container_id": ceqr_number,
                "source_container_title": "CEQR Access",
                "document_title": ceqr_number,
                "document_url": "https://a002-ceqraccess.nyc.gov/ceqr/",
                "action_code": "",
                "ulurp_number": "",
                "document_created_at": "",
            },
        )

    if fetch_status != "success":
        failure_rows.append(
            {
                "project_id": project_id,
                "project_name": project_name,
                "cert_year": spine_row.get("cert_year", ""),
                "queue_rank": row_number,
                "api_url": api_url or f"{ZAP_API_HOST}/projects/{urllib.parse.quote(project_id)}",
                "fetch_status": fetch_status,
                "fetch_http_status": fetch_http_status,
                "fetch_error": fetch_error,
            }
        )

    for action in included_rows(data, "actions"):
        attrs = action.get("attributes", {})
        ulurp_number = clean_text(attrs.get("dcp-ulurpnumber"))
        action_code = clean_text(attrs.get("dcp-action-value"))
        action_title = clean_text(attrs.get("dcp-name"))
        action_report = cpc_report_url(ulurp_number, attrs.get("dcp-spabsoluteurl"))
        if action_report:
            add_link(
                project_link_rows,
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
        nycgov_report = nycgov_cpc_report_url(ulurp_number)
        if nycgov_report:
            add_link(
                project_link_rows,
                {
                    **base_source_row,
                    "source_type": "cpc_report_nycgov_fallback",
                    "source_container_id": action.get("id", ""),
                    "source_container_title": action_title,
                    "document_title": f"{ulurp_number} CPC report nyc.gov fallback",
                    "document_url": nycgov_report,
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
                    project_link_rows,
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

    for disposition in included_rows(data, "dispositions"):
        attrs = disposition.get("attributes", {})
        docket_text = clean_text(attrs.get("dcp-docketdescription"))
        if docket_text:
            docket_rows.append(
                {
                    "project_id": project_id,
                    "project_name": project_name,
                    "cert_year": spine_row.get("cert_year", ""),
                    "queue_rank": row_number,
                    "disposition_id": disposition.get("id", ""),
                    "disposition_name": clean_text(attrs.get("dcp-name")),
                    "recommendation_status": clean_text(attrs.get("statuscode")),
                    "borough_president_recommendation": clean_text(attrs.get("dcp-boroughpresidentrecommendation")),
                    "borough_board_recommendation": clean_text(attrs.get("dcp-boroughboardrecommendation")),
                    "community_board_recommendation": clean_text(attrs.get("dcp-communityboardrecommendation")),
                    "docket_description": docket_text,
                    "api_url": api_url,
                    "project_page_url": project_page_url,
                }
            )

            add_link(
                project_link_rows,
                {
                    **base_source_row,
                    "source_type": "docket_description",
                    "source_container_id": disposition.get("id", ""),
                    "source_container_title": clean_text(attrs.get("dcp-name")),
                    "document_title": "ZAP disposition docket description",
                    "document_url": project_page_url,
                    "action_code": clean_text(attrs.get("dcp-projectaction-value")),
                    "ulurp_number": "",
                    "document_created_at": clean_text(attrs.get("dcp-datereceived")),
                },
            )

    deduped_project_links = dedupe_links(project_link_rows)
    link_rows.extend(deduped_project_links)

    summary_rows.append(
        {
            "project_id": project_id,
            "project_name": project_name,
            "cert_year": spine_row.get("cert_year", ""),
            "stratum": spine_row.get("stratum", ""),
            "council_outcome": spine_row.get("council_outcome", ""),
            "queue_rank": row_number,
            "fetch_status": fetch_status,
            "fetch_http_status": fetch_http_status,
            "fetch_error": fetch_error,
            "source_document_link_count": len(deduped_project_links),
            "cpc_report_count": sum(link["document_family"] == "cpc_report" for link in deduped_project_links),
            "m_report_candidate_count": sum(truthy(link["m_report_candidate_flag"]) for link in deduped_project_links),
            "a_application_report_count": sum(truthy(link["a_application_flag"]) for link in deduped_project_links),
            "points_of_agreement_candidate_count": sum(link["document_family"] == "points_of_agreement" for link in deduped_project_links),
            "eis_or_eas_count": sum(link["document_family"] in {"final_eis", "draft_eis", "eas"} for link in deduped_project_links),
            "docket_description_count": sum(link["document_family"] == "docket_description" for link in deduped_project_links),
            "project_page_url": project_page_url,
            "api_url": api_url,
        }
    )

link_rows = dedupe_links(link_rows)
link_rows.sort(key=lambda row: (int(row["queue_rank"]), int(row["source_priority"]), row["source_type"], row["document_title"]))
docket_rows.sort(key=lambda row: (int(row["queue_rank"]), row["disposition_name"]))
summary_rows.sort(key=lambda row: int(row["queue_rank"]))
failure_rows.sort(key=lambda row: int(row["queue_rank"]))

report_text_rows: list[dict[str, object]] = []
report_snippet_rows: list[dict[str, object]] = []
report_failure_rows: list[dict[str, object]] = []
report_links = [link for link in link_rows if selected_report_link(link)]

for report_number, link in enumerate(report_links, start=1):
    document_id = stable_id(
        link.get("project_id", ""),
        link.get("source_container_id", ""),
        link.get("ulurp_number", ""),
        link.get("document_url", ""),
    )
    cache_id = stable_id(link.get("document_url", ""))
    local_path = ""
    text_row_count = 0
    snippet_row_count = 0

    try:
        local_path, page_rows, extraction_method = download_report_url(str(link["document_url"]), cache_id)
        if not page_rows:
            raise RuntimeError("downloaded report produced no extractable text")

        for page_row in page_rows:
            page = page_row["page"]
            document_text = page_row["document_text"]
            if document_text == "":
                continue

            report_text_rows.append(
                {
                    "document_page_id": stable_id(document_id, page),
                    "document_id": document_id,
                    "project_id": link.get("project_id", ""),
                    "project_name": link.get("project_name", ""),
                    "cert_year": link.get("cert_year", ""),
                    "stratum": link.get("stratum", ""),
                    "council_outcome": link.get("council_outcome", ""),
                    "source_type": link.get("source_type", ""),
                    "document_family": link.get("document_family", ""),
                    "source_priority": link.get("source_priority", ""),
                    "document_title": link.get("document_title", ""),
                    "source_doc": link.get("document_url", ""),
                    "local_path": local_path,
                    "action_code": link.get("action_code", ""),
                    "ulurp_number": link.get("ulurp_number", ""),
                    "application_prefix": link.get("application_prefix", ""),
                    "a_application_flag": link.get("a_application_flag", ""),
                    "m_report_candidate_flag": link.get("m_report_candidate_flag", ""),
                    "page": page,
                    "document_text": document_text,
                    "extraction_method": extraction_method,
                    "confidence": "high" if page != "NA_not_stated" else "medium",
                }
            )
            text_row_count += 1

            for snippet in extract_report_snippets(document_text):
                report_snippet_rows.append(
                    {
                        "snippet_id": stable_id(document_id, page, snippet["keyword_family"], snippet["snippet"]),
                        "document_id": document_id,
                        "project_id": link.get("project_id", ""),
                        "project_name": link.get("project_name", ""),
                        "cert_year": link.get("cert_year", ""),
                        "stratum": link.get("stratum", ""),
                        "council_outcome": link.get("council_outcome", ""),
                        "source_type": link.get("source_type", ""),
                        "document_family": link.get("document_family", ""),
                        "source_priority": link.get("source_priority", ""),
                        "document_title": link.get("document_title", ""),
                        "keyword_family": snippet["keyword_family"],
                        "keyword_priority": snippet["keyword_priority"],
                        "source_doc": link.get("document_url", ""),
                        "page": page,
                        "snippet": snippet["snippet"],
                        "extraction_method": extraction_method,
                        "confidence": "high" if page != "NA_not_stated" else "medium",
                    }
                )
                snippet_row_count += 1

    except Exception as error:  # pragma: no cover - network and binary extraction audit path
        report_failure_rows.append(
            {
                "document_id": document_id,
                "project_id": link.get("project_id", ""),
                "project_name": link.get("project_name", ""),
                "cert_year": link.get("cert_year", ""),
                "source_type": link.get("source_type", ""),
                "document_family": link.get("document_family", ""),
                "source_priority": link.get("source_priority", ""),
                "document_title": link.get("document_title", ""),
                "source_doc": link.get("document_url", ""),
                "local_path": local_path or report_local_path(cache_id),
                "failure_stage": "download_or_extract",
                "failure_reason": clean_text(error),
            }
        )

    if report_number == 1 or report_number % 50 == 0 or report_number == len(report_links):
        print(
            "Fetched "
            f"{report_number}/{len(report_links)} targeted ZAP CPC/M report links "
            f"({text_row_count} text rows, {snippet_row_count} snippets for current link)",
            flush=True,
        )

qc_rows = [
    {
        "metric": "queued_project_count",
        "value": len(spine_rows),
        "status": "pass" if len(spine_rows) > 0 else "fail",
        "note": "Projects read from the ULURP modification spine.",
    },
    {
        "metric": "api_success_project_count",
        "value": sum(row["fetch_status"] == "success" for row in summary_rows),
        "status": "pass" if any(row["fetch_status"] == "success" for row in summary_rows) else "fail",
        "note": "Projects successfully read from the public ZAP API.",
    },
    {
        "metric": "source_document_link_count",
        "value": len(link_rows),
        "status": "pass" if len(link_rows) > 0 else "fail",
        "note": "Official project-page, CPC-report, CEQR, public-document, and docket source rows.",
    },
    {
        "metric": "project_with_cpc_report_count",
        "value": sum(int(row["cpc_report_count"]) > 0 for row in summary_rows),
        "status": "pass",
        "note": "Projects with at least one CPC report candidate.",
    },
    {
        "metric": "project_with_m_report_candidate_count",
        "value": sum(int(row["m_report_candidate_count"]) > 0 for row in summary_rows),
        "status": "pass",
        "note": "Projects with at least one M-report candidate from ZAP action metadata.",
    },
    {
        "metric": "api_failure_project_count",
        "value": len(failure_rows),
        "status": "pass",
        "note": "Projects with row-level ZAP API fetch failures retained for follow-up.",
    },
    {
        "metric": "targeted_cpc_report_link_count",
        "value": len(report_links),
        "status": "pass" if len(report_links) > 0 else "fail",
        "note": "CPC/M report links selected for targeted download and text extraction.",
    },
    {
        "metric": "targeted_cpc_report_text_row_count",
        "value": len(report_text_rows),
        "status": "pass" if len(report_links) == 0 or len(report_text_rows) > 0 else "fail",
        "note": "Page-level text rows extracted from targeted CPC/M reports.",
    },
    {
        "metric": "targeted_cpc_report_snippet_row_count",
        "value": len(report_snippet_rows),
        "status": "pass" if len(report_links) == 0 or len(report_snippet_rows) > 0 else "fail",
        "note": "Keyword snippets extracted from targeted CPC/M reports.",
    },
    {
        "metric": "project_with_targeted_cpc_report_text_count",
        "value": len({row["project_id"] for row in report_text_rows}),
        "status": "pass" if len(report_links) == 0 or len({row["project_id"] for row in report_text_rows}) > 0 else "fail",
        "note": "Projects with at least one extracted targeted CPC/M report page.",
    },
    {
        "metric": "targeted_cpc_report_fetch_failure_count",
        "value": len(report_failure_rows),
        "status": "pass",
        "note": "Targeted CPC/M report download or extraction failures retained for follow-up.",
    },
]

link_fieldnames = [
    "project_id",
    "project_name",
    "cert_year",
    "stratum",
    "council_outcome",
    "queue_rank",
    "source_type",
    "source_container_id",
    "source_container_title",
    "document_title",
    "document_family",
    "source_priority",
    "document_url",
    "api_url",
    "action_code",
    "ulurp_number",
    "application_prefix",
    "a_application_flag",
    "m_report_candidate_flag",
    "ceqr_number",
    "document_created_at",
    "fetch_status",
    "fetch_http_status",
    "fetch_error",
]

summary_fieldnames = [
    "project_id",
    "project_name",
    "cert_year",
    "stratum",
    "council_outcome",
    "queue_rank",
    "fetch_status",
    "fetch_http_status",
    "fetch_error",
    "source_document_link_count",
    "cpc_report_count",
    "m_report_candidate_count",
    "a_application_report_count",
    "points_of_agreement_candidate_count",
    "eis_or_eas_count",
    "docket_description_count",
    "project_page_url",
    "api_url",
]

docket_fieldnames = [
    "project_id",
    "project_name",
    "cert_year",
    "queue_rank",
    "disposition_id",
    "disposition_name",
    "recommendation_status",
    "borough_president_recommendation",
    "borough_board_recommendation",
    "community_board_recommendation",
    "docket_description",
    "api_url",
    "project_page_url",
]

failure_fieldnames = [
    "project_id",
    "project_name",
    "cert_year",
    "queue_rank",
    "api_url",
    "fetch_status",
    "fetch_http_status",
    "fetch_error",
]

report_text_fieldnames = [
    "document_page_id",
    "document_id",
    "project_id",
    "project_name",
    "cert_year",
    "stratum",
    "council_outcome",
    "source_type",
    "document_family",
    "source_priority",
    "document_title",
    "source_doc",
    "local_path",
    "action_code",
    "ulurp_number",
    "application_prefix",
    "a_application_flag",
    "m_report_candidate_flag",
    "page",
    "document_text",
    "extraction_method",
    "confidence",
]

report_snippet_fieldnames = [
    "snippet_id",
    "document_id",
    "project_id",
    "project_name",
    "cert_year",
    "stratum",
    "council_outcome",
    "source_type",
    "document_family",
    "source_priority",
    "document_title",
    "keyword_family",
    "keyword_priority",
    "source_doc",
    "page",
    "snippet",
    "extraction_method",
    "confidence",
]

report_failure_fieldnames = [
    "document_id",
    "project_id",
    "project_name",
    "cert_year",
    "source_type",
    "document_family",
    "source_priority",
    "document_title",
    "source_doc",
    "local_path",
    "failure_stage",
    "failure_reason",
]

write_csv_if_changed(link_rows, link_fieldnames, "../output/ulurp_modification_zap_document_links.csv")
write_csv_if_changed(summary_rows, summary_fieldnames, "../output/ulurp_modification_zap_project_summary.csv")
write_csv_if_changed(docket_rows, docket_fieldnames, "../output/ulurp_modification_zap_docket_text.csv")
write_csv_if_changed(failure_rows, failure_fieldnames, "../output/ulurp_modification_zap_document_fetch_failures.csv")
write_csv_if_changed(report_text_rows, report_text_fieldnames, "../output/ulurp_modification_zap_report_text.csv")
write_csv_if_changed(report_snippet_rows, report_snippet_fieldnames, "../output/ulurp_modification_zap_report_snippets.csv")
write_csv_if_changed(report_failure_rows, report_failure_fieldnames, "../output/ulurp_modification_zap_report_fetch_failures.csv")
write_csv_if_changed(qc_rows, ["metric", "value", "status", "note"], "../output/ulurp_modification_zap_document_discovery_qc.csv")

if any(row["status"] == "fail" for row in qc_rows):
    raise RuntimeError("ULURP modification ZAP document discovery QC failed.")

print("Wrote ULURP modification ZAP document discovery outputs to ../output")
