#!/usr/bin/env python3

import csv
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


def write_csv_if_changed(rows, fieldnames, path):
    lines = []
    writer_buffer = io.StringIO()
    writer = csv.DictWriter(writer_buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    lines.append(writer_buffer.getvalue())
    new_text = "".join(lines)

    try:
        with open(path, "r", encoding="utf-8", newline="") as existing_file:
            old_text = existing_file.read()
    except FileNotFoundError:
        old_text = None

    if old_text != new_text:
        with open(path, "w", encoding="utf-8", newline="") as output_file:
            output_file.write(new_text)


def truthy(value):
    return str(value).strip().upper() in {"TRUE", "T", "1", "YES"}


def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def ulurp_pdf_stem(ulurp_number):
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


def sharepoint_server_relative_url(absolute_url):
    if not absolute_url:
        return ""
    parsed = urllib.parse.urlparse(absolute_url)
    if parsed.netloc.lower() != "nyco365.sharepoint.com":
        return parsed.path
    return urllib.parse.unquote(parsed.path)


def cpc_report_url(ulurp_number, absolute_url):
    stem = ulurp_pdf_stem(ulurp_number)
    relative = sharepoint_server_relative_url(absolute_url)
    if not stem or not relative:
        return ""
    return f"{ZAP_API_HOST}/document/projectaction{urllib.parse.quote(relative)}/{stem}.pdf"


def nycgov_cpc_report_url(ulurp_number):
    stem = ulurp_pdf_stem(ulurp_number)
    if not stem:
        return ""
    return f"https://www.nyc.gov/assets/planning/download/pdf/about/cpc/{stem}.pdf"


def document_family(source_type, container_title, document_title, action_code=""):
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


def document_priority(source_type, family, action_code="", document_title=""):
    title = document_title.upper()
    if source_type.startswith("cpc_report") and action_code == "ZM":
        return 1
    if source_type.startswith("cpc_report") and action_code == "ZR":
        return 2
    if family == "docket_description":
        return 2
    if family in {"land_use_application", "project_description", "land_use", "zoning_document"}:
        return 3
    if family in {"final_eis", "draft_eis", "eas"} and (
        "PROJECT DESCRIPTION" in title or "LAND_USE" in title or "LAND USE" in title or "EXECUTIVE" in title
    ):
        return 3
    if family in {"final_eis", "draft_eis", "eas", "technical_memo"}:
        return 4
    if family in {"map", "scope"}:
        return 5
    if source_type == "project_page":
        return 7
    if source_type == "ceqr_access":
        return 8
    return 6


def fetch_project(project_id):
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
            "nyc-rezoning-research/0.1",
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


def included_rows(data, record_type):
    return [row for row in data.get("included", []) if row.get("type") == record_type]


def add_link(link_rows, row):
    row["document_family"] = document_family(
        row["source_type"],
        row.get("source_container_title", ""),
        row.get("document_title", ""),
        row.get("action_code", ""),
    )
    row["source_priority"] = document_priority(
        row["source_type"],
        row["document_family"],
        row.get("action_code", ""),
        row.get("document_title", ""),
    )
    row["preferred_for_direction_scope_review"] = row["source_priority"] <= 3
    link_rows.append(row)


def dedupe_links(links):
    deduped_links = []
    seen_links = set()
    for link in links:
        key = (link["project_id"], link["source_type"], link["document_url"], link["document_title"])
        if key in seen_links:
            continue
        seen_links.add(key)
        deduped_links.append(link)
    return deduped_links


with open("../input/zap_rezoning_direction_text_candidate_queue.csv", "r", encoding="utf-8", newline="") as input_file:
    queue_rows = [
        row for row in csv.DictReader(input_file)
        if truthy(row.get("remaining_reviewed_unknown_flag", "TRUE"))
    ]

queue_rows.sort(
    key=lambda row: (
        row.get("source_lookup_priority") != "high",
        row.get("text_candidate_direction") == "no_local_text_candidate",
        -float(row.get("affected_lot_acres") or 0),
        row.get("completed_year", ""),
        row.get("project_id", ""),
    )
)

if os.environ.get("ZAP_DOC_DISCOVERY_LIMIT"):
    queue_rows = queue_rows[:int(os.environ["ZAP_DOC_DISCOVERY_LIMIT"])]

link_rows = []
docket_rows = []
summary_rows = []
failure_rows = []

for row_number, queue_row in enumerate(queue_rows, start=1):
    project_id = queue_row["project_id"]
    project_page_url = f"https://zap.planning.nyc.gov/projects/{urllib.parse.quote(project_id)}"
    api_url = ""
    fetch_status = "success"
    fetch_http_status = ""
    fetch_error = ""
    data = {}

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

    if row_number == 1 or row_number % 25 == 0 or row_number == len(queue_rows):
        print(f"Fetched {row_number}/{len(queue_rows)} ZAP projects", flush=True)

    project_attrs = data.get("data", {}).get("attributes", {}) if data else {}
    ceqr_number = clean_text(project_attrs.get("dcp-ceqrnumber")) or clean_text(queue_row.get("ceqr_number"))
    project_name = clean_text(project_attrs.get("dcp-projectname")) or queue_row.get("project_name", "")
    project_link_rows = []
    base_source_row = {
        "project_id": project_id,
        "project_name": project_name,
        "completed_year": queue_row.get("completed_year", ""),
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
                "completed_year": queue_row.get("completed_year", ""),
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
                    "completed_year": queue_row.get("completed_year", ""),
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
            "completed_year": queue_row.get("completed_year", ""),
            "queue_rank": row_number,
            "fetch_status": fetch_status,
            "fetch_http_status": fetch_http_status,
            "fetch_error": fetch_error,
            "source_lookup_priority": queue_row.get("source_lookup_priority", ""),
            "text_candidate_direction": queue_row.get("text_candidate_direction", ""),
            "affected_lot_acres": queue_row.get("affected_lot_acres", ""),
            "source_link_count": len(deduped_project_links),
            "preferred_source_link_count": sum(link["preferred_for_direction_scope_review"] for link in deduped_project_links),
            "cpc_report_count": sum(link["document_family"] == "cpc_report" for link in deduped_project_links),
            "zm_cpc_report_count": sum(link["document_family"] == "cpc_report" and link["action_code"] == "ZM" for link in deduped_project_links),
            "public_document_count": sum(
                link["source_type"] in {"public_package_document", "public_artifact_document", "recommendation_document"}
                for link in deduped_project_links
            ),
            "docket_description_count": sum(link["document_family"] == "docket_description" for link in deduped_project_links),
            "ceqr_access_count": sum(link["source_type"] == "ceqr_access" for link in deduped_project_links),
            "project_page_url": project_page_url,
            "api_url": api_url,
        }
    )

deduped_links = dedupe_links(link_rows)
deduped_links.sort(key=lambda row: (int(row["queue_rank"]), int(row["source_priority"]), row["source_type"], row["document_title"]))
docket_rows.sort(key=lambda row: (int(row["queue_rank"]), row["disposition_name"]))
summary_rows.sort(key=lambda row: int(row["queue_rank"]))
failure_rows.sort(key=lambda row: int(row["queue_rank"]))

qc_rows = [
    {
        "metric": "queued_unresolved_project_count",
        "value": len(queue_rows),
        "status": "pass" if len(queue_rows) > 0 else "fail",
        "note": "Parser-unknown and still unresolved projects read from the rezoning text-candidate queue.",
    },
    {
        "metric": "api_success_project_count",
        "value": sum(row["fetch_status"] == "success" for row in summary_rows),
        "status": "pass" if any(row["fetch_status"] == "success" for row in summary_rows) else "fail",
        "note": "Projects successfully read from the public ZAP API.",
    },
    {
        "metric": "api_failure_project_count",
        "value": len(failure_rows),
        "status": "pass",
        "note": "Projects with row-level ZAP API fetch failures retained for follow-up.",
    },
    {
        "metric": "source_document_link_count",
        "value": len(deduped_links),
        "status": "pass" if len(deduped_links) > 0 else "fail",
        "note": "Discovered official project-page, CPC-report, CEQR, public-document, and docket source rows.",
    },
    {
        "metric": "zm_cpc_report_project_count",
        "value": sum(int(row["zm_cpc_report_count"]) > 0 for row in summary_rows),
        "status": "pass",
        "note": "Projects with a Zoning Map Amendment CPC report link discovered from ZAP action metadata.",
    },
    {
        "metric": "docket_description_project_count",
        "value": sum(int(row["docket_description_count"]) > 0 for row in summary_rows),
        "status": "pass",
        "note": "Projects with ZAP disposition docket text, often containing exact zoning-map change language.",
    },
    {
        "metric": "preferred_source_coverage_share",
        "value": round(
            sum(int(row["preferred_source_link_count"]) > 0 for row in summary_rows) / len(summary_rows),
            4,
        ) if summary_rows else 0,
        "status": "pass" if any(int(row["preferred_source_link_count"]) > 0 for row in summary_rows) else "fail",
        "note": "Share of projects with at least one high-priority source for direction/scope review.",
    },
]

link_fieldnames = [
    "project_id",
    "project_name",
    "completed_year",
    "queue_rank",
    "source_type",
    "source_container_id",
    "source_container_title",
    "document_title",
    "document_family",
    "source_priority",
    "preferred_for_direction_scope_review",
    "document_url",
    "api_url",
    "action_code",
    "ulurp_number",
    "ceqr_number",
    "document_created_at",
    "fetch_status",
    "fetch_http_status",
    "fetch_error",
]

summary_fieldnames = [
    "project_id",
    "project_name",
    "completed_year",
    "queue_rank",
    "fetch_status",
    "fetch_http_status",
    "fetch_error",
    "source_lookup_priority",
    "text_candidate_direction",
    "affected_lot_acres",
    "source_link_count",
    "preferred_source_link_count",
    "cpc_report_count",
    "zm_cpc_report_count",
    "public_document_count",
    "docket_description_count",
    "ceqr_access_count",
    "project_page_url",
    "api_url",
]

docket_fieldnames = [
    "project_id",
    "project_name",
    "completed_year",
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
    "completed_year",
    "queue_rank",
    "api_url",
    "fetch_status",
    "fetch_http_status",
    "fetch_error",
]

write_csv_if_changed(deduped_links, link_fieldnames, "../output/zap_project_source_document_links.csv")
write_csv_if_changed(summary_rows, summary_fieldnames, "../output/zap_project_source_project_summary.csv")
write_csv_if_changed(docket_rows, docket_fieldnames, "../output/zap_project_source_docket_text.csv")
write_csv_if_changed(failure_rows, failure_fieldnames, "../output/zap_project_source_fetch_failures.csv")
write_csv_if_changed(qc_rows, ["metric", "value", "status", "note"], "../output/zap_project_source_discovery_qc.csv")

if any(row["status"] == "fail" for row in qc_rows):
    print("ZAP source document discovery QC failed.", file=sys.stderr)
    sys.exit(1)

print("Wrote ZAP source document discovery outputs to ../output")
