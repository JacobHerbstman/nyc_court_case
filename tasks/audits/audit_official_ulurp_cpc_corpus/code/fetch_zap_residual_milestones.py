#!/usr/bin/env python3

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_official_ulurp_cpc_corpus/code")

import csv
import json
import re
import subprocess
import time
import urllib.parse


ZAP_API_HOST = "https://zap-api-production.herokuapp.com"
FETCH_ATTEMPTS = 3
CONNECT_TIMEOUT_SECONDS = 10
MAX_TIME_SECONDS = 120


def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def application_key(value):
    return re.sub(r"[^A-Z0-9]", "", clean_text(value).upper()).lstrip("CNMI")


def fetch_project(project_id):
    url = (
        f"{ZAP_API_HOST}/projects/{urllib.parse.quote(project_id)}"
        "?include=actions,milestones"
    )
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        result = subprocess.run(
            [
                "curl",
                "--silent",
                "--show-error",
                "--location",
                "--user-agent",
                "nyc-ulurp-corpus-research/0.1",
                "--connect-timeout",
                str(CONNECT_TIMEOUT_SECONDS),
                "--max-time",
                str(MAX_TIME_SECONDS),
                "--write-out",
                "\n__HTTP_STATUS__:%{http_code}",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=MAX_TIME_SECONDS + 10,
            check=False,
        )
        if "\n__HTTP_STATUS__:" in result.stdout:
            body, status = result.stdout.rsplit("\n__HTTP_STATUS__:", 1)
            status = clean_text(status)[:3]
        else:
            body = result.stdout
            status = ""
        if result.returncode == 0 and status == "200":
            try:
                return url, "success", "", json.loads(body)
            except json.JSONDecodeError as error:
                return url, "json_error", clean_text(error), {}
        if attempt < FETCH_ATTEMPTS and (
            result.returncode != 0
            or status in {"", "000", "403", "408", "429", "500", "502", "503", "504"}
        ):
            time.sleep(2)
            continue
        error = clean_text(result.stderr) or clean_text(body)[:500]
        return url, f"http_{status or 'missing'}", error, {}
    raise RuntimeError("Unreachable ZAP fetch state")


def milestone_values(milestones, pattern, field):
    values = []
    for row in milestones:
        attributes = row.get("attributes", {})
        name = " ".join(
            [
                clean_text(attributes.get("dcp-name")),
                clean_text(attributes.get("milestonename")),
                clean_text(attributes.get("display-name")),
            ]
        )
        if re.search(pattern, name, re.IGNORECASE):
            value = clean_text(attributes.get(field))
            if value:
                values.append(value[:10] if field.endswith("date") else value)
    return "; ".join(sorted(set(values)))


with open(
    "../output/official_ulurp_cpc_zap_residual.csv",
    encoding="utf-8",
    newline="",
) as input_file:
    residual_rows = [
        row
        for row in csv.DictReader(input_file)
        if row["match_status"] == "completed_zap_application_without_cpc_report_match"
    ]

output_rows = []
for row_number, row in enumerate(residual_rows, start=1):
    api_url, fetch_status, fetch_error, project = fetch_project(row["project_id"])
    included = project.get("included", [])
    actions = [item for item in included if item.get("type") == "actions"]
    milestones = [item for item in included if item.get("type") == "milestones"]
    target_key = application_key(row["raw_application_number"])
    matching_actions = [
        item.get("attributes", {})
        for item in actions
        if application_key(item.get("attributes", {}).get("dcp-ulurpnumber"))
        == target_key
    ]

    output_rows.append(
        {
            "project_id": row["project_id"],
            "raw_application_number": row["raw_application_number"],
            "application_key": target_key,
            "project_name": row["project_name"],
            "certified_referred_date": row["certified_referred_date"],
            "completed_date": row["completed_date"],
            "api_url": api_url,
            "api_fetch_status": fetch_status,
            "api_fetch_error": fetch_error,
            "matching_action_count": len(matching_actions),
            "action_numbers": "; ".join(
                sorted(
                    {
                        clean_text(action.get("dcp-ulurpnumber"))
                        for action in matching_actions
                        if clean_text(action.get("dcp-ulurpnumber"))
                    }
                )
            ),
            "action_statuses": "; ".join(
                sorted(
                    {
                        clean_text(action.get("statuscode"))
                        for action in matching_actions
                        if clean_text(action.get("statuscode"))
                    }
                )
            ),
            "action_document_urls": "; ".join(
                sorted(
                    {
                        clean_text(action.get("dcp-spabsoluteurl"))
                        for action in matching_actions
                        if clean_text(action.get("dcp-spabsoluteurl"))
                    }
                )
            ),
            "certification_dates": milestone_values(
                milestones,
                r"Review Session - Certified / Referred",
                "dcp-actualenddate",
            ),
            "certification_outcomes": milestone_values(
                milestones,
                r"Review Session - Certified / Referred",
                "outcome",
            ),
            "cpc_hearing_dates": milestone_values(
                milestones,
                r"CPC Public Meeting - Public Hearing|City Planning Commission Review",
                "dcp-actualenddate",
            ),
            "cpc_hearing_outcomes": milestone_values(
                milestones,
                r"CPC Public Meeting - Public Hearing|City Planning Commission Review",
                "outcome",
            ),
            "cpc_vote_dates": milestone_values(
                milestones,
                r"CPC Public Meeting - Vote|City Planning Commission Vote",
                "dcp-actualenddate",
            ),
            "cpc_vote_outcomes": milestone_values(
                milestones,
                r"CPC Public Meeting - Vote|City Planning Commission Vote",
                "outcome",
            ),
            "cpc_vote_statuses": milestone_values(
                milestones,
                r"CPC Public Meeting - Vote|City Planning Commission Vote",
                "statuscode",
            ),
            "council_review_dates": milestone_values(
                milestones,
                r"City Council Review",
                "dcp-actualenddate",
            ),
            "council_review_outcomes": milestone_values(
                milestones,
                r"City Council Review",
                "outcome",
            ),
            "final_letter_dates": milestone_values(
                milestones,
                r"Final Letter Sent",
                "dcp-actualenddate",
            ),
            "milestone_names": "; ".join(
                sorted(
                    {
                        clean_text(item.get("attributes", {}).get("display-name"))
                        or clean_text(item.get("attributes", {}).get("milestonename"))
                        for item in milestones
                        if clean_text(item.get("attributes", {}).get("display-name"))
                        or clean_text(item.get("attributes", {}).get("milestonename"))
                    }
                )
            ),
            "milestone_count": len(milestones),
        }
    )
    if row_number % 25 == 0:
        print(f"Fetched {row_number} of {len(residual_rows)} ZAP projects.", flush=True)

with open(
    "../output/official_ulurp_cpc_zap_milestones.csv",
    "w",
    encoding="utf-8",
    newline="",
) as output_file:
    writer = csv.DictWriter(output_file, fieldnames=list(output_rows[0].keys()))
    writer.writeheader()
    writer.writerows(output_rows)

print(f"Wrote milestone evidence for {len(output_rows)} ZAP residual applications.")
