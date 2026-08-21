#!/usr/bin/env python3

import csv
import hashlib
import json
import os
import sys
import time
from pathlib import Path

import requests


# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/pilot_ulurp_cpc_llm_labels/code")
# variant = "sol_medium_v2"


label_fields = [
    "specific_project",
    "zone_change",
    "dev_direction",
    "substantial_local_opposition",
    "local_request_condition",
    "revision_or_concession",
    "procedural_response",
    "explicit_local_response",
    "approved_unresolved_objection",
    "cb_request_or_opposition",
    "bp_request_or_opposition",
    "councilmember_position",
    "civic_group_position",
    "cpc_support_speakers",
    "cpc_opposition_speakers",
    "cb_support_votes",
    "cb_opposition_votes",
    "affordability_displacement",
    "traffic_parking",
    "scale_character_preservation",
    "infrastructure_services",
    "environment_open_space",
]

cli_args = sys.argv[1:]
if len(cli_args) != 1:
    raise SystemExit("Usage: python3 run_ulurp_cpc_llm_pilot.py <variant>")

variant = cli_args[0]
if variant != "sol_medium_v2":
    raise SystemExit("Invalid pilot variant.")

api_key = os.environ.get("OPENAI_API_KEY", "").strip()
if not api_key:
    for line in Path("../../../../.env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        if name.strip() == "OPENAI_API_KEY":
            api_key = value.strip().strip('"').strip("'")
            break
if not api_key:
    raise SystemExit("OPENAI_API_KEY is not set in the environment or project-root .env.")

with open(
    f"../output/ulurp_cpc_llm_pilot_requests_{variant}.jsonl",
    encoding="utf-8",
) as input_file:
    requests_json = [json.loads(line) for line in input_file if line.strip()]

with open("../input/ulurp_cpc_training_labels_jacob.csv", newline="", encoding="utf-8-sig") as input_file:
    human_by_document_id = {
        row["document_id"]: row
        for row in csv.DictReader(input_file)
    }

session = requests.Session()
session.headers.update(
    {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
)

raw_rows = []
for request_number, request_json in enumerate(requests_json, start=1):
    response = None
    for attempt in range(3):
        response = session.post(
            "https://api.openai.com/v1/responses",
            json=request_json["body"],
            timeout=600,
        )
        if response.status_code not in {429, 500, 502, 503, 504}:
            break
        time.sleep(2 ** attempt)

    if response is None or response.status_code != 200:
        message = response.text[:1000] if response is not None else "No response"
        raise SystemExit(
            f"OpenAI request failed for {request_json['custom_id']}: "
            f"{getattr(response, 'status_code', 'unknown')} {message}"
        )

    raw_rows.append(
        {
            "custom_id": request_json["custom_id"],
            "request_sha256": hashlib.sha256(
                json.dumps(request_json, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest(),
            "response": response.json(),
        }
    )
    print(
        f"Completed {request_number}/{len(requests_json)}: {request_json['custom_id']}",
        flush=True,
    )

with open(
    f"../temp/ulurp_cpc_llm_pilot_responses_{variant}.jsonl",
    "w",
    encoding="utf-8",
) as output_file:
    for row in raw_rows:
        output_file.write(json.dumps(row, ensure_ascii=True, separators=(",", ":")) + "\n")

comparison_rows = []
for raw_row in raw_rows:
    response_json = raw_row["response"]
    output_texts = [
        content["text"]
        for item in response_json.get("output", [])
        if item.get("type") == "message"
        for content in item.get("content", [])
        if content.get("type") == "output_text"
    ]
    if len(output_texts) != 1:
        raise SystemExit(f"Expected one output text for {raw_row['custom_id']}.")

    model_output = json.loads(output_texts[0])
    if model_output["document_id"] != raw_row["custom_id"]:
        raise SystemExit(f"Model returned the wrong document_id for {raw_row['custom_id']}.")

    human_row = human_by_document_id[raw_row["custom_id"]]
    evidence_by_field = {field: [] for field in label_fields}
    for evidence in model_output["evidence"]:
        evidence_by_field[evidence["field"]].append(evidence)

    usage = response_json.get("usage", {})
    for field in label_fields:
        human_value = human_row[field].strip()
        model_value = model_output["labels"][field]
        normalized_human_value = "null" if human_value == "" else human_value
        normalized_model_value = "null" if model_value is None else str(model_value)
        field_evidence = evidence_by_field[field]
        comparison_rows.append(
            {
                "review_id": human_row["review_id"],
                "document_id": raw_row["custom_id"],
                "project_name": human_row["project_name"],
                "vote_year": human_row["vote_year"],
                "action_code": human_row["action_code"],
                "sample_group": human_row["sample_group"],
                "field": field,
                "human_value": normalized_human_value,
                "model_value": normalized_model_value,
                "agreement": str(int(normalized_human_value == normalized_model_value)),
                "model_confidence": model_output["confidence"][field],
                "evidence_pdf_pages": " | ".join(str(item["pdf_page"]) for item in field_evidence),
                "evidence_quotes": " | ".join(item["quote"] for item in field_evidence),
                "evidence_explanations": " | ".join(item["explanation"] for item in field_evidence),
                "model_summary": model_output["summary"],
                "response_id": response_json.get("id", ""),
                "response_model": response_json.get("model", ""),
                "input_tokens": usage.get("input_tokens", ""),
                "output_tokens": usage.get("output_tokens", ""),
                "total_tokens": usage.get("total_tokens", ""),
                "request_sha256": raw_row["request_sha256"],
            }
        )

with open(
    f"../output/ulurp_cpc_llm_pilot_comparison_{variant}.csv",
    "w",
    newline="",
    encoding="utf-8",
) as output_file:
    writer = csv.DictWriter(
        output_file,
        fieldnames=comparison_rows[0].keys(),
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(comparison_rows)

Path(f"../temp/ulurp_cpc_llm_pilot_responses_{variant}.jsonl").replace(
    f"../output/ulurp_cpc_llm_pilot_responses_{variant}.jsonl"
)

print(
    f"Wrote {len(raw_rows)} raw responses and "
    f"{len(comparison_rows)} report-field comparisons."
)
