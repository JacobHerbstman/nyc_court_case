#!/usr/bin/env python3

import csv
import json




rows = []
seen_ids = set()
with open("../output/manual_queue_web_review_batches/manual_queue_web_review_responses.jsonl", "r", encoding="utf-8") as input_file:
    for line_number, line in enumerate(input_file, start=1):
        if line.strip():
            row = json.loads(line)
            if row["signature_review_id"] in seen_ids:
                raise ValueError(f"Duplicate signature_review_id in response ledger: {row['signature_review_id']}")
            rows.append(row)
            seen_ids.add(row["signature_review_id"])


spot_checks = {}
with open("manual_queue_human_spot_checks.csv", "r", encoding="utf-8", newline="") as input_file:
    for row in csv.DictReader(input_file):
        if row["signature_review_id"] in spot_checks:
            raise ValueError(f"Duplicate signature_review_id in human spot-check ledger: {row['signature_review_id']}")
        spot_checks[row["signature_review_id"]] = row

missing_spot_check_ids = set(spot_checks) - seen_ids
if missing_spot_check_ids:
    raise ValueError(f"Human spot-check ledger contains IDs absent from response ledger: {sorted(missing_spot_check_ids)}")


batch_acceptance = {}
with open("manual_queue_researcher_batch_acceptance.csv", "r", encoding="utf-8", newline="") as input_file:
    for row in csv.DictReader(input_file):
        batch_acceptance[row["manual_queue_batch_decision"]] = row

accept_all_52 = batch_acceptance.get("manual_queue_web_review_52", {}).get("accept_all_52", "") == "yes"
acceptance_note = batch_acceptance.get("manual_queue_web_review_52", {}).get("decision_note", "")


def district_string(value):
    if isinstance(value, list):
        return ";".join(str(district) for district in value)
    return str(value)


def compact_note(row):
    text = " ".join(row.get("official_geography_basis", "").split())
    if len(text) > 260:
        text = text[:257].rstrip() + "..."
    return text


with open("../output/manual_queue_web_review_batches/manual_queue_web_review_summary.csv", "w", encoding="utf-8", newline="") as output_file:
    writer = csv.DictWriter(
        output_file,
        fieldnames=[
            "signature_review_id",
            "recommended_council_districts",
            "confidence",
            "promotion_decision",
            "evidence_type",
            "human_review_needed",
            "human_spot_check_verdict",
            "researcher_batch_acceptance",
            "human_review_needed_after_spot_check",
            "codex_review_note",
        ],
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                "signature_review_id": row["signature_review_id"],
                "recommended_council_districts": district_string(row.get("recommended_council_districts", "")),
                "confidence": row.get("confidence", ""),
                "promotion_decision": row.get("promotion_decision", ""),
                "evidence_type": row.get("evidence_type", ""),
                "human_review_needed": row.get("human_review_needed", ""),
                "human_spot_check_verdict": spot_checks.get(row["signature_review_id"], {}).get("human_spot_check_verdict", ""),
                "researcher_batch_acceptance": acceptance_note if accept_all_52 else "",
                "human_review_needed_after_spot_check": spot_checks.get(row["signature_review_id"], {}).get(
                    "human_review_needed_after_spot_check",
                    "no" if accept_all_52 else row.get("human_review_needed", ""),
                ),
                "codex_review_note": compact_note(row),
            }
        )


with open("../output/manual_queue_web_review_batches/manual_queue_web_review_run_log.csv", "w", encoding="utf-8", newline="") as output_file:
    writer = csv.DictWriter(output_file, fieldnames=["signature_review_id", "run_status", "note"])
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                "signature_review_id": row["signature_review_id"],
                "run_status": "completed",
                "note": (
                    f"{row.get('promotion_decision', '')} / "
                    f"{row.get('confidence', '')} / "
                    f"human_review_needed={row.get('human_review_needed', '')}"
                ),
            }
        )
