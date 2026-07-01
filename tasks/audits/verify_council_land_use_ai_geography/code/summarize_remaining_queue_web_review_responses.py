#!/usr/bin/env python3

import csv
import json
import re




def parse_response_text(row):
    text = row.get("response_text", "").strip()
    if not text:
        raise ValueError(f"Missing response_text for {row.get('signature_review_id', '')}")

    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"No JSON object found in response_text for {row.get('signature_review_id', '')}")

    try:
        parsed = json.JSONDecoder(strict=False).decode(text[start : end + 1])
    except json.JSONDecodeError:
        parsed = {
            "signature_review_id": extract_string_field(text, "signature_review_id"),
            "recommended_council_districts": extract_districts(text),
            "confidence": extract_string_field(text, "confidence"),
            "promotion_decision": extract_string_field(text, "promotion_decision"),
            "evidence_type": extract_string_field(text, "evidence_type"),
            "human_review_needed": extract_string_field(text, "human_review_needed"),
            "official_geography_basis": extract_string_field(text, "official_geography_basis"),
        }
    if parsed["signature_review_id"] != row["signature_review_id"]:
        raise ValueError(
            "Response ID mismatch: "
            f"ledger={row['signature_review_id']} parsed={parsed['signature_review_id']}"
        )
    return parsed


def extract_string_field(text, field):
    match = re.search(rf'"{field}"\s*:\s*"(.+?)"\s*(?:,|\}})', text, flags=re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract {field} from malformed JSON response")
    return match.group(1).replace('\\"', '"').replace("\\n", " ").strip()


def extract_districts(text):
    match = re.search(r'"recommended_council_districts"\s*:\s*(\[[^\]]*\]|"[^"]*")', text, flags=re.DOTALL)
    if not match:
        raise ValueError("Could not extract recommended_council_districts from malformed JSON response")
    value = match.group(1).strip()
    if value.startswith('"'):
        return value.strip('"')
    if value == "[]":
        return []
    return [part.strip().strip('"') for part in value.strip("[]").split(",") if part.strip()]


def district_string(value):
    if isinstance(value, list):
        return ";".join(str(district) for district in value)
    return str(value)


def compact_note(row):
    text = " ".join(row.get("official_geography_basis", "").split())
    if len(text) > 260:
        text = text[:257].rstrip() + "..."
    return text


def include_in_geography_repair(row):
    return (
        row.get("human_review_needed", "") == "no"
        and row.get("promotion_decision", "") in {"promote", "promote_with_caveat"}
        and district_string(row.get("recommended_council_districts", "")).strip() != ""
    )


rows = []
seen_ids = set()
with open("../output/remaining_queue_web_review_batches/remaining_queue_web_review_responses.jsonl", "r", encoding="utf-8") as input_file:
    for line_number, line in enumerate(input_file, start=1):
        if line.strip():
            ledger_row = json.loads(line)
            if ledger_row["signature_review_id"] in seen_ids:
                raise ValueError(f"Duplicate signature_review_id in response ledger: {ledger_row['signature_review_id']}")
            parsed_row = parse_response_text(ledger_row)
            parsed_row["batch_file"] = ledger_row.get("batch_file", "")
            parsed_row["conversation_url"] = ledger_row.get("conversation_url", "")
            parsed_row["prompt_chars"] = ledger_row.get("prompt_chars", "")
            rows.append(parsed_row)
            seen_ids.add(ledger_row["signature_review_id"])


adjudications = {}
with open("remaining_queue_researcher_adjudication.csv", "r", encoding="utf-8", newline="") as input_file:
    for row in csv.DictReader(input_file):
        if row["signature_review_id"] in adjudications:
            raise ValueError(f"Duplicate signature_review_id in researcher adjudication: {row['signature_review_id']}")
        adjudications[row["signature_review_id"]] = row

missing_adjudication_ids = set(adjudications) - seen_ids
if missing_adjudication_ids:
    raise ValueError(f"Researcher adjudication contains IDs absent from response ledger: {sorted(missing_adjudication_ids)}")


with open("../output/remaining_queue_web_review_batches/remaining_queue_web_review_summary.csv", "w", encoding="utf-8", newline="") as output_file:
    writer = csv.DictWriter(
        output_file,
        fieldnames=[
            "signature_review_id",
            "batch_file",
            "recommended_council_districts",
            "confidence",
            "promotion_decision",
            "evidence_type",
            "human_review_needed",
            "researcher_adjudication",
            "final_promotion_decision",
            "final_recommended_council_districts",
            "final_include_in_geography_repair",
            "final_human_review_needed",
            "researcher_adjudication_note",
            "prompt_chars",
            "conversation_url",
            "codex_review_note",
        ],
    )
    writer.writeheader()
    for row in rows:
        adjudication = adjudications.get(row["signature_review_id"], {})
        final_promotion_decision = adjudication.get("researcher_adjudication", row.get("promotion_decision", ""))
        final_recommended_council_districts = adjudication.get(
            "final_recommended_council_districts",
            district_string(row.get("recommended_council_districts", "")),
        )
        final_include_in_geography_repair = adjudication.get(
            "final_include_in_geography_repair",
            "yes" if include_in_geography_repair(row) else "no",
        )
        final_human_review_needed = "no" if adjudication else row.get("human_review_needed", "")
        writer.writerow(
            {
                "signature_review_id": row["signature_review_id"],
                "batch_file": row.get("batch_file", ""),
                "recommended_council_districts": district_string(row.get("recommended_council_districts", "")),
                "confidence": row.get("confidence", ""),
                "promotion_decision": row.get("promotion_decision", ""),
                "evidence_type": row.get("evidence_type", ""),
                "human_review_needed": row.get("human_review_needed", ""),
                "researcher_adjudication": adjudication.get("researcher_adjudication", ""),
                "final_promotion_decision": final_promotion_decision,
                "final_recommended_council_districts": final_recommended_council_districts,
                "final_include_in_geography_repair": final_include_in_geography_repair,
                "final_human_review_needed": final_human_review_needed,
                "researcher_adjudication_note": adjudication.get("decision_note", ""),
                "prompt_chars": row.get("prompt_chars", ""),
                "conversation_url": row.get("conversation_url", ""),
                "codex_review_note": compact_note(row),
            }
        )


with open("../output/remaining_queue_web_review_batches/remaining_queue_web_review_run_log.csv", "w", encoding="utf-8", newline="") as output_file:
    writer = csv.DictWriter(output_file, fieldnames=["signature_review_id", "run_status", "note"])
    writer.writeheader()
    for row in rows:
        adjudication = adjudications.get(row["signature_review_id"], {})
        final_human_review_needed = "no" if adjudication else row.get("human_review_needed", "")
        writer.writerow(
            {
                "signature_review_id": row["signature_review_id"],
                "run_status": "completed",
                "note": (
                    f"{row.get('promotion_decision', '')} / "
                    f"{row.get('confidence', '')} / "
                    f"human_review_needed={row.get('human_review_needed', '')} / "
                    f"final_human_review_needed={final_human_review_needed}"
                ),
            }
        )


with open("../output/remaining_queue_web_review_batches/remaining_queue_human_review_needed_materials.csv", "w", encoding="utf-8", newline="") as output_file:
    writer = csv.DictWriter(
        output_file,
        fieldnames=[
            "signature_review_id",
            "promotion_decision",
            "evidence_type",
            "recommended_council_districts",
            "prompt_file",
            "batch_file",
            "conversation_url",
            "researcher_adjudication",
            "final_recommended_council_districts",
            "final_include_in_geography_repair",
            "final_human_review_needed",
            "researcher_adjudication_note",
            "codex_review_note",
        ],
    )
    writer.writeheader()
    for row in rows:
        if row.get("human_review_needed", "") == "yes":
            writer.writerow(
                {
                    "signature_review_id": row["signature_review_id"],
                    "promotion_decision": row.get("promotion_decision", ""),
                    "evidence_type": row.get("evidence_type", ""),
                    "recommended_council_districts": district_string(row.get("recommended_council_districts", "")),
                    "prompt_file": f"{row['signature_review_id']}_remaining_queue_web_prompt.md",
                    "batch_file": row.get("batch_file", ""),
                    "conversation_url": row.get("conversation_url", ""),
                    "researcher_adjudication": adjudications.get(row["signature_review_id"], {}).get("researcher_adjudication", ""),
                    "final_recommended_council_districts": adjudications.get(row["signature_review_id"], {}).get(
                        "final_recommended_council_districts",
                        "",
                    ),
                    "final_include_in_geography_repair": adjudications.get(row["signature_review_id"], {}).get(
                        "final_include_in_geography_repair",
                        "no",
                    ),
                    "final_human_review_needed": "no" if row["signature_review_id"] in adjudications else row.get("human_review_needed", ""),
                    "researcher_adjudication_note": adjudications.get(row["signature_review_id"], {}).get("decision_note", ""),
                    "codex_review_note": compact_note(row),
                }
            )
