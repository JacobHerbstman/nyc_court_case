#!/usr/bin/env python3

import csv
import glob
import io
import os



EXPECTED_COLUMNS = [
    "project_id",
    "chatgpt_review_status",
    "suggested_rezoning_direction",
    "suggested_rezoning_class",
    "suggested_housing_intent",
    "suggested_scope_type",
    "suggested_scope_blocks",
    "suggested_scope_acres",
    "suggested_confidence",
    "suggested_evidence_note",
]

ALLOWED_DIRECTIONS = {
    "upzoning",
    "downzoning",
    "mixed",
    "no_material_residential_change",
    "unknown",
}
ALLOWED_HOUSING_INTENT = {"yes", "no", "unclear"}
ALLOWED_SCOPE_TYPES = {
    "single_site",
    "small_area",
    "corridor",
    "neighborhood",
    "large_neighborhood",
    "very_large_neighborhood",
    "unknown",
}
ALLOWED_CONFIDENCE = {"high", "medium", "low"}


def write_csv_if_changed(rows, fieldnames, path):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    new_text = output.getvalue()

    try:
        with open(path, "r", encoding="utf-8", newline="") as existing_file:
            old_text = existing_file.read()
    except FileNotFoundError:
        old_text = None

    if old_text != new_text:
        with open(path, "w", encoding="utf-8", newline="") as output_file:
            output_file.write(new_text)


with open("../output/zap_rezoning_chatgpt_review_packet.csv", "r", encoding="utf-8", newline="") as input_file:
    packet_rows = list(csv.DictReader(input_file))

with open("../output/zap_rezoning_chatgpt_review_batch_manifest.csv", "r", encoding="utf-8", newline="") as input_file:
    batch_rows = list(csv.DictReader(input_file))

batch_by_project_id = {}
for batch_row in batch_rows:
    for project_id in batch_row["project_ids"].split("|"):
        batch_by_project_id[project_id] = batch_row["batch_id"]

response_rows = []
malformed_files = []

for response_path in sorted(glob.glob("../output/chatgpt_responses/*.csv")):
    with open(response_path, "r", encoding="utf-8-sig", newline="") as input_file:
        reader = csv.DictReader(input_file)
        if reader.fieldnames != EXPECTED_COLUMNS:
            malformed_files.append(os.path.basename(response_path))
            continue
        for row in reader:
            response_rows.append(
                {
                    "source_response_file": os.path.basename(response_path),
                    **{column: row.get(column, "") for column in EXPECTED_COLUMNS},
                }
            )

seen_project_ids = set()
duplicate_project_ids = set()
deduped_response_rows = []

for row in response_rows:
    project_id = row["project_id"]
    if project_id in seen_project_ids:
        duplicate_project_ids.add(project_id)
        continue
    seen_project_ids.add(project_id)
    deduped_response_rows.append(row)

packet_by_project_id = {row["project_id"]: row for row in packet_rows}
missing_project_ids = [
    row["project_id"]
    for row in packet_rows
    if row["project_id"] not in seen_project_ids
]

invalid_response_rows = []
for row in deduped_response_rows:
    invalid_reasons = []
    if row["project_id"] not in packet_by_project_id:
        invalid_reasons.append("project_id_not_in_review_packet")
    if row["suggested_rezoning_direction"] not in ALLOWED_DIRECTIONS:
        invalid_reasons.append("invalid_suggested_rezoning_direction")
    if row["suggested_housing_intent"] not in ALLOWED_HOUSING_INTENT:
        invalid_reasons.append("invalid_suggested_housing_intent")
    if row["suggested_scope_type"] not in ALLOWED_SCOPE_TYPES:
        invalid_reasons.append("invalid_suggested_scope_type")
    if row["suggested_confidence"] not in ALLOWED_CONFIDENCE:
        invalid_reasons.append("invalid_suggested_confidence")
    if invalid_reasons:
        invalid_response_rows.append(
            {
                "project_id": row["project_id"],
                "source_response_file": row["source_response_file"],
                "invalid_reasons": "|".join(invalid_reasons),
            }
        )

manual_review_rows = []
response_by_project_id = {row["project_id"]: row for row in deduped_response_rows}

for packet_row in packet_rows:
    response_row = response_by_project_id.get(packet_row["project_id"], {})
    manual_review_rows.append(
        {
            "project_id": packet_row["project_id"],
            "batch_id": batch_by_project_id.get(packet_row["project_id"], ""),
            "completed_year": packet_row["completed_year"],
            "project_name": packet_row["project_name"],
            "borough_name_standardized": packet_row["borough_name_standardized"],
            "affected_lot_acres": packet_row["affected_lot_acres"],
            "missing_direction_reason": packet_row["missing_direction_reason"],
            "text_candidate_direction": packet_row["text_candidate_direction"],
            "text_zoning_codes": packet_row["text_zoning_codes"],
            "parsed_zoning_changes": packet_row["parsed_zoning_changes"],
            "selected_document_count": packet_row["selected_document_count"],
            "evidence_snippet_count": packet_row["evidence_snippet_count"],
            "source_response_file": response_row.get("source_response_file", ""),
            "chatgpt_review_status": response_row.get("chatgpt_review_status", ""),
            "suggested_rezoning_direction": response_row.get("suggested_rezoning_direction", ""),
            "suggested_rezoning_class": response_row.get("suggested_rezoning_class", ""),
            "suggested_housing_intent": response_row.get("suggested_housing_intent", ""),
            "suggested_scope_type": response_row.get("suggested_scope_type", ""),
            "suggested_scope_blocks": response_row.get("suggested_scope_blocks", ""),
            "suggested_scope_acres": response_row.get("suggested_scope_acres", ""),
            "suggested_confidence": response_row.get("suggested_confidence", ""),
            "suggested_evidence_note": response_row.get("suggested_evidence_note", ""),
            "source_documents": packet_row["source_documents"],
            "official_source_evidence": packet_row["official_source_evidence"],
        }
    )

missing_rows = [
    {
        "project_id": project_id,
        "batch_id": batch_by_project_id.get(project_id, ""),
        "queue_rank": packet_by_project_id[project_id]["queue_rank"],
        "completed_year": packet_by_project_id[project_id]["completed_year"],
        "project_name": packet_by_project_id[project_id]["project_name"],
        "affected_lot_acres": packet_by_project_id[project_id]["affected_lot_acres"],
    }
    for project_id in missing_project_ids
]

qc_rows = [
    {
        "metric": "review_packet_project_count",
        "value": len(packet_rows),
        "status": "pass" if len(packet_rows) > 0 else "fail",
        "note": "Projects in the source-evidence review packet.",
    },
    {
        "metric": "response_file_count",
        "value": len(glob.glob("../output/chatgpt_responses/*.csv")),
        "status": "pass" if len(glob.glob("../output/chatgpt_responses/*.csv")) > 0 else "fail",
        "note": "Raw ChatGPT CSV response files found.",
    },
    {
        "metric": "malformed_response_file_count",
        "value": len(malformed_files),
        "status": "pass" if len(malformed_files) == 0 else "fail",
        "note": "|".join(malformed_files),
    },
    {
        "metric": "unique_labeled_project_count",
        "value": len(seen_project_ids),
        "status": "pass",
        "note": "Unique project IDs returned by ChatGPT so far.",
    },
    {
        "metric": "missing_project_count",
        "value": len(missing_project_ids),
        "status": "pass" if len(missing_project_ids) == 0 else "needs_more_labels",
        "note": "Projects still missing a ChatGPT suggestion.",
    },
    {
        "metric": "duplicate_labeled_project_count",
        "value": len(duplicate_project_ids),
        "status": "pass" if len(duplicate_project_ids) == 0 else "fail",
        "note": "|".join(sorted(duplicate_project_ids)),
    },
    {
        "metric": "invalid_response_row_count",
        "value": len(invalid_response_rows),
        "status": "pass" if len(invalid_response_rows) == 0 else "fail",
        "note": "Rows with invalid controlled-vocabulary values or unknown project IDs.",
    },
]

write_csv_if_changed(
    deduped_response_rows,
    ["source_response_file", *EXPECTED_COLUMNS],
    "../output/zap_rezoning_chatgpt_review_responses_combined.csv",
)

write_csv_if_changed(
    manual_review_rows,
    [
        "project_id",
        "batch_id",
        "completed_year",
        "project_name",
        "borough_name_standardized",
        "affected_lot_acres",
        "missing_direction_reason",
        "text_candidate_direction",
        "text_zoning_codes",
        "parsed_zoning_changes",
        "selected_document_count",
        "evidence_snippet_count",
        "source_response_file",
        "chatgpt_review_status",
        "suggested_rezoning_direction",
        "suggested_rezoning_class",
        "suggested_housing_intent",
        "suggested_scope_type",
        "suggested_scope_blocks",
        "suggested_scope_acres",
        "suggested_confidence",
        "suggested_evidence_note",
        "source_documents",
        "official_source_evidence",
    ],
    "../output/zap_rezoning_chatgpt_manual_review_queue.csv",
)

write_csv_if_changed(
    missing_rows,
    ["project_id", "batch_id", "queue_rank", "completed_year", "project_name", "affected_lot_acres"],
    "../output/zap_rezoning_chatgpt_review_missing_projects.csv",
)

write_csv_if_changed(
    invalid_response_rows,
    ["project_id", "source_response_file", "invalid_reasons"],
    "../output/zap_rezoning_chatgpt_review_invalid_rows.csv",
)

write_csv_if_changed(
    qc_rows,
    ["metric", "value", "status", "note"],
    "../output/zap_rezoning_chatgpt_review_response_qc.csv",
)

print(
    f"Combined {len(deduped_response_rows)} unique ChatGPT labels; "
    f"{len(missing_project_ids)} projects remain unlabeled."
)
