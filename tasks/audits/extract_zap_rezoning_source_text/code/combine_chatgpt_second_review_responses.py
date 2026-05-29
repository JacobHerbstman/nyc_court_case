#!/usr/bin/env python3

import csv
import glob
import io
import os


# setwd("tasks/audits/extract_zap_rezoning_source_text/code")

EXPECTED_COLUMNS = [
    "project_id",
    "second_review_status",
    "first_pass_direction",
    "first_pass_confidence",
    "second_pass_direction",
    "second_pass_class",
    "second_pass_housing_intent",
    "second_pass_scope_type",
    "up_component_present",
    "down_component_present",
    "dominant_capacity_effect",
    "mixed_split_needed",
    "manual_review_priority",
    "second_pass_confidence",
    "review_recommendation",
    "key_source_citation",
    "second_pass_note",
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
ALLOWED_YES_NO_UNCLEAR = {"yes", "no", "unclear"}
ALLOWED_PRIORITY = {"high", "medium", "low"}
ALLOWED_CONFIDENCE = {"high", "medium", "low"}
ALLOWED_RECOMMENDATION = {
    "accept_first_pass",
    "revise_direction",
    "keep_mixed_needs_split",
    "needs_manual_source_review",
    "extraction_failure",
}


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


with open("../output/zap_rezoning_chatgpt_manual_review_queue.csv", "r", encoding="utf-8", newline="") as input_file:
    manual_rows = list(csv.DictReader(input_file))

target_rows = []
for row in manual_rows:
    if row["suggested_confidence"] == "medium" or row["suggested_rezoning_direction"] == "mixed":
        target_row = dict(row)
        target_row["second_review_target_rank"] = str(len(target_rows) + 1)
        target_row["second_review_reason"] = "|".join(
            reason
            for reason in [
                "first_pass_medium_confidence" if row["suggested_confidence"] == "medium" else "",
                "first_pass_mixed" if row["suggested_rezoning_direction"] == "mixed" else "",
            ]
            if reason
        )
        target_rows.append(target_row)

with open("../output/zap_rezoning_chatgpt_second_review_batch_manifest.csv", "r", encoding="utf-8", newline="") as input_file:
    batch_rows = list(csv.DictReader(input_file))

batch_by_project_id = {}
for batch_row in batch_rows:
    for project_id in batch_row["project_ids"].split("|"):
        batch_by_project_id[project_id] = batch_row["batch_id"]

response_rows = []
malformed_files = []

for response_path in sorted(glob.glob("../output/chatgpt_second_review_responses/*.csv")):
    with open(response_path, "r", encoding="utf-8-sig", newline="") as input_file:
        reader = csv.DictReader(input_file)
        if reader.fieldnames != EXPECTED_COLUMNS:
            malformed_files.append(os.path.basename(response_path))
            continue
        for row in reader:
            response_rows.append(
                {
                    "second_review_response_file": os.path.basename(response_path),
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

target_by_project_id = {row["project_id"]: row for row in target_rows}
missing_project_ids = [
    row["project_id"]
    for row in target_rows
    if row["project_id"] not in seen_project_ids
]

invalid_response_rows = []
for row in deduped_response_rows:
    invalid_reasons = []
    target_row = target_by_project_id.get(row["project_id"])
    if not target_row:
        invalid_reasons.append("project_id_not_in_second_review_queue")
    if target_row and row["first_pass_direction"] != target_row["suggested_rezoning_direction"]:
        invalid_reasons.append("first_pass_direction_mismatch")
    if target_row and row["first_pass_confidence"] != target_row["suggested_confidence"]:
        invalid_reasons.append("first_pass_confidence_mismatch")
    if row["second_pass_direction"] not in ALLOWED_DIRECTIONS:
        invalid_reasons.append("invalid_second_pass_direction")
    if row["second_pass_housing_intent"] not in ALLOWED_HOUSING_INTENT:
        invalid_reasons.append("invalid_second_pass_housing_intent")
    if row["second_pass_scope_type"] not in ALLOWED_SCOPE_TYPES:
        invalid_reasons.append("invalid_second_pass_scope_type")
    if row["up_component_present"] not in ALLOWED_YES_NO_UNCLEAR:
        invalid_reasons.append("invalid_up_component_present")
    if row["down_component_present"] not in ALLOWED_YES_NO_UNCLEAR:
        invalid_reasons.append("invalid_down_component_present")
    if row["dominant_capacity_effect"] not in ALLOWED_DIRECTIONS:
        invalid_reasons.append("invalid_dominant_capacity_effect")
    if row["mixed_split_needed"] not in ALLOWED_YES_NO_UNCLEAR:
        invalid_reasons.append("invalid_mixed_split_needed")
    if row["manual_review_priority"] not in ALLOWED_PRIORITY:
        invalid_reasons.append("invalid_manual_review_priority")
    if row["second_pass_confidence"] not in ALLOWED_CONFIDENCE:
        invalid_reasons.append("invalid_second_pass_confidence")
    if row["review_recommendation"] not in ALLOWED_RECOMMENDATION:
        invalid_reasons.append("invalid_review_recommendation")
    if invalid_reasons:
        invalid_response_rows.append(
            {
                "project_id": row["project_id"],
                "second_review_response_file": row["second_review_response_file"],
                "invalid_reasons": "|".join(invalid_reasons),
            }
        )

response_by_project_id = {row["project_id"]: row for row in deduped_response_rows}
manual_review_rows = []

for target_row in target_rows:
    response_row = response_by_project_id.get(target_row["project_id"], {})
    manual_review_rows.append(
        {
            "project_id": target_row["project_id"],
            "second_review_batch_id": batch_by_project_id.get(target_row["project_id"], ""),
            "second_review_target_rank": target_row["second_review_target_rank"],
            "second_review_reason": target_row["second_review_reason"],
            "completed_year": target_row["completed_year"],
            "project_name": target_row["project_name"],
            "borough_name_standardized": target_row["borough_name_standardized"],
            "affected_lot_acres": target_row["affected_lot_acres"],
            "first_pass_response_file": target_row["source_response_file"],
            "first_pass_direction": target_row["suggested_rezoning_direction"],
            "first_pass_class": target_row["suggested_rezoning_class"],
            "first_pass_housing_intent": target_row["suggested_housing_intent"],
            "first_pass_scope_type": target_row["suggested_scope_type"],
            "first_pass_scope_blocks": target_row["suggested_scope_blocks"],
            "first_pass_scope_acres": target_row["suggested_scope_acres"],
            "first_pass_confidence": target_row["suggested_confidence"],
            "first_pass_evidence_note": target_row["suggested_evidence_note"],
            "second_review_response_file": response_row.get("second_review_response_file", ""),
            "second_review_status": response_row.get("second_review_status", ""),
            "second_pass_direction": response_row.get("second_pass_direction", ""),
            "second_pass_class": response_row.get("second_pass_class", ""),
            "second_pass_housing_intent": response_row.get("second_pass_housing_intent", ""),
            "second_pass_scope_type": response_row.get("second_pass_scope_type", ""),
            "up_component_present": response_row.get("up_component_present", ""),
            "down_component_present": response_row.get("down_component_present", ""),
            "dominant_capacity_effect": response_row.get("dominant_capacity_effect", ""),
            "mixed_split_needed": response_row.get("mixed_split_needed", ""),
            "manual_review_priority": response_row.get("manual_review_priority", ""),
            "second_pass_confidence": response_row.get("second_pass_confidence", ""),
            "review_recommendation": response_row.get("review_recommendation", ""),
            "key_source_citation": response_row.get("key_source_citation", ""),
            "second_pass_note": response_row.get("second_pass_note", ""),
            "text_candidate_direction": target_row["text_candidate_direction"],
            "missing_direction_reason": target_row["missing_direction_reason"],
            "text_zoning_codes": target_row["text_zoning_codes"],
            "parsed_zoning_changes": target_row["parsed_zoning_changes"],
            "source_documents": target_row["source_documents"],
            "official_source_evidence": target_row["official_source_evidence"],
        }
    )

missing_rows = [
    {
        "project_id": project_id,
        "second_review_batch_id": batch_by_project_id.get(project_id, ""),
        "second_review_target_rank": target_by_project_id[project_id]["second_review_target_rank"],
        "completed_year": target_by_project_id[project_id]["completed_year"],
        "project_name": target_by_project_id[project_id]["project_name"],
        "first_pass_direction": target_by_project_id[project_id]["suggested_rezoning_direction"],
        "first_pass_confidence": target_by_project_id[project_id]["suggested_confidence"],
        "affected_lot_acres": target_by_project_id[project_id]["affected_lot_acres"],
    }
    for project_id in missing_project_ids
]

qc_rows = [
    {
        "metric": "second_review_target_project_count",
        "value": len(target_rows),
        "status": "pass" if len(target_rows) > 0 else "fail",
        "note": "First-pass medium-confidence projects plus all first-pass mixed projects.",
    },
    {
        "metric": "second_review_response_file_count",
        "value": len(glob.glob("../output/chatgpt_second_review_responses/*.csv")),
        "status": "pass" if len(glob.glob("../output/chatgpt_second_review_responses/*.csv")) > 0 else "needs_more_labels",
        "note": "Raw second-pass ChatGPT CSV response files found.",
    },
    {
        "metric": "malformed_second_review_response_file_count",
        "value": len(malformed_files),
        "status": "pass" if len(malformed_files) == 0 else "fail",
        "note": "|".join(malformed_files),
    },
    {
        "metric": "unique_second_review_labeled_project_count",
        "value": len(seen_project_ids),
        "status": "pass",
        "note": "Unique target project IDs returned by ChatGPT so far.",
    },
    {
        "metric": "missing_second_review_project_count",
        "value": len(missing_project_ids),
        "status": "pass" if len(missing_project_ids) == 0 else "needs_more_labels",
        "note": "Target projects still missing a second-pass suggestion.",
    },
    {
        "metric": "duplicate_second_review_project_count",
        "value": len(duplicate_project_ids),
        "status": "pass" if len(duplicate_project_ids) == 0 else "fail",
        "note": "|".join(sorted(duplicate_project_ids)),
    },
    {
        "metric": "invalid_second_review_row_count",
        "value": len(invalid_response_rows),
        "status": "pass" if len(invalid_response_rows) == 0 else "fail",
        "note": "Rows with invalid controlled-vocabulary values, mismatched first-pass labels, or unknown project IDs.",
    },
]

write_csv_if_changed(
    deduped_response_rows,
    ["second_review_response_file", *EXPECTED_COLUMNS],
    "../output/zap_rezoning_chatgpt_second_review_responses_combined.csv",
)

write_csv_if_changed(
    manual_review_rows,
    [
        "project_id",
        "second_review_batch_id",
        "second_review_target_rank",
        "second_review_reason",
        "completed_year",
        "project_name",
        "borough_name_standardized",
        "affected_lot_acres",
        "first_pass_response_file",
        "first_pass_direction",
        "first_pass_class",
        "first_pass_housing_intent",
        "first_pass_scope_type",
        "first_pass_scope_blocks",
        "first_pass_scope_acres",
        "first_pass_confidence",
        "first_pass_evidence_note",
        "second_review_response_file",
        "second_review_status",
        "second_pass_direction",
        "second_pass_class",
        "second_pass_housing_intent",
        "second_pass_scope_type",
        "up_component_present",
        "down_component_present",
        "dominant_capacity_effect",
        "mixed_split_needed",
        "manual_review_priority",
        "second_pass_confidence",
        "review_recommendation",
        "key_source_citation",
        "second_pass_note",
        "text_candidate_direction",
        "missing_direction_reason",
        "text_zoning_codes",
        "parsed_zoning_changes",
        "source_documents",
        "official_source_evidence",
    ],
    "../output/zap_rezoning_chatgpt_second_manual_review_queue.csv",
)

write_csv_if_changed(
    missing_rows,
    [
        "project_id",
        "second_review_batch_id",
        "second_review_target_rank",
        "completed_year",
        "project_name",
        "first_pass_direction",
        "first_pass_confidence",
        "affected_lot_acres",
    ],
    "../output/zap_rezoning_chatgpt_second_review_missing_projects.csv",
)

write_csv_if_changed(
    invalid_response_rows,
    ["project_id", "second_review_response_file", "invalid_reasons"],
    "../output/zap_rezoning_chatgpt_second_review_invalid_rows.csv",
)

write_csv_if_changed(
    qc_rows,
    ["metric", "value", "status", "note"],
    "../output/zap_rezoning_chatgpt_second_review_response_qc.csv",
)

print(
    f"Combined {len(deduped_response_rows)} unique second-review labels; "
    f"{len(missing_project_ids)} target projects remain unlabeled."
)
