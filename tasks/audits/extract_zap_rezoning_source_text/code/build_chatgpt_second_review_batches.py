#!/usr/bin/env python3

import csv
import io
import textwrap


# setwd("tasks/audits/extract_zap_rezoning_source_text/code")

MAX_PROJECTS_PER_BATCH = 4
TARGET_BATCH_CHAR_LIMIT = 90000


PROMPT_HEADER = textwrap.dedent(
    """\
    # ZAP Rezoning Source-Based Second Review

    Audit the first-pass classification for each NYC zoning map amendment using only the official-source evidence below.
    This second pass focuses on projects that were first-pass medium confidence or first-pass mixed.

    Return CSV with exactly these columns:
    project_id,second_review_status,first_pass_direction,first_pass_confidence,second_pass_direction,second_pass_class,second_pass_housing_intent,second_pass_scope_type,up_component_present,down_component_present,dominant_capacity_effect,mixed_split_needed,manual_review_priority,second_pass_confidence,review_recommendation,key_source_citation,second_pass_note

    Allowed second_pass_direction values: upzoning, downzoning, mixed, no_material_residential_change, unknown.
    Allowed second_pass_housing_intent values: yes, no, unclear.
    Allowed second_pass_scope_type values: single_site, small_area, corridor, neighborhood, large_neighborhood, very_large_neighborhood, unknown.
    Allowed up_component_present values: yes, no, unclear.
    Allowed down_component_present values: yes, no, unclear.
    Allowed dominant_capacity_effect values: upzoning, downzoning, mixed, no_material_residential_change, unknown.
    Allowed mixed_split_needed values: yes, no, unclear.
    Allowed manual_review_priority values: high, medium, low.
    Allowed second_pass_confidence values: high, medium, low.
    Allowed review_recommendation values: accept_first_pass, revise_direction, keep_mixed_needs_split, needs_manual_source_review, extraction_failure.

    Rules:
    - Base direction on residential capacity, not on whether the project is described as neighborhood preservation.
    - Treat contextual envelope restrictions as downzoning or mixed when the source indicates lower residential development capacity.
    - Treat commercial overlays alone as no_material_residential_change unless the source shows an underlying residential district change.
    - For mixed projects, identify whether both upzoning and downzoning components are actually present.
    - Use keep_mixed_needs_split when the project appears truly mixed and should eventually be split into gross up/down components.
    - Use revise_direction only when the first-pass direction appears substantively wrong.
    - Use extraction_failure if the supplied evidence is unreadable or too thin to audit.
    - In key_source_citation, cite the strongest document and page reference available.
    - Keep second_pass_note concise and focused on why the second-pass label agrees with or changes the first pass.

    """
)

OUTPUT_COLUMNS = [
    "batch_id",
    "batch_path",
    "project_count",
    "first_target_rank",
    "last_target_rank",
    "char_count",
    "project_ids",
]


def write_text_if_changed(text, path):
    try:
        with open(path, "r", encoding="utf-8") as existing_file:
            old_text = existing_file.read()
    except FileNotFoundError:
        old_text = None

    if old_text != text:
        with open(path, "w", encoding="utf-8") as output_file:
            output_file.write(text)


def write_csv_if_changed(rows, fieldnames, path):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
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

project_records = []
for row in target_rows:
    project_records.append(
        "\n".join(
            [
                "-----",
                f"project_id: {row['project_id']}",
                f"second_review_target_rank: {row['second_review_target_rank']}",
                f"second_review_reason: {row['second_review_reason']}",
                f"completed_year: {row['completed_year']}",
                f"project_name: {row['project_name']}",
                f"borough: {row['borough_name_standardized']}",
                f"affected_lot_acres_current_bbl_scope: {row['affected_lot_acres']}",
                "",
                "first_pass_label:",
                f"direction: {row['suggested_rezoning_direction']}",
                f"class: {row['suggested_rezoning_class']}",
                f"housing_intent: {row['suggested_housing_intent']}",
                f"scope_type: {row['suggested_scope_type']}",
                f"scope_blocks: {row['suggested_scope_blocks']}",
                f"scope_acres: {row['suggested_scope_acres']}",
                f"confidence: {row['suggested_confidence']}",
                f"evidence_note: {row['suggested_evidence_note']}",
                "",
                "text_parser_inputs:",
                f"text_candidate_direction: {row['text_candidate_direction']}",
                f"missing_direction_reason: {row['missing_direction_reason']}",
                f"text_zoning_codes: {row['text_zoning_codes']}",
                f"parsed_zoning_changes: {row['parsed_zoning_changes']}",
                "",
                "source_documents:",
                row["source_documents"],
                "",
                "official_source_evidence:",
                row["official_source_evidence"],
                "",
            ]
        )
    )

batch_rows = []
current_records = []
current_target_rows = []

for row, record in zip(target_rows, project_records):
    candidate_text = PROMPT_HEADER + "".join(current_records + [record])
    batch_is_full = len(current_records) >= MAX_PROJECTS_PER_BATCH
    batch_is_too_long = len(current_records) > 0 and len(candidate_text) > TARGET_BATCH_CHAR_LIMIT

    if batch_is_full or batch_is_too_long:
        batch_id = f"{len(batch_rows) + 1:03d}"
        batch_text = PROMPT_HEADER + "".join(current_records)
        batch_path = f"../output/chatgpt_second_review_batches/zap_rezoning_chatgpt_second_review_batch_{batch_id}.md"
        write_text_if_changed(batch_text, batch_path)
        batch_rows.append(
            {
                "batch_id": batch_id,
                "batch_path": batch_path,
                "project_count": len(current_target_rows),
                "first_target_rank": current_target_rows[0]["second_review_target_rank"],
                "last_target_rank": current_target_rows[-1]["second_review_target_rank"],
                "char_count": len(batch_text),
                "project_ids": "|".join(project_row["project_id"] for project_row in current_target_rows),
            }
        )
        current_records = []
        current_target_rows = []

    current_records.append(record)
    current_target_rows.append(row)

if current_records:
    batch_id = f"{len(batch_rows) + 1:03d}"
    batch_text = PROMPT_HEADER + "".join(current_records)
    batch_path = f"../output/chatgpt_second_review_batches/zap_rezoning_chatgpt_second_review_batch_{batch_id}.md"
    write_text_if_changed(batch_text, batch_path)
    batch_rows.append(
        {
            "batch_id": batch_id,
            "batch_path": batch_path,
            "project_count": len(current_target_rows),
            "first_target_rank": current_target_rows[0]["second_review_target_rank"],
            "last_target_rank": current_target_rows[-1]["second_review_target_rank"],
            "char_count": len(batch_text),
            "project_ids": "|".join(project_row["project_id"] for project_row in current_target_rows),
        }
    )

write_csv_if_changed(batch_rows, OUTPUT_COLUMNS, "../output/zap_rezoning_chatgpt_second_review_batch_manifest.csv")

print(f"Wrote {len(batch_rows)} second-review batches for {len(target_rows)} medium/mixed projects.")
