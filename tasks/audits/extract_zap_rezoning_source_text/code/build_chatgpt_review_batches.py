#!/usr/bin/env python3

import csv
import os
import textwrap


# setwd("tasks/audits/extract_zap_rezoning_source_text/code")

MAX_PROJECTS_PER_BATCH = 6
TARGET_BATCH_CHAR_LIMIT = 90000


PROMPT_HEADER = textwrap.dedent(
    """\
    # ZAP Rezoning Source-Based First-Pass Review

    Classify each NYC zoning map amendment using only the official-source evidence below.
    This is a first pass for human review, not a final research label.

    Return CSV with exactly these columns:
    project_id,chatgpt_review_status,suggested_rezoning_direction,suggested_rezoning_class,suggested_housing_intent,suggested_scope_type,suggested_scope_blocks,suggested_scope_acres,suggested_confidence,suggested_evidence_note

    Allowed suggested_rezoning_direction values: upzoning, downzoning, mixed, no_material_residential_change, unknown.
    Allowed suggested_housing_intent values: yes, no, unclear.
    Allowed suggested_scope_type values: single_site, small_area, corridor, neighborhood, large_neighborhood, very_large_neighborhood, unknown.
    Allowed suggested_confidence values: high, medium, low.

    Rules:
    - Base direction on residential capacity, not whether the project is politically described as neighborhood preservation.
    - Treat contextual/form restrictions as downzoning or mixed when they reduce residential envelope even without a lower numeric FAR.
    - Treat commercial overlays alone as no_material_residential_change unless the source shows an underlying residential district change.
    - Use unknown when evidence does not identify the before/after zoning or residential capacity implication.
    - In suggested_evidence_note, cite the document number and page number from the evidence.

    """
)


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
    import io

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


with open("../output/zap_rezoning_chatgpt_review_packet.csv", "r", encoding="utf-8", newline="") as input_file:
    packet_rows = list(csv.DictReader(input_file))

project_records = []
for row in packet_rows:
    project_records.append(
        "\n".join(
            [
                "-----",
                f"project_id: {row['project_id']}",
                f"completed_year: {row['completed_year']}",
                f"project_name: {row['project_name']}",
                f"borough: {row['borough_name_standardized']}",
                f"affected_lot_acres_current_bbl_scope: {row['affected_lot_acres']}",
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

os.makedirs("../output/chatgpt_batches", exist_ok=True)

batch_rows = []
current_records = []
current_packet_rows = []

for row, record in zip(packet_rows, project_records):
    candidate_text = PROMPT_HEADER + "".join(current_records + [record])
    batch_is_full = len(current_records) >= MAX_PROJECTS_PER_BATCH
    batch_is_too_long = (
        len(current_records) > 0
        and len(candidate_text) > TARGET_BATCH_CHAR_LIMIT
    )

    if batch_is_full or batch_is_too_long:
        batch_id = f"{len(batch_rows) + 1:03d}"
        batch_text = PROMPT_HEADER + "".join(current_records)
        batch_path = f"../output/chatgpt_batches/zap_rezoning_chatgpt_review_batch_{batch_id}.md"
        write_text_if_changed(batch_text, batch_path)
        batch_rows.append(
            {
                "batch_id": batch_id,
                "batch_path": batch_path,
                "project_count": len(current_packet_rows),
                "first_queue_rank": current_packet_rows[0]["queue_rank"],
                "last_queue_rank": current_packet_rows[-1]["queue_rank"],
                "char_count": len(batch_text),
                "project_ids": "|".join(project_row["project_id"] for project_row in current_packet_rows),
            }
        )
        current_records = []
        current_packet_rows = []

    current_records.append(record)
    current_packet_rows.append(row)

if current_records:
    batch_id = f"{len(batch_rows) + 1:03d}"
    batch_text = PROMPT_HEADER + "".join(current_records)
    batch_path = f"../output/chatgpt_batches/zap_rezoning_chatgpt_review_batch_{batch_id}.md"
    write_text_if_changed(batch_text, batch_path)
    batch_rows.append(
        {
            "batch_id": batch_id,
            "batch_path": batch_path,
            "project_count": len(current_packet_rows),
            "first_queue_rank": current_packet_rows[0]["queue_rank"],
            "last_queue_rank": current_packet_rows[-1]["queue_rank"],
            "char_count": len(batch_text),
            "project_ids": "|".join(project_row["project_id"] for project_row in current_packet_rows),
        }
    )

write_csv_if_changed(
    batch_rows,
    [
        "batch_id",
        "batch_path",
        "project_count",
        "first_queue_rank",
        "last_queue_rank",
        "char_count",
        "project_ids",
    ],
    "../output/zap_rezoning_chatgpt_review_batch_manifest.csv",
)

print(f"Wrote {len(batch_rows)} ChatGPT review batches for {len(packet_rows)} projects.")
