#!/usr/bin/env python3

import csv
import hashlib
import io
import json
import math
import os
import sys
import textwrap

import pandas as pd


# setwd("tasks/audits/build_zap_rezoning_llm_review/code")
# batch_size <- 6
# max_review_projects <- 0
# random_audit_per_stratum <- 1
# prompt_version <- "v1"


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
    "citywide_or_text_only",
    "unknown",
}
ALLOWED_YES_NO_UNCLEAR = {"yes", "no", "unclear"}
ALLOWED_CONFIDENCE = {"high", "medium", "low", "unknown"}
ALLOWED_RECOMMENDATIONS = {
    "accept_first_pass",
    "revise_direction",
    "keep_mixed_needs_split",
    "needs_human_review",
    "insufficient_evidence",
}

PROMPT_HEADER = """# NYC ZAP Rezoning LLM Review

You are helping audit completed NYC zoning map amendments for an academic research database.

Use only the project metadata, parser facts, scope facts, and official-source excerpts supplied below. Do not use outside memory unless the supplied evidence is too thin, and if you do rely on outside knowledge, mark `outside_knowledge_used` as `yes` and explain the source in `review_note`.

Classify formal zoning-capacity effects, not whether the project narrative sounds pro-development or preservationist.

Return one compact JSON object per project, one object per line, with exactly this top-level schema:

```json
{
  "project_id": "",
  "prompt_version": "",
  "review_stage": "llm_source_review",
  "model": "",
  "outside_knowledge_used": "no",
  "official_documents_used": [
    {
      "document_title": "",
      "document_url": "",
      "pages_used": ""
    }
  ],
  "evidence_spans": [
    {
      "document_title": "",
      "page_number": "",
      "quoted_or_paraphrased_evidence": "",
      "why_it_matters": ""
    }
  ],
  "zoning_components": [
    {
      "from_zoning": "",
      "to_zoning": "",
      "component_direction": "upzoning",
      "component_scope": "",
      "capacity_reason": "",
      "residential_capacity_relevant": "yes"
    }
  ],
  "project_classification": {
    "second_pass_direction": "upzoning",
    "housing_intent": "yes",
    "scope_type": "single_site",
    "up_component_present": "yes",
    "down_component_present": "no",
    "dominant_capacity_effect": "upzoning",
    "mixed_split_needed": "no",
    "classification_confidence": "high",
    "scope_confidence": "medium",
    "magnitude_confidence": "medium",
    "evidence_confidence": "high",
    "review_recommendation": "accept_first_pass",
    "human_review_required": "no"
  },
  "scope_review": {
    "source_stated_blocks": "",
    "source_stated_lots": "",
    "source_stated_acres": "",
    "bbl_scope_appears_complete": "unclear",
    "map_or_polygon_needed": "yes"
  },
  "magnitude_review": {
    "far_change_explicit": "no",
    "contextual_or_form_restriction": "yes",
    "nonresidential_capacity_relevant": "no",
    "notes": ""
  },
  "adjudication": {
    "recommended_database_action": "needs_human_review",
    "review_note": ""
  }
}
```

Allowed `second_pass_direction`, `dominant_capacity_effect`, and `component_direction` values: upzoning, downzoning, mixed, no_material_residential_change, unknown.
Allowed `housing_intent`, `up_component_present`, `down_component_present`, `mixed_split_needed`, `human_review_required`, `outside_knowledge_used`, `bbl_scope_appears_complete`, `map_or_polygon_needed`, `far_change_explicit`, `contextual_or_form_restriction`, and `nonresidential_capacity_relevant` values: yes, no, unclear.
Allowed `scope_type` values: single_site, small_area, corridor, neighborhood, large_neighborhood, very_large_neighborhood, citywide_or_text_only, unknown.
Allowed confidence values: high, medium, low, unknown.
Allowed `review_recommendation` values: accept_first_pass, revise_direction, keep_mixed_needs_split, needs_human_review, insufficient_evidence.

Rules:
- Treat commercial overlays alone as no_material_residential_change unless the source shows a residential or mixed-use capacity change.
- Treat contextual envelope restrictions, detached-only mappings, height limits, lot-size limits, or other form controls as downzoning or mixed when they restrict feasible residential capacity even without a lower numeric FAR.
- Treat true neighborhood rezonings with both capacity-increasing corridors and preservation side streets as mixed unless one side is clearly immaterial.
- If direction is mixed, identify both up and down components in `zoning_components` and set `mixed_split_needed` to yes unless the split is already clear from the supplied data.
- If source evidence is too thin, use `second_pass_direction = unknown`, `review_recommendation = insufficient_evidence`, and `human_review_required = yes`.
- Cite page numbers when the packet supplies them. Keep evidence excerpts short.
"""

FRAME_COLUMNS = [
    "project_id",
    "completed_year",
    "decade",
    "project_name",
    "borough_name_standardized",
    "community_district",
    "reviewed_rezoning_direction",
    "reviewed_direction_source",
    "review_source_status",
    "review_source_title",
    "review_source_url",
    "reviewed_policy_scope_source",
    "reviewed_scope_bin",
    "review_source_scope_description",
    "review_priority",
    "review_priority_score",
    "primary_review_need",
    "review_reason_all",
    "size_bin",
    "source_bundle_status",
    "source_document_count",
    "source_snippet_count",
    "selected_for_llm_review",
    "llm_selection_reason",
    "llm_queue_rank",
    "llm_batch_id",
    "llm_batch_path",
    "linked_bbl_count",
    "strict_assigned_bbl_count",
    "strict_assigned_district_count",
    "strict_bbl_match_share",
    "affected_lot_acres",
    "block_expanded_assigned_bbl_count",
    "block_expanded_assigned_district_count",
    "block_expanded_affected_lot_acres",
    "strict_or_expanded_acres",
    "reviewed_policy_scope_blocks",
    "reviewed_policy_scope_acres",
    "review_source_scope_blocks",
    "review_source_scope_acres",
    "project_net_far_delta",
    "project_gross_up_far_delta",
    "project_gross_down_far_delta",
    "gross_up_far_acres",
    "gross_down_far_acres",
    "net_far_acres",
    "magnitude_bin",
    "parsed_pair_count",
    "known_pair_count",
    "parsed_zoning_changes",
    "unrecognized_zoning_codes",
    "missing_direction_reason",
    "text_candidate_direction",
    "text_candidate_confidence",
    "text_candidate_basis",
    "text_zoning_codes",
]

RESPONSE_COLUMNS = [
    "project_id",
    "response_line_number",
    "duplicate_response_flag",
    "validation_status",
    "validation_errors",
    "batch_id",
    "prompt_version",
    "review_stage",
    "model",
    "outside_knowledge_used",
    "second_pass_direction",
    "housing_intent",
    "scope_type",
    "up_component_present",
    "down_component_present",
    "dominant_capacity_effect",
    "mixed_split_needed",
    "classification_confidence",
    "scope_confidence",
    "magnitude_confidence",
    "evidence_confidence",
    "review_recommendation",
    "human_review_required",
    "source_stated_blocks",
    "source_stated_lots",
    "source_stated_acres",
    "bbl_scope_appears_complete",
    "map_or_polygon_needed",
    "far_change_explicit",
    "contextual_or_form_restriction",
    "nonresidential_capacity_relevant",
    "component_count",
    "evidence_span_count",
    "official_document_count",
    "recommended_database_action",
    "review_note",
]

ERROR_COLUMNS = [
    "project_id",
    "response_line_number",
    "error_type",
    "error_detail",
]

MISSING_COLUMNS = [
    "project_id",
    "llm_batch_id",
    "llm_queue_rank",
    "completed_year",
    "project_name",
    "borough_name_standardized",
    "reviewed_rezoning_direction",
    "review_priority",
    "primary_review_need",
    "source_bundle_status",
    "llm_batch_path",
]


def clean_cell(value):
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    text = str(value)
    if text.lower() == "nan":
        return ""
    return text


def number_value(value):
    text = clean_cell(value)
    if text == "":
        return float("nan")
    try:
        return float(text)
    except ValueError:
        return float("nan")


def yes_value(value):
    text = clean_cell(value).strip().lower()
    return text in {"true", "1", "yes", "y"}


def stable_hash(text):
    return int(hashlib.sha256(text.encode("utf-8")).hexdigest()[:12], 16)


def first_present(row, columns):
    for column in columns:
        if column in row and clean_cell(row[column]) != "":
            return clean_cell(row[column])
    return ""


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


def compact_text(text, limit):
    text = " ".join(clean_cell(text).split())
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + " ..."


def format_project_record(row, documents_by_project, snippets_by_project, prompt_version):
    project_id = row["project_id"]
    documents = documents_by_project.get(project_id, [])
    snippets = snippets_by_project.get(project_id, [])

    document_lines = []
    if clean_cell(row.get("review_source_url")) != "":
        document_lines.append(
            "; ".join(
                part
                for part in [
                    "source=direction_scope_ledger",
                    f"status={clean_cell(row.get('review_source_status'))}",
                    f"title={clean_cell(row.get('review_source_title'))}",
                    f"url={clean_cell(row.get('review_source_url'))}",
                    f"scope_description={clean_cell(row.get('review_source_scope_description'))}",
                ]
                if part
            )
        )
    for document in documents:
        document_lines.append(
            "; ".join(
                part
                for part in [
                    f"rank={clean_cell(document.get('document_rank'))}",
                    f"title={clean_cell(document.get('document_title'))}",
                    f"url={clean_cell(document.get('document_url'))}",
                    f"text_status={clean_cell(document.get('text_status'))}",
                    f"text_char_count={clean_cell(document.get('text_char_count'))}",
                ]
                if part
            )
        )
    if not document_lines:
        document_lines.append("No ledger source or extracted official source document bundle is currently available for this project.")

    snippet_lines = []
    for snippet in snippets:
        snippet_lines.append(
            "\n".join(
                [
                    f"- document_rank: {clean_cell(snippet.get('document_rank'))}; page: {clean_cell(snippet.get('page_number'))}; title: {clean_cell(snippet.get('document_title'))}",
                    f"  snippet: {compact_text(snippet.get('snippet_text'), 2200)}",
                ]
            )
        )
    if not snippet_lines:
        snippet_lines.append("No official-source snippets are currently available. Use insufficient_evidence unless the ZAP/parser facts alone are decisive.")

    return "\n".join(
        [
            "-----",
            f"project_id: {project_id}",
            f"prompt_version: {prompt_version}",
            f"llm_queue_rank: {row['llm_queue_rank']}",
            f"llm_selection_reason: {row['llm_selection_reason']}",
            "",
            "project_metadata:",
            f"completed_year: {clean_cell(row['completed_year'])}",
            f"project_name: {clean_cell(row['project_name'])}",
            f"borough: {clean_cell(row['borough_name_standardized'])}",
            f"community_district: {clean_cell(row['community_district'])}",
            f"project_brief: {compact_text(row.get('project_brief'), 1200)}",
            "",
            "current_database_label:",
            f"reviewed_rezoning_direction: {clean_cell(row['reviewed_rezoning_direction'])}",
            f"reviewed_direction_source: {clean_cell(row['reviewed_direction_source'])}",
            f"review_source_status: {clean_cell(row.get('review_source_status'))}",
            f"review_source_title: {clean_cell(row.get('review_source_title'))}",
            f"review_source_url: {clean_cell(row.get('review_source_url'))}",
            f"review_priority: {clean_cell(row['review_priority'])}",
            f"primary_review_need: {clean_cell(row['primary_review_need'])}",
            f"review_reason_all: {clean_cell(row['review_reason_all'])}",
            "",
            "parser_facts:",
            f"text_zoning_codes: {clean_cell(row.get('text_zoning_codes'))}",
            f"parsed_pair_count: {clean_cell(row['parsed_pair_count'])}",
            f"known_pair_count: {clean_cell(row['known_pair_count'])}",
            f"parsed_zoning_changes: {clean_cell(row['parsed_zoning_changes'])}",
            f"unrecognized_zoning_codes: {clean_cell(row['unrecognized_zoning_codes'])}",
            f"missing_direction_reason: {clean_cell(row['missing_direction_reason'])}",
            f"text_candidate_direction: {clean_cell(row['text_candidate_direction'])}",
            f"text_candidate_confidence: {clean_cell(row['text_candidate_confidence'])}",
            f"text_candidate_basis: {clean_cell(row['text_candidate_basis'])}",
            "",
            "scope_and_magnitude_facts:",
            f"size_bin: {clean_cell(row['size_bin'])}",
            f"linked_bbl_count: {clean_cell(row['linked_bbl_count'])}",
            f"strict_assigned_bbl_count: {clean_cell(row['strict_assigned_bbl_count'])}",
            f"strict_bbl_match_share: {clean_cell(row['strict_bbl_match_share'])}",
            f"affected_lot_acres_strict_bbl: {clean_cell(row['affected_lot_acres'])}",
            f"block_expanded_assigned_bbl_count: {clean_cell(row['block_expanded_assigned_bbl_count'])}",
            f"block_expanded_affected_lot_acres: {clean_cell(row['block_expanded_affected_lot_acres'])}",
            f"strict_or_expanded_acres: {clean_cell(row['strict_or_expanded_acres'])}",
            f"reviewed_policy_scope_blocks: {clean_cell(row['reviewed_policy_scope_blocks'])}",
            f"reviewed_policy_scope_acres: {clean_cell(row['reviewed_policy_scope_acres'])}",
            f"reviewed_policy_scope_source: {clean_cell(row.get('reviewed_policy_scope_source'))}",
            f"reviewed_scope_bin: {clean_cell(row.get('reviewed_scope_bin'))}",
            f"review_source_scope_blocks: {clean_cell(row['review_source_scope_blocks'])}",
            f"review_source_scope_acres: {clean_cell(row['review_source_scope_acres'])}",
            f"review_source_scope_description: {clean_cell(row.get('review_source_scope_description'))}",
            f"project_net_far_delta: {clean_cell(row['project_net_far_delta'])}",
            f"project_gross_up_far_delta: {clean_cell(row['project_gross_up_far_delta'])}",
            f"project_gross_down_far_delta: {clean_cell(row['project_gross_down_far_delta'])}",
            f"gross_up_far_acres: {clean_cell(row['gross_up_far_acres'])}",
            f"gross_down_far_acres: {clean_cell(row['gross_down_far_acres'])}",
            f"net_far_acres: {clean_cell(row['net_far_acres'])}",
            f"magnitude_bin: {clean_cell(row['magnitude_bin'])}",
            "",
            "source_documents:",
            "\n".join(document_lines),
            "",
            "official_source_evidence:",
            "\n".join(snippet_lines),
            "",
        ]
    )


def nested_value(response, container_name, field_name):
    direct_value = response.get(field_name, "")
    if clean_cell(direct_value) != "":
        return clean_cell(direct_value)
    container = response.get(container_name, {})
    if isinstance(container, dict):
        return clean_cell(container.get(field_name, ""))
    return ""


if len(sys.argv) != 5:
    raise SystemExit(
        "Usage: python3 build_zap_rezoning_llm_review.py "
        "<batch_size> <max_review_projects> <random_audit_per_stratum> <prompt_version>"
    )

batch_size = int(sys.argv[1])
max_review_projects = int(sys.argv[2])
random_audit_per_stratum = int(sys.argv[3])
prompt_version = sys.argv[4]

ledger = pd.read_csv("../input/zap_rezoning_direction_review_ledger.csv")
classification = pd.read_csv("../input/zap_rezoning_direction_project_classification.csv")
snippets = pd.read_csv("../input/zap_rezoning_source_text_snippets.csv")
documents = pd.read_csv("../input/zap_rezoning_source_document_index.csv")

if ledger["project_id"].duplicated().any():
    duplicate_ids = sorted(ledger.loc[ledger["project_id"].duplicated(), "project_id"].unique())
    raise ValueError(f"Review ledger project_id is not unique: {duplicate_ids[:10]}")

if classification["project_id"].duplicated().any():
    duplicate_ids = sorted(classification.loc[classification["project_id"].duplicated(), "project_id"].unique())
    raise ValueError(f"Project classification project_id is not unique: {duplicate_ids[:10]}")

classification_keep = [
    "project_id",
    "text_zoning_codes",
    "text_commercial_overlay_codes",
    "text_residential_base_codes",
    "text_manufacturing_codes",
    "text_other_non_overlay_codes",
    "parser_stages",
]
for column in classification_keep:
    if column not in classification.columns:
        classification[column] = ""

review_frame = ledger.merge(
    classification[classification_keep],
    on="project_id",
    how="left",
    validate="one_to_one",
)

review_frame["decade"] = (pd.to_numeric(review_frame["completed_year"], errors="coerce") // 10 * 10).astype("Int64").astype(str) + "s"

def classify_size(row):
    strict_or_expanded_acres = number_value(row.get("strict_or_expanded_acres"))
    source_acres = number_value(row.get("reviewed_policy_scope_acres"))
    source_blocks = number_value(row.get("reviewed_policy_scope_blocks"))
    linked_bbl_count = number_value(row.get("linked_bbl_count"))

    max_acres = max(
        value
        for value in [
            strict_or_expanded_acres if not math.isnan(strict_or_expanded_acres) else 0,
            source_acres if not math.isnan(source_acres) else 0,
        ]
    )
    blocks = source_blocks if not math.isnan(source_blocks) else 0
    bbls = linked_bbl_count if not math.isnan(linked_bbl_count) else 0

    if max_acres >= 300 or blocks >= 150:
        return "very_large"
    if max_acres >= 75 or blocks >= 50:
        return "large"
    if max_acres >= 20 or blocks >= 15:
        return "medium_large"
    if max_acres > 0 or bbls > 0:
        return "small_or_site"
    return "no_scope"

review_frame["size_bin"] = [classify_size(row) for row in review_frame.to_dict("records")]

document_counts = documents.groupby("project_id").size().rename("source_document_count")
snippet_counts = snippets.groupby("project_id").size().rename("source_snippet_count")
review_frame = review_frame.merge(document_counts, on="project_id", how="left", validate="one_to_one")
review_frame = review_frame.merge(snippet_counts, on="project_id", how="left", validate="one_to_one")
review_frame["source_document_count"] = review_frame["source_document_count"].fillna(0).astype(int)
review_frame["source_snippet_count"] = review_frame["source_snippet_count"].fillna(0).astype(int)
review_frame["source_bundle_status"] = "missing_source_bundle"
review_frame.loc[
    (review_frame["source_document_count"] > 0) & (review_frame["source_snippet_count"] == 0),
    "source_bundle_status",
] = "documents_without_snippets"
review_frame.loc[
    (review_frame["source_document_count"] == 0)
    & (review_frame["source_snippet_count"] == 0)
    & (review_frame["review_source_url"].fillna("").astype(str) != ""),
    "source_bundle_status",
] = "ledger_review_source_only"
review_frame.loc[review_frame["source_snippet_count"] > 0, "source_bundle_status"] = "source_snippets_available"

priority_order = {
    "highest": 1,
    "high": 2,
    "medium": 3,
    "low": 4,
    "no_immediate_gap": 5,
}
review_frame["priority_order"] = review_frame["review_priority"].map(priority_order).fillna(6)
review_frame["review_needed_bool"] = [yes_value(value) for value in review_frame["review_needed_flag"]]
review_frame["priority_score_numeric"] = pd.to_numeric(review_frame["review_priority_score"], errors="coerce").fillna(0)
review_frame["review_sort_hash"] = [
    stable_hash(f"{project_id}|{prompt_version}|review")
    for project_id in review_frame["project_id"]
]

review_queue = review_frame.loc[review_frame["review_needed_bool"]].copy()
review_queue = review_queue.sort_values(
    ["priority_order", "priority_score_numeric", "completed_year", "review_sort_hash", "project_id"],
    ascending=[True, False, True, True, True],
)
if max_review_projects > 0:
    review_queue = review_queue.head(max_review_projects).copy()
review_project_ids = set(review_queue["project_id"])

audit_pool = review_frame.loc[~review_frame["project_id"].isin(review_project_ids)].copy()
audit_pool["audit_stratum"] = (
    audit_pool["reviewed_rezoning_direction"].fillna("missing").astype(str)
    + "|"
    + audit_pool["decade"].fillna("missing").astype(str)
    + "|"
    + audit_pool["borough_name_standardized"].fillna("missing").astype(str)
    + "|"
    + audit_pool["size_bin"].fillna("missing").astype(str)
)
audit_pool["audit_hash"] = [
    stable_hash(f"{project_id}|{prompt_version}|audit")
    for project_id in audit_pool["project_id"]
]
audit_rows = []
if random_audit_per_stratum > 0 and len(audit_pool) > 0:
    for _, group in audit_pool.sort_values(["audit_stratum", "audit_hash", "project_id"]).groupby("audit_stratum", sort=True):
        audit_rows.append(group.head(random_audit_per_stratum))
if audit_rows:
    audit_queue = pd.concat(audit_rows, ignore_index=True)
else:
    audit_queue = audit_pool.head(0).copy()

review_frame["selected_for_llm_review"] = False
review_frame["llm_selection_reason"] = "not_selected_no_immediate_gap"
review_frame.loc[review_frame["project_id"].isin(review_project_ids), "selected_for_llm_review"] = True
review_frame.loc[review_frame["project_id"].isin(review_project_ids), "llm_selection_reason"] = "review_needed_queue"
review_frame.loc[review_frame["project_id"].isin(set(audit_queue["project_id"])), "selected_for_llm_review"] = True
review_frame.loc[review_frame["project_id"].isin(set(audit_queue["project_id"])), "llm_selection_reason"] = "stratified_no_gap_audit"

selected_queue = review_frame.loc[review_frame["selected_for_llm_review"]].copy()
selected_queue["selection_order"] = selected_queue["llm_selection_reason"].map(
    {"review_needed_queue": 1, "stratified_no_gap_audit": 2}
).fillna(3)
selected_queue = selected_queue.sort_values(
    ["selection_order", "priority_order", "priority_score_numeric", "completed_year", "review_sort_hash", "project_id"],
    ascending=[True, True, False, True, True, True],
)
selected_queue["llm_queue_rank"] = range(1, len(selected_queue) + 1)

documents_by_project = {}
for project_id, group in documents.sort_values(["project_id", "document_rank", "source_priority"]).groupby("project_id"):
    documents_by_project[project_id] = group.head(8).to_dict("records")

snippets_by_project = {}
for project_id, group in snippets.sort_values(["project_id", "document_rank", "snippet_rank", "snippet_score"]).groupby("project_id"):
    snippets_by_project[project_id] = group.head(10).to_dict("records")

selected_records = selected_queue.to_dict("records")
batches = []
batch_project_rows = []
for start_index in range(0, len(selected_records), batch_size):
    batch_id = f"{len(batches) + 1:03d}"
    batch_records = selected_records[start_index : start_index + batch_size]
    batch_path = f"../output/batches/zap_rezoning_llm_review_batch_{batch_id}.md"
    batch_text = PROMPT_HEADER + "\n\n" + "\n".join(
        format_project_record(row, documents_by_project, snippets_by_project, prompt_version)
        for row in batch_records
    )
    write_text_if_changed(batch_text, batch_path)
    batch_project_ids = [row["project_id"] for row in batch_records]
    for row in batch_records:
        batch_project_rows.append(
            {
                "project_id": row["project_id"],
                "llm_batch_id": batch_id,
                "llm_batch_path": batch_path,
                "llm_queue_rank": row["llm_queue_rank"],
            }
        )
    batches.append(
        {
            "batch_id": batch_id,
            "batch_path": batch_path,
            "project_count": len(batch_records),
            "first_queue_rank": batch_records[0]["llm_queue_rank"],
            "last_queue_rank": batch_records[-1]["llm_queue_rank"],
            "char_count": len(batch_text),
            "project_ids": "|".join(batch_project_ids),
        }
    )

batch_lookup = pd.DataFrame(batch_project_rows)
review_frame["llm_queue_rank"] = ""
review_frame["llm_batch_id"] = ""
review_frame["llm_batch_path"] = ""
if len(batch_lookup) > 0:
    review_frame = review_frame.merge(batch_lookup, on="project_id", how="left", suffixes=("", "_from_batch"), validate="one_to_one")
    for column in ["llm_queue_rank", "llm_batch_id", "llm_batch_path"]:
        review_frame[column] = review_frame[f"{column}_from_batch"].fillna("")
        review_frame = review_frame.drop(columns=[f"{column}_from_batch"])

write_text_if_changed(PROMPT_HEADER, "../output/zap_rezoning_llm_review_prompt.md")

batches_jsonl = "".join(json.dumps(batch, ensure_ascii=True) + "\n" for batch in batches)
write_text_if_changed(batches_jsonl, "../output/zap_rezoning_llm_review_batches.jsonl")

seen_response_project_ids = set()
parsed_response_rows = []
error_rows = []

with open("llm_review_responses.jsonl", "r", encoding="utf-8") as response_file:
    for line_number, raw_line in enumerate(response_file, start=1):
        line = raw_line.strip()
        if line == "" or line.startswith("```"):
            continue
        try:
            parsed_line = json.loads(line)
        except json.JSONDecodeError as error:
            error_rows.append(
                {
                    "project_id": "",
                    "response_line_number": line_number,
                    "error_type": "json_parse_error",
                    "error_detail": str(error),
                }
            )
            continue

        response_objects = parsed_line if isinstance(parsed_line, list) else [parsed_line]
        for response in response_objects:
            if not isinstance(response, dict):
                error_rows.append(
                    {
                        "project_id": "",
                        "response_line_number": line_number,
                        "error_type": "json_not_object",
                        "error_detail": type(response).__name__,
                    }
                )
                continue

            project_id = clean_cell(response.get("project_id", ""))
            project_classification = response.get("project_classification", {})
            scope_review = response.get("scope_review", {})
            magnitude_review = response.get("magnitude_review", {})
            adjudication = response.get("adjudication", {})
            evidence_spans = response.get("evidence_spans", [])
            official_documents_used = response.get("official_documents_used", [])
            zoning_components = response.get("zoning_components", [])

            if not isinstance(project_classification, dict):
                project_classification = {}
            if not isinstance(scope_review, dict):
                scope_review = {}
            if not isinstance(magnitude_review, dict):
                magnitude_review = {}
            if not isinstance(adjudication, dict):
                adjudication = {}
            if not isinstance(evidence_spans, list):
                evidence_spans = []
            if not isinstance(official_documents_used, list):
                official_documents_used = []
            if not isinstance(zoning_components, list):
                zoning_components = []

            duplicate_flag = project_id in seen_response_project_ids
            seen_response_project_ids.add(project_id)

            response_row = {
                "project_id": project_id,
                "response_line_number": line_number,
                "duplicate_response_flag": duplicate_flag,
                "prompt_version": clean_cell(response.get("prompt_version", "")),
                "review_stage": clean_cell(response.get("review_stage", "")),
                "model": clean_cell(response.get("model", "")),
                "outside_knowledge_used": clean_cell(response.get("outside_knowledge_used", "")),
                "second_pass_direction": nested_value(response, "project_classification", "second_pass_direction"),
                "housing_intent": nested_value(response, "project_classification", "housing_intent"),
                "scope_type": nested_value(response, "project_classification", "scope_type"),
                "up_component_present": nested_value(response, "project_classification", "up_component_present"),
                "down_component_present": nested_value(response, "project_classification", "down_component_present"),
                "dominant_capacity_effect": nested_value(response, "project_classification", "dominant_capacity_effect"),
                "mixed_split_needed": nested_value(response, "project_classification", "mixed_split_needed"),
                "classification_confidence": nested_value(response, "project_classification", "classification_confidence"),
                "scope_confidence": nested_value(response, "project_classification", "scope_confidence"),
                "magnitude_confidence": nested_value(response, "project_classification", "magnitude_confidence"),
                "evidence_confidence": nested_value(response, "project_classification", "evidence_confidence"),
                "review_recommendation": nested_value(response, "project_classification", "review_recommendation"),
                "human_review_required": nested_value(response, "project_classification", "human_review_required"),
                "source_stated_blocks": clean_cell(scope_review.get("source_stated_blocks", "")),
                "source_stated_lots": clean_cell(scope_review.get("source_stated_lots", "")),
                "source_stated_acres": clean_cell(scope_review.get("source_stated_acres", "")),
                "bbl_scope_appears_complete": clean_cell(scope_review.get("bbl_scope_appears_complete", "")),
                "map_or_polygon_needed": clean_cell(scope_review.get("map_or_polygon_needed", "")),
                "far_change_explicit": clean_cell(magnitude_review.get("far_change_explicit", "")),
                "contextual_or_form_restriction": clean_cell(magnitude_review.get("contextual_or_form_restriction", "")),
                "nonresidential_capacity_relevant": clean_cell(magnitude_review.get("nonresidential_capacity_relevant", "")),
                "component_count": len(zoning_components),
                "evidence_span_count": len(evidence_spans),
                "official_document_count": len(official_documents_used),
                "recommended_database_action": clean_cell(adjudication.get("recommended_database_action", "")),
                "review_note": clean_cell(adjudication.get("review_note", "")),
            }

            validation_errors = []
            if project_id == "":
                validation_errors.append("missing_project_id")
            if project_id not in set(review_frame["project_id"]):
                validation_errors.append("project_id_not_in_review_frame")
            if project_id in set(review_frame.loc[~review_frame["selected_for_llm_review"], "project_id"]):
                validation_errors.append("project_id_not_selected_for_llm_review")
            if duplicate_flag:
                validation_errors.append("duplicate_project_response")
            if response_row["prompt_version"] != prompt_version:
                validation_errors.append("prompt_version_mismatch")
            if response_row["review_stage"] != "llm_source_review":
                validation_errors.append("invalid_review_stage")
            if response_row["outside_knowledge_used"] not in ALLOWED_YES_NO_UNCLEAR:
                validation_errors.append("invalid_outside_knowledge_used")
            if response_row["second_pass_direction"] not in ALLOWED_DIRECTIONS:
                validation_errors.append("invalid_second_pass_direction")
            if response_row["housing_intent"] not in ALLOWED_HOUSING_INTENT:
                validation_errors.append("invalid_housing_intent")
            if response_row["scope_type"] not in ALLOWED_SCOPE_TYPES:
                validation_errors.append("invalid_scope_type")
            if response_row["up_component_present"] not in ALLOWED_YES_NO_UNCLEAR:
                validation_errors.append("invalid_up_component_present")
            if response_row["down_component_present"] not in ALLOWED_YES_NO_UNCLEAR:
                validation_errors.append("invalid_down_component_present")
            if response_row["dominant_capacity_effect"] not in ALLOWED_DIRECTIONS:
                validation_errors.append("invalid_dominant_capacity_effect")
            if response_row["mixed_split_needed"] not in ALLOWED_YES_NO_UNCLEAR:
                validation_errors.append("invalid_mixed_split_needed")
            for confidence_column in ["classification_confidence", "scope_confidence", "magnitude_confidence", "evidence_confidence"]:
                if response_row[confidence_column] not in ALLOWED_CONFIDENCE:
                    validation_errors.append(f"invalid_{confidence_column}")
            if response_row["review_recommendation"] not in ALLOWED_RECOMMENDATIONS:
                validation_errors.append("invalid_review_recommendation")
            if response_row["human_review_required"] not in ALLOWED_YES_NO_UNCLEAR:
                validation_errors.append("invalid_human_review_required")
            for yes_no_column in [
                "bbl_scope_appears_complete",
                "map_or_polygon_needed",
                "far_change_explicit",
                "contextual_or_form_restriction",
                "nonresidential_capacity_relevant",
            ]:
                if response_row[yes_no_column] not in ALLOWED_YES_NO_UNCLEAR and response_row[yes_no_column] != "":
                    validation_errors.append(f"invalid_{yes_no_column}")
            if response_row["second_pass_direction"] == "mixed":
                if response_row["up_component_present"] != "yes" or response_row["down_component_present"] != "yes":
                    validation_errors.append("mixed_without_up_and_down_components")
            if response_row["second_pass_direction"] == "unknown" and response_row["human_review_required"] != "yes":
                validation_errors.append("unknown_without_human_review_required")
            if response_row["review_recommendation"] == "insufficient_evidence" and response_row["human_review_required"] != "yes":
                validation_errors.append("insufficient_evidence_without_human_review_required")
            if project_id in set(review_frame.loc[review_frame["source_bundle_status"] == "source_snippets_available", "project_id"]):
                if response_row["evidence_span_count"] == 0 and response_row["review_recommendation"] != "insufficient_evidence":
                    validation_errors.append("source_available_but_no_evidence_spans")

            response_row["validation_status"] = "invalid" if validation_errors else "valid"
            response_row["validation_errors"] = "|".join(validation_errors)
            parsed_response_rows.append(response_row)

            for validation_error in validation_errors:
                error_rows.append(
                    {
                        "project_id": project_id,
                        "response_line_number": line_number,
                        "error_type": "response_validation_error",
                        "error_detail": validation_error,
                    }
                )

for row in parsed_response_rows:
    row["batch_id"] = clean_cell(review_frame.loc[review_frame["project_id"] == row["project_id"], "llm_batch_id"].iloc[0]) if row["project_id"] in set(review_frame["project_id"]) else ""

response_project_ids = {row["project_id"] for row in parsed_response_rows if row["project_id"] != ""}

selected_for_missing = review_frame.loc[review_frame["selected_for_llm_review"]].copy()
missing_response_rows = []
for row in selected_for_missing.sort_values(["llm_queue_rank", "project_id"]).to_dict("records"):
    if row["project_id"] not in response_project_ids:
        missing_response_rows.append(
            {
                "project_id": row["project_id"],
                "llm_batch_id": clean_cell(row["llm_batch_id"]),
                "llm_queue_rank": clean_cell(row["llm_queue_rank"]),
                "completed_year": clean_cell(row["completed_year"]),
                "project_name": clean_cell(row["project_name"]),
                "borough_name_standardized": clean_cell(row["borough_name_standardized"]),
                "reviewed_rezoning_direction": clean_cell(row["reviewed_rezoning_direction"]),
                "review_priority": clean_cell(row["review_priority"]),
                "primary_review_need": clean_cell(row["primary_review_need"]),
                "source_bundle_status": clean_cell(row["source_bundle_status"]),
                "llm_batch_path": clean_cell(row["llm_batch_path"]),
            }
        )

missing_batches = []
for batch in batches:
    batch_project_ids = batch["project_ids"].split("|") if batch["project_ids"] else []
    missing_count = sum(1 for project_id in batch_project_ids if project_id not in response_project_ids)
    reviewed_count = len(batch_project_ids) - missing_count
    batch["response_status"] = "complete" if missing_count == 0 else "missing_responses"
    batch["reviewed_project_count"] = reviewed_count
    batch["missing_project_count"] = missing_count
    if missing_count > 0:
        missing_batches.append(batch)

if missing_batches:
    with open(missing_batches[0]["batch_path"], "r", encoding="utf-8") as batch_file:
        next_batch_text = batch_file.read()
else:
    next_batch_text = "# NYC ZAP Rezoning LLM Review\n\nAll selected LLM review batches have a response in code/llm_review_responses.jsonl.\n"
write_text_if_changed(next_batch_text, "../output/zap_rezoning_llm_review_next_batch.md")

frame_rows = []
for row in review_frame.sort_values(["selected_for_llm_review", "llm_queue_rank", "project_id"], ascending=[False, True, True]).to_dict("records"):
    frame_rows.append({column: clean_cell(row.get(column, "")) for column in FRAME_COLUMNS})

batch_manifest_rows = []
for batch in batches:
    batch_manifest_rows.append(
        {
            "batch_id": batch["batch_id"],
            "batch_path": batch["batch_path"],
            "project_count": batch["project_count"],
            "first_queue_rank": batch["first_queue_rank"],
            "last_queue_rank": batch["last_queue_rank"],
            "char_count": batch["char_count"],
            "response_status": batch.get("response_status", "missing_responses"),
            "reviewed_project_count": batch.get("reviewed_project_count", 0),
            "missing_project_count": batch.get("missing_project_count", batch["project_count"]),
            "project_ids": batch["project_ids"],
        }
    )

write_csv_if_changed(frame_rows, FRAME_COLUMNS, "../output/zap_rezoning_llm_review_frame.csv")
write_csv_if_changed(
    batch_manifest_rows,
    [
        "batch_id",
        "batch_path",
        "project_count",
        "first_queue_rank",
        "last_queue_rank",
        "char_count",
        "response_status",
        "reviewed_project_count",
        "missing_project_count",
        "project_ids",
    ],
    "../output/zap_rezoning_llm_review_batch_manifest.csv",
)
write_csv_if_changed(parsed_response_rows, RESPONSE_COLUMNS, "../output/zap_rezoning_llm_review_response_compiled.csv")
write_csv_if_changed(error_rows, ERROR_COLUMNS, "../output/zap_rezoning_llm_review_response_errors.csv")
write_csv_if_changed(missing_response_rows, MISSING_COLUMNS, "../output/zap_rezoning_llm_review_missing_responses.csv")

qc_rows = [
    {
        "check": "review_ledger_project_count",
        "value": len(review_frame),
        "status": "pass" if len(review_frame) == 1396 else "review",
        "detail": "All ZAP ZM project rows from the direction-scope ledger.",
    },
    {
        "check": "selected_project_count",
        "value": int(review_frame["selected_for_llm_review"].sum()),
        "status": "pass",
        "detail": "Review-needed projects plus a stratified audit sample of no-immediate-gap projects.",
    },
    {
        "check": "review_needed_selected_count",
        "value": int((review_frame["llm_selection_reason"] == "review_needed_queue").sum()),
        "status": "pass",
        "detail": "Projects selected because the upstream ledger marks a review need.",
    },
    {
        "check": "stratified_audit_selected_count",
        "value": int((review_frame["llm_selection_reason"] == "stratified_no_gap_audit").sum()),
        "status": "pass",
        "detail": "Projects selected to avoid auditing only visibly problematic rows.",
    },
    {
        "check": "selected_with_source_snippets_count",
        "value": int(((review_frame["selected_for_llm_review"]) & (review_frame["source_bundle_status"] == "source_snippets_available")).sum()),
        "status": "pass",
        "detail": "Selected projects that already have official-source excerpts in the source-text audit task.",
    },
    {
        "check": "selected_with_documents_without_snippets_count",
        "value": int(((review_frame["selected_for_llm_review"]) & (review_frame["source_bundle_status"] == "documents_without_snippets")).sum()),
        "status": "pass",
        "detail": "Selected projects with discovered source documents but no usable extracted snippets.",
    },
    {
        "check": "selected_with_ledger_source_only_count",
        "value": int(((review_frame["selected_for_llm_review"]) & (review_frame["source_bundle_status"] == "ledger_review_source_only")).sum()),
        "status": "pass",
        "detail": "Selected projects with source URLs from the direction-scope ledger but no extracted snippet bundle.",
    },
    {
        "check": "selected_missing_source_bundle_count",
        "value": int(((review_frame["selected_for_llm_review"]) & (review_frame["source_bundle_status"] == "missing_source_bundle")).sum()),
        "status": "review",
        "detail": "Selected projects that still need source discovery, document extraction, or manual lookup.",
    },
    {
        "check": "batch_count",
        "value": len(batches),
        "status": "pass",
        "detail": f"Batch size is {batch_size}.",
    },
    {
        "check": "response_row_count",
        "value": len(parsed_response_rows),
        "status": "pass",
        "detail": "Parsed rows from code/llm_review_responses.jsonl.",
    },
    {
        "check": "invalid_response_error_count",
        "value": len(error_rows),
        "status": "pass" if len(error_rows) == 0 else "review",
        "detail": "Parse and validation errors in pasted LLM output.",
    },
    {
        "check": "missing_selected_response_count",
        "value": len(missing_response_rows),
        "status": "needs_more_labels" if len(missing_response_rows) > 0 else "pass",
        "detail": "Selected projects without a pasted LLM response yet.",
    },
]
write_csv_if_changed(qc_rows, ["check", "value", "status", "detail"], "../output/zap_rezoning_llm_review_qc.csv")

print(
    f"Wrote {len(batches)} LLM review batches for "
    f"{int(review_frame['selected_for_llm_review'].sum())} selected projects; "
    f"{len(missing_response_rows)} selected projects still need pasted responses."
)
