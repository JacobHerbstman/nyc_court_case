#!/usr/bin/env python3

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
import sys
import textwrap
from pathlib import Path

import pandas as pd


# batch_size <- 5
# max_review_projects <- 0
# random_audit_per_stratum <- 0
# prompt_version <- "v2"
# source_packet_mode <- "zap_cpc_reports_only"


ALLOWED_YES_NO_UNCLEAR = {"yes", "no", "unclear"}
ALLOWED_CONFIDENCE = {"high", "medium", "low", "unknown"}
ALLOWED_FINAL_DECISIONS = {
    "modification_confirmed",
    "modification_signal_only",
    "no_council_stage_modification",
    "source_lookup_needed",
    "insufficient_evidence",
    "unclear",
}
ALLOWED_ACTIONS = {
    "accept_ai_label",
    "needs_human_review",
    "source_lookup_needed",
    "insufficient_evidence",
    "revise_existing_label",
}
ALLOWED_STAGES = {"certified", "cpc_approved", "council_stage", "built", "unknown"}
ALLOWED_DIRECTIONS = {"cut", "expansion", "no_quantity_change", "commitment_only", "unclear"}
ALLOWED_CATEGORIES = {
    "Q-UN_units",
    "A-AF_affordability",
    "P-PK_parking",
    "D-BK_design_or_bulk",
    "C-MT_cost_or_infrastructure_mitigation",
    "B-LB_local_benefit_commitment",
    "T-TX_citywide_text_or_district_rule",
    "O-MD_unspecified_modification_signal",
    "O-UN_uncategorized",
}

PROMPT_HEADER = """# ULURP Modification Content AI Review

You are helping code NYC ULURP project modifications for an academic research dataset.

Your task is to produce coded data rows, not a narrative summary. For each project, determine both:

1. the type of Council-stage modification, using the allowed category codes; and
2. the measurable change, where the evidence states quantities, including before, after, and delta values.

Use only the project metadata, existing first-pass rows, and official-source excerpts supplied below. Do not use outside memory. Do not open or infer from source links whose text is not excerpted in the packet. If the packet only says "approved with modifications" but does not describe what changed, classify the project as `source_lookup_needed` or `modification_signal_only`, not as a substantive quantity cut.

Return one compact JSON object per project, one object per line, with exactly this top-level schema:

```json
{
  "project_id": "",
  "prompt_version": "",
  "review_stage": "ulurp_modification_ai_review",
  "model": "",
  "outside_knowledge_used": "no",
  "documents_used": [
    {
      "source_doc": "",
      "page": "",
      "why_used": ""
    }
  ],
  "evidence_spans": [
    {
      "source_doc": "",
      "page": "",
      "quoted_or_paraphrased_evidence": "",
      "why_it_matters": ""
    }
  ],
  "project_classification": {
    "council_stage_modification_present": "yes",
    "final_decision": "modification_confirmed",
    "human_review_required": "yes",
    "classification_confidence": "medium",
    "source_gap_resolved": "no"
  },
  "quantity_versions": [
    {
      "stage": "council_stage",
      "quantity_field": "units",
      "quantity_value": "",
      "quantity_missing_status": "NA_not_stated",
      "source_doc": "",
      "page": "",
      "evidence_snippet": "",
      "confidence": "low"
    }
  ],
  "modifications": [
    {
      "modification_stage": "council_stage",
      "modification_category_code": "Q-UN_units",
      "direction": "cut",
      "quantity_field": "units",
      "before_value": "",
      "after_value": "",
      "delta_value": "",
      "description": "",
      "local_member_attribution": "not_attributed",
      "source_doc": "",
      "page": "",
      "evidence_snippet": "",
      "confidence": "low"
    }
  ],
  "commitments": [
    {
      "commitment_category": "B-LB_local_benefit_commitment",
      "description": "",
      "source_doc": "",
      "page": "",
      "evidence_snippet": "",
      "confidence": "low"
    }
  ],
  "adjudication": {
    "recommended_action": "needs_human_review",
    "review_note": ""
  }
}
```

Allowed `council_stage_modification_present`, `human_review_required`, `source_gap_resolved`, and `outside_knowledge_used` values: yes, no, unclear.
Allowed `final_decision` values: modification_confirmed, modification_signal_only, no_council_stage_modification, source_lookup_needed, insufficient_evidence, unclear.
Allowed confidence values: high, medium, low, unknown.
Allowed `modification_stage` and quantity `stage` values: certified, cpc_approved, council_stage, built, unknown.
Allowed `modification_category_code` values: Q-UN_units, A-AF_affordability, P-PK_parking, D-BK_design_or_bulk, C-MT_cost_or_infrastructure_mitigation, B-LB_local_benefit_commitment, T-TX_citywide_text_or_district_rule, O-MD_unspecified_modification_signal, O-UN_uncategorized.
Allowed `direction` values: cut, expansion, no_quantity_change, commitment_only, unclear.
Allowed `recommended_action` values: accept_ai_label, needs_human_review, source_lookup_needed, insufficient_evidence, revise_existing_label.

Rules:
- Keep CPC-stage and Council-stage modifications separate. Code a Council-stage modification only when the source says Council modified, approved with modifications, proposed for modification, or gives changed Council approval terms.
- Do not attribute a modification to the local member unless the supplied evidence explicitly says the member negotiated, requested, opposed, or secured it.
- Separate bundled packages into separate modification rows: units, affordability, parking, design/bulk, infrastructure/cost mitigation, side commitments, and text changes.
- For each project, actively look for quantity evidence before deciding. Check current_first_pass_summary, first_pass_quantity_rows, first_pass_modification_rows, official_source_excerpts, and then source_document_links.
- If a source states certified, CPC-approved, Council-stage/adopted, or built quantities, include a `quantity_versions` row for each stated stage and field.
- For every quantity modification, fill `before_value`, `after_value`, and `delta_value` when the evidence gives enough information. Use negative deltas for cuts and positive deltas for expansions.
- If the evidence identifies a quantity-related modification but does not state the numeric before/after/delta, still code the modification type, set missing numeric fields blank, set confidence low, and set recommended_action to `needs_human_review` or `source_lookup_needed`.
- Do not treat generic phrases like "modifying Appendix F" or "special permit to modify bulk" as a unit cut unless the packet states the affected quantity.
- Do not list a linked document as used unless its text appears in the packet. If a likely CPC report, M report, committee report, or resolution link needs lookup for exact quantities, mention that in `adjudication.review_note`.
- Every non-gap modification row must have source_doc, page, evidence_snippet, and confidence. If evidence_snippet is missing or only procedural, use low confidence and human_review_required=yes.
- If no source states the modified content, set final_decision to source_lookup_needed or modification_signal_only.
- If quantity values are stated, extract them exactly and identify the stage. Do not infer adopted units from certified/CPC units.
- Use `accept_ai_label` only when the type and any claimed numeric change are both directly supported by supplied evidence. Otherwise use `needs_human_review` or `source_lookup_needed`.
"""

FRAME_COLUMNS = [
    "project_id",
    "project_name",
    "cert_year",
    "cert_era",
    "borough_name",
    "stratum",
    "council_outcome",
    "council_modification_signal",
    "source_gap_modification_any",
    "council_stage_modification_any",
    "modification_categories",
    "certified_units_first_pass",
    "cpc_units_first_pass",
    "adopted_units_first_pass",
    "built_units_0_10",
    "local_member_names",
    "local_member_vote_statuses",
    "member_deference_vote_signals",
    "source_text_status",
    "manual_review_reason_count",
    "zap_report_snippet_count",
    "zap_report_text_count",
    "council_snippet_count",
    "council_text_count",
    "zap_document_link_count",
    "council_document_link_count",
    "selected_for_ai_review",
    "ai_selection_reason",
    "ai_queue_rank",
    "ai_batch_id",
    "ai_batch_path",
]

SEED_LABEL_COLUMNS = [
    "project_id",
    "project_name",
    "seed_label_source",
    "seed_final_decision",
    "seed_council_stage_modification_present",
    "seed_human_review_required",
    "seed_confidence",
    "seed_category_codes",
    "seed_non_gap_modification_count",
    "seed_source_gap_flag",
    "seed_note",
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
    "council_stage_modification_present",
    "final_decision",
    "human_review_required",
    "classification_confidence",
    "source_gap_resolved",
    "modification_count",
    "commitment_count",
    "quantity_version_count",
    "evidence_span_count",
    "document_count",
    "category_codes",
    "direction_codes",
    "adopted_units_ai",
    "cpc_units_ai",
    "certified_units_ai",
    "council_vs_cpc_units_ai",
    "recommended_action",
    "review_note",
]

MODIFICATION_COLUMNS = [
    "project_id",
    "response_line_number",
    "modification_index",
    "modification_stage",
    "modification_category_code",
    "direction",
    "quantity_field",
    "before_value",
    "after_value",
    "delta_value",
    "description",
    "local_member_attribution",
    "source_doc",
    "page",
    "evidence_snippet",
    "confidence",
]

QUANTITY_COLUMNS = [
    "project_id",
    "response_line_number",
    "quantity_index",
    "stage",
    "quantity_field",
    "quantity_value",
    "quantity_missing_status",
    "source_doc",
    "page",
    "evidence_snippet",
    "confidence",
]

ERROR_COLUMNS = [
    "project_id",
    "response_line_number",
    "error_type",
    "error_detail",
]

MISSING_COLUMNS = [
    "project_id",
    "ai_batch_id",
    "ai_queue_rank",
    "cert_year",
    "project_name",
    "borough_name",
    "stratum",
    "source_text_status",
    "source_gap_modification_any",
    "ai_selection_reason",
    "ai_batch_path",
]


def clean_cell(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    text = str(value)
    if text.lower() == "nan":
        return ""
    return text.strip()


def yes_value(value: object) -> bool:
    return clean_cell(value).lower() in {"true", "t", "1", "yes", "y"}


def number_value(value: object) -> float:
    text = clean_cell(value).replace(",", "")
    if text == "":
        return float("nan")
    try:
        return float(text)
    except ValueError:
        return float("nan")


def stable_hash(text: str) -> int:
    return int(hashlib.sha256(text.encode("utf-8")).hexdigest()[:12], 16)


def compact_text(text: object, limit: int) -> str:
    value = " ".join(clean_cell(text).split())
    if len(value) <= limit:
        return value
    return value[:limit].rstrip() + " ..."


def collapse_values(values: object) -> str:
    out = []
    for value in values:
        text = clean_cell(value)
        if text != "" and text not in out:
            out.append(text)
    return "; ".join(out)


def write_text_if_changed(text: str, path: str) -> None:
    output_path = Path(path)
    try:
        old_text = output_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        old_text = None

    if old_text != text:
        output_path.write_text(text, encoding="utf-8")
    elif output_path.exists():
        output_path.touch()


def write_csv_if_changed(rows: list[dict[str, object]], fieldnames: list[str], path: str) -> None:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    new_text = output.getvalue()

    output_path = Path(path)
    try:
        old_text = output_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        old_text = None

    if old_text != new_text:
        output_path.write_text(new_text, encoding="utf-8")
    elif output_path.exists():
        output_path.touch()


def read_csv(path: str) -> pd.DataFrame:
    if not Path(path).exists():
        raise RuntimeError(f"Required input is missing: {path}")
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def assert_unique(df: pd.DataFrame, key_cols: list[str], df_name: str) -> None:
    duplicates = df[df.duplicated(key_cols, keep=False)]
    if not duplicates.empty:
        sample = duplicates[key_cols].head(5).to_dict("records")
        raise RuntimeError(f"{df_name} is not unique by {', '.join(key_cols)}: {sample}")


def explode_project_ids(df: pd.DataFrame, project_col: str = "project_ids") -> pd.DataFrame:
    if project_col not in df.columns:
        if "project_id" in df.columns:
            out = df.copy()
            out["project_id"] = out["project_id"].map(clean_cell)
            return out[out["project_id"] != ""]
        return df.head(0).copy()

    rows = []
    for row in df.to_dict("records"):
        project_ids = [part.strip() for part in clean_cell(row.get(project_col)).split(";") if part.strip()]
        for project_id in project_ids:
            new_row = dict(row)
            new_row["project_id"] = project_id
            rows.append(new_row)
    return pd.DataFrame(rows)


def source_text_status(row: pd.Series) -> str:
    if int(row["zap_report_snippet_count"]) > 0:
        return "zap_cpc_report_snippets_available"
    if int(row["zap_report_text_count"]) > 0:
        return "zap_cpc_report_text_available"
    if int(row["council_snippet_count"]) > 0:
        return "council_snippets_available"
    if int(row["council_text_count"]) > 0:
        return "council_action_text_available"
    if int(row["zap_document_link_count"]) > 0 or int(row["council_document_link_count"]) > 0:
        return "document_links_only"
    return "missing_source_packet"


def format_document_lines(rows: list[dict[str, object]], max_rows: int) -> str:
    lines = []
    for row in rows[:max_rows]:
        parts = []
        for key in [
            "source_priority",
            "source_type",
            "document_family",
            "document_role",
            "document_title",
            "source_label",
            "document_url",
            "source_url",
        ]:
            value = clean_cell(row.get(key))
            if value != "":
                parts.append(f"{key}={value}")
        if parts:
            lines.append("- " + "; ".join(parts))
    if not lines:
        return "- No discovered document links in the current packet."
    return "\n".join(lines)


def format_existing_rows(rows: list[dict[str, object]], fields: list[str], max_rows: int, empty_text: str) -> str:
    lines = []
    for row in rows[:max_rows]:
        parts = []
        for field in fields:
            value = compact_text(row.get(field), 360)
            if value != "":
                parts.append(f"{field}={value}")
        if parts:
            lines.append("- " + "; ".join(parts))
    if not lines:
        return f"- {empty_text}"
    return "\n".join(lines)


def page_sort_value(value: object) -> int:
    text = clean_cell(value)
    if text.isdigit():
        return int(text)
    return 9999


def select_report_text_pages(rows: list[dict[str, object]], max_rows: int) -> list[dict[str, object]]:
    scored_rows = []
    for row in rows:
        text = clean_cell(row.get("document_text"))
        if text == "":
            continue

        score = 0
        if re.search(r"\b\d[\d,]*\s+(?:homes?|apartments?|dwelling\s+units?|residential\s+units?|units?)\b", text, re.IGNORECASE):
            score += 6
        if re.search(r"\b\d[\d,]*(?:\.\d+)?\s*(?:million\s+)?square[- ]feet\b|\b\d[\d,]*(?:\.\d+)?-square-foot\b", text, re.IGNORECASE):
            score += 2
        if re.search(r"\bmodified?\b|\bmodifications?\b|\bapproved with modifications\b", text, re.IGNORECASE):
            score += 4
        if re.search(r"\breduced?\b|\breduction\b|\bfewer\b", text, re.IGNORECASE):
            score += 4
        if re.search(r"\bCity Council\b|\bCouncil\b", text, re.IGNORECASE):
            score += 1
        if score == 0:
            continue

        scored_rows.append(
            (
                -score,
                page_sort_value(row.get("source_priority")),
                clean_cell(row.get("source_doc")),
                page_sort_value(row.get("page")),
                row,
            )
        )

    scored_rows.sort(key=lambda item: item[:4])
    return [row for *_, row in scored_rows[:max_rows]]


def format_source_excerpts(project_id: str, packet_data: dict[str, dict[str, list[dict[str, object]]]], spine_row: dict[str, object]) -> str:
    lines = []

    project_brief = clean_cell(spine_row.get("project_brief"))
    if project_brief != "":
        lines.append(
            "\n".join(
                [
                    "- source_doc: zap_ulurp_redev_project_base.csv:project_brief; page: NA_not_stated",
                    f"  snippet: {compact_text(project_brief, 1600)}",
                ]
            )
        )

    for row in packet_data["docket"].get(project_id, [])[:2]:
        text = clean_cell(row.get("docket_description"))
        if text != "":
            lines.append(
                "\n".join(
                    [
                        f"- source_doc: {clean_cell(row.get('project_page_url')) or clean_cell(row.get('api_url'))}; page: NA_not_stated",
                        f"  snippet: {compact_text(text, 1800)}",
                    ]
                )
            )

    for row in packet_data["zap_report_snippets"].get(project_id, [])[:12]:
        lines.append(
            "\n".join(
                [
                    f"- source_doc: {clean_cell(row.get('source_doc'))}; page: {clean_cell(row.get('page'))}; document_family: {clean_cell(row.get('document_family'))}; keyword_family: {clean_cell(row.get('keyword_family'))}; confidence: {clean_cell(row.get('confidence'))}",
                    f"  snippet: {compact_text(row.get('snippet'), 1600)}",
                ]
            )
        )

    for row in select_report_text_pages(packet_data["zap_report_text"].get(project_id, []), 6):
        lines.append(
            "\n".join(
                [
                    f"- source_doc: {clean_cell(row.get('source_doc'))}; page: {clean_cell(row.get('page'))}; document_family: {clean_cell(row.get('document_family'))}; extraction_method: {clean_cell(row.get('extraction_method'))}; confidence: {clean_cell(row.get('confidence'))}",
                    f"  snippet: {compact_text(row.get('document_text'), 1700)}",
                ]
            )
        )

    for row in packet_data["council_snippets"].get(project_id, [])[:12]:
        lines.append(
            "\n".join(
                [
                    f"- source_doc: {clean_cell(row.get('source_doc'))}; page: {clean_cell(row.get('page'))}; keyword_family: {clean_cell(row.get('keyword_family'))}; confidence: {clean_cell(row.get('confidence'))}",
                    f"  snippet: {compact_text(row.get('snippet'), 1600)}",
                ]
            )
        )

    for row in packet_data["council_text"].get(project_id, [])[:6]:
        text = clean_cell(row.get("document_text"))
        if text != "":
            lines.append(
                "\n".join(
                    [
                        f"- source_doc: {clean_cell(row.get('source_doc'))}; page: {clean_cell(row.get('page'))}; document_family: {clean_cell(row.get('document_family'))}",
                        f"  snippet: {compact_text(text, 1500)}",
                    ]
                )
            )

    if not lines:
        return "- No source excerpts are available. Use source_lookup_needed or insufficient_evidence."
    return "\n".join(lines)


def format_project_record(
    row: dict[str, object],
    spine_by_project: dict[str, dict[str, object]],
    packet_data: dict[str, dict[str, list[dict[str, object]]]],
    prompt_version: str,
) -> str:
    project_id = clean_cell(row["project_id"])
    spine_row = spine_by_project.get(project_id, {})

    return "\n".join(
        [
            "-----",
            f"project_id: {project_id}",
            f"prompt_version: {prompt_version}",
            f"ai_queue_rank: {clean_cell(row.get('ai_queue_rank'))}",
            f"ai_selection_reason: {clean_cell(row.get('ai_selection_reason'))}",
            "",
            "project_metadata:",
            f"project_name: {clean_cell(row.get('project_name'))}",
            f"cert_year: {clean_cell(row.get('cert_year'))}",
            f"borough_name: {clean_cell(row.get('borough_name'))}",
            f"stratum: {clean_cell(row.get('stratum'))}",
            f"council_outcome: {clean_cell(row.get('council_outcome'))}",
            f"council_modification_signal: {clean_cell(row.get('council_modification_signal'))}",
            f"local_member_names: {clean_cell(row.get('local_member_names'))}",
            f"local_member_vote_statuses: {clean_cell(row.get('local_member_vote_statuses'))}",
            f"member_deference_vote_signals: {clean_cell(row.get('member_deference_vote_signals'))}",
            "",
            "current_first_pass_summary:",
            f"source_gap_modification_any: {clean_cell(row.get('source_gap_modification_any'))}",
            f"council_stage_modification_any: {clean_cell(row.get('council_stage_modification_any'))}",
            f"modification_categories: {clean_cell(row.get('modification_categories'))}",
            f"certified_units_first_pass: {clean_cell(row.get('certified_units_first_pass'))}",
            f"cpc_units_first_pass: {clean_cell(row.get('cpc_units_first_pass'))}",
            f"adopted_units_first_pass: {clean_cell(row.get('adopted_units_first_pass'))}",
            f"built_units_0_10: {clean_cell(row.get('built_units_0_10'))}",
            "",
            "first_pass_modification_rows:",
            format_existing_rows(
                packet_data["modifications"].get(project_id, []),
                [
                    "modification_category_code",
                    "keyword_family",
                    "document_family",
                    "source_gap_flag",
                    "source_gap_reason",
                    "confidence",
                    "snippet",
                    "source_doc",
                ],
                10,
                "No first-pass modification rows for this project.",
            ),
            "",
            "first_pass_commitment_rows:",
            format_existing_rows(
                packet_data["commitments"].get(project_id, []),
                ["commitment_category", "commitment_stage", "confidence", "snippet", "source_doc"],
                8,
                "No first-pass commitment rows for this project.",
            ),
            "",
            "first_pass_quantity_rows:",
            format_existing_rows(
                packet_data["versions"].get(project_id, []),
                ["stage", "quantity_field", "quantity_value", "quantity_missing_status", "confidence", "snippet", "source_doc"],
                12,
                "No first-pass quantity rows for this project.",
            ),
            "",
            "manual_review_queue_rows:",
            format_existing_rows(
                packet_data["manual"].get(project_id, []),
                ["queue_reason", "source_gap_flag", "confidence", "snippet", "source_doc"],
                8,
                "No manual-review list row for this project.",
            ),
            "",
            "source_document_links:",
            "ZAP links:",
            format_document_lines(packet_data["zap_links"].get(project_id, []), 10),
            "Council links:",
            format_document_lines(packet_data["council_links"].get(project_id, []), 12),
            "",
            "official_source_excerpts:",
            format_source_excerpts(project_id, packet_data, spine_row),
            "",
        ]
    )


def nested_value(response: dict[str, object], container_name: str, field_name: str) -> str:
    direct_value = response.get(field_name, "")
    if clean_cell(direct_value) != "":
        return clean_cell(direct_value)
    container = response.get(container_name, {})
    if isinstance(container, dict):
        return clean_cell(container.get(field_name, ""))
    return ""


def list_value(response: dict[str, object], field_name: str) -> list[object]:
    value = response.get(field_name, [])
    if isinstance(value, list):
        return value
    return []


def quantity_from_response(quantity_rows: list[dict[str, object]], stage: str, quantity_field: str) -> float:
    values = []
    for row in quantity_rows:
        if clean_cell(row.get("stage")) == stage and clean_cell(row.get("quantity_field")) == quantity_field:
            value = number_value(row.get("quantity_value"))
            if not math.isnan(value):
                values.append(value)
    if not values:
        return float("nan")
    return max(values)


def clean_number_for_csv(value: float) -> str:
    if math.isnan(value):
        return ""
    if value == int(value):
        return str(int(value))
    return str(value)


if len(sys.argv) != 6:
    raise SystemExit(
        "Usage: python3 build_ulurp_modification_ai_review.py "
        "<batch_size> <max_review_projects> <random_audit_per_stratum> <prompt_version> <source_packet_mode>"
    )

batch_size = int(sys.argv[1])
max_review_projects = int(sys.argv[2])
random_audit_per_stratum = int(sys.argv[3])
prompt_version = sys.argv[4]
source_packet_mode = sys.argv[5]

if batch_size <= 0:
    raise RuntimeError("batch_size must be positive.")
if max_review_projects < 0:
    raise RuntimeError("max_review_projects must be nonnegative.")
if random_audit_per_stratum < 0:
    raise RuntimeError("random_audit_per_stratum must be nonnegative.")
if prompt_version not in {"v1", "v2"}:
    raise RuntimeError(f"Unsupported prompt_version: {prompt_version}")
if source_packet_mode not in {"all_selected", "zap_cpc_reports_only"}:
    raise RuntimeError(f"Unsupported source_packet_mode: {source_packet_mode}")

spine = read_csv("../input/ulurp_modification_project_spine.csv")
summary = read_csv("../input/ulurp_modification_project_summary.csv")
modifications = read_csv("../input/ulurp_modification_discrete_modifications.csv")
commitments = read_csv("../input/ulurp_modification_commitments.csv")
versions = read_csv("../input/ulurp_modification_project_versions.csv")
manual = read_csv("../input/ulurp_modification_manual_review_queue.csv")
zap_links = read_csv("../input/ulurp_modification_zap_document_links.csv")
docket = read_csv("../input/ulurp_modification_zap_docket_text.csv")
zap_report_text = read_csv("../input/ulurp_modification_zap_report_text.csv")
zap_report_snippets = read_csv("../input/ulurp_modification_zap_report_snippets.csv")
council_links = read_csv("../input/ulurp_modification_council_document_links.csv")
council_text = read_csv("../input/ulurp_modification_council_document_text.csv")
council_snippets = read_csv("../input/ulurp_modification_council_document_snippets.csv")

assert_unique(spine, ["project_id"], "Modification spine")
assert_unique(summary, ["project_id"], "Modification project summary")
assert_unique(modifications, ["modification_id"], "Discrete modifications")
assert_unique(commitments, ["commitment_id"], "Commitments")
assert_unique(versions, ["project_version_id"], "Project versions")
assert_unique(manual, ["manual_review_id"], "Manual review list")
assert_unique(zap_report_text, ["document_page_id"], "ZAP report text")
assert_unique(zap_report_snippets, ["snippet_id"], "ZAP report snippets")
assert_unique(council_links, ["document_id"], "Council document links")
assert_unique(council_snippets, ["snippet_id"], "Council document snippets")

for frame in [spine, summary, modifications, commitments, versions, manual, zap_links, docket, zap_report_text, zap_report_snippets]:
    if "project_id" in frame.columns:
        frame["project_id"] = frame["project_id"].map(clean_cell)

council_links_project = explode_project_ids(council_links)
council_text_project = explode_project_ids(council_text)
council_snippets_project = explode_project_ids(council_snippets)

for frame in [zap_links, zap_report_text, zap_report_snippets, council_links_project]:
    if "source_priority" in frame.columns:
        frame["source_priority_sort"] = pd.to_numeric(frame["source_priority"].map(clean_cell), errors="coerce").fillna(9999).astype(int)

for frame in [zap_report_text, zap_report_snippets]:
    if "page" in frame.columns:
        frame["page_sort"] = pd.to_numeric(frame["page"].map(clean_cell), errors="coerce").fillna(9999).astype(int)

if "keyword_priority" in zap_report_snippets.columns:
    zap_report_snippets["keyword_priority_sort"] = pd.to_numeric(
        zap_report_snippets["keyword_priority"].map(clean_cell),
        errors="coerce",
    ).fillna(9999).astype(int)

zap_report_snippets["packet_relevance_score"] = 50
zap_snippet_text = zap_report_snippets["snippet"].map(clean_cell)
zap_report_snippets.loc[zap_report_snippets["keyword_family"].map(clean_cell) == "unit_quantity", "packet_relevance_score"] -= 20
zap_report_snippets.loc[
    zap_snippet_text.str.contains(r"\b\d[\d,]*\s+(?:new\s+)?(?:homes?|apartments?|dwelling\s+units?|residential\s+units?|units?)\b", case=False, regex=True),
    "packet_relevance_score",
] -= 15
zap_report_snippets.loc[
    zap_snippet_text.str.contains(r"\bproposed development\b|\bwould contain\b|\bwould include\b|\bwould provide\b", case=False, regex=True),
    "packet_relevance_score",
] -= 6
zap_report_snippets.loc[
    zap_snippet_text.str.contains(r"\bsurrounding blocks\b|\brecent development\b|\bneighborhood\b", case=False, regex=True),
    "packet_relevance_score",
] += 6

spine_by_project = {row["project_id"]: row for row in spine.to_dict("records")}

def build_group_dict(df: pd.DataFrame, sort_cols: list[str], max_per_project: int = 50) -> dict[str, list[dict[str, object]]]:
    if df.empty or "project_id" not in df.columns:
        return {}
    for col in sort_cols:
        if col not in df.columns:
            df[col] = ""
    sorted_df = df.sort_values(["project_id"] + sort_cols, kind="stable")
    out = {}
    for project_id, group in sorted_df.groupby("project_id", sort=True):
        out[project_id] = group.head(max_per_project).to_dict("records")
    return out


packet_data = {
    "modifications": build_group_dict(modifications, ["source_gap_flag", "modification_category_code", "modification_id"]),
    "commitments": build_group_dict(commitments, ["commitment_stage", "commitment_category", "commitment_id"]),
    "versions": build_group_dict(versions, ["stage", "quantity_field", "project_version_id"]),
    "manual": build_group_dict(manual, ["queue_reason", "manual_review_id"]),
    "zap_links": build_group_dict(zap_links, ["source_priority_sort", "source_type", "document_title"], 30),
    "docket": build_group_dict(docket, ["project_id"]),
    "zap_report_text": build_group_dict(zap_report_text, ["source_priority_sort", "document_title", "page_sort"], 20),
    "zap_report_snippets": build_group_dict(
        zap_report_snippets,
        ["source_priority_sort", "keyword_priority_sort", "packet_relevance_score", "page_sort", "snippet_id"],
        40,
    ),
    "council_links": build_group_dict(council_links_project, ["source_priority_sort", "document_family", "matter_file"], 40),
    "council_text": build_group_dict(council_text_project, ["document_family", "source_doc"], 20),
    "council_snippets": build_group_dict(council_snippets_project, ["confidence", "keyword_family", "snippet_id"], 30),
}

summary_work = summary.copy()
summary_work["council_modification_signal_bool"] = summary_work["council_modification_signal"].map(yes_value)
summary_work["source_gap_bool"] = summary_work["source_gap_modification_any"].map(yes_value)
summary_work["council_stage_modification_bool"] = summary_work["council_stage_modification_any"].map(yes_value)
summary_work["selected_for_ai_review"] = (
    (summary_work["council_outcome"] == "approve_w_mods")
    & summary_work["council_modification_signal_bool"]
)
summary_work["ai_selection_reason"] = "not_selected"
summary_work.loc[
    summary_work["selected_for_ai_review"] & summary_work["source_gap_bool"],
    "ai_selection_reason",
] = "approve_w_mods_source_gap"
summary_work.loc[
    summary_work["selected_for_ai_review"] & ~summary_work["source_gap_bool"],
    "ai_selection_reason",
] = "approve_w_mods_existing_content"

summary_work["manual_review_reason_count"] = summary_work["project_id"].map(
    manual.groupby("project_id").size().to_dict()
).fillna(0).astype(int)
summary_work["zap_report_snippet_count"] = summary_work["project_id"].map(
    zap_report_snippets.groupby("project_id").size().to_dict()
).fillna(0).astype(int)
summary_work["zap_report_text_count"] = summary_work["project_id"].map(
    zap_report_text.groupby("project_id").size().to_dict()
).fillna(0).astype(int)
summary_work["council_snippet_count"] = summary_work["project_id"].map(
    council_snippets_project.groupby("project_id").size().to_dict()
).fillna(0).astype(int)
summary_work["council_text_count"] = summary_work["project_id"].map(
    council_text_project.groupby("project_id").size().to_dict()
).fillna(0).astype(int)
summary_work["zap_document_link_count"] = summary_work["project_id"].map(
    zap_links.groupby("project_id").size().to_dict()
).fillna(0).astype(int)
summary_work["council_document_link_count"] = summary_work["project_id"].map(
    council_links_project.groupby("project_id").size().to_dict()
).fillna(0).astype(int)
summary_work["source_text_status"] = [source_text_status(row) for _, row in summary_work.iterrows()]

if source_packet_mode == "zap_cpc_reports_only":
    no_zap_report_evidence = (
        summary_work["selected_for_ai_review"]
        & (summary_work["zap_report_snippet_count"] == 0)
        & (summary_work["zap_report_text_count"] == 0)
    )
    summary_work.loc[no_zap_report_evidence, "selected_for_ai_review"] = False
    summary_work.loc[no_zap_report_evidence, "ai_selection_reason"] = "excluded_no_zap_cpc_report_text"

review_queue = summary_work.loc[summary_work["selected_for_ai_review"]].copy()
review_queue["selection_order"] = review_queue["ai_selection_reason"].map(
    {"approve_w_mods_source_gap": 1, "approve_w_mods_existing_content": 2}
).fillna(3)
review_queue["cert_year_numeric"] = pd.to_numeric(review_queue["cert_year"], errors="coerce").fillna(9999)
review_queue["source_gap_order"] = review_queue["source_gap_bool"].map({True: 0, False: 1}).fillna(2)
review_queue["review_sort_hash"] = [
    stable_hash(f"{project_id}|{prompt_version}|ai_review")
    for project_id in review_queue["project_id"]
]
review_queue = review_queue.sort_values(
    ["selection_order", "cert_year_numeric", "borough_name", "project_name", "review_sort_hash", "project_id"],
    ascending=[True, True, True, True, True, True],
)
if max_review_projects > 0:
    review_queue = review_queue.head(max_review_projects).copy()

selected_project_ids = set(review_queue["project_id"])

if random_audit_per_stratum > 0:
    audit_pool = summary_work.loc[~summary_work["project_id"].isin(selected_project_ids)].copy()
    audit_pool["audit_stratum"] = (
        audit_pool["cert_era"].fillna("").astype(str)
        + "|"
        + audit_pool["borough_name"].fillna("").astype(str)
        + "|"
        + audit_pool["stratum"].fillna("").astype(str)
    )
    audit_pool["audit_hash"] = [
        stable_hash(f"{project_id}|{prompt_version}|audit")
        for project_id in audit_pool["project_id"]
    ]
    audit_rows = []
    for _, group in audit_pool.sort_values(["audit_stratum", "audit_hash", "project_id"]).groupby("audit_stratum", sort=True):
        audit_rows.append(group.head(random_audit_per_stratum))
    if audit_rows:
        audit_queue = pd.concat(audit_rows, ignore_index=True)
        audit_queue["ai_selection_reason"] = "stratified_no_modification_audit"
        review_queue = pd.concat([review_queue, audit_queue], ignore_index=True)

review_queue = review_queue.sort_values(
    ["selection_order", "cert_year_numeric", "borough_name", "project_name", "project_id"],
    ascending=[True, True, True, True, True],
)
review_queue["ai_queue_rank"] = range(1, len(review_queue) + 1)

summary_work["ai_queue_rank"] = ""
summary_work["ai_batch_id"] = ""
summary_work["ai_batch_path"] = ""
summary_work.loc[summary_work["project_id"].isin(set(review_queue["project_id"])), "selected_for_ai_review"] = True

selected_records = review_queue.to_dict("records")
batches = []
batch_project_rows = []
for start_index in range(0, len(selected_records), batch_size):
    batch_id = f"{len(batches) + 1:03d}"
    batch_records = selected_records[start_index : start_index + batch_size]
    batch_path = f"../output/batches/ulurp_modification_ai_review_batch_{batch_id}.md"
    batch_text = PROMPT_HEADER + "\n\n" + "\n".join(
        format_project_record(row, spine_by_project, packet_data, prompt_version)
        for row in batch_records
    )
    write_text_if_changed(batch_text, batch_path)
    batch_project_ids = [row["project_id"] for row in batch_records]
    for row in batch_records:
        batch_project_rows.append(
            {
                "project_id": row["project_id"],
                "ai_batch_id": batch_id,
                "ai_batch_path": batch_path,
                "ai_queue_rank": row["ai_queue_rank"],
            }
        )
    batches.append(
        {
            "batch_id": batch_id,
            "batch_path": batch_path,
            "project_count": len(batch_records),
            "first_queue_rank": batch_records[0]["ai_queue_rank"],
            "last_queue_rank": batch_records[-1]["ai_queue_rank"],
            "char_count": len(batch_text),
            "project_ids": "|".join(batch_project_ids),
        }
    )

batch_lookup = pd.DataFrame(batch_project_rows)
if not batch_lookup.empty:
    summary_work = summary_work.merge(batch_lookup, on="project_id", how="left", suffixes=("", "_from_batch"), validate="one_to_one")
    for column in ["ai_queue_rank", "ai_batch_id", "ai_batch_path"]:
        summary_work[column] = summary_work[f"{column}_from_batch"].fillna(summary_work[column])
        summary_work = summary_work.drop(columns=[f"{column}_from_batch"])
    review_queue = review_queue.merge(
        batch_lookup[["project_id", "ai_batch_id", "ai_batch_path"]],
        on="project_id",
        how="left",
        validate="one_to_one",
    )

seed_rows = []
modification_counts = modifications.groupby("project_id").size().to_dict()
non_gap_modifications = modifications.loc[~modifications["source_gap_flag"].map(yes_value)].copy()
non_gap_counts = non_gap_modifications.groupby("project_id").size().to_dict()
non_gap_categories = non_gap_modifications.groupby("project_id")["modification_category_code"].apply(collapse_values).to_dict()
non_gap_confidence = non_gap_modifications.groupby("project_id")["confidence"].apply(collapse_values).to_dict()

for row in review_queue.to_dict("records"):
    project_id = clean_cell(row["project_id"])
    source_gap = yes_value(row.get("source_gap_modification_any"))
    non_gap_count = int(non_gap_counts.get(project_id, 0))
    categories = clean_cell(non_gap_categories.get(project_id, ""))
    confidence_text = clean_cell(non_gap_confidence.get(project_id, ""))
    if source_gap:
        final_decision = "source_lookup_needed"
        present = "yes"
        confidence = "low"
        note = "Council approved-with-modifications signal exists, but current source packet has no substantive modification text."
    elif non_gap_count > 0:
        final_decision = "modification_signal_only" if categories == "O-MD_unspecified_modification_signal" else "modification_confirmed"
        present = "yes"
        confidence = "medium" if "medium" in confidence_text or "high" in confidence_text else "low"
        note = "Seed label summarizes existing first-pass extracted Council-stage rows; it is not an LLM adjudication."
    else:
        final_decision = "unclear"
        present = "unclear"
        confidence = "unknown"
        note = "No source-gap row or non-gap modification row was available."
    seed_rows.append(
        {
            "project_id": project_id,
            "project_name": clean_cell(row.get("project_name")),
            "seed_label_source": "deterministic_seed_from_first_pass",
            "seed_final_decision": final_decision,
            "seed_council_stage_modification_present": present,
            "seed_human_review_required": "yes",
            "seed_confidence": confidence,
            "seed_category_codes": categories,
            "seed_non_gap_modification_count": non_gap_count,
            "seed_source_gap_flag": "true" if source_gap else "false",
            "seed_note": note,
        }
    )

write_text_if_changed(PROMPT_HEADER, "../output/ulurp_modification_ai_review_prompt.md")
write_text_if_changed(
    "".join(json.dumps(batch, ensure_ascii=True) + "\n" for batch in batches),
    "../output/ulurp_modification_ai_review_batches.jsonl",
)

seen_response_project_ids = set()
parsed_response_rows = []
modification_response_rows = []
quantity_response_rows = []
error_rows = []
review_project_ids = set(review_queue["project_id"])
frame_project_ids = set(summary_work["project_id"])

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

            project_id = clean_cell(response.get("project_id"))
            duplicate_flag = project_id in seen_response_project_ids
            seen_response_project_ids.add(project_id)

            project_classification = response.get("project_classification", {})
            adjudication = response.get("adjudication", {})
            if not isinstance(project_classification, dict):
                project_classification = {}
            if not isinstance(adjudication, dict):
                adjudication = {}

            quantity_versions = [row for row in list_value(response, "quantity_versions") if isinstance(row, dict)]
            modifications_json = [row for row in list_value(response, "modifications") if isinstance(row, dict)]
            commitments_json = [row for row in list_value(response, "commitments") if isinstance(row, dict)]
            evidence_spans = [row for row in list_value(response, "evidence_spans") if isinstance(row, dict)]
            documents_used = [row for row in list_value(response, "documents_used") if isinstance(row, dict)]

            category_codes = collapse_values(clean_cell(row.get("modification_category_code")) for row in modifications_json)
            direction_codes = collapse_values(clean_cell(row.get("direction")) for row in modifications_json)
            adopted_units = quantity_from_response(quantity_versions, "council_stage", "units")
            cpc_units = quantity_from_response(quantity_versions, "cpc_approved", "units")
            certified_units = quantity_from_response(quantity_versions, "certified", "units")
            unit_delta = adopted_units - cpc_units if not math.isnan(adopted_units) and not math.isnan(cpc_units) else float("nan")

            response_row = {
                "project_id": project_id,
                "response_line_number": line_number,
                "duplicate_response_flag": "true" if duplicate_flag else "false",
                "prompt_version": clean_cell(response.get("prompt_version")),
                "review_stage": clean_cell(response.get("review_stage")),
                "model": clean_cell(response.get("model")),
                "outside_knowledge_used": clean_cell(response.get("outside_knowledge_used")),
                "council_stage_modification_present": nested_value(response, "project_classification", "council_stage_modification_present"),
                "final_decision": nested_value(response, "project_classification", "final_decision"),
                "human_review_required": nested_value(response, "project_classification", "human_review_required"),
                "classification_confidence": nested_value(response, "project_classification", "classification_confidence"),
                "source_gap_resolved": nested_value(response, "project_classification", "source_gap_resolved"),
                "modification_count": len(modifications_json),
                "commitment_count": len(commitments_json),
                "quantity_version_count": len(quantity_versions),
                "evidence_span_count": len(evidence_spans),
                "document_count": len(documents_used),
                "category_codes": category_codes,
                "direction_codes": direction_codes,
                "adopted_units_ai": clean_number_for_csv(adopted_units),
                "cpc_units_ai": clean_number_for_csv(cpc_units),
                "certified_units_ai": clean_number_for_csv(certified_units),
                "council_vs_cpc_units_ai": clean_number_for_csv(unit_delta),
                "recommended_action": clean_cell(adjudication.get("recommended_action")),
                "review_note": clean_cell(adjudication.get("review_note")),
            }

            validation_errors = []
            if project_id == "":
                validation_errors.append("missing_project_id")
            if project_id not in frame_project_ids:
                validation_errors.append("project_id_not_in_frame")
            if project_id not in review_project_ids:
                validation_errors.append("project_id_not_selected_for_ai_review")
            if duplicate_flag:
                validation_errors.append("duplicate_project_response")
            if response_row["prompt_version"] != prompt_version:
                validation_errors.append("prompt_version_mismatch")
            if response_row["review_stage"] != "ulurp_modification_ai_review":
                validation_errors.append("invalid_review_stage")
            if response_row["outside_knowledge_used"] not in ALLOWED_YES_NO_UNCLEAR:
                validation_errors.append("invalid_outside_knowledge_used")
            if response_row["council_stage_modification_present"] not in ALLOWED_YES_NO_UNCLEAR:
                validation_errors.append("invalid_council_stage_modification_present")
            if response_row["final_decision"] not in ALLOWED_FINAL_DECISIONS:
                validation_errors.append("invalid_final_decision")
            if response_row["human_review_required"] not in ALLOWED_YES_NO_UNCLEAR:
                validation_errors.append("invalid_human_review_required")
            if response_row["classification_confidence"] not in ALLOWED_CONFIDENCE:
                validation_errors.append("invalid_classification_confidence")
            if response_row["source_gap_resolved"] not in ALLOWED_YES_NO_UNCLEAR:
                validation_errors.append("invalid_source_gap_resolved")
            if response_row["recommended_action"] not in ALLOWED_ACTIONS:
                validation_errors.append("invalid_recommended_action")
            if response_row["final_decision"] == "modification_confirmed" and response_row["modification_count"] == 0:
                validation_errors.append("modification_confirmed_without_modification_rows")
            if response_row["recommended_action"] == "accept_ai_label" and response_row["evidence_span_count"] == 0:
                validation_errors.append("accepted_ai_label_without_evidence_spans")
            if response_row["outside_knowledge_used"] != "no":
                validation_errors.append("outside_knowledge_not_allowed")

            for index, modification in enumerate(modifications_json, start=1):
                mod_row = {
                    "project_id": project_id,
                    "response_line_number": line_number,
                    "modification_index": index,
                    "modification_stage": clean_cell(modification.get("modification_stage")),
                    "modification_category_code": clean_cell(modification.get("modification_category_code")),
                    "direction": clean_cell(modification.get("direction")),
                    "quantity_field": clean_cell(modification.get("quantity_field")),
                    "before_value": clean_cell(modification.get("before_value")),
                    "after_value": clean_cell(modification.get("after_value")),
                    "delta_value": clean_cell(modification.get("delta_value")),
                    "description": clean_cell(modification.get("description")),
                    "local_member_attribution": clean_cell(modification.get("local_member_attribution")),
                    "source_doc": clean_cell(modification.get("source_doc")),
                    "page": clean_cell(modification.get("page")),
                    "evidence_snippet": clean_cell(modification.get("evidence_snippet")),
                    "confidence": clean_cell(modification.get("confidence")),
                }
                modification_response_rows.append(mod_row)
                if mod_row["modification_stage"] not in ALLOWED_STAGES:
                    validation_errors.append(f"invalid_modification_stage_{index}")
                if mod_row["modification_category_code"] not in ALLOWED_CATEGORIES:
                    validation_errors.append(f"invalid_modification_category_{index}")
                if mod_row["direction"] not in ALLOWED_DIRECTIONS:
                    validation_errors.append(f"invalid_modification_direction_{index}")
                if mod_row["confidence"] not in ALLOWED_CONFIDENCE:
                    validation_errors.append(f"invalid_modification_confidence_{index}")
                if mod_row["source_doc"] == "" or mod_row["evidence_snippet"] == "":
                    validation_errors.append(f"modification_missing_evidence_{index}")

            for index, quantity in enumerate(quantity_versions, start=1):
                quantity_row = {
                    "project_id": project_id,
                    "response_line_number": line_number,
                    "quantity_index": index,
                    "stage": clean_cell(quantity.get("stage")),
                    "quantity_field": clean_cell(quantity.get("quantity_field")),
                    "quantity_value": clean_cell(quantity.get("quantity_value")),
                    "quantity_missing_status": clean_cell(quantity.get("quantity_missing_status")),
                    "source_doc": clean_cell(quantity.get("source_doc")),
                    "page": clean_cell(quantity.get("page")),
                    "evidence_snippet": clean_cell(quantity.get("evidence_snippet")),
                    "confidence": clean_cell(quantity.get("confidence")),
                }
                quantity_response_rows.append(quantity_row)
                if quantity_row["stage"] not in ALLOWED_STAGES:
                    validation_errors.append(f"invalid_quantity_stage_{index}")
                if quantity_row["confidence"] not in ALLOWED_CONFIDENCE:
                    validation_errors.append(f"invalid_quantity_confidence_{index}")
                if quantity_row["quantity_missing_status"] == "observed":
                    if quantity_row["source_doc"] == "" or quantity_row["evidence_snippet"] == "":
                        validation_errors.append(f"observed_quantity_missing_evidence_{index}")

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
    if row["project_id"] in review_project_ids:
        batch_id = clean_cell(summary_work.loc[summary_work["project_id"] == row["project_id"], "ai_batch_id"].iloc[0])
    else:
        batch_id = ""
    row["batch_id"] = batch_id

response_project_ids = {row["project_id"] for row in parsed_response_rows if row["project_id"] != ""}
missing_response_rows = []
for row in review_queue.sort_values(["ai_queue_rank", "project_id"]).to_dict("records"):
    if row["project_id"] not in response_project_ids:
        missing_response_rows.append(
            {
                "project_id": clean_cell(row["project_id"]),
                "ai_batch_id": clean_cell(row.get("ai_batch_id")),
                "ai_queue_rank": clean_cell(row.get("ai_queue_rank")),
                "cert_year": clean_cell(row.get("cert_year")),
                "project_name": clean_cell(row.get("project_name")),
                "borough_name": clean_cell(row.get("borough_name")),
                "stratum": clean_cell(row.get("stratum")),
                "source_text_status": clean_cell(row.get("source_text_status")),
                "source_gap_modification_any": clean_cell(row.get("source_gap_modification_any")),
                "ai_selection_reason": clean_cell(row.get("ai_selection_reason")),
                "ai_batch_path": clean_cell(row.get("ai_batch_path")),
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
    next_batch_text = Path(missing_batches[0]["batch_path"]).read_text(encoding="utf-8")
else:
    next_batch_text = "# ULURP Modification Content AI Review\n\nAll selected AI review batches have a response in code/llm_review_responses.jsonl.\n"
write_text_if_changed(next_batch_text, "../output/ulurp_modification_ai_review_next_batch.md")

frame_rows = []
for row in summary_work.sort_values(["selected_for_ai_review", "ai_queue_rank", "project_id"], ascending=[False, True, True]).to_dict("records"):
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

ready_label_rows = [
    row
    for row in parsed_response_rows
    if row.get("validation_status") == "valid"
    and row.get("recommended_action") == "accept_ai_label"
    and row.get("human_review_required") == "no"
]

qc_rows = [
    {
        "check_name": "project_frame_rows",
        "check_value": len(summary_work),
        "status": "pass" if len(summary_work) == 573 else "review",
        "note": "Rows in the modification project summary frame.",
    },
    {
        "check_name": "selected_ai_review_project_count",
        "check_value": len(review_queue),
        "status": "pass",
        "note": "Approved-with-modifications projects selected for paste-based AI review.",
    },
    {
        "check_name": "excluded_no_zap_cpc_report_text_project_count",
        "check_value": int((summary_work["ai_selection_reason"] == "excluded_no_zap_cpc_report_text").sum()),
        "status": "pass",
        "note": "Approved-with-modifications projects excluded by source_packet_mode because no ZAP CPC/M report text was extracted.",
    },
    {
        "check_name": "selected_source_gap_project_count",
        "check_value": int(review_queue["source_gap_bool"].sum()) if "source_gap_bool" in review_queue.columns else 0,
        "status": "review",
        "note": "Selected projects where current first-pass content has a source gap.",
    },
    {
        "check_name": "selected_existing_content_project_count",
        "check_value": int((review_queue["ai_selection_reason"] == "approve_w_mods_existing_content").sum()),
        "status": "pass",
        "note": "Selected projects with existing non-gap first-pass modification content.",
    },
    {
        "check_name": "batch_count",
        "check_value": len(batches),
        "status": "pass",
        "note": f"Batch size is {batch_size}.",
    },
    {
        "check_name": "seed_label_rows",
        "check_value": len(seed_rows),
        "status": "pass",
        "note": "Deterministic seed labels used only to prioritize review before AI responses are pasted.",
    },
    {
        "check_name": "response_row_count",
        "check_value": len(parsed_response_rows),
        "status": "pass",
        "note": "Parsed AI response rows from code/llm_review_responses.jsonl.",
    },
    {
        "check_name": "valid_response_row_count",
        "check_value": sum(1 for row in parsed_response_rows if row.get("validation_status") == "valid"),
        "status": "pass",
        "note": "AI responses that pass schema and evidence validation.",
    },
    {
        "check_name": "invalid_response_error_count",
        "check_value": len(error_rows),
        "status": "pass" if len(error_rows) == 0 else "review",
        "note": "Parse and validation errors in pasted AI output.",
    },
    {
        "check_name": "missing_selected_response_count",
        "check_value": len(missing_response_rows),
        "status": "needs_ai_labels" if len(missing_response_rows) > 0 else "pass",
        "note": "Selected projects without a pasted AI response yet.",
    },
    {
        "check_name": "ready_label_count",
        "check_value": len(ready_label_rows),
        "status": "review" if len(ready_label_rows) == 0 and len(parsed_response_rows) > 0 else "pass",
        "note": "Valid AI labels that the model says can be accepted without further human review.",
    },
]

write_csv_if_changed(frame_rows, FRAME_COLUMNS, "../output/ulurp_modification_ai_review_frame.csv")
write_csv_if_changed(seed_rows, SEED_LABEL_COLUMNS, "../output/ulurp_modification_ai_review_seed_labels.csv")
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
    "../output/ulurp_modification_ai_review_batch_manifest.csv",
)
write_csv_if_changed(parsed_response_rows, RESPONSE_COLUMNS, "../output/ulurp_modification_ai_review_response_compiled.csv")
write_csv_if_changed(modification_response_rows, MODIFICATION_COLUMNS, "../output/ulurp_modification_ai_review_response_modifications.csv")
write_csv_if_changed(quantity_response_rows, QUANTITY_COLUMNS, "../output/ulurp_modification_ai_review_response_quantities.csv")
write_csv_if_changed(error_rows, ERROR_COLUMNS, "../output/ulurp_modification_ai_review_response_errors.csv")
write_csv_if_changed(missing_response_rows, MISSING_COLUMNS, "../output/ulurp_modification_ai_review_missing_responses.csv")
write_csv_if_changed(ready_label_rows, RESPONSE_COLUMNS, "../output/ulurp_modification_ai_review_ready_labels.csv")
write_csv_if_changed(qc_rows, ["check_name", "check_value", "status", "note"], "../output/ulurp_modification_ai_review_qc.csv")

print(
    f"Wrote {len(batches)} AI review batches for {len(review_queue)} selected projects; "
    f"{len(missing_response_rows)} selected projects still need pasted responses."
)
