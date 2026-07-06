#!/usr/bin/env python3

import csv
import hashlib
import json
import re
import sys
import textwrap
from collections import defaultdict
from pathlib import Path


# setwd("tasks/audits/build_ulurp_cpc_llm_exploration/code")
# prompt_version = "v1"
# revision_batch_size = 25
# revision_max_reports = 0
# decade_sample_per_decade = 40
# max_revision_chars = 12000
# max_decade_snippet_chars = 1800


REVISION_KEYWORDS = [
    "modified",
    "modification",
    "revised",
    "revision",
    "changed",
    "amended",
    "community board",
    "borough president",
    "city council",
    "council member",
    "at the request",
    "opposition",
    "support",
    "recommended",
    "condition",
    "withdrawn",
]

DECADE_KEYWORDS = [
    "community board",
    "borough president",
    "city council",
    "council member",
    "opposition",
    "oppose",
    "support",
    "public hearing",
    "testified",
    "modified",
    "revised",
    "condition",
    "concerns",
    "neighborhood",
    "civic association",
    "community organization",
]

REVISION_PROMPT_HEADER = """# ULURP CPC Report Revision Narrative Pass

You are helping build an exploratory research index from official New York City CPC reports.

These are CPC reports, not raw ULURP application forms. Use only the supplied report excerpts. The goal is to identify whether the report says the proposal was revised, modified, conditioned, negotiated, opposed, or changed during review, and to capture named actors mentioned in that discussion.

Return one compact JSON object per report, one object per line, with exactly these fields:

```json
{
  "document_id": "",
  "project_id": "",
  "application_number": "",
  "year": "",
  "revision_or_modification": "yes/no/unclear",
  "revision_narrative": "",
  "revision_when": "",
  "requested_by": "",
  "changed_what": "",
  "opposition_or_negotiation_narrative": "",
  "member_names": [],
  "civic_association_names": [],
  "applicant_attorney_or_law_firm_names": [],
  "other_land_use_actor_names": [],
  "confidence": "high/medium/low",
  "evidence_quote": ""
}
```

Rules:
- `revision_narrative` should be one sentence of 35 words or fewer.
- If the excerpt has no evidence of revision, modification, negotiation, or opposition, say so plainly in `revision_narrative`.
- `requested_by` can be a public body, elected official, community board, civic group, applicant, or `not stated`.
- Put law firms, land-use attorneys, lobbyists, expediters, or applicant representatives in `applicant_attorney_or_law_firm_names`.
- Keep `evidence_quote` short and quote only the strongest phrase from the excerpt.
- Use `unclear` and low confidence when the excerpt is too thin.
- Do not add markdown, commentary, or bullets outside the JSON lines.
"""

DECADE_PROMPT_HEADER = """# ULURP CPC Report Decade Comparison Pass

You are helping generate hypotheses for an academic project about NYC land-use review.

These are official CPC report excerpts sampled from one decade. Use only the supplied excerpts. Focus on how the reports describe negotiation, opposition, community-board or borough-president positions, City Council or council-member roles, modifications, and applicant concessions.

Return one compact JSON object for the decade with exactly these fields:

```json
{
  "decade": "",
  "reports_read": 0,
  "overall_summary": "",
  "negotiation_and_opposition_patterns": [],
  "member_deference_or_council_role_patterns": [],
  "revision_or_modification_patterns": [],
  "named_actor_patterns": [],
  "suggested_countable_measures": [],
  "evidence_examples": [
    {
      "document_id": "",
      "application_number": "",
      "year": "",
      "example": ""
    }
  ],
  "caveats": []
}
```

Rules:
- Treat this as hypothesis generation, not final evidence.
- Separate direct textual patterns from your interpretation.
- Keep examples short and tied to supplied document ids.
- Do not use outside memory.
- Do not add markdown outside the JSON object.
"""

FRAME_COLUMNS = [
    "document_id",
    "project_id",
    "corpus_reference_year",
    "decade",
    "project_name",
    "raw_application_number",
    "application_prefix",
    "parsed_action_code",
    "borough_name",
    "community_district",
    "primary_applicant",
    "applicant_type",
    "ceqr_number",
    "source_doc",
    "project_page_url",
    "usable_text_source_type",
    "usable_text_status",
    "usable_text_char_count",
    "usable_local_text_path",
    "text_file_status",
    "selected_for_revision_narrative",
    "revision_batch_id",
    "revision_batch_sequence",
    "selected_for_decade_comparison",
    "decade_sample_rank",
    "decade_batch_id",
]


def as_int(value):
    if value in ("", None):
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def stable_hash(*values):
    return hashlib.sha256("|".join(str(value) for value in values).encode("utf-8")).hexdigest()


def decade_from_year(year):
    if year is None:
        return "missing"
    return f"{year // 10 * 10}s"


def resolve_text_path(raw_path, manifest_real_path):
    if not raw_path:
        return None

    path = Path(raw_path)
    if path.is_absolute():
        return path

    task_root = manifest_real_path.parent.parent
    return task_root / "code" / path


def text_file_status(text_path, usable_text_char_count):
    if text_path is None:
        return "no_usable_text_path"
    if not text_path.exists():
        return "text_path_missing"

    stat_result = text_path.stat()
    if getattr(stat_result, "st_blocks", 1) == 0 and stat_result.st_size > 0:
        return "local_file_dataless"
    if usable_text_char_count <= 0 or stat_result.st_size < 100:
        return "empty_or_tiny_text"

    return "readable"


def read_ocr_fallbacks(ocr_manifest_path):
    ocr_manifest_real_path = ocr_manifest_path.resolve()
    fallback_rows = {}
    with ocr_manifest_path.open(newline="", encoding="utf-8") as ocr_manifest_file:
        for row in csv.DictReader(ocr_manifest_file):
            text_char_count = as_int(row.get("text_char_count", "")) or 0
            if row.get("ocr_status", "") != "text_extracted_ocr" or text_char_count <= 0:
                continue
            text_path = resolve_text_path(row.get("output_text_path", ""), ocr_manifest_real_path)
            fallback_rows[row["raw_application_number"]] = {
                "text_path": text_path,
                "text_char_count": text_char_count,
                "frame_text_path": (
                    "../../ocr_ulurp_flushing_commons_cpc_reports/output/"
                    + Path(row.get("output_text_path", "")).name
                ),
            }
    return fallback_rows


def read_report_text(text_path):
    return text_path.read_text(encoding="utf-8", errors="replace")


def normalized_text(text):
    return re.sub(r"\s+", " ", text).strip()


def excerpt_from_text(text, keywords, max_chars):
    text = normalized_text(text)
    if len(text) <= max_chars:
        return text

    spans = [(0, min(2200, len(text)))]
    lower_text = text.lower()
    for keyword in keywords:
        for match in re.finditer(re.escape(keyword), lower_text):
            spans.append((max(0, match.start() - 650), min(len(text), match.end() + 1200)))
            break

    spans = sorted(spans)
    merged_spans = []
    for start, end in spans:
        if not merged_spans or start > merged_spans[-1][1] + 200:
            merged_spans.append([start, end])
        else:
            merged_spans[-1][1] = max(merged_spans[-1][1], end)

    excerpt_parts = []
    used_chars = 0
    for start, end in merged_spans:
        part = text[start:end].strip()
        if not part:
            continue
        remaining_chars = max_chars - used_chars
        if remaining_chars <= 0:
            break
        if len(part) > remaining_chars:
            part = part[:remaining_chars].rsplit(" ", 1)[0].strip()
        excerpt_parts.append(part)
        used_chars += len(part)

    return "\n\n[...]\n\n".join(excerpt_parts).strip()


def report_block(row, text, max_chars, keywords):
    excerpt = excerpt_from_text(text, keywords, max_chars)
    return textwrap.dedent(f"""
    Document ID: {row["document_id"]}
    Project ID: {row["project_id"]}
    Year: {row["corpus_reference_year"]}
    Decade: {row["decade"]}
    Project name: {row["project_name"]}
    Application number: {row["raw_application_number"]}
    Action code: {row["parsed_action_code"]}
    Borough: {row["borough_name"]}
    Community district: {row["community_district"]}
    Applicant: {row["primary_applicant"]}
    Applicant type: {row["applicant_type"]}
    Source URL: {row["source_doc"]}
    Project page: {row["project_page_url"]}

    Excerpt:
    {excerpt}
    """).strip()


def write_csv(path, rows, columns):
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def write_prompt_template(path, prompt_header):
    path.write_text(prompt_header.strip() + "\n", encoding="utf-8")


def build_revision_prompt(batch_rows, text_by_document_id, max_revision_chars):
    blocks = [
        report_block(row, text_by_document_id[row["document_id"]], max_revision_chars, REVISION_KEYWORDS)
        for row in batch_rows
    ]
    return REVISION_PROMPT_HEADER.strip() + "\n\n# Reports\n\n" + "\n\n---\n\n".join(blocks) + "\n"


def build_decade_prompt(decade, decade_rows, text_by_document_id, max_decade_snippet_chars):
    blocks = [
        report_block(row, text_by_document_id[row["document_id"]], max_decade_snippet_chars, DECADE_KEYWORDS)
        for row in decade_rows
    ]
    return (
        DECADE_PROMPT_HEADER.strip()
        + f"\n\n# Decade\n\n{decade}\n\n# Sampled Reports\n\n"
        + "\n\n---\n\n".join(blocks)
        + "\n"
    )


def chunks(rows, chunk_size):
    for index in range(0, len(rows), chunk_size):
        yield rows[index:index + chunk_size]


def main():
    if len(sys.argv) != 7:
        raise SystemExit(
            "Usage: python3 build_ulurp_cpc_llm_exploration.py "
            "PROMPT_VERSION REVISION_BATCH_SIZE REVISION_MAX_REPORTS "
            "DECADE_SAMPLE_PER_DECADE MAX_REVISION_CHARS MAX_DECADE_SNIPPET_CHARS"
        )

    prompt_version = sys.argv[1]
    revision_batch_size = int(sys.argv[2])
    revision_max_reports = int(sys.argv[3])
    decade_sample_per_decade = int(sys.argv[4])
    max_revision_chars = int(sys.argv[5])
    max_decade_snippet_chars = int(sys.argv[6])

    manifest_path = Path("../input/ulurp_cpc_report_manifest.csv")
    manifest_real_path = manifest_path.resolve()
    with manifest_path.open(newline="", encoding="utf-8") as manifest_file:
        manifest_rows = list(csv.DictReader(manifest_file))
    ocr_fallbacks = read_ocr_fallbacks(Path("../input/flushing_commons_cpc_ocr_manifest.csv"))

    frame_rows = []
    readable_rows = []
    text_by_document_id = {}

    for manifest_row in manifest_rows:
        year = as_int(manifest_row.get("corpus_reference_year", ""))
        usable_text_char_count = as_int(manifest_row.get("usable_text_char_count", "")) or 0
        text_path = resolve_text_path(manifest_row.get("usable_local_text_path", ""), manifest_real_path)
        status = text_file_status(text_path, usable_text_char_count)
        usable_text_source_type = manifest_row.get("usable_text_source_type", "")
        usable_text_status = manifest_row.get("usable_text_status", "")
        usable_local_text_path = manifest_row.get("usable_local_text_path", "")

        ocr_fallback = ocr_fallbacks.get(manifest_row.get("raw_application_number", ""))
        if status != "readable" and ocr_fallback is not None:
            text_path = ocr_fallback["text_path"]
            usable_text_char_count = ocr_fallback["text_char_count"]
            status = text_file_status(text_path, usable_text_char_count)
            if status == "readable":
                usable_text_source_type = "audit_ocr_cpc_report"
                usable_text_status = "text_extracted_ocr_audit"
                usable_local_text_path = ocr_fallback["frame_text_path"]

        row = {
            "document_id": manifest_row.get("document_id", ""),
            "project_id": manifest_row.get("project_id", ""),
            "corpus_reference_year": year if year is not None else "",
            "decade": decade_from_year(year),
            "project_name": manifest_row.get("project_name", ""),
            "raw_application_number": manifest_row.get("raw_application_number", ""),
            "application_prefix": manifest_row.get("application_prefix", ""),
            "parsed_action_code": manifest_row.get("parsed_action_code", ""),
            "borough_name": manifest_row.get("borough_name", ""),
            "community_district": manifest_row.get("community_district", ""),
            "primary_applicant": manifest_row.get("primary_applicant", ""),
            "applicant_type": manifest_row.get("applicant_type", ""),
            "ceqr_number": manifest_row.get("ceqr_number", ""),
            "source_doc": manifest_row.get("source_doc", ""),
            "project_page_url": manifest_row.get("project_page_url", ""),
            "usable_text_source_type": usable_text_source_type,
            "usable_text_status": usable_text_status,
            "usable_text_char_count": usable_text_char_count,
            "usable_local_text_path": usable_local_text_path,
            "text_file_status": status,
            "selected_for_revision_narrative": 0,
            "revision_batch_id": "",
            "revision_batch_sequence": "",
            "selected_for_decade_comparison": 0,
            "decade_sample_rank": "",
            "decade_batch_id": "",
        }

        if status == "readable" and usable_text_char_count > 0:
            readable_rows.append(row)
            text_by_document_id[row["document_id"]] = read_report_text(text_path)

        frame_rows.append(row)

    selected_revision_rows = sorted(
        readable_rows,
        key=lambda row: (
            row["corpus_reference_year"] if row["corpus_reference_year"] != "" else 9999,
            row["raw_application_number"],
            row["document_id"],
        ),
    )
    if revision_max_reports > 0:
        selected_revision_rows = selected_revision_rows[:revision_max_reports]

    frame_by_document_id = {row["document_id"]: row for row in frame_rows}
    for sequence, row in enumerate(selected_revision_rows, start=1):
        batch_number = (sequence - 1) // revision_batch_size + 1
        batch_id = f"revision_{prompt_version}_{batch_number:04d}"
        frame_by_document_id[row["document_id"]]["selected_for_revision_narrative"] = 1
        frame_by_document_id[row["document_id"]]["revision_batch_id"] = batch_id
        frame_by_document_id[row["document_id"]]["revision_batch_sequence"] = sequence
        row["revision_batch_id"] = batch_id
        row["revision_batch_sequence"] = sequence

    readable_rows_by_decade = defaultdict(list)
    for row in readable_rows:
        if row["decade"] != "missing":
            readable_rows_by_decade[row["decade"]].append(row)

    decade_sample_rows = []
    for decade in sorted(readable_rows_by_decade):
        candidate_rows = sorted(
            readable_rows_by_decade[decade],
            key=lambda row: stable_hash(
                "decade_sample",
                decade,
                row["document_id"],
                row["raw_application_number"],
                row["project_id"],
            ),
        )
        for rank, row in enumerate(candidate_rows[:decade_sample_per_decade], start=1):
            batch_id = f"decade_{prompt_version}_{decade}"
            frame_by_document_id[row["document_id"]]["selected_for_decade_comparison"] = 1
            frame_by_document_id[row["document_id"]]["decade_sample_rank"] = rank
            frame_by_document_id[row["document_id"]]["decade_batch_id"] = batch_id
            sample_row = dict(row)
            sample_row["selected_for_decade_comparison"] = 1
            sample_row["decade_sample_rank"] = rank
            sample_row["decade_batch_id"] = batch_id
            decade_sample_rows.append(sample_row)

    write_csv(Path("../output/ulurp_cpc_llm_exploration_frame.csv"), frame_rows, FRAME_COLUMNS)
    write_prompt_template(Path("../output/ulurp_cpc_revision_narrative_prompt.md"), REVISION_PROMPT_HEADER)
    write_prompt_template(Path("../output/ulurp_cpc_decade_comparison_prompt.md"), DECADE_PROMPT_HEADER)

    write_csv(
        Path("../output/ulurp_cpc_decade_comparison_sample.csv"),
        decade_sample_rows,
        FRAME_COLUMNS,
    )

    first_revision_prompt = ""
    with Path("../output/ulurp_cpc_revision_narrative_batches.jsonl").open("w", encoding="utf-8") as output_file:
        for batch_rows in chunks(selected_revision_rows, revision_batch_size):
            batch_prompt = build_revision_prompt(batch_rows, text_by_document_id, max_revision_chars)
            if not first_revision_prompt:
                first_revision_prompt = batch_prompt
            output_file.write(json.dumps({
                "batch_id": batch_rows[0]["revision_batch_id"],
                "prompt_version": prompt_version,
                "task": "ulurp_cpc_revision_narrative",
                "report_count": len(batch_rows),
                "document_ids": [row["document_id"] for row in batch_rows],
                "prompt": batch_prompt,
            }, ensure_ascii=True) + "\n")

    Path("../output/ulurp_cpc_revision_narrative_next_batch.md").write_text(
        first_revision_prompt,
        encoding="utf-8",
    )

    decade_sample_rows_by_decade = defaultdict(list)
    for row in decade_sample_rows:
        decade_sample_rows_by_decade[row["decade"]].append(row)

    with Path("../output/ulurp_cpc_decade_comparison_batches.jsonl").open("w", encoding="utf-8") as output_file:
        for decade in sorted(decade_sample_rows_by_decade):
            decade_rows = sorted(
                decade_sample_rows_by_decade[decade],
                key=lambda row: int(row["decade_sample_rank"]),
            )
            batch_prompt = build_decade_prompt(
                decade,
                decade_rows,
                text_by_document_id,
                max_decade_snippet_chars,
            )
            output_file.write(json.dumps({
                "batch_id": f"decade_{prompt_version}_{decade}",
                "prompt_version": prompt_version,
                "task": "ulurp_cpc_decade_comparison",
                "decade": decade,
                "report_count": len(decade_rows),
                "document_ids": [row["document_id"] for row in decade_rows],
                "prompt": batch_prompt,
            }, ensure_ascii=True) + "\n")


if __name__ == "__main__":
    main()
