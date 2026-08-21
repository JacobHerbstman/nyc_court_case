#!/usr/bin/env python3

import csv
import hashlib
import json
import re
import sys
from collections import defaultdict
from pathlib import Path


# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/pilot_ulurp_cpc_llm_labels/code")
# variant = "sol_medium_v2"
# model = "gpt-5.6-sol"
# reasoning_effort = "medium"
# max_output_tokens = 6000
# pilot_documents = 20
# sample_seed = "ulurp-cpc-llm-pilot-v1"


PAGE_HEADER = re.compile(
    r"(?i)^\s*(?:page\s+)?\d+\s+(?:C\s*)?\d{6}(?:\s*\([A-Z]\))?\s*[A-Z]{2,4}\s*$"
)


def normalize_narrative(text):
    kept_lines = []
    for line in text.replace("\f", "\n").splitlines():
        stripped = line.strip()
        if not stripped or re.fullmatch(r"[_\-]{10,}", stripped):
            continue
        if PAGE_HEADER.fullmatch(stripped):
            continue
        kept_lines.append(stripped)
    return re.sub(r"\s+", " ", " ".join(kept_lines)).strip().lower()


cli_args = sys.argv[1:]
if len(cli_args) != 6:
    raise SystemExit(
        "Usage: python3 build_ulurp_cpc_llm_pilot_requests.py "
        "<variant> <model> <reasoning_effort> <max_output_tokens> "
        "<pilot_documents> <sample_seed>"
    )

variant = cli_args[0]
model = cli_args[1]
reasoning_effort = cli_args[2]
max_output_tokens = int(cli_args[3])
pilot_documents = int(cli_args[4])
sample_seed = cli_args[5]

if variant != "sol_medium_v2":
    raise SystemExit("Invalid pilot variant.")
if reasoning_effort not in {"none", "low", "medium", "high", "xhigh", "max"}:
    raise SystemExit("Invalid reasoning effort.")
if max_output_tokens < 1:
    raise SystemExit("MAX_OUTPUT_TOKENS must be positive.")
if pilot_documents < 1:
    raise SystemExit("PILOT_DOCUMENTS must be positive.")

with open("../input/ulurp_cpc_training_labels_jacob.csv", newline="", encoding="utf-8-sig") as input_file:
    human_rows = list(csv.DictReader(input_file))

with open("../input/official_ulurp_cpc_narrative_manifest.csv", newline="", encoding="utf-8-sig") as input_file:
    narrative_rows = list(csv.DictReader(input_file))

narrative_by_document_id = {
    row["document_id"]: row
    for row in narrative_rows
    if row["analysis_narrative_unit_flag"] == "TRUE"
}

candidates = [
    row
    for row in human_rows
    if row["sample_group"] == "jacob_only"
    and row["coding_complete"] == "1"
    and row["document_id"] in narrative_by_document_id
]

if len(candidates) < pilot_documents:
    raise SystemExit(f"Only {len(candidates)} completed jacob_only reports are available.")

rows_by_decade = defaultdict(list)
for row in candidates:
    decade = f"{int(row['vote_year']) // 10 * 10}s"
    row["pilot_decade"] = decade
    row["pilot_rank"] = hashlib.sha256(
        f"{sample_seed}|{row['document_id']}".encode("utf-8")
    ).hexdigest()
    rows_by_decade[decade].append(row)

for rows in rows_by_decade.values():
    rows.sort(key=lambda row: row["pilot_rank"])

selected_rows = []
while len(selected_rows) < pilot_documents:
    added = False
    for decade in sorted(rows_by_decade):
        if rows_by_decade[decade] and len(selected_rows) < pilot_documents:
            selected_rows.append(rows_by_decade[decade].pop(0))
            added = True
    if not added:
        break

selected_rows.sort(key=lambda row: (row["pilot_decade"], row["pilot_rank"]))

prompt = Path("cpc_llm_prompt.txt").read_text(encoding="utf-8").strip()
schema = json.loads(Path("cpc_llm_schema.json").read_text(encoding="utf-8"))

with open(
    f"../output/ulurp_cpc_llm_pilot_requests_{variant}.jsonl",
    "w",
    encoding="utf-8",
) as output_file:
    for human_row in selected_rows:
        narrative_row = narrative_by_document_id[human_row["document_id"]]
        text_path = Path(narrative_row["local_text_path"])
        if not text_path.is_file():
            raise SystemExit(f"Missing source text for {human_row['document_id']}: {text_path}")

        full_text = text_path.read_text(encoding="utf-8", errors="replace")
        if hashlib.sha256(full_text.encode("utf-8")).hexdigest() != narrative_row["source_text_sha256"]:
            raise SystemExit(f"Source text hash changed for {human_row['document_id']}.")

        narrative_start = int(narrative_row["narrative_start_char"])
        narrative_end = int(narrative_row["narrative_end_char"])
        narrative = full_text[narrative_start:narrative_end]
        if (
            hashlib.sha256(normalize_narrative(narrative).encode("utf-8")).hexdigest()
            != narrative_row["narrative_sha256"]
        ):
            raise SystemExit(f"Narrative hash changed for {human_row['document_id']}.")

        first_pdf_page = full_text[:narrative_start].count("\f") + 1
        page_blocks = []
        for page_offset, page_text in enumerate(narrative.split("\f")):
            if page_text.strip():
                page_blocks.append(
                    f"[PDF PAGE {first_pdf_page + page_offset}]\n{page_text.strip()}"
                )

        report_input = "\n".join(
            [
                "Document metadata",
                f"document_id: {human_row['document_id']}",
                f"application_number: {human_row['application_number']}",
                f"project_name: {human_row['project_name']}",
                f"vote_year: {human_row['vote_year']}",
                f"action_code: {human_row['action_code']}",
                f"community_district: {human_row['community_district']}",
                f"official_pdf_url: {narrative_row['official_pdf_url']}",
                "",
                "Report narrative",
                "\n\n".join(page_blocks),
            ]
        )

        request = {
            "custom_id": human_row["document_id"],
            "method": "POST",
            "url": "/v1/responses",
            "body": {
                "model": model,
                "reasoning": {"effort": reasoning_effort},
                "input": [
                    {"role": "developer", "content": prompt},
                    {"role": "user", "content": report_input},
                ],
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": "ulurp_cpc_report_labels",
                        "strict": True,
                        "schema": schema,
                    }
                },
                "max_output_tokens": max_output_tokens,
            },
        }
        output_file.write(json.dumps(request, ensure_ascii=True, separators=(",", ":")) + "\n")

print(
    f"Wrote {len(selected_rows)} requests across "
    f"{len({row['pilot_decade'] for row in selected_rows})} decades."
)
