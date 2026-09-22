#!/usr/bin/env python3
"""Reuse blinded CPC source packets for a bounded Jev classification trial."""

import csv
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/pilot_ulurp_cpc_llm_labels/code")
# max_state_characters = 90000
max_state_characters, = map(int, sys.argv[1:])
assert max_state_characters > 10000
codebook = json.loads(Path("cpc_jev_codebook_v1.json").read_text())
with open("../output/cpc_subagent_sample_v3.csv", newline="") as source:
    sample = list(csv.DictReader(source))
assert len(sample) == len({r["document_id"] for r in sample}) == 30

questions = {}
for field, question in codebook["binary_fields"].items():
    questions[field] = dict(type="choice", instructions=dict(question=question, rules=codebook["rules"]),
        criteria={"1": "Documented under the definition.", "0": "Not documented in the supplied text.",
                  "unclear": "Genuinely contradictory or insufficient evidence; simple absence is 0."})
for field, definition in codebook["choice_fields"].items():
    questions[field] = dict(type="choice",
        instructions=dict(question=definition["question"], rules=codebook["rules"]),
        criteria=definition["criteria"])
schema = json.loads(Path("cpc_subagent_schema_v3.json").read_text())
subjective = {f for f, spec in schema["properties"]["labels"]["properties"].items() if "anyOf" not in spec}
assert set(questions) == subjective and len(questions) == 31

packets = {}
for reader in "abc":
    for line in Path(f"../output/cpc_subagent_reader_{reader}_v3.jsonl").read_text().splitlines():
        packet = json.loads(line)
        assert hashlib.sha256(json.dumps(packet["request"], sort_keys=True, ensure_ascii=False).encode()).hexdigest() == packet["request_sha256"]
        doc = packet["request"]["custom_id"]
        assert doc not in packets or packets[doc] == packet
        packets[doc] = packet
assert set(packets) == {r["document_id"] for r in sample}

requests, sample_rows = [], []
for row in sample:
    packet = packets[row["document_id"]]
    assert packet["request_sha256"] == row["request_sha256"]
    original = json.loads(packet["request"]["body"]["input"][1]["content"])
    focal = {k: original[k] for k in ("document_id", "application_number", "project_name", "year")}
    pages = [dict(source_document_id=s["source_document_id"], application_number=s["application_number"],
                  role=s["role"], source_text_sha256=s["source_text_sha256"], **p)
             for s in original["sources"] for p in s["pages"]]
    parts, current = [], []
    for page in pages:
        size = len(json.dumps(dict(focal=focal, part_number=len(pages), total_parts=len(pages), pages=current + [page]), ensure_ascii=False))
        if size > max_state_characters and current:
            parts.append(current)
            current = []
        assert len(json.dumps(dict(focal=focal, part_number=len(pages), total_parts=len(pages), pages=[page]), ensure_ascii=False)) <= max_state_characters
        current.append(page)
    parts.append(current)
    assert [p for part in parts for p in part] == pages, "Every original source page must be retained in order."
    for part_number, part in enumerate(parts, 1):
        state = dict(focal=focal, part_number=part_number, total_parts=len(parts), pages=part)
        assert len(json.dumps(state, ensure_ascii=False)) <= max_state_characters
        body = dict(model="typesafe-ai/jev", state=state, questions=questions)
        fingerprint = hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        requests.append(dict(request_id=f"{row['document_id']}_part{part_number}",
            document_id=row["document_id"], request_sha256=fingerprint, body=body))
    sample_rows.append(dict(document_id=row["document_id"], application_number=row["application_number"],
        year=row["year"], human_group=row["human_group"], source_count=len(original["sources"]),
        source_pages=len(pages), source_words=sum(len(p["text"].split()) for p in pages),
        request_parts=len(parts), report_comparison_status="whole_bundle" if len(parts) == 1 else "split_bundle_requires_joint_review",
        sol_request_sha256=packet["request_sha256"]))

with open("../output/cpc_jev_requests_v1.jsonl", "w") as output:
    for request in requests:
        output.write(json.dumps(request, ensure_ascii=False) + "\n")
save_csv(sample_rows, list(sample_rows[0]), "../output/cpc_jev_sample_v1.csv", ["document_id"])
split = sum(r["request_parts"] > 1 for r in sample_rows)
lines = ["# Jev trial prepared inputs", "",
    f"The existing 30-report Sol development sample is preserved: {sum(r['source_pages'] for r in sample_rows)} source pages, "
    f"{sum(r['source_words'] for r in sample_rows):,} source words, and 31 non-count questions per request.", "",
    f"Prepared {len(requests)} requests with a {max_state_characters:,}-character working state limit. "
    f"{split} long bundles are split at page boundaries; every source page is retained. "
    "The character limit is a planning rule, not an exact Jev token count. No source is truncated.", "",
    "Split-bundle answers remain part-level observations and require joint review before report-level comparison. "
    "The first trial returns classifications and probabilities, not verified supporting quotations. "
    "Counts remain in the existing regex/Codex workflow. No original human labels or predictions enter the requests.", "",
    "This preparation runs locally without inference. Live acquisition uses Vercel's TypeSafe-compatible endpoint and needs AI_GATEWAY_API_KEY. "
    "The gateway model name typesafe-ai/jev is an alias, not a pinned release; archive the returned model identifier. "
    "Normal Make builds never call the API. The sample is for development, not an untouched accuracy test."]
Path("../output/cpc_jev_preparation_v1.md").write_text("\n".join(lines) + "\n")
print(f"Prepared {len(requests)} Jev requests for 30 reports; {split} split bundles retained for joint review.")
