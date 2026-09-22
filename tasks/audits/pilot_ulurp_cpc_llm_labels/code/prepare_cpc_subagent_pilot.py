#!/usr/bin/env python3
"""Freeze blinded, page-marked inputs for a small Codex Sol reading pilot."""

import csv
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/pilot_ulurp_cpc_llm_labels/code")
# reports_per_group = 10
# sample_seed = "cpc-subagent-v3-20260915"
reports_per_group, sample_seed = sys.argv[1:]
reports_per_group = int(reports_per_group)
assert reports_per_group >= 1

with open("../input/ulurp_cpc_text_labels.csv", newline="") as source:
    narratives = list(csv.DictReader(source))
with open("../input/ulurp_cpc_narrative_sources.csv", newline="") as source:
    source_rows = list(csv.DictReader(source))
with open("../input/ulurp_cpc_report_manifest.csv", newline="") as source:
    manifest_rows = list(csv.DictReader(source))
with open("../input/ulurp_cpc_human_coding.csv", newline="") as source:
    human_rows = list(csv.DictReader(source))
with open("cpc_subagent_exclusions_v3.csv", newline="") as source:
    old_pilot_ids = {row["document_id"] for row in csv.DictReader(source)}
assert len(narratives) == len({r["document_id"] for r in narratives})
assert len(manifest_rows) == len({r["document_id"] for r in manifest_rows})
manifest = {r["document_id"]: r for r in manifest_rows}
sources_by_report = defaultdict(list)
for row in source_rows:
    sources_by_report[row["document_id"]].append(row)
completed_coders = defaultdict(set)
for row in human_rows:
    for coder in ("jacob", "tyler"):
        if row[coder + "_value"] and row[coder + "_coding_complete"] == "1":
            completed_coders[row["source_document_id"]].add(coder)

# Exclude the old pilot's source bundles and avoid shared sources within this sample.
used_sources = {r["source_document_id"] for doc in old_pilot_ids for r in sources_by_report[doc]}
candidates = defaultdict(lambda: defaultdict(list))
for row in narratives:
    coders = completed_coders[row["document_id"]]
    if not coders:
        continue
    group = "both" if len(coders) == 2 else next(iter(coders))
    decade = int(row["year"]) // 10 * 10
    rank = hashlib.sha256(f"{sample_seed}|{row['document_id']}".encode()).hexdigest()
    candidates[group][decade].append((rank, row))
for decades in candidates.values():
    for rows in decades.values():
        rows.sort(key=lambda item: item[0])

selected = []
for group in ("jacob", "tyler", "both"):
    count = 0
    while count < reports_per_group:
        added = False
        for decade, rows in sorted(candidates[group].items()):
            while rows:
                _, row = rows.pop(0)
                source_ids = {r["source_document_id"] for r in sources_by_report[row["document_id"]]}
                if source_ids & used_sources:
                    continue
                selected.append(dict(row, human_group=group))
                used_sources.update(source_ids)
                count += 1
                added = True
                break
            if count == reports_per_group:
                break
        assert added, f"Insufficient disjoint source bundles for {group}"

prompt = Path("cpc_subagent_prompt_v3.txt").read_text()
schema = json.loads(Path("cpc_subagent_schema_v3.json").read_text())
packets = {}
sample = []
for index, row in enumerate(selected):
    doc = row["document_id"]
    sources = []
    for link in sorted(sources_by_report[doc], key=lambda r: (r["source_document_id"] != doc, r["source_document_id"])):
        if link["text_included_flag"] != "TRUE":
            continue
        original = manifest[link["source_document_id"]]
        source_path = Path("../../../build_ulurp_cpc_report_corpus/code") / original["local_text_path"]
        text = source_path.read_text(encoding="utf-8")
        assert hashlib.sha256(text.encode()).hexdigest() == link["source_text_sha256"]
        pages = [{"pdf_page": p, "text": " ".join(t.split())}
                 for p, t in enumerate(text.split("\f"), 1) if t.strip()]
        sources.append(dict(source_document_id=link["source_document_id"],
            application_number=link["source_application_number"], role=link["link_role"],
            source_text_sha256=link["source_text_sha256"], pages=pages))
    assert sources and sources[0]["source_document_id"] == doc
    metadata = {k: row[k] for k in ("document_id", "application_number", "project_name", "year")}
    metadata["sources"] = sources
    body = dict(model="gpt-5.6-sol", reasoning={"effort": "medium"},
        input=[{"role": "developer", "content": prompt},
               {"role": "user", "content": json.dumps(metadata, ensure_ascii=False)}],
        text={"format": {"type": "json_schema", "name": "cpc_labels_v3", "strict": True, "schema": schema}},
        max_output_tokens=9000)
    request = dict(custom_id=doc, method="POST", url="/v1/responses", body=body)
    fingerprint = hashlib.sha256(json.dumps(request, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    packets[doc] = dict(request=request, request_sha256=fingerprint)
    sample.append(dict(document_id=doc, application_number=row["application_number"],
        year=row["year"], human_group=row["human_group"], action_code=row["action_code"],
        primary_reader="abc"[index % 3], source_count=len(sources),
        source_words=sum(len(p["text"].split()) for s in sources for p in s["pages"]),
        request_sha256=fingerprint))

# Each reader receives ten primary reports and one report assigned to another reader.
for index, reader in enumerate("abc"):
    primary = [r["document_id"] for r in sample if r["primary_reader"] == reader]
    repeated = next(r["document_id"] for r in sample if r["primary_reader"] == "abc"[(index + 1) % 3])
    with open(f"../output/cpc_subagent_reader_{reader}_v3.jsonl", "w") as output:
        for doc in primary + [repeated]:
            output.write(json.dumps(packets[doc], ensure_ascii=False) + "\n")
save_csv(sample, list(sample[0]), "../output/cpc_subagent_sample_v3.csv", ["document_id"])
print(f"Prepared {len(sample)} reports and 3 repeat readings; {sum(r['source_words'] for r in sample):,} primary source words.")
