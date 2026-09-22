#!/usr/bin/env python3
"""Freeze a three-stage topic test on prior cases and additional positives."""

import csv
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# Run from tasks/audits/pilot_ulurp_cpc_llm_labels/code.
# additional_reports = 10
# positive_target = 4
# sample_seed = "jev-topics-v4-20260919"
additional_reports, positive_target = map(int, sys.argv[1:3])
sample_seed = sys.argv[3]
codebook = json.loads(Path("cpc_jev_codebook_v4.json").read_text())
prior = [json.loads(s) for s in Path("../output/cpc_jev_requests_v2.jsonl").read_text().splitlines()]
prior = {p["document_id"]: p for p in prior if p["repeat"] == 1}
with open("../output/cpc_jev_sample_v3.csv", newline="") as source:
    old_sample = list(csv.DictReader(source))
with open("../input/ulurp_cpc_reconciled_coding.csv", newline="") as source:
    humans = list(csv.DictReader(source))
assert len(humans) == len({(r["source_document_id"], r["field"]) for r in humans})
selected = {r["document_id"] for r in old_sample}
assert len(selected) == len(old_sample) == 20 and selected <= prior.keys()
positives = {d: {r["field"] for r in humans if r["source_document_id"] == d and r["field"] in codebook["legacy_topics"]
    and r["human_status"] in {"human_agreement", "single_human_coder"} and r["human_value"] == "1"} for d in prior}
source_ids = {d: {p["source_document_id"] for p in q["body"]["state"]["pages"]} for d, q in prior.items()}
used_sources = set().union(*(source_ids[d] for d in selected))
additional, counts = set(), Counter()
for _ in range(additional_reports):
    candidates = sorted([d for d in prior if d not in selected and not source_ids[d] & used_sources],
        key=lambda d: hashlib.sha256(f"{sample_seed}|{d}".encode()).hexdigest())
    assert candidates, "Insufficient distinct-source candidate reports."
    chosen = max(candidates, key=lambda d: sum(counts[f] < positive_target for f in positives[d]))
    selected.add(chosen)
    additional.add(chosen)
    counts.update(positives[chosen])
    used_sources.update(source_ids[chosen])

packets, roster, passages = [], [], []
for doc, original in prior.items():
    cohort = "additional_topics" if doc in additional else "prior_v3" if doc in selected else "not_selected"
    roster.append(dict(document_id=doc, application_number=original["body"]["state"]["focal"]["application_number"],
        cohort=cohort, original_positive_fields=";".join(sorted(positives[doc])),
        selection_reason="prior twenty retained" if cohort == "prior_v3" else "positive coverage with distinct sources" if cohort == "additional_topics" else "outside fixed test size",
        prior_request_sha256=original["request_sha256"]))
    if doc not in selected:
        continue
    report_passages = []
    for page in original["body"]["state"]["pages"]:
        start, part, chunks = 0, 0, []
        while start < len(page["text"]):
            end = min(start + 1800, len(page["text"]))
            if end < len(page["text"]):
                boundary = page["text"].rfind(" ", start + 1, end)
                if boundary > start:
                    end = boundary
            part += 1
            text = page["text"][start:end]
            report_passages.append(dict(document_id=doc, passage_id=f"{page['source_document_id'][:8]}p{page['pdf_page']}b{part}",
                source_document_id=page["source_document_id"], source_application_number=page["application_number"], source_role=page["role"],
                pdf_page=page["pdf_page"], page_start=start, page_end=end, source_text_sha256=page["source_text_sha256"],
                passage_text=text, passage_sha256=hashlib.sha256(text.encode()).hexdigest()))
            chunks.append(text)
            start = end
        assert "".join(chunks) == page["text"]
    assert len(report_passages) == len({p["passage_id"] for p in report_passages})
    options = {p["passage_id"]: None for p in report_passages}
    options["none"] = "No supplied passage establishes this particular topic/stage."
    questions = {f"{topic}__{stage}": dict(type="choice", instructions=dict(scope=codebook["scope"],
        question=definition.format(topic=description)), criteria=options)
        for topic, description in codebook["topics"].items() for stage, definition in codebook["stages"].items()}
    state = dict(focal=original["body"]["state"]["focal"], passages=[{k: p[k] for k in
        ("passage_id", "source_document_id", "source_application_number", "source_role", "pdf_page", "passage_text")} for p in report_passages])
    body = dict(model="typesafe-ai/jev", state=state, questions=questions)
    packets.append(dict(request_id=f"{doc}_topics", document_id=doc,
        request_sha256=hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest(), body=body))
    passages.extend(report_passages)
assert len(packets) == 20 + additional_reports
with open("../output/cpc_jev_requests_v4.jsonl", "w") as output:
    for p in packets:
        output.write(json.dumps(p, ensure_ascii=False) + "\n")
save_csv(roster, list(roster[0]), "../output/cpc_jev_sample_v4.csv", ["document_id"])
save_csv(passages, list(passages[0]), "../output/cpc_jev_passages_v4.csv", ["document_id", "passage_id"])
print(f"Prepared {len(packets)} distinct requests, 24 questions each. Additional original positives: {dict(counts)}.")
