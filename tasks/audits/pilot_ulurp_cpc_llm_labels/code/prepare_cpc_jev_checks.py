#!/usr/bin/env python3
"""Check all positive v4 issue passages, with context and separate source scope."""

import csv
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# Run from tasks/audits/pilot_ulurp_cpc_llm_labels/code.
codebook = json.loads(Path("cpc_jev_codebook_v5.json").read_text())
topics = json.loads(Path("cpc_jev_codebook_v4.json").read_text())["topics"]
with open("../output/cpc_jev_evidence_v4.csv", newline="") as source:
    evidence = list(csv.DictReader(source))
with open("../output/cpc_jev_passages_v4.csv", newline="") as source:
    passages = list(csv.DictReader(source))
assert len(evidence) == len({(r["document_id"], r["topic"], r["stage"]) for r in evidence})
assert len(passages) == len({(r["document_id"], r["passage_id"]) for r in passages})
prior = [json.loads(s) for s in Path("../output/cpc_jev_requests_v4.jsonl").read_text().splitlines()]
assert len(prior) == len({p["document_id"] for p in prior})
prior = {p["document_id"]: p for p in prior}
positive = [r for r in evidence if r["value"] == "1" and r["stage"] != "discussed"]
packets, claims = [], []
for doc in sorted({r["document_id"] for r in positive}):
    rows = [r for r in positive if r["document_id"] == doc]
    source_ids = {r["source_document_id"] for r in rows} | {doc}
    sources, windows, questions = [], [], {}
    for source_id in sorted(source_ids):
        source_passages = [p for p in passages if p["document_id"] == doc and p["source_document_id"] == source_id]
        source_passages.sort(key=lambda p: (int(p["pdf_page"]), int(p["page_start"])))
        introductions = [p for p in source_passages if int(p["pdf_page"]) <= 2]
        sources.append(dict(source_document_id=source_id, application_number=source_passages[0]["source_application_number"],
            introduction=[{k: p[k] for k in ("passage_id", "pdf_page", "passage_text")} for p in introductions]))
        if source_id != doc:
            questions[f"scope_{source_id}"] = dict(type="choice", instructions=dict(
                question=codebook["scope_question"], focal_source=doc, candidate_source=source_id), criteria=codebook["scope_choices"])
    for passage_id in sorted({r["selected_passage_id"] for r in rows}):
        selected = next(p for p in passages if p["document_id"] == doc and p["passage_id"] == passage_id)
        source_passages = [p for p in passages if p["document_id"] == doc and p["source_document_id"] == selected["source_document_id"]]
        source_passages.sort(key=lambda p: (int(p["pdf_page"]), int(p["page_start"])))
        index = source_passages.index(selected)
        context = source_passages[max(0, index - 1):index + 2]
        windows.append(dict(window_id=passage_id, source_document_id=selected["source_document_id"],
            application_number=selected["source_application_number"],
            passages=[{k: p[k] for k in ("passage_id", "pdf_page", "passage_text")} for p in context]))
        for topic in sorted({r["topic"] for r in rows if r["selected_passage_id"] == passage_id}):
            question_id = f"event_{passage_id}_{topic}"
            questions[question_id] = dict(type="choice", instructions=dict(question=codebook["event_question"],
                window_id=passage_id, topic=topics[topic], boundary=codebook["topic_boundaries"][topic]), criteria=codebook["event_choices"])
            for r in rows:
                if r["selected_passage_id"] == passage_id and r["topic"] == topic:
                    claims.append(dict(r, request_id=f"{doc}_checks", event_question_id=question_id,
                        scope_question_id="" if selected["source_document_id"] == doc else f"scope_{selected['source_document_id']}",
                        context_passage_ids=";".join(p["passage_id"] for p in context)))
    body = dict(model="typesafe-ai/jev", state=dict(focal=prior[doc]["body"]["state"]["focal"],
        source_introductions=sources, evidence_windows=windows), questions=questions)
    assert len(json.dumps(body["state"], ensure_ascii=False)) < 90000
    packets.append(dict(request_id=f"{doc}_checks", document_id=doc,
        request_sha256=hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest(), body=body))
assert len(claims) == len(positive)
with open("../output/cpc_jev_requests_v5.jsonl", "w") as output:
    for p in packets:
        output.write(json.dumps(p, ensure_ascii=False) + "\n")
save_csv(claims, list(claims[0]), "../output/cpc_jev_claims_v5.csv", ["document_id", "topic", "stage"])
print(f"Prepared {len(packets)} report requests checking {len(claims)} claims; {sum(len(p['body']['questions']) for p in packets)} questions.")
