#!/usr/bin/env python3
"""Freeze short-question controls before observing answers."""

import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# Run from tasks/audits/pilot_ulurp_cpc_llm_labels/code.
codebook = json.loads(Path("cpc_jev_codebook_v6.json").read_text())
prior = [json.loads(s) for s in Path("../output/cpc_jev_requests_v5.jsonl").read_text().splitlines()]
prior = {p["document_id"]: p for p in prior}
bundles = [json.loads(s) for s in Path("../output/cpc_jev_requests_v2.jsonl").read_text().splitlines()]
bundles = {p["document_id"]: p for p in bundles if p["repeat"] == 1}
packets, controls = {}, []
for case in codebook["cases"]:
    doc = case["document_id"]
    window = next(w for w in prior[doc]["body"]["state"]["evidence_windows"] if w["window_id"] == case["window_id"])
    source_id = window["source_document_id"]
    pages = [p for p in bundles[doc]["body"]["state"]["pages"] if p["source_document_id"] == source_id]
    assert len(pages) == len({p["pdf_page"] for p in pages})
    pages.sort(key=lambda p: int(p["pdf_page"]))
    for question in case["questions"]:
        reference = next(p["text"] for p in pages if int(p["pdf_page"]) == question["reference_page"])
        assert question["reference_quote"] in reference, (case["case_id"], question["id"])
    contexts = {"excerpt": [(p["pdf_page"], p["passage_text"]) for p in window["passages"]],
        "full": [(p["pdf_page"], p["text"]) for p in pages]}
    if "guided_page" in case:
        contexts["guided"] = [(p["pdf_page"], p["text"]) for p in pages if int(p["pdf_page"]) == case["guided_page"]]
    for context, segments in contexts.items():
        request_id = f"{source_id}_full" if context == "full" else f"{case['case_id']}_{context}"
        state = dict(report=dict(application_number=window["application_number"],
            text="\n\n".join(f"PDF page {page}\n{text}" for page, text in segments)))
        if request_id not in packets:
            packets[request_id] = dict(request_id=request_id, document_id=doc,
                body=dict(model="typesafe-ai/jev", state=state, questions={}))
        assert packets[request_id]["body"]["state"] == state
        for question in case["questions"]:
            for primitive in ("choice", "noul"):
                question_id = f"{case['case_id']}__{question['id']}__{primitive}"
                assert question_id not in packets[request_id]["body"]["questions"]
                criteria = codebook["criteria"] if primitive == "choice" else {
                    "true": codebook["criteria"]["yes"], "false": codebook["criteria"]["no"]}
                packets[request_id]["body"]["questions"][question_id] = dict(type=primitive, instructions=question["question"], criteria=criteria)
                controls.append(dict(case_id=case["case_id"], proposition=question["id"], document_id=doc,
                    source_document_id=source_id, source_application_number=window["application_number"],
                    context=context, primitive=primitive, request_id=request_id, question_id=question_id,
                    question=question["question"], expected=question[f"expected_{context}"],
                    expectation_provenance="AI-authored diagnostic, frozen before calls; not an original human label",
                    reference_page=question["reference_page"], reference_quote=question["reference_quote"], reason=question["reason"],
                    excerpt_passage_ids=";".join(p["passage_id"] for p in window["passages"]),
                    supplied_pdf_pages=";".join(dict.fromkeys(str(p) for p, _ in segments)),
                    source_text_sha256=pages[0]["source_text_sha256"],
                    context_sha256=hashlib.sha256(state["report"]["text"].encode()).hexdigest()))
# Interleave short and full contexts in the fixed insertion order, so failures do
# not systematically leave all full reports until last. Identical bodies are forbidden.
with open("../output/cpc_jev_requests_v6.jsonl", "w") as output:
    for packet in packets.values():
        assert len(json.dumps(packet["body"], ensure_ascii=False)) < 110000
        packet["request_sha256"] = hashlib.sha256(json.dumps(packet["body"], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        output.write(json.dumps(packet, ensure_ascii=False) + "\n")
assert len(packets) == len({p["request_sha256"] for p in packets.values()})
save_csv(controls, list(controls[0]), "../output/cpc_jev_controls_v6.csv", ["case_id", "proposition", "context", "primitive"])
print(f"Prepared {len(packets)} requests and {len(controls)} diagnostic answers, with expectations frozen before calls.")
