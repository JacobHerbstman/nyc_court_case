#!/usr/bin/env python3
"""Freeze a narrow acceptance test using unchanged v6 source text."""

import csv
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# Run from tasks/audits/pilot_ulurp_cpc_llm_labels/code.
codebook = json.loads(Path("cpc_jev_codebook_v7.json").read_text())
prior = [json.loads(s) for s in Path("../output/cpc_jev_requests_v6.jsonl").read_text().splitlines()]
assert len(prior) == len({p["request_id"] for p in prior})
prior = {p["request_id"]: p for p in prior}
with open("../output/cpc_jev_controls_v6.csv", newline="") as source:
    previous_controls = list(csv.DictReader(source))
packets, controls = [], []
for case in codebook["cases"]:
    for context in ("excerpt", "full"):
        original = prior[case[context + "_request"]]
        metadata = next(r for r in previous_controls if r["case_id"] == case["case_id"] and r["context"] == context)
        request_id = f"{case['case_id']}_{context}_acceptance"
        questions = {}
        for q in case["questions"]:
            instructions = q["question"] if "question" in q else codebook["acceptance_question"].format(actor=q["actor"], action=q["action"])
            question_id = q["id"]
            assert q["reference_quote"] in prior[case["full_request"]]["body"]["state"]["report"]["text"]
            questions[question_id] = dict(type="noul", instructions=instructions,
                criteria=q["criteria"] if "criteria" in q else codebook["acceptance_criteria"])
            controls.append(dict(case_id=case["case_id"], proposition=q["id"], document_id=metadata["document_id"],
                source_document_id=metadata["source_document_id"], source_application_number=metadata["source_application_number"],
                context=context, primitive="noul", request_id=request_id, question_id=question_id,
                question=instructions, expected=q["expected_" + context], expectation_provenance=metadata["expectation_provenance"],
                reference_page=q["reference_page"], reference_quote=q["reference_quote"], reason=q["reason"],
                excerpt_passage_ids=metadata["excerpt_passage_ids"], supplied_pdf_pages=metadata["supplied_pdf_pages"],
                source_text_sha256=metadata["source_text_sha256"], context_sha256=metadata["context_sha256"]))
        body = dict(model="typesafe-ai/jev", state=original["body"]["state"], questions=questions)
        assert not any(q == old for q in questions.values() for old in original["body"]["questions"].values()), "No identical question/state repeat."
        packets.append(dict(request_id=request_id, document_id=original["document_id"],
            request_sha256=hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest(), body=body))
assert len(packets) == len({p["request_sha256"] for p in packets})
with open("../output/cpc_jev_requests_v7.jsonl", "w") as output:
    for p in packets:
        output.write(json.dumps(p, ensure_ascii=False) + "\n")
save_csv(controls, list(controls[0]), "../output/cpc_jev_controls_v7.csv", ["case_id", "proposition", "context", "primitive"])
print(f"Prepared {len(packets)} requests and {len(controls)} acceptance/timing checks; source text is unchanged.")
