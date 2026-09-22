#!/usr/bin/env python3
"""Freeze Council questions, source judgments and unchanged source text."""

import csv
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# Run from tasks/audits/pilot_ulurp_cpc_llm_labels/code.
# experiment = "v9"
experiment = sys.argv[1]
assert experiment in {"v9", "v10"}
book = json.loads(Path(f"cpc_jev_codebook_{experiment}.json").read_text())
prior = [json.loads(s) for s in Path("../output/cpc_jev_requests_v8.jsonl").read_text().splitlines()]
assert len(prior) == len({p["document_id"] for p in prior})
prior = {p["document_id"]: p for p in prior}
with open("../output/cpc_jev_pages_v8.csv", newline="") as f:
    previous_pages = list(csv.DictReader(f))
with open("../input/ulurp_cpc_report_manifest.csv", newline="") as f:
    manifest_rows = list(csv.DictReader(f))
with open("../input/ulurp_cpc_narrative_sources.csv", newline="") as f:
    links = list(csv.DictReader(f))
with open("../input/ulurp_cpc_reconciled_coding.csv", newline="") as f:
    humans = [r for r in csv.DictReader(f) if r["field"] == "councilmember_position"]
assert len(manifest_rows) == len({r["document_id"] for r in manifest_rows})
assert len(humans) == len({r["source_document_id"] for r in humans})
assert len(links) == len({(r["document_id"], r["source_document_id"]) for r in links})
manifest = {r["document_id"]: r for r in manifest_rows}
human = {r["source_document_id"]: r for r in humans}
packets, controls = [], []
for case in book["cases"]:
    doc = case["document_id"]
    if doc in prior:
        pages = [r for r in previous_pages if r["document_id"] == doc]
        full_state = prior[doc]["body"]["state"]
    else:
        pages = []
        for link in sorted([r for r in links if r["document_id"] == doc and r["text_included_flag"] == "TRUE"], key=lambda r: (r["source_document_id"] != doc, r["source_document_id"])):
            m = manifest[link["source_document_id"]]
            text = (Path("../../../build_ulurp_cpc_report_corpus/code") / m["local_text_path"]).read_text()
            assert hashlib.sha256(text.encode()).hexdigest() == link["source_text_sha256"]
            pages.extend(dict(page_id=f"{link['source_document_id']}:p{i}", source_document_id=link["source_document_id"],
                source_application_number=link["source_application_number"], source_role=link["link_role"], pdf_page=i,
                source_text_sha256=link["source_text_sha256"], public_pdf_url=m["resolved_pdf_url"], text=" ".join(t.split()))
                for i, t in enumerate(text.split("\f"), 1) if t.strip())
        full_state = dict(report=dict(application_number=manifest[doc]["application_number"],
            text="\n\n".join(f"Source {r['source_application_number']} ({r['source_role']}), page ID {r['page_id']}\n{r['text']}" for r in pages)))
    assert pages and len(pages) == len({p["page_id"] for p in pages})
    reference = next(p for p in pages if case["reference_quote"] in p["text"])
    assert set(case["excerpt_page_ids"]) <= {p["page_id"] for p in pages}
    for context in (["excerpt"] if case["excerpt_page_ids"] else []) + ["full"]:
        supplied = [p for p in pages if context == "full" or p["page_id"] in case["excerpt_page_ids"]]
        state = full_state if context == "full" else dict(report=dict(application_number=full_state["report"]["application_number"],
            text="\n\n".join(f"Source {r['source_application_number']} ({r['source_role']}), page ID {r['page_id']}\n{r['text']}" for r in supplied)))
        assert len(json.dumps(state, ensure_ascii=False)) <= 90000
        questions = {field: dict(type="noul", instructions=q["question"] + " " + book["scope"],
            criteria={k: q[k] for k in ("true", "false")}) for field, q in book["questions"].items()}
        body = dict(model="typesafe-ai/jev", state=state, questions=questions)
        request_id = f"{case['case_id']}_{context}_council"
        packets.append(dict(request_id=request_id, document_id=doc,
            request_sha256=hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest(), body=body))
        for field, question in questions.items():
            controls.append(dict(case_id=case["case_id"], proposition=field, document_id=doc,
                source_document_id=reference["source_document_id"], source_application_number=reference["source_application_number"],
                context=context, primitive="noul", request_id=request_id, question_id=field, question=question["instructions"],
                expected=case["expected"][field], expectation_provenance="codex_source_judgment_frozen_before_calls_not_human_gold",
                reference_page=reference["pdf_page"], reference_quote=case["reference_quote"], reason=case["reason"],
                excerpt_passage_ids=";".join(case["excerpt_page_ids"]), supplied_pdf_pages=";".join(p["page_id"] for p in supplied),
                source_text_sha256=reference["source_text_sha256"], context_sha256=hashlib.sha256(state["report"]["text"].encode()).hexdigest(),
                cohort=case["cohort"], focal_application_number=manifest[doc]["application_number"],
                original_council_code=human[doc]["human_value"], jacob_council_code=human[doc]["jacob_value"], tyler_council_code=human[doc]["tyler_value"],
                public_source_urls=";".join(sorted({p["public_pdf_url"] for p in supplied}))))
assert len(packets) == len({p["request_sha256"] for p in packets})
with open(f"../output/cpc_jev_requests_{experiment}.jsonl", "w") as f:
    for p in packets:
        f.write(json.dumps(p, ensure_ascii=False) + "\n")
save_csv(controls, list(controls[0]), f"../output/cpc_jev_controls_{experiment}.csv", ["case_id", "proposition", "context", "primitive"])
print(f"Frozen {len(packets)} requests, {len(controls)} Council checks; prior text and codes unchanged.")
