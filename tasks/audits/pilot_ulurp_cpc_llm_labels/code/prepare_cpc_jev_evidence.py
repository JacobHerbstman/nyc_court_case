#!/usr/bin/env python3
"""Prepare passage selection, then a distinct check of selected evidence."""

import csv
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# Run from tasks/audits/pilot_ulurp_cpc_llm_labels/code.
stage = sys.argv[1]
assert stage in {"retrieve", "verify"}
codebook = json.loads(Path("cpc_jev_codebook_v3.json").read_text())
packets = []
if stage == "retrieve":
    additional_reports = int(sys.argv[2])
    prior = [json.loads(s) for s in Path("../output/cpc_jev_requests_v2.jsonl").read_text().splitlines()]
    prior = {p["body"]["state"]["focal"]["application_number"]: p for p in prior if p["repeat"] == 1}
    with open("cpc_jev_evidence_sample_v3.csv", newline="") as source:
        selected = list(csv.DictReader(source))
    assert len(selected) == len({r["application_number"] for r in selected}) == 12
    with open("../input/ulurp_cpc_reconciled_coding.csv", newline="") as source:
        humans = list(csv.DictReader(source))
    assert len(humans) == len({(r["source_document_id"], r["field"]) for r in humans})
    topics = {f: 5 for f in ("affordability_displacement", "traffic_parking", "scale_character_preservation", "infrastructure_services", "environment_open_space")}
    topics.update({f + ":" + position: 1 for f in codebook["position_fields"] for position in ("support_or_request", "opposition")})
    positives = {app: set() for app in prior}
    applications = {p["document_id"]: app for app, p in prior.items()}
    for row in humans:
        if row["source_document_id"] not in applications or row["human_status"] not in {"human_agreement", "single_human_coder"}:
            continue
        target = row["field"] if row["human_value"] == "1" else row["field"] + ":" + row["human_value"]
        if target in topics:
            positives[applications[row["source_document_id"]]].add(target)
    for _ in range(additional_reports):
        selected_apps = {r["application_number"] for r in selected}
        counts = Counter(t for app in selected_apps for t in positives[app])
        candidates = sorted(set(prior) - selected_apps, key=lambda app: hashlib.sha256(f"jev-evidence-v3|{app}".encode()).hexdigest())
        chosen = max(candidates, key=lambda app: (sum(":" in t and counts[t] < topics[t] for t in positives[app]),
            sum(counts[t] < topics[t] for t in positives[app])))
        selected.append(dict(application_number=chosen, selection_reason="additional original positive coverage: " + ";".join(sorted(positives[chosen]))))
    passages, sample = [], []
    for row in selected:
        original = prior[row["application_number"]]
        doc = original["document_id"]
        report_passages = []
        for page in original["body"]["state"]["pages"]:
            start, part = 0, 0
            page_passages = []
            while start < len(page["text"]):
                end = min(start + 1800, len(page["text"]))
                if end < len(page["text"]):
                    boundary = page["text"].rfind(" ", start + 1, end)
                    if boundary > start:
                        end = boundary
                part += 1
                text = page["text"][start:end]
                passage = dict(document_id=doc, passage_id=f"{page['source_document_id'][:8]}p{page['pdf_page']}b{part}",
                    source_document_id=page["source_document_id"], source_application_number=page["application_number"],
                    source_role=page["role"], pdf_page=page["pdf_page"], page_start=start, page_end=end,
                    source_text_sha256=page["source_text_sha256"], passage_text=text,
                    passage_sha256=hashlib.sha256(text.encode()).hexdigest())
                report_passages.append(passage)
                page_passages.append(text)
                start = end
            assert "".join(page_passages) == page["text"], "No text may be omitted or duplicated."
        assert len(report_passages) == len({p["passage_id"] for p in report_passages})
        options = {p["passage_id"]: None for p in report_passages}
        options["none"] = "No supplied passage establishes the event."
        questions = {f: dict(type="choice", instructions=dict(scope=codebook["scope"],
            question=q + " Choose the strongest single supporting passage ID, or none. Use neighboring passages to resolve context."), criteria=options)
            for f, q in codebook["questions"].items()}
        body = dict(model="typesafe-ai/jev", state=dict(focal=original["body"]["state"]["focal"], passages=[{k: p[k] for k in ("passage_id", "source_document_id", "source_application_number", "source_role", "pdf_page", "passage_text")} for p in report_passages]), questions=questions)
        packets.append(dict(request_id=f"{doc}_retrieve", document_id=doc, body=body))
        passages.extend(report_passages)
        sample.append(dict(document_id=doc, application_number=row["application_number"], selection_reason=row["selection_reason"],
            source_pages=len(original["body"]["state"]["pages"]), passage_count=len(report_passages),
            source_words=sum(len(p["text"].split()) for p in original["body"]["state"]["pages"]), prior_request_sha256=original["request_sha256"]))
    save_csv(passages, list(passages[0]), "../output/cpc_jev_passages_v3.csv", ["document_id", "passage_id"])
    save_csv(sample, list(sample[0]), "../output/cpc_jev_sample_v3.csv", ["document_id"])
else:
    retrieval = [json.loads(s) for s in Path("../input/cpc_jev_requests_v3_retrieve.jsonl").read_text().splitlines()]
    retrieval = {p["request_id"]: p for p in retrieval}
    assert Path("../input/cpc_jev_requests_v3_retrieve.jsonl").read_bytes() == Path("../output/cpc_jev_requests_v3_retrieve.jsonl").read_bytes()
    successful = {}
    for line in Path("../input/cpc_jev_responses_v3_retrieve.jsonl").read_text().splitlines():
        response = json.loads(line)
        if response["event"] == "received" and response["valid_response"]:
            assert response["request_id"] not in successful
            assert response["request_sha256"] == retrieval[response["request_id"]]["request_sha256"]
            successful[response["request_id"]] = response
    for request_id, response in successful.items():
        original = retrieval[request_id]
        answers = json.loads(response["raw_response"])["answers"]
        assert set(answers) == set(codebook["questions"])
        candidates, questions, context_passages = {}, {}, {}
        passages = original["body"]["state"]["passages"]
        for field, answer in answers.items():
            choice = answer["choice"]
            assert choice in original["body"]["questions"][field]["criteria"]
            if choice == "none":
                continue
            index = next(i for i, p in enumerate(passages) if p["passage_id"] == choice)
            selected = passages[index]
            context = [p for p in passages[max(0, index - 1):index + 2] if p["source_document_id"] == selected["source_document_id"]]
            candidates[field] = dict(selected=choice, neighboring_passages=[p["passage_id"] for p in context])
            context_passages.update({p["passage_id"]: p for p in context})
            questions[field] = dict(type="choice", instructions=dict(scope=codebook["scope"], rule=codebook["verification"],
                question=f"Inspect candidates.{field}; its passage IDs refer to passages in the state. " + codebook["questions"][field]),
                criteria={"supported": "The selected passage and its context establish the event.",
                    "not_supported": "They do not establish the event under the stated definition.", "unclear": "A material relationship remains unresolved."})
            if field in codebook["position_fields"]:
                questions[field]["instructions"]["rule"] = "Classify only the specified actor’s stance established by the selected passage and neighboring context. Return opposition when any such actor opposes any part; otherwise support_or_request, none_or_procedural, or unclear."
                questions[field]["criteria"] = {"support_or_request": "The specified actor supports or requests without opposing.",
                    "opposition": "The specified actor opposes any part.", "none_or_procedural": "No substantive position by the specified actor is established.",
                    "unclear": "Actor or stance is unresolved in this excerpt."}
        if not questions:
            continue
        body = dict(model="typesafe-ai/jev", state=dict(focal=original["body"]["state"]["focal"], candidates=candidates, passages=context_passages), questions=questions)
        packets.append(dict(request_id=f"{original['document_id']}_verify", document_id=original["document_id"], body=body))
    assert packets, "No passages were selected for a verification call."

with open(f"../output/cpc_jev_requests_v3_{stage}.jsonl", "w") as output:
    for packet in packets:
        packet["request_sha256"] = hashlib.sha256(json.dumps(packet["body"], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        output.write(json.dumps(packet, ensure_ascii=False) + "\n")
print(f"Prepared {len(packets)} distinct {stage} requests; no repeated readings.")
