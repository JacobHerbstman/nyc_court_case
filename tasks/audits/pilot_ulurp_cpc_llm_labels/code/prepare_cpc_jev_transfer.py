#!/usr/bin/env python3
"""Freeze new source bundles and reusable topic/actor questions."""

import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# Run from tasks/audits/pilot_ulurp_cpc_llm_labels/code.
# report_count = 10
# coverage_target = 2
# max_state_characters = 90000
# sample_seed = "jev-transfer-v8-20260919"
report_count, coverage_target, max_state_characters = map(int, sys.argv[1:4])
sample_seed = sys.argv[4]
experiment = sys.argv[5] if len(sys.argv) == 6 else "v8"
assert experiment in {"v8", "v11"}
book = json.loads(Path(f"cpc_jev_codebook_{experiment}.json").read_text())
with open("../input/ulurp_cpc_reconciled_coding.csv", newline="") as f:
    humans = list(csv.DictReader(f))
with open("../input/ulurp_cpc_narrative_sources.csv", newline="") as f:
    links = list(csv.DictReader(f))
with open("../input/ulurp_cpc_report_manifest.csv", newline="") as f:
    manifest_rows = list(csv.DictReader(f))
with open("cpc_subagent_exclusions_v3.csv", newline="") as f:
    excluded = {r["document_id"] for r in csv.DictReader(f)}
assert len(humans) == len({(r["source_document_id"], r["field"]) for r in humans})
assert len(links) == len({(r["document_id"], r["source_document_id"]) for r in links})
assert len(manifest_rows) == len({r["document_id"] for r in manifest_rows})
manifest = {r["document_id"]: r for r in manifest_rows}
human = {(r["source_document_id"], r["field"]): r for r in humans}
sources = defaultdict(list)
for r in links:
    if r["text_included_flag"] == "TRUE":
        sources[r["document_id"]].append(r)
excluded |= {r["source_document_id"] for r in humans if r["reconciliation_provenance"].startswith("source_review")}
old = [json.loads(s) for s in Path("../output/cpc_jev_requests_v1.jsonl").read_text().splitlines()]
old += [json.loads(s) for s in Path("../output/cpc_jev_requests_v2.jsonl").read_text().splitlines()]
if experiment == "v11":
    old += [json.loads(s) for s in Path("../output/cpc_jev_requests_v8.jsonl").read_text().splitlines()]
    old += [json.loads(s) for s in Path("../output/cpc_jev_requests_v9.jsonl").read_text().splitlines()]
excluded |= {p["document_id"] for p in old}
used_ids = excluded | {page["source_document_id"] for p in old for page in p["body"]["state"].get("pages", [])}
used_ids |= {r["source_document_id"] for d in excluded for r in sources[d]}
used_hashes = {r["source_text_sha256"] for r in links if r["source_document_id"] in used_ids}
fields = list(book["legacy_topics"]) + ["councilmember_position", "civic_group_position"] if experiment == "v8" else ["councilmember_position"]
roster, candidates = [], {}
for doc in sorted({r["source_document_id"] for r in humans}, key=lambda d: hashlib.sha256(f"{sample_seed}|{d}".encode()).hexdigest()):
    refs = [human.get((doc, f), {}) for f in fields]
    cells = [f + "=" + r["human_value"] for f, r in zip(fields, refs)
        if r.get("human_status") in {"human_agreement", "single_human_coder"} and r.get("human_value") in {"0", "1", "none_or_procedural", "support_or_request", "opposition"}]
    reason, pages, state_size = "candidate", [], 0
    if not sources[doc]:
        reason = "not_a_retained_narrative"
    elif any(r["source_document_id"] in used_ids or r["source_text_sha256"] in used_hashes for r in sources[doc]):
        reason = "prior_pilot_or_source_review_overlap"
    elif len(cells) != len(fields):
        reason = "incomplete_or_conflicting_original_reference"
    else:
        for link in sorted(sources[doc], key=lambda r: (r["source_document_id"] != doc, r["source_document_id"])):
            m = manifest[link["source_document_id"]]
            text = (Path("../../../build_ulurp_cpc_report_corpus/code") / m["local_text_path"]).read_text()
            assert hashlib.sha256(text.encode()).hexdigest() == link["source_text_sha256"]
            pages.extend(dict(document_id=doc, page_id=f"{link['source_document_id']}:p{i}",
                source_document_id=link["source_document_id"], source_application_number=link["source_application_number"],
                source_role=link["link_role"], pdf_page=i, source_text_sha256=link["source_text_sha256"],
                public_pdf_url=m["resolved_pdf_url"], text=" ".join(t.split()))
                for i, t in enumerate(text.split("\f"), 1) if t.strip())
        state = dict(report=dict(application_number=manifest[doc]["application_number"],
            text="\n\n".join(f"Source {r['source_application_number']} ({r['source_role']}), page ID {r['page_id']}\n{r['text']}" for r in pages)))
        state_size = len(json.dumps(state, ensure_ascii=False))
        if state_size > max_state_characters:
            reason = "whole_bundle_exceeds_state_limit"
        else:
            candidates[doc] = dict(state=state, pages=pages, cells=cells)
    roster.append(dict(document_id=doc, application_number=refs[0].get("application_number", ""),
        selected=0, selection_reason=reason, reference_cells=";".join(cells), state_characters=state_size or None))
counts, selected = Counter(), []
for _ in range(report_count):
    eligible = [d for d, c in candidates.items() if d not in selected and not any(p["source_text_sha256"] in used_hashes for p in c["pages"])]
    assert eligible, "Insufficient eligible distinct-source bundles."
    if experiment == "v8":
        chosen = max(eligible, key=lambda d: sum(counts[cell] < coverage_target for cell in candidates[d]["cells"]))
    else:
        chosen = max(eligible, key=lambda d: candidates[d]["cells"][0] != "councilmember_position=none_or_procedural")
    selected.append(chosen)
    counts.update(candidates[chosen]["cells"])
    used_hashes.update(p["source_text_sha256"] for p in candidates[chosen]["pages"])
for r in roster:
    r["selected"] = int(r["document_id"] in selected)
    if r["selection_reason"] == "candidate":
        r["selection_reason"] = "original_label_coverage" if r["selected"] else "outside_fixed_test_size_or_selected_source_overlap"
    if experiment == "v11" and r["selected"]:
        r["selection_reason"] = "all_eligible_original_positives" if r["reference_cells"] != "councilmember_position=none_or_procedural" else "seeded_original_negatives"

definitions = {}
for topic, description in book.get("topics", {}).items():
    for stage, rule in book["stages"].items():
        definitions[f"{topic}__{stage}"] = dict(type="noul",
            instructions=rule["question"].format(topic=description) + " " + book["scope"],
            criteria={k: rule[k] for k in ("true", "false")})
for actor, description in book.get("actors", {}).items():
    for position, phrase in book["positions"].items():
        definitions[f"{actor}__{position}"] = dict(type="noul",
            instructions=book["actor_question"].format(actor=description, position=phrase) + " " + book["scope"],
            criteria=book["actor_criteria"])
if experiment == "v11":
    assert sum(c["cells"][0] != "councilmember_position=none_or_procedural" for c in candidates.values()) <= report_count, "Increase the test size to include all eligible positives."
    definitions = {field: dict(type="noul", instructions=q["question"] + " " + book["scope"],
        criteria={k: q[k] for k in ("true", "false")}) for field, q in book["questions"].items()}
packets, pages = [], []
for doc in selected:
    c = candidates[doc]
    questions = dict(definitions)
    for field, q in definitions.items():
        if not field.endswith("__discussed"):
            questions["page__" + field] = dict(type="choice", instructions="Which supplied page best supports a YES answer to this question? Select none if no page does. " + q["instructions"],
                criteria={**{p["page_id"]: None for p in c["pages"]}, "none": "No page establishes a yes answer."})
    body = dict(model="typesafe-ai/jev", state=c["state"], questions=questions)
    packets.append(dict(request_id=f"{doc}_transfer", document_id=doc,
        request_sha256=hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest(), body=body))
    pages.extend(c["pages"])
assert len(packets) == report_count and len({p["request_sha256"] for p in packets}) == report_count
with open(f"../output/cpc_jev_requests_{experiment}.jsonl", "w") as f:
    for p in packets:
        f.write(json.dumps(p, ensure_ascii=False) + "\n")
save_csv(roster, list(roster[0]), f"../output/cpc_jev_sample_{experiment}.csv", ["document_id"])
save_csv(pages, list(pages[0]), f"../output/cpc_jev_pages_{experiment}.csv", ["document_id", "page_id"])
print(f"Frozen {len(packets)} new-source bundles, {len(questions)} questions each; original label coverage: {dict(counts)}")
