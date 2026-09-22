#!/usr/bin/env python3
"""Freeze repeated requests and a positive-balanced additional development test."""

import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# Run from tasks/audits/pilot_ulurp_cpc_llm_labels/code.
# repeats = 5
# reports_per_cell = 5
# max_state_characters = 90000
# sample_seed = "jev-repeat-v2-20260919"
repeats, reports_per_cell, max_state_characters, sample_seed = sys.argv[1:]
repeats, reports_per_cell, max_state_characters = map(int, (repeats, reports_per_cell, max_state_characters))
assert repeats == 5 and reports_per_cell > 0
fields = ("scale_character_preservation", "revision_or_concession")
codebook = json.loads(Path("cpc_jev_codebook_v2.json").read_text())
old_packets = [json.loads(s) for s in Path("../output/cpc_jev_requests_v1.jsonl").read_text().splitlines()]
with open("../output/cpc_jev_sample_v1.csv", newline="") as source:
    old_sample = {r["document_id"]: r for r in csv.DictReader(source)}
with open("../input/ulurp_cpc_reconciled_coding.csv", newline="") as source:
    human_rows = list(csv.DictReader(source))
with open("../input/ulurp_cpc_text_labels.csv", newline="") as source:
    narrative_rows = list(csv.DictReader(source))
with open("../input/ulurp_cpc_report_manifest.csv", newline="") as source:
    manifest_rows = list(csv.DictReader(source))
with open("../input/ulurp_cpc_narrative_sources.csv", newline="") as source:
    links = list(csv.DictReader(source))
with open("cpc_subagent_exclusions_v3.csv", newline="") as source:
    earlier_ids = {r["document_id"] for r in csv.DictReader(source)}
assert len(human_rows) == len({(r["source_document_id"], r["field"]) for r in human_rows})
assert len(narrative_rows) == len({r["document_id"] for r in narrative_rows})
assert len(manifest_rows) == len({r["document_id"] for r in manifest_rows})
human = {(r["source_document_id"], r["field"]): r for r in human_rows}
narratives = {r["document_id"]: r for r in narrative_rows}
manifest = {r["document_id"]: r for r in manifest_rows}
sources = defaultdict(list)
for row in links:
    if row["text_included_flag"] == "TRUE":
        sources[row["document_id"]].append(row)
reviewed_ids = {r["source_document_id"] for r in human_rows if r["reconciliation_provenance"].startswith("source_review")}
used_sources = {p["source_document_id"] for packet in old_packets for p in packet["body"]["state"]["pages"]}
used_sources |= {r["source_document_id"] for d in earlier_ids | reviewed_ids for r in sources[d]}
states = {p["document_id"]: p["body"]["state"] for p in old_packets if p["body"]["state"]["total_parts"] == 1}
old_questions = {f: old_packets[0]["body"]["questions"][f] for f in fields}

roster = []
cells = Counter()
candidate_ids = {r["source_document_id"] for r in human_rows} | set(old_sample)
for doc in sorted(candidate_ids, key=lambda d: hashlib.sha256(f"{sample_seed}|{d}".encode()).hexdigest()):
    references = [human.get((doc, f), {}) for f in fields]
    cell = "".join(r.get("human_value", "") for r in references)
    eligible = all(r.get("human_status") in {"human_agreement", "single_human_coder"} and r.get("human_value") in {"0", "1"} for r in references)
    cohort, reason = "not_selected", "incomplete_or_conflicting_original_reference"
    state_size = 0
    if doc in old_sample:
        cohort = "original_development"
        reason = "selected" if doc in states else "long_bundle_retained_not_repeated"
    elif doc not in narratives or not sources[doc]:
        reason = "not_a_retained_narrative"
    elif eligible:
        source_ids = {r["source_document_id"] for r in sources[doc]}
        if source_ids & used_sources:
            reason = "shares_source_with_prior_review_or_selected_report"
        elif cells[cell] >= reports_per_cell:
            reason = "cell_target_filled"
        else:
            pages = []
            for link in sorted(sources[doc], key=lambda r: (r["source_document_id"] != doc, r["source_document_id"])):
                original = manifest[link["source_document_id"]]
                text = (Path("../../../build_ulurp_cpc_report_corpus/code") / original["local_text_path"]).read_text()
                assert hashlib.sha256(text.encode()).hexdigest() == link["source_text_sha256"]
                pages.extend(dict(source_document_id=link["source_document_id"], application_number=link["source_application_number"],
                    role=link["link_role"], source_text_sha256=link["source_text_sha256"], pdf_page=i, text=" ".join(t.split()))
                    for i, t in enumerate(text.split("\f"), 1) if t.strip())
            state = dict(focal={k: narratives[doc][k] for k in ("document_id", "application_number", "project_name", "year")},
                part_number=1, total_parts=1, pages=pages)
            state_size = len(json.dumps(state, ensure_ascii=False))
            if state_size > max_state_characters:
                reason = "exceeds_whole_bundle_state_limit"
            else:
                cohort, reason = "additional_balanced_test", "selected"
                states[doc] = state
                cells[cell] += 1
                used_sources.update(source_ids)
    if doc in states:
        state_size = len(json.dumps(states[doc], ensure_ascii=False))
    roster.append(dict(document_id=doc, application_number=references[0].get("application_number", old_sample.get(doc, {}).get("application_number", "")),
        cohort=cohort, selection_reason=reason, reference_cell=cell if eligible else "", state_characters=state_size or None,
        expected_repeats=repeats if reason == "selected" else 0))
assert cells == Counter({cell: reports_per_cell for cell in ("00", "01", "10", "11")}), cells
assert len(states) == 26 + 4 * reports_per_cell

packets = []
for repeat in range(1, repeats + 1):
    for row in roster:
        if row["selection_reason"] != "selected":
            continue
        doc = row["document_id"]
        questions = {"original_" + f: q for f, q in old_questions.items()}
        for variant, definitions in codebook["variants"].items():
            for field, definition in definitions.items():
                questions[variant + "_" + field] = dict(type="choice", instructions=dict(scope=codebook["scope"], question=definition),
                    criteria={"1": "Qualifying evidence is documented.", "0": "No qualifying evidence is documented.", "unclear": "Relevant evidence cannot be resolved."})
        for name, definition in codebook["diagnostics"].items():
            questions[name] = dict(type="choice", instructions=dict(scope=codebook["scope"], question=definition["instructions"]), criteria=definition["criteria"])
        page_options = {f"{p['source_document_id']}:p{p['pdf_page']}": f"{p['application_number']}, PDF page {p['pdf_page']}" for p in states[doc]["pages"]}
        for field, definition in codebook["variants"]["clear"].items():
            questions["page_" + field] = dict(type="choice", instructions=dict(scope=codebook["scope"],
                question="Select the ONE supplied page providing the strongest qualifying evidence for a positive answer to this question, or none if absent. Selecting a page does not itself verify the interpretation. " + definition),
                criteria=dict(page_options, none="No supplied page establishes qualifying positive evidence."))
        body = dict(model="typesafe-ai/jev", state=states[doc], questions=questions)
        fingerprint = hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        packets.append(dict(request_id=f"{doc}_repeat{repeat}", document_id=doc, repeat=repeat, cohort=row["cohort"], request_sha256=fingerprint, body=body))
for doc in states:
    assert len({p["request_sha256"] for p in packets if p["document_id"] == doc}) == 1
with open("../output/cpc_jev_requests_v2.jsonl", "w") as output:
    for packet in packets:
        output.write(json.dumps(packet, ensure_ascii=False) + "\n")
save_csv(roster, list(roster[0]), "../output/cpc_jev_sample_v2.csv", ["document_id"])
print(f"Frozen {len(states)} complete bundles x {repeats} identical requests = {len(packets)} requests; additional cells {dict(cells)}. Four original split bundles retained without truncation.")
