#!/usr/bin/env python3
"""Apply recorded source reviews while retaining every original human value."""

import csv
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, "../../_lib")
from data_reports import save_csv

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/summarize_text_cpc_trends/code")
# issue_scope = "broad"
issue_scope, = sys.argv[1:]
assert issue_scope in {"local", "broad"}

with open("../output/ulurp_cpc_human_coding.csv", newline="") as source:
    human_rows = list(csv.DictReader(source))
with open("../input/ulurp_cpc_coding_adjudications.csv", newline="") as source:
    review_rows = list(csv.DictReader(source))
with open("../input/ulurp_cpc_report_manifest.csv", newline="") as source:
    manifest_rows = list(csv.DictReader(source))
with open("../output/ulurp_cpc_narrative_sources.csv", newline="") as source:
    source_links = list(csv.DictReader(source))
assert len(human_rows) == len({(r["source_document_id"], r["field"]) for r in human_rows})
assert len(review_rows) == len({(r["document_id"], r["field"]) for r in review_rows})
assert len(manifest_rows) == len({r["document_id"] for r in manifest_rows})
human = {(r["source_document_id"], r["field"]): r for r in human_rows}
reviews = {(r["document_id"], r["field"]): r for r in review_rows}
manifest = {r["document_id"]: r for r in manifest_rows}
differences = {key for key, r in human.items()
               if r["jacob_value"] and r["tyler_value"] and r["jacob_value"] != r["tyler_value"]}
assert set(reviews) == differences, "Recorded reviews must cover precisely the current human disagreements."
supplied_sources = {(r["document_id"], r["source_document_id"]) for r in source_links
                    if r["text_included_flag"] == "TRUE"}

# Check the actual source vintage and page text before applying any ruling.
evidence = {key: json.loads(r["evidence_json"]) for key, r in reviews.items()}
source_ids = {e["source_document_id"] for items in evidence.values() for e in items}
pages, source_hashes = {}, {}
for source_id in sorted(source_ids):
    text = (Path("../../build_ulurp_cpc_report_corpus/code") /
            manifest[source_id]["local_text_path"]).read_text(encoding="utf-8")
    source_hashes[source_id] = hashlib.sha256(text.encode()).hexdigest()
    pages[source_id] = {i: " ".join(t.split()) for i, t in enumerate(text.split("\f"), 1)}
for key, review in reviews.items():
    original = human[key]
    assert review["application_number"] == original["application_number"]
    assert review["jacob_value"] == original["jacob_value"]
    assert review["tyler_value"] == original["tyler_value"]
    assert review["status"] in {"resolved", "ambiguous", "source_insufficient", "definition_mismatch"}
    assert review["confidence"] in {"high", "medium", "low"}
    assert review["reason"] and evidence[key]
    for item in evidence[key]:
        source_id = item["source_document_id"]
        assert (key[0], source_id) in supplied_sources
        assert item["source_text_sha256"] == source_hashes[source_id]
        quote = " ".join(item["quote"].split())
        assert quote and quote in pages[source_id][int(item["pdf_page"])], (key, item)
    for scope in ("local", "broad"):
        value = review[f"value_{scope}_scope"]
        if key[1].endswith(("_speakers", "_votes")):
            assert value == "" or value.isdigit()
        elif key[1] in {"councilmember_position", "civic_group_position"}:
            assert value in {"", "none_or_procedural", "support_or_request", "opposition"}
        else:
            assert value in {"", "0", "1"}

reconciled = []
for original in human_rows:
    key = (original["source_document_id"], original["field"])
    review = reviews.get(key)
    value = original["human_value"] if original["human_status"] in {
        "human_agreement", "single_human_coder"} else ""
    provenance = "original_" + original["human_status"] if value else "unresolved_original"
    if review:
        value = review[f"value_{issue_scope}_scope"] if review["status"] == "resolved" else ""
        if value:
            coder = ("tyler" if value == original["tyler_value"] else
                     "jacob" if value == original["jacob_value"] else "neither")
            provenance = "source_review_" + coder
        else:
            provenance = "unresolved_source_review"
    reconciled.append(dict(original, reconciled_value=value, reconciliation_provenance=provenance,
        reconciliation_issue_scope=issue_scope,
        reconciliation_scope_sensitive=bool(review and review["value_local_scope"] != review["value_broad_scope"]),
        reconciliation_review_status=review["status"] if review else "",
        reconciliation_confidence=review["confidence"] if review else "",
        reconciliation_reason=review["reason"] if review else "",
        reconciliation_manager_note=review["manager_note"] if review else "",
        reconciliation_evidence_json=review["evidence_json"] if review else ""))

assert len(reconciled) == len(human_rows)
save_csv(reconciled, list(reconciled[0]), "../output/ulurp_cpc_reconciled_coding.csv",
         ["source_document_id", "field"])
print(f"Applied {len(review_rows)} source reviews with {issue_scope} issue scope; original human columns preserved.")
