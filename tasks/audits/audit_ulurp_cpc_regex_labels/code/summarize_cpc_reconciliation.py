#!/usr/bin/env python3
"""Report source-based rulings on differing Jacob/Tyler codes, without scoring a model."""

import csv
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

with open("../input/ulurp_cpc_reconciled_coding.csv", newline="") as source:
    rows = list(csv.DictReader(source))
assert len(rows) == len({(r["source_document_id"], r["field"]) for r in rows})
review = [r for r in rows if r["jacob_value"] and r["tyler_value"]
          and r["jacob_value"] != r["tyler_value"]]
fields = ["source_document_id", "application_number", "field", "jacob_value", "tyler_value",
          "jacob_coding_complete", "tyler_coding_complete", "reconciled_value",
          "reconciliation_provenance", "reconciliation_issue_scope", "reconciliation_scope_sensitive",
          "reconciliation_review_status", "reconciliation_confidence", "reconciliation_reason",
          "reconciliation_manager_note", "reconciliation_evidence_json"]
save_csv([{field: r[field] for field in fields} for r in review], fields,
         "../output/ulurp_cpc_reconciliation_review.csv", ["source_document_id", "field"])
scope, = {r["reconciliation_issue_scope"] for r in review}
counts = Counter(r["reconciliation_provenance"] for r in review)
completed = [r for r in review if r["jacob_coding_complete"] == r["tyler_coding_complete"] == "1"]
complete_counts = Counter(r["reconciliation_provenance"] for r in completed)
lines = ["# Reconciliation of Jacob and Tyler's CPC coding", "",
    f"Source review covers {len(review)} differing field values across "
    f"{len({r['source_document_id'] for r in review})} reports. "
    f"Both researchers marked their reading complete for {len(completed)} of these field differences; "
    f"the remaining {len(review) - len(completed)} involve provisional coding.", "",
    f"Selected issue scope: **{scope}**. Broad includes substantive CPC or local discussion; local requires a local actor's concern or condition. "
    "Both exclude neutral descriptions and routine findings. "
    f"The selected value could depend on this scope choice in {sum(r['reconciliation_scope_sensitive'] == 'True' for r in review)} reviewed fields.", "",
    "Rulings are AI-assisted source reviews, not a third human coding round or a validation accuracy estimate. "
    "Matching a coder's value does not validate that person's other entries. Both original human columns remain unchanged. "
    "The older combined development-direction field is not directly comparable with the newer separate definitions and is excluded.", "",
    "| Ruling | All differences | Both readings complete |", "|---|---:|---:|"]
for key, label in [("source_review_tyler", "Use Tyler's value"), ("source_review_jacob", "Use Jacob's value"),
                   ("source_review_neither", "Use a different source-supported value"),
                   ("unresolved_source_review", "Unresolved; no replacement value")]:
    lines.append(f"| {label} | {counts[key]} | {complete_counts[key]} |")
lines += ["", "| Field | Differences | Tyler | Jacob | Neither | Unresolved |",
          "|------------------------------------------------|-----------:|--------:|--------:|--------:|-----------:|"]
for field in sorted({r["field"] for r in review}):
    field_rows = [r for r in review if r["field"] == field]
    c = Counter(r["reconciliation_provenance"] for r in field_rows)
    lines.append(f"| {field.replace('_', ' ')} | {len(field_rows)} | {c['source_review_tyler']} | "
                 f"{c['source_review_jacob']} | {c['source_review_neither']} | {c['unresolved_source_review']} |")
lines += ["", "The review CSV contains every differing field, both original values, the working ruling, "
    "confidence, reason, and source-page quotations. The full reconciled dataset retains all original human records; "
    "unreviewed single-coder and agreed values remain identifiable as original coding. "
    "No regex or model accuracy score is recomputed against these model-assisted rulings."]
Path("../output/ulurp_cpc_reconciliation_findings.md").write_text("\n".join(lines) + "\n")
