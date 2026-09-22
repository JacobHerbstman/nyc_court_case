#!/usr/bin/env python3
"""Reuse existing human decisions without changing their definitions or resolving conflicts."""

import csv
import sys
from collections import defaultdict

sys.path.insert(0, "../../_lib")
from data_reports import save_csv

with open("../input/ulurp_cpc_training_labels_jacob.csv", newline="") as source:
    jacob_rows = list(csv.DictReader(source))
with open("../input/ulurp_cpc_training_labels_tyler.csv", newline="") as source:
    tyler_rows = list(csv.DictReader(source))
with open("../output/ulurp_cpc_text_labels.csv", newline="") as source:
    regex_rows = list(csv.DictReader(source))
with open("../output/ulurp_cpc_narrative_sources.csv", newline="") as source:
    source_links = list(csv.DictReader(source))
for rows in (jacob_rows, tyler_rows, regex_rows):
    assert len(rows) == len({r["document_id"] for r in rows})
jacob = {r["document_id"]: r for r in jacob_rows}
tyler = {r["document_id"]: r for r in tyler_rows}
regex = {r["document_id"]: r for r in regex_rows}
represented_by = defaultdict(set)
for link in source_links:
    if link["represented_application_flag"] == "TRUE":
        represented_by[link["source_document_id"]].add(link["document_id"])

binary_fields = [
    "specific_project", "substantial_local_opposition", "local_request_condition",
    "revision_or_concession", "procedural_response", "explicit_local_response",
    "approved_unresolved_objection", "cb_request_or_opposition", "bp_request_or_opposition",
    "affordability_displacement", "traffic_parking", "scale_character_preservation",
    "infrastructure_services", "environment_open_space",
]
position_fields = ["councilmember_position", "civic_group_position"]
count_fields = ["cpc_support_speakers", "cpc_opposition_speakers", "cb_support_votes", "cb_opposition_votes"]
fields = [*binary_fields, *position_fields, *count_fields, "zone_change", "dev_direction",
          "legacy_development_direction"]
# Tyler's old development-direction field combines meanings now coded separately.
# Keep it under its own name; do not assign a new zoning or practical-effect judgment.
tyler_fields = {field: field for field in fields}
tyler_fields.update(substantial_local_opposition="local_opposition",
    cpc_support_speakers="speakers_for", cpc_opposition_speakers="speakers_against",
    legacy_development_direction="dev_direction", dev_direction="", zone_change="")

human_labels = []
for document_id in sorted(set(jacob) | set(tyler)):
    j = jacob.get(document_id, {})
    t = tyler.get(document_id, {})
    if j and t:
        assert j["application_number"] == t["application_number"]
    label_row = j or t
    if document_id in regex:
        assert label_row["application_number"] == regex[document_id]["application_number"]
    for field in fields:
        j_field = field if field != "legacy_development_direction" else ""
        t_field = tyler_fields[field]
        j_value, t_value = j.get(j_field, ""), t.get(t_field, "")
        values = [v for v in (j_value, t_value) if v != ""]
        if not values:
            continue
        if field in binary_fields:
            valid = all(v in {"0", "1"} for v in values)
        elif field in position_fields:
            valid = all(v in {"none_or_procedural", "support_or_request", "opposition"} for v in values)
        elif field in count_fields:
            valid = all(v.isdigit() for v in values)
        elif field == "dev_direction":
            valid = all(v in {"more", "lower", "mixed", "none"} for v in values)
        else:
            valid = all(v in {"upzone", "downzone", "mixed", "none"} for v in values)
        human_value = ""
        if not valid:
            status = "unclear_or_nonstandard"
        elif len(set(values)) > 1:
            status = "coder_disagreement"
        else:
            human_value = values[0]
            incomplete = ((j_value != "" and j.get("coding_complete") != "1") or
                          (t_value != "" and t.get("coding_complete") != "1"))
            status = "provisional" if incomplete else "human_agreement" if len(values) == 2 else "single_human_coder"
        regex_value = regex.get(document_id, {}).get(field, "")
        match_status = ("retained_focal_narrative" if document_id in regex else
                        "represented_by_other_narrative" if represented_by[document_id] else
                        "no_retained_narrative_match")
        human_labels.append({
            "source_document_id": document_id,
            "application_number": label_row["application_number"],
            "field": field, "human_value": human_value, "human_status": status,
            "jacob_value": j_value, "tyler_value": t_value,
            "jacob_source_field": j_field, "tyler_source_field": t_field,
            "jacob_review_id": j.get("review_id", ""), "tyler_review_id": t.get("review_id", ""),
            "jacob_coding_complete": j.get("coding_complete", ""),
            "tyler_coding_complete": t.get("coding_complete", ""),
            "jacob_evidence_pages": j.get("evidence_pages", ""),
            "tyler_evidence_pages": t.get("evidence_pages", ""),
            "jacob_evidence_summary": j.get("evidence_summary", ""),
            "tyler_evidence_summary": t.get("evidence_summary", ""),
            "jacob_coder_notes": j.get("coder_notes", ""), "tyler_coder_notes": t.get("coder_notes", ""),
            "jacob_confidence": j.get("coding_confidence", ""), "tyler_confidence": t.get("coding_confidence", ""),
            "narrative_match_status": match_status,
            "represented_narrative_ids": "; ".join(sorted(represented_by[document_id])),
            "regex_value": regex_value,
            "current_source_text_sha256": regex.get(document_id, {}).get("source_text_sha256", ""),
            "coding_source_check": "identifier_match_only_no_original_text_hash",
        })

# Every entered label survives in its original coder's column, including provisional values.
assert sum(r["jacob_value"] != "" for r in human_labels) == sum(
    r.get(field, "") != "" for r in jacob_rows for field in fields if field != "legacy_development_direction")
assert sum(r["tyler_value"] != "" for r in human_labels) == sum(
    r.get(field, "") != "" for r in tyler_rows for field in set(tyler_fields.values()) if field)
save_csv(human_labels, list(human_labels[0]), "../output/ulurp_cpc_human_coding.csv",
         ["source_document_id", "field"])
print(f"Preserved {len(human_labels):,} field records across "
      f"{len({r['source_document_id'] for r in human_labels}):,} human-coded reports.")
