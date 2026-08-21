#!/usr/bin/env python3

import csv
from pathlib import Path


BINARY_FIELDS = [
    "substantial_local_opposition",
    "local_request_condition",
    "revision_or_concession",
    "procedural_response",
    "explicit_local_response",
    "approved_unresolved_objection",
    "cb_request_or_opposition",
    "bp_request_or_opposition",
    "affordability_displacement",
    "traffic_parking",
    "scale_character_preservation",
    "infrastructure_services",
    "environment_open_space",
]
POSITION_FIELDS = ["councilmember_position", "civic_group_position"]
COUNT_FIELDS = [
    "cpc_support_speakers",
    "cpc_opposition_speakers",
    "cb_support_votes",
    "cb_opposition_votes",
]


def ratio(numerator, denominator):
    return numerator / denominator if denominator else ""


with Path("../input/ulurp_cpc_text_labels.csv").open(
    newline="", encoding="utf-8-sig"
) as input_file:
    regex_rows = {
        row["document_id"]: row
        for row in csv.DictReader(input_file)
    }

with Path("../input/ulurp_cpc_training_labels_jacob.csv").open(
    newline="", encoding="utf-8-sig"
) as input_file:
    human_rows = list(csv.DictReader(input_file))

if len(human_rows) != len({row["document_id"] for row in human_rows}):
    raise RuntimeError("Human labels are not unique by document_id.")
if any(row["document_id"] not in regex_rows for row in human_rows):
    raise RuntimeError("At least one hand-labeled report is absent from the regex corpus.")

summary_rows = []
for field in BINARY_FIELDS:
    rows = [row for row in human_rows if row[field] in {"0", "1"}]
    true_positive = sum(
        row[field] == "1" and regex_rows[row["document_id"]][field] == "1"
        for row in rows
    )
    false_positive = sum(
        row[field] == "0" and regex_rows[row["document_id"]][field] == "1"
        for row in rows
    )
    false_negative = sum(
        row[field] == "1" and regex_rows[row["document_id"]][field] == "0"
        for row in rows
    )
    true_negative = sum(
        row[field] == "0" and regex_rows[row["document_id"]][field] == "0"
        for row in rows
    )
    exact = true_positive + true_negative
    summary_rows.append(
        {
            "field": field,
            "field_type": "binary",
            "human_nonmissing": len(rows),
            "regex_nonmissing": len(rows),
            "exact_agreement": exact,
            "agreement_share": ratio(exact, len(rows)),
            "true_positive": true_positive,
            "false_positive": false_positive,
            "false_negative": false_negative,
            "true_negative": true_negative,
            "precision": ratio(true_positive, true_positive + false_positive),
            "recall": ratio(true_positive, true_positive + false_negative),
            "exact_when_regex_nonmissing": ratio(exact, len(rows)),
        }
    )

for field in POSITION_FIELDS:
    rows = [row for row in human_rows if row[field]]
    exact = sum(
        row[field] == regex_rows[row["document_id"]][field]
        for row in rows
    )
    summary_rows.append(
        {
            "field": field,
            "field_type": "position",
            "human_nonmissing": len(rows),
            "regex_nonmissing": len(rows),
            "exact_agreement": exact,
            "agreement_share": ratio(exact, len(rows)),
            "true_positive": "",
            "false_positive": "",
            "false_negative": "",
            "true_negative": "",
            "precision": "",
            "recall": "",
            "exact_when_regex_nonmissing": ratio(exact, len(rows)),
        }
    )

for field in COUNT_FIELDS:
    rows = [row for row in human_rows if row[field] != ""]
    parsed_rows = [
        row
        for row in rows
        if regex_rows[row["document_id"]][field] != ""
    ]
    exact = sum(
        row[field] == regex_rows[row["document_id"]][field]
        for row in rows
    )
    exact_parsed = sum(
        row[field] == regex_rows[row["document_id"]][field]
        for row in parsed_rows
    )
    summary_rows.append(
        {
            "field": field,
            "field_type": "count",
            "human_nonmissing": len(rows),
            "regex_nonmissing": len(parsed_rows),
            "exact_agreement": exact,
            "agreement_share": ratio(exact, len(rows)),
            "true_positive": "",
            "false_positive": "",
            "false_negative": "",
            "true_negative": "",
            "precision": "",
            "recall": "",
            "exact_when_regex_nonmissing": ratio(exact_parsed, len(parsed_rows)),
        }
    )

with Path("../output/ulurp_cpc_regex_training_agreement.csv").open(
    "w", newline="", encoding="utf-8"
) as output_file:
    writer = csv.DictWriter(
        output_file,
        fieldnames=summary_rows[0].keys(),
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(summary_rows)

print(f"Compared deterministic rules with {len(human_rows)} hand-labeled CPC reports.")
