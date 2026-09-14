#!/usr/bin/env python3

import csv
import hashlib
import sys
from collections import Counter
from pathlib import Path


TRAINING_BINARY_FIELDS = [
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
VALIDATION_BINARY_FIELDS = [
    *TRAINING_BINARY_FIELDS[:6],
    "cb_opposition",
    *TRAINING_BINARY_FIELDS[6:],
    "restrictive_declaration",
    "points_of_agreement",
]
POSITION_FIELDS = ["councilmember_position", "civic_group_position"]
COUNT_FIELDS = [
    "cpc_support_speakers",
    "cpc_opposition_speakers",
    "cb_support_votes",
    "cb_opposition_votes",
]
DISAGREEMENT_FIELDS = [
    "validation_id",
    "document_id",
    "application_number",
    "year",
    "decade",
    "project_name",
    "field",
    "manual_value",
    "regex_value",
    "error_type",
    "evidence_pages",
    "evidence_summary",
    "coder_notes",
    "official_pdf_url",
]


def ratio(numerator, denominator):
    return numerator / denominator if denominator else ""


def summarize(rows, regex_rows, binary_fields, sample_slice):
    summary = []
    for field in binary_fields:
        coded = [row for row in rows if row[field] in {"0", "1"}]
        true_positive = sum(
            row[field] == "1" and regex_rows[row["document_id"]][field] == "1"
            for row in coded
        )
        false_positive = sum(
            row[field] == "0" and regex_rows[row["document_id"]][field] == "1"
            for row in coded
        )
        false_negative = sum(
            row[field] == "1" and regex_rows[row["document_id"]][field] == "0"
            for row in coded
        )
        true_negative = sum(
            row[field] == "0" and regex_rows[row["document_id"]][field] == "0"
            for row in coded
        )
        exact = true_positive + true_negative
        summary.append(
            {
                "sample_slice": sample_slice,
                "field": field,
                "field_type": "binary",
                "human_nonmissing": len(coded),
                "regex_nonmissing": len(coded),
                "exact_agreement": exact,
                "agreement_share": ratio(exact, len(coded)),
                "true_positive": true_positive,
                "false_positive": false_positive,
                "false_negative": false_negative,
                "true_negative": true_negative,
                "precision": ratio(true_positive, true_positive + false_positive),
                "recall": ratio(true_positive, true_positive + false_negative),
                "exact_when_regex_nonmissing": ratio(exact, len(coded)),
            }
        )

    for field in POSITION_FIELDS:
        coded = [row for row in rows if row[field] not in {"", "unclear"}]
        exact = sum(
            row[field] == regex_rows[row["document_id"]][field] for row in coded
        )
        summary.append(
            {
                "sample_slice": sample_slice,
                "field": field,
                "field_type": "position",
                "human_nonmissing": len(coded),
                "regex_nonmissing": len(coded),
                "exact_agreement": exact,
                "agreement_share": ratio(exact, len(coded)),
                "true_positive": "",
                "false_positive": "",
                "false_negative": "",
                "true_negative": "",
                "precision": "",
                "recall": "",
                "exact_when_regex_nonmissing": ratio(exact, len(coded)),
            }
        )

    for field in COUNT_FIELDS:
        coded = [row for row in rows if row[field] != ""]
        parsed = [
            row for row in coded if regex_rows[row["document_id"]][field] != ""
        ]
        exact = sum(
            row[field] == regex_rows[row["document_id"]][field] for row in coded
        )
        exact_parsed = sum(
            row[field] == regex_rows[row["document_id"]][field] for row in parsed
        )
        summary.append(
            {
                "sample_slice": sample_slice,
                "field": field,
                "field_type": "count",
                "human_nonmissing": len(coded),
                "regex_nonmissing": len(parsed),
                "exact_agreement": exact,
                "agreement_share": ratio(exact, len(coded)),
                "true_positive": "",
                "false_positive": "",
                "false_negative": "",
                "true_negative": "",
                "precision": "",
                "recall": "",
                "exact_when_regex_nonmissing": ratio(exact_parsed, len(parsed)),
            }
        )
    return summary


def draw_sample(regex_rows, excluded_ids, sample_size, sample_seed):
    candidates = [
        row
        for row in regex_rows.values()
        if row["action_code"] in {"ZM", "ZR", "ZS"}
        and row["document_id"] not in excluded_ids
        and int(row["year"]) <= 2025
    ]
    decade_counts = Counter(row["decade"] for row in candidates)
    decade_quotas = {
        decade: count * sample_size // len(candidates)
        for decade, count in decade_counts.items()
    }
    remainders = sorted(
        (
            (count * sample_size / len(candidates) - decade_quotas[decade], decade)
            for decade, count in decade_counts.items()
        ),
        reverse=True,
    )
    for _, decade in remainders[: sample_size - sum(decade_quotas.values())]:
        decade_quotas[decade] += 1

    sample = []
    for decade in sorted(decade_quotas):
        decade_rows = [row for row in candidates if row["decade"] == decade]
        decade_rows.sort(
            key=lambda row: hashlib.sha256(
                f"{sample_seed}|{row['document_id']}".encode("utf-8")
            ).hexdigest()
        )
        sample.extend(decade_rows[: decade_quotas[decade]])
    return sorted(
        sample,
        key=lambda row: (int(row["year"]), row["application_number"]),
    )


def validate_sample(rows, expected_sample, id_prefix, regex_rows, report_rows):
    if [row["document_id"] for row in rows] != [
        row["document_id"] for row in expected_sample
    ]:
        raise RuntimeError("Committed labels do not match the fixed-seed sample.")

    for index, row in enumerate(rows, start=1):
        regex_row = regex_rows[row["document_id"]]
        report_row = report_rows[row["document_id"]]
        expected_values = {
            "validation_id": f"{id_prefix}{index:03d}",
            "application_number": regex_row["application_number"],
            "action_code": regex_row["action_code"],
            "project_name": regex_row["project_name"],
            "year": regex_row["year"],
            "community_district": regex_row["community_district"],
            "source_text_sha256": regex_row["source_text_sha256"],
            "official_pdf_url": report_row["official_pdf_url"],
        }
        for field, expected_value in expected_values.items():
            if row[field] != expected_value:
                raise RuntimeError(
                    f"Stale validation metadata for {row['document_id']}: {field}."
                )
        if row["coding_complete"] != "1":
            raise RuntimeError(f"Incomplete validation coding for {row['validation_id']}.")
        for field in VALIDATION_BINARY_FIELDS:
            if row[field] not in {"0", "1", "unclear"}:
                raise RuntimeError(f"Invalid {field} for {row['validation_id']}.")
        for field in POSITION_FIELDS:
            if row[field] not in {
                "support_or_request",
                "opposition",
                "none_or_procedural",
                "unclear",
            }:
                raise RuntimeError(f"Invalid {field} for {row['validation_id']}.")


def find_disagreements(rows, regex_rows):
    disagreements = []
    for row in rows:
        regex_row = regex_rows[row["document_id"]]
        for field in [*VALIDATION_BINARY_FIELDS, *POSITION_FIELDS, *COUNT_FIELDS]:
            manual_value = row[field]
            regex_value = regex_row[field]
            if manual_value in {"", "unclear"} or manual_value == regex_value:
                continue
            if field in VALIDATION_BINARY_FIELDS:
                error_type = "false_positive" if regex_value == "1" else "false_negative"
            elif field in COUNT_FIELDS and regex_value == "":
                error_type = "regex_missing"
            else:
                error_type = "mismatch"
            disagreements.append(
                {
                    "validation_id": row["validation_id"],
                    "document_id": row["document_id"],
                    "application_number": row["application_number"],
                    "year": row["year"],
                    "decade": regex_row["decade"],
                    "project_name": row["project_name"],
                    "field": field,
                    "manual_value": manual_value,
                    "regex_value": regex_value,
                    "error_type": error_type,
                    "evidence_pages": row["evidence_pages"],
                    "evidence_summary": row["evidence_summary"],
                    "coder_notes": row["coder_notes"],
                    "official_pdf_url": row["official_pdf_url"],
                }
            )
    return disagreements


if len(sys.argv) != 5:
    raise SystemExit(
        "Usage: compare_ulurp_cpc_regex_labels.py "
        "DEVELOPMENT_SAMPLE_SIZE DEVELOPMENT_SAMPLE_SEED "
        "HOLDOUT_SAMPLE_SIZE HOLDOUT_SAMPLE_SEED"
    )

development_sample_size = int(sys.argv[1])
development_sample_seed = int(sys.argv[2])
holdout_sample_size = int(sys.argv[3])
holdout_sample_seed = int(sys.argv[4])
if development_sample_size < 1 or holdout_sample_size < 1:
    raise SystemExit("Sample sizes must be positive.")

with Path("../input/ulurp_cpc_text_labels.csv").open(
    newline="", encoding="utf-8-sig"
) as input_file:
    regex_rows = {row["document_id"]: row for row in csv.DictReader(input_file)}

with Path("../input/ulurp_cpc_training_labels_jacob.csv").open(
    newline="", encoding="utf-8-sig"
) as input_file:
    training_rows = list(csv.DictReader(input_file))

with Path("../input/ulurp_cpc_regex_validation_labels_codex.csv").open(
    newline="", encoding="utf-8-sig"
) as input_file:
    validation_rows = list(csv.DictReader(input_file))

with Path("../input/ulurp_cpc_regex_holdout_labels_codex.csv").open(
    newline="", encoding="utf-8-sig"
) as input_file:
    holdout_rows = list(csv.DictReader(input_file))

with Path("../input/ulurp_cpc_report_manifest.csv").open(
    newline="", encoding="utf-8-sig"
) as input_file:
    report_rows = {row["document_id"]: row for row in csv.DictReader(input_file)}

if len(training_rows) != len({row["document_id"] for row in training_rows}):
    raise RuntimeError("Training labels are not unique by document_id.")
if len(validation_rows) != len({row["document_id"] for row in validation_rows}):
    raise RuntimeError("Validation labels are not unique by document_id.")
if len(holdout_rows) != len({row["document_id"] for row in holdout_rows}):
    raise RuntimeError("Holdout labels are not unique by document_id.")
if any(
    row["document_id"] not in regex_rows
    for row in training_rows + validation_rows + holdout_rows
):
    raise RuntimeError("At least one hand-labeled report is absent from the regex corpus.")

training_ids = {row["document_id"] for row in training_rows}
expected_validation_sample = draw_sample(
    regex_rows,
    training_ids,
    development_sample_size,
    development_sample_seed,
)
expected_validation_ids = {
    row["document_id"] for row in expected_validation_sample
}
expected_holdout_sample = draw_sample(
    regex_rows,
    training_ids | expected_validation_ids,
    holdout_sample_size,
    holdout_sample_seed,
)
if len(validation_rows) != development_sample_size:
    raise RuntimeError(f"Expected {development_sample_size} development labels.")
if len(holdout_rows) != holdout_sample_size:
    raise RuntimeError(f"Expected {holdout_sample_size} holdout labels.")
validate_sample(
    validation_rows, expected_validation_sample, "V", regex_rows, report_rows
)
validate_sample(holdout_rows, expected_holdout_sample, "H", regex_rows, report_rows)

training_summary = summarize(
    training_rows, regex_rows, TRAINING_BINARY_FIELDS, "all"
)
validation_summary = summarize(
    validation_rows, regex_rows, VALIDATION_BINARY_FIELDS, "all"
)
holdout_summary = summarize(holdout_rows, regex_rows, VALIDATION_BINARY_FIELDS, "all")
for decade in sorted(
    {regex_rows[row["document_id"]]["decade"] for row in validation_rows}
):
    validation_summary.extend(
        summarize(
            [
                row
                for row in validation_rows
                if regex_rows[row["document_id"]]["decade"] == decade
            ],
            regex_rows,
            VALIDATION_BINARY_FIELDS,
            decade,
        )
    )
for decade in sorted(
    {regex_rows[row["document_id"]]["decade"] for row in holdout_rows}
):
    holdout_summary.extend(
        summarize(
            [
                row
                for row in holdout_rows
                if regex_rows[row["document_id"]]["decade"] == decade
            ],
            regex_rows,
            VALIDATION_BINARY_FIELDS,
            decade,
        )
    )

for path, rows in (
    (Path("../output/ulurp_cpc_regex_training_agreement.csv"), training_summary),
    (Path("../output/ulurp_cpc_regex_validation_agreement.csv"), validation_summary),
    (Path("../output/ulurp_cpc_regex_holdout_agreement.csv"), holdout_summary),
):
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(
            output_file, fieldnames=rows[0].keys(), lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)

for path, rows in (
    (
        Path("../output/ulurp_cpc_regex_validation_disagreements.csv"),
        find_disagreements(validation_rows, regex_rows),
    ),
    (
        Path("../output/ulurp_cpc_regex_holdout_disagreements.csv"),
        find_disagreements(holdout_rows, regex_rows),
    ),
):
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=DISAGREEMENT_FIELDS,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)

print(
    f"Compared regex labels with {len(training_rows)} training reports and "
    f"{len(validation_rows)} development reports and "
    f"{len(holdout_rows)} holdout reports."
)
