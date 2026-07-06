#!/usr/bin/env python3

import csv
import hashlib
import math
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path


MANIFEST_PATH = Path("../input/ulurp_cpc_report_manifest.csv")
TEXT_SAMPLE_BYTE_LIMIT = 200000

BYTE_VALUES = set(range(256))
ALPHA_BYTE_VALUES = set(range(ord("A"), ord("Z") + 1)) | set(range(ord("a"), ord("z") + 1))
DIGIT_BYTE_VALUES = set(range(ord("0"), ord("9") + 1))
WHITESPACE_BYTE_VALUES = set(b" \n\r\t\f\v")
ASCII_PRINTABLE_BYTE_VALUES = set(range(32, 127)) | WHITESPACE_BYTE_VALUES
ASCII_BYTE_VALUES = set(range(128))

DELETE_NON_ALPHA_BYTES = bytes(sorted(BYTE_VALUES - ALPHA_BYTE_VALUES))
DELETE_NON_DIGIT_BYTES = bytes(sorted(BYTE_VALUES - DIGIT_BYTE_VALUES))
DELETE_NON_WHITESPACE_BYTES = bytes(sorted(BYTE_VALUES - WHITESPACE_BYTE_VALUES))
DELETE_NON_ASCII_PRINTABLE_BYTES = bytes(sorted(BYTE_VALUES - ASCII_PRINTABLE_BYTE_VALUES))
DELETE_NON_ASCII_BYTES = bytes(sorted(BYTE_VALUES - ASCII_BYTE_VALUES))

EXPECTED_PHRASES = [
    "city planning commission",
    "uniform land use review",
    "department of city planning",
    "community board",
    "borough president",
    "calendar no",
]


def as_int(value):
    try:
        if value in ("", None):
            return None
        return int(float(value))
    except ValueError:
        return None


def safe_divide(numerator, denominator):
    if denominator in (0, None):
        return None
    return numerator / denominator


def format_float(value, digits=4):
    if value is None:
        return ""
    return f"{value:.{digits}f}"


def clean_values(values):
    return [value for value in values if value is not None and not math.isnan(value)]


def quantile(values, probability):
    values = sorted(clean_values(values))
    if not values:
        return None
    if len(values) == 1:
        return values[0]

    index = (len(values) - 1) * probability
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return values[int(index)]
    return values[lower] + (values[upper] - values[lower]) * (index - lower)


def median(values):
    values = clean_values(values)
    if not values:
        return None
    return statistics.median(values)


def period_from_year(year):
    if year is None:
        return "missing_year"
    if year <= 1989:
        return "1975-1989"
    if year <= 1999:
        return "1990-1999"
    if year <= 2004:
        return "2000-2004"
    if year <= 2014:
        return "2005-2014"
    return "2015-2026"


def period_sort_key(period):
    if period == "missing_year":
        return 9999
    return int(period[:4])


def resolve_task_path(raw_path, manifest_real_path):
    if not raw_path:
        return None

    path = Path(raw_path)
    if path.is_absolute():
        return path

    task_root = manifest_real_path.parent.parent
    return task_root / "code" / path


def stable_sample_key(row):
    key = "|".join([
        str(row["corpus_reference_year"]),
        row["document_id"],
        row["raw_application_number"],
        row["project_id"],
    ])
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def sample_text_metrics(text_path, manifest_text_byte_count):
    if text_path is None or not text_path.exists():
        return {
            "sample_text_read_status": "text_path_missing",
            "sample_text_available_locally": 0,
            "sample_text_path_exists": 0,
            "text_metric_sample_byte_count": 0,
            "text_metric_sample_share": "",
            "word_count": "",
            "alpha_word_count": "",
            "alpha_char_share": "",
            "digit_char_share": "",
            "punct_symbol_char_share": "",
            "ascii_printable_share": "",
            "non_ascii_share": "",
            "single_char_alpha_word_share": "",
            "no_vowel_alpha_word_share": "",
            "long_alpha_word_share": "",
            "short_line_share": "",
            "median_line_length": "",
            "expected_phrase_count": "",
            "has_city_planning_commission_phrase": "",
            "has_uniform_land_use_review_phrase": "",
            "problem_reasons": "text_path_missing",
            "possible_extraction_problem": 0,
        }

    stat_result = text_path.stat()
    full_byte_count = stat_result.st_size
    if manifest_text_byte_count not in (None, 0):
        full_byte_count = manifest_text_byte_count

    if getattr(stat_result, "st_blocks", 1) == 0 and stat_result.st_size > 0:
        return {
            "sample_text_read_status": "dataless_skipped",
            "sample_text_available_locally": 0,
            "sample_text_path_exists": 1,
            "text_metric_sample_byte_count": 0,
            "text_metric_sample_share": 0,
            "word_count": "",
            "alpha_word_count": "",
            "alpha_char_share": "",
            "digit_char_share": "",
            "punct_symbol_char_share": "",
            "ascii_printable_share": "",
            "non_ascii_share": "",
            "single_char_alpha_word_share": "",
            "no_vowel_alpha_word_share": "",
            "long_alpha_word_share": "",
            "short_line_share": "",
            "median_line_length": "",
            "expected_phrase_count": "",
            "has_city_planning_commission_phrase": "",
            "has_uniform_land_use_review_phrase": "",
            "problem_reasons": "local_file_dataless",
            "possible_extraction_problem": 0,
        }

    with text_path.open("rb") as text_file:
        sample_bytes = text_file.read(TEXT_SAMPLE_BYTE_LIMIT)

    sample_byte_count = len(sample_bytes)
    if sample_byte_count == 0:
        return {
            "sample_text_read_status": "empty_text",
            "sample_text_available_locally": 1,
            "sample_text_path_exists": 1,
            "text_metric_sample_byte_count": 0,
            "text_metric_sample_share": 0,
            "word_count": 0,
            "alpha_word_count": 0,
            "alpha_char_share": "",
            "digit_char_share": "",
            "punct_symbol_char_share": "",
            "ascii_printable_share": "",
            "non_ascii_share": "",
            "single_char_alpha_word_share": "",
            "no_vowel_alpha_word_share": "",
            "long_alpha_word_share": "",
            "short_line_share": "",
            "median_line_length": "",
            "expected_phrase_count": 0,
            "has_city_planning_commission_phrase": 0,
            "has_uniform_land_use_review_phrase": 0,
            "problem_reasons": "empty_text",
            "possible_extraction_problem": 1,
        }

    alpha_char_count = len(sample_bytes.translate(None, DELETE_NON_ALPHA_BYTES))
    digit_char_count = len(sample_bytes.translate(None, DELETE_NON_DIGIT_BYTES))
    whitespace_char_count = len(sample_bytes.translate(None, DELETE_NON_WHITESPACE_BYTES))
    ascii_printable_count = len(sample_bytes.translate(None, DELETE_NON_ASCII_PRINTABLE_BYTES))
    ascii_count = len(sample_bytes.translate(None, DELETE_NON_ASCII_BYTES))
    nonspace_char_count = sample_byte_count - whitespace_char_count
    punct_symbol_char_count = sample_byte_count - alpha_char_count - digit_char_count - whitespace_char_count

    sampled_text = sample_bytes.decode("utf-8", errors="replace")
    words = re.findall(r"[A-Za-z]+(?:'[A-Za-z]+)?|\d+(?:\.\d+)?", sampled_text)
    alpha_words = [word for word in words if re.search(r"[A-Za-z]", word)]
    alpha_word_lengths = [len(re.sub(r"[^A-Za-z]", "", word)) for word in alpha_words]
    no_vowel_alpha_words = [
        word for word in alpha_words
        if len(re.sub(r"[^A-Za-z]", "", word)) >= 4 and not re.search(r"[AEIOUYaeiouy]", word)
    ]
    long_alpha_words = [
        word for word in alpha_words
        if len(re.sub(r"[^A-Za-z]", "", word)) > 20
    ]

    lines = sampled_text.splitlines()
    line_lengths = [len(line.strip()) for line in lines if line.strip()]
    short_line_count = sum(1 for length in line_lengths if length <= 3)

    normalized_text = re.sub(r"\s+", " ", sampled_text).lower()
    expected_phrase_hits = {
        phrase: int(phrase in normalized_text)
        for phrase in EXPECTED_PHRASES
    }

    alpha_char_share = safe_divide(alpha_char_count, nonspace_char_count)
    single_char_alpha_word_share = safe_divide(
        sum(1 for length in alpha_word_lengths if length == 1),
        len(alpha_word_lengths),
    )
    no_vowel_alpha_word_share = safe_divide(len(no_vowel_alpha_words), len(alpha_word_lengths))
    short_line_share = safe_divide(short_line_count, len(line_lengths))

    problem_reasons = []
    if full_byte_count < 1000 or len(words) < 150:
        problem_reasons.append("very_short_text")
    if alpha_char_share is not None and alpha_char_share < 0.45:
        problem_reasons.append("low_alpha_character_share")
    if len(alpha_word_lengths) >= 100 and single_char_alpha_word_share is not None and single_char_alpha_word_share > 0.20:
        problem_reasons.append("many_single_character_words")
    if len(alpha_word_lengths) >= 100 and no_vowel_alpha_word_share is not None and no_vowel_alpha_word_share > 0.25:
        problem_reasons.append("many_no_vowel_words")
    if len(line_lengths) >= 50 and short_line_share is not None and short_line_share > 0.35:
        problem_reasons.append("fragmented_lines")

    return {
        "sample_text_read_status": "read_sampled",
        "sample_text_available_locally": 1,
        "sample_text_path_exists": 1,
        "text_metric_sample_byte_count": sample_byte_count,
        "text_metric_sample_share": safe_divide(sample_byte_count, full_byte_count),
        "word_count": len(words),
        "alpha_word_count": len(alpha_words),
        "alpha_char_share": alpha_char_share,
        "digit_char_share": safe_divide(digit_char_count, nonspace_char_count),
        "punct_symbol_char_share": safe_divide(punct_symbol_char_count, nonspace_char_count),
        "ascii_printable_share": safe_divide(ascii_printable_count, sample_byte_count),
        "non_ascii_share": safe_divide(sample_byte_count - ascii_count, sample_byte_count),
        "single_char_alpha_word_share": single_char_alpha_word_share,
        "no_vowel_alpha_word_share": no_vowel_alpha_word_share,
        "long_alpha_word_share": safe_divide(len(long_alpha_words), len(alpha_word_lengths)),
        "short_line_share": short_line_share,
        "median_line_length": median(line_lengths),
        "expected_phrase_count": sum(expected_phrase_hits.values()),
        "has_city_planning_commission_phrase": expected_phrase_hits["city planning commission"],
        "has_uniform_land_use_review_phrase": expected_phrase_hits["uniform land use review"],
        "problem_reasons": ";".join(problem_reasons),
        "possible_extraction_problem": int(bool(problem_reasons)),
    }


def summarize_group(full_rows, sample_rows, group_name, group_value):
    usable_rows = [row for row in full_rows if row["has_usable_text"] == 1]
    direct_rows = [row for row in full_rows if row["usable_text_source_type"] == "direct_cpc_report"]
    sibling_rows = [row for row in full_rows if row["usable_text_source_type"] == "sibling_project_cpc_report"]
    readable_sample_rows = [row for row in sample_rows if row["sample_text_read_status"] == "read_sampled"]
    dataless_rows = [row for row in sample_rows if row["sample_text_read_status"] == "dataless_skipped"]
    missing_path_rows = [row for row in sample_rows if row["sample_text_read_status"] == "text_path_missing"]
    problem_rows = [row for row in readable_sample_rows if row["possible_extraction_problem"] == 1]

    return {
        group_name: group_value,
        "applications": len(full_rows),
        "usable_text_count": len(usable_rows),
        "usable_text_share": format_float(safe_divide(len(usable_rows), len(full_rows))),
        "direct_text_count": len(direct_rows),
        "sibling_text_count": len(sibling_rows),
        "missing_usable_text_count": len(full_rows) - len(usable_rows),
        "missing_usable_text_share": format_float(safe_divide(len(full_rows) - len(usable_rows), len(full_rows))),
        "usable_text_char_count_p10": format_float(quantile([row["usable_text_char_count"] for row in usable_rows], 0.10), 1),
        "usable_text_char_count_median": format_float(median([row["usable_text_char_count"] for row in usable_rows]), 1),
        "usable_text_char_count_p90": format_float(quantile([row["usable_text_char_count"] for row in usable_rows], 0.90), 1),
        "quality_sample_count": len(sample_rows),
        "quality_sample_readable_count": len(readable_sample_rows),
        "quality_sample_dataless_count": len(dataless_rows),
        "quality_sample_dataless_share": format_float(safe_divide(len(dataless_rows), len(sample_rows))),
        "quality_sample_missing_path_count": len(missing_path_rows),
        "possible_extraction_problem_count": len(problem_rows),
        "possible_extraction_problem_share_readable_sample": format_float(safe_divide(len(problem_rows), len(readable_sample_rows))),
        "sample_alpha_char_share_median": format_float(median([row["alpha_char_share"] for row in readable_sample_rows])),
        "sample_ascii_printable_share_median": format_float(median([row["ascii_printable_share"] for row in readable_sample_rows])),
        "sample_single_char_alpha_word_share_median": format_float(median([row["single_char_alpha_word_share"] for row in readable_sample_rows])),
        "sample_no_vowel_alpha_word_share_median": format_float(median([row["no_vowel_alpha_word_share"] for row in readable_sample_rows])),
        "sample_short_line_share_median": format_float(median([row["short_line_share"] for row in readable_sample_rows])),
        "sample_expected_phrase_count_median": format_float(median([row["expected_phrase_count"] for row in readable_sample_rows]), 1),
        "sample_city_planning_commission_phrase_share": format_float(
            safe_divide(sum(row["has_city_planning_commission_phrase"] for row in readable_sample_rows), len(readable_sample_rows))
        ),
        "sample_uniform_land_use_review_phrase_share": format_float(
            safe_divide(sum(row["has_uniform_land_use_review_phrase"] for row in readable_sample_rows), len(readable_sample_rows))
        ),
    }


def write_csv(path, rows, fieldnames):
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_base_rows(manifest_rows, manifest_real_path):
    base_rows = []
    for row in manifest_rows:
        year = as_int(row.get("corpus_reference_year"))
        usable_text_char_count = as_int(row.get("usable_text_char_count")) or 0
        has_usable_text = int(bool(row.get("usable_text_status", "").strip()) and usable_text_char_count > 0)

        base_rows.append({
            "document_id": row.get("document_id", ""),
            "project_id": row.get("project_id", ""),
            "project_name": row.get("project_name", ""),
            "corpus_reference_year": year if year is not None else "",
            "period": period_from_year(year),
            "raw_application_number": row.get("raw_application_number", ""),
            "application_prefix": row.get("application_prefix", ""),
            "parsed_action_code": row.get("parsed_action_code", ""),
            "borough_name": row.get("borough_name", ""),
            "download_status": row.get("download_status", ""),
            "text_status": row.get("text_status", ""),
            "usable_text_source_type": row.get("usable_text_source_type", ""),
            "usable_text_status": row.get("usable_text_status", ""),
            "usable_text_char_count": usable_text_char_count,
            "has_usable_text": has_usable_text,
            "usable_local_text_path": row.get("usable_local_text_path", ""),
            "resolved_usable_text_path": resolve_task_path(row.get("usable_local_text_path", ""), manifest_real_path),
            "project_page_url": row.get("project_page_url", ""),
            "source_doc": row.get("source_doc", ""),
        })
    return base_rows


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python3 audit_ulurp_cpc_text_extraction_consistency.py SAMPLE_DOCUMENTS_PER_YEAR")
    sample_documents_per_year = int(sys.argv[1])

    manifest_real_path = MANIFEST_PATH.resolve()
    with MANIFEST_PATH.open(newline="", encoding="utf-8") as manifest_file:
        manifest_rows = list(csv.DictReader(manifest_file))

    base_rows = build_base_rows(manifest_rows, manifest_real_path)

    usable_rows_by_year = defaultdict(list)
    for row in base_rows:
        if row["has_usable_text"] == 1 and row["corpus_reference_year"] != "":
            usable_rows_by_year[row["corpus_reference_year"]].append(row)

    sampled_rows = []
    for year in sorted(usable_rows_by_year):
        year_rows = sorted(usable_rows_by_year[year], key=stable_sample_key)
        sampled_rows.extend(year_rows[:sample_documents_per_year])

    sample_output_rows = []
    for row_number, row in enumerate(sampled_rows, start=1):
        if row_number == 1 or row_number % 100 == 0:
            print(f"Reading sampled text {row_number} of {len(sampled_rows)}", flush=True)

        metrics = sample_text_metrics(
            row["resolved_usable_text_path"],
            row["usable_text_char_count"],
        )
        output_row = dict(row)
        output_row.pop("resolved_usable_text_path")
        output_row.update(metrics)
        sample_output_rows.append(output_row)

    write_csv(
        Path("../output/ulurp_cpc_text_extraction_sample_document_metrics.csv"),
        sample_output_rows,
        list(sample_output_rows[0].keys()),
    )

    base_rows_by_year = defaultdict(list)
    sample_rows_by_year = defaultdict(list)
    base_rows_by_period = defaultdict(list)
    sample_rows_by_period = defaultdict(list)
    for row in base_rows:
        base_rows_by_year[row["corpus_reference_year"]].append(row)
        base_rows_by_period[row["period"]].append(row)
    for row in sample_output_rows:
        sample_rows_by_year[row["corpus_reference_year"]].append(row)
        sample_rows_by_period[row["period"]].append(row)

    year_rows = [
        summarize_group(
            base_rows_by_year[year],
            sample_rows_by_year[year],
            "corpus_reference_year",
            year,
        )
        for year in sorted(year for year in base_rows_by_year if year != "")
    ]
    write_csv(
        Path("../output/ulurp_cpc_text_extraction_by_year.csv"),
        year_rows,
        list(year_rows[0].keys()),
    )

    period_rows = [
        summarize_group(
            base_rows_by_period[period],
            sample_rows_by_period[period],
            "period",
            period,
        )
        for period in sorted(base_rows_by_period, key=period_sort_key)
    ]
    write_csv(
        Path("../output/ulurp_cpc_text_extraction_by_period.csv"),
        period_rows,
        list(period_rows[0].keys()),
    )

    flagged_examples = []
    for period in sorted(base_rows_by_period, key=period_sort_key):
        missing_rows = [
            row for row in base_rows_by_period[period]
            if row["has_usable_text"] == 0
        ]
        for row in missing_rows[:5]:
            flagged_examples.append({
                "example_type": "missing_usable_text",
                "problem_reasons": "no_usable_text",
                **{field: row[field] for field in [
                    "period",
                    "corpus_reference_year",
                    "project_id",
                    "project_name",
                    "raw_application_number",
                    "parsed_action_code",
                    "borough_name",
                    "usable_text_status",
                    "usable_text_source_type",
                    "usable_text_char_count",
                    "usable_local_text_path",
                    "source_doc",
                    "project_page_url",
                ]},
            })

        dataless_rows = [
            row for row in sample_rows_by_period[period]
            if row["sample_text_read_status"] == "dataless_skipped"
        ]
        for row in dataless_rows[:10]:
            flagged_examples.append({
                "example_type": "sample_local_file_dataless",
                "problem_reasons": row["problem_reasons"],
                **{field: row[field] for field in [
                    "period",
                    "corpus_reference_year",
                    "project_id",
                    "project_name",
                    "raw_application_number",
                    "parsed_action_code",
                    "borough_name",
                    "usable_text_status",
                    "usable_text_source_type",
                    "usable_text_char_count",
                    "usable_local_text_path",
                    "source_doc",
                    "project_page_url",
                ]},
            })

        problem_rows = [
            row for row in sample_rows_by_period[period]
            if row["possible_extraction_problem"] == 1
        ]
        problem_rows = sorted(
            problem_rows,
            key=lambda row: (
                row["usable_text_char_count"],
                row["raw_application_number"],
            ),
        )
        for row in problem_rows[:10]:
            flagged_examples.append({
                "example_type": "sample_possible_extraction_problem",
                "problem_reasons": row["problem_reasons"],
                **{field: row[field] for field in [
                    "period",
                    "corpus_reference_year",
                    "project_id",
                    "project_name",
                    "raw_application_number",
                    "parsed_action_code",
                    "borough_name",
                    "usable_text_status",
                    "usable_text_source_type",
                    "usable_text_char_count",
                    "usable_local_text_path",
                    "source_doc",
                    "project_page_url",
                ]},
            })

    write_csv(
        Path("../output/ulurp_cpc_text_extraction_flagged_examples.csv"),
        flagged_examples,
        list(flagged_examples[0].keys()),
    )


if __name__ == "__main__":
    main()
