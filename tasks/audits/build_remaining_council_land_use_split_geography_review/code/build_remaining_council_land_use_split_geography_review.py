# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/build_remaining_council_land_use_split_geography_review/code")
# batch_size <- 5
# max_bundles <- 0
# prompt_version <- "v1"

from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path


if len(sys.argv) != 4:
    raise RuntimeError("Usage: python3 build_remaining_council_land_use_split_geography_review.py BATCH_SIZE MAX_BUNDLES PROMPT_VERSION")

batch_size = int(sys.argv[1])
max_bundles = int(sys.argv[2])
prompt_version = sys.argv[3]

if batch_size <= 0:
    raise RuntimeError("BATCH_SIZE must be positive.")
if max_bundles < 0:
    raise RuntimeError("MAX_BUNDLES must be nonnegative.")
if prompt_version.strip() == "":
    raise RuntimeError("PROMPT_VERSION must be nonempty.")


def read_csv(path: str) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: str, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    temp_path = Path(path).with_suffix(Path(path).suffix + ".tmp")
    with open(temp_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
    temp_path.replace(path)


def write_text(path: str, value: str) -> None:
    temp_path = Path(path).with_suffix(Path(path).suffix + ".tmp")
    temp_path.write_text(value, encoding="utf-8")
    temp_path.replace(path)


def clean_space(value: object) -> str:
    return re.sub(r"\s+", " ", "" if value is None else str(value)).strip()


def split_values(value: object) -> list[str]:
    if value is None:
        return []
    return [part.strip() for part in str(value).split(";") if part.strip()]


def collapse_values(values: list[object]) -> str:
    output = []
    for value in values:
        text = clean_space(value)
        if text and text not in output:
            output.append(text)
    return "; ".join(output)


def collapse_long(values: list[object], max_items: int = 8, max_chars: int = 1800) -> str:
    text = collapse_values(values[:max_items])
    if len(text) > max_chars:
        return text[: max_chars - 3].rstrip() + "..."
    return text


def as_int(value: object) -> int:
    try:
        return int(float(str(value)))
    except ValueError:
        return 0


def is_true(value: object) -> bool:
    return str(value).strip().lower() == "true"


def normalize_title_key(value: object) -> str:
    text = clean_space(value).lower()
    text = re.sub(r"\b(?:application|app)\s*(?:no\.?|number)?\s*[cnm]?\s*\d{6,8}\s*(?:\([a-z0-9]+\)\s*)?[a-z]{2,4}\b", " ", text)
    text = re.sub(r"\b(l\.u\.|lu|res|resolution|approving|decision|city planning commission|pursuant|section|sections|charter|new york city|borough|community district|council district)\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split()[:12])


def application_suffixes(value: object) -> list[str]:
    suffixes = []
    for key in split_values(value):
        match = re.search(r"([A-Z]{2,4})$", key.strip().upper())
        if match and match.group(1) not in suffixes:
            suffixes.append(match.group(1))
    return suffixes


def has_address_or_lot_language(value: object) -> bool:
    text = clean_space(value).lower()
    return bool(
        re.search(r"\b(block|lot|lots|located at|bounded by|between|a/k/a|aka|street|avenue|boulevard|road|place|drive|parkway)\b", text)
    )


def deterministic_category(row: dict[str, str]) -> str:
    text = clean_space(row.get("title", "")).lower()
    suffixes = application_suffixes(row.get("application_keys", ""))

    if "citywide" in text or "city-wide" in text:
        return "likely_citywide_or_text_only"
    if "revocable consent" in text or "sidewalk cafe" in text or "franchise" in text:
        return "likely_franchise_consent_or_street_use"
    if "landmark" in text or "historic district" in text or any(suffix.startswith("HK") for suffix in suffixes):
        return "likely_landmark_or_historic_designation"
    if "urban development action area" in text or "udaap" in text or any(suffix.startswith("HA") for suffix in suffixes):
        return "likely_housing_property_action"
    if "site selection" in text or "acquisition" in text or "disposition" in text or "school" in text:
        return "likely_public_facility_or_property_action"
    if any(suffix.startswith("MM") for suffix in suffixes):
        return "likely_city_map_or_street_mapping"
    if row.get("zap_project_ids", "").strip() or any(suffix.startswith(("ZM", "ZS", "ZR")) for suffix in suffixes) or has_address_or_lot_language(text):
        return "likely_project_geography_needed"
    return "unknown_needs_review"


def priority_for_category(category: str) -> int:
    if category == "likely_project_geography_needed":
        return 1
    if category in {"likely_housing_property_action", "likely_public_facility_or_property_action", "likely_city_map_or_street_mapping"}:
        return 2
    if category == "unknown_needs_review":
        return 3
    if category == "mixed_bundle_needs_review":
        return 4
    return 5


def review_action_for_category(category: str) -> str:
    if category in {
        "likely_project_geography_needed",
        "likely_housing_property_action",
        "likely_public_facility_or_property_action",
        "likely_city_map_or_street_mapping",
        "unknown_needs_review",
        "mixed_bundle_needs_review",
    }:
        return "send_to_chatgpt_for_geography_or_not_applicable_classification"
    return "send_to_chatgpt_to_confirm_not_local_project_geography"


def bundle_key(row: dict[str, str]) -> str:
    vote_key = "|".join(
        [
            row.get("query_year", ""),
            row.get("vote_source", ""),
            row.get("vote_date", ""),
            row.get("vote_margin", ""),
            row.get("affirmative_count", ""),
            row.get("negative_count", ""),
            row.get("abstain_count", ""),
        ]
    )
    if row.get("zap_project_ids", "").strip():
        item_key = "zap:" + row["zap_project_ids"].strip()
    elif row.get("application_keys", "").strip():
        item_key = "app:" + row["application_keys"].strip()
    else:
        item_key = "title:" + normalize_title_key(row.get("title", ""))
    return vote_key + "|" + item_key


decision_rows = read_csv("../input/council_land_use_decision_panel.csv")
nonaffirmative_rows = read_csv("../input/council_land_use_split_vote_nonaffirmative_member_rows.csv")

nonaffirmative_by_matter: dict[str, list[str]] = {}
for row in nonaffirmative_rows:
    matter_id = row.get("matter_id", "")
    vote = clean_space(row.get("vote", ""))
    name = clean_space(row.get("person_name", ""))
    if matter_id and name and vote:
        nonaffirmative_by_matter.setdefault(matter_id, [])
        value = f"{name}: {vote}"
        if value not in nonaffirmative_by_matter[matter_id]:
            nonaffirmative_by_matter[matter_id].append(value)

matter_rows = []
for row in decision_rows:
    split_vote = is_true(row.get("matter_in_main_vote_sample", "")) and (
        as_int(row.get("negative_count", "")) > 0 or as_int(row.get("abstain_count", "")) > 0
    )
    missing_geography = not is_true(row.get("has_affected_council_district", ""))
    if not split_vote or not missing_geography:
        continue

    category = deterministic_category(row)
    matter_rows.append(
        {
            "query_year": row.get("query_year", ""),
            "matter_id": row.get("matter_id", ""),
            "matter_file": row.get("matter_file", ""),
            "matter_type": row.get("matter_type", ""),
            "disposition_group": row.get("disposition_group", ""),
            "vote_source": row.get("vote_source", ""),
            "vote_date": row.get("vote_date", ""),
            "vote_margin": row.get("vote_margin", ""),
            "affirmative_count": row.get("affirmative_count", ""),
            "negative_count": row.get("negative_count", ""),
            "abstain_count": row.get("abstain_count", ""),
            "application_keys": row.get("application_keys", ""),
            "application_suffixes": collapse_values(application_suffixes(row.get("application_keys", ""))),
            "zap_project_ids": row.get("zap_project_ids", ""),
            "zap_project_names": row.get("zap_project_names", ""),
            "affected_district_source": row.get("affected_district_source", ""),
            "deterministic_category": category,
            "review_action": review_action_for_category(category),
            "bundle_key": bundle_key(row),
            "nonaffirmative_member_votes": collapse_values(nonaffirmative_by_matter.get(row.get("matter_id", ""), [])),
            "title": row.get("title", ""),
            "matter_url": row.get("matter_url", ""),
            "history_detail_url": row.get("history_detail_url", ""),
        }
    )

if not matter_rows:
    raise RuntimeError("No remaining split-vote missing-geography matter rows were found.")

bundles_by_key: dict[str, list[dict[str, object]]] = {}
for row in matter_rows:
    bundles_by_key.setdefault(str(row["bundle_key"]), []).append(row)

bundle_rows = []
for rows in bundles_by_key.values():
    categories = []
    for row in rows:
        category = str(row["deterministic_category"])
        if category not in categories:
            categories.append(category)
    bundle_category = categories[0] if len(categories) == 1 else "mixed_bundle_needs_review"
    first = sorted(rows, key=lambda x: (str(x["query_year"]), str(x["vote_date"]), str(x["matter_file"])))[0]
    bundle_rows.append(
        {
            "stable_id": "",
            "query_year": first["query_year"],
            "vote_source": first["vote_source"],
            "vote_date": first["vote_date"],
            "vote_margin": first["vote_margin"],
            "affirmative_count": first["affirmative_count"],
            "negative_count": first["negative_count"],
            "abstain_count": first["abstain_count"],
            "matter_rows": len(rows),
            "land_use_application_rows": sum(1 for row in rows if row["matter_type"] == "Land Use Application"),
            "resolution_rows": sum(1 for row in rows if row["matter_type"] == "Resolution"),
            "call_up_rows": sum(1 for row in rows if row["matter_type"] == "Land Use Call-Up"),
            "matter_files": collapse_values([row["matter_file"] for row in rows]),
            "matter_ids": collapse_values([row["matter_id"] for row in rows]),
            "application_keys": collapse_values([key for row in rows for key in split_values(row["application_keys"])]),
            "application_suffixes": collapse_values([suffix for row in rows for suffix in split_values(row["application_suffixes"])]),
            "zap_project_ids": collapse_values([row["zap_project_ids"] for row in rows]),
            "zap_project_names": collapse_values([row["zap_project_names"] for row in rows]),
            "nonaffirmative_member_votes": collapse_values(
                [vote for row in rows for vote in split_values(row["nonaffirmative_member_votes"])]
            ),
            "deterministic_category": bundle_category,
            "review_action": review_action_for_category(bundle_category),
            "title_examples": collapse_long([row["title"] for row in rows], max_items=4),
            "matter_urls": collapse_long([row["matter_url"] for row in rows], max_items=4, max_chars=1200),
            "history_detail_urls": collapse_long([row["history_detail_url"] for row in rows], max_items=4, max_chars=1200),
            "bundle_key": first["bundle_key"],
        }
    )

bundle_rows = sorted(
    bundle_rows,
    key=lambda row: (
        priority_for_category(str(row["deterministic_category"])),
        int(row["query_year"]),
        str(row["vote_date"]),
        str(row["matter_files"]),
    ),
)
for index, row in enumerate(bundle_rows, start=1):
    row["review_id"] = f"rem_split_geo_{index:04d}"
    row["stable_id"] = row["review_id"]
    row["review_priority"] = priority_for_category(str(row["deterministic_category"]))

if max_bundles > 0:
    batch_rows = bundle_rows[:max_bundles]
else:
    batch_rows = bundle_rows

prompt = f"""# Remaining Council Land-Use Split-Vote Geography Review

You are helping audit NYC Council land-use roll-call matters that still lack affected Council district geography after deterministic Legistar/ZAP matching and prior AI/manual repair passes.

For each review item, decide whether there is a defensible affected Council district assignment for the local-member deference series.

Return one JSON object per line, with exactly these keys:

- `review_id`
- `status`: one of `project_geography`, `citywide_or_text_only`, `not_project_local_geography`, `mixed_bundle`, `unresolved`, `ambiguous`
- `affected_council_districts`: semicolon-separated district numbers, or empty if not applicable
- `confidence`: one of `high`, `medium`, `low`
- `evidence_basis`: one of `official_source`, `legistar_title`, `news_or_secondary_source`, `title_inference`, `no_evidence`
- `official_sources_checked`: short text naming official records checked, or empty
- `source_urls`: semicolon-separated URLs used, or empty
- `local_member_names_if_known`: semicolon-separated names, or empty
- `needs_human_review`: `true` or `false`
- `short_explanation`: one sentence explaining the classification

Rules:
- Prefer official Legistar, CPC, ZAP, ULURP, DCP, LPC, HPD, or Council records.
- If the item is citywide, text-only, a franchise/consent action, a landmark designation, or otherwise lacks a project-specific affected district, use `citywide_or_text_only` or `not_project_local_geography`.
- If the item contains multiple distinct projects with different geographies, use `mixed_bundle` and list all defensible affected districts.
- Do not invent a district from a borough or community district alone.
- For multi-district projects, list every affected Council district.
- If you cannot verify enough to assign districts, use `unresolved` or `ambiguous`.

Prompt version: {prompt_version}
"""

write_text("../output/council_land_use_remaining_split_geography_prompt.md", prompt)

batch_manifest_rows = []
batch_json_rows = []
for batch_start in range(0, len(batch_rows), batch_size):
    batch_index = batch_start // batch_size + 1
    rows = batch_rows[batch_start : batch_start + batch_size]
    batch_path = f"../output/batches/council_land_use_remaining_split_geography_batch_{batch_index:03d}.md"
    body = [prompt, "\n## Review Items\n"]
    for row in rows:
        body.append(f"\n### {row['review_id']}\n")
        body.append(f"- Deterministic triage: {row['deterministic_category']}\n")
        body.append(f"- Vote: {row['query_year']} | {row['vote_source']} | {row['vote_date']} | {row['vote_margin']}\n")
        body.append(f"- Matter files: {row['matter_files']}\n")
        body.append(f"- Application keys: {row['application_keys']}\n")
        body.append(f"- ZAP project IDs/names: {row['zap_project_ids']} | {row['zap_project_names']}\n")
        body.append(f"- Nonaffirmative member votes: {row['nonaffirmative_member_votes']}\n")
        body.append(f"- Title examples: {row['title_examples']}\n")
        body.append(f"- Legistar URLs: {row['matter_urls']}\n")
        body.append(f"- Vote detail URLs: {row['history_detail_urls']}\n")
    write_text(batch_path, "".join(body))
    batch_manifest_rows.append(
        {
            "batch_id": f"batch_{batch_index:03d}",
            "batch_path": batch_path,
            "bundle_count": len(rows),
            "first_review_priority": rows[0]["review_priority"],
            "last_review_priority": rows[-1]["review_priority"],
            "first_review_id": rows[0]["review_id"],
            "last_review_id": rows[-1]["review_id"],
            "char_count": len("".join(body)),
            "review_ids": collapse_values([row["review_id"] for row in rows]),
        }
    )
    batch_json_rows.append(
        {
            "batch_id": f"batch_{batch_index:03d}",
            "batch_path": batch_path,
            "review_ids": [row["review_id"] for row in rows],
        }
    )

next_batch_path = ""
if batch_manifest_rows:
    next_batch_path = str(batch_manifest_rows[0]["batch_path"])
    write_text("../output/council_land_use_remaining_split_geography_next_batch.md", Path(next_batch_path).read_text(encoding="utf-8"))
else:
    write_text("../output/council_land_use_remaining_split_geography_next_batch.md", "No remaining bundles.\n")

with open("../output/council_land_use_remaining_split_geography_batches.jsonl.tmp", "w", encoding="utf-8") as handle:
    for row in batch_json_rows:
        handle.write(json.dumps(row) + "\n")
Path("../output/council_land_use_remaining_split_geography_batches.jsonl.tmp").replace(
    "../output/council_land_use_remaining_split_geography_batches.jsonl"
)

valid_statuses = {
    "project_geography",
    "citywide_or_text_only",
    "not_project_local_geography",
    "mixed_bundle",
    "unresolved",
    "ambiguous",
}
valid_confidences = {"high", "medium", "low"}
valid_evidence = {"official_source", "legistar_title", "news_or_secondary_source", "title_inference", "no_evidence"}

responses = []
response_errors = []
with open("chatgpt_remaining_split_geography_responses.jsonl", encoding="utf-8") as handle:
    for line_number, line in enumerate(handle, start=1):
        text = line.strip()
        if not text:
            continue
        try:
            row = json.loads(text)
        except json.JSONDecodeError as error:
            response_errors.append(
                {
                    "response_line_number": line_number,
                    "review_id": "",
                    "validation_errors": f"json_parse_error: {error}",
                    "raw_line": text,
                }
            )
            continue
        errors = []
        review_id = clean_space(row.get("review_id", ""))
        status = clean_space(row.get("status", ""))
        confidence = clean_space(row.get("confidence", ""))
        evidence_basis = clean_space(row.get("evidence_basis", ""))
        districts = clean_space(row.get("affected_council_districts", ""))
        needs_human_review = str(row.get("needs_human_review", "")).strip().lower()

        if review_id not in {str(queue_row["review_id"]) for queue_row in bundle_rows}:
            errors.append("review_id_not_in_queue")
        if status not in valid_statuses:
            errors.append("invalid_status")
        if confidence not in valid_confidences:
            errors.append("invalid_confidence")
        if evidence_basis not in valid_evidence:
            errors.append("invalid_evidence_basis")
        if needs_human_review not in {"true", "false"}:
            errors.append("invalid_needs_human_review")
        if districts and not re.fullmatch(r"\d{1,2}(;\s*\d{1,2})*", districts):
            errors.append("invalid_district_format")
        for district in re.findall(r"\d{1,2}", districts):
            if int(district) < 1 or int(district) > 51:
                errors.append("district_out_of_range")
        if status in {"project_geography", "mixed_bundle"} and districts == "":
            errors.append("project_status_requires_district")
        if status in {"citywide_or_text_only", "not_project_local_geography"} and districts != "":
            errors.append("nonproject_status_should_not_have_district")

        parsed = {
            "response_line_number": line_number,
            "review_id": review_id,
            "status": status,
            "affected_council_districts": districts,
            "confidence": confidence,
            "evidence_basis": evidence_basis,
            "official_sources_checked": clean_space(row.get("official_sources_checked", "")),
            "source_urls": clean_space(row.get("source_urls", "")),
            "local_member_names_if_known": clean_space(row.get("local_member_names_if_known", "")),
            "needs_human_review": needs_human_review,
            "short_explanation": clean_space(row.get("short_explanation", "")),
        }
        if errors:
            response_errors.append(
                {
                    "response_line_number": line_number,
                    "review_id": review_id,
                    "validation_errors": collapse_values(errors),
                    "raw_line": text,
                }
            )
        else:
            responses.append(parsed)

response_ids = {row["review_id"] for row in responses}
missing_response_rows = [
    {
        "review_id": row["review_id"],
        "review_priority": row["review_priority"],
        "deterministic_category": row["deterministic_category"],
        "query_year": row["query_year"],
        "vote_date": row["vote_date"],
        "vote_margin": row["vote_margin"],
        "matter_rows": row["matter_rows"],
        "matter_files": row["matter_files"],
        "application_keys": row["application_keys"],
        "zap_project_names": row["zap_project_names"],
    }
    for row in batch_rows
    if row["review_id"] not in response_ids
]

next_batch_path = ""
for row in batch_manifest_rows:
    if any(review_id not in response_ids for review_id in split_values(row["review_ids"])):
        next_batch_path = str(row["batch_path"])
        break
if next_batch_path:
    write_text(
        "../output/council_land_use_remaining_split_geography_next_batch.md",
        Path(next_batch_path).read_text(encoding="utf-8"),
    )
else:
    write_text("../output/council_land_use_remaining_split_geography_next_batch.md", "No remaining bundles.\n")

response_by_id = {row["review_id"]: row for row in responses}
candidate_rows = []
for row in bundle_rows:
    response = response_by_id.get(str(row["review_id"]), {})
    status = response.get("status", "")
    confidence = response.get("confidence", "")
    if not response:
        candidate_category = "awaiting_chatgpt_review"
    elif status in {"project_geography", "mixed_bundle"} and confidence == "high" and response.get("evidence_basis") == "official_source":
        candidate_category = "official_project_geography_candidate"
    elif status in {"project_geography", "mixed_bundle"}:
        candidate_category = "project_geography_needs_human_verification"
    elif status in {"citywide_or_text_only", "not_project_local_geography"}:
        candidate_category = "not_local_project_geography_candidate"
    elif status in {"unresolved", "ambiguous"}:
        candidate_category = "unresolved_after_chatgpt_review"
    else:
        candidate_category = "response_needs_correction"

    candidate_rows.append(
        {
            "review_id": row["review_id"],
            "candidate_category": candidate_category,
            "deterministic_category": row["deterministic_category"],
            "chatgpt_status": status,
            "chatgpt_confidence": confidence,
            "chatgpt_evidence_basis": response.get("evidence_basis", ""),
            "chatgpt_affected_council_districts": response.get("affected_council_districts", ""),
            "needs_human_review": response.get("needs_human_review", ""),
            "matter_rows": row["matter_rows"],
            "matter_files": row["matter_files"],
            "application_keys": row["application_keys"],
            "zap_project_names": row["zap_project_names"],
            "nonaffirmative_member_votes": row["nonaffirmative_member_votes"],
            "title_examples": row["title_examples"],
            "source_urls": response.get("source_urls", ""),
            "short_explanation": response.get("short_explanation", ""),
        }
    )

summary_counts: dict[tuple[str, str], int] = {}
for row in bundle_rows:
    key = (str(row["deterministic_category"]), str(row["review_action"]))
    summary_counts[key] = summary_counts.get(key, 0) + 1
summary_rows = [
    {
        "deterministic_category": category,
        "review_action": review_action,
        "bundle_rows": count,
        "share_of_bundles": count / len(bundle_rows),
    }
    for (category, review_action), count in sorted(summary_counts.items(), key=lambda item: (-item[1], item[0][0]))
]

qc_rows = [
    {"metric": "remaining_split_missing_geography_matter_rows", "value": len(matter_rows), "status": "info"},
    {"metric": "remaining_split_missing_geography_review_bundles", "value": len(bundle_rows), "status": "info"},
    {"metric": "batch_size", "value": batch_size, "status": "info"},
    {"metric": "max_bundles", "value": max_bundles, "status": "info"},
    {"metric": "batch_count", "value": len(batch_manifest_rows), "status": "info"},
    {"metric": "valid_chatgpt_response_rows", "value": len(responses), "status": "info"},
    {"metric": "missing_chatgpt_response_rows", "value": len(missing_response_rows), "status": "info"},
    {"metric": "chatgpt_response_error_rows", "value": len(response_errors), "status": "pass" if len(response_errors) == 0 else "review"},
]
for row in summary_rows:
    qc_rows.append(
        {
            "metric": f"bundle_category_{row['deterministic_category']}",
            "value": row["bundle_rows"],
            "status": "info",
        }
    )

workflow = """# Remaining Split-Vote Geography Review Workflow

This task audits split final-action Council land-use matter rows that still lack affected Council district geography after the main deterministic and accepted AI/manual repair pipeline.

## Interpretation

- Unit in the matter-row file: one Legistar matter row.
- Unit in the bundle queue: one conservative review bundle, usually a project/application row or an obvious duplicate group.
- The task does not update the analytic member-deference panel.
- ChatGPT responses are triage evidence only. A later repair task should promote only source-checked rows.

## Browser/ChatGPT Steps

1. Run `make` from this task's `code/` folder.
2. Open `../output/council_land_use_remaining_split_geography_next_batch.md`.
3. Paste the batch into ChatGPT in Browser.
4. Ask ChatGPT to return JSONL only.
5. Append the response lines to `code/chatgpt_remaining_split_geography_responses.jsonl`.
6. Rerun `make`.
7. Review `../output/council_land_use_remaining_split_geography_ai_candidates.csv`.
8. Promote no row into the analytic panel until a separate source-checking repair task records the accepted source and district assignment.

## Key Research Choice

For multi-district projects, every affected district is treated as local. A Council approval over opposition by any affected district member is a local-member opposition event. For citywide, text-only, franchise, landmark, or other non-project actions where a local affected member is not well-defined, the correct classification is not local project geography rather than forced assignment.
"""

checklist = """# Research Understanding Checklist

## Session Goal
- [x] Research question or task: identify remaining split-vote land-use matters where missing geography could affect the local-member deference series.
- [x] Why this matters: missing geography on split votes can hide local-member opposition; missing geography on non-project or citywide actions should not be forced into the deference estimand.
- [x] What changed in this session: created an audit-only queue for remaining split-vote missing-geography rows.

## Stage 1: Problem And Motivation
- [x] What problem existed? The current plotted series has geography for observed local-member-position rows, but some split-vote rows remain without affected districts.
- [x] Why would a naive approach fail? Hand-coding all matter rows as local projects would force districts onto citywide/text/franchise/landmark items where affected member is not defined.
- [ ] Mastery status: needs researcher review after reading the queue.

## Stage 3: Cleaning And Construction Logic
- [x] Included rows: final-action vote-sample matter rows with at least one negative/abstain vote and no affected Council district.
- [x] Mechanical output: matter-row frame and bundled ChatGPT review queue.
- [x] Substantive output: deterministic triage category for prioritizing review, not final coding.
- [ ] Mastery status: needs researcher review after first batch.

## Open Questions
- [ ] How many remaining bundles are true project geographies after ChatGPT/source review?
- [ ] How many are citywide/text/non-project cases where local-member deference is not well-defined?
- [ ] Does adding accepted repairs materially change the rolling deference series?
"""

matter_fieldnames = [
    "query_year",
    "matter_id",
    "matter_file",
    "matter_type",
    "disposition_group",
    "vote_source",
    "vote_date",
    "vote_margin",
    "affirmative_count",
    "negative_count",
    "abstain_count",
    "application_keys",
    "application_suffixes",
    "zap_project_ids",
    "zap_project_names",
    "affected_district_source",
    "deterministic_category",
    "review_action",
    "bundle_key",
    "nonaffirmative_member_votes",
    "title",
    "matter_url",
    "history_detail_url",
]
bundle_fieldnames = [
    "review_id",
    "review_priority",
    "stable_id",
    "query_year",
    "vote_source",
    "vote_date",
    "vote_margin",
    "affirmative_count",
    "negative_count",
    "abstain_count",
    "matter_rows",
    "land_use_application_rows",
    "resolution_rows",
    "call_up_rows",
    "matter_files",
    "matter_ids",
    "application_keys",
    "application_suffixes",
    "zap_project_ids",
    "zap_project_names",
    "nonaffirmative_member_votes",
    "deterministic_category",
    "review_action",
    "title_examples",
    "matter_urls",
    "history_detail_urls",
    "bundle_key",
]
response_fieldnames = [
    "response_line_number",
    "review_id",
    "status",
    "affected_council_districts",
    "confidence",
    "evidence_basis",
    "official_sources_checked",
    "source_urls",
    "local_member_names_if_known",
    "needs_human_review",
    "short_explanation",
]

write_csv("../output/council_land_use_remaining_split_geography_matter_rows.csv", matter_rows, matter_fieldnames)
write_csv("../output/council_land_use_remaining_split_geography_bundle_queue.csv", bundle_rows, bundle_fieldnames)
write_csv(
    "../output/council_land_use_remaining_split_geography_triage_summary.csv",
    summary_rows,
    ["deterministic_category", "review_action", "bundle_rows", "share_of_bundles"],
)
write_csv(
    "../output/council_land_use_remaining_split_geography_batch_manifest.csv",
    batch_manifest_rows,
    [
        "batch_id",
        "batch_path",
        "bundle_count",
        "first_review_priority",
        "last_review_priority",
        "first_review_id",
        "last_review_id",
        "char_count",
        "review_ids",
    ],
)
write_csv("../output/council_land_use_remaining_split_geography_responses_combined.csv", responses, response_fieldnames)
write_csv(
    "../output/council_land_use_remaining_split_geography_missing_responses.csv",
    missing_response_rows,
    [
        "review_id",
        "review_priority",
        "deterministic_category",
        "query_year",
        "vote_date",
        "vote_margin",
        "matter_rows",
        "matter_files",
        "application_keys",
        "zap_project_names",
    ],
)
write_csv(
    "../output/council_land_use_remaining_split_geography_response_errors.csv",
    response_errors,
    ["response_line_number", "review_id", "validation_errors", "raw_line"],
)
write_csv(
    "../output/council_land_use_remaining_split_geography_ai_candidates.csv",
    candidate_rows,
    [
        "review_id",
        "candidate_category",
        "deterministic_category",
        "chatgpt_status",
        "chatgpt_confidence",
        "chatgpt_evidence_basis",
        "chatgpt_affected_council_districts",
        "needs_human_review",
        "matter_rows",
        "matter_files",
        "application_keys",
        "zap_project_names",
        "nonaffirmative_member_votes",
        "title_examples",
        "source_urls",
        "short_explanation",
    ],
)
write_csv("../output/council_land_use_remaining_split_geography_qc.csv", qc_rows, ["metric", "value", "status"])
write_text("../output/council_land_use_remaining_split_geography_workflow.md", workflow)
write_text("../output/research_understanding_checklist.md", checklist)
