#!/usr/bin/env python3

import csv
import io
import json
import os
import sys
import textwrap


# batch_size = 4
# max_signatures = 0
# prompt_version = "v1"


EXPECTED_RESPONSE_KEYS = [
    "signature_review_id",
    "prompt_version",
    "review_status",
    "affected_council_districts",
    "district_source",
    "district_confidence",
    "local_member_names",
    "project_name",
    "borough",
    "official_sources_used",
    "evidence_note",
    "outside_knowledge_used",
    "needs_human_review",
    "human_review_reason",
]
EXPECTED_SECOND_PASS_KEYS = [
    "signature_review_id",
    "prompt_version",
    "second_pass_status",
    "affected_council_districts",
    "district_source",
    "district_confidence",
    "local_member_names",
    "project_name",
    "borough",
    "project_area",
    "official_sources_used",
    "evidence_note",
    "disagreement_with_first_pass",
    "outside_knowledge_used",
    "needs_human_review",
    "human_review_reason",
]
EXPECTED_ADJUDICATION_KEYS = [
    "signature_review_id",
    "prompt_version",
    "adjudication_status",
    "affected_council_districts",
    "district_source",
    "district_confidence",
    "local_member_names",
    "project_name",
    "borough",
    "project_area",
    "official_sources_used",
    "source_check_summary",
    "prior_pass_agreement",
    "evidence_limitations",
    "recommended_researcher_action",
    "needs_human_review",
    "human_review_reason",
]

ALLOWED_STATUSES = {
    "resolved",
    "partial",
    "not_land_use",
    "citywide_or_text_only",
    "insufficient_evidence",
    "ambiguous",
}
ALLOWED_SECOND_PASS_STATUSES = {
    "confirmed",
    "corrected",
    "downgraded_citywide_or_text_only",
    "downgraded_not_land_use",
    "unresolved",
    "ambiguous",
}
ALLOWED_ADJUDICATION_STATUSES = {
    "accept_project_geography",
    "reject_project_geography",
    "confirm_citywide_or_text_only",
    "confirm_not_land_use",
    "unresolved",
    "ambiguous",
}
ALLOWED_DISTRICT_SOURCES = {
    "official_legistar",
    "official_ulurp",
    "official_zap",
    "official_dcp",
    "title_inference",
    "outside_search",
    "unknown",
}
ALLOWED_CONFIDENCE = {"high", "medium", "low"}
ALLOWED_YES_NO = {"yes", "no"}
ALLOWED_RESEARCHER_ACTIONS = {
    "spot_check_and_enter_manual_project_geography",
    "enter_manual_citywide_or_text_only",
    "enter_manual_not_land_use",
    "enter_manual_unresolved",
    "reject_ai_suggestion",
    "do_not_promote_without_more_evidence",
}
ALLOWED_MANUAL_STATUSES = {
    "verified_project_geography",
    "verified_citywide_or_text_only",
    "verified_not_land_use",
    "verified_unresolved",
    "reject_ai_suggestion",
}

PROMPT_HEADER = """# NYC Council Land-Use Missing Geography Review

You are helping audit an academic research database of New York City Council land-use decisions.

Task: For each final-action roll-call signature below, identify the affected New York City Council district(s), project area, and local Council member(s), if the official record supports doing so.

Return one compact JSON object per signature, one object per line, with exactly these keys:

```json
{
  "signature_review_id": "",
  "prompt_version": "",
  "review_status": "resolved",
  "affected_council_districts": "",
  "district_source": "official_legistar",
  "district_confidence": "high",
  "local_member_names": "",
  "project_name": "",
  "borough": "",
  "official_sources_used": [
    {"title": "", "url": "", "evidence_note": ""}
  ],
  "evidence_note": "",
  "outside_knowledge_used": "no",
  "needs_human_review": "yes",
  "human_review_reason": ""
}
```

Allowed `review_status` values: resolved, partial, not_land_use, citywide_or_text_only, insufficient_evidence, ambiguous.
Allowed `district_source` values: official_legistar, official_ulurp, official_zap, official_dcp, title_inference, outside_search, unknown.
Allowed `district_confidence` values: high, medium, low.
Allowed `outside_knowledge_used` and `needs_human_review` values: yes, no.

Rules:
- Use official sources first: Legistar matter pages, Legistar attachments/minutes, ZAP, DCP/CPC reports, or ULURP documents.
- Use web search only to find official records or clearly identify the project location. Prefer official NYC, Legistar, ZAP, DCP, CPC, HPD, LPC, EDC, or archived Council sources over news or blogs.
- If the source gives a Council district, use that district.
- If the source gives a project area, neighborhood, address, BBL, block/lots, or bounded area, summarize it in `evidence_note`.
- If you can identify the local member from the Council district and date, include the name in `local_member_names`; otherwise leave it blank and explain why.
- If there are multiple project components in different districts, list all affected districts separated by semicolons.
- If the item is a citywide zoning text amendment or has no project-specific geography, use `review_status = citywide_or_text_only`.
- If this is not actually a land-use/project geography matter, use `review_status = not_land_use`.
- Do not treat a borough suffix in an application number as a Council district. It can help identify borough only.
- If you infer from title/address rather than an official district field, use `district_source = title_inference`, lower confidence, and `needs_human_review = yes`.
- If evidence is too thin or conflicting, use `review_status = insufficient_evidence` or `ambiguous`.
- Keep evidence notes short, but include the source and the fact that supports the district.
- Return only JSONL. No markdown fence and no prose.

"""

SECOND_PASS_PROMPT_HEADER = """# NYC Council Land-Use Missing Geography Review: Second Pass

You are doing a second-pass audit of an academic research database of New York City Council land-use decisions.

Task: For each roll-call signature below, review the original Council record and the first-pass AI answer. Confirm the first-pass geography only if official evidence supports it. Otherwise correct it, downgrade it to citywide/not-land-use, or mark it unresolved.

Return one compact JSON object per signature, one object per line, with exactly these keys:

```json
{
  "signature_review_id": "",
  "prompt_version": "v2",
  "second_pass_status": "confirmed",
  "affected_council_districts": "",
  "district_source": "official_legistar",
  "district_confidence": "high",
  "local_member_names": "",
  "project_name": "",
  "borough": "",
  "project_area": "",
  "official_sources_used": [
    {"title": "", "url": "", "evidence_note": ""}
  ],
  "evidence_note": "",
  "disagreement_with_first_pass": "",
  "outside_knowledge_used": "no",
  "needs_human_review": "yes",
  "human_review_reason": ""
}
```

Allowed `second_pass_status` values: confirmed, corrected, downgraded_citywide_or_text_only, downgraded_not_land_use, unresolved, ambiguous.
Allowed `district_source` values: official_legistar, official_ulurp, official_zap, official_dcp, title_inference, outside_search, unknown.
Allowed `district_confidence` values: high, medium, low.
Allowed `outside_knowledge_used` and `needs_human_review` values: yes, no.

Rules:
- Use official sources first: Legistar matter pages, Legistar attachments/minutes, ZAP, DCP/CPC reports, ULURP documents, HPD, LPC, EDC, or archived Council records.
- Use web search only to find official records or clearly identify the project location. Do not rely on blogs or news when official records are available.
- If an official source gives a Council district, use that district and set `district_source` to the official source type.
- If official records provide only a project area, address, BBL, block/lots, or bounded area, summarize it in `project_area`; infer districts only when the location is specific enough.
- If you infer from title/address/project geography rather than an official Council district field, set `district_source = title_inference`, use medium or low confidence, and set `needs_human_review = yes`.
- If the item is citywide, text-only, a franchise with no local project geography, or has no project-specific geography, use `second_pass_status = downgraded_citywide_or_text_only`.
- If it is not actually land use/project geography for this database, use `second_pass_status = downgraded_not_land_use`.
- If evidence is too thin or conflicting, use `second_pass_status = unresolved` or `ambiguous`.
- For `confirmed` or `corrected`, list all affected Council districts separated by semicolons.
- Do not treat a borough suffix in an application number as a Council district.
- Return only JSONL. No markdown fence and no prose.

"""

ADJUDICATION_PROMPT_HEADER = """# NYC Council Land-Use Missing Geography Review: Adjudication Pass

You are doing a stricter AI-assisted adjudication of an academic research database of New York City Council land-use decisions.

Task: For each roll-call signature below, treat the first-pass and second-pass AI answers as claims, not facts. Re-check the official records and decide whether the project geography is strong enough for a researcher to spot-check and enter into the manual verdict ledger, whether the row should stay out as citywide/not land use, or whether it remains unresolved.

Return one compact JSON object per signature, one object per line, with exactly these keys:

```json
{
  "signature_review_id": "",
  "prompt_version": "v3",
  "adjudication_status": "accept_project_geography",
  "affected_council_districts": "",
  "district_source": "official_legistar",
  "district_confidence": "high",
  "local_member_names": "",
  "project_name": "",
  "borough": "",
  "project_area": "",
  "official_sources_used": [
    {"title": "", "url": "", "evidence_note": ""}
  ],
  "source_check_summary": "",
  "prior_pass_agreement": "",
  "evidence_limitations": "",
  "recommended_researcher_action": "spot_check_and_enter_manual_project_geography",
  "needs_human_review": "yes",
  "human_review_reason": ""
}
```

Allowed `adjudication_status` values: accept_project_geography, reject_project_geography, confirm_citywide_or_text_only, confirm_not_land_use, unresolved, ambiguous.
Allowed `district_source` values: official_legistar, official_ulurp, official_zap, official_dcp, title_inference, outside_search, unknown.
Allowed `district_confidence` values: high, medium, low.
Allowed `recommended_researcher_action` values: spot_check_and_enter_manual_project_geography, enter_manual_citywide_or_text_only, enter_manual_not_land_use, enter_manual_unresolved, reject_ai_suggestion, do_not_promote_without_more_evidence.
Allowed `needs_human_review` values: yes, no.

Rules:
- Use official sources first: Legistar matter pages, Legistar attachments/minutes, ZAP, DCP/CPC reports, ULURP documents, HPD, LPC, EDC, or archived Council records.
- Do not accept project geography merely because a prior AI pass said it. Accept only if the official source gives a district or gives a specific address, BBL, block/lots, bounded project area, or named project that is specific enough to infer affected Council district(s).
- If geography is inferred from address/project area rather than an explicit official Council district field, set `district_source = title_inference`, use medium or low confidence, and set `needs_human_review = yes`.
- If the official evidence supports project geography, use `adjudication_status = accept_project_geography` and list all affected Council districts separated by semicolons.
- If a prior AI geography claim is not supported, use `adjudication_status = reject_project_geography`.
- If the row is citywide or text-only, use `adjudication_status = confirm_citywide_or_text_only`.
- If the row is not a land-use/project-geography decision for this database, use `adjudication_status = confirm_not_land_use`.
- If official evidence is too thin, unreachable, or conflicting, use `adjudication_status = unresolved` or `ambiguous`.
- Do not treat a borough suffix in an application number as a Council district.
- Keep `source_check_summary` short and cite the decisive official evidence.
- Return only JSONL. No markdown fence and no prose.

"""


def write_text_if_changed(text, path):
    try:
        with open(path, "r", encoding="utf-8") as old_file:
            old_text = old_file.read()
    except FileNotFoundError:
        old_text = None

    if old_text != text:
        with open(path, "w", encoding="utf-8") as output_file:
            output_file.write(text)


def write_csv_if_changed(rows, fieldnames, path):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    new_text = output.getvalue()

    try:
        with open(path, "r", encoding="utf-8", newline="") as old_file:
            old_text = old_file.read()
    except FileNotFoundError:
        old_text = None

    if old_text != new_text:
        with open(path, "w", encoding="utf-8", newline="") as output_file:
            output_file.write(new_text)


def read_csv_rows(path):
    with open(path, "r", encoding="utf-8", newline="") as input_file:
        return list(csv.DictReader(input_file))


def clean_value(value):
    if value is None:
        return ""
    return " ".join(str(value).split())


def bool_text(value):
    return "yes" if str(value).strip().upper() == "TRUE" else "no"


def json_scalar(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True)
    return clean_value(value)


def parse_response_lines(path):
    rows = []
    errors = []
    with open(path, "r", encoding="utf-8") as input_file:
        for line_number, line in enumerate(input_file, start=1):
            line = line.strip()
            if line == "" or line.startswith("```"):
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                errors.append(
                    {
                        "response_line_number": line_number,
                        "signature_review_id": "",
                        "validation_errors": f"json_decode_error:{exc.msg}",
                        "raw_line": line[:500],
                    }
                )
                continue
            row["response_line_number"] = line_number
            rows.append(row)
    return rows, errors


def parse_response_lines_if_exists(path):
    if not os.path.exists(path):
        return [], []
    return parse_response_lines(path)


def split_districts(value):
    districts = []
    for part in str(value).replace(",", ";").replace("|", ";").split(";"):
        part = part.strip()
        if part.isdigit() and 1 <= int(part) <= 51 and part not in districts:
            districts.append(part)
    return districts


def review_category(response):
    validation_status = response.get("validation_status", "")
    review_status = response.get("review_status", "")
    districts = response.get("normalized_affected_council_districts", "")
    district_source = response.get("district_source", "")
    district_confidence = response.get("district_confidence", "")
    needs_human_review = response.get("needs_human_review", "")

    if validation_status != "pass":
        return "response_needs_correction"
    if review_status in {"citywide_or_text_only", "not_land_use"}:
        return "not_project_geography"
    if review_status in {"insufficient_evidence", "ambiguous"} or districts == "":
        return "unresolved_after_ai_review"
    if (
        review_status == "resolved"
        and district_source in {"official_legistar", "official_ulurp", "official_zap", "official_dcp"}
        and district_confidence == "high"
        and needs_human_review == "no"
    ):
        return "official_high_confidence_ai_candidate"
    return "needs_human_verification"


def next_action(response):
    category = review_category(response)
    if category == "official_high_confidence_ai_candidate":
        return "spot_check_official_source_then_promote"
    if category == "needs_human_verification":
        return "read_cited_sources_and_enter_manual_verdict"
    if category == "not_project_geography":
        return "keep_out_of_local_member_geography_repair"
    if category == "unresolved_after_ai_review":
        return "manual_search_or_leave_unresolved"
    return "fix_json_or_controlled_vocabulary"


def second_pass_review_category(response):
    validation_status = response.get("validation_status", "")
    second_pass_status = response.get("second_pass_status", "")
    districts = response.get("normalized_affected_council_districts", "")
    district_source = response.get("district_source", "")
    district_confidence = response.get("district_confidence", "")
    needs_human_review = response.get("needs_human_review", "")

    if validation_status != "pass":
        return "response_needs_correction"
    if second_pass_status in {"downgraded_citywide_or_text_only", "downgraded_not_land_use"}:
        return "not_project_geography"
    if second_pass_status in {"unresolved", "ambiguous"} or districts == "":
        return "unresolved_after_second_pass"
    if (
        second_pass_status in {"confirmed", "corrected"}
        and district_source in {"official_legistar", "official_ulurp", "official_zap", "official_dcp"}
        and district_confidence == "high"
        and needs_human_review == "no"
    ):
        return "official_high_confidence_second_pass_candidate"
    return "needs_human_verification"


def second_pass_next_action(response):
    category = second_pass_review_category(response)
    if category == "official_high_confidence_second_pass_candidate":
        return "spot_check_official_source_then_promote"
    if category == "needs_human_verification":
        return "read_cited_sources_and_enter_manual_verdict"
    if category == "not_project_geography":
        return "keep_out_of_local_member_geography_repair"
    if category == "unresolved_after_second_pass":
        return "manual_search_or_leave_unresolved"
    return "fix_json_or_controlled_vocabulary"


def adjudication_category(response):
    validation_status = response.get("validation_status", "")
    adjudication_status = response.get("adjudication_status", "")
    districts = response.get("normalized_affected_council_districts", "")
    district_source = response.get("district_source", "")
    district_confidence = response.get("district_confidence", "")
    needs_human_review = response.get("needs_human_review", "")

    if validation_status != "pass":
        return "response_needs_correction"
    if adjudication_status == "accept_project_geography":
        if districts == "":
            return "response_needs_correction"
        if (
            district_source in {"official_legistar", "official_ulurp", "official_zap", "official_dcp"}
            and district_confidence == "high"
            and needs_human_review == "no"
        ):
            return "ai_adjudicated_official_project_geography"
        if district_source == "title_inference" and district_confidence == "medium" and needs_human_review == "yes":
            return "official_location_strong_district_inferred"
        return "ai_adjudicated_project_geography_needs_spot_check"
    if adjudication_status in {"confirm_citywide_or_text_only", "confirm_not_land_use"}:
        return "ai_adjudicated_not_project_geography"
    if adjudication_status == "reject_project_geography":
        return "ai_rejected_prior_geography"
    if adjudication_status in {"unresolved", "ambiguous"}:
        return "unresolved_after_adjudication"
    return "response_needs_correction"


def adjudication_next_action(response):
    category = adjudication_category(response)
    if category == "ai_adjudicated_official_project_geography":
        return "spot_check_official_source_then_enter_manual_verdict"
    if category == "official_location_strong_district_inferred":
        return "spot_check_official_location_then_enter_manual_verdict"
    if category == "ai_adjudicated_project_geography_needs_spot_check":
        return "spot_check_inferred_geography_before_manual_verdict"
    if category == "ai_adjudicated_not_project_geography":
        return "keep_out_of_local_member_geography_repair"
    if category == "ai_rejected_prior_geography":
        return "inspect_before_rejecting_prior_ai_suggestion"
    if category == "unresolved_after_adjudication":
        return "manual_search_or_leave_unresolved"
    return "fix_json_or_controlled_vocabulary"


def second_pass_priority(row):
    category = row["repair_candidate_category"]
    if category == "official_high_confidence_ai_candidate":
        return 1, "first_pass_official_high_confidence"
    if category == "needs_human_verification":
        return 2, "first_pass_needs_human_verification"
    if category == "unresolved_after_ai_review":
        return 3, "first_pass_unresolved"
    if category == "not_project_geography":
        return 4, "first_pass_not_project_geography"
    return 5, "first_pass_response_issue"


batch_size = int(sys.argv[1])
max_signatures = int(sys.argv[2])
prompt_version = sys.argv[3]
second_pass_prompt_version = "v2"
adjudication_prompt_version = "v3"

queue_rows = read_csv_rows("../input/council_land_use_missing_geography_roll_call_repair_queue.csv")
manual_verdict_rows = read_csv_rows("council_land_use_missing_geography_manual_verdicts.csv")
if max_signatures > 0:
    queue_rows = queue_rows[:max_signatures]

frame_rows = []
for index, row in enumerate(queue_rows, start=1):
    frame_rows.append(
        {
            "signature_review_id": f"clu_geo_{index:03d}",
            "queue_rank": index,
            "prompt_version": prompt_version,
            "repair_priority": clean_value(row["repair_priority"]),
            "repair_priority_reason": clean_value(row["repair_priority_reason"]),
            "probable_non_project_false_positive": bool_text(row["probable_non_project_false_positive"]),
            "query_year": clean_value(row["query_year"]),
            "vote_date": clean_value(row["vote_date"]),
            "vote_source_group": clean_value(row["vote_source_group"]),
            "vote_margin": clean_value(row["vote_margin"]),
            "affirmative_count": clean_value(row["affirmative_count"]),
            "negative_count": clean_value(row["negative_count"]),
            "abstain_count": clean_value(row["abstain_count"]),
            "dissent_count": clean_value(row["dissent_count"]),
            "matter_rows": clean_value(row["matter_rows"]),
            "land_use_application_rows": clean_value(row["land_use_application_rows"]),
            "resolution_rows": clean_value(row["resolution_rows"]),
            "call_up_rows": clean_value(row["call_up_rows"]),
            "local_member_vote_rows": clean_value(row["local_member_vote_rows"]),
            "matter_files": clean_value(row["matter_files"]),
            "application_keys": clean_value(row["application_keys"]),
            "zap_project_ids": clean_value(row["zap_project_ids"]),
            "zap_project_names": clean_value(row["zap_project_names"]),
            "title_examples": clean_value(row["title_examples"]),
            "matter_urls": clean_value(row["matter_urls"]),
            "history_detail_urls": clean_value(row["history_detail_urls"]),
            "roll_call_signature": clean_value(row["roll_call_signature"]),
        }
    )

records = []
for row in frame_rows:
    records.append(
        "\n".join(
            [
                "-----",
                f"signature_review_id: {row['signature_review_id']}",
                f"queue_rank: {row['queue_rank']}",
                f"prompt_version: {row['prompt_version']}",
                f"repair_priority_reason: {row['repair_priority_reason']}",
                f"probable_non_project_false_positive: {row['probable_non_project_false_positive']}",
                f"query_year: {row['query_year']}",
                f"vote_date: {row['vote_date']}",
                f"vote_source_group: {row['vote_source_group']}",
                f"vote_margin: {row['vote_margin']}",
                f"negative_count: {row['negative_count']}",
                f"abstain_count: {row['abstain_count']}",
                f"matter_rows: {row['matter_rows']}",
                f"matter_files: {row['matter_files']}",
                f"application_keys: {row['application_keys']}",
                f"zap_project_ids: {row['zap_project_ids']}",
                f"zap_project_names: {row['zap_project_names']}",
                f"title_examples: {row['title_examples']}",
                f"matter_urls: {row['matter_urls']}",
                f"history_detail_urls: {row['history_detail_urls']}",
                "",
            ]
        )
    )

os.makedirs("../output/batches", exist_ok=True)

batch_rows = []
for start in range(0, len(frame_rows), batch_size):
    batch_id = f"{len(batch_rows) + 1:03d}"
    batch_frame_rows = frame_rows[start : start + batch_size]
    batch_text = PROMPT_HEADER + "\n".join(records[start : start + batch_size])
    batch_path = f"../output/batches/council_land_use_missing_geography_chatgpt_review_batch_{batch_id}.md"
    write_text_if_changed(batch_text, batch_path)
    batch_rows.append(
        {
            "batch_id": batch_id,
            "batch_path": batch_path,
            "signature_count": len(batch_frame_rows),
            "first_queue_rank": batch_frame_rows[0]["queue_rank"],
            "last_queue_rank": batch_frame_rows[-1]["queue_rank"],
            "char_count": len(batch_text),
            "signature_review_ids": "|".join(row["signature_review_id"] for row in batch_frame_rows),
        }
    )

batch_jsonl = "\n".join(json.dumps(row, ensure_ascii=True) for row in batch_rows)
if batch_jsonl:
    batch_jsonl += "\n"

response_rows, parse_errors = parse_response_lines("chatgpt_geography_review_responses.jsonl")

frame_by_id = {row["signature_review_id"]: row for row in frame_rows}
batch_by_id = {}
for batch_row in batch_rows:
    for signature_review_id in batch_row["signature_review_ids"].split("|"):
        batch_by_id[signature_review_id] = batch_row["batch_id"]

seen_ids = set()
duplicate_ids = set()
combined_rows = []
validation_errors = []

for row in response_rows:
    signature_review_id = clean_value(row.get("signature_review_id", ""))
    errors = []
    if signature_review_id in seen_ids:
        duplicate_ids.add(signature_review_id)
        continue
    seen_ids.add(signature_review_id)

    if signature_review_id not in frame_by_id:
        errors.append("signature_review_id_not_in_frame")
    if clean_value(row.get("prompt_version", "")) != prompt_version:
        errors.append("prompt_version_mismatch")
    if clean_value(row.get("review_status", "")) not in ALLOWED_STATUSES:
        errors.append("invalid_review_status")
    if clean_value(row.get("district_source", "")) not in ALLOWED_DISTRICT_SOURCES:
        errors.append("invalid_district_source")
    if clean_value(row.get("district_confidence", "")) not in ALLOWED_CONFIDENCE:
        errors.append("invalid_district_confidence")
    if clean_value(row.get("outside_knowledge_used", "")) not in ALLOWED_YES_NO:
        errors.append("invalid_outside_knowledge_used")
    if clean_value(row.get("needs_human_review", "")) not in ALLOWED_YES_NO:
        errors.append("invalid_needs_human_review")
    if clean_value(row.get("review_status", "")) in {"resolved", "partial"}:
        if not split_districts(row.get("affected_council_districts", "")):
            errors.append("resolved_or_partial_without_valid_district")

    combined_row = {
        "source_response_line_number": row.get("response_line_number", ""),
        "batch_id": batch_by_id.get(signature_review_id, ""),
        **{key: json_scalar(row.get(key, "")) for key in EXPECTED_RESPONSE_KEYS},
        "normalized_affected_council_districts": "; ".join(split_districts(row.get("affected_council_districts", ""))),
        "validation_status": "fail" if errors else "pass",
        "validation_errors": "|".join(errors),
    }
    combined_rows.append(combined_row)

    if errors:
        validation_errors.append(
            {
                "response_line_number": row.get("response_line_number", ""),
                "signature_review_id": signature_review_id,
                "validation_errors": "|".join(errors),
                "raw_line": "",
            }
        )

missing_rows = []
for row in frame_rows:
    if row["signature_review_id"] in seen_ids:
        continue
    missing_rows.append(
        {
            "signature_review_id": row["signature_review_id"],
            "batch_id": batch_by_id.get(row["signature_review_id"], ""),
            "queue_rank": row["queue_rank"],
            "repair_priority": row["repair_priority"],
            "repair_priority_reason": row["repair_priority_reason"],
            "query_year": row["query_year"],
            "vote_date": row["vote_date"],
            "vote_margin": row["vote_margin"],
            "matter_rows": row["matter_rows"],
            "application_keys": row["application_keys"],
            "zap_project_names": row["zap_project_names"],
            "title_examples": row["title_examples"],
        }
    )

manual_review_rows = []
response_by_id = {row["signature_review_id"]: row for row in combined_rows}
for row in frame_rows:
    response = response_by_id.get(row["signature_review_id"], {})
    manual_review_rows.append(
        {
            **row,
            "batch_id": batch_by_id.get(row["signature_review_id"], ""),
            "chatgpt_review_status": response.get("review_status", ""),
            "chatgpt_affected_council_districts": response.get("normalized_affected_council_districts", ""),
            "chatgpt_district_source": response.get("district_source", ""),
            "chatgpt_district_confidence": response.get("district_confidence", ""),
            "chatgpt_needs_human_review": response.get("needs_human_review", ""),
            "chatgpt_evidence_note": response.get("evidence_note", ""),
            "chatgpt_human_review_reason": response.get("human_review_reason", ""),
        }
    )

repair_candidate_rows = []
for row in frame_rows:
    response = response_by_id.get(row["signature_review_id"], {})
    repair_candidate_rows.append(
        {
            "signature_review_id": row["signature_review_id"],
            "queue_rank": row["queue_rank"],
            "batch_id": batch_by_id.get(row["signature_review_id"], ""),
            "query_year": row["query_year"],
            "vote_date": row["vote_date"],
            "vote_margin": row["vote_margin"],
            "matter_rows": row["matter_rows"],
            "matter_files": row["matter_files"],
            "application_keys": row["application_keys"],
            "zap_project_ids": row["zap_project_ids"],
            "zap_project_names": row["zap_project_names"],
            "matter_urls": row["matter_urls"],
            "history_detail_urls": row["history_detail_urls"],
            "title_examples": row["title_examples"],
            "ai_review_status": response.get("review_status", ""),
            "ai_affected_council_districts": response.get("normalized_affected_council_districts", ""),
            "ai_district_source": response.get("district_source", ""),
            "ai_district_confidence": response.get("district_confidence", ""),
            "ai_local_member_names": response.get("local_member_names", ""),
            "ai_project_name": response.get("project_name", ""),
            "ai_borough": response.get("borough", ""),
            "ai_official_sources_used": response.get("official_sources_used", ""),
            "ai_evidence_note": response.get("evidence_note", ""),
            "ai_outside_knowledge_used": response.get("outside_knowledge_used", ""),
            "ai_needs_human_review": response.get("needs_human_review", ""),
            "ai_human_review_reason": response.get("human_review_reason", ""),
            "ai_validation_status": response.get("validation_status", ""),
            "ai_validation_errors": response.get("validation_errors", ""),
            "repair_candidate_category": review_category(response),
            "recommended_next_action": next_action(response),
        }
    )

second_pass_frame_rows = []
for row in repair_candidate_rows:
    priority, priority_reason = second_pass_priority(row)
    second_pass_frame_rows.append(
        {
            **row,
            "second_pass_prompt_version": second_pass_prompt_version,
            "second_pass_priority": priority,
            "second_pass_priority_reason": priority_reason,
        }
    )

second_pass_frame_rows = sorted(
    second_pass_frame_rows,
    key=lambda row: (row["second_pass_priority"], int(row["queue_rank"])),
)

second_pass_records = []
for row in second_pass_frame_rows:
    second_pass_records.append(
        "\n".join(
            [
                "-----",
                f"signature_review_id: {row['signature_review_id']}",
                f"queue_rank: {row['queue_rank']}",
                f"second_pass_priority_reason: {row['second_pass_priority_reason']}",
                f"query_year: {row['query_year']}",
                f"vote_date: {row['vote_date']}",
                f"vote_margin: {row['vote_margin']}",
                f"matter_rows: {row['matter_rows']}",
                f"matter_files: {row['matter_files']}",
                f"application_keys: {row['application_keys']}",
                f"zap_project_ids: {row['zap_project_ids']}",
                f"zap_project_names: {row['zap_project_names']}",
                f"title_examples: {row['title_examples']}",
                f"matter_urls: {row['matter_urls']}",
                f"history_detail_urls: {row['history_detail_urls']}",
                "",
                "First-pass AI answer:",
                f"first_pass_category: {row['repair_candidate_category']}",
                f"first_pass_status: {row['ai_review_status']}",
                f"first_pass_districts: {row['ai_affected_council_districts']}",
                f"first_pass_district_source: {row['ai_district_source']}",
                f"first_pass_confidence: {row['ai_district_confidence']}",
                f"first_pass_local_members: {row['ai_local_member_names']}",
                f"first_pass_project_name: {row['ai_project_name']}",
                f"first_pass_borough: {row['ai_borough']}",
                f"first_pass_sources: {row['ai_official_sources_used']}",
                f"first_pass_evidence_note: {row['ai_evidence_note']}",
                f"first_pass_human_review_reason: {row['ai_human_review_reason']}",
                "",
            ]
        )
    )

second_pass_batch_rows = []
for start in range(0, len(second_pass_frame_rows), batch_size):
    batch_id = f"{len(second_pass_batch_rows) + 1:03d}"
    batch_frame_rows = second_pass_frame_rows[start : start + batch_size]
    batch_text = SECOND_PASS_PROMPT_HEADER + "\n".join(second_pass_records[start : start + batch_size])
    batch_path = f"../output/batches/council_land_use_missing_geography_second_pass_batch_{batch_id}.md"
    write_text_if_changed(batch_text, batch_path)
    second_pass_batch_rows.append(
        {
            "batch_id": batch_id,
            "batch_path": batch_path,
            "signature_count": len(batch_frame_rows),
            "first_second_pass_priority": batch_frame_rows[0]["second_pass_priority"],
            "last_second_pass_priority": batch_frame_rows[-1]["second_pass_priority"],
            "first_queue_rank": batch_frame_rows[0]["queue_rank"],
            "last_queue_rank": batch_frame_rows[-1]["queue_rank"],
            "char_count": len(batch_text),
            "signature_review_ids": "|".join(row["signature_review_id"] for row in batch_frame_rows),
        }
    )

second_pass_batch_jsonl = "\n".join(json.dumps(row, ensure_ascii=True) for row in second_pass_batch_rows)
if second_pass_batch_jsonl:
    second_pass_batch_jsonl += "\n"

second_pass_response_rows, second_pass_parse_errors = parse_response_lines_if_exists(
    "chatgpt_geography_second_pass_responses.jsonl"
)

second_pass_frame_by_id = {row["signature_review_id"]: row for row in second_pass_frame_rows}
second_pass_batch_by_id = {}
for batch_row in second_pass_batch_rows:
    for signature_review_id in batch_row["signature_review_ids"].split("|"):
        second_pass_batch_by_id[signature_review_id] = batch_row["batch_id"]

second_pass_seen_ids = set()
second_pass_duplicate_ids = set()
second_pass_combined_rows = []
second_pass_validation_errors = []

for row in second_pass_response_rows:
    signature_review_id = clean_value(row.get("signature_review_id", ""))
    errors = []
    if signature_review_id in second_pass_seen_ids:
        second_pass_duplicate_ids.add(signature_review_id)
        continue
    second_pass_seen_ids.add(signature_review_id)

    if signature_review_id not in second_pass_frame_by_id:
        errors.append("signature_review_id_not_in_second_pass_frame")
    if clean_value(row.get("prompt_version", "")) != second_pass_prompt_version:
        errors.append("prompt_version_mismatch")
    if clean_value(row.get("second_pass_status", "")) not in ALLOWED_SECOND_PASS_STATUSES:
        errors.append("invalid_second_pass_status")
    if clean_value(row.get("district_source", "")) not in ALLOWED_DISTRICT_SOURCES:
        errors.append("invalid_district_source")
    if clean_value(row.get("district_confidence", "")) not in ALLOWED_CONFIDENCE:
        errors.append("invalid_district_confidence")
    if clean_value(row.get("outside_knowledge_used", "")) not in ALLOWED_YES_NO:
        errors.append("invalid_outside_knowledge_used")
    if clean_value(row.get("needs_human_review", "")) not in ALLOWED_YES_NO:
        errors.append("invalid_needs_human_review")
    if clean_value(row.get("second_pass_status", "")) in {"confirmed", "corrected"}:
        if not split_districts(row.get("affected_council_districts", "")):
            errors.append("confirmed_or_corrected_without_valid_district")

    combined_row = {
        "source_response_line_number": row.get("response_line_number", ""),
        "batch_id": second_pass_batch_by_id.get(signature_review_id, ""),
        **{key: json_scalar(row.get(key, "")) for key in EXPECTED_SECOND_PASS_KEYS},
        "normalized_affected_council_districts": "; ".join(split_districts(row.get("affected_council_districts", ""))),
        "validation_status": "fail" if errors else "pass",
        "validation_errors": "|".join(errors),
    }
    second_pass_combined_rows.append(combined_row)

    if errors:
        second_pass_validation_errors.append(
            {
                "response_line_number": row.get("response_line_number", ""),
                "signature_review_id": signature_review_id,
                "validation_errors": "|".join(errors),
                "raw_line": "",
            }
        )

second_pass_missing_rows = []
for row in second_pass_frame_rows:
    if row["signature_review_id"] in second_pass_seen_ids:
        continue
    second_pass_missing_rows.append(
        {
            "signature_review_id": row["signature_review_id"],
            "batch_id": second_pass_batch_by_id.get(row["signature_review_id"], ""),
            "queue_rank": row["queue_rank"],
            "second_pass_priority": row["second_pass_priority"],
            "second_pass_priority_reason": row["second_pass_priority_reason"],
            "query_year": row["query_year"],
            "vote_date": row["vote_date"],
            "vote_margin": row["vote_margin"],
            "matter_rows": row["matter_rows"],
            "application_keys": row["application_keys"],
            "zap_project_names": row["zap_project_names"],
            "first_pass_category": row["repair_candidate_category"],
            "first_pass_districts": row["ai_affected_council_districts"],
            "title_examples": row["title_examples"],
        }
    )

second_pass_response_by_id = {row["signature_review_id"]: row for row in second_pass_combined_rows}
second_pass_candidate_rows = []
for row in second_pass_frame_rows:
    response = second_pass_response_by_id.get(row["signature_review_id"], {})
    second_pass_candidate_rows.append(
        {
            "signature_review_id": row["signature_review_id"],
            "queue_rank": row["queue_rank"],
            "batch_id": second_pass_batch_by_id.get(row["signature_review_id"], ""),
            "second_pass_priority": row["second_pass_priority"],
            "second_pass_priority_reason": row["second_pass_priority_reason"],
            "query_year": row["query_year"],
            "vote_date": row["vote_date"],
            "vote_margin": row["vote_margin"],
            "matter_rows": row["matter_rows"],
            "matter_files": row["matter_files"],
            "application_keys": row["application_keys"],
            "zap_project_ids": row["zap_project_ids"],
            "zap_project_names": row["zap_project_names"],
            "title_examples": row["title_examples"],
            "first_pass_category": row["repair_candidate_category"],
            "first_pass_status": row["ai_review_status"],
            "first_pass_districts": row["ai_affected_council_districts"],
            "first_pass_local_member_names": row["ai_local_member_names"],
            "first_pass_project_name": row["ai_project_name"],
            "second_pass_status": response.get("second_pass_status", ""),
            "second_pass_affected_council_districts": response.get("normalized_affected_council_districts", ""),
            "second_pass_district_source": response.get("district_source", ""),
            "second_pass_district_confidence": response.get("district_confidence", ""),
            "second_pass_local_member_names": response.get("local_member_names", ""),
            "second_pass_project_name": response.get("project_name", ""),
            "second_pass_borough": response.get("borough", ""),
            "second_pass_project_area": response.get("project_area", ""),
            "second_pass_official_sources_used": response.get("official_sources_used", ""),
            "second_pass_evidence_note": response.get("evidence_note", ""),
            "second_pass_disagreement_with_first_pass": response.get("disagreement_with_first_pass", ""),
            "second_pass_outside_knowledge_used": response.get("outside_knowledge_used", ""),
            "second_pass_needs_human_review": response.get("needs_human_review", ""),
            "second_pass_human_review_reason": response.get("human_review_reason", ""),
            "second_pass_validation_status": response.get("validation_status", ""),
            "second_pass_validation_errors": response.get("validation_errors", ""),
            "second_pass_candidate_category": second_pass_review_category(response),
            "second_pass_recommended_next_action": second_pass_next_action(response),
        }
    )

second_pass_human_verification_rows = [
    row
    for row in second_pass_candidate_rows
    if row["second_pass_candidate_category"] in {
        "official_high_confidence_second_pass_candidate",
        "needs_human_verification",
        "response_needs_correction",
    }
]

adjudication_frame_rows = []
for row in second_pass_candidate_rows:
    source_row = frame_by_id.get(row["signature_review_id"], {})
    category = row["second_pass_candidate_category"]
    if category == "official_high_confidence_second_pass_candidate":
        priority, priority_reason = 1, "second_pass_official_high_confidence"
    elif category == "needs_human_verification":
        priority, priority_reason = 2, "second_pass_needs_human_verification"
    elif category == "unresolved_after_second_pass":
        priority, priority_reason = 3, "second_pass_unresolved"
    elif category == "not_project_geography":
        priority, priority_reason = 4, "second_pass_not_project_geography"
    else:
        priority, priority_reason = 5, "second_pass_response_issue"

    adjudication_frame_rows.append(
        {
            **row,
            "second_pass_batch_id": row["batch_id"],
            "adjudication_prompt_version": adjudication_prompt_version,
            "adjudication_priority": priority,
            "adjudication_priority_reason": priority_reason,
            "matter_urls": source_row.get("matter_urls", ""),
            "history_detail_urls": source_row.get("history_detail_urls", ""),
        }
    )

adjudication_frame_rows = sorted(
    adjudication_frame_rows,
    key=lambda row: (row["adjudication_priority"], int(row["queue_rank"])),
)

adjudication_records = []
for row in adjudication_frame_rows:
    adjudication_records.append(
        "\n".join(
            [
                "-----",
                f"signature_review_id: {row['signature_review_id']}",
                f"queue_rank: {row['queue_rank']}",
                f"adjudication_priority_reason: {row['adjudication_priority_reason']}",
                f"query_year: {row['query_year']}",
                f"vote_date: {row['vote_date']}",
                f"vote_margin: {row['vote_margin']}",
                f"matter_rows: {row['matter_rows']}",
                f"matter_files: {row['matter_files']}",
                f"application_keys: {row['application_keys']}",
                f"zap_project_ids: {row['zap_project_ids']}",
                f"zap_project_names: {row['zap_project_names']}",
                f"title_examples: {row['title_examples']}",
                f"matter_urls: {row['matter_urls']}",
                f"history_detail_urls: {row['history_detail_urls']}",
                "",
                "First-pass AI answer:",
                f"first_pass_category: {row['first_pass_category']}",
                f"first_pass_status: {row['first_pass_status']}",
                f"first_pass_districts: {row['first_pass_districts']}",
                f"first_pass_local_members: {row['first_pass_local_member_names']}",
                f"first_pass_project_name: {row['first_pass_project_name']}",
                "",
                "Second-pass AI answer:",
                f"second_pass_category: {row['second_pass_candidate_category']}",
                f"second_pass_status: {row['second_pass_status']}",
                f"second_pass_districts: {row['second_pass_affected_council_districts']}",
                f"second_pass_district_source: {row['second_pass_district_source']}",
                f"second_pass_confidence: {row['second_pass_district_confidence']}",
                f"second_pass_local_members: {row['second_pass_local_member_names']}",
                f"second_pass_project_name: {row['second_pass_project_name']}",
                f"second_pass_borough: {row['second_pass_borough']}",
                f"second_pass_project_area: {row['second_pass_project_area']}",
                f"second_pass_sources: {row['second_pass_official_sources_used']}",
                f"second_pass_evidence_note: {row['second_pass_evidence_note']}",
                f"second_pass_disagreement_with_first_pass: {row['second_pass_disagreement_with_first_pass']}",
                f"second_pass_human_review_reason: {row['second_pass_human_review_reason']}",
                "",
            ]
        )
    )

adjudication_batch_rows = []
for start in range(0, len(adjudication_frame_rows), batch_size):
    batch_id = f"{len(adjudication_batch_rows) + 1:03d}"
    batch_frame_rows = adjudication_frame_rows[start : start + batch_size]
    batch_text = ADJUDICATION_PROMPT_HEADER + "\n".join(adjudication_records[start : start + batch_size])
    batch_path = f"../output/batches/council_land_use_missing_geography_adjudication_batch_{batch_id}.md"
    write_text_if_changed(batch_text, batch_path)
    adjudication_batch_rows.append(
        {
            "batch_id": batch_id,
            "batch_path": batch_path,
            "signature_count": len(batch_frame_rows),
            "first_adjudication_priority": batch_frame_rows[0]["adjudication_priority"],
            "last_adjudication_priority": batch_frame_rows[-1]["adjudication_priority"],
            "first_queue_rank": batch_frame_rows[0]["queue_rank"],
            "last_queue_rank": batch_frame_rows[-1]["queue_rank"],
            "char_count": len(batch_text),
            "signature_review_ids": "|".join(row["signature_review_id"] for row in batch_frame_rows),
        }
    )

adjudication_batch_jsonl = "\n".join(json.dumps(row, ensure_ascii=True) for row in adjudication_batch_rows)
if adjudication_batch_jsonl:
    adjudication_batch_jsonl += "\n"

adjudication_response_rows, adjudication_parse_errors = parse_response_lines_if_exists(
    "chatgpt_geography_adjudication_responses.jsonl"
)

adjudication_frame_by_id = {row["signature_review_id"]: row for row in adjudication_frame_rows}
adjudication_batch_by_id = {}
for batch_row in adjudication_batch_rows:
    for signature_review_id in batch_row["signature_review_ids"].split("|"):
        adjudication_batch_by_id[signature_review_id] = batch_row["batch_id"]

adjudication_seen_ids = set()
adjudication_duplicate_ids = set()
adjudication_combined_rows = []
adjudication_validation_errors = []

for row in adjudication_response_rows:
    signature_review_id = clean_value(row.get("signature_review_id", ""))
    errors = []
    if signature_review_id in adjudication_seen_ids:
        adjudication_duplicate_ids.add(signature_review_id)
        continue
    adjudication_seen_ids.add(signature_review_id)

    if signature_review_id not in adjudication_frame_by_id:
        errors.append("signature_review_id_not_in_adjudication_frame")
    if clean_value(row.get("prompt_version", "")) != adjudication_prompt_version:
        errors.append("prompt_version_mismatch")
    if clean_value(row.get("adjudication_status", "")) not in ALLOWED_ADJUDICATION_STATUSES:
        errors.append("invalid_adjudication_status")
    if clean_value(row.get("district_source", "")) not in ALLOWED_DISTRICT_SOURCES:
        errors.append("invalid_district_source")
    if clean_value(row.get("district_confidence", "")) not in ALLOWED_CONFIDENCE:
        errors.append("invalid_district_confidence")
    if clean_value(row.get("recommended_researcher_action", "")) not in ALLOWED_RESEARCHER_ACTIONS:
        errors.append("invalid_recommended_researcher_action")
    if clean_value(row.get("needs_human_review", "")) not in ALLOWED_YES_NO:
        errors.append("invalid_needs_human_review")
    if clean_value(row.get("adjudication_status", "")) == "accept_project_geography":
        if not split_districts(row.get("affected_council_districts", "")):
            errors.append("accepted_project_geography_without_valid_district")

    combined_row = {
        "source_response_line_number": row.get("response_line_number", ""),
        "batch_id": adjudication_batch_by_id.get(signature_review_id, ""),
        **{key: json_scalar(row.get(key, "")) for key in EXPECTED_ADJUDICATION_KEYS},
        "normalized_affected_council_districts": "; ".join(split_districts(row.get("affected_council_districts", ""))),
        "validation_status": "fail" if errors else "pass",
        "validation_errors": "|".join(errors),
    }
    adjudication_combined_rows.append(combined_row)

    if errors:
        adjudication_validation_errors.append(
            {
                "response_line_number": row.get("response_line_number", ""),
                "signature_review_id": signature_review_id,
                "validation_errors": "|".join(errors),
                "raw_line": "",
            }
        )

adjudication_missing_rows = []
for row in adjudication_frame_rows:
    if row["signature_review_id"] in adjudication_seen_ids:
        continue
    adjudication_missing_rows.append(
        {
            "signature_review_id": row["signature_review_id"],
            "batch_id": adjudication_batch_by_id.get(row["signature_review_id"], ""),
            "queue_rank": row["queue_rank"],
            "adjudication_priority": row["adjudication_priority"],
            "adjudication_priority_reason": row["adjudication_priority_reason"],
            "query_year": row["query_year"],
            "vote_date": row["vote_date"],
            "vote_margin": row["vote_margin"],
            "matter_rows": row["matter_rows"],
            "application_keys": row["application_keys"],
            "zap_project_names": row["zap_project_names"],
            "second_pass_category": row["second_pass_candidate_category"],
            "second_pass_districts": row["second_pass_affected_council_districts"],
            "title_examples": row["title_examples"],
        }
    )

adjudication_response_by_id = {row["signature_review_id"]: row for row in adjudication_combined_rows}
adjudication_candidate_rows = []
for row in adjudication_frame_rows:
    response = adjudication_response_by_id.get(row["signature_review_id"], {})
    adjudication_candidate_rows.append(
        {
            "signature_review_id": row["signature_review_id"],
            "queue_rank": row["queue_rank"],
            "batch_id": adjudication_batch_by_id.get(row["signature_review_id"], ""),
            "adjudication_priority": row["adjudication_priority"],
            "adjudication_priority_reason": row["adjudication_priority_reason"],
            "query_year": row["query_year"],
            "vote_date": row["vote_date"],
            "vote_margin": row["vote_margin"],
            "matter_rows": row["matter_rows"],
            "matter_files": row["matter_files"],
            "application_keys": row["application_keys"],
            "zap_project_ids": row["zap_project_ids"],
            "zap_project_names": row["zap_project_names"],
            "matter_urls": row["matter_urls"],
            "history_detail_urls": row["history_detail_urls"],
            "title_examples": row["title_examples"],
            "first_pass_category": row["first_pass_category"],
            "first_pass_status": row["first_pass_status"],
            "first_pass_districts": row["first_pass_districts"],
            "second_pass_category": row["second_pass_candidate_category"],
            "second_pass_status": row["second_pass_status"],
            "second_pass_districts": row["second_pass_affected_council_districts"],
            "second_pass_project_name": row["second_pass_project_name"],
            "adjudication_status": response.get("adjudication_status", ""),
            "adjudication_affected_council_districts": response.get("normalized_affected_council_districts", ""),
            "adjudication_district_source": response.get("district_source", ""),
            "adjudication_district_confidence": response.get("district_confidence", ""),
            "adjudication_local_member_names": response.get("local_member_names", ""),
            "adjudication_project_name": response.get("project_name", ""),
            "adjudication_borough": response.get("borough", ""),
            "adjudication_project_area": response.get("project_area", ""),
            "adjudication_official_sources_used": response.get("official_sources_used", ""),
            "adjudication_source_check_summary": response.get("source_check_summary", ""),
            "adjudication_prior_pass_agreement": response.get("prior_pass_agreement", ""),
            "adjudication_evidence_limitations": response.get("evidence_limitations", ""),
            "adjudication_recommended_researcher_action": response.get("recommended_researcher_action", ""),
            "adjudication_needs_human_review": response.get("needs_human_review", ""),
            "adjudication_human_review_reason": response.get("human_review_reason", ""),
            "adjudication_validation_status": response.get("validation_status", ""),
            "adjudication_validation_errors": response.get("validation_errors", ""),
            "adjudication_candidate_category": adjudication_category(response),
            "adjudication_next_action": adjudication_next_action(response),
        }
    )

adjudication_spot_check_rows = [
    row
    for row in adjudication_candidate_rows
    if row["adjudication_candidate_category"] in {
        "ai_adjudicated_official_project_geography",
        "official_location_strong_district_inferred",
        "ai_adjudicated_project_geography_needs_spot_check",
        "ai_rejected_prior_geography",
        "response_needs_correction",
    }
]

human_verification_rows = [
    row
    for row in repair_candidate_rows
    if row["repair_candidate_category"] in {
        "official_high_confidence_ai_candidate",
        "needs_human_verification",
        "response_needs_correction",
    }
]

manual_verdict_errors = []
manual_seen_ids = set()
human_verified_rows = []

for line_number, verdict in enumerate(manual_verdict_rows, start=2):
    signature_review_id = clean_value(verdict.get("signature_review_id", ""))
    manual_status = clean_value(verdict.get("manual_status", ""))
    verified_districts = "; ".join(split_districts(verdict.get("verified_affected_council_districts", "")))
    errors = []

    if signature_review_id == "" and all(clean_value(value) == "" for value in verdict.values()):
        continue
    if signature_review_id == "":
        errors.append("missing_signature_review_id")
    if signature_review_id in manual_seen_ids:
        errors.append("duplicate_signature_review_id")
    manual_seen_ids.add(signature_review_id)
    if signature_review_id not in frame_by_id:
        errors.append("signature_review_id_not_in_frame")
    if manual_status not in ALLOWED_MANUAL_STATUSES:
        errors.append("invalid_manual_status")
    if manual_status == "verified_project_geography" and verified_districts == "":
        errors.append("verified_project_geography_without_valid_district")

    if errors:
        manual_verdict_errors.append(
            {
                "manual_line_number": line_number,
                "signature_review_id": signature_review_id,
                "validation_errors": "|".join(errors),
            }
        )
        continue

    if manual_status == "verified_project_geography":
        source_row = frame_by_id[signature_review_id]
        response = response_by_id.get(signature_review_id, {})
        human_verified_rows.append(
            {
                "signature_review_id": signature_review_id,
                "query_year": source_row["query_year"],
                "vote_date": source_row["vote_date"],
                "vote_margin": source_row["vote_margin"],
                "matter_files": source_row["matter_files"],
                "application_keys": source_row["application_keys"],
                "verified_affected_council_districts": verified_districts,
                "verified_local_member_names": clean_value(verdict.get("verified_local_member_names", "")),
                "verified_project_name": clean_value(verdict.get("verified_project_name", "")),
                "verified_borough": clean_value(verdict.get("verified_borough", "")),
                "verification_source_type": clean_value(verdict.get("verification_source_type", "")),
                "verification_source_urls": clean_value(verdict.get("verification_source_urls", "")),
                "verification_note": clean_value(verdict.get("verification_note", "")),
                "reviewer": clean_value(verdict.get("reviewer", "")),
                "review_date": clean_value(verdict.get("review_date", "")),
                "ai_affected_council_districts": response.get("normalized_affected_council_districts", ""),
                "ai_local_member_names": response.get("local_member_names", ""),
                "ai_project_name": response.get("project_name", ""),
                "ai_evidence_note": response.get("evidence_note", ""),
            }
        )

browser_workflow_text = """# Browser ChatGPT Workflow for Missing Council Land-Use Geography

This task uses ChatGPT as a source-discovery assistant. AI rows are not final data.

## First-Pass Review Steps

1. Run `make` from this task's `code/` folder.
2. Open `../output/council_land_use_missing_geography_chatgpt_review_next_batch.md`.
3. In the Codex Browser ChatGPT tab, paste one batch at a time.
4. Ask ChatGPT to use web search only to find official or clearly citable records.
5. Copy the JSONL response exactly into `code/chatgpt_geography_review_responses.jsonl`.
6. Rerun `make`.
7. Read `../output/council_land_use_missing_geography_ai_repair_candidates.csv`.
8. Promote a row only after human source verification, using the cited Legistar/ZAP/DCP/CPC/ULURP source.

## Second-Pass Review Steps

1. Run `make` from this task's `code/` folder.
2. Open `../output/council_land_use_missing_geography_second_pass_next_batch.md`.
3. Submit the batch to ChatGPT.
4. Append ChatGPT's JSONL response exactly to `code/chatgpt_geography_second_pass_responses.jsonl`.
5. Rerun `make`.
6. Read `../output/council_land_use_missing_geography_second_pass_ai_repair_candidates.csv`.
7. Use the second-pass files to prioritize human source verification, not as final geography.

The second-pass queue reviews all rows. It puts the first-pass official candidates and human-verification rows first, then unresolved rows, then citywide/not-project rows as a challenge set.

## Adjudication-Pass Review Steps

1. Run `make` from this task's `code/` folder.
2. Open `../output/council_land_use_missing_geography_adjudication_next_batch.md`.
3. Submit the batch to ChatGPT.
4. Append ChatGPT's JSONL response exactly to `code/chatgpt_geography_adjudication_responses.jsonl`.
5. Rerun `make`.
6. Read `../output/council_land_use_missing_geography_adjudication_ai_repair_candidates.csv`.
7. Use `../output/council_land_use_missing_geography_adjudication_spot_check_queue.csv` as the highest-priority source-check list.

The adjudication pass covers all rows again. It treats first-pass and second-pass answers as claims, asks for a stricter source-read, and separates AI-adjudicated project geography from rows that should remain out or unresolved.

## Acceptance Rule

- `official_high_confidence_ai_candidate`: fastest to verify; still requires source check before promotion.
- `needs_human_verification`: includes title/address/search inference such as Dock Street; useful, but not analysis-ready.
- `not_project_geography`: citywide, text-only, or not land use; do not turn these into local-member geography.
- `unresolved_after_ai_review`: leave unresolved unless a separate manual search finds official evidence.
- `response_needs_correction`: fix the JSON/control vocabulary before review.
"""

qc_rows = [
    {
        "metric": "review_signature_count",
        "value": len(frame_rows),
        "status": "pass" if frame_rows else "fail",
        "note": "Missing-geography roll-call signatures in the review frame.",
    },
    {
        "metric": "probable_non_project_false_positive_count",
        "value": sum(row["probable_non_project_false_positive"] == "yes" for row in frame_rows),
        "status": "pass",
        "note": "These remain in the queue but are expected to be classified separately.",
    },
    {
        "metric": "batch_count",
        "value": len(batch_rows),
        "status": "pass" if batch_rows else "fail",
        "note": "Markdown batches generated for Browser/ChatGPT review.",
    },
    {
        "metric": "unique_response_count",
        "value": len(seen_ids),
        "status": "pass",
        "note": "Unique ChatGPT responses read from chatgpt_geography_review_responses.jsonl.",
    },
    {
        "metric": "missing_response_count",
        "value": len(missing_rows),
        "status": "pass" if not missing_rows else "needs_more_labels",
        "note": "Signatures still awaiting ChatGPT review.",
    },
    {
        "metric": "duplicate_response_count",
        "value": len(duplicate_ids),
        "status": "pass" if not duplicate_ids else "fail",
        "note": "|".join(sorted(duplicate_ids)),
    },
    {
        "metric": "response_error_count",
        "value": len(parse_errors) + len(validation_errors),
        "status": "pass" if not parse_errors and not validation_errors else "fail",
        "note": "Parse or controlled-vocabulary validation errors.",
    },
    {
        "metric": "official_high_confidence_ai_candidate_count",
        "value": sum(row["repair_candidate_category"] == "official_high_confidence_ai_candidate" for row in repair_candidate_rows),
        "status": "pass",
        "note": "Rows with resolved high-confidence official-source AI suggestions.",
    },
    {
        "metric": "needs_human_verification_count",
        "value": sum(row["repair_candidate_category"] == "needs_human_verification" for row in repair_candidate_rows),
        "status": "pass",
        "note": "Rows with plausible AI geography that require human source review.",
    },
    {
        "metric": "not_project_geography_count",
        "value": sum(row["repair_candidate_category"] == "not_project_geography" for row in repair_candidate_rows),
        "status": "pass",
        "note": "Rows classified as citywide, text-only, or not project geography.",
    },
    {
        "metric": "unresolved_after_ai_review_count",
        "value": sum(row["repair_candidate_category"] == "unresolved_after_ai_review" for row in repair_candidate_rows),
        "status": "pass",
        "note": "Rows still missing usable geography after AI review.",
    },
    {
        "metric": "second_pass_batch_count",
        "value": len(second_pass_batch_rows),
        "status": "pass" if second_pass_batch_rows else "fail",
        "note": "Markdown batches generated for second-pass Browser/ChatGPT review.",
    },
    {
        "metric": "second_pass_unique_response_count",
        "value": len(second_pass_seen_ids),
        "status": "pass",
        "note": "Unique second-pass responses read from chatgpt_geography_second_pass_responses.jsonl.",
    },
    {
        "metric": "second_pass_missing_response_count",
        "value": len(second_pass_missing_rows),
        "status": "pass" if not second_pass_missing_rows else "needs_more_labels",
        "note": "Signatures still awaiting second-pass review.",
    },
    {
        "metric": "second_pass_duplicate_response_count",
        "value": len(second_pass_duplicate_ids),
        "status": "pass" if not second_pass_duplicate_ids else "fail",
        "note": "|".join(sorted(second_pass_duplicate_ids)),
    },
    {
        "metric": "second_pass_response_error_count",
        "value": len(second_pass_parse_errors) + len(second_pass_validation_errors),
        "status": "pass" if not second_pass_parse_errors and not second_pass_validation_errors else "fail",
        "note": "Second-pass parse or controlled-vocabulary validation errors.",
    },
    {
        "metric": "second_pass_official_high_confidence_candidate_count",
        "value": sum(
            row["second_pass_candidate_category"] == "official_high_confidence_second_pass_candidate"
            for row in second_pass_candidate_rows
        ),
        "status": "pass",
        "note": "Rows with resolved high-confidence official-source second-pass suggestions.",
    },
    {
        "metric": "second_pass_needs_human_verification_count",
        "value": sum(row["second_pass_candidate_category"] == "needs_human_verification" for row in second_pass_candidate_rows),
        "status": "pass",
        "note": "Rows with plausible second-pass geography that require human source review.",
    },
    {
        "metric": "second_pass_not_project_geography_count",
        "value": sum(row["second_pass_candidate_category"] == "not_project_geography" for row in second_pass_candidate_rows),
        "status": "pass",
        "note": "Rows classified as citywide, text-only, or not project geography in second pass.",
    },
    {
        "metric": "second_pass_unresolved_count",
        "value": sum(row["second_pass_candidate_category"] == "unresolved_after_second_pass" for row in second_pass_candidate_rows),
        "status": "pass",
        "note": "Rows still missing usable geography after second-pass review.",
    },
    {
        "metric": "adjudication_batch_count",
        "value": len(adjudication_batch_rows),
        "status": "pass" if adjudication_batch_rows else "fail",
        "note": "Markdown batches generated for adjudication-pass Browser/ChatGPT review.",
    },
    {
        "metric": "adjudication_unique_response_count",
        "value": len(adjudication_seen_ids),
        "status": "pass",
        "note": "Unique adjudication-pass responses read from chatgpt_geography_adjudication_responses.jsonl.",
    },
    {
        "metric": "adjudication_missing_response_count",
        "value": len(adjudication_missing_rows),
        "status": "pass" if not adjudication_missing_rows else "needs_more_labels",
        "note": "Signatures still awaiting adjudication-pass review.",
    },
    {
        "metric": "adjudication_duplicate_response_count",
        "value": len(adjudication_duplicate_ids),
        "status": "pass" if not adjudication_duplicate_ids else "fail",
        "note": "|".join(sorted(adjudication_duplicate_ids)),
    },
    {
        "metric": "adjudication_response_error_count",
        "value": len(adjudication_parse_errors) + len(adjudication_validation_errors),
        "status": "pass" if not adjudication_parse_errors and not adjudication_validation_errors else "fail",
        "note": "Adjudication-pass parse or controlled-vocabulary validation errors.",
    },
    {
        "metric": "adjudication_official_project_geography_count",
        "value": sum(
            row["adjudication_candidate_category"] == "ai_adjudicated_official_project_geography"
            for row in adjudication_candidate_rows
        ),
        "status": "pass",
        "note": "Rows with official high-confidence project geography after adjudication pass.",
    },
    {
        "metric": "adjudication_official_location_strong_district_inferred_count",
        "value": sum(
            row["adjudication_candidate_category"] == "official_location_strong_district_inferred"
            for row in adjudication_candidate_rows
        ),
        "status": "pass",
        "note": "Rows with official project location evidence but inferred Council district after adjudication pass.",
    },
    {
        "metric": "adjudication_project_geography_spot_check_count",
        "value": sum(
            row["adjudication_candidate_category"]
            in {"official_location_strong_district_inferred", "ai_adjudicated_project_geography_needs_spot_check"}
            for row in adjudication_candidate_rows
        ),
        "status": "pass",
        "note": "Rows with plausible project geography that still require spot-check before manual verdict.",
    },
    {
        "metric": "adjudication_not_project_geography_count",
        "value": sum(
            row["adjudication_candidate_category"] == "ai_adjudicated_not_project_geography"
            for row in adjudication_candidate_rows
        ),
        "status": "pass",
        "note": "Rows adjudicated as citywide, text-only, or not project geography.",
    },
    {
        "metric": "adjudication_rejected_prior_geography_count",
        "value": sum(row["adjudication_candidate_category"] == "ai_rejected_prior_geography" for row in adjudication_candidate_rows),
        "status": "pass",
        "note": "Rows where adjudication rejected a prior AI geography claim.",
    },
    {
        "metric": "adjudication_unresolved_count",
        "value": sum(row["adjudication_candidate_category"] == "unresolved_after_adjudication" for row in adjudication_candidate_rows),
        "status": "pass",
        "note": "Rows still unresolved after adjudication pass.",
    },
    {
        "metric": "manual_verdict_count",
        "value": len([row for row in manual_verdict_rows if clean_value(row.get("signature_review_id", "")) != ""]),
        "status": "pass",
        "note": "Rows entered in the manual verdict ledger.",
    },
    {
        "metric": "manual_verdict_error_count",
        "value": len(manual_verdict_errors),
        "status": "pass" if not manual_verdict_errors else "fail",
        "note": "Manual verdict validation errors.",
    },
    {
        "metric": "human_verified_project_geography_count",
        "value": len(human_verified_rows),
        "status": "pass",
        "note": "Human-verified project-geography rows available for downstream repair.",
    },
]

write_csv_if_changed(
    frame_rows,
    [
        "signature_review_id",
        "queue_rank",
        "prompt_version",
        "repair_priority",
        "repair_priority_reason",
        "probable_non_project_false_positive",
        "query_year",
        "vote_date",
        "vote_source_group",
        "vote_margin",
        "affirmative_count",
        "negative_count",
        "abstain_count",
        "dissent_count",
        "matter_rows",
        "land_use_application_rows",
        "resolution_rows",
        "call_up_rows",
        "local_member_vote_rows",
        "matter_files",
        "application_keys",
        "zap_project_ids",
        "zap_project_names",
        "title_examples",
        "matter_urls",
        "history_detail_urls",
        "roll_call_signature",
    ],
    "../output/council_land_use_missing_geography_chatgpt_review_frame.csv",
)
write_text_if_changed(PROMPT_HEADER, "../output/council_land_use_missing_geography_chatgpt_review_prompt.md")
write_csv_if_changed(
    batch_rows,
    [
        "batch_id",
        "batch_path",
        "signature_count",
        "first_queue_rank",
        "last_queue_rank",
        "char_count",
        "signature_review_ids",
    ],
    "../output/council_land_use_missing_geography_chatgpt_review_batch_manifest.csv",
)
write_text_if_changed(batch_jsonl, "../output/council_land_use_missing_geography_chatgpt_review_batches.jsonl")
write_text_if_changed(
    PROMPT_HEADER + "\n".join(records[:batch_size]),
    "../output/council_land_use_missing_geography_chatgpt_review_next_batch.md",
)
write_csv_if_changed(
    combined_rows,
    [
        "source_response_line_number",
        "batch_id",
        *EXPECTED_RESPONSE_KEYS,
        "normalized_affected_council_districts",
        "validation_status",
        "validation_errors",
    ],
    "../output/council_land_use_missing_geography_chatgpt_review_responses_combined.csv",
)
write_csv_if_changed(
    missing_rows,
    [
        "signature_review_id",
        "batch_id",
        "queue_rank",
        "repair_priority",
        "repair_priority_reason",
        "query_year",
        "vote_date",
        "vote_margin",
        "matter_rows",
        "application_keys",
        "zap_project_names",
        "title_examples",
    ],
    "../output/council_land_use_missing_geography_chatgpt_review_missing_responses.csv",
)
write_csv_if_changed(
    parse_errors + validation_errors,
    ["response_line_number", "signature_review_id", "validation_errors", "raw_line"],
    "../output/council_land_use_missing_geography_chatgpt_review_response_errors.csv",
)
write_csv_if_changed(
    manual_review_rows,
    [
        "signature_review_id",
        "queue_rank",
        "batch_id",
        "repair_priority",
        "repair_priority_reason",
        "probable_non_project_false_positive",
        "query_year",
        "vote_date",
        "vote_source_group",
        "vote_margin",
        "negative_count",
        "abstain_count",
        "dissent_count",
        "matter_rows",
        "matter_files",
        "application_keys",
        "zap_project_names",
        "title_examples",
        "matter_urls",
        "history_detail_urls",
        "chatgpt_review_status",
        "chatgpt_affected_council_districts",
        "chatgpt_district_source",
        "chatgpt_district_confidence",
        "chatgpt_needs_human_review",
        "chatgpt_evidence_note",
        "chatgpt_human_review_reason",
    ],
    "../output/council_land_use_missing_geography_chatgpt_manual_review_queue.csv",
)
write_csv_if_changed(
    repair_candidate_rows,
    [
        "signature_review_id",
        "queue_rank",
        "batch_id",
        "query_year",
        "vote_date",
        "vote_margin",
        "matter_rows",
        "matter_files",
        "application_keys",
        "zap_project_ids",
        "zap_project_names",
        "matter_urls",
        "history_detail_urls",
        "title_examples",
        "ai_review_status",
        "ai_affected_council_districts",
        "ai_district_source",
        "ai_district_confidence",
        "ai_local_member_names",
        "ai_project_name",
        "ai_borough",
        "ai_official_sources_used",
        "ai_evidence_note",
        "ai_outside_knowledge_used",
        "ai_needs_human_review",
        "ai_human_review_reason",
        "ai_validation_status",
        "ai_validation_errors",
        "repair_candidate_category",
        "recommended_next_action",
    ],
    "../output/council_land_use_missing_geography_ai_repair_candidates.csv",
)
write_csv_if_changed(
    human_verification_rows,
    [
        "signature_review_id",
        "queue_rank",
        "batch_id",
        "query_year",
        "vote_date",
        "vote_margin",
        "matter_rows",
        "matter_files",
        "application_keys",
        "zap_project_ids",
        "zap_project_names",
        "matter_urls",
        "history_detail_urls",
        "title_examples",
        "ai_review_status",
        "ai_affected_council_districts",
        "ai_district_source",
        "ai_district_confidence",
        "ai_local_member_names",
        "ai_project_name",
        "ai_borough",
        "ai_official_sources_used",
        "ai_evidence_note",
        "ai_outside_knowledge_used",
        "ai_needs_human_review",
        "ai_human_review_reason",
        "ai_validation_status",
        "ai_validation_errors",
        "repair_candidate_category",
        "recommended_next_action",
    ],
    "../output/council_land_use_missing_geography_human_verification_queue.csv",
)
write_csv_if_changed(
    second_pass_frame_rows,
    [
        "signature_review_id",
        "queue_rank",
        "batch_id",
        "second_pass_prompt_version",
        "second_pass_priority",
        "second_pass_priority_reason",
        "query_year",
        "vote_date",
        "vote_margin",
        "matter_rows",
        "matter_files",
        "application_keys",
        "zap_project_ids",
        "zap_project_names",
        "matter_urls",
        "history_detail_urls",
        "title_examples",
        "ai_review_status",
        "ai_affected_council_districts",
        "ai_district_source",
        "ai_district_confidence",
        "ai_local_member_names",
        "ai_project_name",
        "ai_borough",
        "ai_official_sources_used",
        "ai_evidence_note",
        "repair_candidate_category",
        "recommended_next_action",
    ],
    "../output/council_land_use_missing_geography_second_pass_frame.csv",
)
write_text_if_changed(SECOND_PASS_PROMPT_HEADER, "../output/council_land_use_missing_geography_second_pass_prompt.md")
write_csv_if_changed(
    second_pass_batch_rows,
    [
        "batch_id",
        "batch_path",
        "signature_count",
        "first_second_pass_priority",
        "last_second_pass_priority",
        "first_queue_rank",
        "last_queue_rank",
        "char_count",
        "signature_review_ids",
    ],
    "../output/council_land_use_missing_geography_second_pass_batch_manifest.csv",
)
write_text_if_changed(second_pass_batch_jsonl, "../output/council_land_use_missing_geography_second_pass_batches.jsonl")
write_text_if_changed(
    SECOND_PASS_PROMPT_HEADER + "\n".join(second_pass_records[:batch_size]),
    "../output/council_land_use_missing_geography_second_pass_next_batch.md",
)
write_csv_if_changed(
    second_pass_combined_rows,
    [
        "source_response_line_number",
        "batch_id",
        *EXPECTED_SECOND_PASS_KEYS,
        "normalized_affected_council_districts",
        "validation_status",
        "validation_errors",
    ],
    "../output/council_land_use_missing_geography_second_pass_responses_combined.csv",
)
write_csv_if_changed(
    second_pass_missing_rows,
    [
        "signature_review_id",
        "batch_id",
        "queue_rank",
        "second_pass_priority",
        "second_pass_priority_reason",
        "query_year",
        "vote_date",
        "vote_margin",
        "matter_rows",
        "application_keys",
        "zap_project_names",
        "first_pass_category",
        "first_pass_districts",
        "title_examples",
    ],
    "../output/council_land_use_missing_geography_second_pass_missing_responses.csv",
)
write_csv_if_changed(
    second_pass_parse_errors + second_pass_validation_errors,
    ["response_line_number", "signature_review_id", "validation_errors", "raw_line"],
    "../output/council_land_use_missing_geography_second_pass_response_errors.csv",
)
write_csv_if_changed(
    second_pass_candidate_rows,
    [
        "signature_review_id",
        "queue_rank",
        "batch_id",
        "second_pass_priority",
        "second_pass_priority_reason",
        "query_year",
        "vote_date",
        "vote_margin",
        "matter_rows",
        "matter_files",
        "application_keys",
        "zap_project_ids",
        "zap_project_names",
        "title_examples",
        "first_pass_category",
        "first_pass_status",
        "first_pass_districts",
        "first_pass_local_member_names",
        "first_pass_project_name",
        "second_pass_status",
        "second_pass_affected_council_districts",
        "second_pass_district_source",
        "second_pass_district_confidence",
        "second_pass_local_member_names",
        "second_pass_project_name",
        "second_pass_borough",
        "second_pass_project_area",
        "second_pass_official_sources_used",
        "second_pass_evidence_note",
        "second_pass_disagreement_with_first_pass",
        "second_pass_outside_knowledge_used",
        "second_pass_needs_human_review",
        "second_pass_human_review_reason",
        "second_pass_validation_status",
        "second_pass_validation_errors",
        "second_pass_candidate_category",
        "second_pass_recommended_next_action",
    ],
    "../output/council_land_use_missing_geography_second_pass_ai_repair_candidates.csv",
)
write_csv_if_changed(
    second_pass_human_verification_rows,
    [
        "signature_review_id",
        "queue_rank",
        "batch_id",
        "second_pass_priority",
        "second_pass_priority_reason",
        "query_year",
        "vote_date",
        "vote_margin",
        "matter_rows",
        "matter_files",
        "application_keys",
        "zap_project_ids",
        "zap_project_names",
        "title_examples",
        "first_pass_category",
        "first_pass_status",
        "first_pass_districts",
        "first_pass_local_member_names",
        "first_pass_project_name",
        "second_pass_status",
        "second_pass_affected_council_districts",
        "second_pass_district_source",
        "second_pass_district_confidence",
        "second_pass_local_member_names",
        "second_pass_project_name",
        "second_pass_borough",
        "second_pass_project_area",
        "second_pass_official_sources_used",
        "second_pass_evidence_note",
        "second_pass_disagreement_with_first_pass",
        "second_pass_outside_knowledge_used",
        "second_pass_needs_human_review",
        "second_pass_human_review_reason",
        "second_pass_validation_status",
        "second_pass_validation_errors",
        "second_pass_candidate_category",
        "second_pass_recommended_next_action",
    ],
    "../output/council_land_use_missing_geography_second_pass_human_verification_queue.csv",
)
write_csv_if_changed(
    adjudication_frame_rows,
    [
        "signature_review_id",
        "queue_rank",
        "second_pass_batch_id",
        "adjudication_prompt_version",
        "adjudication_priority",
        "adjudication_priority_reason",
        "query_year",
        "vote_date",
        "vote_margin",
        "matter_rows",
        "matter_files",
        "application_keys",
        "zap_project_ids",
        "zap_project_names",
        "matter_urls",
        "history_detail_urls",
        "title_examples",
        "first_pass_category",
        "first_pass_status",
        "first_pass_districts",
        "second_pass_candidate_category",
        "second_pass_status",
        "second_pass_affected_council_districts",
        "second_pass_district_source",
        "second_pass_district_confidence",
        "second_pass_project_name",
        "second_pass_project_area",
        "second_pass_evidence_note",
        "second_pass_disagreement_with_first_pass",
        "second_pass_human_review_reason",
    ],
    "../output/council_land_use_missing_geography_adjudication_frame.csv",
)
write_text_if_changed(ADJUDICATION_PROMPT_HEADER, "../output/council_land_use_missing_geography_adjudication_prompt.md")
write_csv_if_changed(
    adjudication_batch_rows,
    [
        "batch_id",
        "batch_path",
        "signature_count",
        "first_adjudication_priority",
        "last_adjudication_priority",
        "first_queue_rank",
        "last_queue_rank",
        "char_count",
        "signature_review_ids",
    ],
    "../output/council_land_use_missing_geography_adjudication_batch_manifest.csv",
)
write_text_if_changed(adjudication_batch_jsonl, "../output/council_land_use_missing_geography_adjudication_batches.jsonl")
write_text_if_changed(
    ADJUDICATION_PROMPT_HEADER + "\n".join(adjudication_records[:batch_size]),
    "../output/council_land_use_missing_geography_adjudication_next_batch.md",
)
write_csv_if_changed(
    adjudication_combined_rows,
    [
        "source_response_line_number",
        "batch_id",
        *EXPECTED_ADJUDICATION_KEYS,
        "normalized_affected_council_districts",
        "validation_status",
        "validation_errors",
    ],
    "../output/council_land_use_missing_geography_adjudication_responses_combined.csv",
)
write_csv_if_changed(
    adjudication_missing_rows,
    [
        "signature_review_id",
        "batch_id",
        "queue_rank",
        "adjudication_priority",
        "adjudication_priority_reason",
        "query_year",
        "vote_date",
        "vote_margin",
        "matter_rows",
        "application_keys",
        "zap_project_names",
        "second_pass_category",
        "second_pass_districts",
        "title_examples",
    ],
    "../output/council_land_use_missing_geography_adjudication_missing_responses.csv",
)
write_csv_if_changed(
    adjudication_parse_errors + adjudication_validation_errors,
    ["response_line_number", "signature_review_id", "validation_errors", "raw_line"],
    "../output/council_land_use_missing_geography_adjudication_response_errors.csv",
)
write_csv_if_changed(
    adjudication_candidate_rows,
    [
        "signature_review_id",
        "queue_rank",
        "batch_id",
        "adjudication_priority",
        "adjudication_priority_reason",
        "query_year",
        "vote_date",
        "vote_margin",
        "matter_rows",
        "matter_files",
        "application_keys",
        "zap_project_ids",
        "zap_project_names",
        "matter_urls",
        "history_detail_urls",
        "title_examples",
        "first_pass_category",
        "first_pass_status",
        "first_pass_districts",
        "second_pass_category",
        "second_pass_status",
        "second_pass_districts",
        "second_pass_project_name",
        "adjudication_status",
        "adjudication_affected_council_districts",
        "adjudication_district_source",
        "adjudication_district_confidence",
        "adjudication_local_member_names",
        "adjudication_project_name",
        "adjudication_borough",
        "adjudication_project_area",
        "adjudication_official_sources_used",
        "adjudication_source_check_summary",
        "adjudication_prior_pass_agreement",
        "adjudication_evidence_limitations",
        "adjudication_recommended_researcher_action",
        "adjudication_needs_human_review",
        "adjudication_human_review_reason",
        "adjudication_validation_status",
        "adjudication_validation_errors",
        "adjudication_candidate_category",
        "adjudication_next_action",
    ],
    "../output/council_land_use_missing_geography_adjudication_ai_repair_candidates.csv",
)
write_csv_if_changed(
    adjudication_spot_check_rows,
    [
        "signature_review_id",
        "queue_rank",
        "batch_id",
        "adjudication_priority",
        "adjudication_priority_reason",
        "query_year",
        "vote_date",
        "vote_margin",
        "matter_rows",
        "matter_files",
        "application_keys",
        "zap_project_ids",
        "zap_project_names",
        "matter_urls",
        "history_detail_urls",
        "title_examples",
        "first_pass_category",
        "first_pass_status",
        "first_pass_districts",
        "second_pass_category",
        "second_pass_status",
        "second_pass_districts",
        "second_pass_project_name",
        "adjudication_status",
        "adjudication_affected_council_districts",
        "adjudication_district_source",
        "adjudication_district_confidence",
        "adjudication_local_member_names",
        "adjudication_project_name",
        "adjudication_borough",
        "adjudication_project_area",
        "adjudication_official_sources_used",
        "adjudication_source_check_summary",
        "adjudication_prior_pass_agreement",
        "adjudication_evidence_limitations",
        "adjudication_recommended_researcher_action",
        "adjudication_needs_human_review",
        "adjudication_human_review_reason",
        "adjudication_validation_status",
        "adjudication_validation_errors",
        "adjudication_candidate_category",
        "adjudication_next_action",
    ],
    "../output/council_land_use_missing_geography_adjudication_spot_check_queue.csv",
)
write_csv_if_changed(
    human_verified_rows,
    [
        "signature_review_id",
        "query_year",
        "vote_date",
        "vote_margin",
        "matter_files",
        "application_keys",
        "verified_affected_council_districts",
        "verified_local_member_names",
        "verified_project_name",
        "verified_borough",
        "verification_source_type",
        "verification_source_urls",
        "verification_note",
        "reviewer",
        "review_date",
        "ai_affected_council_districts",
        "ai_local_member_names",
        "ai_project_name",
        "ai_evidence_note",
    ],
    "../output/council_land_use_missing_geography_human_verified_repairs.csv",
)
write_csv_if_changed(
    manual_verdict_errors,
    ["manual_line_number", "signature_review_id", "validation_errors"],
    "../output/council_land_use_missing_geography_manual_verdict_errors.csv",
)
write_text_if_changed(browser_workflow_text, "../output/council_land_use_missing_geography_browser_chatgpt_workflow.md")
write_csv_if_changed(qc_rows, ["metric", "value", "status", "note"], "../output/council_land_use_missing_geography_chatgpt_review_qc.csv")

print(f"Wrote {len(batch_rows)} ChatGPT review batches for {len(frame_rows)} signatures.")
