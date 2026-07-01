from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_csv(path: str, df: pd.DataFrame) -> None:
    temp_path = f"{path}.tmp"
    df.to_csv(temp_path, index=False)
    Path(temp_path).replace(path)


def bool_series(value: pd.Series) -> pd.Series:
    return value.astype(str).str.lower().isin(["true", "1"])


def review_difficulty(row: pd.Series) -> str:
    if row["title_address_candidate"] != "":
        return "medium_address_or_range_review"
    if row["title_bbls"] != "":
        return "medium_historical_bbl_review"
    if row["application_keys"] != "":
        return "document_lookup_review"
    return "low_information_manual_review"


def review_prompt(row: pd.Series) -> str:
    return "\n".join(
        [
            f"Matter: {row['matter_file']} ({row['query_year']})",
            f"Disposition: {row['disposition_group']} / {row['final_history_action']}",
            f"Application keys: {row['application_keys'] or 'none parsed'}",
            f"Parsed title BBLs: {row['title_bbls'] or 'none parsed'}",
            f"Parsed title address: {row['title_address_candidate'] or 'none parsed'}",
            f"Full title: {row['title']}",
            (
                "Task: Identify the affected NYC location and likely Council district. "
                "Prefer official CPC, ULURP, Legistar, HPD, LPC, or city records. "
                "Do not guess if the title is ambiguous."
            ),
        ]
    )


recovery = pd.read_csv("../input/member_deference_nonapproval_geography_recovery.csv", dtype=str, keep_default_na=False)
target_queue = pd.read_csv("../input/member_deference_final_action_vote_queue.csv", dtype=str, keep_default_na=False)

recovery["original_affected_district_missing_bool"] = bool_series(recovery["original_affected_district_missing"])
recovery["recovered_affected_district_missing_bool"] = bool_series(recovery["recovered_affected_district_missing"])
recovery["recovered_district_count_numeric"] = pd.to_numeric(recovery["recovered_district_count"], errors="coerce").fillna(0)

summary = (
    recovery.groupby(["original_affected_district_missing", "geography_recovery_method"], as_index=False)
    .agg(
        matter_count=("matter_id", "size"),
        recovered_matter_count=("recovered_affected_district_missing_bool", lambda x: int((~x).sum())),
        multi_district_matter_count=("recovered_district_count_numeric", lambda x: int((x > 1).sum())),
    )
    .sort_values(["original_affected_district_missing", "geography_recovery_method"])
)
write_csv("../output/member_deference_nonapproval_geography_recovery_summary.csv", summary)

write_csv(
    "../output/member_deference_nonapproval_geography_recovery_unresolved.csv",
    recovery[recovery["recovered_affected_district_missing_bool"]].drop(
        columns=[
            "original_affected_district_missing_bool",
            "recovered_affected_district_missing_bool",
            "recovered_district_count_numeric",
        ]
    ),
)

write_csv(
    "../output/member_deference_nonapproval_application_crosswalk.csv",
    recovery[
        [
            "matter_id",
            "matched_application_keys",
            "zap_project_ids",
            "zap_project_names",
            "zap_project_count",
            "zap_project_cc_districts",
            "zap_project_council_district_first",
            "zap_project_bbl_count",
            "zap_project_bbl_current_mappluto_council_districts",
            "zap_project_bbl_examples",
        ]
    ].copy(),
)
write_csv(
    "../output/member_deference_nonapproval_title_location_candidates.csv",
    recovery[
        [
            "matter_id",
            "matter_file",
            "title_borough_code_for_location_parse",
            "title_borough_source_for_location_parse",
            "title_bbls",
            "title_address_candidate",
            "title_address_normalized",
            "title_bbl_count",
            "title_bbl_current_mappluto_match_count",
            "title_bbl_current_mappluto_council_districts",
            "title_address_current_mappluto_council_districts",
            "title_address_current_mappluto_bbl_examples",
            "title_address_current_mappluto_bbl_count",
            "title_address_variant_count",
            "title_address_variant_current_mappluto_match_count",
            "title_address_variant_current_mappluto_council_districts",
            "title_address_variant_matched_examples",
            "title_address_variant_current_mappluto_bbl_examples",
        ]
    ].copy(),
)

review_queue = recovery[recovery["recovered_affected_district_missing_bool"]].copy()
review_queue["review_difficulty"] = review_queue.apply(review_difficulty, axis=1)
review_queue["chatgpt_plain_text_prompt"] = review_queue.apply(review_prompt, axis=1)
review_queue = review_queue[
    [
        "query_year",
        "matter_id",
        "matter_file",
        "disposition_group",
        "review_difficulty",
        "application_keys",
        "title_bbls",
        "title_address_candidate",
        "title",
        "chatgpt_plain_text_prompt",
    ]
]
write_csv("../output/member_deference_nonapproval_geography_review_queue.csv", review_queue)

batch_lines = [
    "# Member-Deference Nonapproval Geography Review Batches",
    "",
    (
        "Use these prompts in small batches. Any answer must be checked against "
        "official records before becoming final geography."
    ),
]
for batch_start in range(0, len(review_queue), 5):
    batch = review_queue.iloc[batch_start : batch_start + 5]
    batch_lines.extend(["", f"## Batch {batch_start // 5 + 1}", ""])
    for _, row in batch.iterrows():
        batch_lines.extend(["```text", row["chatgpt_plain_text_prompt"], "```", ""])
Path("../output/member_deference_nonapproval_geography_review_batches.md").write_text(
    "\n".join(batch_lines),
    encoding="utf-8",
)

qc = pd.DataFrame(
    [
        {
            "check_name": "first_pass_nonapproval_rows",
            "passed": str(len(recovery) == len(target_queue)),
            "detail": f"Recovery output keeps all {len(target_queue)} first-pass nonapproval matters.",
        },
        {
            "check_name": "recovery_unique_by_matter_id",
            "passed": str(not recovery["matter_id"].duplicated().any()),
            "detail": "Recovery output is unique by matter_id.",
        },
        {
            "check_name": "review_queue_matches_unresolved",
            "passed": str(len(review_queue) == int(recovery["recovered_affected_district_missing_bool"].sum())),
            "detail": f"Review queue contains {len(review_queue)} unresolved first-pass rows.",
        },
    ]
)
write_csv("../output/member_deference_nonapproval_geography_recovery_qc.csv", qc)
