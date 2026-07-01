from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_csv(path: str, df: pd.DataFrame) -> None:
    temp_path = f"{path}.tmp"
    df.to_csv(temp_path, index=False)
    Path(temp_path).replace(path)


def collapse_values(values: object) -> str:
    clean_values = []
    for value in values:
        if pd.isna(value) or str(value).strip() == "":
            continue
        for part in str(value).split(";"):
            part = part.strip()
            if part and part not in clean_values:
                clean_values.append(part)
    return "; ".join(clean_values)


def bool_series(value: pd.Series) -> pd.Series:
    return value.astype(str).str.lower().isin(["true", "1"])


def second_pass_prompt(row: pd.Series) -> str:
    return "\n".join(
        [
            f"Matter: {row['matter_file']} ({row['query_year']})",
            f"Matter ID: {row['matter_id']}",
            f"Application keys: {row['application_keys'] or 'none parsed'}",
            f"Initial ChatGPT suggested district(s): {row['chatgpt_suggested_districts_parsed'] or 'none parsed'}",
            f"First official verification status: {row['verification_status']}",
            f"First official verification notes: {row['verification_notes']}",
            f"Official matter URL: {row['matter_url']}",
            f"Official source URL recorded: {row['verification_source_url'] or 'none'}",
            f"Official direct districts recorded: {row['verified_direct_official_districts'] or 'none'}",
            f"Official BBLs seen in exact matter text: {row['official_matter_bbl_examples'] or 'none'}",
            f"Official BBL current MapPLUTO match count: {row['official_matter_bbl_current_mappluto_match_count']} of {row['official_matter_bbl_count']}",
            f"Official BBL current MapPLUTO district(s): {row['official_matter_bbl_current_mappluto_districts'] or 'none'}",
            f"Title: {row['title']}",
            (
                "Task: Find whether this matter can be verified to a Council district using official records only. "
                "Prefer official Legistar LU pages, Council pages, CPC/DCP calendars or reports, HPD/LPC/DOT records, "
                "ZAP records, or official city GIS/district tools. Do not use property websites as final evidence. "
                "Use final_status='still_unverified' if no official source supports the district."
            ),
        ]
    )


verification = pd.read_csv(
    "../input/member_deference_nonapproval_geography_official_verification.csv",
    dtype=str,
    keep_default_na=False,
)
conservative_queue = pd.read_csv(
    "../input/member_deference_nonapproval_geography_conservative_queue.csv",
    dtype=str,
    keep_default_na=False,
)

official_sources = verification[
    [
        "matter_id",
        "matter_file",
        "verification_status",
        "verification_evidence_level",
        "verification_source_url",
        "verification_source_relation",
        "verified_direct_official_url",
        "verified_direct_official_relation",
        "verified_direct_official_snippet",
        "official_source_to_check_or_source_url",
        "source_links_found_in_cell",
        "matter_url",
        "final_history_detail_url",
        "title",
    ]
].copy()
write_csv("../output/member_deference_nonapproval_geography_official_sources.csv", official_sources)

conservative_queue["affected_districts_conservative_missing_bool"] = bool_series(
    conservative_queue["affected_districts_conservative_missing"]
)
conservative_summary = (
    conservative_queue.groupby("geography_incorporation_status", as_index=False)
    .agg(
        matter_count=("matter_id", "size"),
        usable_geography_count=("affected_districts_conservative_missing_bool", lambda x: int((~x).sum())),
        local_member_count=("local_members_from_roster", lambda x: int((x.astype(str).str.strip() != "").sum())),
    )
    .sort_values("geography_incorporation_status")
)
write_csv("../output/member_deference_nonapproval_geography_conservative_summary.csv", conservative_summary)

verification["verified_districts_match_chatgpt_bool"] = bool_series(verification["verified_districts_match_chatgpt"])
summary = (
    verification.groupby(["verification_status", "verification_evidence_level"], as_index=False)
    .agg(
        matter_count=("matter_id", "size"),
        chatgpt_agreement_count=("verified_districts_match_chatgpt_bool", lambda x: int(x.sum())),
    )
    .sort_values(["verification_status", "verification_evidence_level"])
)
write_csv("../output/member_deference_nonapproval_geography_official_verification_summary.csv", summary)

second_pass_queue = verification[~verification["verification_status"].str.startswith("verified_")].copy()
second_pass_queue["official_urls_checked"] = second_pass_queue[
    ["verification_source_url", "verified_direct_official_url", "official_source_to_check_or_source_url"]
].agg(collapse_values, axis=1)
second_pass_queue["official_direct_districts_seen"] = second_pass_queue["verified_direct_official_districts"]
second_pass_queue["official_bbls_seen"] = second_pass_queue["official_matter_bbl_examples"]
second_pass_queue["second_pass_chatgpt_prompt"] = second_pass_queue.apply(second_pass_prompt, axis=1)
second_pass_queue = second_pass_queue[
    [
        "query_year",
        "matter_id",
        "matter_file",
        "disposition_group",
        "verification_status",
        "verification_evidence_level",
        "chatgpt_suggested_districts_parsed",
        "application_keys",
        "official_matter_bbl_count",
        "official_matter_bbl_current_mappluto_match_count",
        "official_matter_bbl_unmatched_count",
        "official_matter_bbl_current_mappluto_districts",
        "official_matter_bbl_examples",
        "official_urls_checked",
        "official_direct_districts_seen",
        "official_bbls_seen",
        "title",
        "second_pass_chatgpt_prompt",
    ]
]
write_csv("../output/member_deference_nonapproval_geography_second_pass_review_queue.csv", second_pass_queue)

batch_lines = [
    "# Member-Deference Nonapproval Geography Second-Pass Review Batches",
    "",
    (
        "These prompts are for unresolved rows after the first official verification pass. "
        "Any ChatGPT answer remains a lead until the official source is checked and entered into the ledger."
    ),
]
for batch_start in range(0, len(second_pass_queue), 5):
    batch = second_pass_queue.iloc[batch_start : batch_start + 5]
    batch_lines.extend(["", f"## Batch {batch_start // 5 + 1}", ""])
    for _, row in batch.iterrows():
        batch_lines.extend(["```text", row["second_pass_chatgpt_prompt"], "```", ""])
Path("../output/member_deference_nonapproval_geography_second_pass_review_batches.md").write_text(
    "\n".join(batch_lines),
    encoding="utf-8",
)

qc = pd.DataFrame(
    [
        {
            "check_name": "official_verification_unique_by_matter_id",
            "passed": str(not verification["matter_id"].duplicated().any()),
            "detail": "Official verification table is unique by matter_id.",
        },
        {
            "check_name": "conservative_queue_unique_by_matter_id",
            "passed": str(not conservative_queue["matter_id"].duplicated().any()),
            "detail": "Conservative incorporated queue is unique by matter_id.",
        },
        {
            "check_name": "verified_rows_have_districts",
            "passed": str(
                verification.loc[
                    verification["verification_status"].str.startswith("verified_"), "verified_districts"
                ]
                .ne("")
                .all()
            ),
            "detail": "Every verified row has at least one verified district.",
        },
    ]
)
write_csv("../output/member_deference_nonapproval_geography_official_verification_qc.csv", qc)
