#!/usr/bin/env python3

from pathlib import Path
import re

import pandas as pd




def split_semicolon(value):
    if pd.isna(value) or str(value).strip() == "":
        return []
    return [part.strip() for part in str(value).split(";") if part.strip()]


def collapse_int_strings(values):
    districts = []
    for value in values:
        for match in re.findall(r"\d{1,2}", "" if pd.isna(value) else str(value)):
            district = str(int(match))
            if 1 <= int(district) <= 51 and district not in districts:
                districts.append(district)
    return "; ".join(districts)


def clean_text(value):
    return re.sub(r"\s+", " ", "" if pd.isna(value) else str(value)).strip()


def write_csv(path, df):
    temp_path = Path(path).with_suffix(Path(path).suffix + ".tmp")
    df.to_csv(temp_path, index=False)
    temp_path.replace(path)


candidate_frame = pd.read_csv(
    "../input/council_land_use_missing_geography_adjudication_ai_repair_candidates.csv",
    dtype=str,
    keep_default_na=False,
)
deterministic_repairs = pd.read_csv(
    "../output/council_land_use_ai_geography_deterministic_repairs.csv",
    dtype=str,
    keep_default_na=False,
)
manual_queue_summary = pd.read_csv(
    "../output/manual_queue_web_review_batches/manual_queue_web_review_summary.csv",
    dtype=str,
    keep_default_na=False,
)
remaining_queue_summary = pd.read_csv(
    "../output/remaining_queue_web_review_batches/remaining_queue_web_review_summary.csv",
    dtype=str,
    keep_default_na=False,
)

for name, df in [
    ("candidate_frame", candidate_frame),
    ("deterministic_repairs", deterministic_repairs),
    ("manual_queue_summary", manual_queue_summary),
    ("remaining_queue_summary", remaining_queue_summary),
]:
    if df["signature_review_id"].duplicated().any():
        raise RuntimeError(f"{name} must be unique by signature_review_id.")

candidate_by_id = candidate_frame.set_index("signature_review_id").to_dict("index")
deterministic_by_id = deterministic_repairs.set_index("signature_review_id").to_dict("index")

accepted_signature_rows = []
manual_accept = manual_queue_summary[
    manual_queue_summary["human_review_needed_after_spot_check"].eq("no")
    & manual_queue_summary["promotion_decision"].isin(["promote", "promote_with_caveat"])
    & manual_queue_summary["recommended_council_districts"].str.strip().ne("")
].copy()
for row in manual_accept.to_dict("records"):
    accepted_signature_rows.append(
        {
            "signature_review_id": row["signature_review_id"],
            "accepted_council_districts": collapse_int_strings([row["recommended_council_districts"]]),
            "repair_source": "manual_queue_ai_review_researcher_accepted",
            "repair_confidence": row["confidence"],
            "repair_promotion_decision": row["promotion_decision"],
            "repair_evidence_type": row["evidence_type"],
            "repair_note": row["codex_review_note"],
        }
    )

remaining_accept = remaining_queue_summary[
    remaining_queue_summary["final_include_in_geography_repair"].eq("yes")
    & remaining_queue_summary["final_recommended_council_districts"].str.strip().ne("")
].copy()
for row in remaining_accept.to_dict("records"):
    if row["signature_review_id"] in set(manual_accept["signature_review_id"]):
        continue
    if row["signature_review_id"] in deterministic_by_id:
        deterministic = deterministic_by_id[row["signature_review_id"]]
        accepted_signature_rows.append(
            {
                "signature_review_id": row["signature_review_id"],
                "accepted_council_districts": collapse_int_strings([deterministic["manual_verdict_districts"]]),
                "repair_source": "deterministic_geography_verification",
                "repair_confidence": "high",
                "repair_promotion_decision": deterministic["manual_verdict_status"],
                "repair_evidence_type": deterministic["deterministic_verification_basis"],
                "repair_note": deterministic["manual_verdict_note"],
            }
        )
    else:
        accepted_signature_rows.append(
            {
                "signature_review_id": row["signature_review_id"],
                "accepted_council_districts": collapse_int_strings([row["final_recommended_council_districts"]]),
                "repair_source": "remaining_queue_ai_review_researcher_adjudicated",
                "repair_confidence": row["confidence"],
                "repair_promotion_decision": row["final_promotion_decision"],
                "repair_evidence_type": row["evidence_type"],
                "repair_note": row["codex_review_note"],
            }
        )

accepted_signatures = pd.DataFrame(accepted_signature_rows)
if accepted_signatures.empty:
    raise RuntimeError("No accepted geography repairs were built.")
if accepted_signatures["signature_review_id"].duplicated().any():
    raise RuntimeError("Accepted geography repairs must be unique by signature_review_id.")
if accepted_signatures["accepted_council_districts"].str.strip().eq("").any():
    raise RuntimeError("Accepted geography repairs must have nonempty district assignments.")

accepted_not_current = accepted_signatures[
    ~accepted_signatures["signature_review_id"].isin(candidate_frame["signature_review_id"])
].copy()
accepted_not_current["excluded_reason"] = "accepted_signature_not_in_current_missing_geography_queue"
write_csv(
    "../output/council_land_use_ai_geography_accepted_repairs_excluded_by_current_queue.csv",
    accepted_not_current,
)

accepted_signatures = accepted_signatures[
    accepted_signatures["signature_review_id"].isin(candidate_frame["signature_review_id"])
].copy()
if accepted_signatures.empty:
    raise RuntimeError("No accepted geography repairs remain in the current candidate frame.")

accepted_matter_rows = []
for row in accepted_signatures.sort_values("signature_review_id").to_dict("records"):
    source = candidate_by_id[row["signature_review_id"]]
    matter_files = split_semicolon(source["matter_files"])
    if not matter_files:
        raise RuntimeError(f"Accepted repair has no matter files: {row['signature_review_id']}")
    for matter_file in matter_files:
        accepted_matter_rows.append(
            {
                "query_year": source["query_year"],
                "vote_date": pd.to_datetime(source["vote_date"]).strftime("%Y-%m-%d"),
                "matter_file": matter_file,
                "signature_review_id": row["signature_review_id"],
                "accepted_council_districts": row["accepted_council_districts"],
                "repair_source": row["repair_source"],
                "repair_confidence": row["repair_confidence"],
                "repair_promotion_decision": row["repair_promotion_decision"],
                "repair_evidence_type": row["repair_evidence_type"],
                "repair_note": clean_text(row["repair_note"]),
                "signature_matter_files": source["matter_files"],
                "application_keys": source["application_keys"],
                "zap_project_ids": source["zap_project_ids"],
                "zap_project_names": source["zap_project_names"],
                "title_examples": clean_text(source["title_examples"]),
                "matter_urls": source["matter_urls"],
                "history_detail_urls": source["history_detail_urls"],
            }
        )

accepted_repairs = pd.DataFrame(accepted_matter_rows).sort_values(["query_year", "vote_date", "matter_file"])
if accepted_repairs.duplicated(["query_year", "vote_date", "matter_file"]).any():
    duplicates = accepted_repairs[
        accepted_repairs.duplicated(["query_year", "vote_date", "matter_file"], keep=False)
    ][["query_year", "vote_date", "matter_file", "signature_review_id"]]
    raise RuntimeError(f"Accepted repair rows are not unique by query_year, vote_date, matter_file:\n{duplicates}")

write_csv("../output/council_land_use_ai_geography_accepted_repairs.csv", accepted_repairs)
