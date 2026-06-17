# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/fetch_member_deference_nonapproval_action_votes/code")

from __future__ import annotations

import hashlib
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup


def normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", "" if value is None or pd.isna(value) else str(value)).strip()


def safe_stub(value: object) -> str:
    stub = re.sub(r"[^a-z0-9]+", "_", normalize_space(value).lower()).strip("_")
    return stub or "missing"


def norm_name(value: object) -> str:
    value = re.sub(r"[^A-Za-z0-9 ]", " ", "" if value is None or pd.isna(value) else str(value))
    return re.sub(r"\s+", " ", value).strip().lower()


def edge_name(value: object) -> str:
    parts = norm_name(value).split()
    if len(parts) < 2:
        return norm_name(value)
    return f"{parts[0]} {parts[-1]}"


def split_semicolon(value: object) -> list[str]:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return []
    return [part.strip() for part in str(value).split(";") if part.strip()]


def collapse_values(values: object) -> str:
    clean_values = []
    for value in values:
        if value is None or pd.isna(value) or str(value).strip() == "":
            continue
        if str(value) not in clean_values:
            clean_values.append(str(value))
    return "; ".join(clean_values)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def request_with_retries(session: requests.Session, url: str) -> requests.Response:
    last_error = None
    for attempt in range(1, 4):
        try:
            response = session.get(url, timeout=90)
            response.raise_for_status()
            return response
        except requests.RequestException as error:
            last_error = error
            if attempt == 3:
                break
            time.sleep(5 * attempt)
    raise last_error


def table_value(soup: BeautifulSoup, table_id: str) -> str | None:
    table = soup.find("table", id=table_id)
    if table is None:
        return None

    cells = [normalize_space(cell.get_text(" ")) for cell in table.find_all("td")]
    cells = [cell for cell in cells if cell]
    if len(cells) < 2:
        return None

    return cells[1]


def extract_person_id_and_guid(href: str | None) -> tuple[str | None, str | None]:
    if not href:
        return None, None

    parsed = urlparse(href.replace("&amp;", "&"))
    query = parse_qs(parsed.query)
    return query.get("ID", [None])[0], query.get("GUID", [None])[0]


def parse_action_detail(html: str) -> tuple[dict[str, object], list[dict[str, object]]]:
    soup = BeautifulSoup(html, "html.parser")
    vote_label = None
    vote_tab = soup.find("div", id=re.compile(r"tabBottom"))
    if vote_tab is not None:
        vote_label = normalize_space(vote_tab.get_text(" "))

    record_count = None
    record_menu = soup.find("div", id=re.compile(r"menuVote"))
    if record_menu is not None:
        match = re.search(r"([0-9,]+)\s+records", normalize_space(record_menu.get_text(" ")))
        if match:
            record_count = int(match.group(1).replace(",", ""))

    votes = []
    vote_table = soup.find("table", id=re.compile(r"gridVote_ctl00$"))
    if vote_table is not None:
        for tr in vote_table.select("tr.rgRow, tr.rgAltRow"):
            cells = tr.find_all("td", recursive=False)
            if len(cells) < 2:
                continue

            person_link = cells[0].find("a", href=True)
            person_id, person_guid = extract_person_id_and_guid(person_link["href"] if person_link else None)
            votes.append(
                {
                    "person_name": normalize_space(cells[0].get_text(" ")) or None,
                    "person_id": person_id,
                    "person_guid": person_guid,
                    "vote": normalize_space(cells[1].get_text(" ")) or None,
                }
            )

    vote_counts = pd.Series([vote["vote"] for vote in votes], dtype="object").value_counts(dropna=False)
    affirmative_count = int(vote_counts.get("Affirmative", 0))
    negative_count = int(vote_counts.get("Negative", 0))
    abstain_count = int(vote_counts.get("Abstain", 0))
    excused_count = int(vote_counts.get("Excused", 0))
    non_voting_count = int(vote_counts.get("Non-voting", 0))
    named_vote_total = affirmative_count + negative_count + abstain_count + excused_count + non_voting_count
    other_vote_count = len(votes) - named_vote_total

    summary = {
        "action_detail_type": table_value(soup, "ctl00_ContentPlaceHolder1_tblType"),
        "action_detail_title": table_value(soup, "ctl00_ContentPlaceHolder1_tblTitle"),
        "action_detail_result": table_value(soup, "ctl00_ContentPlaceHolder1_tblResult"),
        "agenda_note": table_value(soup, "ctl00_ContentPlaceHolder1_tblAgendaNote"),
        "minutes_note": table_value(soup, "ctl00_ContentPlaceHolder1_tblMinutesNote"),
        "action_detail_action": table_value(soup, "ctl00_ContentPlaceHolder1_tblAction"),
        "action_detail_text": table_value(soup, "ctl00_ContentPlaceHolder1_tblActionText"),
        "vote_tab_label": vote_label,
        "vote_record_count": record_count,
        "parsed_vote_rows": len(votes),
        "affirmative_count": affirmative_count,
        "negative_count": negative_count,
        "abstain_count": abstain_count,
        "excused_count": excused_count,
        "non_voting_count": non_voting_count,
        "other_vote_count": other_vote_count,
        "vote_margin": f"{affirmative_count}-{negative_count}-{abstain_count}",
        "negative_members": "; ".join(
            vote["person_name"] for vote in votes if vote["vote"] == "Negative" and vote["person_name"]
        )
        or None,
        "abstain_members": "; ".join(
            vote["person_name"] for vote in votes if vote["vote"] == "Abstain" and vote["person_name"]
        )
        or None,
    }

    return summary, votes


queue = pd.read_csv("../input/member_deference_final_action_vote_queue.csv", dtype=str, keep_default_na=False)
target_queue = queue[queue["fetch_vote_detail_first_pass"].str.lower().eq("true")].copy()
target_queue = target_queue.sort_values(["query_year", "matter_file", "matter_id"]).reset_index(drop=True)

if target_queue.empty:
    raise RuntimeError("The first-pass non-approval action-detail queue is empty.")
if target_queue["matter_id"].duplicated().any():
    raise RuntimeError("The first-pass non-approval queue must be unique by matter_id.")
if target_queue["final_history_detail_url"].eq("").any():
    raise RuntimeError("Every first-pass non-approval queue row must have a final action-detail URL.")
if target_queue["final_history_detail_url"].duplicated().any():
    raise RuntimeError("The first-pass non-approval action-detail URLs must be unique.")

session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Safari/537.36",
        "Referer": "https://legistar.council.nyc.gov/Legislation.aspx",
    }
)

raw_dir = Path("../output/source_files/member_deference_nonapproval_action_pages")
action_rows = []
member_vote_rows = []
fetch_failures = []

for i, row in enumerate(target_queue.to_dict("records"), start=1):
    raw_path = raw_dir / f"{safe_stub(row['matter_file'])}_{row['matter_id']}.html"

    if not raw_path.exists() or raw_path.stat().st_size == 0:
        try:
            response = request_with_retries(session, row["final_history_detail_url"])
            save_text(raw_path, response.text)
            time.sleep(0.03)
        except requests.RequestException as exc:
            fetch_failures.append(
                {
                    "matter_id": row["matter_id"],
                    "matter_file": row["matter_file"],
                    "final_history_detail_url": row["final_history_detail_url"],
                    "fetch_error": str(exc),
                }
            )
            continue

    summary, votes = parse_action_detail(raw_path.read_text(encoding="utf-8"))
    action_rows.append(
        {
            "query_year": row["query_year"],
            "matter_id": row["matter_id"],
            "matter_file": row["matter_file"],
            "query_matter_type": row["query_matter_type"],
            "matter_status": row["matter_status"],
            "disposition_group": row["disposition_group"],
            "filed_age_group": row["filed_age_group"],
            "final_action_vote_fetch_tier": row["final_action_vote_fetch_tier"],
            "final_history_date": row["final_history_date"],
            "final_history_action_by": row["final_history_action_by"],
            "final_history_action": row["final_history_action"],
            "final_history_result": row["final_history_result"],
            "final_history_detail_url": row["final_history_detail_url"],
            "affected_council_districts": row["affected_council_districts"],
            "affected_district_source": row["affected_district_source"],
            "local_members_from_roster": row["local_members_from_roster"],
            "application_keys": row["application_keys"],
            "title": row["title"],
            "raw_path": str(raw_path),
            "file_size_bytes": raw_path.stat().st_size,
            "checksum_sha256": sha256(raw_path),
            **summary,
        }
    )

    for vote_sequence, vote in enumerate(votes, start=1):
        member_vote_rows.append(
            {
                "query_year": row["query_year"],
                "matter_id": row["matter_id"],
                "matter_file": row["matter_file"],
                "matter_status": row["matter_status"],
                "disposition_group": row["disposition_group"],
                "final_history_date": row["final_history_date"],
                "final_history_action": row["final_history_action"],
                "affected_council_districts": row["affected_council_districts"],
                "local_members_from_roster": row["local_members_from_roster"],
                "vote_sequence": vote_sequence,
                **vote,
            }
        )

    if i == 1 or i % 50 == 0 or i == len(target_queue):
        print(f"Processed first-pass non-approval action-detail page {i} of {len(target_queue)}", flush=True)

if not action_rows:
    raise RuntimeError("No first-pass non-approval action-detail pages were fetched or parsed.")

action_details = pd.DataFrame(action_rows)
member_votes = pd.DataFrame(
    member_vote_rows,
    columns=[
        "query_year",
        "matter_id",
        "matter_file",
        "matter_status",
        "disposition_group",
        "final_history_date",
        "final_history_action",
        "affected_council_districts",
        "local_members_from_roster",
        "vote_sequence",
        "person_name",
        "person_id",
        "person_guid",
        "vote",
    ],
)
fetch_failures_df = pd.DataFrame(
    fetch_failures,
    columns=["matter_id", "matter_file", "final_history_detail_url", "fetch_error"],
)

vote_count_check = action_details[
    ["matter_id", "matter_file", "vote_tab_label", "parsed_vote_rows", "vote_record_count"]
].merge(
    member_votes.groupby("matter_id", dropna=False)
    .agg(vote_rows=("vote", "size"))
    .reset_index(),
    on="matter_id",
    how="left",
    validate="one_to_one",
)
vote_count_check["vote_rows"] = vote_count_check["vote_rows"].fillna(0).astype(int)
vote_count_check["vote_record_count"] = pd.to_numeric(vote_count_check["vote_record_count"], errors="coerce")
vote_count_check["parsed_rows_match_summary"] = vote_count_check["vote_rows"] == vote_count_check["parsed_vote_rows"]
vote_count_check["parsed_rows_match_legistar_record_count"] = (
    vote_count_check["vote_record_count"].isna()
    | (vote_count_check["vote_rows"] == vote_count_check["vote_record_count"])
)
zero_vote_pages = action_details[action_details["parsed_vote_rows"].eq(0)].copy()

member_votes_for_join = member_votes.copy()
if member_votes_for_join.empty:
    member_votes_by_person = pd.DataFrame(
        columns=["matter_id", "local_member_key", "matched_vote_person_names", "local_member_final_action_votes"]
    )
else:
    member_votes_for_join["local_member_key"] = member_votes_for_join["person_name"].map(edge_name)
    member_votes_by_person = (
        member_votes_for_join.groupby(["matter_id", "local_member_key"], as_index=False)
        .agg(
            matched_vote_person_names=("person_name", collapse_values),
            local_member_final_action_votes=("vote", collapse_values),
        )
    )

if member_votes_by_person.duplicated(["matter_id", "local_member_key"]).any():
    raise RuntimeError("Non-approval member-vote rows must be unique by matter_id and normalized person key.")

local_member_rows = []
for row in action_details.to_dict("records"):
    for local_member_name in split_semicolon(row["local_members_from_roster"]):
        local_member_rows.append(
            {
                "query_year": row["query_year"],
                "matter_id": row["matter_id"],
                "matter_file": row["matter_file"],
                "matter_status": row["matter_status"],
                "disposition_group": row["disposition_group"],
                "final_history_date": row["final_history_date"],
                "final_history_action": row["final_history_action"],
                "affected_council_districts": row["affected_council_districts"],
                "local_members_from_roster": row["local_members_from_roster"],
                "local_member_name": local_member_name,
                "local_member_key": edge_name(local_member_name),
                "parsed_vote_rows": row["parsed_vote_rows"],
            }
        )

local_member_votes = pd.DataFrame(
    local_member_rows,
    columns=[
        "query_year",
        "matter_id",
        "matter_file",
        "matter_status",
        "disposition_group",
        "final_history_date",
        "final_history_action",
        "affected_council_districts",
        "local_members_from_roster",
        "local_member_name",
        "local_member_key",
        "parsed_vote_rows",
    ],
)
if not local_member_votes.empty:
    local_member_votes = local_member_votes.merge(
        member_votes_by_person,
        on=["matter_id", "local_member_key"],
        how="left",
        validate="many_to_one",
    )
else:
    local_member_votes["matched_vote_person_names"] = pd.Series(dtype="object")
    local_member_votes["local_member_final_action_votes"] = pd.Series(dtype="object")

local_member_votes["local_member_vote_found"] = local_member_votes["local_member_final_action_votes"].fillna("").ne("")


def local_member_vote_category(value: object) -> str:
    votes = set(split_semicolon(value))
    if not votes:
        return "missing_from_vote_rows"
    if any(vote in {"Negative", "Abstain"} for vote in votes):
        return "negative_or_abstain"
    if votes == {"Affirmative"}:
        return "affirmative"
    if votes.issubset({"Excused", "Non-voting", "Absent", "Maternity"}):
        return "excused_nonvoting_absent"
    return "mixed_or_other"


local_member_votes["local_member_final_action_vote_category"] = local_member_votes[
    "local_member_final_action_votes"
].map(local_member_vote_category)

local_member_base = action_details[
    [
        "query_year",
        "matter_id",
        "matter_file",
        "matter_status",
        "disposition_group",
        "final_history_date",
        "final_history_action",
        "affected_council_districts",
        "local_members_from_roster",
        "parsed_vote_rows",
    ]
].copy()
if local_member_votes.empty:
    local_member_matter = pd.DataFrame(columns=["matter_id"])
else:
    local_member_matter = (
        local_member_votes.groupby("matter_id", as_index=False)
        .agg(
            local_member_rows=("local_member_name", "size"),
            local_member_vote_rows_found=("local_member_vote_found", "sum"),
            matched_vote_person_names=("matched_vote_person_names", collapse_values),
            local_member_final_action_votes=("local_member_final_action_votes", collapse_values),
            local_member_final_action_vote_categories=("local_member_final_action_vote_category", collapse_values),
        )
    )

local_member_summary = local_member_base.merge(local_member_matter, on="matter_id", how="left", validate="one_to_one")
local_member_summary["local_member_rows"] = local_member_summary["local_member_rows"].fillna(0).astype(int)
local_member_summary["local_member_vote_rows_found"] = (
    local_member_summary["local_member_vote_rows_found"].fillna(0).astype(int)
)
for col in [
    "matched_vote_person_names",
    "local_member_final_action_votes",
    "local_member_final_action_vote_categories",
]:
    local_member_summary[col] = local_member_summary[col].fillna("")


def matter_vote_status(row: pd.Series) -> str:
    categories = split_semicolon(row["local_member_final_action_vote_categories"])
    if not split_semicolon(row["local_members_from_roster"]):
        return "no_local_member_from_roster"
    if int(row["parsed_vote_rows"]) == 0:
        return "zero_vote_page"
    if not categories or set(categories) == {"missing_from_vote_rows"}:
        return "local_member_missing_from_vote_rows"
    if "negative_or_abstain" in categories:
        return "local_member_negative_or_abstain"
    if set(categories) == {"affirmative"}:
        return "local_member_affirmative_only"
    if set(categories) == {"excused_nonvoting_absent"}:
        return "local_member_excused_nonvoting_absent_only"
    return "local_member_mixed_or_other"


local_member_summary["local_member_final_action_vote_status"] = local_member_summary.apply(matter_vote_status, axis=1)
local_member_vote_summary = (
    local_member_summary.groupby(["disposition_group", "local_member_final_action_vote_status"], as_index=False)
    .agg(
        matter_count=("matter_id", "size"),
        vote_bearing_matter_count=("parsed_vote_rows", lambda x: int((pd.to_numeric(x, errors="coerce") > 0).sum())),
        local_member_vote_rows_found=("local_member_vote_rows_found", "sum"),
    )
    .sort_values(["disposition_group", "local_member_final_action_vote_status"])
)

qc = pd.DataFrame(
    [
        {
            "check_name": "first_pass_queue_rows_present",
            "passed": len(target_queue) > 0,
            "detail": f"Found {len(target_queue)} first-pass non-approval final action-detail URLs.",
        },
        {
            "check_name": "first_pass_queue_unique_by_matter_id",
            "passed": not target_queue["matter_id"].duplicated().any(),
            "detail": "Each queued matter appears once before fetching.",
        },
        {
            "check_name": "first_pass_queue_unique_by_detail_url",
            "passed": not target_queue["final_history_detail_url"].duplicated().any(),
            "detail": "Each queued final action-detail URL appears once before fetching.",
        },
        {
            "check_name": "action_detail_pages_available",
            "passed": fetch_failures_df.empty and len(action_details) == len(target_queue),
            "detail": (
                f"Parsed {len(action_details)} pages for {len(target_queue)} queued matters; "
                f"{len(fetch_failures_df)} fetch failures."
            ),
        },
        {
            "check_name": "member_vote_rows_match_action_summaries",
            "passed": bool(vote_count_check["parsed_rows_match_summary"].all()),
            "detail": "Long member-vote rows reconcile to parsed_vote_rows on every parsed action-detail page.",
        },
        {
            "check_name": "member_vote_rows_match_legistar_record_counts_when_present",
            "passed": bool(vote_count_check["parsed_rows_match_legistar_record_count"].all()),
            "detail": "Long member-vote rows reconcile to Legistar vote-record counts when the count is displayed.",
        },
        {
            "check_name": "zero_vote_pages_counted",
            "passed": True,
            "detail": f"{len(zero_vote_pages)} parsed non-approval pages contain no individual member-vote rows.",
        },
        {
            "check_name": "nonapproval_member_vote_rows_counted",
            "passed": True,
            "detail": f"Parsed {len(member_votes)} individual member-vote rows from first-pass non-approval pages.",
        },
        {
            "check_name": "local_member_vote_keys_unique",
            "passed": not member_votes_by_person.duplicated(["matter_id", "local_member_key"]).any(),
            "detail": "Non-approval member votes are unique by matter_id and normalized person key before local-member matching.",
        },
        {
            "check_name": "local_member_vote_rows_counted",
            "passed": True,
            "detail": (
                f"Found {len(local_member_votes)} local-member rows across "
                f"{local_member_votes['matter_id'].nunique() if not local_member_votes.empty else 0} first-pass matters."
            ),
        },
    ]
)

action_details.to_csv("../output/member_deference_nonapproval_action_details.csv", index=False)
member_votes.to_csv("../output/member_deference_nonapproval_member_votes.csv", index=False)
qc.to_csv("../output/member_deference_nonapproval_action_vote_qc.csv", index=False)
zero_vote_pages.to_csv("../output/member_deference_nonapproval_zero_vote_pages.csv", index=False)
local_member_votes.to_csv("../output/member_deference_nonapproval_local_member_votes.csv", index=False)
local_member_summary.to_csv("../output/member_deference_nonapproval_local_member_matter_audit.csv", index=False)
local_member_vote_summary.to_csv("../output/member_deference_nonapproval_local_member_vote_summary.csv", index=False)
fetch_failures_df.to_csv("../output/member_deference_nonapproval_fetch_failures.csv", index=False)

if not qc["passed"].all():
    failed_checks = ", ".join(qc.loc[~qc["passed"], "check_name"].astype(str))
    raise RuntimeError(f"Non-approval action-vote fetch failed: {failed_checks}.")
