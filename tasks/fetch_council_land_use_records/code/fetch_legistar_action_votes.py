# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_council_land_use_records/code")

from __future__ import annotations

import hashlib
import re
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup


SOURCE_ID = "nyc_council_legistar_land_use_broad_recall"
BASE_URL = "https://legistar.council.nyc.gov/"
if len(sys.argv) != 2 or not re.fullmatch(r"\d{4}", sys.argv[1]):
    raise RuntimeError("Usage: python3 fetch_legistar_action_votes.py <year>")

QUERY_YEAR = sys.argv[1]
ACTION_DETAILS_OUTPUT = Path(f"../output/legistar_{QUERY_YEAR}_broad_recall_action_details.csv")
MEMBER_VOTES_OUTPUT = Path(f"../output/legistar_{QUERY_YEAR}_broad_recall_member_votes.csv")


def normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", "" if value is None else str(value)).strip()


def safe_stub(value: object) -> str:
    stub = re.sub(r"[^a-z0-9]+", "_", normalize_space(value).lower()).strip("_")
    return stub or "missing"


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


history_events = pd.read_csv(
    f"../output/legistar_{QUERY_YEAR}_broad_recall_history_events.csv",
    dtype=str,
    keep_default_na=False,
)
pull_dates = sorted(set(history_events["pull_date"]) - {""})
if len(pull_dates) != 1:
    raise RuntimeError(f"Expected exactly one pull_date in the {QUERY_YEAR} history-event table.")

approved_by_council = (
    history_events["history_action"]
    .str.replace(",", "", regex=False)
    .str.lower()
    .eq("approved by council")
)
target_events = history_events[
    (history_events["history_action_by"] == "City Council")
    & approved_by_council
    & (history_events["history_detail_url"] != "")
].copy()
target_events["history_date_parsed"] = pd.to_datetime(target_events["history_date"], errors="coerce")
target_events["history_sequence_int"] = pd.to_numeric(target_events["history_sequence"], errors="coerce")
duplicate_approval_event_count = int(target_events["matter_id"].duplicated(keep=False).sum())
duplicate_approval_matter_count = int(target_events.loc[target_events["matter_id"].duplicated(keep=False), "matter_id"].nunique())
duplicate_approval_rows_dropped = duplicate_approval_event_count - duplicate_approval_matter_count
target_events = (
    target_events.sort_values(["matter_id", "history_date_parsed", "history_sequence_int"])
    .drop_duplicates("matter_id", keep="last")
    .drop(columns=["history_date_parsed", "history_sequence_int"])
)

if target_events.empty:
    raise RuntimeError(f"No final Council approval action-detail URLs found for the {QUERY_YEAR} pull.")
if target_events["history_detail_url"].duplicated().any():
    raise RuntimeError("Final Council approval action-detail URLs are not unique.")
if target_events["matter_id"].duplicated().any():
    raise RuntimeError("Final Council approval action-detail events are not unique by matter_id.")

session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Safari/537.36",
        "Referer": "https://legistar.council.nyc.gov/Legislation.aspx",
    }
)

raw_dir = (
    Path("../output/source_files")
    / SOURCE_ID
    / pull_dates[0]
    / f"year_{QUERY_YEAR}"
    / "action_detail_pages"
)

action_rows = []
member_vote_rows = []
for i, row in enumerate(target_events.sort_values(["history_date", "matter_file"]).to_dict("records"), start=1):
    raw_path = raw_dir / f"{safe_stub(row['matter_file'])}_{row['matter_id']}.html"
    if raw_path.exists() and raw_path.stat().st_size > 0:
        page_html = raw_path.read_text(encoding="utf-8")
        fetch_status = "cached"
    else:
        response = request_with_retries(session, row["history_detail_url"])
        page_html = response.text
        save_text(raw_path, page_html)
        fetch_status = "downloaded"
    summary, votes = parse_action_detail(page_html)

    action_rows.append(
        {
            "source_id": row["source_id"],
            "pull_date": row["pull_date"],
            "query_year": row["query_year"],
            "query_matter_type": row["query_matter_type"],
            "matter_id": row["matter_id"],
            "matter_guid": row["matter_guid"],
            "matter_file": row["matter_file"],
            "matter_url": row["matter_url"],
            "history_sequence": row["history_sequence"],
            "history_date": row["history_date"],
            "history_action_by": row["history_action_by"],
            "history_action": row["history_action"],
            "history_result": row["history_result"],
            "history_detail_url": row["history_detail_url"],
            "fetch_status": fetch_status,
            "raw_path": str(raw_path),
            "file_size_bytes": raw_path.stat().st_size,
            "checksum_sha256": sha256(raw_path),
            **summary,
        }
    )

    for vote_sequence, vote in enumerate(votes, start=1):
        member_vote_rows.append(
            {
                "source_id": row["source_id"],
                "pull_date": row["pull_date"],
                "query_year": row["query_year"],
                "query_matter_type": row["query_matter_type"],
                "matter_id": row["matter_id"],
                "matter_guid": row["matter_guid"],
                "matter_file": row["matter_file"],
                "history_sequence": row["history_sequence"],
                "history_date": row["history_date"],
                "history_detail_url": row["history_detail_url"],
                "vote_sequence": vote_sequence,
                **vote,
            }
        )

    if fetch_status == "downloaded":
        time.sleep(0.03)
    if i == 1 or i % 100 == 0 or i == len(target_events):
        print(f"Fetched action-detail page {i} of {len(target_events)}", flush=True)

action_details = pd.DataFrame(action_rows)
member_votes = pd.DataFrame(member_vote_rows)
if action_details.empty:
    raise RuntimeError("No action detail pages were downloaded.")
if member_votes.empty:
    raise RuntimeError("No member votes were parsed from action detail pages.")

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
vote_count_check["parsed_rows_match_summary"] = vote_count_check["vote_rows"] == vote_count_check["parsed_vote_rows"]
vote_count_check["parsed_rows_match_legistar_record_count"] = (
    vote_count_check["vote_rows"] == vote_count_check["vote_record_count"]
)
zero_vote_pages = vote_count_check[vote_count_check["parsed_vote_rows"] == 0]
non_unanimous_vote_count = int(
    (
        (action_details["negative_count"] > 0)
        | (action_details["abstain_count"] > 0)
    ).sum()
)

qc_rows = [
    {
        "check_name": "approval_action_detail_pages_downloaded",
        "passed": len(action_details) == len(target_events),
        "detail": f"Downloaded {len(action_details)} final Council approval action-detail pages for {len(target_events)} target events.",
    },
    {
        "check_name": "approval_action_detail_urls_unique",
        "passed": not action_details["history_detail_url"].duplicated().any(),
        "detail": "Each downloaded action-detail page maps to one final Council approval event.",
    },
    {
        "check_name": "member_vote_rows_match_action_summaries",
        "passed": bool(vote_count_check["parsed_rows_match_summary"].all()),
        "detail": "Long member-vote rows reconcile to parsed_vote_rows on every action-detail page, including zero-row consent-vote pages.",
    },
    {
        "check_name": "member_vote_rows_match_legistar_record_counts",
        "passed": bool(vote_count_check["parsed_rows_match_legistar_record_count"].all()),
        "detail": "Long member-vote rows reconcile to Legistar's displayed vote-record count on every action-detail page, including zero-row consent-vote pages.",
    },
    {
        "check_name": "zero_vote_action_pages_are_consent_zero_zero",
        "passed": bool(
            zero_vote_pages.empty
            or (
                (zero_vote_pages["vote_record_count"] == 0)
                & zero_vote_pages["vote_tab_label"].fillna("").str.contains("\\(0:0\\)")
            ).all()
        ),
        "detail": f"{len(zero_vote_pages)} final approval action-detail pages show a (0:0) vote tab and no individual member-vote rows.",
    },
    {
        "check_name": "non_unanimous_vote_rows_counted",
        "passed": True,
        "detail": f"Found {non_unanimous_vote_count} approved {QUERY_YEAR} land-use matter rows with a negative or abstain member vote.",
    },
    {
        "check_name": "duplicate_approval_events_deduplicated",
        "passed": not action_details["matter_id"].duplicated().any(),
        "detail": (
            f"Dropped {duplicate_approval_rows_dropped} duplicate approval-event rows across "
            f"{duplicate_approval_matter_count} matter IDs before fetching action details."
        ),
    },
]

if QUERY_YEAR == "2001":
    laguardia_negative = member_votes[
        (member_votes["matter_file"] == "Res 1939-2001")
        & (member_votes["person_name"] == "Helen M. Marshall")
        & (member_votes["vote"] == "Negative")
    ]
    qc_rows.append(
        {
            "check_name": "laguardia_hotel_local_member_negative_vote_found",
            "passed": not laguardia_negative.empty,
            "detail": "Res 1939-2001 action detail records Helen M. Marshall voting Negative.",
        }
    )

qc = pd.DataFrame(qc_rows)

action_details.to_csv(ACTION_DETAILS_OUTPUT, index=False)
member_votes.to_csv(MEMBER_VOTES_OUTPUT, index=False)

if not qc["passed"].all():
    failed_checks = ", ".join(qc.loc[~qc["passed"], "check_name"].astype(str))
    raise RuntimeError(f"Legistar {QUERY_YEAR} action-vote fetch failed: {failed_checks}.")
