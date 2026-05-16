# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_council_land_use_records/code")

from __future__ import annotations

import hashlib
import html as html_module
import json
import re
import sys
import time
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup


BASE_URL = "https://legistar.council.nyc.gov/Legislation.aspx"
if len(sys.argv) != 2 or not re.fullmatch(r"\d{4}", sys.argv[1]):
    raise RuntimeError("Usage: python3 fetch_legistar_broad_recall.py <year>")

QUERY_YEAR = sys.argv[1]
SOURCE_ID = "nyc_council_legistar_land_use_broad_recall"
PULL_DATE = date.today().strftime("%Y%m%d")

MATTER_TYPE_QUERIES = [
    {"matter_type": "Land Use Application", "type_value": "10", "slug": "land_use_application"},
    {"matter_type": "Land Use Call-Up", "type_value": "13", "slug": "land_use_call_up"},
    {"matter_type": "Resolution", "type_value": "1", "slug": "resolution"},
]

LAND_USE_TEXT_RE = re.compile(
    r"("
    r"\bULURP\b|"
    r"uniform land use|"
    r"land use review|"
    r"city planning commission|"
    r"section 197-[cd]|"
    r"§\s*197-[cd]|"
    r"\bUDAAP\b|"
    r"urban development action area|"
    r"\bC\s*\d{6}\s*[A-Z]{2,4}\b|"
    r"\bN\s*\d{6}\s*[A-Z]{2,4}\b|"
    r"\bM\s*\d{6}"
    r")",
    re.IGNORECASE,
)

APPLICATION_RE = re.compile(
    r"\b(?:[CNM]\s*)?\d{6}\s*(?:\([A-Z0-9]+\)\s*)?[A-Z]{2,4}\b",
    re.IGNORECASE,
)


def normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", "" if value is None else str(value)).strip()


def parse_form_inputs(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    payload: dict[str, str] = {}

    for inp in soup.find_all("input"):
        name = inp.get("name")
        if not name:
            continue

        input_type = (inp.get("type") or "").lower()
        if input_type in {"submit", "button", "image"}:
            continue
        if input_type in {"checkbox", "radio"} and not inp.has_attr("checked"):
            continue

        payload[name] = inp.get("value") or ""

    return payload


def combo_client_state(value: str, text: str) -> str:
    return json.dumps(
        {
            "logEntries": [],
            "value": value,
            "text": text,
            "enabled": True,
            "checkedIndices": [],
            "checkedItemsTextOverflows": False,
        },
        separators=(",", ":"),
    )


def legislation_payload(html: str, matter_type: str, type_value: str, event_target: str) -> dict[str, str]:
    payload = parse_form_inputs(html)
    payload.update(
        {
            "__EVENTTARGET": event_target,
            "__EVENTARGUMENT": "",
            "ctl00$ContentPlaceHolder1$txtSearch": "",
            "ctl00$ContentPlaceHolder1$lstYears": QUERY_YEAR,
            "ctl00_ContentPlaceHolder1_lstYears_ClientState": combo_client_state(QUERY_YEAR, QUERY_YEAR),
            "ctl00$ContentPlaceHolder1$lstTypeBasic": matter_type,
            "ctl00_ContentPlaceHolder1_lstTypeBasic_ClientState": combo_client_state(type_value, matter_type),
            "ctl00$ContentPlaceHolder1$chkID": "on",
            "ctl00$ContentPlaceHolder1$chkText": "on",
        }
    )
    return payload


def parse_record_count(html: str) -> int | None:
    soup = BeautifulSoup(html, "html.parser")
    for span in soup.select("span.rmText"):
        text = normalize_space(span.get_text(" "))
        match = re.match(r"^([0-9,]+) records$", text)
        if match:
            return int(match.group(1).replace(",", ""))
    return None


def parse_page_info(html: str) -> dict[str, int | None]:
    soup = BeautifulSoup(html, "html.parser")
    match = None
    for div in soup.select("div.rgInfoPart"):
        text = normalize_space(div.get_text(" "))
        match = re.search(
            r"Page\s+(\d+)\s+of\s+(\d+)\s*,\s*items\s+(\d+)\s+to\s+(\d+)\s+of\s+(\d+)\s*\.?",
            text,
        )
        if match:
            break

    if not match:
        record_count = parse_record_count(html)
        return {
            "current_page": 1 if record_count else None,
            "page_count": 1 if record_count else None,
            "page_first_item": 1 if record_count else None,
            "page_last_item": record_count,
            "record_count": record_count,
        }

    return {
        "current_page": int(match.group(1)),
        "page_count": int(match.group(2)),
        "page_first_item": int(match.group(3)),
        "page_last_item": int(match.group(4)),
        "record_count": int(match.group(5)),
    }


def parse_page_links(html: str) -> dict[int, str]:
    soup = BeautifulSoup(html, "html.parser")
    links: dict[int, str] = {}

    for link in soup.select("td.rgPagerCell a"):
        page_text = normalize_space(link.get_text(" "))
        if not page_text.isdigit():
            continue

        href = link.get("href") or ""
        match = re.search(r"__doPostBack\('([^']+)'", href)
        if match:
            links[int(page_text)] = match.group(1)

    return links


def extract_matter_id_and_guid(href: str) -> tuple[str | None, str | None]:
    parsed = urlparse(href.replace("&amp;", "&"))
    query = parse_qs(parsed.query)
    matter_id = query.get("ID", [None])[0]
    matter_guid = query.get("GUID", [None])[0]
    return matter_id, matter_guid


def extract_borough(title: str) -> str | None:
    match = re.search(r"Borough of ([A-Za-z ]+?)(?:,|\.| in |$)", title, flags=re.IGNORECASE)
    if not match:
        return None
    return normalize_space(match.group(1)).title()


def extract_council_districts(title: str) -> str | None:
    match = re.search(
        r"Council District(?:s| Nos?\.?| no\.?)?\s*([0-9,\sand-]+)",
        title,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    districts = re.findall(r"\d{1,2}", match.group(1))
    if not districts:
        return None

    return "; ".join(dict.fromkeys(districts))


def extract_application_numbers(title: str) -> str | None:
    matches = [normalize_space(match.group(0)).upper() for match in APPLICATION_RE.finditer(title)]
    if not matches:
        return None
    return "; ".join(dict.fromkeys(matches))


def parse_grid_rows(html: str, query: dict[str, str], page_info: dict[str, int | None]) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, object]] = []

    for tr in soup.select("tr.rgRow, tr.rgAltRow"):
        cells = tr.find_all("td", recursive=False)
        if len(cells) < 8:
            continue

        link = cells[0].find("a", href=True)
        if link is None:
            continue

        matter_href = link["href"].replace("&amp;", "&")
        matter_id, matter_guid = extract_matter_id_and_guid(matter_href)
        matter_file = normalize_space(link.get_text(" "))
        matter_file_year_match = re.search(r"-(\d{4})$", matter_file)
        title = normalize_space(cells[7].get_text(" "))
        matter_type = normalize_space(cells[2].get_text(" "))
        committee = normalize_space(cells[4].get_text(" "))
        title_land_use_flag = bool(LAND_USE_TEXT_RE.search(title))
        committee_land_use_flag = "land use" in committee.lower()
        land_use_recall_flag = (
            matter_type in {"Land Use Application", "Land Use Call-Up"}
            or committee_land_use_flag
            or title_land_use_flag
        )

        if matter_type in {"Land Use Application", "Land Use Call-Up"}:
            land_use_recall_reason = "official_land_use_matter_type"
        elif committee_land_use_flag:
            land_use_recall_reason = "land_use_committee_resolution"
        elif title_land_use_flag:
            land_use_recall_reason = "land_use_text_resolution"
        else:
            land_use_recall_reason = None

        rows.append(
            {
                "source_id": SOURCE_ID,
                "pull_date": PULL_DATE,
                "query_year": QUERY_YEAR,
                "query_matter_type": query["matter_type"],
                "query_matter_type_value": query["type_value"],
                "query_page": page_info["current_page"],
                "query_record_count": page_info["record_count"],
                "query_page_count": page_info["page_count"],
                "matter_id": matter_id,
                "matter_guid": matter_guid,
                "matter_file": matter_file,
                "matter_file_year": matter_file_year_match.group(1) if matter_file_year_match else None,
                "matter_url": urljoin(BASE_URL, matter_href),
                "law_number": normalize_space(cells[1].get_text(" ")) or None,
                "matter_type": matter_type,
                "status": normalize_space(cells[3].get_text(" ")) or None,
                "committee": committee or None,
                "prime_sponsor": normalize_space(cells[5].get_text(" ")) or None,
                "council_member_sponsors": normalize_space(cells[6].get_text(" ")) or None,
                "title": title,
                "borough": extract_borough(title),
                "affected_council_districts": extract_council_districts(title),
                "application_numbers_in_title": extract_application_numbers(title),
                "ulurp_text_flag": title_land_use_flag,
                "land_use_recall_flag": land_use_recall_flag,
                "land_use_recall_reason": land_use_recall_reason,
                "laguardia_hotel_seed_flag": bool(re.search(r"\bM\s*820995\b", title, flags=re.IGNORECASE)),
            }
        )

    return rows


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def request_with_retries(session: requests.Session, method: str, url: str, **kwargs) -> requests.Response:
    last_error = None
    for attempt in range(1, 4):
        try:
            response = session.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as error:
            last_error = error
            if attempt == 3:
                break
            time.sleep(5 * attempt)
    raise last_error


def safe_stub(value: object) -> str:
    stub = re.sub(r"[^a-z0-9]+", "_", normalize_space(value).lower()).strip("_")
    return stub or "missing"


def fetch_search_pages(session: requests.Session, query: dict[str, str]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    raw_dir = Path("../output/source_files") / SOURCE_ID / PULL_DATE / f"year_{QUERY_YEAR}" / query["slug"] / "index_pages"
    response = request_with_retries(session, "GET", BASE_URL, timeout=90)

    response = request_with_retries(
        session,
        "POST",
        BASE_URL,
        data=legislation_payload(
            response.text,
            query["matter_type"],
            query["type_value"],
            "ctl00$ContentPlaceHolder1$btnSearch",
        ),
        timeout=90,
    )

    page_fetch_rows: list[dict[str, object]] = []
    matter_rows: list[dict[str, object]] = []
    current_html = response.text
    page_info = parse_page_info(current_html)
    page_count = page_info["page_count"] or 0
    page_links = parse_page_links(current_html)

    for page_number in range(1, page_count + 1):
        if page_number > 1:
            if page_number not in page_links:
                raise RuntimeError(f"Missing pager link for {query['matter_type']} page {page_number}.")

            response = request_with_retries(
                session,
                "POST",
                BASE_URL,
                data=legislation_payload(
                    current_html,
                    query["matter_type"],
                    query["type_value"],
                    page_links[page_number],
                ),
                timeout=90,
            )
            current_html = response.text
            page_info = parse_page_info(current_html)
            page_links.update(parse_page_links(current_html))

        raw_path = raw_dir / f"page_{page_number:03d}.html"
        save_text(raw_path, current_html)
        parsed_rows = parse_grid_rows(current_html, query, page_info)
        matter_rows.extend(parsed_rows)
        page_fetch_rows.append(
            {
                "source_id": SOURCE_ID,
                "pull_date": PULL_DATE,
                "query_year": QUERY_YEAR,
                "query_matter_type": query["matter_type"],
                "query_matter_type_value": query["type_value"],
                "query_page": page_number,
                "reported_current_page": page_info["current_page"],
                "reported_page_count": page_info["page_count"],
                "reported_record_count": page_info["record_count"],
                "parsed_rows": len(parsed_rows),
                "raw_path": str(raw_path),
                "file_size_bytes": raw_path.stat().st_size,
                "checksum_sha256": sha256(raw_path),
            }
        )
        print(
            f"Fetched {query['matter_type']} page {page_number} of {page_count}: "
            f"{len(parsed_rows)} rows",
            flush=True,
        )

    return matter_rows, page_fetch_rows


def parse_detail_summary(html: str, history_events: list[dict[str, object]]) -> dict[str, object]:
    soup = BeautifulSoup(html, "html.parser")
    description = soup.find("meta", attrs={"name": "description"})
    detail_title = None
    if description and description.get("content"):
        detail_title = re.sub(r"^Title:\s*", "", normalize_space(description["content"]))

    attachment_links = {
        urljoin(BASE_URL, link["href"].replace("&amp;", "&"))
        for link in soup.find_all("a", href=True)
        if "View.ashx" in link["href"]
    }
    report_links = {
        urljoin(BASE_URL, link["href"].replace("&amp;", "&"))
        for link in soup.find_all("a", href=True)
        if "ViewReport.ashx" in link["href"]
    }
    history_links = sorted(
        {
            event["history_detail_url"]
            for event in history_events
            if event["history_detail_url"]
        }
    )
    meeting_links = sorted(
        {
            event["meeting_detail_url"]
            for event in history_events
            if event["meeting_detail_url"]
        }
    )

    return {
        "detail_title": detail_title,
        "detail_attachment_count": len(attachment_links),
        "detail_report_count": len(report_links),
        "detail_history_count": len(history_events),
        "detail_history_detail_url_count": len(history_links),
        "detail_meeting_detail_url_count": len(meeting_links),
        "detail_attachment_urls": "; ".join(sorted(attachment_links)) or None,
        "detail_report_urls": "; ".join(sorted(report_links)) or None,
        "detail_history_detail_urls": "; ".join(history_links) or None,
        "detail_meeting_detail_urls": "; ".join(meeting_links) or None,
    }


def extract_embedded_url(value: str | None, page_name: str) -> str | None:
    if not value:
        return None

    value = html_module.unescape(value)
    match = re.search(rf"{re.escape(page_name)}\.aspx\?[^'\"\s)]+", value)
    if not match:
        return None

    return urljoin(BASE_URL, match.group(0).replace("&amp;", "&"))


def extract_link_url(cell, page_name: str) -> str | None:
    for link in cell.find_all("a"):
        href_url = extract_embedded_url(link.get("href"), page_name)
        if href_url:
            return href_url

        onclick_url = extract_embedded_url(link.get("onclick"), page_name)
        if onclick_url:
            return onclick_url

    return None


def parse_history_events(html: str) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    history_table = soup.find("table", id=re.compile(r"gridLegislation_ctl00$"))
    if history_table is None:
        return []

    history_rows: list[dict[str, object]] = []
    for history_index, tr in enumerate(history_table.select("tr.rgRow, tr.rgAltRow"), start=1):
        cells = tr.find_all("td", recursive=False)
        if len(cells) < 8:
            continue

        history_rows.append(
            {
                "history_sequence": history_index,
                "history_date": normalize_space(cells[0].get_text(" ")) or None,
                "history_version": normalize_space(cells[1].get_text(" ")) or None,
                "history_prime_sponsor": normalize_space(cells[2].get_text(" ")) or None,
                "history_action_by": normalize_space(cells[3].get_text(" ")) or None,
                "history_action": normalize_space(cells[4].get_text(" ")) or None,
                "history_result": normalize_space(cells[5].get_text(" ")) or None,
                "history_detail_url": extract_link_url(cells[6], "HistoryDetail"),
                "meeting_detail_url": extract_link_url(cells[7], "MeetingDetail"),
            }
        )

    return history_rows


def fetch_detail_pages(session: requests.Session, matter_index: pd.DataFrame) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    detail_rows: list[dict[str, object]] = []
    history_rows: list[dict[str, object]] = []
    raw_dir = Path("../output/source_files") / SOURCE_ID / PULL_DATE / f"year_{QUERY_YEAR}" / "detail_pages"
    detail_targets = matter_index[matter_index["land_use_recall_flag"]].copy()

    sorted_targets = detail_targets.sort_values(["query_matter_type", "matter_file", "matter_id"]).to_dict("records")
    for i, row in enumerate(sorted_targets, start=1):
        raw_path = raw_dir / safe_stub(row["query_matter_type"]) / f"{safe_stub(row['matter_file'])}_{row['matter_id']}.html"
        if raw_path.exists() and raw_path.stat().st_size > 0:
            page_html = raw_path.read_text(encoding="utf-8")
            fetch_status = "cached"
        else:
            response = request_with_retries(session, "GET", row["matter_url"], timeout=90)
            page_html = response.text
            save_text(raw_path, page_html)
            fetch_status = "downloaded"
        detail_history = parse_history_events(page_html)
        summary = parse_detail_summary(page_html, detail_history)

        detail_rows.append(
            {
                "source_id": SOURCE_ID,
                "pull_date": PULL_DATE,
                "query_year": QUERY_YEAR,
                "query_matter_type": row["query_matter_type"],
                "matter_id": row["matter_id"],
                "matter_guid": row["matter_guid"],
                "matter_file": row["matter_file"],
                "matter_url": row["matter_url"],
                "fetch_status": fetch_status,
                "raw_path": str(raw_path),
                "file_size_bytes": raw_path.stat().st_size,
                "checksum_sha256": sha256(raw_path),
                **summary,
            }
        )
        for event in detail_history:
            history_rows.append(
                {
                    "source_id": SOURCE_ID,
                    "pull_date": PULL_DATE,
                    "query_year": QUERY_YEAR,
                    "query_matter_type": row["query_matter_type"],
                    "matter_id": row["matter_id"],
                    "matter_guid": row["matter_guid"],
                    "matter_file": row["matter_file"],
                    "matter_url": row["matter_url"],
                    "detail_raw_path": str(raw_path),
                    **event,
                }
            )
        if fetch_status == "downloaded":
            time.sleep(0.03)
        if i == 1 or i % 100 == 0 or i == len(sorted_targets):
            print(f"Fetched detail page {i} of {len(sorted_targets)}", flush=True)

    return detail_rows, history_rows


def main() -> None:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Safari/537.36",
            "Referer": BASE_URL,
        }
    )

    all_matter_rows: list[dict[str, object]] = []
    all_page_fetch_rows: list[dict[str, object]] = []

    for query in MATTER_TYPE_QUERIES:
        matter_rows, page_fetch_rows = fetch_search_pages(session, query)
        all_matter_rows.extend(matter_rows)
        all_page_fetch_rows.extend(page_fetch_rows)

    matter_index = pd.DataFrame(all_matter_rows)
    page_fetch = pd.DataFrame(all_page_fetch_rows)

    if matter_index.empty:
        raise RuntimeError("No Legistar matter rows were parsed.")

    detail_rows, history_rows = fetch_detail_pages(session, matter_index)
    detail_files = pd.DataFrame(detail_rows)
    history_events = pd.DataFrame(history_rows)
    if detail_files.empty:
        raise RuntimeError("No Legistar detail pages were downloaded.")
    if history_events.empty:
        raise RuntimeError("No Legistar history events were parsed from detail pages.")

    matter_index = matter_index.merge(
        detail_files[
            [
                "matter_id",
                "raw_path",
                "detail_title",
                "detail_attachment_count",
                "detail_report_count",
                "detail_history_count",
                "detail_history_detail_url_count",
                "detail_meeting_detail_url_count",
                "detail_attachment_urls",
                "detail_report_urls",
                "detail_history_detail_urls",
                "detail_meeting_detail_urls",
            ]
        ].rename(columns={"raw_path": "detail_raw_path"}),
        on="matter_id",
        how="left",
        validate="one_to_one",
    )

    count_check = (
        matter_index.groupby("query_matter_type", dropna=False)
        .agg(parsed_rows=("matter_id", "size"), reported_records=("query_record_count", "max"))
        .reset_index()
    )
    count_check["matches_reported_records"] = count_check["parsed_rows"] == count_check["reported_records"]

    recall_rows = matter_index[matter_index["land_use_recall_flag"]]
    detail_ids = set(detail_files["matter_id"].astype(str))
    recall_ids = set(recall_rows["matter_id"].astype(str))

    qc_rows = [
        {
            "check_name": "all_query_counts_reconciled",
            "passed": bool(count_check["matches_reported_records"].all()),
            "detail": "; ".join(
                f"{row.query_matter_type}: parsed {row.parsed_rows} of reported {row.reported_records}"
                for row in count_check.itertuples()
            ),
        },
        {
            "check_name": "unique_matter_ids_in_index",
            "passed": not matter_index["matter_id"].duplicated().any(),
            "detail": f"Each parsed {QUERY_YEAR} Legistar matter ID appears once in the combined index.",
        },
        {
            "check_name": "land_use_application_query_returns_records",
            "passed": bool(
                count_check.loc[
                    count_check["query_matter_type"] == "Land Use Application",
                    "parsed_rows",
                ].gt(0).any()
            ),
            "detail": f"The {QUERY_YEAR} Land Use Application query returns at least one record.",
        },
        {
            "check_name": "land_use_call_up_query_returns_records",
            "passed": bool(
                count_check.loc[
                    count_check["query_matter_type"] == "Land Use Call-Up",
                    "parsed_rows",
                ].gt(0).any()
            ),
            "detail": f"The {QUERY_YEAR} Land Use Call-Up query returns at least one record.",
        },
        {
            "check_name": "resolution_query_returns_records",
            "passed": bool(
                count_check.loc[
                    count_check["query_matter_type"] == "Resolution",
                    "parsed_rows",
                ].gt(0).any()
            ),
            "detail": f"The {QUERY_YEAR} Resolution query is included so land-use resolutions can be linked to LU matters.",
        },
        {
            "check_name": "detail_pages_downloaded_for_recall_rows",
            "passed": recall_ids.issubset(detail_ids),
            "detail": f"Downloaded detail pages for {len(detail_ids)} of {len(recall_ids)} recall rows.",
        },
        {
            "check_name": "resolution_land_use_candidates_present",
            "passed": bool(
                (
                    (matter_index["query_matter_type"] == "Resolution")
                    & matter_index["land_use_recall_flag"]
                ).any()
            ),
            "detail": f"At least one {QUERY_YEAR} resolution is flagged by committee or title as land-use-relevant.",
        },
        {
            "check_name": "history_events_parsed_for_details",
            "passed": bool(len(history_events) >= len(detail_files)),
            "detail": f"Parsed {len(history_events)} history rows from {len(detail_files)} downloaded detail pages.",
        },
    ]

    if QUERY_YEAR == "2001":
        qc_rows.extend(
            [
                {
                    "check_name": "laguardia_hotel_seed_found",
                    "passed": bool(matter_index["laguardia_hotel_seed_flag"].any()),
                    "detail": "Known Charter seed case M 820995 appears in the 2001 Land Use Application recall universe.",
                },
                {
                    "check_name": "laguardia_resolution_history_present",
                    "passed": bool(
                        (
                            (history_events["matter_file"] == "Res 1939-2001")
                            & (history_events["history_action_by"] == "City Council")
                        ).any()
                    ),
                    "detail": "The known LaGuardia hotel resolution has a City Council history event.",
                },
            ]
        )

    qc = pd.DataFrame(qc_rows)

    page_fetch.to_csv(f"../output/legistar_{QUERY_YEAR}_broad_recall_page_fetches.csv", index=False)
    matter_index.to_csv(f"../output/legistar_{QUERY_YEAR}_broad_recall_matter_index.csv", index=False)
    detail_files.to_csv(f"../output/legistar_{QUERY_YEAR}_broad_recall_detail_files.csv", index=False)
    history_events.to_csv(f"../output/legistar_{QUERY_YEAR}_broad_recall_history_events.csv", index=False)
    count_check.to_csv(f"../output/legistar_{QUERY_YEAR}_broad_recall_count_check.csv", index=False)
    qc.to_csv(f"../output/legistar_{QUERY_YEAR}_broad_recall_qc.csv", index=False)

    if not qc["passed"].all():
        raise RuntimeError(f"Legistar {QUERY_YEAR} broad-recall pull failed QC.")


if __name__ == "__main__":
    main()
