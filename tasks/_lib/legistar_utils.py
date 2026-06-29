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


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def bad_cached_html_paths(paths: list[Path]) -> list[Path]:
    bad_paths = []
    for path in paths:
        if not path.exists():
            continue
        stat_result = path.stat()
        if stat_result.st_size == 0 or getattr(stat_result, "st_blocks", 1) == 0:
            bad_paths.append(path)
    return bad_paths


def check_cached_html(paths: list[Path], label: str) -> None:
    bad_paths = bad_cached_html_paths(paths)
    if bad_paths:
        raise RuntimeError(
            f"{label} has {len(bad_paths)} cached HTML files that are empty or not materialized locally. "
            f"Hydrate or delete the cached file before rerunning. Example: {bad_paths[0]}"
        )


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
