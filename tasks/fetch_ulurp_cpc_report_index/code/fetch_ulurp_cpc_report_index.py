#!/usr/bin/env python3

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_ulurp_cpc_report_index/code")
# start_year = 1975
# end_year = 2025

from __future__ import annotations

import csv
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


REQUEST_ATTEMPTS = 4
REQUEST_TIMEOUT_SECONDS = 90


def request_page(session, index_url, method, data=None, require_grid=True):
    last_error = None
    for attempt in range(1, REQUEST_ATTEMPTS + 1):
        try:
            response = session.request(
                method,
                index_url,
                data=data,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            if require_grid and re.search(
                r'id="lbl_internal_error"[^>]*>.*?internal error',
                response.text,
                re.IGNORECASE | re.DOTALL,
            ):
                raise RuntimeError("CPC search returned its internal-error message.")
            if require_grid and "GridView1" not in response.text:
                raise RuntimeError("CPC search did not contain its report grid.")
            return response.text
        except (requests.RequestException, RuntimeError) as error:
            last_error = error
            if attempt < REQUEST_ATTEMPTS:
                time.sleep(2 * attempt)
    raise RuntimeError(f"Could not fetch CPC report index: {last_error}")


def hidden_fields(soup):
    return {
        field.get("name"): field.get("value", "")
        for field in soup.select("input[type=hidden][name]")
    }


def report_rows(soup, index_url, search_year, page_number):
    rows = []
    table = soup.select_one("#GridView1")
    if table is None:
        return rows

    for table_row in table.select("tr"):
        application_link = table_row.select_one("a[id^=GridView1_hyperlink1_][href]")
        project_link = table_row.select_one("a[id^=GridView1_hyperlink2_][href]")
        community_district_node = table_row.select_one("span[id^=GridView1_Label3_]")
        vote_date_node = table_row.select_one("span[id^=GridView1_Label4_]")
        if not all((application_link, project_link, community_district_node, vote_date_node)):
            continue

        application_number = " ".join(application_link.get_text(" ", strip=True).split())
        project_name_raw = " ".join(project_link.get_text(" ", strip=True).split())
        lead_report_flag = project_name_raw.endswith("*")
        project_name = project_name_raw[:-1].strip() if lead_report_flag else project_name_raw
        community_district = " ".join(community_district_node.get_text(" ", strip=True).split())
        vote_date = " ".join(vote_date_node.get_text(" ", strip=True).split())
        if not re.fullmatch(r"\d{2}/\d{2}/\d{4}", vote_date):
            raise RuntimeError(
                f"Unexpected CPC vote date {vote_date!r} for {application_number!r} "
                f"on search year {search_year} page {page_number}."
            )
        pdf_url = urljoin(index_url, application_link["href"]).replace("http://", "https://", 1)
        report_stem = Path(urlparse(pdf_url).path).stem

        rows.append(
            {
                "application_number": application_number,
                "project_name": project_name,
                "lead_report_flag": str(lead_report_flag).upper(),
                "community_district": community_district,
                "vote_date": vote_date,
                "vote_year": datetime.strptime(vote_date, "%m/%d/%Y").year,
                "pdf_url": pdf_url,
                "report_stem": report_stem,
                "search_year": search_year,
                "search_page": page_number,
            }
        )
    return rows


def available_page_numbers(soup):
    page_numbers = set()
    for link in soup.select("#GridView1 a[href]"):
        match = re.search(r"Page\$([0-9]+)", link.get("href", ""))
        if match:
            page_numbers.add(int(match.group(1)))
    return page_numbers


def write_csv(rows, path):
    fieldnames = [
        "application_number",
        "project_name",
        "lead_report_flag",
        "community_district",
        "vote_date",
        "vote_year",
        "pdf_url",
        "report_stem",
        "search_year",
        "search_page",
    ]
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main():
    if len(sys.argv) != 3:
        raise RuntimeError("Usage: python3 fetch_ulurp_cpc_report_index.py <start_year> <end_year>")

    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    if start_year > end_year:
        raise RuntimeError("start_year cannot exceed end_year.")

    with Path("../input/source_catalog.csv").open(newline="", encoding="utf-8") as input_file:
        source_rows = [
            row for row in csv.DictReader(input_file)
            if row["source_id"] == "dcp_cpc_reports"
        ]
    if len(source_rows) != 1:
        raise RuntimeError("Source catalog must contain exactly one dcp_cpc_reports row.")
    index_url = source_rows[0]["official_url"]

    session = requests.Session()
    session.headers.update({"User-Agent": "nyc-ulurp-corpus-research/0.2"})
    all_rows = []

    for search_year in range(start_year, end_year + 1):
        initial_html = request_page(session, index_url, "GET", require_grid=False)
        initial_soup = BeautifulSoup(initial_html, "html.parser")
        payload = hidden_fields(initial_soup)
        payload.update(
            {
                "sel_boro": "ALL",
                "sel_cd": "ALL",
                "ulurp": "",
                "keyword": "",
                "select_type": "ALL",
                "tb_start_date": f"01/01/{search_year}",
                "tb_end_date": f"12/31/{search_year}",
                "button_byAll": "Search Reports",
            }
        )
        search_html = request_page(session, index_url, "POST", payload)
        soup = BeautifulSoup(search_html, "html.parser")
        page_number = 1
        year_rows = report_rows(soup, index_url, search_year, page_number)
        if not year_rows:
            raise RuntimeError(f"CPC search returned no report rows for {search_year}.")

        while page_number + 1 in available_page_numbers(soup):
            page_number += 1
            page_payload = hidden_fields(soup)
            page_payload.update(
                {
                    "__EVENTTARGET": "GridView1",
                    "__EVENTARGUMENT": f"Page${page_number}",
                    "sel_boro": "ALL",
                    "sel_cd": "ALL",
                    "ulurp": "",
                    "keyword": "",
                    "select_type": "ALL",
                    "tb_start_date": f"01/01/{search_year}",
                    "tb_end_date": f"12/31/{search_year}",
                }
            )
            page_html = request_page(session, index_url, "POST", page_payload)
            soup = BeautifulSoup(page_html, "html.parser")
            page_rows = report_rows(soup, index_url, search_year, page_number)
            if not page_rows:
                raise RuntimeError(f"CPC pagination returned no rows for {search_year} page {page_number}.")
            year_rows.extend(page_rows)

        all_rows.extend(year_rows)
        print(
            f"Fetched official CPC index for {search_year}: {len(year_rows)} rows across {page_number} pages",
            flush=True,
        )

    row_keys = [
        (row["application_number"], row["vote_date"], row["pdf_url"])
        for row in all_rows
    ]
    if len(row_keys) != len(set(row_keys)):
        raise RuntimeError("CPC report index contains duplicate fetched rows.")
    output_rows = sorted(
        all_rows,
        key=lambda row: (row["vote_year"], row["vote_date"], row["application_number"], row["pdf_url"]),
    )
    write_csv(output_rows, Path("../output/official_cpc_report_index.csv"))
    print(f"Wrote {len(output_rows)} official CPC report index rows", flush=True)


if __name__ == "__main__":
    main()
