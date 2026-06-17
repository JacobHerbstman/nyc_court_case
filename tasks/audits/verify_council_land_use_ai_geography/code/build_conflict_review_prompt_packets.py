#!/usr/bin/env python3

import csv
import io
import re
import subprocess
import tempfile
from html import unescape
from html.parser import HTMLParser
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen


# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/verify_council_land_use_ai_geography/code")


class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.skip_depth = 0
        self.parts = []

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag in {"script", "style", "noscript"}:
            self.skip_depth += 1
        if self.skip_depth == 0 and tag in {"p", "div", "br", "tr", "td", "th", "li", "h1", "h2", "h3", "h4"}:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in {"script", "style", "noscript"} and self.skip_depth > 0:
            self.skip_depth -= 1
        if self.skip_depth == 0 and tag in {"p", "div", "tr", "li", "h1", "h2", "h3", "h4"}:
            self.parts.append("\n")

    def handle_data(self, data):
        if self.skip_depth == 0:
            value = data.strip()
            if value:
                self.parts.append(value + " ")

    def text(self):
        value = unescape("".join(self.parts))
        value = re.sub(r"[ \t\r\f\v]+", " ", value)
        value = re.sub(r"\n\s*\n+", "\n\n", value)
        return value.strip()


CASES = [
    {
        "signature_review_id": "clu_geo_010",
        "project": "Columbia Manhattanville / Special Manhattanville Mixed Use District",
        "known_conflict": "Prior AI accepted district 7; deterministic BBL verifier found district 3 from BBL 1011100001.",
        "question": "Which Council district(s) should be assigned to this 2007 land-use action for local-member deference analysis?",
        "search_queries": [
            '"Columbia Manhattanville" "Council District 7"',
            '"Special Manhattanville Mixed Use District" "Council District"',
            '"C 070495 ZMM" "Council District"',
            '"Columbia Manhattanville" "Robert Jackson" "City Council"',
            '"Manhattanville" "West 125th" "West 135th" "Council District 7"',
        ],
        "index_terms": [
            "C 070495 ZMM",
            "N 070496 ZRM",
            "Res 1201",
            "Res 1202",
            "Special Manhattanville",
            "Community District 9",
            "Council District 7",
            "District 7",
            "Robert Jackson",
            "West 125th",
            "West 135th",
            "Broadway",
            "Twelfth Avenue",
            "Hudson River",
        ],
        "sources": [
            ("LU 0634-2007, C 070495 ZMM", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=447818&GUID=BEEFF9B9-CA31-49E5-B4FD-62DFD84E820E&Options=ID|Text|&Search="),
            ("LU 0635-2007, N 070496 ZRM", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=447819&GUID=D30D3FF0-8525-4C80-97F0-18B9A6B6DEFD&Options=ID|Text|&Search="),
            ("Res 1201-2007", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=447870&GUID=F2157B9D-A9BD-44D9-AD49-02C155635F2B&Options=ID|Text|&Search="),
            ("Res 1202-2007", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=447871&GUID=C8F31B4C-C79D-48E7-AE2D-DD7ADE9009EA&Options=ID|Text|&Search="),
            ("DASNY SEQR Findings Statement, Columbia University Manhattanville Project", "pdf", "https://www.dasny.org/sites/default/files/inline-files/Columbia_U_Manhattanville_Type_I_3_09_2015.pdf"),
        ],
    },
    {
        "signature_review_id": "clu_geo_119",
        "project": "Grace Asphalt Plant / 130-31 Northern Boulevard",
        "known_conflict": "Prior AI accepted district 20; deterministic verifier found district 21 using Queens BBL 4017910052.",
        "question": "Which Council district(s) should be assigned to this 2009 land-use action?",
        "search_queries": [
            '"Grace Asphalt" "130-31 Northern Boulevard"',
            '"130-31 Northern Boulevard" "Council District"',
            '"C 090366 PCQ" "Council District"',
            '"Block 1791" "Lots 52 and 68" "Northern Boulevard"',
            '"130-31 Northern Boulevard" "asphalt plant" "City Council"',
        ],
        "index_terms": [
            "C 090366 PCQ",
            "C090366PCQ",
            "LU 1085",
            "Res 2043",
            "130-31 Northern Boulevard",
            "Block 1791",
            "Lots 52 and 68",
            "Lot 52",
            "Lot 68",
            "Council District 21",
            "District 21",
            "Community District 7",
            "Queens",
        ],
        "sources": [
            ("LU 1085-2009", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=449417&GUID=D316F464-DAA4-4B7C-BA39-387ECEFEF1CE&Options=ID|Text|&Search="),
            ("Res 2043-2009", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=449661&GUID=F5E3A2AC-829D-4969-B926-628CF792DFF8&Options=ID|Text|&Search="),
        ],
    },
    {
        "signature_review_id": "clu_geo_122",
        "project": "Bronx intermediate school site at 1065 Dr. Martin Luther King Jr. Boulevard",
        "known_conflict": "Prior AI accepted district 16; deterministic verifier found district 17 from Block 2527 / Lot 32.",
        "question": "Which Council district(s) should be assigned to this 2010 land-use action?",
        "search_queries": [
            '"1065 Dr. Martin Luther King" "Council District 17"',
            '"I.S. 285" "Council District 17"',
            '"Block 2527" "Lot 32" "Council District No. 17"',
            '"20105366 SCX" "Council District"',
            '"1065 Dr. Martin Luther King Jr. Boulevard" "City Council"',
        ],
        "index_terms": [
            "20105366 SCX",
            "Res 0355",
            "LU 127",
            "I.S. 285",
            "1065 Dr. Martin Luther King",
            "Block 2527",
            "Lot 32",
            "Part of Lot 32",
            "Council District No. 17",
            "Council District 17",
            "District No. 17",
            "Community Board No. 4",
        ],
        "sources": [
            ("Res 0355-2010", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=679896&GUID=551A9FEF-B99E-42A2-A65F-F36A7C8018EA&Options=ID|Text|&Search="),
            ("City Record Supplement, Stated Meeting June 29, 2010", "pdf", "https://www.nyc.gov/assets/dcas/downloads/pdf/cityrecord/stated-meetings/2010/cityrecord-supplement-11-15-2010.pdf"),
        ],
    },
    {
        "signature_review_id": "clu_geo_124",
        "project": "9, 11-17 Second Avenue property acquisition",
        "known_conflict": "Prior AI accepted district 2; deterministic verifier found district 1 from BBL evidence.",
        "question": "Which Council district(s) should be assigned to this 2011 land-use action?",
        "search_queries": [
            '"9, 11-17 Second Avenue" "Council District no. 1"',
            '"C 110141 PQM" "Council District no. 1"',
            '"Block 456" "Lots 27 and 28" "Second Avenue"',
            '"11-17 Second Avenue" "City Council"',
            '"9-17 Second Avenue" "Council District"',
        ],
        "index_terms": [
            "C 110141 PQM",
            "C110141PQM",
            "LU 312",
            "Res 0780",
            "9, 11-17 Second Avenue",
            "9-17 Second Avenue",
            "11-17 Second Avenue",
            "Block 456",
            "Lots 27 and 28",
            "Council District no. 1",
            "Council District No. 1",
            "District no. 1",
            "Community District 3",
        ],
        "sources": [
            ("Res 0780-2011", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=862241&GUID=3C0B1DAC-8A75-417B-87C4-C1E340A15824&Options=ID|Text|&Search="),
            ("City Record Supplement, Stated Meeting April 6, 2011", "pdf", "https://www.nyc.gov/assets/dcas/downloads/pdf/cityrecord/stated-meetings/2011/cityrecord-supplement-06-30-2011.pdf"),
        ],
    },
    {
        "signature_review_id": "clu_geo_142",
        "project": "Bundled 2019 call-ups: Belmont Cove, 59 Greenwich, 51-53 White Street",
        "known_conflict": "Prior AI accepted districts 1; 3; 15; deterministic verifier found 3; 17 but missed one component. Prior conflict review suggested 1; 3; 17.",
        "question": "Which Council district(s) should be assigned to this bundled 2019 roll-call signature?",
        "search_queries": [
            '"Belmont Cove Rezoning" "Council District 17"',
            '"59 Greenwich Avenue" "Council District 3"',
            '"51-53 White Street" "Council District 1"',
            '"C 190070 ZSM" "Council District 3"',
            '"C 180439 ZSM" "Council District 1"',
        ],
        "index_terms": [
            "Belmont Cove",
            "59 Greenwich",
            "51-53 White Street",
            "C 190049 ZMX",
            "N 190050 ZRX",
            "C 190051 PPX",
            "C 190070 ZSM",
            "C 180439 ZSM",
            "Council District 17",
            "Council District 3",
            "Council District 1",
            "Block 2945",
            "Block 613",
            "Block 175",
        ],
        "sources": [
            ("M 0124-2019 Belmont Cove call-up", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=3830490&GUID=3C923613-11AF-4AA6-879C-121F15849160&Options=ID|Text|&Search="),
            ("LU 0314-2019 Belmont Cove ZMX", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=3829595&GUID=3AE51FEB-31CA-4D85-B38A-34912B58F150&Options=ID|Text|&Search="),
            ("LU 0315-2019 Belmont Cove ZRX", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=3829596&GUID=24926D28-3D3B-4FA3-BE66-C151D61C2CB6&Options=ID|Text|&Search="),
            ("LU 0316-2019 Belmont Cove PPX", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=3829597&GUID=8D603EE6-693C-427F-976B-9805271F4536&Options=ID|Text|&Search="),
            ("M 0125-2019 59 Greenwich call-up", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=3830492&GUID=8AA1A3A5-4E6A-4EFA-8204-6569456A7CF8&Options=ID|Text|&Search="),
            ("LU 0323-2019 59 Greenwich", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=3829621&GUID=23EAA059-F77E-4C7A-A159-71E5968B9899&Options=ID|Text|&Search="),
            ("M 0126-2019 51-53 White Street call-up", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=3830491&GUID=E5770956-7A83-41BF-AFD4-2410326940F9&Options=ID|Text|&Search="),
            ("LU 0322-2019 51-53 White Street", "legistar", "https://legistar.council.nyc.gov/LegislationDetail.aspx?ID=3829620&GUID=5D046B91-4DB9-4FEF-8DD9-310B1AD49F57&Options=ID|Text|&Search="),
        ],
    },
]


def write_text_if_changed(text, path):
    try:
        with open(path, "r", encoding="utf-8") as input_file:
            old_text = input_file.read()
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
    write_text_if_changed(output.getvalue(), path)


def fetch_url(url):
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=45) as response:
        return response.read(), response.headers.get_content_type()


def text_from_pdf_bytes(data):
    with tempfile.NamedTemporaryFile(suffix=".pdf") as pdf_file:
        pdf_file.write(data)
        pdf_file.flush()
        text = subprocess.check_output(["pdftotext", pdf_file.name, "-"], text=True, errors="replace")
    return clean_extracted_text(text)


def clean_extracted_text(text):
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def text_from_html_bytes(data):
    parser = TextExtractor()
    parser.feed(data.decode("utf-8", errors="replace"))
    return parser.text()


def text_from_url(url):
    data, content_type = fetch_url(url)
    if content_type == "application/pdf" or data[:4] == b"%PDF":
        return text_from_pdf_bytes(data), content_type
    return text_from_html_bytes(data), content_type


def legistar_report_url(url):
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    legistar_id = query.get("ID", [""])[0]
    guid = query.get("GUID", [""])[0]
    return (
        "https://legistar.council.nyc.gov/ViewReport.ashx?M=R&N=Master&GID=61"
        f"&ID={legistar_id}&GUID={guid}&Extra=WithText&Title=Legislation+Details+(With+Text)"
    )


def source_text(title, kind, url):
    if kind == "pdf":
        text, content_type = text_from_url(url)
        return f"URL CONTENT TYPE: {content_type}\n\n{text}"

    detail_text, detail_content_type = text_from_url(url)
    report_url = legistar_report_url(url)
    report_text, report_content_type = text_from_url(report_url)
    return (
        f"DETAIL PAGE URL: {url}\n"
        f"DETAIL PAGE CONTENT TYPE: {detail_content_type}\n\n"
        f"DETAIL PAGE TEXT:\n{detail_text}\n\n"
        f"REPORT WITH TEXT URL: {report_url}\n"
        f"REPORT WITH TEXT CONTENT TYPE: {report_content_type}\n\n"
        f"REPORT WITH TEXT FULL EXTRACTED TEXT:\n{report_text}"
    )


def source_index(text, terms):
    lines = [line.strip() for line in text.splitlines()]
    project_hit_lines = set()
    for index, line in enumerate(lines):
        lowered = line.lower()
        if any(term.lower() in lowered for term in terms):
            for nearby in range(max(0, index - 4), min(len(lines), index + 5)):
                project_hit_lines.add(nearby)

    district_hit_lines = set()
    for index, line in enumerate(lines):
        if re.search(r"\bcouncil\s+district\b|\bdistrict\s+no\.?\b", line, flags=re.IGNORECASE):
            if any(term.lower() in line.lower() for term in terms) or index in project_hit_lines:
                for nearby in range(max(0, index - 2), min(len(lines), index + 3)):
                    district_hit_lines.add(nearby)

    selected_lines = sorted(project_hit_lines | district_hit_lines)
    indexed_lines = []
    previous_line = -2
    for line_number in selected_lines[:160]:
        if line_number > previous_line + 1:
            indexed_lines.append("...")
        indexed_lines.append(f"L{line_number + 1}: {lines[line_number]}")
        previous_line = line_number
    if len(selected_lines) > 160:
        indexed_lines.append(f"... [{len(selected_lines) - 160} additional indexed lines omitted]")
    return "\n".join(indexed_lines) if indexed_lines else "No source-index hits found."


def prompt_header(case):
    queries = "\n".join(f"- {query}" for query in case["search_queries"])
    return f"""You are helping adjudicate official NYC Council land-use geography for a PhD research dataset.

Read the source index first, then use the full official text below it. Use web search the way a careful researcher would: start with official NYC Council, DCP/CPC, City Record, agency, or archived map sources; then use reputable news or project pages only to corroborate location, local member, or neighborhood. Do not use news coverage to override official action text unless the official text is ambiguous and the news source gives a clear location.

signature_review_id: {case["signature_review_id"]}
project: {case["project"]}
known_conflict: {case["known_conflict"]}
question: {case["question"]}

Suggested web searches:
{queries}

Return exactly one JSON object, no markdown, with these fields:
signature_review_id, accepted_council_districts, confidence (high|medium|low), verdict (prior_ai_correct|deterministic_correct|both_partly_correct|unresolved), official_document_basis, source_index_lines_used, web_sources_checked, web_or_news_corroboration, remaining_uncertainty, human_review_needed (yes|no).

If the source index contains an explicit Council District line for the project, cite that line and use it. If the official documents identify only address/block/lot/project boundaries, say whether web search or historical boundary checks are still needed.
"""


manifest_rows = []
query_rows = []
for case in CASES:
    full_parts = [prompt_header(case)]
    indexed_parts = [prompt_header(case)]
    for query in case["search_queries"]:
        query_rows.append({"signature_review_id": case["signature_review_id"], "search_query": query})

    for source_number, (title, kind, url) in enumerate(case["sources"], start=1):
        print(f"Fetching {case['signature_review_id']} source {source_number}/{len(case['sources'])}: {title}", flush=True)
        text = source_text(title, kind, url)
        index_text = source_index(text, case["index_terms"])

        indexed_parts.extend(
            [
                "\n" + "=" * 80,
                f"SOURCE {source_number}: {title}",
                f"URL: {url}",
                "SOURCE INDEX:",
                index_text,
            ]
        )
        full_parts.extend(
            [
                "\n" + "=" * 80,
                f"SOURCE {source_number}: {title}",
                f"URL: {url}",
                "SOURCE INDEX:",
                index_text,
                "\nFULL EXTRACTED TEXT:",
                text,
            ]
        )

    full_prompt = "\n".join(full_parts).strip() + "\n"
    indexed_prompt = "\n".join(indexed_parts).strip() + "\n"
    full_path = f"../output/full_document_conflict_prompts/{case['signature_review_id']}_full_document_prompt.md"
    indexed_path = f"../output/full_document_conflict_prompts/{case['signature_review_id']}_indexed_web_prompt.md"
    write_text_if_changed(full_prompt, full_path)
    write_text_if_changed(indexed_prompt, indexed_path)
    manifest_rows.append(
        {
            "signature_review_id": case["signature_review_id"],
            "full_prompt_path": full_path,
            "indexed_web_prompt_path": indexed_path,
            "source_count": str(len(case["sources"])),
            "search_query_count": str(len(case["search_queries"])),
            "full_prompt_characters": str(len(full_prompt)),
            "indexed_web_prompt_characters": str(len(indexed_prompt)),
        }
    )

write_csv_if_changed(
    manifest_rows,
    [
        "signature_review_id",
        "full_prompt_path",
        "indexed_web_prompt_path",
        "source_count",
        "search_query_count",
        "full_prompt_characters",
        "indexed_web_prompt_characters",
    ],
    "../output/full_document_conflict_prompts/conflict_indexed_web_prompt_manifest.csv",
)
write_csv_if_changed(
    query_rows,
    ["signature_review_id", "search_query"],
    "../output/full_document_conflict_prompts/conflict_indexed_web_search_queries.csv",
)
