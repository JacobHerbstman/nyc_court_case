#!/usr/bin/env python3

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_official_ulurp_cpc_corpus/code")

import csv
import json
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path


MEETING_ARCHIVE_URL = (
    "https://www.nyc.gov/assets/planning/json/content/commission/"
    "disposition-sheets.json"
)
WORKER_COUNT = 8


def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def parse_date(value):
    value = clean_text(value)
    if not value:
        return None
    for date_format in ("%Y-%m-%d", "%B %d, %Y"):
        try:
            return datetime.strptime(value[:10] if date_format == "%Y-%m-%d" else value, date_format).date()
        except ValueError:
            continue
    return None


def date_values(value):
    return [
        parsed
        for parsed in (parse_date(part) for part in clean_text(value).split("; "))
        if parsed is not None
    ]


def fetch_text(url):
    result = subprocess.run(
        [
            "curl",
            "--silent",
            "--show-error",
            "--location",
            "--fail",
            "--user-agent",
            "Mozilla/5.0",
            url,
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(clean_text(result.stderr) or f"curl exited {result.returncode}")
    return result.stdout


def download_and_extract(job):
    candidate, target_stems = job
    meeting_date, meeting_type, url = candidate
    filename = re.sub(r"[^A-Za-z0-9_.-]+", "_", url.rsplit("/", 1)[-1])
    pdf_path = Path("../temp") / f"cpc_disposition_{filename}"
    download = subprocess.run(
        [
            "curl",
            "--silent",
            "--show-error",
            "--location",
            "--fail",
            "--user-agent",
            "Mozilla/5.0",
            "--output",
            str(pdf_path),
            url,
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if download.returncode != 0:
        return candidate, "download_failed", clean_text(download.stderr), "", "", ""
    full_text = subprocess.run(
        ["pdftotext", "-layout", str(pdf_path), "-"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    first_page = subprocess.run(
        ["pdftotext", "-f", "1", "-l", "1", "-layout", str(pdf_path), "-"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if full_text.returncode != 0:
        return candidate, "text_failed", clean_text(full_text.stderr), "", "", ""
    full_page_text = full_text.stdout
    first_page_text = first_page.stdout
    text_method = "pdftotext"
    if len(re.findall(r"\b\w+\b", full_page_text)) < 50:
        image_prefix = Path("../temp") / f"cpc_disposition_ocr_{filename}"
        render = subprocess.run(
            [
                "pdftoppm",
                "-r",
                "250",
                "-png",
                str(pdf_path),
                str(image_prefix),
            ],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        image_paths = sorted(
            Path("../temp").glob(f"{image_prefix.name}-*.png"),
            key=lambda path: int(re.search(r"-(\d+)\.png$", path.name).group(1)),
        )
        if render.returncode == 0 and image_paths:
            ocr_pages = []
            found_stems = set()
            for image_path in image_paths:
                ocr = subprocess.run(
                    ["tesseract", str(image_path), "stdout", "--psm", "6"],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    check=False,
                )
                page_text = ocr.stdout if ocr.returncode == 0 else ""
                ocr_pages.append(page_text)
                if len(ocr_pages) == 1:
                    first_page_text = page_text
                compact_page = re.sub(r"[^A-Z0-9]", "", page_text.upper())
                found_stems.update(
                    stem for stem in target_stems if stem in compact_page
                )
                image_path.unlink()
                if found_stems == set(target_stems):
                    for remaining_path in image_paths[len(ocr_pages):]:
                        remaining_path.unlink()
                    break
            full_page_text = "\n\f\n".join(ocr_pages)
            text_method = "full_document_ocr"
    return (
        candidate,
        "text_extracted",
        "",
        full_page_text,
        first_page_text,
        text_method,
    )


with open(
    "../output/official_ulurp_cpc_zap_milestones.csv",
    encoding="utf-8",
    newline="",
) as input_file:
    residual_rows = list(csv.DictReader(input_file))

meeting_archive = json.loads(fetch_text(MEETING_ARCHIVE_URL))
public_meetings = []
for row in meeting_archive:
    meeting_date = parse_date(row.get("date"))
    meeting_type = clean_text(row.get("type"))
    url = clean_text(row.get("dispo"))
    if meeting_date and "Public Meeting" in meeting_type and url:
        public_meetings.append((meeting_date, meeting_type, url))

candidates_by_application = {}
all_candidates = set()
candidate_stems = {}
for row in residual_rows:
    vote_dates = date_values(row["cpc_vote_dates"])
    if vote_dates:
        candidates = {
            meeting
            for meeting in public_meetings
            if meeting[0] in set(vote_dates)
        }
    else:
        hearing_dates = date_values(row["cpc_hearing_dates"])
        start_date = max(hearing_dates) if hearing_dates else parse_date(row["certified_referred_date"])
        end_date = parse_date(row["completed_date"])
        if start_date and (
            end_date is None or end_date < start_date + timedelta(days=180)
        ):
            end_date = start_date + timedelta(days=180)
        if start_date and end_date:
            candidates = {
                meeting
                for meeting in public_meetings
                if start_date - timedelta(days=7)
                <= meeting[0]
                <= end_date + timedelta(days=7)
            }
        else:
            candidates = set()
    candidates_by_application[row["raw_application_number"]] = candidates
    all_candidates.update(candidates)
    stem = re.sub(r"\D", "", row["raw_application_number"])[:6]
    for candidate in candidates:
        candidate_stems.setdefault(candidate, set()).add(stem)

meeting_text = {}
with ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
    for candidate, status, error, full_text, first_page, text_method in executor.map(
        download_and_extract,
        [
            (candidate, tuple(sorted(candidate_stems[candidate])))
            for candidate in sorted(all_candidates)
        ],
    ):
        meeting_text[candidate] = {
            "status": status,
            "error": error,
            "full_text": full_text,
            "first_page": first_page,
            "text_method": text_method,
        }

output_rows = []
for row in residual_rows:
    compact_key = re.sub(r"[^A-Z0-9]", "", row["raw_application_number"].upper())
    if compact_key.startswith("C"):
        compact_key = compact_key[1:]
    identifier_pattern = re.compile(
        r"\b"
        + r"\s*".join(re.escape(character) for character in compact_key)
        + r"\b",
        re.IGNORECASE,
    )
    stem_pattern = re.compile(
        r"\b"
        + r"\s*".join(re.escape(character) for character in compact_key[:6])
        + r"\b",
        re.IGNORECASE,
    )
    application_matches = []
    for candidate in sorted(candidates_by_application[row["raw_application_number"]]):
        extracted = meeting_text[candidate]
        full_match = identifier_pattern.search(extracted["full_text"])
        first_page_match = identifier_pattern.search(extracted["first_page"])
        match_basis = "full_application_identifier"
        if not full_match and not first_page_match:
            full_match = stem_pattern.search(extracted["full_text"])
            first_page_match = stem_pattern.search(extracted["first_page"])
            match_basis = "six_digit_application_stem"
        if not full_match and not first_page_match:
            continue
        source_text = extracted["first_page"] if first_page_match else extracted["full_text"]
        match = first_page_match or full_match
        disposition_window = clean_text(
            source_text[max(0, match.start() - 500): match.end() + 4000]
        )
        disposition_match = re.search(
            r"(COMMISSION\s+FAILED\s+TO\s+ADOPT|"
            r"FAVORABLE\s+REPORT.{0,80}?ADOPTED|"
            r"DISPOSITION\s*:\s*[^\f]{0,250}|"
            r"WITHDRAWN|MOTION\s+TO\s+FILE|DISAPPROVED|"
            r"LAID\s+OVER|HEARING\s+CLOSED|CLOSE\s+THE\s+HEARING)",
            disposition_window,
            re.IGNORECASE,
        )
        context = clean_text(
            source_text[max(0, match.start() - 180): match.end() + 420]
        )
        application_matches.append(
            {
                "meeting_date": candidate[0].isoformat(),
                "meeting_type": candidate[1],
                "meeting_url": candidate[2],
                "first_page_match": str(first_page_match is not None).upper(),
                "match_basis": match_basis,
                "meeting_text_method": extracted["text_method"],
                "disposition_phrase": (
                    clean_text(disposition_match.group(0))
                    if disposition_match
                    else ""
                ),
                "match_context": context,
            }
        )

    if application_matches:
        for match in application_matches:
            output_rows.append(
                {
                    "project_id": row["project_id"],
                    "raw_application_number": row["raw_application_number"],
                    "project_name": row["project_name"],
                    "zap_cpc_vote_dates": row["cpc_vote_dates"],
                    "zap_cpc_vote_outcomes": row["cpc_vote_outcomes"],
                    "candidate_meeting_count": len(
                        candidates_by_application[row["raw_application_number"]]
                    ),
                    "verification_status": "identifier_found_in_disposition_archive",
                    **match,
                }
            )
    else:
        candidate_errors = [
            meeting_text[candidate]["error"]
            for candidate in candidates_by_application[row["raw_application_number"]]
            if meeting_text[candidate]["error"]
        ]
        output_rows.append(
            {
                "project_id": row["project_id"],
                "raw_application_number": row["raw_application_number"],
                "project_name": row["project_name"],
                "zap_cpc_vote_dates": row["cpc_vote_dates"],
                "zap_cpc_vote_outcomes": row["cpc_vote_outcomes"],
                "candidate_meeting_count": len(
                    candidates_by_application[row["raw_application_number"]]
                ),
                "verification_status": (
                    "candidate_download_or_text_failure"
                    if candidate_errors
                    else "identifier_not_found_in_candidate_meetings"
                ),
                "meeting_date": "",
                "meeting_type": "",
                "meeting_url": "",
                "first_page_match": "",
                "match_basis": "",
                "meeting_text_method": "",
                "disposition_phrase": "",
                "match_context": "; ".join(sorted(set(candidate_errors))),
            }
        )

with open(
    "../output/official_ulurp_cpc_zap_disposition_matches.csv",
    "w",
    encoding="utf-8",
    newline="",
) as output_file:
    writer = csv.DictWriter(output_file, fieldnames=list(output_rows[0].keys()))
    writer.writeheader()
    writer.writerows(output_rows)

print(
    f"Checked {len(residual_rows)} residual applications against "
    f"{len(all_candidates)} candidate CPC disposition sheets."
)
