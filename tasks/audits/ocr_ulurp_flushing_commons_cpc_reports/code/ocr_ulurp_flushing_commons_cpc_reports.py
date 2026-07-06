#!/usr/bin/env python3

import csv
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path


# setwd("tasks/audits/ocr_ulurp_flushing_commons_cpc_reports/code")
# ocr_dpi = 150
# ocr_page_timeout_seconds = 60


REPORTS = [
    {
        "raw_application_number": "C100206PPQ",
        "parsed_action_code": "PP",
        "input_pdf_path": "../input/100206_P2010Q0132_C100206PPQ_f1ec804c.pdf",
        "output_text_path": "../output/flushing_commons_C100206PPQ_ocr.txt",
        "source_doc": "https://www.nyc.gov/assets/planning/download/pdf/about/cpc/100206.pdf",
    },
    {
        "raw_application_number": "C100207ZMQ",
        "parsed_action_code": "ZM",
        "input_pdf_path": "../input/100207_P2010Q0132_C100207ZMQ_db53b2c6.pdf",
        "output_text_path": "../output/flushing_commons_C100207ZMQ_ocr.txt",
        "source_doc": "https://www.nyc.gov/assets/planning/download/pdf/about/cpc/100207.pdf",
    },
    {
        "raw_application_number": "C100208ZSQ",
        "parsed_action_code": "ZS",
        "input_pdf_path": "../input/100208_P2010Q0132_C100208ZSQ_2776d16b.pdf",
        "output_text_path": "../output/flushing_commons_C100208ZSQ_ocr.txt",
        "source_doc": "https://www.nyc.gov/assets/planning/download/pdf/about/cpc/100208.pdf",
    },
    {
        "raw_application_number": "C100209ZSQ",
        "parsed_action_code": "ZS",
        "input_pdf_path": "../input/100209_P2010Q0132_C100209ZSQ_c571d786.pdf",
        "output_text_path": "../output/flushing_commons_C100209ZSQ_ocr.txt",
        "source_doc": "https://www.nyc.gov/assets/planning/download/pdf/about/cpc/100209.pdf",
    },
    {
        "raw_application_number": "C100212ZSQ",
        "parsed_action_code": "ZS",
        "input_pdf_path": "../input/100212_P2010Q0132_C100212ZSQ_cf25c4b5.pdf",
        "output_text_path": "../output/flushing_commons_C100212ZSQ_ocr.txt",
        "source_doc": "https://www.nyc.gov/assets/planning/download/pdf/about/cpc/100212.pdf",
    },
]

MANIFEST_COLUMNS = [
    "project_id",
    "project_name",
    "corpus_reference_year",
    "raw_application_number",
    "parsed_action_code",
    "ocr_status",
    "page_count",
    "ocr_page_count",
    "skipped_page_count",
    "text_char_count",
    "input_pdf_path",
    "output_text_path",
    "source_doc",
]


def clean_text(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def pdf_page_count(pdf_path):
    completed = subprocess.run(
        ["pdfinfo", pdf_path],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(clean_text(completed.stderr) or f"pdfinfo failed for {pdf_path}")

    for line in completed.stdout.splitlines():
        if line.startswith("Pages:"):
            pages = clean_text(line.removeprefix("Pages:"))
            if pages.isdigit() and int(pages) > 0:
                return int(pages)
    raise RuntimeError(f"Could not read page count for {pdf_path}")


def ocr_report(report, ocr_dpi, ocr_page_timeout_seconds):
    page_count = pdf_page_count(report["input_pdf_path"])
    page_texts = []
    skipped_pages = []

    with tempfile.TemporaryDirectory() as temp_dir:
        for page_number in range(1, page_count + 1):
            image_prefix = os.path.join(temp_dir, f"page_{page_number}")
            try:
                rendered = subprocess.run(
                    [
                        "pdftoppm",
                        "-f",
                        str(page_number),
                        "-l",
                        str(page_number),
                        "-r",
                        str(ocr_dpi),
                        "-png",
                        report["input_pdf_path"],
                        image_prefix,
                    ],
                    capture_output=True,
                    text=True,
                    timeout=ocr_page_timeout_seconds,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                skipped_pages.append(str(page_number))
                continue

            if rendered.returncode != 0:
                raise RuntimeError(
                    clean_text(rendered.stderr)
                    or f"pdftoppm failed on {report['raw_application_number']} page {page_number}"
                )

            image_paths = sorted(
                os.path.join(temp_dir, filename)
                for filename in os.listdir(temp_dir)
                if filename.startswith(f"page_{page_number}-") and filename.endswith(".png")
            )
            if not image_paths:
                skipped_pages.append(str(page_number))
                continue

            try:
                recognized = subprocess.run(
                    [
                        "tesseract",
                        image_paths[0],
                        "stdout",
                        "--psm",
                        "6",
                        "--dpi",
                        str(ocr_dpi),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=ocr_page_timeout_seconds,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                os.remove(image_paths[0])
                skipped_pages.append(str(page_number))
                continue

            os.remove(image_paths[0])
            if recognized.returncode != 0:
                raise RuntimeError(
                    clean_text(recognized.stderr)
                    or f"tesseract failed on {report['raw_application_number']} page {page_number}"
                )

            page_texts.append(f"[PAGE {page_number}]\n{recognized.stdout.strip()}")

    report_text = "\n\n".join(page_texts).strip() + "\n"

    text_char_count = len(clean_text(report_text))
    return {
        "project_id": "P2010Q0132",
        "project_name": "FLUSHING COMMONS",
        "corpus_reference_year": "2010",
        "raw_application_number": report["raw_application_number"],
        "parsed_action_code": report["parsed_action_code"],
        "ocr_status": "text_extracted_ocr" if text_char_count > 0 else "empty_ocr_text",
        "page_count": page_count,
        "ocr_page_count": len(page_texts),
        "skipped_page_count": len(skipped_pages),
        "text_char_count": text_char_count,
        "input_pdf_path": report["input_pdf_path"],
        "output_text_path": report["output_text_path"],
        "source_doc": report["source_doc"],
    }, report_text


def main():
    if len(sys.argv) != 3:
        raise SystemExit("Usage: python3 ocr_ulurp_flushing_commons_cpc_reports.py OCR_DPI OCR_PAGE_TIMEOUT_SECONDS")

    ocr_dpi = int(sys.argv[1])
    ocr_page_timeout_seconds = int(sys.argv[2])

    rows = []
    text_outputs = []
    for report_number, report in enumerate(REPORTS, start=1):
        print(f"OCR {report_number}/{len(REPORTS)} {report['raw_application_number']}", flush=True)
        row, report_text = ocr_report(report, ocr_dpi, ocr_page_timeout_seconds)
        rows.append(row)
        text_outputs.append((report["output_text_path"], report_text))

    with Path("../output/flushing_commons_cpc_ocr_manifest.csv").open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=MANIFEST_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    for output_text_path, report_text in text_outputs:
        Path(output_text_path).write_text(report_text, encoding="utf-8")


if __name__ == "__main__":
    main()
