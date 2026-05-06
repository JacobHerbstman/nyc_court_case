from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
from pypdf import PdfReader


PAGE_TYPE_MAP = {
    "Socioeconomic Profile Social Characteristics": "social",
    "Socioeconomic Profile Social and Educational Characteristics": "social_education",
    "Socioeconomic Profile Labor Force and Employment Characteristics": "labor_employment",
    "Socioeconomic Profile Labor Force and Income Characteristics": "labor_income",
    "Socioeconomic Profile Income and Poverty Characteristics": "income_poverty",
    "Socioeconomic Profile Housing Characteristics": "housing",
    "Socioeconomic Profile Housing and Economic Characteristics": "housing_economic",
}
PAGE_TYPE_SEQUENCE = list(PAGE_TYPE_MAP.values())
EXPECTED_PROFILE_PAGE_COUNT = len(PAGE_TYPE_MAP)

BoroughCodeMap = {
    "mn": "1",
    "bx": "2",
    "bk": "3",
    "qn": "4",
    "si": "5",
}
EXPECTED_BOROUGH_DISTRICT_COUNT = {
    "mn": 12,
    "bx": 12,
    "bk": 18,
    "qn": 14,
    "si": 3,
}

DISTRICT_RE = re.compile(r"Community District\s+(\d{1,2})\b")


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


def detect_page_type(title: str) -> str | None:
    for prefix, page_type in PAGE_TYPE_MAP.items():
        if title.startswith(prefix):
            return page_type
    return None


def is_section_header(line: str) -> bool:
    if (
        not line
        or line.startswith("Socioeconomic Profile ")
        or line == "1990 and 2000 Census"
        or line == "Number Percent Number Percent Number Percent"
        or line.startswith("Source:")
        or line.startswith("Population Division - New York City Department of City Planning")
        or "Community District" in line and "Change 1990-2000" in line
    ):
        return False

    if line.startswith("(") and line.endswith(")"):
        return False

    letter_tokens = [token for token in re.split(r"\s+", line) if re.search(r"[A-Za-z]", token)]

    if not letter_tokens:
        return False

    allowed_lowercase = {"and", "of", "in", "to", "a", "as"}
    has_header_word = False

    for token in letter_tokens:
        token_letters = re.sub(r"[^A-Za-z]", "", token)

        if not token_letters:
            continue

        if token_letters.lower() in allowed_lowercase:
            continue

        has_header_word = True

        if token_letters != token_letters.upper():
            return False

    return has_header_word


def parse_title(title: str, borough_code: str) -> tuple[str | None, str | None, str | None]:
    page_type = detect_page_type(title)
    if page_type is None:
        return None, None, None

    title_parts = title.split(" - ", 1)
    if len(title_parts) != 2:
        return page_type, None, None

    page_title = title_parts[0]
    district_header = title_parts[1]
    district_match = DISTRICT_RE.search(district_header)

    if district_match is None:
        return page_type, page_title, None

    district_num = int(district_match.group(1))
    district_id = f"{BoroughCodeMap[borough_code]}{district_num:02d}"
    return page_type, page_title, district_id


def main() -> None:
    if len(sys.argv) != 1:
        raise SystemExit("Expected no arguments.")

    file_inventory = pd.read_csv("../input/dcp_cd_profiles_1990_2000_files.csv")

    if file_inventory.empty:
        pd.DataFrame().to_csv("../output/dcp_cd_profiles_1990_2000_raw_files.csv", index=False)
        pd.DataFrame().to_csv("../output/dcp_cd_profiles_1990_2000_raw_qc.csv", index=False)
        return

    file_inventory = file_inventory[
        (file_inventory["file_role"] == "borough_profile_pdf")
        & file_inventory["raw_path"].notna()
        & file_inventory["raw_path"].map(lambda x: Path(x).exists())
    ].copy()

    if file_inventory.empty:
        pd.DataFrame().to_csv("../output/dcp_cd_profiles_1990_2000_raw_files.csv", index=False)
        pd.DataFrame().to_csv("../output/dcp_cd_profiles_1990_2000_raw_qc.csv", index=False)
        return

    index_rows: list[dict[str, object]] = []
    qc_rows: list[dict[str, object]] = []

    for pull_date, pull_df in file_inventory.groupby("pull_date", sort=True):
        line_rows: list[dict[str, object]] = []
        page_rows: list[dict[str, object]] = []

        for row in pull_df.sort_values(["borough_code", "raw_path"]).to_dict("records"):
            pdf_path = Path(row["raw_path"])
            reader = PdfReader(str(pdf_path))
            parsed_pages: list[dict[str, object]] = []
            parsed_page_count = 0
            parsed_line_count = 0

            for pdf_page_number, page in enumerate(reader.pages, start=1):
                text = page.extract_text() or ""
                lines = [normalize_space(line) for line in text.splitlines() if normalize_space(line)]

                if not lines:
                    continue

                title = lines[0]
                page_type, page_title, district_value = parse_title(title, row["borough_code"])

                if page_type is None or page_title is None or district_value is None:
                    continue

                district_header = title.split(" - ", 1)[1]
                section_count = sum(is_section_header(line) for line in lines)
                parsed_pages.append(
                    {
                        "source_id": row["source_id"],
                        "pull_date": row["pull_date"],
                        "borough_code": row["borough_code"],
                        "borough_name": row["borough_name"],
                        "pdf_path": row["raw_path"],
                        "pdf_page_number": pdf_page_number,
                        "title_district_id": district_value,
                        "district_header": district_header,
                        "page_title": page_title,
                        "profile_page_type": page_type,
                        "section_count": section_count,
                        "line_count": len(lines),
                        "lines": lines,
                    }
                )

            parsed_pages.sort(key=lambda page_row: page_row["pdf_page_number"])
            sequence_can_assign = (
                len(parsed_pages)
                == EXPECTED_BOROUGH_DISTRICT_COUNT[row["borough_code"]] * EXPECTED_PROFILE_PAGE_COUNT
                and all(
                    page_row["profile_page_type"]
                    == PAGE_TYPE_SEQUENCE[(page_idx - 1) % EXPECTED_PROFILE_PAGE_COUNT]
                    for page_idx, page_row in enumerate(parsed_pages, start=1)
                )
            )
            district_ids: set[str] = set()
            pdf_page_rows: list[dict[str, object]] = []
            district_page_counts: Counter[str] = Counter()
            district_page_types: dict[str, set[str]] = {}

            for page_idx, page_row in enumerate(parsed_pages, start=1):
                sequence_district_id = None

                if sequence_can_assign:
                    sequence_district_num = (page_idx - 1) // EXPECTED_PROFILE_PAGE_COUNT + 1
                    sequence_district_id = f"{BoroughCodeMap[row['borough_code']]}{sequence_district_num:02d}"

                assigned_district_id = sequence_district_id or page_row["title_district_id"]

                if assigned_district_id is None:
                    continue

                pdf_page_rows.append(
                    {
                        "source_id": page_row["source_id"],
                        "pull_date": page_row["pull_date"],
                        "borough_code": page_row["borough_code"],
                        "borough_name": page_row["borough_name"],
                        "pdf_path": page_row["pdf_path"],
                        "pdf_page_number": page_row["pdf_page_number"],
                        "district_id": assigned_district_id,
                        "title_district_id": page_row["title_district_id"],
                        "sequence_district_id": sequence_district_id,
                        "district_header": page_row["district_header"],
                        "page_title": page_row["page_title"],
                        "profile_page_type": page_row["profile_page_type"],
                        "section_count": page_row["section_count"],
                        "line_count": page_row["line_count"],
                        "status": "parsed_page",
                        "validation_notes": "",
                    }
                )

                parsed_page_count += 1
                parsed_line_count += page_row["line_count"]
                district_ids.add(assigned_district_id)
                district_page_counts[assigned_district_id] += 1
                district_page_types.setdefault(assigned_district_id, set()).add(page_row["profile_page_type"])

                for line_number, line in enumerate(page_row["lines"], start=1):
                    line_rows.append(
                        {
                            "source_id": page_row["source_id"],
                            "pull_date": page_row["pull_date"],
                            "borough_code": page_row["borough_code"],
                            "borough_name": page_row["borough_name"],
                            "pdf_path": page_row["pdf_path"],
                            "pdf_page_number": page_row["pdf_page_number"],
                            "district_id": assigned_district_id,
                            "title_district_id": page_row["title_district_id"],
                            "sequence_district_id": sequence_district_id,
                            "district_header": page_row["district_header"],
                            "page_title": page_row["page_title"],
                            "profile_page_type": page_row["profile_page_type"],
                            "line_number": line_number,
                            "line_text": line,
                        }
                    )

            for page_row in pdf_page_rows:
                validation_notes: list[str] = []
                district_id = str(page_row["district_id"])

                if district_page_counts[district_id] != EXPECTED_PROFILE_PAGE_COUNT:
                    validation_notes.append("unexpected_district_page_count")

                if len(district_page_types[district_id]) != district_page_counts[district_id]:
                    validation_notes.append("duplicate_profile_page_type")

                if page_row["sequence_district_id"] and page_row["sequence_district_id"] != page_row["title_district_id"]:
                    validation_notes.append("sequence_district_id_corrected")

                if validation_notes:
                    page_row["status"] = "review_required"
                    page_row["validation_notes"] = ";".join(validation_notes)

            page_rows.extend(pdf_page_rows)

            pdf_validation_notes = sorted(
                {
                    note
                    for page_row in pdf_page_rows
                    for note in str(page_row["validation_notes"]).split(";")
                    if note
                }
            )

            qc_rows.append(
                {
                    "source_id": row["source_id"],
                    "pull_date": row["pull_date"],
                    "borough_code": row["borough_code"],
                    "borough_name": row["borough_name"],
                    "pdf_path": row["raw_path"],
                    "total_pdf_pages": len(reader.pages),
                    "parsed_page_count": parsed_page_count,
                    "parsed_line_count": parsed_line_count,
                    "district_count": len(district_ids),
                    "validation_notes": ";".join(pdf_validation_notes),
                    "status": "loaded" if parsed_page_count > 0 and not pdf_validation_notes else "review_required",
                }
            )

        if not line_rows:
            continue

        line_df = pd.DataFrame(line_rows)
        page_df = pd.DataFrame(page_rows)

        raw_parquet_local = Path("..") / "output" / f"dcp_cd_profiles_1990_2000_{pull_date}_raw.parquet"
        page_index_local = Path("..") / "output" / f"dcp_cd_profiles_1990_2000_{pull_date}_page_index.csv"

        line_df.to_parquet(raw_parquet_local, index=False)
        page_df.to_csv(page_index_local, index=False)

        raw_parquet_repo = Path("..") / ".." / "load_dcp_cd_profiles_1990_2000_raw" / "output" / raw_parquet_local.name
        page_index_repo = Path("..") / ".." / "load_dcp_cd_profiles_1990_2000_raw" / "output" / page_index_local.name

        index_rows.append(
            {
                "source_id": "dcp_cd_profiles_1990_2000",
                "pull_date": pull_date,
                "raw_parquet_path": str(raw_parquet_repo),
                "page_index_csv_path": str(page_index_repo),
                "status": (
                    "review_required"
                    if any(
                        qc_row["pull_date"] == pull_date and qc_row["status"] == "review_required"
                        for qc_row in qc_rows
                    )
                    else "loaded"
                ),
            }
        )

    pd.DataFrame(index_rows).to_csv("../output/dcp_cd_profiles_1990_2000_raw_files.csv", index=False)
    pd.DataFrame(qc_rows).to_csv("../output/dcp_cd_profiles_1990_2000_raw_qc.csv", index=False)
    print("Wrote raw DCP CD profile outputs to ../output")


if __name__ == "__main__":
    main()
