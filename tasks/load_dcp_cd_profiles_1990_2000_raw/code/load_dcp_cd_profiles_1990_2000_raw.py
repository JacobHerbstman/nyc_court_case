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

BoroughCodeMap = {
    "mn": "1",
    "bx": "2",
    "bk": "3",
    "qn": "4",
    "si": "5",
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
    if len(sys.argv) != 4:
        raise SystemExit(
            "Expected 3 arguments: dcp_cd_profiles_1990_2000_files_csv out_index_csv out_qc_csv"
        )

    dcp_cd_profiles_1990_2000_files_csv = Path(sys.argv[1])
    out_index_csv = Path(sys.argv[2])
    out_qc_csv = Path(sys.argv[3])

    file_inventory = pd.read_csv(dcp_cd_profiles_1990_2000_files_csv)

    if file_inventory.empty:
        pd.DataFrame().to_csv(out_index_csv, index=False)
        pd.DataFrame().to_csv(out_qc_csv, index=False)
        return

    file_inventory = file_inventory[
        (file_inventory["file_role"] == "borough_profile_pdf")
        & file_inventory["raw_path"].notna()
        & file_inventory["raw_path"].map(lambda x: Path(x).exists())
    ].copy()

    if file_inventory.empty:
        pd.DataFrame().to_csv(out_index_csv, index=False)
        pd.DataFrame().to_csv(out_qc_csv, index=False)
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
            district_ids: set[str] = set()

            for group_start in range(0, len(parsed_pages), 7):
                page_group = parsed_pages[group_start : group_start + 7]
                title_district_ids = [
                    page_row["title_district_id"]
                    for page_row in page_group
                    if page_row["title_district_id"] is not None
                ]

                group_district_id = None

                if title_district_ids:
                    group_district_id = Counter(title_district_ids).most_common(1)[0][0]

                for page_row in page_group:
                    assigned_district_id = group_district_id or page_row["title_district_id"]

                    if assigned_district_id is None:
                        continue

                    page_status = (
                        "group_inferred_district"
                        if page_row["title_district_id"] != assigned_district_id
                        else "parsed_page"
                    )

                    page_rows.append(
                        {
                            "source_id": page_row["source_id"],
                            "pull_date": page_row["pull_date"],
                            "borough_code": page_row["borough_code"],
                            "borough_name": page_row["borough_name"],
                            "pdf_path": page_row["pdf_path"],
                            "pdf_page_number": page_row["pdf_page_number"],
                            "district_id": assigned_district_id,
                            "district_header": page_row["district_header"],
                            "page_title": page_row["page_title"],
                            "profile_page_type": page_row["profile_page_type"],
                            "section_count": page_row["section_count"],
                            "line_count": page_row["line_count"],
                            "status": page_status,
                        }
                    )

                    parsed_page_count += 1
                    parsed_line_count += page_row["line_count"]
                    district_ids.add(assigned_district_id)

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
                                "district_header": page_row["district_header"],
                                "page_title": page_row["page_title"],
                                "profile_page_type": page_row["profile_page_type"],
                                "line_number": line_number,
                                "line_text": line,
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
                    "status": "loaded" if parsed_page_count > 0 else "review_required",
                }
            )

        if not line_rows:
            continue

        line_df = pd.DataFrame(line_rows)
        page_df = pd.DataFrame(page_rows)

        raw_parquet_local = Path("..") / "output" / f"dcp_cd_profiles_1990_2000_{pull_date}_raw.parquet"
        page_index_local = Path("..") / "output" / f"dcp_cd_profiles_1990_2000_{pull_date}_page_index.csv"

        raw_parquet_local.parent.mkdir(parents=True, exist_ok=True)
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
                "status": "loaded",
            }
        )

    pd.DataFrame(index_rows).to_csv(out_index_csv, index=False)
    pd.DataFrame(qc_rows).to_csv(out_qc_csv, index=False)
    print(f"Wrote raw DCP CD profile outputs to {out_index_csv.parent}")


if __name__ == "__main__":
    main()
