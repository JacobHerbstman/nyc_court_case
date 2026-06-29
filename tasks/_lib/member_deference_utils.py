from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


APPLICATION_RE = re.compile(
    r"\b(?:[CNM]\s*)?\d{6,8}\s*(?:\([A-Z0-9]+\)\s*)?[A-Z]{2,4}\b",
    flags=re.IGNORECASE,
)


def normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", "" if value is None or pd.isna(value) else str(value)).strip()


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


def application_keys(value: object) -> list[str]:
    keys = []
    for match in APPLICATION_RE.finditer("" if value is None or pd.isna(value) else str(value)):
        key = re.sub(r"[^A-Za-z0-9]", "", match.group(0)).upper()
        key = re.sub(r"^[CNM](?=\d)", "", key)
        if key not in keys:
            keys.append(key)
    return keys


def collapse_districts(values: object) -> str:
    districts = []
    for value in values:
        if value is None or pd.isna(value):
            continue
        for match in re.findall(r"\d{1,2}", str(value)):
            district = int(match)
            if 1 <= district <= 51 and str(district) not in districts:
                districts.append(str(district))
    return "; ".join(districts)


def collapse_values(values: object) -> str:
    clean_values = []
    for value in values:
        if value is None or pd.isna(value) or str(value).strip() == "":
            continue
        if str(value) not in clean_values:
            clean_values.append(str(value))
    return "; ".join(clean_values)


def collapse_semicolon_values(values: object) -> str:
    clean_values = []
    for value in values:
        if value is None or pd.isna(value) or str(value).strip() == "":
            continue
        for part in split_semicolon(value):
            if part not in clean_values:
                clean_values.append(part)
    return "; ".join(clean_values)


def collapse_examples(values: object, limit: int = 20) -> str:
    examples = []
    for value in values:
        if value is None or pd.isna(value) or str(value).strip() == "":
            continue
        value_text = str(value)
        if value_text not in examples:
            examples.append(value_text)
    return "; ".join(examples[:limit])


def district_from_scalar(value: object) -> str:
    return collapse_districts([value])


def write_csv(path: str, df: pd.DataFrame) -> None:
    temp_path = Path(path).with_suffix(Path(path).suffix + ".tmp")
    df.to_csv(temp_path, index=False)
    temp_path.replace(path)
