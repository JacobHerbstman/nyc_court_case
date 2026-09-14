"""Write CSV data and a deterministic, adjacent data report using only stdlib."""

import csv
import hashlib
import json
from collections import Counter
from pathlib import Path


def save_csv(rows, fields, destination, key):
    destination = Path(destination)
    keys = [tuple(row[field] for field in key) for row in rows]
    if any(any(value in (None, "") for value in item) for item in keys):
        raise ValueError(f"Missing key in {destination}")
    if len(keys) != len(set(keys)):
        raise ValueError(f"Duplicate key in {destination}: {key}")
    with destination.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    columns = {}
    for field in fields:
        values = [row.get(field) for row in rows]
        present = [v for v in values if v not in (None, "")]
        counts = Counter(str(v) for v in present)
        columns[field] = {
            "python_types": sorted(set(type(v).__name__ for v in present)),
            "missing": len(values) - len(present),
            "distinct_nonmissing": len(counts),
            "most_common": sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:5],
        }
        if present and all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in present):
            columns[field].update(minimum=min(present), maximum=max(present))
    report = {
        "dataset": destination.name,
        "rows": len(rows),
        "key": key,
        "unique_nonmissing_key": True,
        "sha256_csv_bytes": hashlib.sha256(destination.read_bytes()).hexdigest(),
        "fingerprint_scope": "Saved CSV bytes, including row order and line endings; not a proof of source correctness.",
        "columns": columns,
    }
    (destination.parent.parent / "report" / f"{destination.stem}.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
