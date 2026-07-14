#!/usr/bin/env python3

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_official_ulurp_cpc_event_validation/code")
# narratives_per_decade = 10
# candidate_zoning_per_decade = 3
# candidate_other_per_decade = 3

import csv
import hashlib
import re
import sys
from pathlib import Path


if len(sys.argv) != 4:
    raise RuntimeError(
        "Usage: python3 sample_official_ulurp_cpc_event_validation.py "
        "<narratives_per_decade> <candidate_zoning_per_decade> "
        "<candidate_other_per_decade>"
    )

narratives_per_decade = int(sys.argv[1])
candidate_zoning_per_decade = int(sys.argv[2])
candidate_other_per_decade = int(sys.argv[3])
comparison_per_decade = (
    narratives_per_decade
    - candidate_zoning_per_decade
    - candidate_other_per_decade
)
if min(
    narratives_per_decade,
    candidate_zoning_per_decade,
    candidate_other_per_decade,
    comparison_per_decade,
) < 1:
    raise RuntimeError("Invalid event-validation sample sizes.")

local_actor = re.compile(
    r"community board|borough president|council ?member|councilmember|"
    r"civic|association|residents?|neighbors?",
    re.IGNORECASE,
)
local_stance = re.compile(
    r"oppos|object|concern|request|recommend|condition|disapprov|protest",
    re.IGNORECASE,
)
possible_response = re.compile(
    r"in response|as a result|agreed|commit|revis|amend|modif|reduc|"
    r"eliminat|restrictive declaration|mitigat|study|task force",
    re.IGNORECASE,
)

with Path("../input/official_ulurp_cpc_narrative_manifest.csv").open(
    newline="", encoding="utf-8"
) as input_file:
    rows = [
        row
        for row in csv.DictReader(input_file)
        if row["analysis_narrative_unit_flag"] == "TRUE"
    ]

for row in rows:
    narrative_text = Path(row["local_text_path"]).read_text(
        encoding="utf-8", errors="replace"
    )[: int(row["narrative_end_char"])]
    row["_candidate"] = bool(
        local_actor.search(narrative_text)
        and local_stance.search(narrative_text)
        and possible_response.search(narrative_text)
    )
    row["_zoning"] = row["action_code"] in {"ZM", "ZR", "ZS"}
    row["_decade"] = f"{int(row['official_vote_year']) // 10 * 10}s"
    row["_rank"] = hashlib.sha256(
        f"{row['document_id']}|official-event-validation-v1".encode("utf-8")
    ).hexdigest()

selected_rows = []
for decade in ["1970s", "1980s", "1990s", "2000s", "2010s", "2020s"]:
    decade_rows = [row for row in rows if row["_decade"] == decade]
    candidate_zoning = sorted(
        [row for row in decade_rows if row["_candidate"] and row["_zoning"]],
        key=lambda row: row["_rank"],
    )[:candidate_zoning_per_decade]
    candidate_other = sorted(
        [row for row in decade_rows if row["_candidate"] and not row["_zoning"]],
        key=lambda row: row["_rank"],
    )[:candidate_other_per_decade]
    comparison = sorted(
        [row for row in decade_rows if not row["_candidate"]],
        key=lambda row: row["_rank"],
    )[:comparison_per_decade]
    comparison_overlap = []
    if len(comparison) < comparison_per_decade:
        already_selected = {
            row["document_id"]
            for row in candidate_zoning + candidate_other + comparison
        }
        comparison_overlap = sorted(
            [
                row
                for row in decade_rows
                if row["document_id"] not in already_selected
            ],
            key=lambda row: row["_rank"],
        )[: comparison_per_decade - len(comparison)]
    if (
        len(candidate_zoning) != candidate_zoning_per_decade
        or len(candidate_other) != candidate_other_per_decade
        or len(comparison) + len(comparison_overlap) != comparison_per_decade
    ):
        raise RuntimeError(f"Insufficient event-validation strata in {decade}.")
    for row in candidate_zoning:
        row["selection_stratum"] = "candidate_zoning"
    for row in candidate_other:
        row["selection_stratum"] = "candidate_other"
    for row in comparison:
        row["selection_stratum"] = "comparison"
    for row in comparison_overlap:
        row["selection_stratum"] = "comparison_candidate_overlap"
    selected_rows.extend(
        candidate_zoning + candidate_other + comparison + comparison_overlap
    )

selected_rows.sort(
    key=lambda row: (int(row["official_vote_year"]), row["_rank"])
)
for review_number, row in enumerate(selected_rows, start=1):
    row["review_id"] = f"EV{review_number:03d}"
    row["decade"] = row["_decade"]
    row["candidate_rule_flag"] = str(row["_candidate"]).upper()
    row["selection_rule_version"] = "official-event-validation-v1"

fieldnames = [
    "review_id",
    "document_id",
    "application_number",
    "action_code",
    "official_project_name",
    "official_community_district",
    "official_vote_date",
    "official_vote_year",
    "decade",
    "selection_stratum",
    "candidate_rule_flag",
    "selection_rule_version",
    "source_text_sha256",
    "narrative_sha256",
    "narrative_word_count",
    "official_pdf_url",
    "local_pdf_path",
    "local_text_path",
    "narrative_end_char",
]
with Path("../output/official_ulurp_cpc_event_validation_sample.csv").open(
    "w", newline="", encoding="utf-8"
) as output_file:
    writer = csv.DictWriter(output_file, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(selected_rows)

print(f"Wrote {len(selected_rows)} official narrative validation rows.")
