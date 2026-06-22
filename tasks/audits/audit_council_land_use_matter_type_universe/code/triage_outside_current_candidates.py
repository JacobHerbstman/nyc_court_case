# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_council_land_use_matter_type_universe/code")

import csv
import glob
import re


def contains_any(text, terms):
    return any(term in text for term in terms)


def normalize_application(value):
    return re.sub(r"\s+", "", value.upper())


with open("../output/council_land_use_outside_current_type_candidates.csv", newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

current_recall_rows = []
for path in sorted(glob.glob("../input/legistar_*_broad_recall_matter_index.csv")):
    with open(path, newline="", encoding="utf-8") as f:
        current_recall_rows.extend(csv.DictReader(f))

current_recall_text = []
for row in current_recall_rows:
    current_recall_text.append(
        (
            row,
            normalize_application(
                " ".join(
                    [
                        row.get("application_numbers_in_title", ""),
                        row.get("title", ""),
                        row.get("detail_title", ""),
                    ]
                )
            ),
        )
    )

for row in rows:
    title = row["title"].lower()
    matter_type = row["matter_type"]
    status = row["status"]
    committee = row["committee"].lower()
    has_application = bool(row["application_numbers_in_title"].strip())
    adopted_or_enacted = status in {"Adopted", "Enacted"}

    row["project_application_signal"] = has_application
    row["final_action_status_signal"] = adopted_or_enacted

    if contains_any(
        title,
        [
            "submitting objection",
            "stating his objection",
            "stating her objection",
            "triple no",
            "veto and disapproval",
            "mayor's veto",
            "mayors veto",
            "disapproval message",
        ],
    ):
        row["triage_bucket"] = "land_use_adjacent_procedural_communication"
        row["triage_reason"] = "Has an application number, but the title is a borough-president objection, triple-no notice, or mayor veto/disapproval communication rather than a final Council project vote."
        row["possible_main_series_addition"] = False
    elif has_application and not adopted_or_enacted:
        row["triage_bucket"] = "land_use_adjacent_application_not_final"
        row["triage_reason"] = "Has an application number, but status is not Adopted or Enacted."
        row["possible_main_series_addition"] = False
    elif has_application and adopted_or_enacted:
        row["triage_bucket"] = "possible_project_level_addition"
        row["triage_reason"] = "Has an application number and final-action status; inspect manually before deciding whether it belongs in the member-deference series."
        row["possible_main_series_addition"] = True
    elif contains_any(title, ["change of members", "appointment of"]) and contains_any(
        title,
        [
            "subcommittee on permits",
            "subcommittee on zoning",
            "subcommittee on planning",
            "subcommittee on landmarks",
        ],
    ):
        row["triage_bucket"] = "not_project_vote_committee_administration"
        row["triage_reason"] = "Council committee/subcommittee membership administration, not a project-level land-use decision."
        row["possible_main_series_addition"] = False
    elif contains_any(title, ["city planning commission"]) and contains_any(
        title,
        [
            "appointment",
            "reappointment",
            "submitting the name",
            "advice and consent",
            "member of the city planning commission",
        ],
    ):
        row["triage_bucket"] = "not_project_vote_cpc_appointment"
        row["triage_reason"] = "CPC appointment or reappointment matter, not a project-level land-use decision."
        row["possible_main_series_addition"] = False
    elif matter_type == "Oversight" or title.startswith("oversight"):
        row["triage_bucket"] = "not_project_vote_oversight"
        row["triage_reason"] = "Oversight hearing or oversight calendar item, not a final project vote."
        row["possible_main_series_addition"] = False
    elif matter_type == "SLR" or "state and federal legislation" in committee or contains_any(
        title,
        [
            "state legislation",
            "state legislature",
            '"an act',
            "“an act",
            "an act to amend",
            "an act in relation",
        ],
    ):
        row["triage_bucket"] = "not_project_vote_state_legislation"
        row["triage_reason"] = "State legislation resolution, not a Council project-level land-use action."
        row["possible_main_series_addition"] = False
    elif contains_any(
        title,
        [
            "sidewalk cafe",
            "sidewalk café",
            "bus stop shelter",
            "bus stop shelters",
            "private bus",
            "bus services",
            "bus service",
            "newsstand",
            "newsstands",
            "franchise",
            "revocable consent",
            "unenclosed sidewalk",
            "enclosed sidewalk",
        ],
    ):
        row["triage_bucket"] = "not_project_vote_franchise_consent_or_street_use"
        row["triage_reason"] = "Franchise, consent, sidewalk, shelter, newsstand, or street-use matter, not comparable to project-level land-use approvals."
        row["possible_main_series_addition"] = False
    elif contains_any(
        title,
        [
            "landmark",
            "landmarks preservation",
            "historic district",
        ],
    ):
        row["triage_bucket"] = "probably_not_missing_landmark_related"
        row["triage_reason"] = "Landmark-related outside-current item; likely policy/procedural unless separately tied to an application-level Council action."
        row["possible_main_series_addition"] = False
    elif contains_any(
        title,
        [
            "local law",
            "new york city charter",
            "city charter",
            "administrative code",
            "general municipal law",
            "board of standards and appeals",
            "variance",
            "special permit",
            "zoning lot",
            "accessory sign",
            "outdoor advertising",
            "real property tax",
            "school tax relief",
            "assessment roll",
            "equalization",
            "banking commission",
            "non-payment of taxes",
            "tax surcharge",
            "tax abatement",
            "street names",
            "taxicab fares",
            "community board action",
            "land use review periods",
            "uniform land use review procedure",
        ],
    ):
        row["triage_bucket"] = "not_project_vote_policy_or_process_legislation"
        row["triage_reason"] = "Policy/process legislation mentioning land use, zoning, ULURP, BSA, or related procedures, not a site-specific final project vote."
        row["possible_main_series_addition"] = False
    elif contains_any(
        title,
        [
            "site selection",
            "acquisition",
            "disposition",
            "urban development action area",
            "udaap",
            "urban renewal",
            "real property",
            "community district",
            "borough of",
        ],
    ):
        row["triage_bucket"] = "possible_project_related_without_application"
        row["triage_reason"] = "Site-specific terms appear but no parsed application number; sample manually to check whether this is a true missing project action."
        row["possible_main_series_addition"] = False
    elif contains_any(
        title,
        [
            "zoning",
            "city map",
            "mapping",
            "demapping",
            "street closing",
            "street opening",
            "land use",
        ],
    ):
        row["triage_bucket"] = "possible_land_use_related_without_application"
        row["triage_reason"] = "Land-use vocabulary appears without an application number; sample manually for false positives and possible missing actions."
        row["possible_main_series_addition"] = False
    else:
        row["triage_bucket"] = "residual_manual_review"
        row["triage_reason"] = "Not clearly classified by deterministic text rules."
        row["possible_main_series_addition"] = False

    row["possible_main_series_addition"] = str(row["possible_main_series_addition"])
    row["project_application_signal"] = str(row["project_application_signal"])
    row["final_action_status_signal"] = str(row["final_action_status_signal"])


with open("../output/council_land_use_outside_current_type_candidate_triage.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=list(rows[0].keys()) if rows else [],
        extrasaction="ignore",
    )
    writer.writeheader()
    writer.writerows(rows)


summary = {}
for row in rows:
    key = (
        row["triage_bucket"],
        row["candidate_review_priority"],
        row["matter_type"],
        row["possible_main_series_addition"],
    )
    if key not in summary:
        summary[key] = {
            "triage_bucket": key[0],
            "candidate_review_priority": key[1],
            "matter_type": key[2],
            "possible_main_series_addition": key[3],
            "matter_count": 0,
            "first_year": int(row["query_year"]),
            "last_year": int(row["query_year"]),
        }
    summary[key]["matter_count"] += 1
    summary[key]["first_year"] = min(summary[key]["first_year"], int(row["query_year"]))
    summary[key]["last_year"] = max(summary[key]["last_year"], int(row["query_year"]))

with open("../output/council_land_use_outside_current_type_candidate_triage_summary.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "triage_bucket",
            "candidate_review_priority",
            "matter_type",
            "possible_main_series_addition",
            "matter_count",
            "first_year",
            "last_year",
        ],
    )
    writer.writeheader()
    writer.writerows(
        sorted(
            summary.values(),
            key=lambda row: (
                row["possible_main_series_addition"] != "True",
                row["triage_bucket"],
                -row["matter_count"],
                row["candidate_review_priority"],
                row["matter_type"],
            ),
        )
    )


sample_rows = []
bucket_counts = {}
for row in sorted(rows, key=lambda x: (x["triage_bucket"], int(x["query_year"]), x["matter_type"], x["matter_file"])):
    bucket = row["triage_bucket"]
    bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
    if bucket_counts[bucket] <= 20:
        sample_rows.append(row)

with open("../output/council_land_use_outside_current_type_candidate_triage_samples.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=list(rows[0].keys()) if rows else [],
        extrasaction="ignore",
    )
    writer.writeheader()
    writer.writerows(sample_rows)


crosswalk_rows = []
for row in rows:
    if not row["triage_bucket"].startswith("land_use_adjacent"):
        continue

    applications = [
        normalize_application(application)
        for application in row["application_numbers_in_title"].split(";")
        if normalize_application(application)
    ]
    matches = []
    for application in applications:
        for current_row, current_text in current_recall_text:
            if application in current_text:
                matches.append(current_row)

    unique_matches = []
    seen_matter_ids = set()
    for match in matches:
        if match["matter_id"] in seen_matter_ids:
            continue
        seen_matter_ids.add(match["matter_id"])
        unique_matches.append(match)

    crosswalk_rows.append(
        {
            "outside_matter_file": row["matter_file"],
            "outside_query_year": row["query_year"],
            "outside_matter_type": row["matter_type"],
            "outside_status": row["status"],
            "outside_triage_bucket": row["triage_bucket"],
            "outside_application_numbers_normalized": "; ".join(applications),
            "current_recall_match_count": len(unique_matches),
            "current_recall_match_files": "; ".join(match["matter_file"] for match in unique_matches),
            "current_recall_match_statuses": "; ".join(match["status"] for match in unique_matches),
            "current_recall_match_titles": " || ".join(match["title"] for match in unique_matches[:5]),
            "outside_title": row["title"],
        }
    )

with open("../output/council_land_use_adjacent_current_recall_crosswalk.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "outside_matter_file",
            "outside_query_year",
            "outside_matter_type",
            "outside_status",
            "outside_triage_bucket",
            "outside_application_numbers_normalized",
            "current_recall_match_count",
            "current_recall_match_files",
            "current_recall_match_statuses",
            "current_recall_match_titles",
            "outside_title",
        ],
    )
    writer.writeheader()
    writer.writerows(crosswalk_rows)
