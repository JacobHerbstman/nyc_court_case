#!/usr/bin/env python3

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/audit_official_ulurp_cpc_text_measurement/code")
# start_year = 1975
# end_year = 2025

import csv
import hashlib
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from cpc_narratives import normalize_narrative
from data_reports import save_csv


def application_key(value):
    return re.sub(r"[^A-Z0-9]", "", value.upper())


def project_key(value):
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


if len(sys.argv) != 3:
    raise ValueError("Expected start_year end_year")
start_year, end_year = map(int, sys.argv[1:])
if start_year > end_year:
    raise ValueError("Start year exceeds end year")

with open("../input/official_cpc_report_index.csv", newline="") as stream:
    index_rows = [r for r in csv.DictReader(stream) if start_year <= int(r["vote_year"]) <= end_year]
with open("../input/official_ulurp_cpc_report_manifest.csv", newline="") as stream:
    corpus_rows = [r for r in csv.DictReader(stream) if start_year <= int(r["official_vote_year"]) <= end_year]
with open("../output/official_ulurp_cpc_narrative_manifest.csv", newline="") as stream:
    narrative_rows = list(csv.DictReader(stream))
with open("../input/ulurp_cpc_text_labels.csv", newline="") as stream:
    labels = [r for r in csv.DictReader(stream) if start_year <= int(r["year"]) <= end_year]
with open("../input/ulurp_cpc_narrative_sources.csv", newline="") as stream:
    source_links = list(csv.DictReader(stream))
with open("../input/ulurp_cpc_companion_reports.csv", newline="") as stream:
    verified_links = list(csv.DictReader(stream))
with open("../input/cpc_selection_reviews.csv", newline="") as stream:
    reviews = list(csv.DictReader(stream))
assert len(reviews) == len({r["review_id"] for r in reviews})

corpus = {r["document_id"]: r for r in corpus_rows}
narratives = {r["document_id"]: r for r in narrative_rows}
label_by_id = {r["document_id"]: r for r in labels}
assert len(corpus) == len(corpus_rows) == len(narratives) == len(narrative_rows)
assert len(label_by_id) == len(labels)
assert set(corpus) == set(narratives)
assert set(label_by_id) == {r["document_id"] for r in narrative_rows if r["analysis_narrative_unit_flag"] == "TRUE"}
for document_id, label in label_by_id.items():
    assert label["narrative_sha256"] == narratives[document_id]["narrative_sha256"]

indexed_keys = {application_key(r["application_number"]) for r in index_rows}
included_keys = {application_key(r["application_number"]) for r in corpus_rows}
included_index_keys = {application_key(r["official_index_application_number"]) for r in corpus_rows
                       if r["official_index_application_number"]}
certified = [r for r in corpus_rows if r["corpus_role"] == "certified_ulurp_report"]
certified_project_votes = {
    (project_key(r["official_project_name"]), r["official_vote_date"])
    for r in certified if project_key(r["official_project_name"])
}
certified_project_names = {name for name, date in certified_project_votes}
excluded_n = [r for r in index_rows if application_key(r["application_number"]).startswith("N")
              and application_key(r["application_number"]) not in included_index_keys]

# Keep every explicit N reference, including identifiers absent from the index.
# The nearby-language flag retrieves candidates; it does not decide inclusion.
n_reference = re.compile(r"\bN\s*\d{6}(?:\s*\([A-Z]\)|[A-Z])?\s*[A-Z]{2,4}\s*[A-Z]\b")
referral_language = re.compile(
    r"full (?:background|description)|more detailed|complete discussion|"
    r"summary of the arguments|testimony is summarized|further background|"
    r"all set forth|detailed (?:discussion|description|analysis)", re.I
)
references = []
text_by_id = {}
for row in narrative_rows:
    if row["source_usable"] != "TRUE":
        continue
    text = Path(row["local_text_path"]).read_text(encoding="utf-8", errors="replace")
    assert hashlib.sha256(text.encode()).hexdigest() == row["source_text_sha256"]
    text_by_id[row["document_id"]] = text
    if row["corpus_role"] != "certified_ulurp_report":
        continue
    matches = defaultdict(list)
    for match in n_reference.finditer(text):
        key = application_key(match.group())
        context = re.sub(r"\s+", " ", text[max(0, match.start()-400):match.end()+500]).strip()
        matches[key].append((text.count("\f", 0, match.start())+1, context))
    for key, occurrences in sorted(matches.items()):
        contexts = sorted(set(context for page, context in occurrences))
        nearby = [context for context in contexts if referral_language.search(context)]
        references.append({
            "c_application_number": row["application_number"],
            "n_application_key": key,
            "n_source_status": "included" if key in included_keys | included_index_keys else "excluded_indexed" if key in indexed_keys else "not_in_index",
            "c_pdf_url": row["official_pdf_url"],
            "c_source_text_sha256": row["source_text_sha256"],
            "pdf_pages": "; ".join(map(str, sorted({page for page, context in occurrences}))),
            "occurrence_count": len(occurrences),
            "nearby_referral_language_flag": bool(nearby),
            "example_context": (nearby or contexts)[0],
        })

# Check that every retained text and metadata value is traceable to source rows.
assert len(source_links) == len({(r["document_id"], r["source_document_id"]) for r in source_links})
links_by_document = defaultdict(list)
represented_source_ids = set()
for link in source_links:
    assert link["document_id"] in label_by_id
    source = corpus[link["source_document_id"]]
    narrative = narratives[link["source_document_id"]]
    for output_field, input_field in (
        ("source_application_number", "application_number"),
        ("source_project_name", "official_project_name"),
        ("source_corpus_role", "corpus_role"),
        ("source_zap_project_ids", "zap_project_ids"),
        ("source_action_code", "action_code"),
        ("source_community_district", "official_community_district"),
        ("source_vote_date", "official_vote_date"),
    ):
        assert link[output_field] == source[input_field], "Source metadata was changed in the link table"
    assert link["source_text_sha256"] == narrative["source_text_sha256"]
    assert link["source_narrative_sha256"] == narrative["narrative_sha256"]
    links_by_document[link["document_id"]].append(link)
    if link["represented_application_flag"] == "TRUE":
        represented_source_ids.add(link["source_document_id"])
for document_id, label in label_by_id.items():
    links = links_by_document[document_id]
    represented = [r for r in links if r["represented_application_flag"] == "TRUE"]
    assert sum(r["link_role"] == "focal_report" for r in represented) == 1
    assert set(label["represented_application_numbers"].split("; ")) == {r["source_application_number"] for r in represented}
    assert label["represented_action_codes"] == "; ".join(sorted({r["source_action_code"] for r in represented}))
    assert label["represented_community_districts"] == "; ".join(sorted({r["source_analysis_community_district"] for r in represented if r["source_analysis_community_district"]}))
    project_ids = {value.strip() for r in represented for value in r["source_zap_project_ids"].split(";") if value.strip()}
    assert {value.strip() for value in label["zap_project_ids"].split(";") if value.strip()} == project_ids
    for field in ("analysis_non_pp_flag", "analysis_zm_zr_zs_flag"):
        assert (label[field] == "TRUE") == any(narratives[r["source_document_id"]][field] == "TRUE" for r in represented)
    included = sorted((r for r in links if r["text_included_flag"] == "TRUE"), key=lambda r: int(r["analysis_text_order"]))
    assert [int(r["analysis_text_order"]) for r in included] == list(range(1, len(included)+1))
    assert len(included) == len({r["source_narrative_sha256"] for r in included})
    reconstructed = "\n\n".join(text_by_id[r["source_document_id"]][:int(narratives[r["source_document_id"]]["narrative_end_char"])] for r in included)
    assert hashlib.sha256(normalize_narrative(reconstructed).encode()).hexdigest() == label["analysis_text_sha256"]
    applications = {r["source_application_number"] for r in links}
    for verified in verified_links:
        if verified["certified_application_number"] in applications:
            assert verified["companion_application_number"] in applications, "A verified N source was lost"
for row in narrative_rows:
    if row["analysis_narrative_unit_reason"] in {
        "included_unique_narrative", "exact_duplicate_narrative",
        "related_action_of_designated_lead", "related_action_of_manual_companion",
    }:
        assert row["document_id"] in represented_source_ids, "A collapsed application lost its metadata link"

reviewed_refs = defaultdict(list)
reference_pairs = {(r["c_application_number"], r["n_application_key"]) for r in references}
for review in reviews:
    source = next(r for r in narrative_rows if r["application_number"] == review["c_application_number"])
    assert source["source_text_sha256"] == review["source_text_sha256"], "Stale manual source review"
    if review["review_type"] == "n_referral":
        assert (review["c_application_number"], review["n_application_key"]) in reference_pairs
        reviewed_refs[review["n_application_key"]].append(review)

refs_by_n = defaultdict(list)
for row in references:
    refs_by_n[row["n_application_key"]].append(row)
excluded_rows = []
for row in excluded_n:
    key = application_key(row["application_number"])
    refs = refs_by_n[key]
    excluded_rows.append({
        "application_number": row["application_number"], "vote_date": row["vote_date"],
        "pdf_url": row["pdf_url"], "project_name": row["project_name"],
        "lead_report_flag": row["lead_report_flag"],
        "same_project_and_vote_as_certified_flag": (project_key(row["project_name"]), row["vote_date"]) in certified_project_votes,
        "same_project_name_as_certified_flag": project_key(row["project_name"]) in certified_project_names,
        "referencing_certified_reports": len(refs),
        "nearby_referral_language_flag": any(r["nearby_referral_language_flag"] for r in refs),
        "review_decisions": "; ".join(sorted({r["decision"] for r in reviewed_refs[key]})),
        "reviewed_c_applications": "; ".join(sorted({r["c_application_number"] for r in reviewed_refs[key]})),
    })
save_csv(excluded_rows, list(excluded_rows[0]), "../output/cpc_excluded_n_reports.csv",
         ["application_number", "vote_date", "pdf_url"])
save_csv(references, list(references[0]), "../output/cpc_n_report_references.csv",
         ["c_application_number", "n_application_key"])

groups = defaultdict(list)
for row in narrative_rows:
    if row["analysis_narrative_unit_reason"] in {"included_unique_narrative", "exact_duplicate_narrative"}:
        groups[row["narrative_sha256"]].append(row)
duplicate_rows = []
for narrative_hash, members in sorted(groups.items()):
    if len(members) < 2:
        continue
    representatives = [r for r in members if r["analysis_narrative_unit_flag"] == "TRUE"]
    assert len(representatives) == 1
    representative = representatives[0]
    label = label_by_id[representative["document_id"]]
    prefixes = [text_by_id[r["document_id"]][:int(r["narrative_end_char"])] for r in members]
    assert len({normalize_narrative(text) for text in prefixes}) == 1, "Hash collision or mismatched narrative"
    project_ids = {value.strip() for r in members for value in corpus[r["document_id"]]["zap_project_ids"].split(";") if value.strip()}
    kept_project_ids = {value.strip() for value in label["zap_project_ids"].split(";") if value.strip()}
    all_non_pp = any(r["analysis_non_pp_flag"] == "TRUE" for r in members)
    duplicate_rows.append({
        "narrative_sha256": narrative_hash,
        "representative_application": representative["application_number"],
        "member_applications": "; ".join(sorted(r["application_number"] for r in members)),
        "source_rows": len(members), "rows_collapsed": len(members)-1,
        "distinct_full_extracted_texts": len({r["source_text_sha256"] for r in members}),
        "distinct_unnormalized_narrative_prefixes": len(set(prefixes)),
        "distinct_vote_dates": len({r["official_vote_date"] for r in members}),
        "distinct_project_names": len({r["official_project_name"] for r in members}),
        "distinct_action_codes": len({r["action_code"] for r in members}),
        "distinct_community_district_strings": len({r["official_community_district"] for r in members}),
        "all_member_zap_project_ids": "; ".join(sorted(project_ids)),
        "representative_zap_project_ids": "; ".join(sorted(kept_project_ids)),
        "omitted_zap_project_ids": "; ".join(sorted(project_ids-kept_project_ids)),
        "representative_non_pp_flag": label["analysis_non_pp_flag"],
        "any_member_non_pp_flag": str(all_non_pp).upper(),
        "non_pp_classification_disagrees": (label["analysis_non_pp_flag"] == "TRUE") != all_non_pp,
    })
save_csv(duplicate_rows, list(duplicate_rows[0]), "../output/cpc_duplicate_narrative_groups.csv", ["narrative_sha256"])

reviewed_differences = {r["c_application_number"] for r in reviews if r["decision"] == "same_narrative_different_attachment"}
assert {r["representative_application"] for r in duplicate_rows if r["distinct_full_extracted_texts"] > 1} <= reviewed_differences, "New full-text differences require review"

confirmed = {r["n_application_key"] for r in reviews if r["decision"].startswith("explicit_")}
recovered = confirmed & included_keys
assert recovered == confirmed, "A reviewed N companion is still excluded"
verified_text_sources = {r["source_application_number"] for r in source_links
                         if r["source_corpus_role"] == "related_project_narrative_companion" and r["text_included_flag"] == "TRUE"}
assert verified_text_sources == {r["companion_application_number"] for r in verified_links}
augmented_documents = {r["document_id"] for r in source_links if r["link_role"] == "verified_n_companion" and r["text_included_flag"] == "TRUE"}
excluded_referenced = {r["n_application_key"] for r in references if r["n_source_status"] == "excluded_indexed"}
unindexed_referenced = {r["n_application_key"] for r in references if r["n_source_status"] == "not_in_index"}
findings = f"""# CPC report selection audit

The current {start_year}–{end_year} corpus has {len(corpus_rows):,} source rows and {len(labels):,} retained narratives. The revised builder preserves application metadata and adds verified N companions to existing narratives. This audit checks that the source links reconstruct the saved text and metadata; it does not establish complete historical coverage.

## Excluded N reports

There are {len(excluded_rows):,} excluded indexed N rows. Of these, {sum(r['same_project_and_vote_as_certified_flag'] for r in excluded_rows):,} share a normalized project name and vote date with a certified report. Only {sum(r['lead_report_flag'] == 'TRUE' for r in excluded_rows)} excluded N rows have the official lead flag. Absence of that flag is not evidence that a report lacks useful narrative: DCP documents the flag only for projects since July 2003.

Certified reports explicitly mention {len(excluded_referenced):,} distinct excluded indexed N identifiers. They also mention {len(unindexed_referenced):,} N identifiers absent from the saved index; these include possible OCR errors, amendments, other years, and genuinely unavailable reports. A reference alone does not establish a common project or a missing lead.

The prior review identified {len(confirmed)} excluded N reports with explicit referrals from certified reports. All {len(recovered)} are now in the corpus and contribute text to {len(augmented_documents)} existing narratives. These sources supply background, hearing discussion, consideration, or related-action details. They do not enter as new independent observations or join otherwise separate cases. Other possible referrals remain unreviewed. This is a bounded recovery of verified sources, not a complete count of historically missing narratives.

## Repeated narratives

The {len(duplicate_rows):,} duplicate groups contain {sum(r['source_rows'] for r in duplicate_rows):,} source rows; choosing one per group removes {sum(r['rows_collapsed'] for r in duplicate_rows):,} rows. There are {sum(r['distinct_full_extracted_texts'] == 1 for r in duplicate_rows):,} groups with identical full extracted text and {sum(r['distinct_unnormalized_narrative_prefixes'] == 1 for r in duplicate_rows):,} with identical narrative prefixes even before normalization. No hash collision or unequal normalized narrative was found.

The four groups with different full texts differ after the narrative boundary: three have different agency attachments; one includes a separate appended report (C 790410 HDQ), which is independently present in the corpus. This supports counting the repeated narrative once, but does not make the underlying applications or projects interchangeable.

The retained label rows now omit member ZAP IDs in {sum(bool(r['omitted_zap_project_ids']) for r in duplicate_rows):,} duplicate groups and disagree with the union of member non-PP flags in {sum(r['non_pp_classification_disagrees'] for r in duplicate_rows)} groups. Before correction, these counts were 380 and 4. The source table preserves each application's original project IDs, action, district, date, and text hashes. Geography combines represented applications after applying recorded district corrections. One group still has inconsistent indexed dates (May 17 versus May 18, 1989); both originals are retained in the source table. It has no effect on the annual assignment.

## Preservation checks and remaining scope

The link table has {len(source_links):,} unique narrative/source pairs. Every application collapsed into a lead or exact duplicate retains a represented-application link. Other contextual sources are kept separately from represented applications, so they supply text without automatically changing the focal narrative's action scope or geography. Every included text is stored once per narrative, with its order recorded, and all analysis-text hashes reconstruct exactly from the linked source excerpts.

The narrative audit and label producer now use the same boundary function. All {len(labels):,} retained narrative identifiers and hashes agree. The previous audit used an older boundary rule; identical membership counts did not establish identical text.

Source: [DCP CPC archive notes](https://a030-cpc.nyc.gov/html/cpc/index.aspx), inspected September 14, 2026. The archive also warns of unavailable zoning-text reports from 1987–2003 and imperfect OCR in older scans. Explicit-reference searches cannot prove that nothing is missing.
"""
Path("../output/cpc_selection_findings.md").write_text(findings, encoding="utf-8")
print(f"Audited {len(excluded_rows)} excluded N rows, {len(references)} C-to-N references, and {len(duplicate_rows)} duplicate groups.")
