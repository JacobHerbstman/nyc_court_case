#!/usr/bin/env python3
"""Keep topic stages separate and compare positive recovery on fixed reports."""

import csv
import hashlib
import json
import math
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# Run from tasks/audits/pilot_ulurp_cpc_llm_labels/code.
codebook = json.loads(Path("cpc_jev_codebook_v4.json").read_text())
with open("../output/cpc_jev_sample_v4.csv", newline="") as source:
    roster = list(csv.DictReader(source))
with open("../output/cpc_jev_passages_v4.csv", newline="") as source:
    passages = list(csv.DictReader(source))
with open("../input/ulurp_cpc_reconciled_coding.csv", newline="") as source:
    humans = list(csv.DictReader(source))
with open("../input/ulurp_cpc_narrative_sources.csv", newline="") as source:
    source_links = list(csv.DictReader(source))
with open("../output/cpc_jev_comparison_v3.csv", newline="") as source:
    old_rows = list(csv.DictReader(source))
with open("../output/cpc_jev_comparison_v1.csv", newline="") as source:
    first_rows = list(csv.DictReader(source))
with open("../output/cpc_jev_labels_v2.csv", newline="") as source:
    repeats = list(csv.DictReader(source))
assert len(roster) == len({r["document_id"] for r in roster})
assert len(passages) == len({(r["document_id"], r["passage_id"]) for r in passages})
assert len(humans) == len({(r["source_document_id"], r["field"]) for r in humans})
assert len(old_rows) == len({(r["document_id"], r["field"]) for r in old_rows})
assert len(first_rows) == len({(r["document_id"], r["field"]) for r in first_rows})
assert len(source_links) == len({(r["document_id"], r["source_document_id"]) for r in source_links})
source_link = {(r["document_id"], r["source_document_id"]): r for r in source_links}
sample = [r for r in roster if r["cohort"] != "not_selected"]
passage = {(r["document_id"], r["passage_id"]): r for r in passages}
human = {(r["source_document_id"], r["field"]): r for r in humans}
old = {(r["document_id"], r["field"]): r for r in old_rows}
earlier = {(r["document_id"], r["field"]): (r["jev_value"], "v1") for r in first_rows if r["report_status"] == "whole_bundle"}
for r in repeats:
    if r["repeat"] == "1" and r["question"].startswith("original_"):
        earlier[r["document_id"], r["question"].removeprefix("original_")] = (r["value"], "v2_first_original")

prepared = Path("../output/cpc_jev_requests_v4.jsonl").read_bytes()
assert prepared == Path("../input/cpc_jev_requests_v4.jsonl").read_bytes()
packets = [json.loads(s) for s in prepared.decode().splitlines()]
assert len(packets) == len({p["request_id"] for p in packets}) == len(sample)
assert {p["document_id"] for p in packets} == {r["document_id"] for r in sample}
requests = {p["request_id"]: p for p in packets}
for p in packets:
    assert hashlib.sha256(json.dumps(p["body"], sort_keys=True, ensure_ascii=False).encode()).hexdigest() == p["request_sha256"]
attempts = {}
for line in Path("../input/cpc_jev_responses_v4.jsonl").read_text().splitlines():
    r = json.loads(line)
    assert r["request_sha256"] == requests[r["request_id"]]["request_sha256"] and r["attempt_number"] == 1
    assert r["document_id"] == requests[r["request_id"]]["document_id"]
    if r["event"] == "started":
        assert r["request_id"] not in attempts, "No repeated requests in this experiment."
        attempts[r["request_id"]] = dict(r, status="unfinished")
    else:
        assert r["event"] == "received" and attempts[r["request_id"]]["status"] == "unfinished"
        attempts[r["request_id"]].update(r, status="successful" if r["valid_response"] else "failed")
assert all(r["status"] != "unfinished" for r in attempts.values())
answers, usage = {}, []
for request_id, r in attempts.items():
    try:
        payload = json.loads(r["raw_response"])
    except json.JSONDecodeError:
        assert not r["valid_response"]
        payload = {}
    tokens = payload.get("usage", {})
    usage.append(dict(request_id=request_id, status=r["status"], http_status=r["http_status"],
        input_tokens=tokens.get("input_tokens"), output_tokens=tokens.get("output_tokens"), returned_model=payload.get("model", ""),
        started_at=r["started_at"], received_at=r["received_at"], request_sha256=r["request_sha256"]))
    if not r["valid_response"]:
        continue
    assert r["http_status"] == 200 and set(payload["answers"]) == set(requests[request_id]["body"]["questions"])
    for field, a in payload["answers"].items():
        options = requests[request_id]["body"]["questions"][field]["criteria"]
        assert a["type"] == "choice" and a["choice"] in options and set(a["probabilities"]) == set(options)
        assert all(math.isfinite(v) and 0 <= v <= 1 for v in a["probabilities"].values())
        assert abs(sum(a["probabilities"].values()) - 1) < .03 and math.isfinite(a["confidence"]) and 0 <= a["confidence"] <= 1
        answers[r["document_id"], field] = dict(a, request_sha256=r["request_sha256"])
credits = [json.loads(s) for s in Path("../input/cpc_jev_credits_v4.jsonl").read_text().splitlines()]
assert credits[-1]["phase"] == "after"
charge = Decimal(str(credits[-1]["total_used"])) - Decimal(str(credits[0]["total_used"]))


def any_positive(values):
    return "1" if "1" in values else "0" if all(v == "0" for v in values) else ""


evidence, comparison = [], []
for r in sample:
    doc, values = r["document_id"], {}
    for topic in codebook["topics"]:
        for stage in codebook["stages"]:
            field = f"{topic}__{stage}"
            a = answers.get((doc, field), {})
            choice = a.get("choice", "")
            p = passage.get((doc, choice), {})
            if choice not in {"", "none"}:
                assert p and hashlib.sha256(p["passage_text"].encode()).hexdigest() == p["passage_sha256"]
            value = "" if not a else "0" if choice == "none" else "1"
            source_projects, focal_projects, source_relation = set(), set(), "no_selected_source"
            if p:
                link = source_link[doc, p["source_document_id"]]
                focal = source_link[doc, doc]
                assert link["source_text_sha256"] == p["source_text_sha256"]
                source_projects = {x.strip() for x in link["source_zap_project_ids"].split(";") if x.strip()}
                focal_projects = {x.strip() for x in focal["source_zap_project_ids"].split(";") if x.strip()}
                source_relation = "focal_report" if p["source_document_id"] == doc else "zap_unresolved" if not source_projects or not focal_projects else "shared_zap_project" if source_projects & focal_projects else "disjoint_zap_projects_review"
            values[topic, stage] = value
            evidence.append(dict(document_id=doc, application_number=r["application_number"], cohort=r["cohort"], topic=topic, stage=stage,
                value=value, evidence_status="unavailable" if not a else "no_passage_selected" if choice == "none" else "selected_candidate_not_independently_verified",
                selected_passage_id=choice, source_document_id=p.get("source_document_id", ""),
                source_application_number=p.get("source_application_number", ""), source_role=p.get("source_role", ""),
                pdf_page=p.get("pdf_page", ""), page_start=p.get("page_start", ""), page_end=p.get("page_end", ""),
                passage_text=p.get("passage_text", ""), passage_sha256=p.get("passage_sha256", ""), source_text_sha256=p.get("source_text_sha256", ""),
                source_zap_projects=";".join(sorted(source_projects)), focal_zap_projects=";".join(sorted(focal_projects)),
                source_relation=source_relation, confidence=a.get("confidence", ""), request_sha256=a.get("request_sha256", "")))
    for field, details in codebook["legacy_topics"].items():
        h, previous = human.get((doc, field), {}), old.get((doc, field), {})
        mapped = {stage: any_positive([values[d, stage] for d in details]) for stage in codebook["stages"]}
        review_issue = any_positive([mapped["concern_request"], mapped["adopted"]])
        presence = any_positive(list(mapped.values()))
        contradictions = [d for d in details if values[d, "discussed"] == "0" and any(values[d, stage] == "1" for stage in ("concern_request", "adopted"))]
        prior_value, prior_version = earlier.get((doc, field), ("", ""))
        comparison.append(dict(document_id=doc, application_number=r["application_number"], cohort=r["cohort"], field=field,
            discussed=presence, discussed_direct=mapped["discussed"], concern_request=mapped["concern_request"], adopted=mapped["adopted"],
            review_issue=review_issue, contradiction_details=";".join(contradictions),
            v3_checked=previous.get("value", ""), v3_candidate=previous.get("candidate_value", ""), earlier_jev=prior_value, earlier_version=prior_version,
            original_human_value=h.get("human_value", ""),
            original_human_eligible=h.get("human_status") in {"human_agreement", "single_human_coder"} and h.get("human_value") in {"0", "1"},
            working_value=h.get("reconciled_value", ""), working_eligible=h.get("reconciled_value") in {"0", "1"},
            jacob_value=h.get("jacob_value", ""), jacob_complete=h.get("jacob_coding_complete", ""),
            tyler_value=h.get("tyler_value", ""), tyler_complete=h.get("tyler_coding_complete", ""),
            human_status=h.get("human_status", ""), reconciliation_provenance=h.get("reconciliation_provenance", "")))

agreement = []
for cohort in ("prior_v3", "additional_topics", "all"):
    for field in codebook["legacy_topics"]:
        for reference in ("original_human", "working"):
            rows = [r for r in comparison if (cohort == "all" or r["cohort"] == cohort) and r["field"] == field and r[reference + "_eligible"]]
            truth = reference + "_value"
            for method in ("discussed", "concern_request", "adopted", "review_issue", "v3_checked", "v3_candidate", "earlier_jev"):
                available = [r for r in rows if r[method] in {"0", "1"}]
                paired = [r for r in available if r["review_issue"] in {"0", "1"} and r["v3_checked"] in {"0", "1"}]
                agreement.append(dict(cohort=cohort, field=field, reference=reference, method=method, eligible=len(rows), available=len(available),
                    matches=sum(r[method] == r[truth] for r in available), reference_positive=sum(r[truth] == "1" for r in available),
                    found_positive=sum(r[method] == r[truth] == "1" for r in available),
                    missed_positive=sum(r[method] == "0" and r[truth] == "1" for r in available),
                    extra_positive=sum(r[method] == "1" and r[truth] == "0" for r in available),
                    missing_positive=sum(r[method] not in {"0", "1"} and r[truth] == "1" for r in rows),
                    paired_available=len(paired), paired_reference_positive=sum(r[truth] == "1" for r in paired),
                    paired_found_positive=sum(r[method] == r[truth] == "1" for r in paired),
                    paired_extra_positive=sum(r[method] == "1" and r[truth] == "0" for r in paired),
                    paired_matches=sum(r[method] == r[truth] for r in paired)))
save_csv(evidence, list(evidence[0]), "../output/cpc_jev_evidence_v4.csv", ["document_id", "topic", "stage"])
save_csv(usage, list(usage[0]), "../output/cpc_jev_usage_v4.csv", ["request_id"])
save_csv(comparison, list(comparison[0]), "../output/cpc_jev_comparison_v4.csv", ["document_id", "field"])
save_csv(agreement, list(agreement[0]), "../output/cpc_jev_agreement_v4.csv", ["cohort", "field", "reference", "method"])

names = {"affordability_displacement": "Affordability / displacement", "traffic_parking": "Traffic / parking",
    "infrastructure_services": "Infrastructure / services", "environment_open_space": "Environment / open space", "scale_character_preservation": "Character / scale / preservation"}
lines = ["# Three-stage topic test", "",
    f"{len(sample)} reports were selected from the prior 46 whole bundles: 20 previous v3 reports and ten additional already-coded reports. The additional reports increase positive-topic coverage and share no source with those already selected. They were not used in the v3 passage test, but appeared in the earlier v2 experiment; they are an additional development comparison, not an untouched holdout or a representative sample. The roster retains all 46 candidates with selection reasons. No corpus or ZAP restriction changes.", "",
    f"{len(usage)} distinct attempts returned {sum(r['status'] == 'successful' for r in usage)} successful report responses and {sum(r['status'] == 'failed' for r in usage)} failures. Each request asked 24 questions: eight topic details times discussion, concern/request, and adopted commitment. No identical request was repeated. Observed account usage increased by ${charge}; the last balance was ${credits[-1]['balance']}. Returned token usage totals {sum(r['input_tokens'] or 0 for r in usage):,} input and {sum(r['output_tokens'] or 0 for r in usage):,} output tokens.", "",
    "Every positive selects a verbatim source passage; the selection is a model judgment, not independently validated evidence. Unlike v3, a second model does not reject a selected candidate. Comparing with v3 candidates as well as checked v3 answers distinguishes removal of that check from new question wording. Passage text is unchanged and all pages remain available to each question.", "",
    "The mapping was fixed before responses: combine topic details in code; a legacy review-issue candidate is concern/request OR adopted commitment. Discussion alone is retained separately and is not counted as a recovered concern. An adopted commitment can predate review and is not automatically a concession caused by opposition. Original Jacob, original Tyler, nonconflicting original, and AI-assisted working references remain distinct; original labels are never overwritten.", "",
    "## Same cases: positive recovery against original codes", "",
    "Only cases with comparable original codes and answers from both v3 and v4 enter this table. Found is recovered original positives / available original positives; extra is a positive model label against an original zero. Neither treats the reference as established truth.", "",
    "| Topic | v3 checked found | v3 candidate found | v4 issue found | Extra: checked / candidate / v4 |", "|---|---:|---:|---:|---:|"]
for field in codebook["legacy_topics"]:
    group = {r["method"]: r for r in agreement if r["cohort"] == "prior_v3" and r["field"] == field and r["reference"] == "original_human"}
    methods = ("v3_checked", "v3_candidate", "review_issue")
    lines.append("| " + names[field] + " | " + " | ".join(f"{group[m]['paired_found_positive']}/{group[m]['paired_reference_positive']}" for m in methods) + " | " + " / ".join(str(group[m]["paired_extra_positive"]) for m in methods) + " |")
for cohort, title in (("additional_topics", "Additional reports"), ("all", "All answered reports")):
    lines += ["", "## " + title, "", "Review issue is concern/request OR adopted commitment. Missing readings remain outside answered denominators and are reported separately.", "",
        "| Topic | Positives found | Extra yes | Matches | Missing |", "|---|---:|---:|---:|---:|"]
    for r in agreement:
        if r["cohort"] == cohort and r["reference"] == "original_human" and r["method"] == "review_issue":
            lines.append(f"| {names[r['field']]} | {r['found_positive']}/{r['reference_positive']} | {r['extra_positive']} | {r['matches']}/{r['available']} | {r['eligible']-r['available']} |")
lines += ["", "## Stages are different measurements", "", "Counts below use all selected reports, including fields with no eligible original benchmark. A later stage implies discussion in the derived union, while raw disagreement remains flagged.", "",
    "| Topic | Discussed | Concern / request | Adopted | Review issue |", "|---|---:|---:|---:|---:|"]
for field in codebook["legacy_topics"]:
    rs = [r for r in comparison if r["field"] == field]
    lines.append("| " + names[field] + " | " + " | ".join(str(sum(r[m] == "1" for r in rs)) for m in ("discussed", "concern_request", "adopted", "review_issue")) + " |")
scope_flags = [r for r in evidence if r["source_relation"] == "disjoint_zap_projects_review" and r["stage"] != "discussed"]
lines += ["", "## Source applicability diagnostic", "",
    f"{len(scope_flags)} positive concern/adoption selections use a companion whose recorded ZAP project IDs do not overlap the focal report's IDs. This post-run flag does not prove every link is wrong and does not filter any model code. It identifies evidence requiring an explicit same-project justification. The C 930227 PPQ example in the logbook illustrates a confirmed mismatch; the corpus and source bundles remain preserved.", "",
    "| Focal application | Selected source | Topic / stage |", "|---|---|---|"]
for r in scope_flags:
    lines.append(f"| {r['application_number']} | {r['source_application_number']} | {r['topic'].replace('_', ' ')} / {r['stage'].replace('_', ' ')} |")
lines += ["", f"There are {sum(bool(r['contradiction_details']) for r in comparison)} report/topic rows with a raw discussion-negative detail but a positive later stage. Raw selections are preserved. Grouping fine topics by a fixed union avoids independently asking a broad question that can contradict its details; it does not guarantee that the selected text qualifies.", "",
    "The generated evidence CSV retains all report/topic/stage rows, including service failures. It records exact source/page/offset/hash and the model selection score. The score is not calibrated accuracy. Prompts, raw responses, and before/checkpoint/after account observations are archived. Make rebuilds from those saved observations without inference calls. No counts, actor-position fields, original coding, or production labels are replaced.", "",
    "Jev's [question documentation](https://docs.typesafe.ai/primitives) says each question is evaluated independently; mappings therefore run in code. The [Vercel listing](https://vercel.com/ai-gateway/models/jev) advertised free pricing through September 25, 2026 when checked September 19. Observed charges above are account measurements, not a per-request invoice."]
Path("../output/cpc_jev_findings_v4.md").write_text("\n".join(lines) + "\n")
print(f"Compared {len(sample)} reports, {len(evidence)} topic/stage rows; observed charge ${charge}.")
