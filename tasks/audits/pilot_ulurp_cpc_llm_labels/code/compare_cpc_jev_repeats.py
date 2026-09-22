#!/usr/bin/env python3
"""Separate repeat stability, wording votes, and human-reference agreement."""

import csv
import hashlib
import json
import math
import sys
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# Run from tasks/audits/pilot_ulurp_cpc_llm_labels/code.
with open("../output/cpc_jev_sample_v2.csv", newline="") as source:
    roster = list(csv.DictReader(source))
with open("../input/ulurp_cpc_reconciled_coding.csv", newline="") as source:
    humans = list(csv.DictReader(source))
with open("../output/cpc_jev_comparison_v1.csv", newline="") as source:
    previous = list(csv.DictReader(source))
assert len(roster) == len({r["document_id"] for r in roster})
assert len(humans) == len({(r["source_document_id"], r["field"]) for r in humans})
assert len(previous) == len({(r["document_id"], r["field"]) for r in previous})
human = {(r["source_document_id"], r["field"]): r for r in humans}
old = {(r["document_id"], r["field"]): r for r in previous}
sample = [r for r in roster if r["cohort"] != "not_selected"]
packets = [json.loads(s) for s in Path("../input/cpc_jev_requests_v2.jsonl").read_text().splitlines()]
assert Path("../input/cpc_jev_requests_v2.jsonl").read_bytes() == Path("../output/cpc_jev_requests_v2.jsonl").read_bytes()
assert len(packets) == len({p["request_id"] for p in packets}) == sum(int(r["expected_repeats"]) for r in sample)
requests = {p["request_id"]: p for p in packets}
for p in packets:
    assert hashlib.sha256(json.dumps(p["body"], sort_keys=True, ensure_ascii=False).encode()).hexdigest() == p["request_sha256"]
attempts, successful = {}, {}
for line in Path("../input/cpc_jev_responses_v2.jsonl").read_text().splitlines():
    r = json.loads(line)
    key = (r["request_id"], r["attempt_number"])
    assert r["request_sha256"] == requests[key[0]]["request_sha256"]
    assert r["document_id"] == requests[key[0]]["document_id"]
    if r["event"] == "started":
        assert key not in attempts
        attempts[key] = dict(r, status="unfinished")
    else:
        assert r["event"] == "received" and key in attempts and attempts[key]["status"] == "unfinished"
        attempts[key].update(r, status="successful" if r["valid_response"] else "failed")
        if r["valid_response"]:
            assert key[0] not in successful
            successful[key[0]] = r
assert all(r["status"] != "unfinished" for r in attempts.values()), "Do not analyze an unfinished acquisition."
labels, usage, answers = [], [], {}
for (request_id, attempt), r in attempts.items():
    try:
        payload = json.loads(r["raw_response"])
    except json.JSONDecodeError:
        assert not r["valid_response"]
        payload = {}
    tokens = payload.get("usage", {})
    usage.append(dict(request_id=request_id, attempt_number=attempt, status=r["status"], http_status=r["http_status"],
        started_at=r["started_at"], received_at=r["received_at"], returned_model=payload.get("model", ""),
        input_tokens=tokens.get("input_tokens"), output_tokens=tokens.get("output_tokens"), request_sha256=r["request_sha256"]))
for request_id, r in successful.items():
    p = requests[request_id]
    payload = json.loads(r["raw_response"])
    assert r["http_status"] == 200 and set(payload["answers"]) == set(p["body"]["questions"])
    for field, a in payload["answers"].items():
        options = p["body"]["questions"][field]["criteria"]
        assert a["type"] == "choice" and a["choice"] in options and set(a["probabilities"]) == set(options)
        assert all(math.isfinite(v) and 0 <= v <= 1 for v in a["probabilities"].values())
        assert abs(sum(a["probabilities"].values()) - 1) < 0.03
        assert math.isfinite(a["confidence"]) and 0 <= a["confidence"] <= 1
        answers[p["document_id"], p["repeat"], field] = a
        labels.append(dict(document_id=p["document_id"], cohort=p["cohort"], repeat=p["repeat"], question=field,
            value=a["choice"], confidence=a["confidence"], probabilities_json=json.dumps(a["probabilities"], sort_keys=True),
            request_id=request_id, request_sha256=r["request_sha256"]))


def majority(values):
    if any(v == "" for v in values):
        return ""
    counts = Counter(values)
    return next((v for v in ("0", "1") if counts[v] > len(values) / 2), "unclear")


fields = ("scale_character_preservation", "revision_or_concession")
comparisons, stability = [], []
for r in sample:
    doc = r["document_id"]
    received = sum(p["document_id"] == doc and p["request_id"] in successful for p in packets)
    for field in fields:
        reference = human.get((doc, field), {})
        readings = {variant: [answers.get((doc, repeat, variant + "_" + field), {}).get("choice", "") for repeat in range(1, 6)]
                    for variant in ("original", "clear", "evidence", "boundary")}
        basis = "character_basis" if field == fields[0] else "revision_basis"
        positive = {"concrete_issue"} if field == fields[0] else {"adopted_change", "undertaken_commitment"}
        diagnostic = []
        for repeat in range(1, 6):
            value = answers.get((doc, repeat, basis), {}).get("choice", "")
            diagnostic.append(value if value in {"", "unclear"} else "1" if value in positive else "0")
        methods = dict(original_first=readings["original"][0], original_repeat_majority=majority(readings["original"]),
            clear_first=readings["clear"][0], clear_repeat_majority=majority(readings["clear"]),
            wording_majority=majority([readings[v][0] for v in ("clear", "evidence", "boundary")]),
            wording_and_repeats=majority([value for v in ("clear", "evidence", "boundary") for value in readings[v]]),
            diagnostic_first=diagnostic[0], diagnostic_repeat_majority=majority(diagnostic))
        prior = old.get((doc, field), {})
        for method, value in methods.items():
            comparisons.append(dict(document_id=doc, application_number=r["application_number"], cohort=r["cohort"],
                field=field, method=method, value=value, received_repeats=received, expected_repeats=r["expected_repeats"],
                status="split_bundle_not_tested" if r["selection_reason"] != "selected" else "complete" if received == 5 else "incomplete",
                original_human_value=reference.get("human_value", ""),
                original_human_eligible=reference.get("human_status") in {"human_agreement", "single_human_coder"} and reference.get("human_value") in {"0", "1"},
                working_value=reference.get("reconciled_value", ""), working_eligible=reference.get("reconciled_value") in {"0", "1"},
                human_status=reference.get("human_status", ""), reconciliation_provenance=reference.get("reconciliation_provenance", ""),
                v1_value=prior.get("jev_value", ""), first_original_value=readings["original"][0],
                first_diagnostic=answers.get((doc, 1, basis), {}).get("choice", ""),
                candidate_evidence_page=answers.get((doc, 1, "page_" + field), {}).get("choice", "")))
        for variant, values in readings.items():
            present = [v for v in values if v]
            probability_yes = [answers[doc, i, variant + "_" + field]["probabilities"]["1"] for i in range(1, 6) if (doc, i, variant + "_" + field) in answers]
            stability.append(dict(document_id=doc, cohort=r["cohort"], field=field, variant=variant, received=len(present),
                distinct_choices=len(set(present)), yes_votes=present.count("1"), no_votes=present.count("0"), unclear_votes=present.count("unclear"),
                first_value=values[0], majority_value=majority(values), probability_yes_range=max(probability_yes)-min(probability_yes) if present else None,
                choices_json=json.dumps(values)))

agreement = []
for cohort in ("original_development", "additional_balanced_test"):
    for field in fields:
        for reference in ("original_human", "working"):
            for method in methods:
                rows = [r for r in comparisons if r["cohort"] == cohort and r["field"] == field and r["method"] == method and r["status"] == "complete" and r[reference + "_eligible"]]
                if not rows:
                    continue
                truth = reference + "_value"
                original_misses = [r for r in rows if r["v1_value"] in {"0", "1"} and r["v1_value"] != r[truth]]
                original_matches = [r for r in rows if r["v1_value"] in {"0", "1"} and r["v1_value"] == r[truth]]
                agreement.append(dict(cohort=cohort, field=field, reference=reference, method=method, reports=len(rows),
                    matches=sum(r["value"] == r[truth] for r in rows), available=sum(r["value"] in {"0", "1"} for r in rows),
                    reference_yes=sum(r[truth] == "1" for r in rows), true_positive=sum(r["value"] == r[truth] == "1" for r in rows),
                    extra_positive=sum(r["value"] == "1" and r[truth] == "0" for r in rows),
                    missed_positive=sum(r["value"] != "1" and r[truth] == "1" for r in rows),
                    v1_misses=len(original_misses), fixed_v1_misses=sum(r["value"] == r[truth] for r in original_misses),
                    v1_matches=len(original_matches), regressed_v1_matches=sum(r["value"] != r[truth] for r in original_matches),
                    differs_from_first=sum(r["value"] != r["first_original_value"] for r in rows)))

save_csv(labels, list(labels[0]), "../output/cpc_jev_labels_v2.csv", ["document_id", "repeat", "question"])
save_csv(usage, list(usage[0]), "../output/cpc_jev_usage_v2.csv", ["request_id", "attempt_number"])
save_csv(comparisons, list(comparisons[0]), "../output/cpc_jev_comparison_v2.csv", ["document_id", "field", "method"])
save_csv(stability, list(stability[0]), "../output/cpc_jev_stability_v2.csv", ["document_id", "field", "variant"])
save_csv(agreement, list(agreement[0]), "../output/cpc_jev_agreement_v2.csv", ["cohort", "field", "reference", "method"])
credits = [json.loads(s) for s in Path("../input/cpc_jev_credits_v2.jsonl").read_text().splitlines()]
assert credits[-1]["phase"] == "after"
charge = Decimal(str(credits[-1]["total_used"])) - Decimal(str(credits[0]["total_used"]))
lines = ["# Repeated Jev readings and revised questions", "",
    f"Received {len(successful)}/{len(packets)} requests. Saved {len(labels)} question answers and {len(usage)} attempts. "
    f"Reported usage is {sum(r['input_tokens'] or 0 for r in usage):,} input and {sum(r['output_tokens'] or 0 for r in usage):,} output tokens. "
    f"Observed account-level charge: ${charge}; remaining credit: ${credits[-1]['balance']}.", "",
    "The experiment preserves all 30 original reports, leaving four long split bundles explicitly outside repeat comparison, "
    "and adds 20 whole bundles, five in each original character/revision code combination. Additional bundles have completed, "
    "nonconflicting original codes, fit the fixed context limit, and share no source with earlier pilots, source reconciliation, or each other. "
    "The selection roster records all 340 human-coded reports and their inclusion reasons. These are development comparisons, not population accuracy estimates.", "",
    "Each selected bundle has five identical request bodies. Each request includes the original two questions, three clarified phrasings, "
    "four diagnostic questions, and two candidate evidence-page choices. Original question definitions and source state are preserved; "
    "the question batch differs from v1. TypeSafe documents independent question evaluation. The repeated answers are not independent votes "
    "about truth, and this design cannot distinguish model determinism from any upstream caching. Majority requires more than half of all "
    "planned votes; ties/unclear remain unresolved, and incomplete repeats are excluded from paired summary tables.", "",
    "In the tables, original first is the first response to the original question in this new experiment. Clear first uses the first "
    "clarified phrasing; wording majority combines the three phrasings from their first request; wording and repeats combines all "
    "15 clarified votes. Diagnostic methods map the evidence category to a binary label. These mappings were specified before the run completed.", "",
    "Evidence-page choices locate candidate source pages, not verified supporting quotations. No original or production labels are replaced. "
    "The returned model is an alias. Original and AI-assisted working references remain separate. "
    "[TypeSafe question semantics](https://docs.typesafe.ai/primitives) and "
    "[free Vercel pricing through September 25, 2026](https://vercel.com/ai-gateway/models/jev) were checked September 19."]
lines += ["", "## Repeat stability", "", "| Question | Complete report/field sets | Changed choice | Changed probability |", "|---|---:|---:|---:|"]
for variant in ("original", "clear", "evidence", "boundary"):
    rows = [r for r in stability if r["variant"] == variant and r["received"] == 5]
    lines.append(f"| {variant} | {len(rows)} | {sum(r['distinct_choices'] > 1 for r in rows)} | {sum(r['probability_yes_range'] > 0.000001 for r in rows)} |")
for cohort, reference, title in (("original_development", "working", "Original sample: working reference"),
                                ("additional_balanced_test", "original_human", "Additional balanced sample: original human reference")):
    lines += ["", "\\newpage", "", "## " + title, "",
        "Each cell is matching labels / comparable reports. All methods use the same complete-repeat reports.", "",
        "| Method | Character | Revisions |", "|---|---:|---:|"]
    for method in methods:
        selected = [next((r for r in agreement if r["cohort"] == cohort and r["reference"] == reference and r["field"] == f and r["method"] == method), None) for f in fields]
        lines.append("| " + method.replace("_", " ") + " | " + " | ".join(f"{r['matches']}/{r['reports']}" if r else "unavailable" for r in selected) + " |")
    lines += ["", "| Method / field | Reference yes | Found yes | Extra yes | Missed yes |", "|---|---:|---:|---:|---:|"]
    for r in agreement:
        if r["cohort"] == cohort and r["reference"] == reference and r["method"] in {"original_first", "clear_first", "wording_majority", "diagnostic_first"}:
            lines.append(f"| {r['method'].replace('_', ' ')} / {'character' if r['field'] == fields[0] else 'revision'} | {r['reference_yes']} | {r['true_positive']} | {r['extra_positive']} | {r['missed_positive']} |")
lines += ["", "\\newpage", "", "## What happened to the original mismatches?", "",
    "Against the working reference on the old sample; disagreements are not independently adjudicated errors.", "",
    "| Method / field | Old misses corrected | Old matches lost |", "|---|---:|---:|"]
for r in agreement:
    if r["cohort"] == "original_development" and r["reference"] == "working":
        lines.append(f"| {r['method'].replace('_', ' ')} / {'character' if r['field'] == fields[0] else 'revision'} | {r['fixed_v1_misses']}/{r['v1_misses']} | {r['regressed_v1_matches']}/{r['v1_matches']} |")
incomplete = [r for r in comparisons if r["method"] == "original_first" and r["field"] == fields[0] and r["status"] != "complete"]
lines += ["", "## Reports outside paired comparison", ""]
lines.extend(f"- {r['application_number']}: {r['status']}, {r['received_repeats']}/{r['expected_repeats']} repeats received." for r in incomplete)
Path("../output/cpc_jev_findings_v2.md").write_text("\n".join(lines) + "\n")
print(f"Compared {len(sample)} retained experiment reports; {len(successful)}/{len(packets)} requests received; observed charge ${charge}.")
