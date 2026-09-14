#!/usr/bin/env python3
"""Document our observed behavior; Tyler's implementation is not available here."""

import ast
import csv
import hashlib
from pathlib import Path

with Path("../input/ulurp_cpc_text_labels.csv").open(newline="") as source:
    labels = list(csv.DictReader(source))
assert len(labels) == len({row["document_id"] for row in labels})

with Path("../output/ulurp_cpc_regex_holdout_agreement.csv").open(newline="") as source:
    holdout = [row for row in csv.DictReader(source) if row["sample_slice"] == "all"]
assert len(holdout) == len({row["field"] for row in holdout})

parser_source = Path(
    "../../../summarize_text_cpc_trends/code/build_ulurp_cpc_text_labels.py"
).read_text()
parser_hash = hashlib.sha256(parser_source.encode()).hexdigest()

# Load the existing constants and functions without running the production pipeline.
# Stop at its CLI validation; never substitute a reconstructed counting algorithm.
definitions = []
for statement in ast.parse(parser_source).body:
    if isinstance(statement, ast.If):
        assert ast.get_source_segment(parser_source, statement).startswith("if len(sys.argv)")
        break
    definitions.append(statement)
else:
    raise RuntimeError("The production parser's CLI boundary was not found.")
parser = {}
exec(compile(ast.Module(body=definitions, type_ignores=[]), "production_parser", "exec"), parser)

lines = [
    "# Checks from our parser and existing labels",
    "",
    "These checks execute our parser only. They do not estimate agreement with "
    "Tyler's implementation, which has not been located. The two application "
    "examples are identified in his August 28, 2026 PDF, page 12.",
    "",
    f"Current output: {len(labels):,} unique analysis narratives, "
    f"{min(int(row['year']) for row in labels)}–{max(int(row['year']) for row in labels)}. "
    f"{sum(bool(row['companion_application_numbers']) for row in labels):,} rows "
    "list companion applications. These are not all ZAP projects and are not "
    "directly comparable with Tyler's 11,188 usable-report denominator.",
    "",
    "| Count pair | Both counts present | Share of our narratives |",
    "|---|---:|---:|",
]
for name, first, second in [
    ("Community-board votes", "cb_support_votes", "cb_opposition_votes"),
    ("CPC hearing speakers", "cpc_support_speakers", "cpc_opposition_speakers"),
]:
    complete = sum(row[first] != "" and row[second] != "" for row in labels)
    lines.append(f"| {name} | {complete:,} | {complete / len(labels):.1%} |")

lines += [
    "",
    "## The two actual report examples",
    "",
    "Counts below are read from our production CSV, not inferred from the snippets.",
    "",
    "| Application | Votes for | Votes against | Our CB opposition flag |",
    "|---|---:|---:|---:|",
]
for application in ["C 780349 TCM", "C 160174 ZSR"]:
    matches = [row for row in labels if row["application_number"] == application]
    assert len(matches) == 1, f"Expected one output row for {application}."
    row = matches[0]
    lines.append(
        f"| {application} | {row['cb_support_votes']} | "
        f"{row['cb_opposition_votes']} | {row['cb_opposition']} |"
    )

lines += [
    "",
    "Tyler reports the same two count pairs, but labels both formal board "
    "positions as opposition. Our second flag differs because it compares "
    "votes against with votes for whenever both counts exist. The second "
    "source report explicitly says that five abstentions counted as disapproval votes.",
    "",
    "## Existing Codex holdout benchmark",
    "",
    "This benchmark has 100 reports whose own action codes are ZM, ZR, or ZS. "
    "The reference labels were read by Codex after rule development. They are "
    "not independent human gold labels. Unclear reference values are excluded. "
    "The first rate counts an unparsed regex value as a miss; the second "
    "conditions on the regex returning a value.",
    "",
    "| Field | Reference values | Exact / reference | Exact / parsed |",
    "|---|---:|---:|---:|",
]
for field, label in [
    ("cpc_support_speakers", "Supporting speakers"),
    ("cpc_opposition_speakers", "Opposing speakers"),
    ("cb_support_votes", "Supporting board votes"),
    ("cb_opposition_votes", "Opposing board votes"),
    ("revision_or_concession", "Revision or concession"),
    ("procedural_response", "Procedural response"),
    ("explicit_local_response", "Explicit local response"),
]:
    row = next(row for row in holdout if row["field"] == field)
    lines.append(
        f"| {label} | {row['human_nonmissing']} | "
        f"{row['exact_agreement']}/{row['human_nonmissing']} "
        f"({float(row['agreement_share']):.1%}) | "
        f"{row['exact_agreement']}/{row['regex_nonmissing']} "
        f"({float(row['exact_when_regex_nonmissing']):.1%}) |"
    )

lines += [
    "",
    "\\newpage",
    "",
    "## Controlled snippets",
    "",
    "These are synthetic diagnostic inputs to our actual counting functions. "
    "They reveal possible failure mechanisms, not their prevalence in the corpus. "
    "They bypass section selection and companion-document handling. "
    "No Tyler function is executed.",
    "",
]
for title, function, snippet in [
    (
        "An unreported side becomes zero",
        "extract_cpc_speaker_counts",
        "Four speakers in favor appeared. The hearing was closed.",
    ),
    (
        "An explicitly absent side is also zero",
        "extract_cpc_speaker_counts",
        "Four speakers in favor appeared. There were no other speakers. The hearing was closed.",
    ),
    (
        "A repeated vote is summed twice",
        "extract_cb_vote_counts",
        "The Board recommended approval by a vote of 19 in favor, 3 opposed. "
        "The recommendation was approval by a vote of 19 in favor, 3 opposed.",
    ),
    (
        "An unsupported number word falls through to a singular-speaker rule",
        "extract_cpc_speaker_counts",
        "Seventy speakers in favor and two in opposition appeared.",
    ),
    (
        "Spaced OCR number words do not resolve",
        "extract_cb_vote_counts",
        "The Board recommended approval by a vote of t w e n t y - o n e in favor, 2 opposed.",
    ),
]:
    result = parser[function](snippet)
    rendered = ", ".join("missing" if value is None else str(value) for value in result)
    lines += [f"**{title}.**", "", f"> {snippet}", "", f"Our result (for, against): {rendered}.", ""]

lines += [
    "The parser source fingerprint is SHA-256:",
    "",
    "```",
    parser_hash,
    "```",
]
Path("../output/ulurp_cpc_tyler_comparison_checks.md").write_text("\n".join(lines) + "\n")
print(f"Wrote comparison checks for {len(labels):,} narratives and five controlled snippets.")
