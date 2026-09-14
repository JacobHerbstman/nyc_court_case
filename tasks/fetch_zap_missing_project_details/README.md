# Recover ZAP details omitted from the bulk fields

Run `make` from `code/`. The input is the complete dated project universe from
`build_zap_project_universe`. The task requests every withdrawn, terminated,
record-closed, or unclassified project, including non-ULURP terminal records.
It does not use a ULURP number or the existence of a CPC report for selection.

The public endpoint is
`https://zap-api-production.herokuapp.com/projects/<project_id>?include=actions,milestones,dispositions,packages,artifacts`.
Four concurrent requests and a bounded retry for transient errors keep the
retrieval modest. The request includes only publicly accessible data.

Validated responses are saved unchanged in
`data_raw/dcp_zap_project_details/20260914/`. A restart reuses those raw snapshots;
partial downloads and responses with a mismatched project ID are not published
as successful raw data. This is acquisition caching, not analytical freshness
logic. The fixed API query is declared in the producer script, which is a Make
prerequisite. A changed query requiring new source content needs a new vintage.

`output/zap_missing_project_details.jsonl` is one JSON object per selected
project, with bulk status/scope, URL, snapshot date, SHA-256, fetch status, and
the nested response. Project attributes, actions, milestones, dispositions,
packages, and artifacts retain their source names, IDs, and relationships.
An action is not treated as an independent project. This file is already
machine readable and preserves short project descriptions and document links
without a lossy LLM summary.

Fetch errors remain explicit rows. A 404 does not delete the project from the
master universe or mean that a document never existed. To deliberately retry
failed requests, remove the generated JSONL output and run `make`; successful
raw responses are reused. Exact recreation of a deleted raw response is not
guaranteed by a mutable API. The bulk export and API are different snapshots;
their values are kept separate rather than silently substituted.

The data report records selection, key checks, response coverage, and the
JSONL checksum. Execution progress and errors are in the task's `.log` file.
`audits/audit_zap_universe_coverage` extracts action-level evidence and assesses
links to existing CPC reports. Linked files themselves are not downloaded here.
