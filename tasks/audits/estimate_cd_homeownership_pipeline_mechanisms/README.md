# Invalid exploratory outputs

The outputs in this task are frozen as stale exploratory artifacts. Do not use
them for analysis or paper-facing results.

The ZAP construction feeding this task failed source-integrity checks: historical
`actions` are mostly blank, action proxies were previously built from unsupported
fields, approval timing is not historically usable, and BBL support varies
strongly by period.

Use the audited reconstruction path instead:

- `tasks/audit_zap_source_integrity/`
- `tasks/build_zap_housing_pipeline_from_raw/`
