# Verify ULURP CPC Report Failures

Independently rechecks the remaining CPC report corpus failures.

The verifier starts from the CPC report corpus failure manifest, retries each
official CPC report URL with a fresh atomic download check, records the HTTP
status and PDF test, and checks whether ZAP exposes an action-level CPC report
route for the same project and application number.

This task is intentionally audit-only. If it finds recoverable reports, the
corpus builder should be fixed and rerun rather than manually patching outputs.
