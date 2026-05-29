# Audit Task Convention

Audit tasks live under `tasks/audits/<audit_name>/` and are outside the production dependency graph.

Use audits for diagnostics, coverage checks, exploratory plots, validation tables, manual-review samples, and other outputs that do not serve as paper-facing outputs, downstream handoff files, or canonical production data.

Audit task Makefiles should follow the same simple shape as production tasks: explicit file targets, explicit symlink inputs, `all`, `link-inputs`, and `include ../../generic.make`.

From `tasks/audits/<audit_name>/code`, link production task outputs with paths like `../../../<task>/output/<file>`. Link sibling audit outputs only when the audit truly depends on another audit.
