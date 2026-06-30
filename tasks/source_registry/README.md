# Source Registry

Copies the version-controlled source catalog used by data-fetching tasks.

Input: `code/source_catalog.csv`.

Output: `source_catalog.csv`, a stable copy in `output/` for downstream tasks.

The catalog includes only sources used by currently active tasks. Broader
validation sources and exploratory source leads are preserved under
`tasks/audits/source_registry_exploratory_sources/`.
