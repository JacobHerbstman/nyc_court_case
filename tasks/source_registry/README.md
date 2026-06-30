# Source Registry

Publishes the committed source catalog used by data-fetching tasks.

The key input is `code/source_catalog.csv`. The output is
`source_catalog.csv`, a stable copy in `output/` for downstream Makefile
dependencies.

The catalog is limited to sources consumed by active production tasks. Broader
validation sources and archival leads are preserved under
`tasks/audits/source_registry_exploratory_sources/`.
