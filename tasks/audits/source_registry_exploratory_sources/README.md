# Source Registry Exploratory Sources

This audit folder preserves broader source leads that are not read by the
current production task graph.

The production `source_registry` task only publishes sources consumed by active
Makefile dependencies. Dormant validation sources, archival leads, and possible
future data lanes are kept here so they do not clutter the replication-facing
pipeline.
