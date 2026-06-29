# Task Graph

Builds a compact inventory of production task outputs, upstream dependencies,
and terminal production outputs.

The inputs are production task Makefiles. Outputs are `task_flow.png`,
`task_edges.csv`, `task_inventory.csv`, `task_output_inventory.csv`, and
`task_graph_summary.csv`. Annual `$(foreach ...)` output lists are collapsed to
their pattern target so the inventory stays compact.
