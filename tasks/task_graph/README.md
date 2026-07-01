# Task Graph

Builds the production task graph, dependency table, and task list.

The inputs are production task Makefiles. Outputs are `task_flow.dot`,
`task_flow.png`, `task_edges.csv`, `task_inventory.csv`,
`task_output_inventory.csv`, and `task_graph_summary.csv`. Annual
`$(foreach ...)` output lists are collapsed to their pattern target so the graph
stays readable. `setup_environment`, `source_registry`, and `task_graph` are
treated as infrastructure rather than analysis tasks.
