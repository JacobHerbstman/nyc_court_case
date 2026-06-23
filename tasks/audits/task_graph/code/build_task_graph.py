#!/usr/bin/env python3

import csv
import re
from collections import defaultdict
from pathlib import Path


def logical_lines(path):
    lines = []
    buffer = ""

    for raw_line in path.read_text().splitlines():
        if buffer:
            buffer += raw_line.lstrip()
        else:
            buffer = raw_line

        if buffer.endswith("\\"):
            buffer = buffer[:-1] + " "
            continue

        lines.append(buffer)
        buffer = ""

    if buffer:
        lines.append(buffer)

    return lines


def is_rule_line(line):
    stripped = line.strip()

    if not stripped or stripped.startswith("#") or line.startswith("\t"):
        return False

    if ":" not in line:
        return False

    if ":=" in line or "?=" in line or "+=" in line or "=" in line.split(":", 1)[0]:
        return False

    return True


def parse_makefile(path, scope):
    output_targets = set()
    all_targets = set()
    upstream_outputs = set()

    for line in logical_lines(path):
        if not is_rule_line(line):
            continue

        lhs, rhs = line.split(":", 1)
        normal_prereqs = rhs.split("|", 1)[0]

        if lhs.strip() == "all":
            for target in normal_prereqs.split():
                if target.startswith("../output/"):
                    all_targets.add(target)

        for target in lhs.split():
            if target.startswith("../output/"):
                output_targets.add(target)

        for prereq in normal_prereqs.split():
            if scope == "production":
                audit_match = re.match(r"\.\./\.\./audits/([^/\s]+)/output/([^\s|]+)", prereq)
                production_match = re.match(r"\.\./\.\./([^/\s]+)/output/([^\s|]+)", prereq)

                if audit_match:
                    upstream_outputs.add((f"audits/{audit_match.group(1)}", audit_match.group(2)))
                elif production_match:
                    upstream_outputs.add((production_match.group(1), production_match.group(2)))

            if scope == "audit":
                production_match = re.match(r"\.\./\.\./\.\./([^/\s]+)/output/([^\s|]+)", prereq)
                audit_match = re.match(r"\.\./\.\./([^/\s]+)/output/([^\s|]+)", prereq)

                if production_match:
                    upstream_outputs.add((production_match.group(1), production_match.group(2)))
                elif audit_match:
                    upstream_outputs.add((f"audits/{audit_match.group(1)}", audit_match.group(2)))

    return output_targets, all_targets, upstream_outputs


def find_cycle(tasks, edges):
    children = defaultdict(list)
    for upstream, downstream, _ in edges:
        children[upstream].append(downstream)

    visiting = set()
    visited = set()
    stack = []

    def visit(task):
        if task in visiting:
            start = stack.index(task)
            return stack[start:] + [task]

        if task in visited:
            return None

        visiting.add(task)
        stack.append(task)

        for child in sorted(children[task]):
            cycle = visit(child)
            if cycle:
                return cycle

        stack.pop()
        visiting.remove(task)
        visited.add(task)
        return None

    for task in sorted(tasks):
        cycle = visit(task)
        if cycle:
            return cycle

    return None


def main():
    edges_csv = Path("../output/task_edges.csv")
    inventory_csv = Path("../output/task_inventory.csv")
    output_inventory_csv = Path("../output/task_output_inventory.csv")
    boundary_edges_csv = Path("../output/task_boundary_edges.csv")
    dot_file = Path("../output/task_flow.dot")
    summary_csv = Path("../output/task_graph_summary.csv")
    tasks_root = Path("../../..").resolve()
    audits_root = tasks_root / "audits"
    repo_root = tasks_root.parent

    production_makefiles = {
        path.parents[1].name: (path, "production")
        for path in sorted(tasks_root.glob("*/code/Makefile"))
        if path.parents[1].name not in {"archive", "_lib", "audits"}
    }

    audit_makefiles = {}
    if audits_root.exists():
        audit_makefiles = {
            f"audits/{path.parents[1].name}": (path, "audit")
            for path in sorted(audits_root.glob("*/code/Makefile"))
        }

    task_makefiles = {**production_makefiles, **audit_makefiles}
    task_outputs = {}
    task_all_outputs = {}
    upstream_refs = {}
    task_scopes = {}

    for task, (makefile, scope) in task_makefiles.items():
        outputs, all_outputs, refs = parse_makefile(makefile, scope)
        task_outputs[task] = outputs
        task_all_outputs[task] = all_outputs
        upstream_refs[task] = refs
        task_scopes[task] = scope

    edges = []
    missing_tasks = []
    missing_targets = []

    for downstream, refs in upstream_refs.items():
        for upstream, output_rel in sorted(refs):
            edges.append((upstream, downstream, output_rel))

            if upstream not in task_makefiles:
                missing_tasks.append((downstream, upstream, output_rel))
                continue

            upstream_target = "../output/" + output_rel
            if upstream_target not in task_outputs[upstream]:
                missing_targets.append((downstream, upstream, upstream_target))

    cycle = find_cycle(task_makefiles, edges)
    downstream_counts = defaultdict(int)
    upstream_counts = defaultdict(int)
    output_downstream_tasks = defaultdict(set)
    production_audit_edges = []

    for upstream, downstream, output_rel in edges:
        downstream_counts[upstream] += 1
        upstream_counts[downstream] += 1
        output_downstream_tasks[(upstream, output_rel)].add(downstream)

        if task_scopes.get(upstream) == "audit" and task_scopes.get(downstream) == "production":
            production_audit_edges.append((downstream, upstream, output_rel))

    paper_makefile = repo_root / "paper" / "Makefile"
    paper_facing_tasks = set()
    paper_facing_outputs = set()
    if paper_makefile.exists():
        for match in re.finditer(r"\.\./tasks/([^/\s]+)/output/([^\\\s{}]+)", paper_makefile.read_text()):
            paper_facing_tasks.add(match.group(1))
            paper_facing_outputs.add((match.group(1), match.group(2)))

    for tex_file in sorted((repo_root / "paper").glob("sections/*.tex")):
        for match in re.finditer(r"\.\./tasks/([^/\s]+)/output/([^\\\s{}]+)", tex_file.read_text()):
            paper_facing_tasks.add(match.group(1))
            paper_facing_outputs.add((match.group(1), match.group(2)))

    production_sinks = [
        task
        for task, scope in sorted(task_scopes.items())
        if scope == "production"
        and downstream_counts[task] == 0
        and task not in paper_facing_tasks
        and task != "setup_environment"
    ]

    with edges_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "upstream_task",
            "upstream_scope",
            "downstream_task",
            "downstream_scope",
            "upstream_output",
        ])
        writer.writerows([
            [
                upstream,
                task_scopes.get(upstream, "missing"),
                downstream,
                task_scopes.get(downstream, "missing"),
                output_rel,
            ]
            for upstream, downstream, output_rel in edges
        ])

    with inventory_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "task",
            "scope",
            "makefile",
            "output_target_count",
            "upstream_task_count",
            "downstream_task_count",
            "paper_facing",
        ])
        for task in sorted(task_makefiles):
            makefile, _ = task_makefiles[task]
            writer.writerow([
                task,
                task_scopes[task],
                str(makefile.relative_to(tasks_root)),
                len(task_outputs[task]),
                upstream_counts[task],
                downstream_counts[task],
                str(task in paper_facing_tasks).lower(),
            ])

    sidecar_terms = [
        "qc",
        "audit",
        "summary",
        "coverage",
        "example",
        "validation",
        "failure",
        "unresolved",
        "diagnostic",
        "check",
        "inventory",
    ]

    with output_inventory_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "task",
            "scope",
            "output_target",
            "output_file",
            "in_all_target",
            "downstream_task_count",
            "downstream_tasks",
            "paper_facing",
            "terminal_output",
            "sidecar_named",
        ])

        for task in sorted(task_makefiles):
            for output_target in sorted(task_outputs[task]):
                output_file = output_target.removeprefix("../output/")
                downstream_tasks = sorted(output_downstream_tasks[(task, output_file)])
                paper_facing = (task, output_file) in paper_facing_outputs
                sidecar_named = any(term in output_file.lower() for term in sidecar_terms)
                writer.writerow([
                    task,
                    task_scopes[task],
                    output_target,
                    output_file,
                    str(output_target in task_all_outputs[task]).lower(),
                    len(downstream_tasks),
                    ";".join(downstream_tasks),
                    str(paper_facing).lower(),
                    str(len(downstream_tasks) == 0 and not paper_facing).lower(),
                    str(sidecar_named).lower(),
                ])

    with boundary_edges_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "boundary_type",
            "downstream_task",
            "downstream_scope",
            "upstream_task",
            "upstream_scope",
            "upstream_output",
        ])
        for upstream, downstream, output_rel in sorted(edges):
            upstream_scope = task_scopes.get(upstream, "missing")
            downstream_scope = task_scopes.get(downstream, "missing")
            if upstream_scope == downstream_scope:
                continue

            writer.writerow([
                f"{upstream_scope}_to_{downstream_scope}",
                downstream,
                downstream_scope,
                upstream,
                upstream_scope,
                output_rel,
            ])

    with dot_file.open("w") as f:
        f.write("digraph G {\n")
        f.write("  rankdir=LR;\n")
        for upstream, downstream, _ in sorted(edges):
            f.write(f'  "{upstream}" -> "{downstream}";\n')
        f.write("}\n")

    summary_rows = [
        ["production_task_count", str(len(production_makefiles))],
        ["audit_task_count", str(len(audit_makefiles))],
        ["task_count", str(len(task_makefiles))],
        ["production_output_target_count", str(sum(len(task_outputs[task]) for task in production_makefiles))],
        ["production_all_output_target_count", str(sum(len(task_all_outputs[task]) for task in production_makefiles))],
        [
            "production_terminal_all_output_count",
            str(sum(
                1
                for task in production_makefiles
                for output_target in task_all_outputs[task]
                if len(output_downstream_tasks[(task, output_target.removeprefix("../output/"))]) == 0
                and (task, output_target.removeprefix("../output/")) not in paper_facing_outputs
            )),
        ],
        [
            "production_sidecar_named_all_output_count",
            str(sum(
                1
                for task in production_makefiles
                for output_target in task_all_outputs[task]
                if any(term in output_target.lower() for term in sidecar_terms)
            )),
        ],
        [
            "production_edge_count",
            str(sum(1 for _, downstream, _ in edges if task_scopes.get(downstream) == "production")),
        ],
        [
            "audit_edge_count",
            str(sum(1 for _, downstream, _ in edges if task_scopes.get(downstream) == "audit")),
        ],
        ["edge_count", str(len(edges))],
        ["production_to_audit_dependency_count", str(len(production_audit_edges))],
        ["production_sink_without_downstream_or_paper_count", str(len(production_sinks))],
        ["missing_upstream_task_count", str(len(missing_tasks))],
        ["missing_upstream_target_count", str(len(missing_targets))],
        ["cycle", " -> ".join(cycle) if cycle else ""],
    ]

    for downstream, upstream, output_rel in production_audit_edges:
        summary_rows.append([
            "production_to_audit_dependency",
            f"{downstream} reads {upstream}/output/{output_rel}",
        ])

    for task in production_sinks:
        summary_rows.append([
            "production_sink_without_downstream_or_paper",
            task,
        ])

    for downstream, upstream, output_rel in missing_tasks:
        summary_rows.append([
            "missing_upstream_task",
            f"{downstream} reads {upstream}/output/{output_rel}",
        ])

    for downstream, upstream, upstream_target in missing_targets:
        summary_rows.append([
            "missing_upstream_target",
            f"{downstream} reads {upstream}/{upstream_target}",
        ])

    with summary_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value"])
        writer.writerows(summary_rows)

    if missing_tasks or missing_targets or cycle:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
