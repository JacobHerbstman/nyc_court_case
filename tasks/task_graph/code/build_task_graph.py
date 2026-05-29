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
    upstream_outputs = set()

    for line in logical_lines(path):
        if not is_rule_line(line):
            continue

        lhs, rhs = line.split(":", 1)
        normal_prereqs = rhs.split("|", 1)[0]

        for target in lhs.split():
            if target.startswith("../output/"):
                output_targets.add(target)

        for prereq in normal_prereqs.split():
            if scope == "production":
                match = re.match(r"\.\./\.\./([^/\s]+)/output/([^\s|]+)", prereq)
                if match:
                    upstream_outputs.add((match.group(1), match.group(2)))

            if scope == "audit":
                production_match = re.match(r"\.\./\.\./\.\./([^/\s]+)/output/([^\s|]+)", prereq)
                audit_match = re.match(r"\.\./\.\./([^/\s]+)/output/([^\s|]+)", prereq)

                if production_match:
                    upstream_outputs.add((production_match.group(1), production_match.group(2)))
                elif audit_match:
                    upstream_outputs.add((f"audits/{audit_match.group(1)}", audit_match.group(2)))

    return output_targets, upstream_outputs


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
    dot_file = Path("../output/task_flow.dot")
    audit_csv = Path("../output/task_graph_audit.csv")
    tasks_root = Path("..").resolve().parent
    audits_root = tasks_root / "audits"

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
    upstream_refs = {}
    task_scopes = {}

    for task, (makefile, scope) in task_makefiles.items():
        outputs, refs = parse_makefile(makefile, scope)
        task_outputs[task] = outputs
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

    with dot_file.open("w") as f:
        f.write("digraph G {\n")
        f.write("  rankdir=LR;\n")
        for upstream, downstream, _ in sorted(edges):
            f.write(f'  "{upstream}" -> "{downstream}";\n')
        f.write("}\n")

    audit_rows = [
        ["production_task_count", "ok", str(len(production_makefiles))],
        ["audit_task_count", "ok", str(len(audit_makefiles))],
        ["task_count", "ok", str(len(task_makefiles))],
        ["production_edge_count", "ok", str(sum(1 for _, downstream, _ in edges if task_scopes.get(downstream) == "production"))],
        ["audit_edge_count", "ok", str(sum(1 for _, downstream, _ in edges if task_scopes.get(downstream) == "audit"))],
        ["edge_count", "ok", str(len(edges))],
        ["missing_upstream_tasks", "fail" if missing_tasks else "ok", str(len(missing_tasks))],
        ["missing_upstream_targets", "fail" if missing_targets else "ok", str(len(missing_targets))],
        ["cycles", "fail" if cycle else "ok", " -> ".join(cycle) if cycle else ""],
    ]

    for downstream, upstream, output_rel in missing_tasks:
        audit_rows.append([
            "missing_upstream_task_detail",
            "fail",
            f"{downstream} needs {upstream}/output/{output_rel}",
        ])

    for downstream, upstream, upstream_target in missing_targets:
        audit_rows.append([
            "missing_upstream_target_detail",
            "fail",
            f"{downstream} needs {upstream}/{upstream_target}",
        ])

    with audit_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["check", "status", "detail"])
        writer.writerows(audit_rows)

    if missing_tasks or missing_targets or cycle:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
