SHELL := bash
.NOTPARALLEL:

all: ../output/task_flow.dot ../output/task_edges.csv ../output/task_graph_audit.csv ../output/task_flow.png

../output/task_flow.dot: build_task_graph.py FORCE | ../output
	python3 $<

../output/task_edges.csv: ../output/task_flow.dot
	@test -s "$@"

../output/task_graph_audit.csv: ../output/task_flow.dot
	@test -s "$@"

../output/task_flow.png: ../output/task_flow.dot | ../output
	dot -Grankdir=LR -Tpng $< -o $@

link-inputs:

include ../../generic.make
