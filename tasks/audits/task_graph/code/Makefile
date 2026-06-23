.NOTPARALLEL:

all: ../output/task_flow.dot \
	../output/task_edges.csv \
	../output/task_inventory.csv \
	../output/task_output_inventory.csv \
	../output/task_boundary_edges.csv \
	../output/task_graph_summary.csv \
	../output/task_flow.png

../output/task_flow.dot: build_task_graph.py $(wildcard ../../../*/code/Makefile) $(wildcard ../../*/code/Makefile) | ../output
	python3 $<

../output/task_edges.csv: ../output/task_flow.dot
	@test -s "$@"

../output/task_inventory.csv: ../output/task_flow.dot
	@test -s "$@"

../output/task_output_inventory.csv: ../output/task_flow.dot
	@test -s "$@"

../output/task_boundary_edges.csv: ../output/task_flow.dot
	@test -s "$@"

../output/task_graph_summary.csv: ../output/task_flow.dot
	@test -s "$@"

../output/task_flow.png: ../output/task_flow.dot | ../output
	dot -Grankdir=LR -Tpng $< -o $@

link-inputs:

include ../../generic.make
