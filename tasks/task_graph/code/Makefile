.NOTPARALLEL:

all: ../output/task_flow.dot \
	../output/task_edges.csv \
	../output/task_list.csv \
	../output/task_outputs.csv \
	../output/task_graph_summary.csv \
	../output/task_flow.png

../%/task_flow.dot ../%/task_edges.csv ../%/task_list.csv ../%/task_outputs.csv ../%/task_graph_summary.csv: build_task_graph.py $(wildcard ../../*/code/Makefile) | ../output
	python3 $<

../output/task_flow.png: ../output/task_flow.dot | ../output
	dot -Grankdir=LR -Tpng $< -o $@

link-inputs:

include ../../generic.make
