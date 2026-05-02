SHELL := /bin/bash
.DEFAULT_GOAL := paper

.PHONY: all paper task-graph

all: paper

paper:
	$(MAKE) -C paper

task-graph:
	$(MAKE) -C tasks/task_graph/code
