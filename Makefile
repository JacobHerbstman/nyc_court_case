SHELL := /bin/bash
.DEFAULT_GOAL := paper

.PHONY: all paper setup task-graph

all: paper

paper: tasks/setup_environment/output/R_packages.txt tasks/setup_environment/output/python_packages.txt
	$(MAKE) -C paper

setup: tasks/setup_environment/output/R_packages.txt tasks/setup_environment/output/python_packages.txt

tasks/setup_environment/output/R_packages.txt: tasks/setup_environment/code/packages.R
	$(MAKE) -C tasks/setup_environment/code ../output/R_packages.txt

tasks/setup_environment/output/python_packages.txt: tasks/setup_environment/code/python_packages.py
	$(MAKE) -C tasks/setup_environment/code ../output/python_packages.txt

task-graph:
	$(MAKE) -C tasks/task_graph/code
