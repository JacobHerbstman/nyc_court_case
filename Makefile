SHELL := /bin/bash
.DEFAULT_GOAL := paper

.PHONY: all paper setup task-graph

all: paper

paper: tasks/setup_environment/output/system_requirements.txt tasks/setup_environment/output/R_packages.txt tasks/setup_environment/output/python_packages.txt
	$(MAKE) -C paper

setup: tasks/setup_environment/output/system_requirements.txt tasks/setup_environment/output/R_packages.txt tasks/setup_environment/output/python_packages.txt

tasks/setup_environment/output/system_requirements.txt: tasks/setup_environment/code/system_requirements.sh
	$(MAKE) -C tasks/setup_environment/code ../output/system_requirements.txt

tasks/setup_environment/output/R_packages.txt: tasks/setup_environment/code/packages.R tasks/setup_environment/output/system_requirements.txt
	$(MAKE) -C tasks/setup_environment/code ../output/R_packages.txt

tasks/setup_environment/output/python_packages.txt: tasks/setup_environment/code/python_packages.py
	$(MAKE) -C tasks/setup_environment/code ../output/python_packages.txt

task-graph:
	$(MAKE) -C tasks/task_graph/code
