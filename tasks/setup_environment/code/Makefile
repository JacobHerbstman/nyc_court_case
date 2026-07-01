SHELL := bash
include ../../shell_functions.make

all: ../output/system_requirements.txt ../output/R_packages.txt ../output/python_packages.txt

../output/system_requirements.txt: system_requirements.sh | ../output
	bash $<

../output/R_packages.txt: packages.R ../output/system_requirements.txt | ../output
	$(R) $<

../output/python_packages.txt: python_packages.py | ../output
	python3 $<

link-inputs:

include ../../generic.make
