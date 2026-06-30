SHELL := bash
include ../../shell_functions.make

all: ../output/R_packages.txt ../output/python_packages.txt

../output/R_packages.txt: packages.R | ../output
	$(R) $<

../output/python_packages.txt: python_packages.py | ../output
	python3 $<

link-inputs:

include ../../generic.make
