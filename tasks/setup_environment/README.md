# Setup Environment

Installs and records the R and Python packages used by the task pipeline.

Inputs are `packages.R` and `python_packages.py`. Outputs are `R_packages.txt`
and `python_packages.txt`, records of the package versions available to the
project.

Runtime is a few seconds after packages are installed. A first run can take
longer if packages need to be downloaded from CRAN or PyPI.
