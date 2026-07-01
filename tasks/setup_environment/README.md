# Setup Environment

Checks command-line tools, installs missing R and Python packages, and records
the package versions used by the task pipeline.

Inputs are `system_requirements.sh`, `packages.R`, and `python_packages.py`.
Outputs are `system_requirements.txt`, `R_packages.txt`, and
`python_packages.txt`.

The task does not silently install Homebrew or apt packages. If a compiled R
package such as `sf` cannot install or load, the setup output gives the macOS
and Ubuntu/Debian commands needed for the geospatial libraries.

Runtime is a few seconds after packages are installed. A first run can take
longer if packages need to be downloaded from CRAN or PyPI.
