# Setup Environment

Installs and records the R packages used by the task pipeline.

The input is `packages.R`. The output is `R_packages.txt`, a record of the R
package versions available to the project.

Runtime after packages are installed: about 2 seconds. A first run can take
longer if R packages need to be installed.
