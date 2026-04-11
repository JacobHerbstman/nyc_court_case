# setwd("tasks/setup_environment/code")

out_txt <- "../output/R_packages.txt"

options(repos = c(CRAN = "https://cloud.r-project.org"))

cran_pkgs <- c(
  "arrow", "data.table", "DBI", "duckdb", "dplyr", "fixest",
  "ggplot2", "jsonlite", "lubridate", "optparse", "readr", "sf", "stringr",
  "tibble", "tidyr"
)

for (pkg in cran_pkgs) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
}

pkgs <- as.data.frame(installed.packages()[, c("Package", "Version")])
write.table(pkgs, out_txt, sep = "\t", row.names = FALSE, quote = FALSE)
cat("Wrote", nrow(pkgs), "packages to", out_txt, "\n")
