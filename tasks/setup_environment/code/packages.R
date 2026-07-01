options(repos = c(CRAN = "https://cloud.r-project.org"))

cran_pkgs <- c(
  "arrow", "dplyr", "fixest", "foreign",
  "ggplot2", "ipumsr", "jsonlite", "lubridate", "readr", "sf", "stringr",
  "tibble", "tidyr"
)

for (pkg in cran_pkgs) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
}

pkgs <- as.data.frame(installed.packages()[, c("Package", "Version")])
write.table(pkgs, "../output/R_packages.txt", sep = "\t", row.names = FALSE, quote = FALSE)
cat("Wrote", nrow(pkgs), "packages to ../output/R_packages.txt\n")
