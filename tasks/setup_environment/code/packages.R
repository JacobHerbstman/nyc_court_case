options(repos = c(CRAN = "https://cloud.r-project.org"))

system_dependency_help <- paste(
  "If installation or loading fails for sf, install the geospatial system libraries first.",
  "macOS with Homebrew:",
  "  brew install gdal geos proj udunits",
  "Ubuntu/Debian:",
  "  sudo apt-get update && sudo apt-get install -y libgdal-dev libgeos-dev libproj-dev libudunits2-dev libcurl4-openssl-dev libssl-dev libxml2-dev",
  sep = "\n"
)

cran_pkgs <- c(
  "arrow", "dplyr", "fixest", "foreign",
  "ggplot2", "ipumsr", "jsonlite", "lubridate", "readr", "sf", "stringr",
  "tibble", "tidyr"
)

for (pkg in cran_pkgs) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    tryCatch(
      install.packages(pkg),
      error = function(e) {
        stop(
          "Failed to install R package '", pkg, "'.\n\n",
          system_dependency_help,
          "\n\nOriginal error:\n",
          conditionMessage(e),
          call. = FALSE
        )
      }
    )
  }

  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(
      "R package '", pkg, "' is still not available after installation.\n\n",
      system_dependency_help,
      call. = FALSE
    )
  }
}

pkgs <- as.data.frame(installed.packages()[, c("Package", "Version")])
write.table(pkgs, "../output/R_packages.txt", sep = "\t", row.names = FALSE, quote = FALSE)
cat("Wrote", nrow(pkgs), "packages to ../output/R_packages.txt\n")
