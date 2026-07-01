#!/usr/bin/env bash
set -euo pipefail

output="../output/system_requirements.txt"
tmp="${output}.tmp"

required_commands=(
  make
  R
  python3
  curl
  unzip
  pdflatex
  bibtex
)

missing_commands=()

{
  echo "command	path"
  for command_name in "${required_commands[@]}"; do
    if command_path="$(command -v "${command_name}" 2>/dev/null)"; then
      echo "${command_name}	${command_path}"
    else
      echo "${command_name}	MISSING"
      missing_commands+=("${command_name}")
    fi
  done

  echo
  echo "If R package installation fails for sf, install the geospatial system libraries first."
  echo "macOS with Homebrew:"
  echo "  brew install gdal geos proj udunits"
  echo "Ubuntu/Debian:"
  echo "  sudo apt-get update && sudo apt-get install -y libgdal-dev libgeos-dev libproj-dev libudunits2-dev libcurl4-openssl-dev libssl-dev libxml2-dev"
} > "${tmp}"

mv "${tmp}" "${output}"

if ((${#missing_commands[@]} > 0)); then
  echo "Missing required command-line tools: ${missing_commands[*]}" >&2
  echo "See ${output} for setup notes." >&2
  exit 1
fi

echo "Wrote system requirements check to ${output}"
