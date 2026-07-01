from __future__ import annotations

import importlib
import importlib.metadata
import subprocess
import sys


packages = [
    ("pandas", "pandas"),
    ("requests", "requests"),
    ("beautifulsoup4", "bs4"),
]

rows = []
for package_name, import_name in packages:
    try:
        importlib.import_module(import_name)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])

    rows.append(
        {
            "Package": package_name,
            "Version": importlib.metadata.version(package_name),
        }
    )

with open("../output/python_packages.txt", "w", encoding="utf-8") as out_file:
    out_file.write("Package\tVersion\n")
    for row in rows:
        out_file.write(f"{row['Package']}\t{row['Version']}\n")

print(f"Wrote {len(rows)} packages to ../output/python_packages.txt")
