import csv


with open("../output/council_land_use_matter_type_query_summary.csv", newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

for row in rows:
    reported_records = int(row["reported_records"] or 0)
    parsed_rows = int(row["parsed_rows"] or 0)
    reported_pages = int(row["reported_pages"] or 0)
    row["possible_legistar_1000_record_cap_flag"] = (
        reported_records == 1000
        and parsed_rows == 1000
        and reported_pages == 10
    )

cap_suspects = [row for row in rows if row["possible_legistar_1000_record_cap_flag"]]

with open("../output/council_land_use_matter_type_cap_suspects.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "query_year",
            "query_matter_type",
            "query_matter_type_value",
            "reported_records",
            "parsed_rows",
            "reported_pages",
            "fetched_pages",
            "rows_match_reported_records",
            "possible_legistar_1000_record_cap_flag",
        ],
        extrasaction="ignore",
    )
    writer.writeheader()
    writer.writerows(cap_suspects)
