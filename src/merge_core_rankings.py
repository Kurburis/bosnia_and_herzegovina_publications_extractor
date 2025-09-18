import os
import glob
import csv
import argparse
from collections import defaultdict, OrderedDict

def main():
    parser = argparse.ArgumentParser(description="Merge CORE conference rankings from multiple years into one CSV.")
    parser.add_argument('--data_dir', default='data', help='Directory containing CORE CSV files')
    parser.add_argument('--prefix', default='CORE_', help='Prefix for CORE CSV files')
    parser.add_argument('--output', required=True, help='Output CSV file')
    args = parser.parse_args()

    # Find all matching files
    files = glob.glob(os.path.join(args.data_dir, f"{args.prefix}*.csv"))
    if not files:
        print("No files found.")
        return

    # Extract years from filenames and sort
    year_files = []
    for f in files:
        try:
            year = int(os.path.splitext(os.path.basename(f))[0].split('_')[-1])
            year_files.append((year, f))
        except Exception:
            continue
    year_files.sort()
    years = [str(y) for y, _ in year_files]

    # Data structure: id -> {"name": ..., "abbr": ..., "rankings": {year: ranking}}
    conferences = {}

    for year, file in year_files:
        with open(file, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) < 5:
                    continue
                conf_id = row[0].strip()
                conf_name = row[1].strip()
                conf_abbr = row[2].strip()
                ranking = row[4].strip()
                if conf_id not in conferences:
                    conferences[conf_id] = {"name": conf_name, "abbr": conf_abbr, "rankings": {}}
                conferences[conf_id]["rankings"][str(year)] = ranking

    # Prepare output rows
    header = ["id", "conference name", "conference abbreviation"] + years
    output_rows = []
    for conf_id, info in conferences.items():
        row = [conf_id, info["name"], info["abbr"]]
        for y in years:
            row.append(info["rankings"].get(y, ""))
        output_rows.append(row)

    # Sort by conference name
    output_rows.sort(key=lambda x: x[1].lower())

    # Write to output CSV
    with open(args.output, "w", newline='', encoding='utf-8') as outcsv:
        writer = csv.writer(outcsv)
        writer.writerow(header)
        writer.writerows(output_rows)

    print(f"Written merged CSV to {args.output}")

if __name__ == "__main__":
    main()