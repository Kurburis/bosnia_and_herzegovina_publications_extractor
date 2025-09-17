import os
import glob
import csv
import argparse
import logging

logging.basicConfig(level=logging.INFO)

def format_issn(issn):
    """
    Formats an ISSN by inserting a hyphen between the fourth and fifth digits.
    If the ISSN is not 8 digits long, it is returned unchanged.
    """
    issn = issn.strip()
    if len(issn) == 8 and issn.isdigit():
        return f"{issn[:4]}-{issn[4:]}"
    return issn


def main():
    parser = argparse.ArgumentParser(description="Merge SCImago journal rankings from multiple years into one CSV.")
    parser.add_argument('--data_dir', default='data', help='Directory containing SCImago CSV files')
    parser.add_argument('--prefix', default='scimagojr_', help='Prefix for SCImago CSV files')
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
            # Split by whitespace instead of underscores
            parts = os.path.splitext(os.path.basename(f))[0].split()
            year = int(parts[-2])  # Try to extract the second-to-last part as the year
            year_files.append((year, f))
        except Exception:
            try:
                year = int(parts[-1])  # Try to extract the last part as the year
                year_files.append((year, f))
            except Exception:
                continue
    year_files.sort()
    years = [str(y) for y, _ in year_files]

    # Data structure: issn -> {"title": ..., "type": ..., "sourceid": ..., "quartiles": {year: quartile}, "hindex": {year: hindex}}
    journals = {}

    for year, file in year_files:
        logging.info(f"Processing {file}...")
        with open(file, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            for ix, row in enumerate(reader):
                if ix == 2:
                    logging.error(f"Sample row: {row}\n")
                issn = row.get("Issn", "").strip()
                if ix == 2:
                    logging.info(f"Sample ISSN: {issn}\n")
                issn_list = [format_issn(i) for i in issn.split(",")] if issn else []  # Format each ISSN
                if ix == 2:
                    logging.info(f"Sample ISSN list: {issn_list}\n")
                if not issn_list:
                    continue
                title = row.get("Title", "").strip()
                journal_type = row.get("Type", "").strip()
                sourceid = row.get("Sourceid", "").strip()
                quartile = row.get("SJR Best Quartile", "").strip()
                hindex = row.get("H index", "").strip()

                for single_issn in issn_list:
                    if single_issn not in journals:
                        journals[single_issn] = {
                            "title": title,
                            "type": journal_type,
                            "sourceid": sourceid,
                            "quartiles": {},
                            "hindex": {}
                        }
                    # Always update to latest info
                    journals[single_issn]["title"] = title
                    journals[single_issn]["type"] = journal_type
                    journals[single_issn]["sourceid"] = sourceid
                    journals[single_issn]["quartiles"][str(year)] = quartile
                    journals[single_issn]["hindex"][str(year)] = hindex

    # Prepare output rows
    quartile_cols = [f"Quartile - {y}" for y in years]
    hindex_cols = [f"H index - {y}" for y in years]
    header = ["Issn", "Title", "Type", "Sourceid"] + quartile_cols + hindex_cols
    output_rows = []
    for issn, info in journals.items():
        row = [issn, info["title"], info["type"], info["sourceid"]]
        for y in years:
            row.append(info["quartiles"].get(y, ""))
        for y in years:
            row.append(info["hindex"].get(y, ""))
        output_rows.append(row)

    # Sort by title
    output_rows.sort(key=lambda x: x[1].lower())

    # Write to output CSV
    with open(args.output, "w", newline='', encoding='utf-8') as outcsv:
        writer = csv.writer(outcsv)
        writer.writerow(header)
        writer.writerows(output_rows)

    print(f"Written merged CSV to {args.output}")

if __name__ == "__main__":
    main()