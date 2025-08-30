import csv
import argparse
import logging
import sys

csv.field_size_limit(10**7)  

def setup_logging(verbose, log_file):
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers
    )

def main():
    parser = argparse.ArgumentParser(description="Join OpenAlex and Imenik CSVs, marking Imenik rows with addedViaImenik=1.")
    parser.add_argument('--openalex', required=True, help="OpenAlex CSV file")
    parser.add_argument('--imenik', required=True, help="Imenik CSV file")
    parser.add_argument('--output', required=True, help="Output CSV file")
    parser.add_argument('--log', help="Log file location")
    parser.add_argument('--verbose', action='store_true', help="Verbose logging")
    args = parser.parse_args()

    setup_logging(args.verbose, args.log)

    # Read headers from both files
    with open(args.openalex, newline='', encoding='utf-8') as f1, \
         open(args.imenik, newline='', encoding='utf-8') as f2:
        reader1 = csv.DictReader(f1)
        reader2 = csv.DictReader(f2)
        # Union of all columns plus 'addedViaImenik'
        all_columns = list(dict.fromkeys(
            list(reader1.fieldnames or []) +
            list(reader2.fieldnames or []) +
            ["addedViaImenik"]
        ))

    count = 0
    with open(args.output, 'w', newline='', encoding='utf-8') as fout, \
         open(args.openalex, newline='', encoding='utf-8') as f1, \
         open(args.imenik, newline='', encoding='utf-8') as f2:
        writer = csv.DictWriter(fout, fieldnames=all_columns)
        writer.writeheader()

        reader1 = csv.DictReader(f1)
        for row in reader1:
            row_out = {col: row.get(col, "") for col in all_columns}
            row_out["addedViaImenik"] = ""
            writer.writerow(row_out)
            count += 1

        reader2 = csv.DictReader(f2)
        for row in reader2:
            row_out = {col: row.get(col, "") for col in all_columns}
            row_out["addedViaImenik"] = "1"
            writer.writerow(row_out)
            count += 1

    logging.info(f"Combined CSVs. Total rows written (excluding header): {count}")

if __name__ == "__main__":
    main()