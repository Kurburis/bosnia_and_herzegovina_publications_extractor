import csv
import argparse
import logging
import sys
import os
import tempfile
import shutil

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
    parser = argparse.ArgumentParser(description="Remove rows containing '&amp' from a CSV file (in any column).")
    parser.add_argument('--file', required=True, help="CSV file to clean (will be overwritten)")
    parser.add_argument('--log', help="Log file location")
    parser.add_argument('--verbose', action='store_true', help="Verbose logging")
    args = parser.parse_args()

    setup_logging(args.verbose, args.log)

    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    os.close(temp_fd)

    count_in, count_out, count_removed = 0, 0, 0

    with open(args.file, newline='', encoding='utf-8') as infile, \
         open(temp_path, "w", newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            count_in += 1
            row_strs = [str(value) if value is not None else "" for value in row.values()]
            if any("&amp" in val or "&lt" in val for val in row_strs):
                count_removed += 1
                continue
            writer.writerow(row)
            count_out += 1

    shutil.move(temp_path, args.file)
    logging.info(f"Processed {count_in} rows. Written: {count_out}, Removed: {count_removed}")

if __name__ == "__main__":
    main()