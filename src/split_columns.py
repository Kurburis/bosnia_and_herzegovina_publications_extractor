import argparse
import logging
import pandas as pd
import ast
import os
import tempfile
import shutil

def setup_logging(verbose):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

def split_and_expand_columns(df, columns, counts):
    for col, count in zip(columns, counts):
        for i in range(1, count + 1):
            new_col = f"{col}.{i}"
            df[new_col] = None
        for idx, val in df[col].fillna("").items():
            # Parse value as list if possible, else treat as single value
            if isinstance(val, list):
                parts = val
            else:
                try:
                    # Try to parse as Python literal (e.g., JSON-like array)
                    parsed = ast.literal_eval(val)
                    if isinstance(parsed, list):
                        parts = parsed
                    elif parsed == "" or parsed is None:
                        parts = []
                    else:
                        parts = [parsed]
                except Exception:
                    # If not a list, treat as single value unless empty
                    parts = [val] if val != "" else []
            for i in range(count):
                df.at[idx, f"{col}.{i+1}"] = parts[i] if i < len(parts) else None
    return df

def main():
    parser = argparse.ArgumentParser(description="Split columns and save to CSV (can overwrite input file)")
    parser.add_argument("--input_csv", required=True, help="Input CSV file (can be same as output)")
    parser.add_argument("--columns", nargs="+", required=True, help="Columns to split")
    parser.add_argument("--counts", nargs="+", type=int, required=True, help="Max splits for each column")
    parser.add_argument("--output_csv", required=True, help="Output CSV file (can be same as input)")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    setup_logging(args.verbose)

    try:
        df = pd.read_csv(args.input_csv)
        logging.info(f"Loaded {len(df)} rows from {args.input_csv}")
    except Exception as e:
        logging.error(f"Failed to read CSV: {e}")
        return

    try:
        df = split_and_expand_columns(df, args.columns, args.counts)
        logging.info("Columns split and expanded.")
    except Exception as e:
        logging.error(f"Column splitting failed: {e}")
        return

    # Support in-place overwrite
    if os.path.abspath(args.input_csv) == os.path.abspath(args.output_csv):
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv")
        os.close(tmp_fd)
        try:
            df.to_csv(tmp_path, index=False)
            shutil.move(tmp_path, args.input_csv)
            logging.info(f"Overwrote input file {args.input_csv}")
        except Exception as e:
            logging.error(f"Failed to save CSV: {e}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
    else:
        try:
            df.to_csv(args.output_csv, index=False)
            logging.info(f"Saved expanded data to {args.output_csv}")
        except Exception as e:
            logging.error(f"Failed to save CSV: {e}")

if __name__ == "__main__":
    main()