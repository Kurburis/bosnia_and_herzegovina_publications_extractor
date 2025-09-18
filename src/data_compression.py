# pip install pandas pyarrow
import os
import argparse
import logging
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

LIMIT = 95 * 1024 * 1024  # 95MB safety margin for GitHub's 100MB limit
COMPRESSION = "snappy"

def write_split(df, base_name, start_idx=0, limit_bytes=LIMIT, out_dir=Path(".")):
    """Write df to Parquet; if file > limit, split df in half recursively."""
    if len(df) == 0:
        return start_idx
    path = out_dir / f"{base_name}_{start_idx:04}.parquet"
    tbl = pa.Table.from_pandas(df, preserve_index=False)  # Convert DataFrame to PyArrow Table
    pq.write_table(tbl, path, compression=COMPRESSION)  # Write to Parquet file
    if path.stat().st_size > limit_bytes and len(df) > 1:
        # File too large: delete and split into halves
        path.unlink(missing_ok=True)
        mid = len(df) // 2
        start_idx = write_split(df.iloc[:mid], base_name, start_idx, limit_bytes, out_dir)
        start_idx = write_split(df.iloc[mid:], base_name, start_idx, limit_bytes, out_dir)
        return start_idx
    return start_idx + 1

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Split large CSV or Parquet files into smaller Parquet files.")
    parser.add_argument('--input', required=True, help="Path to the input file (CSV or Parquet).")
    parser.add_argument('--output', required=True, help="Path to the output directory.")
    parser.add_argument('--limit', type=int, default=LIMIT, help="Maximum file size in bytes (default: 95MB).")
    args = parser.parse_args()

    # Set up logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # Validate input file
    input_path = Path(args.input)
    if not input_path.exists():
        logging.error(f"Input file does not exist: {input_path}")
        return

    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Read and split
    file_idx = 0
    if input_path.suffix == ".csv":
        logging.info(f"Processing CSV file: {input_path}")
        for i, chunk in enumerate(pd.read_csv(input_path, chunksize=250_000, low_memory=False)):
            logging.info(f"Processing chunk {i}...")
            file_idx = write_split(chunk, "part", start_idx=file_idx, limit_bytes=args.limit, out_dir=output_dir)
    elif input_path.suffix == ".parquet":
        logging.info(f"Processing Parquet file: {input_path}")
        df = pd.read_parquet(input_path)
        file_idx = write_split(df, "part", start_idx=file_idx, limit_bytes=args.limit, out_dir=output_dir)
    else:
        logging.error("Unsupported file format. Please provide a CSV or Parquet file.")
        return

    logging.info(f"Done. Files written to: {output_dir}")

if __name__ == "__main__":
    main()
