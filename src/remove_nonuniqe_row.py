import argparse
import pandas as pd

def main():
    parser = argparse.ArgumentParser(description="Remove duplicate rows from a CSV file, keeping the first occurrence.")
    parser.add_argument('--input_csv', required=True, help="Path to input CSV file")
    parser.add_argument('--output_csv', required=True, help="Path to output CSV file")
    parser.add_argument('--column', required=True, help="Column name to check for uniqueness")
    args = parser.parse_args()

    df = pd.read_csv(args.input_csv, low_memory=False)
    id_col = args.column
    # Keep the first occurrence of each ID (remove subsequent duplicates)
    unique_df = df[~df.duplicated(subset=id_col, keep='first')]
    unique_df.to_csv(args.output_csv, index=False)

if __name__ == "__main__":
    main()