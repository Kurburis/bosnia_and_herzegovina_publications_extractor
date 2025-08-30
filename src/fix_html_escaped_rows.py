import csv
import argparse
import logging
import sys
import requests
import json
import tempfile
import shutil
import html
from more_itertools import chunked  
import re

csv.field_size_limit(10**7)  

HTML_ESCAPES = [
    "&amp", "&lt", "&gt", "&quot", "&apos", "&#39", "&#34", "&#47", "&#92", "&nbsp"
]

def row_has_html_escape(row):
    for value in row.values():
        s = str(value)
        for esc in HTML_ESCAPES:
            if esc in s:
                return True
        if re.search(r'&#\d{2,5}', s):
            return True
    return False

def fetch_openalex_works_batch(ids, email=None):
    """Fetch up to 200 works by OpenAlex IDs in a single API call."""
    url = "https://api.openalex.org/works"
    params = {
        "filter": "ids.openalex:" + "|".join(ids),
        "per-page": 50
    }
    if email:
        params["mailto"] = email
    try:
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        return data.get("results", [])
    except Exception as e:
        logging.warning(f"Failed to fetch batch: {e}")
        return []

def get_all_paths(d, parent_key='', sep='.'):
    paths = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            paths.extend(get_all_paths(v, new_key, sep=sep))
        elif isinstance(v, list) and v and isinstance(v[0], dict):
            paths.extend(get_all_paths(v[0], new_key, sep=sep))
        else:
            paths.append(new_key)
    return paths

def get_nested(d, path):
    keys = path.split('.')
    def extract(obj, keys):
        if not keys:
            return obj
        key = keys[0]
        rest = keys[1:]
        if isinstance(obj, dict):
            return extract(obj.get(key, ""), rest)
        elif isinstance(obj, list):
            return [extract(item, keys) for item in obj]
        else:
            return ""
    value = extract(d, keys)
    if isinstance(value, list):
        flat = []
        def flatten(x):
            if isinstance(x, list):
                for i in x:
                    flatten(i)
            elif x not in [None, ""]:
                flat.append(x)
        flatten(value)
        # if len(flat) == 1:
        #    return flat[0]
        return flat
    if isinstance(value, dict):
        return value
    return value if value is not None else ""

def recursive_unescape(val):
    if isinstance(val, str):
        prev = None
        curr = val
        while prev != curr:
            prev = curr
            curr = html.unescape(prev)
        return curr
    elif isinstance(val, list):
        return [recursive_unescape(v) for v in val]
    elif isinstance(val, dict):
        return {k: recursive_unescape(v) for k, v in val.items()}
    else:
        return val

def main():
    parser = argparse.ArgumentParser(description="Fix HTML-escaped rows in OpenAlex CSV by redownloading them in batches.")
    parser.add_argument('--input_csv', required=True, help="Input CSV file")
    parser.add_argument('--output_csv', required=True, help="Output CSV file (can be same as input)")
    parser.add_argument('--id_column', default='id', help="Column with OpenAlex work IDs")
    parser.add_argument('--email', help="Your email for OpenAlex API")
    parser.add_argument('--log', help="Log file")
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)] + ([logging.FileHandler(args.log)] if args.log else [])
    )

    # Read CSV and identify bad rows
    with open(args.input_csv, newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)
        fieldnames = reader.fieldnames

    bad_indices = []
    bad_ids = []
    for i, row in enumerate(rows):
        if row_has_html_escape(row):
            bad_indices.append(i)
            full_id = row[args.id_column]
            if full_id.startswith("http"):
                openalex_id = full_id.rstrip("/")
            else:
                openalex_id = f"https://openalex.org/{full_id}"
            bad_ids.append(openalex_id)

    logging.info(f"Found {len(bad_indices)} rows with HTML escapes.")

    # Batch redownload and replace bad rows
    id_to_index = {id_: idx for idx, id_ in zip(bad_indices, bad_ids)}
    for id_batch in chunked(bad_ids, 50):
        works = fetch_openalex_works_batch(id_batch, email=args.email)
        # Map OpenAlex id to work (API returns full URLs)
        work_map = {w["id"]: w for w in works if "id" in w}
        for openalex_id in id_batch:
            idx = id_to_index.get(openalex_id)
            work = work_map.get(openalex_id)
            if not work:
                logging.warning(f"Could not redownload {openalex_id}, leaving row as is.")
                continue
            new_row = {}
            for col in fieldnames:
                val = get_nested(work, col)
                if col == "title":  # or the column you want to check
                    print("RAW:", repr(val))
                val = recursive_unescape(val)
                if col == "title":
                    print("UNESCAPED:", repr(val))
                if isinstance(val, (list, dict)):
                    val = json.dumps(val, ensure_ascii=False)
                new_row[col] = val
            rows[idx] = new_row
            logging.info(f"Replaced row {idx} for {openalex_id}")

    # Write output
    with open(args.output_csv, "w", newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    logging.info(f"Saved fixed CSV to {args.output_csv}")

if __name__ == "__main__":
    main()