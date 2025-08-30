import requests
import csv
import argparse
import logging
import sys
import time
import subprocess

def send_notification(message):
    try:
        subprocess.run(['notify-send', message], check=True)
    except Exception as e:
        logging.warning(f"Could not send notification: {e}")

def fetch_authors(country_code, output_csv, per_page=200, verbose=False, email=None, max_authors=None):
    base_url = "https://api.openalex.org/authors"
    params = {
        "filter": f"last_known_institutions.country_code:countries/{country_code}",
        "per-page": per_page,
        "cursor": "*"
    }
    if email:
        params["mailto"] = email

    cursor = "*"
    total = None
    count = 0
    buffer = []
    headers = None

    with open(output_csv, "w", newline='', encoding="utf-8") as csvfile:
        writer = None

        while True:
            try:
                params["cursor"] = cursor
                response = requests.get(base_url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                logging.error(f"API request failed: {e}")
                break

            if total is None:
                total = data.get("meta", {}).get("count", "?")
                logging.info(f"Total authors to fetch: {total}")

            results = data.get("results", [])
            if not results:
                logging.info("No more results.")
                break

            # Dynamically determine headers from the first author
            if headers is None and results:
                def get_all_paths(d, parent_key='', sep='.'):
                    paths = []
                    for k, v in d.items():
                        new_key = f"{parent_key}{sep}{k}" if parent_key else k
                        if isinstance(v, dict):
                            paths.extend(get_all_paths(v, new_key, sep=sep))
                        elif isinstance(v, list) and v and isinstance(v[0], dict):
                            # For list of dicts, get all keys from first dict
                            paths.extend(get_all_paths(v[0], new_key, sep=sep))
                        else:
                            paths.append(new_key)
                    return paths
                headers = sorted(get_all_paths(results[0]))
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()

            for author in results:
                row = {}
                for h in headers:
                    val = get_nested(author, h)
                    # For CSV, convert lists/dicts to JSON strings for readability
                    if isinstance(val, (list, dict)):
                        import json
                        val = json.dumps(val, ensure_ascii=False)
                    row[h] = val
                buffer.append(row)
                count += 1
                if verbose and count % 100 == 0:
                    logging.info(f"Fetched {count} authors...")
                if max_authors and count >= max_authors:
                    logging.info(f"Reached max_authors limit: {max_authors}")
                    # Write remaining buffer before exit
                    for buf_row in buffer:
                        writer.writerow(buf_row)
                    send_notification(f"OpenAlex: Exported and notified at {count} authors (final)")
                    return

                # Save every 1000 authors
                if count % 1000 == 0:
                    for buf_row in buffer:
                        writer.writerow(buf_row)
                    buffer.clear()
                    logging.info(f"Exported {count} authors so far.")
                    send_notification(f"OpenAlex: Exported and notified at {count} authors")

            cursor = data.get("meta", {}).get("next_cursor")
            if not cursor:
                logging.info("No next cursor, finished fetching.")
                break

            # Be polite to the API
            time.sleep(0.25)

        # Write any remaining authors in buffer
        if buffer and writer is not None:
            for buf_row in buffer:
                writer.writerow(buf_row)
            send_notification(f"OpenAlex: Exported and notified at {count} authors (final)")

    logging.info(f"Finished. Total authors fetched: {count}")

def get_nested(d, path):
    """
    Get a nested value from a dict using dot notation, return '' if not found.
    If value is a list of dicts, return a list of the target key from all dicts.
    If value is a list of primitives, return the list.
    """
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
        if len(flat) == 1:
            return flat[0]
        return flat
    if isinstance(value, dict):
        return value  # or json.dumps(value, ensure_ascii=False) if you want a string
    return value if value is not None else ""

def main():
    parser = argparse.ArgumentParser(description="Fetch all OpenAlex authors with Bosnia and Herzegovina affiliations and save to CSV.")
    parser.add_argument('--country', default="ba", help="Country code (default: ba for Bosnia and Herzegovina)")
    parser.add_argument('--output', required=True, help="Output CSV file path")
    parser.add_argument('--per_page', type=int, default=200, help="Results per page (max 200)")
    parser.add_argument('--verbose', action='store_true', help="Verbose logging")
    parser.add_argument('--email', help="Your email for polite API usage")
    parser.add_argument('--max_authors', type=int, help="Maximum number of authors to fetch (for testing)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    fetch_authors(
        country_code=args.country,
        output_csv=args.output,
        per_page=args.per_page,
        verbose=args.verbose,
        email=args.email,
        max_authors=args.max_authors
    )

if __name__ == "__main__":
    main()