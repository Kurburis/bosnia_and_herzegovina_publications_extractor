import requests
import argparse
import logging
import sys
import time
import json
import csv
import subprocess

def send_notification(message):
    try:
        subprocess.run(['notify-send', message], check=True)
    except Exception as e:
        logging.warning(f"Could not send notification: {e}")

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
        #if len(flat) == 1:
        #    return flat[0]
        return flat
    if isinstance(value, dict):
        return value  # or json.dumps(value, ensure_ascii=False) if you want a string
    return value if value is not None else ""

def get_all_paths(d, parent_key='', sep='.'):
    """Recursively get all dot-notated paths from a nested dict."""
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

def fetch_publications_for_author(author_id, email=None, per_page=200):
    """Fetch all publications for a given OpenAlex author ID."""
    base_url = f"https://api.openalex.org/works"
    params = {
        "filter": f"authorships.author.id:{author_id}",
        "per-page": per_page,
        "cursor": "*"
    }
    if email:
        params["mailto"] = email

    all_works = []
    cursor = "*"
    while True:
        try:
            params["cursor"] = cursor
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logging.error(f"API request failed for author {author_id}: {e}")
            break

        results = data.get("results", [])
        all_works.extend(results)

        next_cursor = data.get("meta", {}).get("next_cursor")
        if not next_cursor:
            break
        cursor = next_cursor
        time.sleep(0.5)  # Be polite to the API

    return all_works

def main():
    parser = argparse.ArgumentParser(description="Download OpenAlex publications for authors from a CSV or a single author ID.")
    parser.add_argument('--authors_csv', help="CSV file with OpenAlex author IDs")
    parser.add_argument('--author_id', help="Single OpenAlex author ID to fetch")
    parser.add_argument('--output', required=True, help="Output CSV file for publications")
    parser.add_argument('--per_page', type=int, default=200, help="Results per page (max 200)")
    parser.add_argument('--verbose', action='store_true', help="Verbose logging")
    parser.add_argument('--email', help="Your email for polite API usage")
    parser.add_argument('--max_authors', type=int, help="Maximum number of authors to process (for testing)")
    parser.add_argument('--start', type=int, default=0, help="Start index for authors in CSV (inclusive, 0-based)")
    parser.add_argument('--end', type=int, help="End index for authors in CSV (exclusive, 0-based)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    # Determine authors to process
    authors = []
    if args.author_id:
        authors = [args.author_id]
    elif args.authors_csv:
        with open(args.authors_csv, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            all_rows = [row for row in reader]
            selected_rows = all_rows[args.start:args.end] if args.end is not None else all_rows[args.start:]
            for row in selected_rows:
                author_id = row.get("id") or row.get("ids.openalex")
                if author_id:
                    authors.append(author_id)
                if args.max_authors and len(authors) >= args.max_authors:
                    break
    else:
        author_id = input("Enter OpenAlex author ID: ").strip()
        if author_id:
            authors = [author_id]
        else:
            logging.error("No author ID provided. Exiting.")
            return

    logging.info(f"Loaded {len(authors)} author(s)")

    buffer = []
    headers = None
    count = 0

    with open(args.output, "w", newline='', encoding="utf-8") as csvfile:
        writer = None

        for i, author_id in enumerate(authors, 1):
            logging.info(f"[{i}/{len(authors)}] Fetching publications for author: {author_id}")
            works = fetch_publications_for_author(author_id, email=args.email, per_page=args.per_page)
            if not works:
                logging.info(f"  No publications found for author {author_id}")
                continue

            # Dynamically determine headers from the first work
            if headers is None and works:
                headers = sorted(get_all_paths(works[0]))
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()

            for work in works:
                row = {}
                for h in headers:
                    val = get_nested(work, h)
                    # For CSV, convert lists/dicts to JSON strings for readability
                    if isinstance(val, (list, dict)):
                        val = json.dumps(val, ensure_ascii=False)
                    row[h] = val
                buffer.append(row)
                count += 1

                if args.verbose and count % 100 == 0:
                    logging.info(f"Fetched {count} publications...")

                # Save every 1000 publications
                if count % 1000 == 0:
                    for buf_row in buffer:
                        writer.writerow(buf_row)
                    buffer.clear()
                    logging.info(f"Exported {count} publications so far.")
                    send_notification(f"OpenAlex: Exported and notified at {count} publications")

            logging.info(f"  Fetched {len(works)} publications for author {author_id}")
            #time.sleep(0.25)  # Be polite to the API

        # Write any remaining buffer
        if buffer:
            for buf_row in buffer:
                writer.writerow(buf_row)
            logging.info(f"Exported final {len(buffer)} publications.")
            send_notification(f"OpenAlex: Exported and notified at {count} publications (final)")

    logging.info(f"Saved {count} publications to {args.output}")

if __name__ == "__main__":
    main()