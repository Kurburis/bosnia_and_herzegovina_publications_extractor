import os
import sys
import json
import logging
import argparse
import pandas as pd
import requests
import re
import time
from rapidfuzz import fuzz, process
from more_itertools import chunked  # pip install more-itertools

def normalize_string(s):
    if not isinstance(s, str):
        return ""
    return re.sub(r'[^a-z0-9]', '', s.lower())

def setup_logging(verbose, log_file):
    # Remove all handlers associated with the root logger object.
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers
    )

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
        return flat
    if isinstance(value, dict):
        return value
    return value if value is not None else ""

def flatten_json(y, parent_key='', sep='.'):
    items = []
    for k, v in y.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def fetch_openalex_by_ids(dois=None, mags=None, email=None):
    url = "https://api.openalex.org/works"
    filters = []
    if dois:
        filters.append("doi:" + "|".join(dois))
    if mags:
        filters.append("mag:" + "|".join(mags))
    params = {
        "filter": ",".join(filters),
        "per-page": (len(dois or []) + len(mags or [])) or 25,
    }
    if email:
        params["mailto"] = email
    results = []
    next_url = url
    next_params = params.copy()
    try:
        while next_url:
            r = requests.get(next_url, params=next_params, timeout=30)
            r.raise_for_status()
            data = r.json()
            results.extend(data.get("results", []))
            # OpenAlex uses 'meta' for pagination info
            next_url = data.get("meta", {}).get("next_cursor")
            if next_url:
                # For cursor-based pagination, set 'cursor' param and remove others
                next_params = {"cursor": next_url}
                if email:
                    next_params["mailto"] = email
            else:
                break
    except Exception as e:
        logging.warning(f"OpenAlex API batch error: {e}")
        return []
    return results

def fetch_openalex_by_titles(titles, email=None, per_title=50, batch_size=1):
    """Query OpenAlex for top N cited papers per title, one at a time (no batching)."""
    results = []
    for title in titles:
        url = "https://api.openalex.org/works"
        params = {
            "search": title,
            "sort": "cited_by_count:desc",
            "per-page": per_title
        }
        if email:
            params["mailto"] = email
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            results.append(data.get("results", []))
        except Exception as e:
            logging.warning(f"OpenAlex API error for title '{title}': {e}")
            results.append([])
        time.sleep(0.5)
    return results

def main():
    parser = argparse.ArgumentParser(description="Find and update publications from imenik using OpenAlex API.")
    parser.add_argument('--csv', required=True, help='Path to input CSV file')
    parser.add_argument('--output', help='Path to output CSV file (default: overwrite input)')
    parser.add_argument('--log', default="find_imenik_publication_oa_variant.log", help='Log file location')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    parser.add_argument('--email', required=True, help='Your email for OpenAlex API')
    parser.add_argument('--batch_size', type=int, default=25, help='Batch size for id queries')
    parser.add_argument('--per_title', type=int, default=20, help='Number of results per title')
    args = parser.parse_args()

    setup_logging(args.verbose, args.log)

    # Now logging will work for all subsequent calls
    logging.info("This will go to both stdout and the log file.")

    # 1. Load CSV
    logging.info(f"Loading CSV from {args.csv}")
    df = pd.read_csv(args.csv, dtype=str).fillna("")
    if "addedViaImenik" not in df.columns:
        logging.error("Input CSV must have 'addedViaImenik' column.")
        sys.exit(1)
    if "semantic_id" not in df.columns:
        logging.error("Input CSV must have 'semantic_id' column.")
        sys.exit(1)

    # 2. Filter rows with addedViaImenik non-empty and not nan/none
    mask = (
        df["addedViaImenik"].astype(str).str.strip().str.lower().replace("nan", "").replace("none", "") != ""
    ) & (
        df["id"].astype(str).str.strip() == ""
    )
    imenik_rows = df[mask].copy()
    logging.info(f"Found {len(imenik_rows)} rows with addedViaImenik=True.")

    # 3. Batch fetch by DOI and MAG
    doi_to_idx = {}
    mag_to_idx = {}
    for idx, row in imenik_rows.iterrows():
        doi = row.get("ids.doi", "").strip().lower()
        mag = row.get("ids.mag", "").strip()
        logging.info(f"Row {idx} - DOI: {doi}, MAG: {mag}")
        if doi:
            doi_to_idx[doi] = idx
        elif mag:
            mag_to_idx[mag] = idx

    updated_indices = set()
    all_dois = list(doi_to_idx.keys())
    all_mags = list(mag_to_idx.keys())

    # # Batch fetch DOIs
    for doi_batch in chunked(all_dois, args.batch_size):
        results = fetch_openalex_by_ids(dois=doi_batch, email=args.email)
        for result in results:
            doi = (result.get("doi") or "").lower()
            logging.info(f"Processing DOI: {doi}")
            idx = doi_to_idx.get(doi)
            if idx is not None:
                for col in df.columns:
                    if col in ["addedViaImenik", "semantic_id"]:
                        continue
                    df.at[idx, col] = get_nested(result, col)
                df.at[idx, "addedViaImenik"] = imenik_rows.at[idx, "addedViaImenik"]
                df.at[idx, "semantic_id"] = imenik_rows.at[idx, "semantic_id"]
                updated_indices.add(idx)
                logging.info(f"Row {idx} updated from OpenAlex by DOI.")

    # Exclude MAGs for rows already updated by DOI
    mag_to_idx = {mag: idx for mag, idx in mag_to_idx.items() if idx not in updated_indices}
    all_mags = list(mag_to_idx.keys())

    logging.info(f"Remaining MAGs to process after DOI updates: {len(all_mags)}")
    # Now batch fetch MAGs only for those not already updated
    for mag_batch in chunked(all_mags, args.batch_size):
        results = fetch_openalex_by_ids(mags=mag_batch, email=args.email)
        for result in results:
            mag = str(result.get("ids", {}).get("mag", ""))
            logging.info(f"Processing MAG: {mag}")
            idx = mag_to_idx.get(mag)
            if idx is not None:
                for col in df.columns:
                    if col in ["addedViaImenik", "semantic_id"]:
                        continue
                    df.at[idx, col] = get_nested(result, col)
                df.at[idx, "addedViaImenik"] = imenik_rows.at[idx, "addedViaImenik"]
                df.at[idx, "semantic_id"] = imenik_rows.at[idx, "semantic_id"]
                updated_indices.add(idx)
                logging.info(f"Row {idx} updated from OpenAlex by MAG.")

    # #4. After all, try to find by title for unmatched
    # unmatched = [
    #     (idx, row["display_name"])
    #     for idx, row in imenik_rows.iterrows()
    #     if idx not in updated_indices and str(row["display_name"]).strip() != ""
    # ]
    # logging.info(f"Total unmatched rows to try by title: {len(unmatched)}")
    # if unmatched:
    #     logging.info(f"Trying to match {len(unmatched)} rows by title using OpenAlex API and fuzzy matching.")
    #     for idx, title in unmatched:
    #         if not str(title).strip():
    #             logging.info(f"Skipping idx={idx} because title is empty after stripping.")
    #             continue
    #         logging.info(f"Querying OpenAlex for idx={idx} with title: '{title}'")
    #         api_results = fetch_openalex_by_titles([title], email=args.email, per_title=args.per_title, batch_size=1)
    #         candidates = api_results[0] if api_results else []
    #         if not candidates:
    #             logging.info(f"No candidates returned from OpenAlex for idx={idx} title: '{title}'")
    #             continue
    #         norm_title = normalize_string(title)
    #         logging.info(f"Normalized search title for idx={idx}: '{norm_title}'")
    #         best_score = 0
    #         best_cand = None
    #         all_scores = []
    #         for cand in candidates:
    #             cand_title = cand.get("title") or cand.get("display_name", "")
    #             if not cand_title or not cand_title.strip():
    #                 logging.info(f"Skipping candidate with empty title for idx={idx}")
    #                 continue
    #             norm_cand_title = normalize_string(cand_title)
    #             score = fuzz.ratio(norm_title, norm_cand_title)
    #             all_scores.append((score, cand_title))
    #             logging.info(f"Candidate title: '{cand_title}' (normalized: '{norm_cand_title}'), score: {score}")
    #             if score > best_score:
    #                 best_score = score
    #                 best_cand = cand
    #         if not all_scores:
    #             logging.info(f"All candidates had empty titles for idx={idx} title: '{title}'")
    #         logging.info(f"Title idx={idx} '{title}' candidate scores: " +
    #                      "; ".join([f"{score}: '{ctitle}'" for score, ctitle in all_scores]))
    #         if best_score >= 95 and best_cand:
    #             logging.info(f"Best match for idx={idx} is '{best_cand.get('title') or best_cand.get('display_name', '')}' with score {best_score}")
    #             for col in df.columns:
    #                 if col in ["addedViaImenik", "semantic_id"]:
    #                     continue
    #                 df.at[idx, col] = get_nested(best_cand, col)
    #             df.at[idx, "addedViaImenik"] = imenik_rows.at[idx, "addedViaImenik"]
    #             df.at[idx, "semantic_id"] = imenik_rows.at[idx, "semantic_id"]
    #             logging.info(f"Row {idx} updated from OpenAlex by fuzzy title match (score={best_score}).")
    #         else:
    #             logging.info(f"Row {idx} not matched by fuzzy title (best score={best_score}).")
    #         time.sleep(1)  # Be polite to API

    # 5. Save result
    output_file = args.output if args.output else args.csv
    logging.info(f"Saving result to {output_file}")
    df.to_csv(output_file, index=False)
    logging.info("Done.")

if __name__ == "__main__":
    main()