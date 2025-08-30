import os
import sys
import json
import logging
import argparse
import pandas as pd
import mariadb
import re
from dotenv import load_dotenv
from rapidfuzz import fuzz, process

def normalize_string(s):
    if not isinstance(s, str):
        return ""
    return re.sub(r'[^a-z0-9]', '', s.lower())

def extract_external_ids(ext_ids_json):
    """Extract DOI, MAG, PubMed from externalIds JSON string."""
    doi, mag, pubmed = "", "", ""
    try:
        ids = json.loads(ext_ids_json) if ext_ids_json else {}
        doi = ids.get("DOI", "")
        mag = str(ids.get("MAG", "")).strip()
        pubmed = ids.get("PubMed", "")
        # Format as OpenAlex
        if doi and not doi.startswith("https://doi.org/"):
            doi = f"https://doi.org/{doi}".lower()
        if pubmed and not pubmed.startswith("https://pubmed.ncbi.nlm.nih.gov/"):
            pubmed = f"https://pubmed.ncbi.nlm.nih.gov/{pubmed}"
    except Exception as e:
        logging.warning(f"Could not parse externalIds: {e}")
    return doi, mag, pubmed

def main():
    parser = argparse.ArgumentParser(description="Compare Akademski Imenik DB with OpenAlex CSV and find unmatched publications.")
    parser.add_argument('--openalex_csv', required=True, help='CSV file with OpenAlex publications')
    parser.add_argument('--output_csv', required=True, help='CSV file to save unmatched Akademski Imenik publications')
    parser.add_argument('--db_table', default='publications', help='MariaDB table name for Akademski Imenik publications')
    parser.add_argument('--start', type=int, default=0, help='Start id (inclusive) for DB publications')
    parser.add_argument('--end', type=int, default=None, help='End id (exclusive) for DB publications')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    args = parser.parse_args()

    os.makedirs("log", exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler("log/sync_imenik_vs_openalex.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Step 1: Load OpenAlex CSV
    try:
        oa_df = pd.read_csv(args.openalex_csv, dtype=str).fillna("")
        logging.info(f"Loaded OpenAlex CSV: {args.openalex_csv} ({len(oa_df)} rows)")
    except Exception as e:
        logging.error(f"Failed to load OpenAlex CSV: {e}")
        return

    # Step 2: Connect to MariaDB and load publications table
    load_dotenv()
    db_user = os.getenv("MARIADB_USER")
    db_pass = os.getenv("MARIADB_PASS")
    db_host = os.getenv("MARIADB_HOST", "localhost")
    db_name = os.getenv("MARIADB_DB")
    table = args.db_table

    try:
        conn = mariadb.connect(
            user=db_user,
            password=db_pass,
            host=db_host,
            database=db_name
        )
        cursor = conn.cursor()
        # Use id column for filtering
        where_clause = ""
        if args.end is not None:
            where_clause = f"WHERE id >= {args.start} AND id < {args.end}"
        elif args.start > 0:
            where_clause = f"WHERE id >= {args.start}"
        query = f"SELECT * FROM {table} {where_clause}"
        logging.info(f"Executing DB query: {query}")
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        db_pubs = [dict(zip(columns, row)) for row in cursor.fetchall()]
        db_df = pd.DataFrame(db_pubs)
        logging.info(f"Loaded {len(db_df)} publications from DB table '{table}' with id range: start={args.start}, end={args.end}")
    except Exception as e:
        logging.error(f"Could not connect to MariaDB or load table: {e}")
        return

    # Step 3: Process externalIds column
    for col in ["doi_imenik", "mag_imenik", "pubmed_imenik"]:
        if col not in db_df.columns:
            db_df[col] = ""
    for idx, row in db_df.iterrows():
        doi, mag, pubmed = extract_external_ids(row.get("externalIds", ""))
        db_df.at[idx, "doi_imenik"] = doi
        db_df.at[idx, "mag_imenik"] = mag
        #db_df.at[idx, "pubmed_imenik"] = pubmed
        logging.debug(f"Row id={row.get('id')}: DOI={doi}, MAG={mag}")

    # Step 4: Compare with OpenAlex CSV by DOI, MAG, PubMed
    oa_doi = set(oa_df.get("ids.doi", pd.Series(dtype=str)).str.lower().dropna())
    oa_doi.discard("")  # Remove empty string
    oa_mag = set(oa_df.get("ids.mag", pd.Series(dtype=str)).dropna())
    oa_mag.discard("")

    mask = ~(
        db_df["doi_imenik"].str.lower().isin(oa_doi) |
        db_df["mag_imenik"].isin(oa_mag)
    )
    unmatched_df = db_df[mask].copy()
    logging.info(f"{len(unmatched_df)} publications remain after ID matching (out of {len(db_df)}).")
    logging.debug(f"Unmatched IDs after ID matching: {unmatched_df['id'].tolist()}")

    # Step 5: Fuzzy title matching (vectorized)
    oa_titles = oa_df.get("title", pd.Series(dtype=str)).fillna("").tolist()
    oa_titles_norm = [normalize_string(t) for t in oa_titles]
    unmatched_titles = unmatched_df.get("title", pd.Series(dtype=str)).fillna("").tolist()
    unmatched_titles_norm = [normalize_string(t) for t in unmatched_titles]

    if unmatched_titles_norm and oa_titles_norm:
        scores = process.cdist(
            unmatched_titles_norm, oa_titles_norm, scorer=fuzz.ratio, dtype=int
        )
        best_scores = scores.max(axis=1)
        keep_indices = unmatched_df.index[best_scores < 95]
        final_df = unmatched_df.loc[keep_indices]
        for idx, score in zip(unmatched_df.index, best_scores):
            logging.debug(f"Row id={unmatched_df.at[idx, 'id']}: best fuzzy score={score}")
    else:
        final_df = unmatched_df

    logging.info(f"{len(final_df)} publications remain after fuzzy title matching.")

    # Step 6: Save to output CSV
    try:
        final_df.to_csv(args.output_csv, index=False)
        logging.info(f"Saved unmatched publications to {args.output_csv}")
    except Exception as e:
        logging.error(f"Failed to save output CSV: {e}")

if __name__ == "__main__":
    main()