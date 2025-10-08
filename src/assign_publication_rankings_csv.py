import os
import sys
import logging
import pandas as pd
import argparse
import re
from rapidfuzz import fuzz, process

def normalize_venue_name(name):
    if not isinstance(name, str):
        return ""
    # Remove years (e.g., 2023, 1999)
    name = re.sub(r'\b(19|20)\d{2}\b', '', name)
    # Remove numbering words (1st, 2nd, 3rd, 4th, etc.)
    name = re.sub(r'\b\d+(st|nd|rd|th)\b', '', name, flags=re.IGNORECASE)
    # Remove acronyms in brackets (e.g., (ICAT))
    name = re.sub(r'\([A-Z0-9]{2,}\)', '', name)
    # Remove special characters and spaces
    name = re.sub(r'[^a-zA-Z0-9]', '', name)
    # Lowercase
    return name.lower()

def normalize_issn(raw: str, create_array: bool = True) -> str | None:
    """
    Canonical ISSN (no hyphen): 8 chars, digits with optional final X.
    - Remove non-alphanumerics (hyphens, spaces, etc.)
    - Uppercase final X
    - Left-pad with zeros to length 8 if shorter
    - Return None if cannot be normalized to 8 chars matching \d{7}[\dX]
    """
    if raw is None:
        return None

    if create_array:
        raw_list = raw.split(", ")
        for idx in range(0, len(raw_list)):
            s = re.sub(r"[^0-9xX]", "", str(raw_list[idx])).upper()  # e.g., '0038-093x' -> '0038093X'
            if len(s) < 8:
                s = s.zfill(8)  # add leading zeros
            raw_list[idx] = s
            # if len(s) != 8 or not re.fullmatch(r"\d{7}[\dX]", s):
            #     return None
        return raw_list
    else:
        s = re.sub(r"[^0-9xX]", "", str(raw)).upper()  # e.g., '0038-093x' -> '0038093X'
        if len(s) < 8:
            s = s.zfill(8)  # add leading zeros
        return s


def extract_acronym(name):
    """Extract acronym in brackets, e.g. 'International Conference on AI (ICAT)' -> 'ICAT'"""
    match = re.search(r'\(([A-Z0-9]{2,})\)', name)
    return match.group(1) if match else ""

def get_conference_intervals(conf_row, year_cols):
    years = sorted([int(col) for col in year_cols if pd.notnull(conf_row[col]) and conf_row[col] != ""])
    intervals = []
    for i, year in enumerate(years):
        start = year
        end = years[i+1] - 1 if i+1 < len(years) else 9999
        intervals.append((start, end, conf_row[str(year)]))
    return intervals

def get_journal_intervals(journal_row, quartile_cols):
    years = sorted([int(col.replace("Quartile - ", "")) for col in quartile_cols if pd.notnull(journal_row[col]) and journal_row[col] != ""])
    intervals = []
    for i, year in enumerate(years):
        start = year
        end = years[i+1] - 1 if i+1 < len(years) else 9999
        quartile = journal_row[f"Quartile - {year}"]
        hindex = journal_row.get(f"H index - {year}", "")
        intervals.append((start, end, quartile, hindex))
    return intervals

def main():
    parser = argparse.ArgumentParser(description="Assign publication rankings from journal and/or conference databases, output to CSV.")
    parser.add_argument('--journal_csv', help='Path to journal rankings CSV')
    parser.add_argument('--conference_csv', help='Path to conference rankings CSV')
    parser.add_argument('--publications_csv', required=True, help='Path to publications CSV')
    parser.add_argument('--output_csv', required=True, help='Output CSV file for results')
    parser.add_argument('--issn_column', help='Name of the ISSN column in publications CSV (optional)')
    parser.add_argument('--venue_column', default='venue', help='Name of venue column in publications CSV')
    parser.add_argument('--year_column', default='year', help='Name of year column in publications CSV')
    parser.add_argument('--start', type=int, default=0, help='Start index of publications to process')
    parser.add_argument('--end', type=int, default=None, help='End index (exclusive) of publications to process')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    parser.add_argument('--mode', choices=['journal', 'conference', 'both'], default='both', help='Which ranking(s) to assign')
    args = parser.parse_args()

    os.makedirs("log", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler("log/assign_publication_rankings.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Read publications CSV
    try:
        pubs_df = pd.read_csv(args.publications_csv, dtype=str).fillna("")
    except Exception as e:
        logging.error(f"Error reading publications CSV: {e}")
        return

    # Prepare output columns
    if args.mode in ['journal', 'both']:
        if not args.journal_csv:
            logging.error("Journal mode requires --journal_csv.")
            return
        pubs_df["scimagoRank"] = ""
        pubs_df["jHindex"] = ""
    if args.mode in ['conference', 'both']:
        logging.info("Conference ranking mode enabled.")
        if "coreRank" not in pubs_df.columns:
            pubs_df["coreRank"] = ""
        if "conferenceConfidence" not in pubs_df.columns:
            pubs_df["conferenceConfidence"] = ""

    # --- Journal logic ---
    if args.mode in ['journal', 'both']:
        if not args.journal_csv:
            logging.error("Journal mode requires --journal_csv.")
            return
        try:
            journal_df = pd.read_csv(args.journal_csv, dtype=str).fillna("")
        except Exception as e:
            logging.error(f"Error reading journal CSV: {e}")
            return
 
        journal_lookup = {}
        quartile_cols = [col for col in journal_df.columns if col.startswith("Quartile - ")]

        skipped, dup = 0, 0
        for _, row in journal_df.iterrows():
            issn_val = row.get("Issn", "")
            if issn_val == "00380938, 1573093X":
                print("Nasao ga svemu")
                if not normalize_issn(issn_val):
                    print("Ne razdvaja zareze")
            norm = normalize_issn(issn_val)
            if not norm:
                skipped += 1
                continue
            for issn in norm:
                if issn in journal_lookup:
                    dup += 1
                journal_lookup[issn] = row  # keep last occurrence; change if you prefer first

        logging.info(
            f"Journal lookup: {len(journal_lookup)} unique ISSNs "
            f"(skipped {skipped} invalid, {dup} duplicates after normalization)"
        )

        if args.issn_column and args.issn_column in pubs_df.columns:
            for idx, pub in pubs_df.iterrows():
                pub_id = pub.get("id", "")

                #if pub_id != "https://openalex.org/W2142234851":
                #    continue

                year_val = pub.get(args.year_column, "")
                try:
                    pub_year = int(float(year_val)) if year_val else None
                except Exception:
                    pub_year = None
                journal_rank = None
                jhindex = None
                # Parse ISSNs from the single column
                issn_column_value = pub.get(args.issn_column, "")
                if not issn_column_value or issn_column_value.lower() == "nan":
                    logging.debug(f"Publication idx {idx} id {pub_id} has no ISSNs.")
                    continue

                try:
                    issn_list = eval(issn_column_value) if isinstance(issn_column_value, str) else []
                    if not isinstance(issn_list, list):
                        logging.warning(f"Publication idx {idx} id {pub_id} has invalid ISSN format: {issn_column_value}")
                        continue
                except Exception as e:
                    logging.warning(f"Error parsing ISSNs for publication idx {idx} id {pub_id}: {e}")
                    continue

                logging.debug(f"Publication idx {idx} id {pub_id} ISSNs: {issn_list} year: {pub_year}")
                for issn in issn_list:
                    print(issn)
                    issn = normalize_issn(issn, False)
                    print(issn)
                    if not issn:
                        logging.debug(f"Publication idx {idx} id {pub_id} has invalid ISSN: {issn}")
                        continue 
                    journal_row = journal_lookup.get(issn)
                    if journal_row is not None:
                        intervals = get_journal_intervals(journal_row, quartile_cols)
                        logging.debug(f"Journal ISSN {issn} intervals: {intervals}")
                        for start, end, quartile, hindex in intervals:
                            if pub_year and start <= pub_year <= end:
                                journal_rank = quartile
                                jhindex = hindex
                                logging.info(f"Assigned journal rank '{quartile}' and h-index '{hindex}' to pub idx {idx} (id {pub_id}) for year {pub_year} (ISSN {issn})")
                                break
                        if journal_rank or jhindex:
                            break
                
                pubs_df.at[idx, "scimagoRank"] = journal_rank if journal_rank else ""
                pubs_df.at[idx, "jHindex"] = jhindex if jhindex else ""
        else:
            logging.warning("No ISSN column provided or found in the publications CSV. Skipping journal ranking.")

    # --- Conference logic ---
    if args.mode in ['conference', 'both']:
        if not args.conference_csv:
            logging.error("Conference mode requires --conference_csv.")
            return
        try:
            conf_df = pd.read_csv(args.conference_csv, dtype=str).fillna("")
        except Exception as e:
            logging.error(f"Error reading conference CSV: {e}")
            return

        conf_df["norm_name"] = conf_df["conference name"].apply(normalize_venue_name)
        conf_df["abbr_norm"] = conf_df["conference abbreviation"].fillna("").apply(normalize_venue_name)
        conf_year_cols = [col for col in conf_df.columns if col.isdigit()]

        pubs_df["norm_venue"] = pubs_df[args.venue_column].apply(normalize_venue_name)
        pubs_df["venue_acronym"] = pubs_df[args.venue_column].apply(extract_acronym)

        norm_venues = pubs_df["norm_venue"].unique().tolist()
        logging.info(f"Total unique normalized venues in publications: {len(norm_venues)}")
        logging.info(f"Total conferences to match: {len(conf_df)}")

        for conf_idx, conf_row in conf_df.iterrows():
            conf_norm = conf_row["norm_name"]
            conf_abbr = conf_row["abbr_norm"]
            matches = process.extract(conf_norm, norm_venues, scorer=fuzz.ratio, score_cutoff=90)
            logging.debug(f"Conference '{conf_row['conference name']}' ({conf_norm}) matches: {matches}")
            for match_name, score, _ in matches:
                matched_pubs = pubs_df[pubs_df["norm_venue"] == match_name]
                for pub_idx, pub_row in matched_pubs.iterrows():
                    pub_acronym = pub_row["venue_acronym"]
                    abbr_match = (conf_abbr and pub_acronym and conf_abbr == pub_acronym) or not pub_acronym
                    if abbr_match:
                        pubs_df.at[pub_idx, "conferenceConfidence"] = score
                        pub_year = pub_row.get(args.year_column, "")
                        try:
                            pub_year = int(float(pub_year)) if pub_year else None
                        except Exception:
                            pub_year = None
                        intervals = get_conference_intervals(conf_row, conf_year_cols)
                        logging.debug(f"Pub idx {pub_idx} '{pub_row[args.venue_column]}' year {pub_year} intervals: {intervals}")
                        for start, end, rank in intervals:
                            if pub_year and start <= pub_year <= end:
                                pubs_df.at[pub_idx, "coreRank"] = rank
                                logging.info(f"Assigned conference rank '{rank}' to pub idx {pub_idx} ({pub_row[args.venue_column]}) for year {pub_year}")
                                break

        pubs_df = pubs_df.drop(columns=["norm_venue", "venue_acronym"], errors="ignore")

    # Save to output CSV (can be same as input)
    try:
        pubs_df.to_csv(args.output_csv, index=False)
        logging.info(f"Saved results to {args.output_csv}")
    except Exception as e:
        logging.error(f"Failed to save CSV: {e}")

    logging.info("Script finished.")

if __name__ == "__main__":
    main()