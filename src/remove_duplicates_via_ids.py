import pandas as pd
import argparse
import logging
import sys
import re
from rapidfuzz import fuzz, process

def setup_logging(verbose, log_file):
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers
    )

def normalize_title(title):
    # Lowercase, remove punctuation, but keep all letters (including Cyrillic, etc.)
    return re.sub(r'[^\w\s]', '', title.lower(), flags=re.UNICODE).strip()

def is_imenik(val):
    return str(val).strip() in {"1", "1.0", "True", "true"}

def main():
    parser = argparse.ArgumentParser(description="Deduplicate publications by DOI, MAG, and title.")
    parser.add_argument('--csv', required=True, help='Input CSV file')
    parser.add_argument('--originals', required=True, help='Output CSV for originals')
    parser.add_argument('--duplicates', required=True, help='Output CSV for duplicates')
    parser.add_argument('--log', default="deduplicate_publications.log", help='Log file')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    args = parser.parse_args()

    setup_logging(args.verbose, args.log)

    try:
        df = pd.read_csv(args.csv, dtype=str).fillna("")
    except Exception as e:
        logging.error(f"Failed to read CSV: {e}")
        sys.exit(1)

    df["ids.doi"] = df["ids.doi"].str.lower().str.strip()
    df["ids.mag"] = df["ids.mag"].str.lower().str.strip()
    df["display_name"] = df["display_name"].str.strip()

    df["removedDoi"] = ""
    df["removedMag"] = ""
    df["removedTitle"] = ""

    originals = []
    duplicates = []

    # 1. Deduplicate by DOI
    doi_groups = df.groupby("ids.doi")
    for doi, group in doi_groups:
        if doi == "" or len(group) == 1:
            continue
        has_id = group["id"].str.strip() != ""
        if has_id.any():
            for idx in group.index:
                if group.at[idx, "id"].strip() == "":
                    df.at[idx, "removedDoi"] = 1
                    duplicates.append(idx)
                    logging.info(f"Removed row {idx} as DOI duplicate (DOI: '{doi}'), no id present.")
        # else: if no id present, do not remove any

    # 2. Deduplicate by MAG (excluding already marked as duplicate)
    mag_groups = df.loc[~df.index.isin(duplicates)].groupby("ids.mag")
    for mag, group in mag_groups:
        if mag == "" or len(group) == 1:
            continue
        has_id = group["id"].str.strip() != ""
        if has_id.any():
            for idx in group.index:
                if group.at[idx, "id"].strip() == "":
                    df.at[idx, "removedMag"] = 1
                    duplicates.append(idx)
                    logging.info(f"Removed row {idx} as MAG duplicate (MAG: '{mag}'), no id present.")
        # else: if no id present, do not remove any

    # 3. Deduplicate by fuzzy title (excluding already marked as duplicate) using blocking
    title_candidates = df.loc[~df.index.isin(duplicates)].reset_index()
    title_candidates["norm_title"] = title_candidates["display_name"].apply(normalize_title)
    empty_norm = title_candidates[title_candidates["norm_title"] == ""]
    if not empty_norm.empty:
        logging.warning(f"{len(empty_norm)} titles normalized to empty string. Examples: {empty_norm['display_name'].head(5).tolist()}")

    # Skip empty normalized titles
    title_candidates = title_candidates[title_candidates["norm_title"] != ""]

    title_candidates["block"] = title_candidates["norm_title"].str[:4]  # Block by first 4 chars

    logging.info(f"Starting fuzzy title deduplication with {len(title_candidates)} candidates using blocking.")

    used = set()
    match_count = 0
    removed_count = 0

    debug_title = "Albanian Journal of Trauma and Emergency Surgery AJTES Official Publication of the Albanian Society for Trauma and Emergency Surgery -ASTES Chairman of the editorial board:"


    for block, block_df in title_candidates.groupby("block"):

        logging.debug(f"Processing block '{block}' with {len(block_df)} titles.")
        block_titles = block_df["norm_title"].tolist()
        block_indices = block_df["index"].tolist()
        if len(block_titles) < 2:
            logging.debug(f"Skipping block '{block}' (only {len(block_titles)} title).")
            continue

        matrix = process.cdist(
            block_titles, block_titles, scorer=fuzz.ratio, score_cutoff=95, workers=-1
        )
        match_in_block = 0
        n = len(block_titles)
        for i in range(n):
            for j in range(i + 1, n):
                score = matrix[i, j]
                if score < 95:
                    continue
                idx_i = block_indices[i]
                idx_j = block_indices[j]
                if idx_i in used or idx_j in used:
                    continue
                match_count += 1
                match_in_block += 1
                group = block_df.iloc[[i, j]]
                logging.debug(
                    f"Block '{block}': Fuzzy match (score={score}) between idx_i={idx_i} and idx_j={idx_j} "
                    f"('{group.iloc[0]['display_name']}' <-> '{group.iloc[1]['display_name']}')"
                )
                has_id = group["id"].str.strip() != ""
                if has_id.any():
                    for k, idx in zip(group.index, [idx_i, idx_j]):
                        if not has_id.loc[k]:
                            other_pos = 1 if group.index.get_loc(k) == 0 else 0
                            other_title = group.iloc[other_pos]["display_name"]
                            df.at[idx, "removedTitle"] = 1
                            duplicates.append(idx)
                            removed_count += 1
                            logging.info(
                                f"Removed row {idx} as TITLE duplicate in block '{block}' "
                                f"(Title: '{group.at[k, 'display_name']}', match: '{other_title}'), no id present."
                            )
                            used.add(idx)
                # else:
                #     for k, idx in zip(group.index, [idx_i, idx_j]):
                #         df.at[idx, "removedTitle"] = 1
                #         duplicates.append(idx)
                #         removed_count += 1
                #         other_pos = 1 if group.index.get_loc(k) == 0 else 0
                #         other_title = group.iloc[other_pos]["display_name"]
                #         logging.info(
                #             f"Removed row {idx} as TITLE duplicate in block '{block}' "
                #             f"(Title: '{group.at[k, 'display_name']}', match: '{other_title}'), no id present in group."
                #         )
                #         used.add(idx)
                else:
                    # Neither has an id, check addedViaImenik logic
                    added_via_imenik = group["addedViaImenik"].fillna("0").astype(str).values
                    a = is_imenik(added_via_imenik[0])
                    b = is_imenik(added_via_imenik[1])

                    if a ^ b:
                        # Only one has addedViaImenik == "1", remove the other
                        remove_pos = 0 if not a else 1
                        keep_pos = 1 - remove_pos
                        idx = [idx_i, idx_j][remove_pos]
                        other_title = group.iloc[keep_pos]["display_name"]
                        df.at[idx, "removedTitle"] = 1
                        duplicates.append(idx)
                        removed_count += 1
                        logging.info(
                            f"Removed row {idx} as TITLE duplicate in block '{block}' "
                            f"(Title: '{group.iloc[remove_pos]['display_name']}', match: '{other_title}'), kept one with addedViaImenik=1."
                        )
                        used.add(idx)
                    elif a and b:
                        # Both have addedViaImenik == "1", remove only the second
                        idx = idx_j
                        other_title = group.iloc[0]["display_name"]
                        df.at[idx, "removedTitle"] = 1
                        duplicates.append(idx)
                        removed_count += 1
                        logging.info(
                            f"Removed row {idx} as TITLE duplicate in block '{block}' "
                            f"(Title: '{group.iloc[1]['display_name']}', match: '{other_title}'), both have addedViaImenik=1, removed second only."
                        )
                        used.add(idx)
                    else:
                        # Neither has addedViaImenik == "1", remove both
                        for pos, idx in enumerate([idx_i, idx_j]):
                            other_pos = 1 - pos
                            other_title = group.iloc[other_pos]["display_name"]
                            df.at[idx, "removedTitle"] = 1
                            duplicates.append(idx)
                            removed_count += 1
                            logging.info(
                                f"Removed row {idx} as TITLE duplicate in block '{block}' "
                                f"(Title: '{group.iloc[pos]['display_name']}', match: '{other_title}'), neither has id or addedViaImenik=1, removed both."
                            )
                            used.add(idx)
        if match_in_block > 0:
            logging.info(f"Block '{block}': {match_in_block} fuzzy match pairs found.")

    logging.info(f"Fuzzy title deduplication complete. {match_count} match pairs processed, {removed_count} rows marked as duplicates.")

    # 4. Write outputs
    df_duplicates = df.loc[duplicates].copy()
    df_duplicates["removedDoi"] = df_duplicates["removedDoi"].astype(bool)
    df_duplicates["removedMag"] = df_duplicates["removedMag"].astype(bool)
    df_duplicates["removedTitle"] = df_duplicates["removedTitle"].astype(bool)
    cols_to_remove = ["removedDoi", "removedMag", "removedTitle"]
    df_originals = df.drop(index=duplicates).drop(columns=cols_to_remove).copy()

    try:
        df_originals.to_csv(args.originals, index=False)
        df_duplicates.to_csv(args.duplicates, index=False)
        logging.info(f"Saved {len(df_originals)} originals to {args.originals}")
        logging.info(f"Saved {len(df_duplicates)} duplicates to {args.duplicates}")
    except Exception as e:
        logging.error(f"Failed to write output files: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()