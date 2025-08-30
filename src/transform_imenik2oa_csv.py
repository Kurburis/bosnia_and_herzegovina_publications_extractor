import csv
import json
import argparse
import logging
import sys

def setup_logging(verbose, log_file):
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers
    )

def transform_row(row):
    try:
        # 1. Parse externalIds JSON and map to ids columns
        ids = {}
        if row.get("externalIds"):
            try:
                ext_ids = json.loads(row["externalIds"])
                if isinstance(ext_ids, dict):
                    if "DOI" in ext_ids:
                        ids["ids.doi"] = ext_ids["DOI"]
                    if "MAG" in ext_ids:
                        ids["ids.mag"] = ext_ids["MAG"]
            except Exception as e:
                logging.warning(f"Could not parse externalIds: {row.get('externalIds')} ({e})")

        # 2. venue: if empty, try journal.name
        venue = row.get("venue", "")
        if not venue or venue.lower() == "nan":
            try:
                journal = json.loads(row.get("journal", ""))
                if isinstance(journal, dict):
                    venue = journal.get("name", "") or venue
            except Exception as e:
                logging.warning(f"Could not parse journal: {row.get('journal')} ({e})")

        # 3. best_oa_location.source.issn: try publicationVenue JSON
        issn = ""
        if row.get("publicationVenue"):
            try:
                pub_venue = json.loads(row["publicationVenue"])
                if isinstance(pub_venue, dict):
                    issn_list = []
                    # Add main issn if present
                    if "issn" in pub_venue and pub_venue["issn"]:
                        if isinstance(pub_venue["issn"], list):
                            issn_list.extend(pub_venue["issn"])
                        else:
                            issn_list.append(pub_venue["issn"])
                    # Add alternate_issns if present
                    if "alternate_issns" in pub_venue and pub_venue["alternate_issns"]:
                        if isinstance(pub_venue["alternate_issns"], list):
                            issn_list.extend(pub_venue["alternate_issns"])
                        else:
                            issn_list.append(pub_venue["alternate_issns"])
                    if issn_list:
                        issn = json.dumps(issn_list, ensure_ascii=False)
            except Exception as e:
                logging.warning(f"Could not parse publicationVenue: {row.get('publicationVenue')} ({e})")

        # 4. best_oa_location.pdf_url: extract from openAccessPdf JSON property 'url'
        pdf_url = ""
        if row.get("openAccessPdf"):
            try:
                pdf_json = json.loads(row["openAccessPdf"])
                if isinstance(pdf_json, dict):
                    pdf_url = pdf_json.get("url", "")
            except Exception as e:
                logging.warning(f"Could not parse openAccessPdf: {row.get('openAccessPdf')} ({e})")

        out_row = {
            "semantic_id": row.get("url", ""),
            "display_name": row.get("title", ""),
            "ids.doi": f"https://doi.org/{ids['ids.doi']}" if ids.get("ids.doi") else "",
            "ids.mag": ids.get("ids.mag", ""),
            "abstract": row.get("abstract", ""),
            "best_oa_location.source.display_name": venue,
            "best_oa_location.source.issn": issn,
            "publication_year": row.get("year", ""),
            "referenced_works_count": row.get("referenceCount", ""),
            "cited_by_count": row.get("citationCount", ""),
            "best_oa_location.source.is_oa": row.get("isOpenAccess", ""),
            "best_oa_location.pdf_url": pdf_url,
            "publicationTypes": row.get("publicationTypes", ""),
            "authorships.author.display_name": row.get("author_names", ""),
            "category": row.get("category", "")
        }
        return out_row
    except Exception as e:
        logging.error(f"Error transforming row: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Transform Semantic Scholar CSV to OpenAlex-compatible CSV.")
    parser.add_argument('--input', required=True, help="Input CSV file from imenik/Semantic Scholar")
    parser.add_argument('--output', required=True, help="Output CSV file compatible with OpenAlex format")
    parser.add_argument('--log', help="Log file location")
    parser.add_argument('--verbose', action='store_true', help="Verbose logging")
    args = parser.parse_args()

    setup_logging(args.verbose, args.log)

    # Define output columns (OpenAlex compatible)
    columns = [
        "semantic_id",
        "display_name",
        "ids.doi",
        "ids.mag",
        "abstract",
        "best_oa_location.source.display_name",
        "best_oa_location.source.issn",
        "publication_year",
        "referenced_works_count",
        "cited_by_count",
        "best_oa_location.source.is_oa",
        "best_oa_location.pdf_url",
        "publicationTypes",
        "authorships.author.display_name",
        "category"
    ]

    count_in, count_out, count_err = 0, 0, 0

    with open(args.input, newline='', encoding='utf-8') as infile, \
         open(args.output, "w", newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=columns)
        writer.writeheader()

        for row in reader:
            count_in += 1
            try:
                out_row = transform_row(row)
                if out_row is not None:
                    writer.writerow(out_row)
                    count_out += 1
                else:
                    count_err += 1
            except Exception as e:
                logging.error(f"Failed to process row {count_in}: {e}")
                count_err += 1

    logging.info(f"Processed {count_in} rows. Written: {count_out}, Errors: {count_err}")

if __name__ == "__main__":
    main()