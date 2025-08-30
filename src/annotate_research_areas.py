import os
import sys
import time
import logging
import json
import argparse
import pandas as pd
from dotenv import load_dotenv
import openai
from openai import AzureOpenAI
import subprocess

# CONFIGURATION
TITLE_COLUMN = "title"
ABSTRACT_COLUMN = "abstract"

DOMAINS = [ "Social Sciences", "Life Sciences", "Health Sciences", "Physical Sciences"]
SOCIAL_FIELDS = [
    "Decision Sciences",
    "Business, Management and Accounting",
    "Psychology",
    "Economics, Econometrics and Finance",
    "Social Sciences",
    "Arts and Humanities"
]
LIFE_FIELDS = [
    "Agricultural and Biological Sciences",
    "Biochemistry, Genetics and Molecular Biology",
    "Immunology and Microbiology",
    "Neuroscience",
    "Pharmacology, Toxicology and Pharmaceutics"
]
HEALTH_FIELDS = [
    "Health Professions",
    "Medicine",
    "Nursing",
    "Dentistry",
    "Veterinary"
]
PHYSICAL_FIELDS = [
    "Mathematics",
    "Chemical Engineering",
    "Computer Science",
    "Earth and Planetary Sciences",
    "Physics and Astronomy",
    "Energy",
    "Environmental Science",
    "Engineering",
    "Materials Science",
    "Chemistry"
]

FIELDS_BY_DOMAIN = {
    "Social Sciences": SOCIAL_FIELDS,
    "Life Sciences": LIFE_FIELDS,
    "Health Sciences": HEALTH_FIELDS,
    "Physical Sciences": PHYSICAL_FIELDS
}

# Notification helpers for mako/wayland/hyprland
def send_notification(summary, body):
    try:
        subprocess.run(["notify-send", summary, body], check=True)
    except Exception as e:
        logging.warning(f"Could not send notification: {e}")

def prompt_gpt(title, abstract, category, client):
    prompt = (
        f"Given the title, abstract, and category of a research paper, identify its research area from most specific to most broad. "
        f"Return a JSON object with keys: topic, subfield, field, domain. "
        f"Domains are: {', '.join(DOMAINS)}. "
        f"Fields for each domain are: {json.dumps(FIELDS_BY_DOMAIN)}. "
        f"topic and subfield are free to choose. "
        f"Only use the title, abstract, and category provided. "
        f"Return only the JSON object, nothing else. "
        f"If the input is insufficient, return an empty JSON.\n\n"
        f"Title: {title}\n"
        f"Abstract: {abstract if abstract else 'N/A'}\n"
        f"Category: {category if category else 'N/A'}"
    )
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[
                {"role": "system", "content": "You are a scientific research area annotator for a backend service."},
                {"role": "user", "content": prompt}
            ]
        )
        content = completion.choices[0].message.content.strip()
        return content
    except Exception as e:
        logging.error(f"OpenAI API error: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Annotate research areas in a CSV using GPT.")
    parser.add_argument('--input', required=True, help='Input CSV file')
    parser.add_argument('--output', required=True, help='Output CSV file')
    parser.add_argument('--log', default="annotate_research_areas.log", help='Log file location')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(args.log),
            logging.StreamHandler(sys.stdout)
        ]
    )

    send_notification("Research Area Annotation", "Script started and running...")

    # Load environment variables
    load_dotenv()
    openai.api_key = os.getenv("OPENAI_API_KEY")
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2023-12-01-preview")
    client = AzureOpenAI(
        api_key=os.getenv("OPENAI_API_KEY"),
        azure_endpoint=azure_endpoint,
        api_version=api_version
    )

    # Load CSV
    df = pd.read_csv(args.input, dtype=str).fillna("")

    # Ensure columns exist
    for col in ["topic", "subfield", "field", "domain"]:
        if col not in df.columns:
            df[col] = ""

    # Filter rows to annotate
    mask = (df["addedViaImenik"].astype(str) == "1") & (df["primary_topic.display_name"] == "")
    rows_to_annotate = df[mask]
    total = len(rows_to_annotate)
    logging.info(f"Annotating {total} rows.")

    processed = 0
    for idx, row in rows_to_annotate.iterrows():
        title = row.get(TITLE_COLUMN, "")
        abstract = row.get(ABSTRACT_COLUMN, "")
        category = row.get("category", "")
        if not title:
            logging.warning(f"Row {idx} has no title, skipping.")
            continue

        logging.info(f"Annotating row {idx}: {title[:60]}...")
        gpt_response = prompt_gpt(title, abstract, category, client)
        logging.info(f"GPT response for row {idx}: {gpt_response}")
        if not gpt_response:
            logging.error(f"Failed to get GPT response for row {idx}")
            continue

        try:
            areas = json.loads(gpt_response)
            for key in ["topic", "subfield", "field", "domain"]:
                df.at[idx, key] = areas.get(key, "")
            logging.info(f"Updated row {idx} with areas: {areas}")
        except Exception as e:
            logging.error(f"Error processing GPT response for row {idx}: {e}")
            continue

        processed += 1
        if processed % 10 == 0:
            send_notification("Research Area Annotation", f"Processed {processed}/{total} rows.")
        time.sleep(2)  # Be polite to the API

    # Save result
    df.to_csv(args.output, index=False)
    send_notification("Research Area Annotation", f"Finished! Processed {processed}/{total} rows.")
    logging.info("Script finished.")

if __name__ == "__main__":
    main()