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
TITLE_COLUMN = "display_name"
ABSTRACT_COLUMN = "abstract"
CATEGORY_COLUMN = "category"

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
    # prompt = (
    #     f"Given the title, abstract, and category of a research paper, identify its research area from most specific to most broad. "
    #     f"Return a JSON object with keys: topic, subfield, field, domain. The values of these keys should be string arrays."
    #     f"Topic is the most specific, while domain is the most general. "
    #     f"At most there should be 3 topic, subfield, field, domain values identified. "
    #     f"For example, a paper about deep learning in medical imaging might have topic: "
    #     f"['deep learning', 'medical imaging'], subfield: ['artificial intelligence'], field: ['computer science'], domain: ['physical sciences']."
    #     f"The first object in the arrays should be the primary research area of the publication. "
    #     f"Domains are: {', '.join(DOMAINS)}. "
    #     f"Fields for each domain are: {json.dumps(FIELDS_BY_DOMAIN)}. "
    #     f"Topic and subfield are free to choose. "
    #     f"Only use the title, abstract, and category provided. "
    #     f"Do not infer any data based on previous training, strictly use only source text given below as input."
    #     f"Do not make up any domains or fields outside of the provided list. "
    #     f"Return only the JSON object, nothing else. "
    #     f"If the input is insufficient, return an empty JSON.\n\n"
    #     f"Title: {title}\n"
    #     f"Abstract: {abstract if abstract else 'N/A'}\n"
    #     f"Category: {category if category else 'N/A'}"
    # )
    prompt = (
    "You are a validator and research area extractor. Return ONLY strict JSON with an array 'areas'.\n"
    "Each element describes ONE domain-bound classification:\n"
    "  areas[i] = {\"domain\": <str>, \"field\": <str>, \"subfield\": <str>, \"topic\": <str>}\n"
    "\n"
    "INPUT SOURCES:\n"
    "Use ONLY the Title, Abstract, and Category provided below. Do NOT use prior knowledge.\n"
    "\n"
    "GOAL:\n"
    "Identify 1–3 best-matching items.\n"
    "\n"
    "HARD CONSTRAINTS:\n"
    "• 1–3 items total in 'areas' (no more).\n"
    "• 'domain' MUST be EXACTLY one of: " + ", ".join(DOMAINS) + "\n"
    "• 'field' MUST be EXACTLY one of the allowed fields for the SAME domain per:\n"
    + json.dumps(FIELDS_BY_DOMAIN) + "\n"
    "• 'subfield' and 'topic' are short, non-empty free-text phrases; 'topic' is the most specific.\n"
    "• Use exact casing/spelling for domain/field as listed. Do NOT invent new domains/fields.\n"
    "\n"
    "MAPPING & FILTERING RULES:\n"
    "1) Propose candidate items.\n"
    "2) For EACH item, verify: field ∈ FIELDS_BY_DOMAIN[domain]. If it fails, DROP the item.\n"
    "3) After dropping invalid items:\n"
    "   • If ≥1 valid item remains (max 3), return {\"areas\": [ ...valid items in confidence order... ]}.\n"
    "   • If 0 valid items remain OR you cannot identify all of domain/field/subfield/topic, return {} (empty JSON) and nothing else.\n"
    "\n"
    "STYLE:\n"
    "• Return ONLY JSON. No prose or markdown.\n"
    "• Successful shape:\n"
    "  {\"areas\": [\n"
    "    {\"domain\":\"...\",\"field\":\"...\",\"subfield\":\"...\",\"topic\":\"...\"},\n"
    "    {\"domain\":\"...\",\"field\":\"...\",\"subfield\":\"...\",\"topic\":\"...\"}\n"
    "  ]}\n"
    "\n"
    "GOOD EXAMPLES:\n"
    "  {\"areas\":[\n"
    "    {\"domain\":\"Social Sciences\",\"field\":\"Decision Sciences\",\"subfield\":\"behavioral decision making\",\"topic\":\"heuristics and biases\"},\n"
    "    {\"domain\":\"Physical Sciences\",\"field\":\"Mathematics\",\"subfield\":\"functional analysis\",\"topic\":\"Neuberger spectra\"}\n"
    "  ]}\n"
    "  {\"areas\":[\n"
    "    {\"domain\":\"Health Sciences\",\"field\":\"Medicine\",\"subfield\":\"neurology\",\"topic\":\"status epilepticus\"}\n"
    "  ]}\n"
    "INSUFFICIENT → return empty: {}\n"
    "\n"
    "INPUT\n"
    "Title: " + (title or "N/A") + "\n"
    "Abstract: " + (abstract or "N/A") + "\n"
    "Category: " + (category or "N/A")
    )


    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            response_format={"type":"json_object"},
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

def validate_areas(payload, domains, fields_by_domain, max_items=3):
    """
    Accepts parsed JSON (dict). Returns {} or a cleaned dict: {"areas":[...]}.
    Enforces:
      - 1..max_items items
      - domain in domains
      - field in fields_by_domain[domain]
      - non-empty subfield/topic strings
    Deduplicates identical (domain, field, subfield, topic) rows preserving order.
    """
    if not isinstance(payload, dict) or "areas" not in payload or not isinstance(payload["areas"], list):
        return {}
    out = []
    seen = set()
    for item in payload["areas"]:
        if not isinstance(item, dict): 
            continue
        d = item.get("domain", "")
        f = item.get("field", "")
        s = (item.get("subfield") or "").strip()
        t = (item.get("topic") or "").strip()
        if d not in domains:
            continue
        if f not in fields_by_domain.get(d, []):
            continue
        if not s or not t:
            continue
        key = (d, f, s, t)
        if key in seen:
            continue
        seen.add(key)
        out.append({"domain": d, "field": f, "subfield": s, "topic": t})
        if len(out) >= max_items:
            break
    return {"areas": out} if out else {}

def main():
    parser = argparse.ArgumentParser(description="Annotate research areas in a CSV using GPT.")
    parser.add_argument('--input', required=True, help='Input CSV file')
    parser.add_argument('--output', required=True, help='Output CSV file')
    parser.add_argument('--log', default="log/annotate_research_areas.log", help='Log file location')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    parser.add_argument('--start', type=int, default=0, help='Start index of rows to process (inclusive)')
    parser.add_argument('--end', type=int, default=None, help='End index of rows to process (exclusive)')
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

    # Apply start and end indices to the original DataFrame
    if args.end is None:
        args.end = len(df)
    df = df.iloc[args.start:args.end]

    # Filter rows to annotate
    mask = (df["addedViaImenik"].astype(str) == "1.0") & (df["primary_topic.display_name"] == "")
    rows_to_annotate = df[mask]
    print(f"Total rows to annotate based on criteria: {len(rows_to_annotate)}")

    total = len(rows_to_annotate)
    logging.info(f"Annotating {total} rows (indices {args.start} to {args.end}).")

    processed = 0
    for idx, row in rows_to_annotate.iterrows():
        title = row.get(TITLE_COLUMN, "")
        abstract = row.get(ABSTRACT_COLUMN, "")
        category = row.get(CATEGORY_COLUMN, "")
        if not title and not abstract:
            logging.warning(f"Row {idx} has no title or abstract, skipping.")
            continue

        logging.info(f"Annotating row {idx}: {title[:60]}...")
        gpt_response = prompt_gpt(title, abstract, category, client)
        logging.info(f"GPT response for row {idx}: {gpt_response}")
        if not gpt_response:
            logging.error(f"Failed to get GPT response for row {idx}")
            continue

        try:
            raw_areas = json.loads(gpt_response)
            clean_areas = validate_areas(raw_areas, DOMAINS, FIELDS_BY_DOMAIN, max_items=3)

            if not clean_areas:  # If validation fails, store empty values
                df.at[idx, "domain"] = ""
                df.at[idx, "field"] = ""
                df.at[idx, "subfield"] = ""
                df.at[idx, "topic"] = ""
                logging.info(f"Row {idx}: No valid areas found")
                continue
            else:
                # Extract validated areas and insert into DataFrame
                domains = [a["domain"] for a in clean_areas["areas"]]
                fields = [a["field"] for a in clean_areas["areas"]]
                subfields = [a["subfield"] for a in clean_areas["areas"]]
                topics = [a["topic"] for a in clean_areas["areas"]]

                df.at[idx, "domain"] = "; ".join(domains)
                df.at[idx, "field"] = "; ".join(fields)
                df.at[idx, "subfield"] = "; ".join(subfields)
                df.at[idx, "topic"] = "; ".join(topics)

                logging.info(f"Row {idx}: Updated with validated areas: {clean_areas}")
        except Exception as e:
            logging.error(f"Error processing GPT response for row {idx}: {e}")
            continue

        # Map JSON keys to CSV column names
        json_to_csv_mapping = {
            "topic": "topics.display_name",
            "subfield": "topics.subfield.display_name",
            "field": "topics.field.display_name",
            "domain": "topics.domain.display_name"
        }

        # Map JSON keys to primary column names
        json_to_primary_mapping = {
            "topic": "primary_topic.display_name",
            "subfield": "primary_topic.subfield.display_name",
            "field": "primary_topic.field.display_name",
            "domain": "primary_topic.domain.display_name"
        }

        # Update the CSV with the JSON data
        for key in ["topic", "subfield", "field", "domain"]:
            # Extract the list of values for this key from clean_areas["areas"]
            values = [a[key] for a in clean_areas.get("areas", [])]
            # Insert the full array into the corresponding CSV column
            df.at[idx, json_to_csv_mapping[key]] = ", ".join(values)
            # Insert the first value of the array into the primary column
            df.at[idx, json_to_primary_mapping[key]] = values[0] if values else ""

        processed += 1
        if processed % 10 == 0:
            send_notification("Research Area Annotation", f"Processed {processed}/{total} rows.")
            df.to_csv(args.output, index=False)
        time.sleep(5)  # Be polite to the API

    # Save result
    df.to_csv(args.output, index=False)
    send_notification("Research Area Annotation", f"Finished! Processed {processed}/{total} rows.")
    logging.info("Script finished.")

if __name__ == "__main__":
    main()