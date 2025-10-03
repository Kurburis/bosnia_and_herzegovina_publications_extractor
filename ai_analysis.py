import pandas as pd
import plotly.express as px
import streamlit as st
from pathlib import Path
import ast

# ----------------------- App config -----------------------
st.set_page_config(page_title="AI Publications Analysis", layout="wide")
st.title("AI Publications Analysis")

# ----------------------- Load Data -----------------------
DATA_PATH = "data/compressed/publications/"
AI_SUBTOPIC = "Artificial Intelligence"

@st.cache_data(show_spinner=True)
def load_data_from_folder(folder_path: str, columns: list[str]) -> pd.DataFrame:
    # Get all Parquet files in the folder
    folder = Path(folder_path)
    parquet_files = list(folder.glob("*.parquet"))
    
    # Load and concatenate all Parquet files
    dataframes = []
    for file in parquet_files:
        df = pd.read_parquet(file, columns=columns)
        dataframes.append(df)
    
    # Combine all DataFrames into one
    combined_df = pd.concat(dataframes, ignore_index=True)
    return combined_df

# Columns of interest
COLUMNS = [
    "id", "authorships.author.display_name", "authorships.author.id",
    "authorships.author_position", "authorships.institutions.country_code",
    "authorships.institutions.display_name", "cited_by_count", "display_name",
    "primary_location.source.display_name", "primary_location.source.type",
    "publication_year", "topics.subfield.display_name", "scimagoRank"
]

# Load data
df = load_data_from_folder(DATA_PATH, COLUMNS)

# Parse the string representations of lists into actual lists
df["topics.subfield.display_name"] = df["topics.subfield.display_name"].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else x
)

# ----------------------- Filter AI Papers -----------------------

# Define multiple subtopics for AI
AI_SUBTOPICS = ["Artificial Intelligence", "artificial intelligence", "AI", "Machine Learning", "Deep Learning", "data analysis", "machine learning"]

# Filter AI papers
ai_papers = df[df["topics.subfield.display_name"].apply(
    lambda x: isinstance(x, list) and any(subtopic in x for subtopic in AI_SUBTOPICS)
)]
print(f"Total AI-related papers: {len(ai_papers)}")

# ----------------------- Analysis -----------------------

# Total number of papers
st.subheader("Total Number of Papers Related to AI")
total_papers = len(ai_papers)
st.write(f"Total number of papers: {total_papers}")

# Histogram of citations
st.subheader("Histogram of Citations for AI Papers")
citations = pd.to_numeric(ai_papers["cited_by_count"], errors="coerce").dropna()
fig_citations = px.histogram(citations, nbins=50, labels={"value": "Citations", "count": "Number of Papers"}, title="Citations Distribution")
st.plotly_chart(fig_citations, use_container_width=True)

# Filter for journals only
ai_papers["is_journal"] = (
    ai_papers.get("primary_location.source.type", pd.Series(index=ai_papers.index))
    .astype(str).str.lower().eq("journal")
)

journals = ai_papers[ai_papers["is_journal"]]

# Normalize Scimago ranks
journals["scimago_norm"] = journals.get("scimagoRank", pd.Series(index=journals.index)).map(
    lambda val: "Not in Scimago" if pd.isna(val) else str(val).strip().upper()
)
journals["scimago_norm"] = journals["scimago_norm"].replace("-", "Unranked (that year)")

# Papers by Scimago quartiles (including non-ranked)
st.subheader("Number of Papers by Scimago Quartiles (Including Non-Ranked)")
quartiles = journals["scimago_norm"].value_counts()
order = ["Q1", "Q2", "Q3", "Q4", "Unranked (that year)", "Not in Scimago", "Other"]
quartiles = quartiles.reindex(order, fill_value=0)
fig_quartiles = px.bar(
    quartiles,
    labels={"index": "Scimago Quartile", "value": "Number of Papers"},
    title="Papers by Scimago Quartiles (Including Non-Ranked)"
)
st.plotly_chart(fig_quartiles, use_container_width=True)

# Histogram of citations for journals only
st.subheader("Histogram of Citations for Journals (Including Non-Ranked)")
citations_journals = pd.to_numeric(journals["cited_by_count"], errors="coerce").dropna()
fig_citations_journals = px.histogram(
    citations_journals,
    nbins=50,
    labels={"value": "Citations", "count": "Number of Papers"},
    title="Citations Distribution for Journals (Including Non-Ranked)"
)
st.plotly_chart(fig_citations_journals, use_container_width=True)

# Papers by scientific fields
st.subheader("Number of Papers by Scientific Fields Mentioned with AI")
fields = ai_papers["topics.subfield.display_name"].explode().value_counts()
fig_fields = px.bar(fields, orientation="h", labels={"index": "Scientific Field", "value": "Number of Papers"}, title="Papers by Scientific Fields")
st.plotly_chart(fig_fields, use_container_width=True)

# ----------------------- Filtered Analyses -----------------------

# Papers in Q1
st.subheader("Analysis for Papers Published in Q1 Journals")
q1_papers = journals[journals["scimagoRank"] == "Q1"]
q1_count = len(q1_papers)
st.write(f"Total number of papers in Q1: {q1_count}")

# Papers where first author affiliation is in BiH
st.subheader("Analysis for Papers Where First Author Affiliation is in BiH")
bih_papers = ai_papers[ai_papers["authorships.institutions.country_code"].apply(lambda x: "BA" in x if isinstance(x, list) else False)]
bih_count = len(bih_papers)
st.write(f"Total number of papers with first author affiliation in BiH: {bih_count}")

# Papers where first author affiliation is outside BiH
st.subheader("Analysis for Papers Where First Author Affiliation is Outside BiH")
outside_bih_papers = ai_papers[ai_papers["authorships.institutions.country_code"].apply(lambda x: "BA" not in x if isinstance(x, list) else False)]
outside_bih_count = len(outside_bih_papers)
st.write(f"Total number of papers with first author affiliation outside BiH: {outside_bih_count}")