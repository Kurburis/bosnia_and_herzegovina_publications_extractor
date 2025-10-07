# streamlit_app.py
import json
import ast
from pathlib import Path
from typing import Any, List, Optional

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

import pyarrow as pa
import pyarrow.dataset as ds

# ----------------------- App config -----------------------
st.set_page_config(page_title="AI Publications Analysis", layout="wide")
st.title("AI Publications Analysis")

# ----------------------- Defaults -------------------------
DEFAULT_SHARD_DIR_PUBLICATIONS = "data/compressed/publications/"
DEFAULT_SHARD_DIR_AUTHORS = "data/compressed/authors/"

# Columns we need from the publications shards
PUB_COLS = [
    "id",
    "authorships.author.display_name",
    "authorships.author.id",
    "authorships.author_position",
    "authorships.institutions.country_code",
    "authorships.institutions.display_name",
    "cited_by_count",
    "display_name",
    "primary_location.source.display_name",
    "primary_location.source.type",
    "publication_year",
    "topics.subfield.display_name",
    "scimagoRank",
    "coreRank",
]

# ----------------------- Helpers --------------------------
def resolve_dataset_columns(dataset: ds.Dataset, wanted: list[str]) -> list[str]:
    """Return the subset of wanted columns that actually exist (match with stripped names)."""
    actual = list(dataset.schema.names)
    stripped = {}
    for name in actual:
        k = name.strip()
        if k not in stripped:
            stripped[k] = name
    return [stripped[c] for c in wanted if c in stripped]

def infer_year_min_max(dataset: ds.Dataset) -> tuple[int, int]:
    if "publication_year" not in dataset.schema.names:
        return (1999, 2025)
    tbl = dataset.to_table(columns=["publication_year"])
    s = pd.Series(tbl.column("publication_year"))
    s = pd.to_numeric(s, errors="coerce").dropna()
    return (max(1999, int(s.min())) if len(s) else 1999, int(s.max()) if len(s) else 2025)

@st.cache_data(show_spinner=True)
def load_year_filtered(dataset_uri: str, year_range: tuple[int, int], cols: list[str]) -> pd.DataFrame:
    """Load only the requested years & columns using pyarrow for speed."""
    dataset = ds.dataset(dataset_uri, format="parquet")
    flt = None
    if "publication_year" in dataset.schema.names and year_range:
        y0, y1 = year_range
        fld = ds.field("publication_year")
        flt = (fld >= pa.scalar(y0)) & (fld <= pa.scalar(y1))
    columns = resolve_dataset_columns(dataset, cols) if cols else None
    table = dataset.to_table(filter=flt, columns=columns)
    df = table.to_pandas()
    return df.rename(columns=lambda c: c.strip())

def safe_parse_list(x: Any) -> Optional[List[Any]]:
    """
    Make sure a column becomes a Python list (or None).
    Accepts:
      - already-a-list
      - JSON-like string: '["A","B"]'
      - Python literal string: "['A', 'B']'
      - comma-separated: "A, B, C"
    """
    if isinstance(x, list):
        return x
    if pd.isna(x):
        return None
    if isinstance(x, str):
        s = x.strip().strip('"').strip("'")
        # Try JSON
        try:
            val = json.loads(s)
            return val if isinstance(val, list) else None
        except Exception:
            pass
        # Try Python literal
        try:
            val = ast.literal_eval(s)
            return val if isinstance(val, list) else None
        except Exception:
            pass
        # Comma-separated without brackets
        if "," in s and "[" not in s and "]" not in s:
            return [part.strip() for part in s.split(",") if part.strip()]
        return [s] if s else None
    # other sequence-like types
    try:
        return list(x)
    except Exception:
        return None

def lower_list(x: Optional[List[Any]]) -> Optional[List[str]]:
    if not isinstance(x, list):
        return None
    return [str(v).strip().lower() for v in x if v is not None]

def norm_scimago(x: Any) -> str:
    if pd.isna(x) or str(x).strip() == "":
        return "Not in Scimago"
    s = str(x).strip().upper()
    return "Unranked (that year)" if s == "-" else s

def _flatten_countries(x):
    """Flatten country codes possibly nested (list-of-lists)."""
    L = safe_parse_list(x)
    if not isinstance(L, list):
        return []
    out = []
    for item in L:
        if isinstance(item, (list, tuple, set, np.ndarray)):
            out.extend([str(c).upper() for c in item if pd.notna(c)])
        elif pd.notna(item):
            out.append(str(item).upper())
    return out

def extract_first_author_country_codes(row: pd.Series) -> List[str]:
    """
    Aligns across authorship lists to find FIRST author's affiliation countries.
    authorships.author_position vs authorships.institutions.country_code.
    """
    positions = safe_parse_list(row.get("authorships.author_position"))
    countries = safe_parse_list(row.get("authorships.institutions.country_code"))

    if not positions or not countries:
        return []

    n = min(len(positions), len(countries))
    positions = positions[:n]
    countries = countries[:n]

    first_idxs = [i for i, p in enumerate(positions) if isinstance(p, str) and p.lower().startswith("first")]
    if not first_idxs:
        first_idxs = [0] if n > 0 else []

    out = []
    for i in first_idxs:
        first_c = countries[i]
        out.extend(_flatten_countries(first_c))

    # dedupe
    seen = set()
    uniq = []
    for c in out:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq

def to_list(x):
    """Like in your working explorer, but lean: always returns list (possibly empty)."""
    if isinstance(x, (list, tuple, set, np.ndarray)):
        return list(x)
    if pd.isna(x):
        return []
    if isinstance(x, str):
        s = x.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                v = ast.literal_eval(s)
                return list(v) if isinstance(v, (list, tuple, set, np.ndarray)) else []
            except Exception:
                return []
        return [s] if s else []
    return [x]

def norm_core(val):
    if pd.isna(val): return "Missing"
    s = str(val).strip().upper()
    return s

# ----------------------- Sidebar: data & year ----------------------
with st.sidebar:
    st.header("Data source")
    pub_dir = st.text_input(
        "Publications Parquet directory",
        value=DEFAULT_SHARD_DIR_PUBLICATIONS,
        key="pub_dir",
        help="Folder with publications Parquet shards (part_0000.parquet, …).",
    )
    pub_path = Path(pub_dir)
    if not pub_path.exists():
        st.error(f"Not found: {pub_path}")
        st.stop()

    # Build dataset for year inference
    pub_dataset = ds.dataset(str(pub_path), format="parquet")
    y_min, y_max = infer_year_min_max(pub_dataset)
    years = st.slider(
        "Publication year", min_value=int(y_min), max_value=int(y_max),
        value=(max(y_min, 2010), y_max), key="years",
    )

# ----------------------- Load & derive ----------------------------
# Load only the years we need
df_year = load_year_filtered(str(pub_path), (years[0], years[1]), PUB_COLS)
if df_year.empty:
    st.warning("No rows in this year range.")
    st.stop()

# Derivations (on the year-filtered data)
df_year = df_year.copy()
# numeric types
df_year["publication_year"] = pd.to_numeric(df_year.get("publication_year"), errors="coerce").astype("Int64")
df_year["cited_by_count"] = pd.to_numeric(df_year.get("cited_by_count"), errors="coerce")

# normalized columns
df_year["source_type_norm"] = (
    df_year.get("primary_location.source.type", pd.Series(index=df_year.index))
          .astype(str).str.strip().str.lower()
)
df_year["scimago_norm"] = df_year.get("scimagoRank", pd.Series(index=df_year.index)).map(norm_scimago)

# parsed lists
df_year["topics_subfields"] = df_year["topics.subfield.display_name"].apply(safe_parse_list).apply(lower_list)
# first-author country codes
df_year["first_author_countries"] = df_year.apply(extract_first_author_country_codes, axis=1)
df_year["core_norm"] = df_year.get("coreRank", pd.Series(index=df_year.index)).map(norm_core)

# ----------------------- Filters (sidebar) ------------------------
with st.sidebar:
    st.header("Filters")

    # only_journals = st.checkbox(
    #     "Only journals (primary_location.source.type = 'journal')",
    #     value=True, key="only_journals_checkbox"
    # )
    # rank_opts = ["Q1", "Q2", "Q3", "Q4", "Unranked (that year)", "Not in Scimago", "Other"]
    # rank_sel = st.multiselect(
    #     "Scimago ranks",
    #     rank_opts,
    #     default=["Q1", "Q2", "Q3", "Q4", "Unranked (that year)", "Not in Scimago"],
    #     disabled=not only_journals,
    #     key="rank_sel_widget",
    # )

    # show_core_for_all = st.checkbox(
    #     "CORE: show for all publications (ignore AI filter)",
    #     value=False,
    #     key="filter_core_all"
    # )

    subset = st.radio(
        "Show which AI papers?",
        options=("All AI papers", "Only CORE-ranked (conferences)", "Only Scimago-ranked (journals)"),
        index=0,
        key="subset_ai"
    )
    
    # Scimago ranks (only meaningful for the 'Only Scimago-ranked' subset)
    rank_opts = ["Q1","Q2","Q3","Q4","Unranked (that year)","Not in Scimago","Other"]
    rank_sel = st.multiselect(
        "Scimago ranks",
        rank_opts,
        default=["Q1","Q2","Q3","Q4","Unranked (that year)","Not in Scimago"],
        disabled=(subset != "Only Scimago-ranked (journals)"),
        key="subset_rank_sel"
    )


    # AI subtopics (editable)
    st.markdown("**AI subtopics (case-insensitive match):**")
    default_ai = ["artificial intelligence", "ai", "machine learning", "deep learning", "data analysis"]
    ai_subtopics_text = st.text_input(
        "Comma separated",
        value=", ".join(default_ai),
        key="ai_subtopics_text",
        help="Used to match entries in topics.subfield.display_name"
    )
    AI_SUBTOPICS = {t.strip().lower() for t in ai_subtopics_text.split(",") if t.strip()}

    # First-author BA filter
    want_first_author_ba = st.checkbox(
        "First author affiliation country = BA",
        value=False, key="first_ba_checkbox"
    )

    min_citations = st.number_input(
        "Minimum citations",
        min_value=0,
        value=0,
        step=1,
        help="Show only papers with cited_by_count ≥ this number.",
        key="min_citations",
    )

    top_n_subfields = st.number_input(
        "Number of top subfields to display",
        min_value=1,
        max_value=50,
        value=10,
        step=1,
        help="Show only the top N scientific subfields in the chart.",
        key="top_n_subfields",
    )


# ----------------------- Apply filters ----------------------------
# AI mask
ai_mask = df_year["topics_subfields"].apply(
    lambda lst: isinstance(lst, list) and any(s in AI_SUBTOPICS for s in lst)
)
ai_papers = df_year.loc[ai_mask].copy()


if "cited_by_count" in ai_papers.columns:
    ai_papers = ai_papers[ai_papers["cited_by_count"].fillna(0) >= min_citations]

# Subset selection
if subset == "Only CORE-ranked (conferences)":
    # keep AI papers that have any CORE value (exclude Missing)
    ai_papers = ai_papers[ai_papers["core_norm"].ne("Missing")]

elif subset == "Only Scimago-ranked (journals)":
    # must be journals AND have Scimago information (exclude 'Not in Scimago')
    ai_papers = ai_papers[ai_papers["source_type_norm"].eq("journal")]
    # optional further narrowing by selected ranks
    if rank_sel:
        ai_papers = ai_papers[ai_papers["scimago_norm"].isin(rank_sel)]

# First-author BA
if want_first_author_ba:
    ba_mask = ai_papers["first_author_countries"].apply(lambda lst: isinstance(lst, list) and ("BA" in lst))
    ai_papers = ai_papers[ba_mask]

# --- Apply min citations filter ---
if "cited_by_count" in ai_papers.columns:
    ai_papers = ai_papers[ai_papers["cited_by_count"].fillna(0) >= min_citations]

# --- Apply min citations filter AFTER other filters on ai_papers ---
if "cited_by_count" in ai_papers.columns:
    ai_papers = ai_papers[ai_papers["cited_by_count"].fillna(0) >= min_citations]
st.caption(f"Active AI papers: {len(ai_papers):,}")

# ----------------------- Analysis / Visuals -----------------------
st.subheader("Total Number of AI-related Papers")
st.write(f"Total number of papers: {len(ai_papers):,}")

# Citations histogram (AI papers)
st.subheader("Histogram of Citations (AI papers)")
if "cited_by_count" in ai_papers.columns and not ai_papers["cited_by_count"].dropna().empty:
    cits = ai_papers["cited_by_count"].dropna()
    fig_citations = px.histogram(
        cits,
        nbins=50,
        labels={"value": "Citations", "count": "Number of Papers"},
        title="Citations Distribution"
    )
    st.plotly_chart(fig_citations, use_container_width=True)
else:
    st.info("No citation data in the active selection.")

if subset is not "Only CORE-ranked (conferences)":
    # Scimago quartiles (journals only)
    st.subheader("Number of Journal Articles by Scimago Quartiles (Including Non-Ranked)")
    journals = ai_papers[ai_papers["source_type_norm"].eq("journal")].copy()
    if not journals.empty and "scimago_norm" in journals.columns:
        quartiles = journals["scimago_norm"].value_counts()
        order = ["Q1", "Q2", "Q3", "Q4", "Unranked (that year)", "Not in Scimago", "Other"]
        quartiles = quartiles.reindex(order, fill_value=0)
        fig_quartiles = px.bar(
            quartiles,
            labels={"index": "Scimago Quartile", "value": "Number of Papers"},
            title="Papers by Scimago Quartiles (Including Non-Ranked)"
        )
        st.plotly_chart(fig_quartiles, use_container_width=True)
    else:
        st.info("No journal rows in the active selection.")

if (subset is not "Only Scimago-ranked (journals)") and ("core_norm" in ai_papers.columns):
    st.subheader("CORE rankings distribution (AI papers)")
    core_counts = ai_papers["core_norm"].value_counts()
    # include_missing = st.checkbox("Include 'Missing' in CORE plot", value=False, key="core_show_missing")
    # if not include_missing:
    core_counts = core_counts.drop(index=["Missing"], errors="ignore")

    order = ["A*", "A", "B", "C", "Unranked", "NATIONAL: USA"]
    core_counts = core_counts.reindex(order, fill_value=0)

    if core_counts.sum() > 0:
        fig_core = px.bar(
            core_counts,
            labels={"index": "CORE rank", "value": "Publications"},
            title="Publications by CORE ranking (current AI selection)"
        )
        st.plotly_chart(fig_core, use_container_width=True)
    else:
        st.info("No publications with CORE ranking in the current AI selection.")
# else:
#     st.info("Column 'coreRanking' not found.")

st.subheader("Top sources")
top_n = st.slider("How many sources to show", 5, 50, 20, 5, key="n_sources")
src_col = "primary_location.source.display_name"
if src_col in ai_papers.columns:
    top_sources = ai_papers[src_col].dropna().astype(str).value_counts().head(top_n).sort_values(ascending=True)
    fig = px.bar(top_sources, orientation="h",
                 labels={"index": "Source", "value": "Count"},
                 title=f"Top {top_n} sources")
    st.plotly_chart(fig, use_container_width=True)


# Scientific subfields with AI
st.subheader("Number of Papers by Scientific Subfields Mentioned with AI (excluding 'Artificial Intelligence')")
top_n_subfields = st.slider("How many subfields to show", 5, 50, 20, 5, key="n_subfields")

fields_series = (
    #df_year.loc[ai_mask, "topics.subfield.display_name"]
    ai_papers["topics.subfield.display_name"]
          .apply(safe_parse_list)
          .explode()
          .dropna()
)

if not fields_series.empty:
    # Exclude "Artificial Intelligence" (case-insensitive)
    fields_series = fields_series[
        fields_series.astype(str).str.strip().str.lower() != "artificial intelligence"
    ]

    # Count and keep only top N
    fields_counts = fields_series.value_counts().head(top_n_subfields)

    if not fields_counts.empty:
        fig_fields = px.bar(
            fields_counts.sort_values(ascending=True),
            orientation="h",
            labels={"index": "Scientific Subfield", "value": "Number of Papers"},
            title=f"Top {top_n_subfields} Scientific Subfields (excluding 'Artificial Intelligence')"
        )
        st.plotly_chart(fig_fields, use_container_width=True)
    else:
        st.info("No subfields to display after filtering.")
else:
    st.info("No subfield information for the active selection.")



# # Q1 papers
# st.subheader("Analysis for Papers Published in Q1 Journals")
# q1_papers = journals[journals["scimago_norm"].eq("Q1")]
# st.write(f"Total number of papers in Q1: {len(q1_papers):,}")

# First author in BiH vs outside BiH (on AI set)
st.subheader("First Author Affiliation: BA vs non-BA (AI papers)")
bih_mask_full = ai_papers["first_author_countries"].apply(lambda lst: isinstance(lst, list) and "BA" in lst)
bih_count = int(bih_mask_full.sum())
outside_bih_count = int((~bih_mask_full).sum())
st.write(f"In BiH (BA): **{bih_count:,}**")
st.write(f"Outside BiH: **{outside_bih_count:,}**")

# Sample rows
st.subheader("Sample rows")
present_cols = [
    c for c in [
        "publication_year", "display_name",
        "primary_location.source.display_name",
        "scimago_norm", "cited_by_count", "id"
    ] if c in ai_papers.columns
]
if present_cols:
    st.dataframe(ai_papers[present_cols].head(200), use_container_width=True)

# ----------------------- Debug (optional) --------------------------
with st.expander("Debug samples", expanded=False):
    st.write("Positions:", df_year["authorships.author_position"].dropna().astype(str).head(3).tolist())
    st.write("Countries:", df_year["authorships.institutions.country_code"].dropna().astype(str).head(3).tolist())
