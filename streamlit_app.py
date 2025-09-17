import ast
from pathlib import Path
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

import pyarrow as pa
import pyarrow.dataset as ds

# ----------------------- App config -----------------------
st.set_page_config(page_title="Publications Explorer", layout="wide")
st.title("Publications Explorer")

# Default directory where your compression script wrote the shards
DEFAULT_SHARD_DIR = "data/compressed"

# Columns we’ll work with (canonical names without accidental spaces)
CANON_COLS = [
    "id",
    "display_name",  # we'll strip spaces from column names after loading
    "publication_year",
    "primary_location.source.type",
    "best_oa_location.source.display_name",
    "best_oa_location.source.issn",
    "cited_by_count",
    "scimagoRank",
    # arrays
    "authorships.author.display_name",
    "authorships.author_position",
    "authorships.institutions.country_code",
    "topics.domain.display_name",
    "topics.field.display_name",
    "topics.subfield.display_name",
    "topics.display_name",
]

# ----------------------- Helpers -----------------------
def to_list(x):
    """Coerce a cell to list. Supports real lists and JSON-like strings."""
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

def first(x):
    lst = to_list(x)
    return lst[0] if lst else np.nan

def norm_scimago(val):
    if pd.isna(val):
        return "Not in Scimago"
    s = str(val).strip().upper()
    if s in {"Q1", "Q2", "Q3", "Q4"}:
        return s
    if s == "-":
        return "Unranked (that year)"
    return "Other"

def first_author_ba_mask(df: pd.DataFrame) -> pd.Series:
    """Heuristic: find 'first' in author_position and map to same-index institution country."""
    pos = df.get("authorships.author_position", pd.Series(index=df.index)).apply(to_list)
    countries = df.get("authorships.institutions.country_code", pd.Series(index=df.index)).apply(to_list)
    def is_ba(i):
        pi = pos.iat[i] if i < len(pos) else []
        ci = countries.iat[i] if i < len(countries) else []
        try:
            idx = next(j for j, p in enumerate(pi) if str(p).lower().startswith("first"))
            if idx < len(ci):
                return str(ci[idx]).upper() == "BA"
        except StopIteration:
            pass
        # fallback: first institution
        return bool(ci) and str(ci[0]).upper() == "BA"
    return pd.Series([is_ba(i) for i in range(len(df))], index=df.index)

def resolve_dataset_columns(dataset: ds.Dataset, wanted_canon_cols: list[str]) -> list[str]:
    """
    Match wanted canonical names to actual dataset columns by stripping whitespace.
    Returns list of actual names to request from Arrow.
    """
    actual = list(dataset.schema.names)
    # map stripped->actual (first occurrence wins)
    stripper = {}
    for name in actual:
        key = name.strip()
        if key not in stripper:
            stripper[key] = name
    resolved = [stripper[c] for c in wanted_canon_cols if c in stripper]
    return resolved

def infer_year_min_max_from_dataset(dataset: ds.Dataset) -> tuple[int, int] | tuple[int, int]:
    if "publication_year" not in dataset.schema.names:
        return (1990, 2025)
    tbl = dataset.to_table(columns=["publication_year"])
    s = pd.Series(tbl.column("publication_year")).astype("Int64")
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return (1990, 2025)
    return (int(s.min()), int(s.max()))

# ----------------------- Sidebar: data path + filters -----------------------
with st.sidebar:
    st.header("Data & Filters")

    data_dir = st.text_input(
        "Parquet shards directory",
        value=DEFAULT_SHARD_DIR,
        help="Folder that contains files like part_0000.parquet produced by your size-checked splitter.",
    )
    shard_dir = Path(data_dir)
    if not shard_dir.exists():
        st.warning(f"Directory not found: {shard_dir} — adjust the path.")
        st.stop()

    # Build a dataset from the directory
    dataset = ds.dataset(str(shard_dir), format="parquet")
    # Resolve which actual column names to read (handles accidental leading spaces)
    actual_cols = resolve_dataset_columns(dataset, CANON_COLS)

    # Year slider bounds (read only the year column once)
    y_min, y_max = infer_year_min_max_from_dataset(dataset)
    years = st.slider("Publication year", min_value=int(y_min), max_value=int(y_max),
                      value=(max(y_min, 2010), y_max))

    only_journals = st.checkbox("Only journals (primary_location.source.type = 'journal')", value=True)
    want_first_author_ba = st.checkbox("First author affiliation country = BA", value=False)

    rank_opts = ["Q1", "Q2", "Q3", "Q4", "Unranked (that year)", "Not in Scimago", "Other"]
    rank_sel = st.multiselect("Scimago ranks", rank_opts, default=rank_opts[:5])

# ----------------------- Data loading -----------------------
@st.cache_data(show_spinner=True)
def load_filtered(dataset_uri: str, year_range: tuple[int, int], arrow_cols: list[str]) -> pd.DataFrame:
    dataset = ds.dataset(dataset_uri, format="parquet")
    # Build filter on publication_year if present
    flt = None
    if "publication_year" in dataset.schema.names and year_range:
        y0, y1 = year_range
        fld = ds.field("publication_year")
        flt = (fld >= pa.scalar(y0)) & (fld <= pa.scalar(y1))
    table = dataset.to_table(filter=flt, columns=arrow_cols if arrow_cols else None)
    df = table.to_pandas()
    # Normalize headers by stripping accidental whitespace globally
    df = df.rename(columns=lambda c: c.strip())
    return df

df = load_filtered(str(shard_dir), (years[0], years[1]), actual_cols)

if df.empty:
    st.warning("No data loaded. Check the selected directory and filters.")
    st.stop()

# ----------------------- Cleaning / derived -----------------------
df = df.copy()

# year
df["publication_year"] = pd.to_numeric(df.get("publication_year"), errors="coerce").astype("Int64")

# journal flag
if "primary_location.source.type" in df.columns:
    df["is_journal"] = df["primary_location.source.type"].astype(str).str.lower().eq("journal")
else:
    df["is_journal"] = True

# scimago
df["scimago_norm"] = df.get("scimagoRank").map(norm_scimago) if "scimagoRank" in df.columns else "Not in Scimago"

# apply non-year filters (year already pushed down on load)
if only_journals:
    df = df[df["is_journal"]]
df = df[df["scimago_norm"].isin(rank_sel)]

# first author BA
if want_first_author_ba:
    with st.spinner("Filtering by first author affiliation (BA)…"):
        mask_ba = first_author_ba_mask(df)
        df = df[mask_ba]

st.caption(f"Active rows: {len(df):,}")

# ----------------------- Visual 1: Publications by year & Scimago rank -----------------------
c1, c2 = st.columns(2)
with c1:
    by_year = df["publication_year"].value_counts().sort_index()
    fig = px.bar(by_year, labels={"index": "Year", "value": "Publications"},
                 title="Publications per year")
    st.plotly_chart(fig, use_container_width=True)

with c2:
    order = ["Q1", "Q2", "Q3", "Q4", "Unranked (that year)", "Not in Scimago", "Other"]
    by_rank = df["scimago_norm"].value_counts().reindex(order, fill_value=0)
    fig = px.bar(by_rank, labels={"index": "Scimago rank", "value": "Count"},
                 title="Publications by Scimago rank")
    st.plotly_chart(fig, use_container_width=True)

# ----------------------- Visual 2: Top sources -----------------------
st.subheader("Top sources")
top_n = st.slider("How many sources to show", 5, 50, 20, 5, key="n_sources")
src_col = "best_oa_location.source.display_name"
if src_col in df.columns:
    top_sources = (
        df[src_col].dropna().astype(str).value_counts().head(top_n).sort_values(ascending=True)
    )
    fig = px.bar(top_sources, orientation="h",
                 labels={"index": "Source", "value": "Count"},
                 title=f"Top {top_n} sources")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Column 'best_oa_location.source.display_name' not available in the loaded data.")

# ----------------------- Visual 3: Citations distribution -----------------------
st.subheader("Citations")
if "cited_by_count" in df.columns:
    cits = pd.to_numeric(df["cited_by_count"], errors="coerce")
    fig = px.histogram(cits.dropna(), nbins=50,
                       labels={"value": "Citations", "count": "Publications"},
                       title="Citations distribution")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Column 'cited_by_count' not available.")

# ----------------------- Visual 4: Institution countries (first listed) map -----------------------
st.subheader("Institution countries (first listed)")
if "authorships.institutions.country_code" in df.columns:
    countries = df["authorships.institutions.country_code"].apply(first)
    country_counts = (
        pd.Series(countries).dropna().astype(str).str.upper()
          .value_counts().rename_axis("iso_a2").reset_index(name="count")
    )
    if not country_counts.empty:
        fig = px.choropleth(country_counts, locations="iso_a2", color="count",
                            color_continuous_scale="Viridis",
                            title="Institution countries (first listed)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No country data to map in current selection.")
else:
    st.info("Column 'authorships.institutions.country_code' not available.")

# ----------------------- Visual 5: Topics snapshot -----------------------
st.subheader("Topics snapshot (top 25)")
def explode_counts(col):
    if col not in df.columns:
        return pd.Series(dtype=int)
    return (
        df[col].apply(to_list).explode().dropna().astype(str).str.strip()
          .value_counts().head(25).sort_values(ascending=True)
    )

tc1, tc2, tc3 = st.columns(3)
with tc1:
    s = explode_counts("topics.domain.display_name")
    fig = px.bar(s, orientation="h", labels={"index": "Domain", "value": "Count"},
                 title="Top Domains")
    st.plotly_chart(fig, use_container_width=True)
with tc2:
    s = explode_counts("topics.field.display_name")
    fig = px.bar(s, orientation="h", labels={"index": "Field", "value": "Count"},
                 title="Top Fields")
    st.plotly_chart(fig, use_container_width=True)
with tc3:
    s = explode_counts("topics.subfield.display_name")
    fig = px.bar(s, orientation="h", labels={"index": "Subfield", "value": "Count"},
                 title="Top Subfields")
    st.plotly_chart(fig, use_container_width=True)

# ----------------------- Data table & download -----------------------
st.subheader("Sample of rows")
# After renaming, our canonical names should exist if they were in the dataset
present_cols = [c for c in ["publication_year", "display_name",
                            "best_oa_location.source.display_name",
                            "scimago_norm", "cited_by_count", "id"]
                if c in df.columns]
if present_cols:
    st.dataframe(df[present_cols].head(200), use_container_width=True)
    st.download_button(
        "Download current subset (CSV)",
        df[present_cols].to_csv(index=False).encode("utf-8"),
        file_name="subset.csv",
        mime="text/csv",
    )
else:
    st.info("Sample view: none of the expected display columns are present.")
