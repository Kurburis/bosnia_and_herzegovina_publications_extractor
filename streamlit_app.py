# streamlit_app.py
import ast
from pathlib import Path
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
import pyarrow as pa
import pyarrow.dataset as ds

# ----------------------- App config -----------------------
st.set_page_config(page_title="B&H Publications Explorer", layout="wide")
st.title("B&H Publications Explorer")

DEFAULT_SHARD_DIR_PUBLICATIONS = "data/compressed/publications/"
DEFAULT_SHARD_DIR_AUTHORS = "data/compressed/authors/"

CANON_COLS = [
    "id",
    "display_name",
    "publication_year",
    "primary_location.source.type",
    "best_oa_location.source.display_name",
    "cited_by_count",
    "scimagoRank",
    "primary_location.source.display_name",
    # arrays
    "locations.source.issn",
    "authorships.author.display_name",
    "authorships.author.id",
    "authorships.author_position",
    "authorships.institutions.country_code",
    "authorships.institutions.display_name",
    "topics.domain.display_name",
    "topics.field.display_name",
    "topics.subfield.display_name",
    "topics.display_name",
]

# --------- helpers
def to_list(x):
    if isinstance(x, (list, tuple, set, np.ndarray)): return list(x)
    if pd.isna(x): return []
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

def norm_scimago(val):
    if pd.isna(val): return "Not in Scimago"
    s = str(val).strip().upper()
    if s in {"Q1","Q2","Q3","Q4"}: return s
    if s == "-": return "Unranked (that year)"
    return "Other"

def first_author_ba_mask(df: pd.DataFrame) -> pd.Series:
    pos = df.get("authorships.author_position", pd.Series(index=df.index)).apply(to_list)
    countries = df.get("authorships.institutions.country_code", pd.Series(index=df.index)).apply(to_list)
    def is_ba(i):
        pi = pos.iat[i] if i < len(pos) else []
        ci = countries.iat[i] if i < len(countries) else []
        try:
            idx = next(j for j, p in enumerate(pi) if str(p).lower().startswith("first"))
            if idx < len(ci): return str(ci[idx]).upper() == "BA"
        except StopIteration:
            pass
        return bool(ci) and str(ci[0]).upper() == "BA"
    return pd.Series([is_ba(i) for i in range(len(df))], index=df.index)

def resolve_dataset_columns(dataset: ds.Dataset, wanted: list[str]) -> list[str]:
    actual = list(dataset.schema.names)
    stripped = {}
    for name in actual:
        k = name.strip()
        if k not in stripped: stripped[k] = name
    return [stripped[c] for c in wanted if c in stripped]

def infer_year_min_max(dataset: ds.Dataset) -> tuple[int,int]:
    if "publication_year" not in dataset.schema.names: return (1999, 2025)
    tbl = dataset.to_table(columns=["publication_year"])
    s = pd.Series(tbl.column("publication_year"))
    s = pd.to_numeric(s, errors="coerce").dropna()
    return (max(1999, int(s.min())) if len(s) else 1999,
            int(s.max()) if len(s) else 2025)

def unique_opts(df: pd.DataFrame, col: str):
    if col not in df.columns: return []
    return sorted(
        df[col].apply(to_list).explode().dropna().astype(str).str.strip().unique().tolist()
    )

def rows_intersecting(cell_vals, selected_set):
    return bool(set(to_list(cell_vals)) & selected_set)

@st.cache_data(show_spinner=True)
def load_year_filtered(dataset_uri: str, year_range: tuple[int,int], cols: list[str]) -> pd.DataFrame:
    dataset = ds.dataset(dataset_uri, format="parquet")
    flt = None
    if "publication_year" in dataset.schema.names and year_range:
        y0, y1 = year_range
        fld = ds.field("publication_year")
        flt = (fld >= pa.scalar(y0)) & (fld <= pa.scalar(y1))
    table = dataset.to_table(filter=flt, columns=cols if cols else None)
    df = table.to_pandas()
    return df.rename(columns=lambda c: c.strip())

# ----------------------- Sidebar (state-safe) -----------------------
with st.sidebar:
    st.header("Data & Filters")

    # Publications directory
    pub_dir = st.text_input(
        "Publications Parquet directory",
        value=DEFAULT_SHARD_DIR_PUBLICATIONS,  # Default publications directory
        key="pub_dir",
        help="Folder containing publications Parquet files (e.g., part_0000.parquet, part_0001.parquet, …)",
    )
    pub_shard_dir = Path(st.session_state["pub_dir"])
    if not pub_shard_dir.exists():
        st.warning(f"Publications directory not found: {pub_shard_dir}")
        st.stop()

    # Authors directory
    auth_dir = st.text_input(
        "Authors Parquet directory",
        value=DEFAULT_SHARD_DIR_AUTHORS,  # Default authors directory
        key="auth_dir",
        help="Folder containing authors Parquet files (e.g., authors_0000.parquet, authors_0001.parquet, …)",
    )
    auth_shard_dir = Path(st.session_state["auth_dir"])
    if not auth_shard_dir.exists():
        st.warning(f"Authors directory not found: {auth_shard_dir}")
        st.stop()

    # Load publications dataset
    pub_dataset = ds.dataset(str(pub_shard_dir), format="parquet")
    actual_cols = resolve_dataset_columns(pub_dataset, CANON_COLS)

    # Infer year range for publications
    y_min, y_max = infer_year_min_max(pub_dataset)
    years = st.slider(
        "Publication year",
        min_value=int(y_min), max_value=int(y_max),
        value=(max(y_min, 2010), y_max),
        key="years",
    )

# ---- Load publications dataframe (YEAR-ONLY filter applied here)
df_year = load_year_filtered(str(pub_shard_dir), (years[0], years[1]), actual_cols)
if df_year.empty:
    st.warning("No data loaded. Check the publications directory and year range.")
    st.stop()

# Precompute derived columns once (on year-filtered df)
df_year = df_year.copy()
df_year["publication_year"] = pd.to_numeric(df_year.get("publication_year"), errors="coerce").astype("Int64")
df_year["is_journal"] = (
    df_year.get("primary_location.source.type", pd.Series(index=df_year.index))
          .astype(str).str.lower().eq("journal")
)
df_year["scimago_norm"] = df_year.get("scimagoRank", pd.Series(index=df_year.index)).map(norm_scimago)

# ---- Load authors dataframe
author_db_path = auth_shard_dir / "authors_0000.parquet"
if not author_db_path.exists():
    st.warning(f"Author database not found: {author_db_path}")
    st.stop()

author_df = pd.read_parquet(author_db_path, columns=["id", "affiliations.years"])

# Create df_years table with columns id and oldest_year
if "id" in author_df.columns and "affiliations.years" in author_df.columns:
    author_df["affiliations.years"] = author_df["affiliations.years"].apply(to_list)
    df_years = author_df[["id", "affiliations.years"]] 
    df_years["oldest_year"] = df_years["affiliations.years"].apply(lambda years: min(years) if years else None)
    df_years = df_years[["id", "oldest_year"]]
else:
    st.warning("Required columns ('id', 'affiliations.years') are missing in the author database.")
    st.stop()

# ---------- Widget state init (so selections persist across reruns) ----------
for key, default in {
    "only_journals": True,
    "rank_sel": ["Q1","Q2","Q3","Q4","Unranked (that year)","Not in Scimago"],
    "want_first_author_ba": False,
    "sel_domains": [],
    "sel_fields": [],
    "sel_subfields": [],
    "sel_institutions": [],
    "sel_authors": [],
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

with st.sidebar:
    # Journals & ranks (kept in session_state)
    st.session_state.only_journals = st.checkbox(
        "Only journals (primary_location.source.type = 'journal')",
        value=st.session_state.only_journals, key="only_journals_checkbox"
    )
    # keep rank selection even when disabled
    rank_opts = ["Q1","Q2","Q3","Q4","Unranked (that year)","Not in Scimago","Other"]
    st.session_state.rank_sel = st.multiselect(
        "Scimago ranks",
        rank_opts,
        default=st.session_state.rank_sel,
        disabled=not st.session_state.only_journals,
        key="rank_sel_widget",
        help="Enabled only when filtering to journals."
    )

    # Add new filter: First author affiliation country = BA
    st.session_state.want_first_author_ba = st.checkbox(
        "First author affiliation country = BA",
        value=st.session_state.want_first_author_ba, key="first_ba_checkbox"
    )

    # Make the second checkbox and slider disabled until the first checkbox is checked
    st.session_state.first_author_since_enabled = st.checkbox(
        "AND the first author has been publishing since",
        value=False, key="first_author_since_checkbox",
        disabled=not st.session_state.want_first_author_ba  # Disable if the first checkbox is not checked
    )

    st.session_state.first_author_since = st.slider(
        "Select year range", 2015, 2025,
        (2020, 2025), key="first_author_since_slider",
        disabled=not st.session_state.first_author_since_enabled  # Disable if the second checkbox is not checked
    )

# ---------- Build OPTIONS from df_year ONLY, union with current selections ---
def union_with_selection(opts, selected):
    return sorted(set(opts).union(set(selected)))

domain_opts   = union_with_selection(
    unique_opts(df_year, "topics.domain.display_name"),
    st.session_state.sel_domains
)
field_opts    = union_with_selection(
    unique_opts(df_year, "topics.field.display_name"),
    st.session_state.sel_fields
)
subfield_opts = union_with_selection(
    unique_opts(df_year, "topics.subfield.display_name"),
    st.session_state.sel_subfields
)
inst_opts     = union_with_selection(
    unique_opts(df_year, "authorships.institutions.display_name"),
    st.session_state.sel_institutions
)
author_opts   = union_with_selection(
    unique_opts(df_year, "authorships.author.display_name"),
    st.session_state.sel_authors
)

with st.sidebar:
    st.markdown("---")
    st.subheader("Topics filter")

    st.session_state.sel_domains = st.multiselect(
        "Domains", domain_opts, default=st.session_state.sel_domains, key="domains_ms"
    )
    st.session_state.sel_fields = st.multiselect(
        "Fields", field_opts, default=st.session_state.sel_fields, key="fields_ms"
    )
    st.session_state.sel_subfields = st.multiselect(
        "Subfields", subfield_opts, default=st.session_state.sel_subfields, key="subfields_ms"
    )

    st.markdown("---")
    st.subheader("Authorship filters")

    st.session_state.sel_institutions = st.multiselect(
        "Institutions", inst_opts, default=st.session_state.sel_institutions, key="inst_ms"
    )
    st.session_state.sel_authors = st.multiselect(
        "Authors", author_opts, default=st.session_state.sel_authors, key="auth_ms"
    )

# ----------------------- APPLY FILTERS (in a stable order) -------------------
df = df_year.copy()

# Topics
if st.session_state.sel_domains:
    dset = set(st.session_state.sel_domains)
    df = df[df["topics.domain.display_name"].apply(lambda v: rows_intersecting(v, dset))]
if st.session_state.sel_fields:
    fset = set(st.session_state.sel_fields)
    df = df[df["topics.field.display_name"].apply(lambda v: rows_intersecting(v, fset))]
if st.session_state.sel_subfields:
    sfset = set(st.session_state.sel_subfields)
    df = df[df["topics.subfield.display_name"].apply(lambda v: rows_intersecting(v, sfset))]

# Authorship
if st.session_state.sel_institutions:
    inst_set = set(st.session_state.sel_institutions)
    df = df[df["authorships.institutions.display_name"].apply(lambda v: rows_intersecting(v, inst_set))]
if st.session_state.sel_authors:
    auth_set = set(st.session_state.sel_authors)
    df = df[df["authorships.author.display_name"].apply(lambda v: rows_intersecting(v, auth_set))]

# Journal & rank
if st.session_state.only_journals:
    df = df[df_year["is_journal"]]
    df = df[df["scimago_norm"].isin(st.session_state.rank_sel)]

# First author BA
if st.session_state.want_first_author_ba:
    with st.spinner("Filtering by first author affiliation (BA)…"):
        df = df[first_author_ba_mask(df)]

# First author publishing since
if st.session_state.want_first_author_ba and st.session_state.first_author_since_enabled:
    with st.spinner("Filtering by first author publishing year…"):
        min_year = st.session_state.first_author_since[0]
        max_year = st.session_state.first_author_since[1]

        # Ensure `oldest_year` is numeric and drop NaN values
        df_years["oldest_year"] = pd.to_numeric(df_years["oldest_year"], errors="coerce").dropna()

        # Apply the filter based on the selected year range
        valid_ids = df_years.loc[df_years["oldest_year"].between(min_year, max_year, inclusive="both"), "id"]

        # Apply the filter based on valid IDs and check only the first element of arrays
        df = df[df["authorships.author.id"].apply(
            lambda authors: to_list(authors)[0] in valid_ids.values if to_list(authors) else False
        )]

st.caption(f"Active rows: {len(df):,}")

# ----------------------- VISUALS --------------------------------------------
c1, c2 = st.columns(2)
with c1:
    by_year = df["publication_year"].value_counts().sort_index()
    fig = px.bar(by_year, labels={"index": "Year", "value": "Publications"},
                 title="Publications per year")
    st.plotly_chart(fig, use_container_width=True)

with c2:
    if st.session_state.only_journals:
        order = ["Q1","Q2","Q3","Q4","Unranked (that year)","Not in Scimago","Other"]
        by_rank = df["scimago_norm"].value_counts().reindex(order, fill_value=0)
        fig = px.bar(by_rank, labels={"index": "Scimago rank", "value": "Count"},
                     title="Publications by Scimago rank (journals only)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Scimago ranking applies to journals—enable 'Only journals' to see this plot.")

st.subheader("Top sources")
top_n = st.slider("How many sources to show", 5, 50, 20, 5, key="n_sources")
src_col = "primary_location.source.display_name"
if src_col in df.columns:
    top_sources = df[src_col].dropna().astype(str).value_counts().head(top_n).sort_values(ascending=True)
    fig = px.bar(top_sources, orientation="h",
                 labels={"index": "Source", "value": "Count"},
                 title=f"Top {top_n} sources")
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Citations distribution")
if "cited_by_count" in df.columns:
    cits = pd.to_numeric(df["cited_by_count"], errors="coerce").dropna()
    col_left, col_right = st.columns([1,1])
    with col_left:
        cap_pct = st.slider("Cap x-axis at percentile", 80, 100, 99, 1, key="cap_pct")
    with col_right:
        use_logx = st.checkbox("Log scale (x)", value=False, key="logx")
    xmax = cits.quantile(cap_pct / 100.0)
    cits_capped = cits[cits <= xmax]
    fig = px.histogram(cits_capped, nbins=50,
                       labels={"value": "Citations", "count": "Publications"},
                       title=f"Citations (≤ {cap_pct}th percentile: ≤ {int(xmax)})")
    if use_logx: fig.update_layout(xaxis_type="log")
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Subfield co-occurrence")
sf_col = "topics.subfield.display_name"
if sf_col in df.columns:
    lists = df[sf_col].apply(to_list)
    if st.session_state.sel_subfields:
        sel_set = set(st.session_state.sel_subfields)
        mask = lists.apply(lambda L: bool(set(L) & sel_set))
        co_counts = (
            lists[mask].apply(lambda L: [x for x in L if x not in sel_set])
                       .explode().dropna().astype(str).str.strip()
                       .value_counts().head(25)
        )
        if not co_counts.empty:
            fig = px.bar(co_counts.sort_values(ascending=True),
                         orientation="h",
                         labels={"index": "Co-occurring subfield", "value": "Count"},
                         title="Top co-occurring subfields with current selection")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No co-occurring subfields for the current selection.")
    else:
        st.info("Select one or more subfields in the sidebar to see co-occurrence.")

st.subheader("Sample of rows")
present_cols = [c for c in ["publication_year","display_name",
                            "primary_location.source.display_name",
                            "scimago_norm","cited_by_count","id"] if c in df.columns]
if present_cols:
    st.dataframe(df[present_cols].head(200), use_container_width=True)