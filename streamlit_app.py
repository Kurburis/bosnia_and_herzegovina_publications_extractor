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

# Default directory where your compression script wrote the shards
DEFAULT_SHARD_DIR = "data/compressed"

# Canonical column names (we'll strip whitespace after load to match these)
CANON_COLS = [
    "id",
    "display_name",
    "publication_year",
    "primary_location.source.type",
    "best_oa_location.source.display_name",
    "best_oa_location.source.issn",
    "cited_by_count",
    "scimagoRank",
    "primary_location.source.display_name",
    # arrays
    "authorships.author.display_name",
    "authorships.author_position",
    "authorships.institutions.country_code",
    "authorships.institutions.display_name",
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
    """Find 'first' in author_position and map to same-index institution country = BA."""
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
        return bool(ci) and str(ci[0]).upper() == "BA"  # fallback
    return pd.Series([is_ba(i) for i in range(len(df))], index=df.index)

def resolve_dataset_columns(dataset: ds.Dataset, wanted_canon_cols: list[str]) -> list[str]:
    """Match canonical names to actual dataset columns by stripping whitespace."""
    actual = list(dataset.schema.names)
    stripped_to_actual = {}
    for name in actual:
        key = name.strip()
        if key not in stripped_to_actual:
            stripped_to_actual[key] = name
    return [stripped_to_actual[c] for c in wanted_canon_cols if c in stripped_to_actual]

def infer_year_min_max_from_dataset(dataset: ds.Dataset) -> tuple[int, int]:
    if "publication_year" not in dataset.schema.names:
        return (1999, 2025)
    tbl = dataset.to_table(columns=["publication_year"])
    s = pd.Series(tbl.column("publication_year")).astype("Int64")
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return (1999, 2025)
    return (int(s.min()), int(s.max()))

def unique_opts_from_list_col(df: pd.DataFrame, col: str):
    if col not in df.columns:
        return []
    return sorted(
        df[col].apply(to_list).explode().dropna().astype(str).str.strip().unique().tolist()
    )

def rows_intersecting(cell_vals, selected_set):
    vals = set(to_list(cell_vals))
    return bool(vals & selected_set)

# ----------------------- Sidebar: data path + filters -----------------------
with st.sidebar:
    st.header("Data & Filters")

    data_dir = st.text_input(
        "Parquet shards directory",
        value=DEFAULT_SHARD_DIR,
        help="Folder containing part_0000.parquet, part_0001.parquet, …",
    )
    shard_dir = Path(data_dir)
    if not shard_dir.exists():
        st.warning(f"Directory not found: {shard_dir}")
        st.stop()

    dataset = ds.dataset(str(shard_dir), format="parquet")
    actual_cols = resolve_dataset_columns(dataset, CANON_COLS)

    # Year slider (min capped at 1999)
    y_min, y_max = infer_year_min_max_from_dataset(dataset)
    y_min = max(1999, y_min)
    years = st.slider("Publication year", min_value=int(y_min), max_value=int(y_max),
                      value=(max(y_min, 2010), y_max))

    only_journals = st.checkbox(
        "Only journals (primary_location.source.type = 'journal')",
        value=True
    )

    # Scimago select (enabled and meaningful only if journals are on)
    rank_opts = ["Q1", "Q2", "Q3", "Q4", "Unranked (that year)", "Not in Scimago", "Other"]
    rank_sel = st.multiselect(
        "Scimago ranks",
        rank_opts,
        default=rank_opts[:6],  # include Not in Scimago by default
        disabled=not only_journals,
        help="Enabled only when filtering to journals."
    )

    want_first_author_ba = st.checkbox("First author affiliation country = BA", value=False)

# ----------------------- Data loading -----------------------
@st.cache_data(show_spinner=True)
def load_filtered(dataset_uri: str, year_range: tuple[int, int], arrow_cols: list[str]) -> pd.DataFrame:
    dataset = ds.dataset(dataset_uri, format="parquet")
    flt = None
    if "publication_year" in dataset.schema.names and year_range:
        y0, y1 = year_range
        fld = ds.field("publication_year")
        flt = (fld >= pa.scalar(y0)) & (fld <= pa.scalar(y1))
    table = dataset.to_table(filter=flt, columns=arrow_cols if arrow_cols else None)
    df = table.to_pandas()
    df = df.rename(columns=lambda c: c.strip())  # strip accidental whitespace
    return df

df = load_filtered(str(shard_dir), (years[0], years[1]), actual_cols)
if df.empty:
    st.warning("No data loaded. Check the directory and filters.")
    st.stop()

# ----------------------- Cleaning / derived -----------------------
df = df.copy()
df["publication_year"] = pd.to_numeric(df.get("publication_year"), errors="coerce").astype("Int64")

if "primary_location.source.type" in df.columns:
    df["is_journal"] = df["primary_location.source.type"].astype(str).str.lower().eq("journal")
else:
    df["is_journal"] = True

df["scimago_norm"] = df.get("scimagoRank", pd.Series(index=df.index)).map(norm_scimago)

# Journal/rank filters
if only_journals:
    df = df[df["is_journal"]]
    df = df[df["scimago_norm"].isin(rank_sel)]

# First author BA
if want_first_author_ba:
    with st.spinner("Filtering by first author affiliation (BA)…"):
        mask_ba = first_author_ba_mask(df)
        df = df[mask_ba]

# ----------------------- Topic filters (Domain / Field / Subfield) ----------
st.sidebar.markdown("---")
st.sidebar.subheader("Topics filter")

domain_opts   = unique_opts_from_list_col(df, "topics.domain.display_name")
field_opts    = unique_opts_from_list_col(df, "topics.field.display_name")
subfield_opts = unique_opts_from_list_col(df, "topics.subfield.display_name")

sel_domains   = st.sidebar.multiselect("Domains", domain_opts, default=[])
sel_fields    = st.sidebar.multiselect("Fields", field_opts, default=[])
sel_subfields = st.sidebar.multiselect("Subfields", subfield_opts, default=[])

if sel_domains:
    dset = set(sel_domains)
    df = df[df["topics.domain.display_name"].apply(lambda v: rows_intersecting(v, dset))]
if sel_fields:
    fset = set(sel_fields)
    df = df[df["topics.field.display_name"].apply(lambda v: rows_intersecting(v, fset))]
if sel_subfields:
    sfset = set(sel_subfields)
    df = df[df["topics.subfield.display_name"].apply(lambda v: rows_intersecting(v, sfset))]

st.caption(f"Active rows: {len(df):,}")

# ----------------------- Sidebar: Authorship filters -----------------------
st.sidebar.markdown("---")
st.sidebar.subheader("Authorship filters")

# Extract unique options for institutions and authors
institution_opts = unique_opts_from_list_col(df, "authorships.institutions.display_name")
author_opts = unique_opts_from_list_col(df, "authorships.author.display_name")

# Sidebar multiselect for institutions and authors
sel_institutions = st.sidebar.multiselect("Institutions", institution_opts, default=[])
sel_authors = st.sidebar.multiselect("Authors", author_opts, default=[])

# Apply filters based on selected institutions and authors
if sel_institutions:
    inst_set = set(sel_institutions)
    df = df[df["authorships.institutions.display_name"].apply(lambda v: rows_intersecting(v, inst_set))]
if sel_authors:
    auth_set = set(sel_authors)
    df = df[df["authorships.author.display_name"].apply(lambda v: rows_intersecting(v, auth_set))]

st.caption(f"Active rows after authorship filters: {len(df):,}")

# ----------------------- Visual 1: Publications by year ----------------------
c1, c2 = st.columns(2)
with c1:
    by_year = df["publication_year"].value_counts().sort_index()
    fig = px.bar(by_year, labels={"index": "Year", "value": "Publications"},
                 title="Publications per year")
    st.plotly_chart(fig, use_container_width=True)

# ----------------------- Visual 1b: Scimago rank (journals only) -------------
with c2:
    if only_journals:
        order = ["Q1", "Q2", "Q3", "Q4", "Unranked (that year)", "Not in Scimago", "Other"]
        by_rank = df["scimago_norm"].value_counts().reindex(order, fill_value=0)
        fig = px.bar(by_rank, labels={"index": "Scimago rank", "value": "Count"},
                     title="Publications by Scimago rank (journals only)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Scimago ranking applies to journals—enable 'Only journals' to see this plot.")

# ----------------------- Visual 2: Top sources -------------------------------
st.subheader("Top sources")
top_n = st.slider("How many sources to show", 5, 50, 20, 5, key="n_sources")
src_col = "primary_location.source.display_name"
if src_col in df.columns:
    top_sources = (
        df[src_col].dropna().astype(str).value_counts().head(top_n).sort_values(ascending=True)
    )
    fig = px.bar(top_sources, orientation="h",
                 labels={"index": "Source", "value": "Count"},
                 title=f"Top {top_n} sources")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Column 'primary_location.source.display_name' not available in the loaded data.")

# ----------------------- Visual 3: Citations distribution (improved) ---------
st.subheader("Citations distribution")
if "cited_by_count" in df.columns:
    cits = pd.to_numeric(df["cited_by_count"], errors="coerce").dropna()
    col_left, col_right = st.columns([1,1])
    with col_left:
        cap_pct = st.slider("Cap x-axis at percentile", 80, 100, 99, 1,
                            help="Clips extreme outliers so the distribution is readable.")
    with col_right:
        use_logx = st.checkbox("Log scale (x)", value=False,
                               help="Log10 on x-axis for heavy-tailed distributions.")
    xmax = cits.quantile(cap_pct / 100.0)
    cits_capped = cits[cits <= xmax]
    fig = px.histogram(cits_capped, nbins=50,
                       labels={"value": "Citations", "count": "Publications"},
                       title=f"Citations (≤ {cap_pct}th percentile: ≤ {int(xmax)})")
    if use_logx:
        fig.update_layout(xaxis_type="log")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Column 'cited_by_count' not available.")

# ----------------------- Visual 4: Subfield co-occurrence --------------------
st.subheader("Subfield co-occurrence")
sf_col = "topics.subfield.display_name"
if sf_col in df.columns:
    lists = df[sf_col].apply(to_list)

    if sel_subfields:
        sel_set = set(sel_subfields)
        # rows that contain any selected subfield(s)
        mask = lists.apply(lambda L: bool(set(L) & sel_set))
        sub_lists = lists[mask]
        # count other subfields that appear alongside the selected ones
        co_counts = (
            sub_lists.apply(lambda L: [x for x in L if x not in sel_set])
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
else:
    st.info("Column 'topics.subfield.display_name' not available.")

# ----------------------- Data table & download -------------------------------
st.subheader("Sample of rows")
present_cols = [c for c in ["publication_year", "display_name",
                            "primary_location.source.display_name",
                            "scimago_norm", "cited_by_count", "id"]
                if c in df.columns]
if present_cols:
    st.dataframe(df[present_cols].head(200), use_container_width=True)
else:
    st.info("Sample view: none of the expected display columns are present.")