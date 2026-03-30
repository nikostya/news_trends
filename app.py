import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path
import altair as alt

# -----------------------
# Config
# -----------------------
DATA_PATH = "storage/data"

st.set_page_config(layout="wide")


# date generations
def generate_date_paths(start_date, end_date):
    dates = []
    current = pd.to_datetime(start_date)

    while current <= pd.to_datetime(end_date):
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return dates


# -----------------------
# Sources autodetection
# -----------------------
@st.cache_data(ttl=3600)
def get_available_sources():
    base_path = Path(DATA_PATH)
    sources = []

    for p in base_path.glob("source=*"):
        source_name = p.name.split("=")[1]
        sources.append(source_name)

    return sorted(sources)

from datetime import timedelta

# -----------------------
# Data load
# -----------------------
from pathlib import Path

@st.cache_data(ttl=3600)
def load_data(start_date, end_date, keyword, sources):
    dates = generate_date_paths(start_date, end_date)

    paths = []

    for d in dates:
        path = Path(f"{DATA_PATH}/source=*/date={d}")
        
        # check if any folders with that date exists
        matches = list(Path(DATA_PATH).glob(f"source=*/date={d}"))

        if matches:
            paths.append(f"{DATA_PATH}/source=*/date={d}/**.parquet")

    # ❗ if no any data
    if not paths:
        return pd.DataFrame()

    paths_str = ", ".join([f"'{p}'" for p in paths])

    query = f"""
    SELECT date, source, title
    FROM read_parquet(
        [{paths_str}],
        hive_partitioning=1        
    )
    """

    # sources filter
    if sources:
        sources_str = ",".join([f"'{s}'" for s in sources])
        query += f" WHERE source IN ({sources_str})"

    # keyword filter
    if keyword:
        condition = f"lower(title) LIKE '%{keyword.lower()}%'"
        if "WHERE" in query:
            query += f" AND {condition}"
        else:
            query += f" WHERE {condition}"

    return duckdb.query(query).df()

# -----------------------
# UI — title
# -----------------------
st.title("News Trends Dashboard")

# -----------------------
# Sources (dinamics)
# -----------------------
available_sources = get_available_sources()

if not available_sources:
    st.error("Sources not found in storage/data")
    st.stop()


# -----------------------
# Date range
# -----------------------
min_date = pd.to_datetime("2024-01-01")
max_date = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)
# max_date = pd.to_datetime("2026-03-26")


# -----------------------------
# all filters in one form
# -----------------------------

with st.form("filters_form"):
    sources = st.multiselect(
        "Sources:",
        options=available_sources,
        default=available_sources
    )

    date_range = st.date_input(
        "Date range:",
        value=(min_date, max_date)
    )

    keyword = st.text_input("Key word:")

    submitted = st.form_submit_button("🔍 Apply filters")


start_date, end_date = date_range

# -----------------------
# Data load
# -----------------------
df = load_data(start_date, end_date, keyword, sources)

if df.empty:
    st.warning("No data")
    st.stop()

# -----------------------
# keyword filer (for plot)
# -----------------------
if keyword:
    df["match"] = df["title"].str.contains(keyword, case=False, na=False)
else:
    df["match"] = True

# -----------------------
# Aggregation level selector
# -----------------------
period = st.radio(
    "Aggregation period",
    ["Day", "Month"],
    horizontal=True
)

# -----------------------
# Aggregation
# -----------------------
if period == "Day":
    agg = (
        df.groupby([df["date"].dt.date, "source"])["match"]
        .sum()
        .reset_index()
    )
    x_title = "Date"
    x_format = "%d %b"

else:  # Month
    agg = (
        df.groupby(
            [df["date"].dt.to_period("M").dt.to_timestamp(), "source"]
        )["match"]
        .sum()
        .reset_index()
    )
    x_title = "Month"
    x_format = "%b %Y"

agg.columns = ["date", "source", "count"]

# -----------------------
# Plot
# -----------------------
st.subheader(f"Frequency of mentions (by {period.lower()})")

selection = alt.selection_point(fields=["source"], bind="legend")

chart = alt.Chart(agg).mark_bar().encode(
    x=alt.X(
        "date:T",
        title=x_title,
        axis=alt.Axis(format=x_format)
    ),
    y=alt.Y("sum(count):Q", title="Count"),
    color=alt.Color("source:N", title="Source"),
    opacity=alt.condition(selection, alt.value(1), alt.value(0.2))
).add_params(
    selection
).properties(
    height=400
)

st.altair_chart(chart, width="stretch")

# -----------------------
# News table
# -----------------------
# st.subheader("News")

# st.dataframe(
#    df.sort_values("date", ascending=False)[["date", "source", "title", "url"]],
#    use_container_width=True
# )