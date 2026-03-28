import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path
import altair as alt

# -----------------------
# Конфиг
# -----------------------
DATA_PATH = "storage/data"

st.set_page_config(layout="wide")

# -----------------------
# Автоопределение источников
# -----------------------
@st.cache_data(ttl=60)
def get_available_sources():
    base_path = Path(DATA_PATH)
    sources = []

    for p in base_path.glob("source=*"):
        source_name = p.name.split("=")[1]
        sources.append(source_name)

    return sorted(sources)


# -----------------------
# Загрузка данных
# -----------------------
@st.cache_data(ttl=300)
def load_data(start_date, end_date, keyword, sources):
    query = f"""
    SELECT *,
           CAST(date AS DATE) as date,
           source
    FROM read_parquet(
        '{DATA_PATH}/source=*/date=*/**.parquet',
        hive_partitioning=1,
        union_by_name=True
    )
    WHERE CAST(date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    """

    if sources:
        sources_str = ",".join([f"'{s}'" for s in sources])
        query += f" AND source IN ({sources_str})"

    if keyword:
        query += f" AND lower(title) LIKE '%{keyword.lower()}%'"

    return duckdb.query(query).df()


# -----------------------
# UI — заголовок
# -----------------------
st.title("News Trends Dashboard")

# -----------------------
# Источники (динамически)
# -----------------------
available_sources = get_available_sources()

if not available_sources:
    st.error("Не найдено ни одного источника в storage/data")
    st.stop()

sources = st.multiselect(
    "Источник",
    options=available_sources,
    default=available_sources
)

# -----------------------
# Выбор даты
# -----------------------
min_date = pd.to_datetime("2025-01-01")
max_date = pd.to_datetime("today")

date_range = st.date_input(
    "Диапазон дат",
    value=(min_date, max_date)
)

start_date, end_date = date_range

# -----------------------
# Поиск
# -----------------------
keyword = st.text_input("Ключевое слово")

# -----------------------
# Кнопка обновления
# -----------------------
if st.button("🔄 Обновить данные"):
    st.cache_data.clear()
    st.experimental_rerun()

# -----------------------
# Загрузка данных
# -----------------------
df = load_data(start_date, end_date, keyword, sources)

if df.empty:
    st.warning("Нет данных")
    st.stop()

# -----------------------
# Фильтр keyword (для графика)
# -----------------------
if keyword:
    df["match"] = df["title"].str.contains(keyword, case=False, na=False)
else:
    df["match"] = True

# -----------------------
# Агрегация (stacked)
# -----------------------
agg = (
    df.groupby([df["date"].dt.date, "source"])["match"]
    .sum()
    .reset_index()
)

agg.columns = ["date", "source", "count"]

# -----------------------
# График (stacked bar)
# -----------------------
st.subheader("Частота упоминаний (по источникам)")

selection = alt.selection_multi(fields=["source"], bind="legend")

chart = alt.Chart(agg).mark_bar().encode(
    x=alt.X("date:T", title="Дата"),
    y=alt.Y("sum(count):Q", title="Количество"),
    color=alt.Color("source:N", title="Источник"),
    opacity=alt.condition(selection, alt.value(1), alt.value(0.2))
).add_params(
    selection
).properties(
    height=400
)

st.altair_chart(chart, use_container_width=True)

# -----------------------
# Таблица новостей
# -----------------------
st.subheader("Новости")

st.dataframe(
    df.sort_values("date", ascending=False)[["date", "source", "title", "url"]],
    use_container_width=True
)