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


# генерация дат
def generate_date_paths(start_date, end_date):
    dates = []
    current = pd.to_datetime(start_date)

    while current <= pd.to_datetime(end_date):
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return dates


# -----------------------
# Автоопределение источников
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
# Загрузка данных
# -----------------------
from pathlib import Path

@st.cache_data(ttl=3600)
def load_data(start_date, end_date, keyword, sources):
    dates = generate_date_paths(start_date, end_date)

    paths = []

    for d in dates:
        path = Path(f"{DATA_PATH}/source=*/date={d}")

        # проверяем, есть ли хоть одна папка с таким date
        matches = list(Path(DATA_PATH).glob(f"source=*/date={d}"))

        if matches:
            paths.append(f"{DATA_PATH}/source=*/date={d}/**.parquet")

    # ❗ если вообще нет данных
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

    # фильтр по источникам
    if sources:
        sources_str = ",".join([f"'{s}'" for s in sources])
        query += f" WHERE source IN ({sources_str})"

    # фильтр по keyword
    if keyword:
        condition = f"lower(title) LIKE '%{keyword.lower()}%'"
        if "WHERE" in query:
            query += f" AND {condition}"
        else:
            query += f" WHERE {condition}"

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

# sources = st.multiselect(
#     "Источник",
#     options=available_sources,
#     default=available_sources
# )

# -----------------------
# Выбор даты
# -----------------------
min_date = pd.to_datetime("2024-01-01")
max_date = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)
# max_date = pd.to_datetime("2026-03-26")


# -----------------------------
# all filters in one form
# -----------------------------

with st.form("filters_form"):
    sources = st.multiselect(
        "Источник",
        options=available_sources,
        default=available_sources
    )

    date_range = st.date_input(
        "Диапазон дат",
        value=(min_date, max_date)
    )

    keyword = st.text_input("Ключевое слово")

    submitted = st.form_submit_button("🔍 Применить фильтры")







# date_range = st.date_input(
#     "Диапазон дат",
#     value=(min_date, max_date)
# )

start_date, end_date = date_range

# -----------------------
# Поиск
# -----------------------
# keyword = st.text_input("Ключевое слово")

# -----------------------
# Кнопка обновления
# -----------------------
# if st.button("🔄 Обновить данные"):
#     st.cache_data.clear()
#     st.rerun()

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

#selection = alt.selection_multi(fields=["source"], bind="legend")
selection = alt.selection_point(fields=["source"], bind="legend")

chart = alt.Chart(agg).mark_bar().encode(
    x=alt.X("date:T", title="Дата"),
    # y=alt.Y("sum(count):Q", title="Количество", scale=alt.Scale(domain=[0, 150], clamp=True)),
    y=alt.Y("sum(count):Q", title="Количество"),
    color=alt.Color("source:N", title="Источник"),
    opacity=alt.condition(selection, alt.value(1), alt.value(0.2))
).add_params(
    selection
).properties(
    height=400
)

st.altair_chart(chart, width="stretch")
#st.altair_chart(chart, use_container_width=True)


# -----------------------
# Таблица новостей
# -----------------------
# st.subheader("Новости")

# st.dataframe(
#    df.sort_values("date", ascending=False)[["date", "source", "title", "url"]],
#    use_container_width=True
# )