import streamlit as st
import pandas as pd

# -----------------------
# Загрузка данных
# -----------------------
import duckdb

@st.cache_data
def load_data(start_date, end_date, keyword):
    query = f"""
    SELECT *,
       CAST(date AS DATE) as date
    FROM read_parquet(
        'storage/data/source=lenta/date=*/**.parquet',
        union_by_name=True
    )
    WHERE CAST(date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    """

    if keyword:
        query += f"""
        AND lower(title) LIKE '%{keyword.lower()}%'
        """

    return duckdb.query(query).df()

# -----------------------
# UI — заголовок
# -----------------------
st.title("News Trends Dashboard")

# -----------------------
# Выбор даты
# -----------------------
min_date = pd.to_datetime("2024-01-01")
max_date = pd.to_datetime("today")

date_range = st.date_input(
    "Выбери диапазон дат",
    value=(min_date, max_date)
)

start_date, end_date = date_range

# -----------------------
# Поиск
# -----------------------
keyword = st.text_input("Ключевое слово")

# -----------------------
# Загрузка данных
# -----------------------
df = load_data(start_date, end_date, keyword)

if df.empty:
    st.warning("Нет данных")
    st.stop()

# -----------------------
# График частоты
# -----------------------
st.subheader("Частота упоминаний")

df_filtered = df

if keyword:
    df_filtered["match"] = df_filtered["title"].str.contains(keyword, case=False, na=False)
else:
    df_filtered["match"] = True

# график по дням
# freq = (
#     df_filtered
#     .groupby(df_filtered["date"].dt.date)["match"]
#     .sum()
#     .reset_index()
# )
# freq.columns = ["date", "count"]

# график по неделям
freq = (
    df_filtered
    .assign(
        iso_year=df_filtered["date"].dt.isocalendar().year,
        iso_week=df_filtered["date"].dt.isocalendar().week
    )
    .groupby(["iso_year", "iso_week"])["match"]
    .sum()
    .reset_index()
)

# делаем удобную дату (понедельник недели)
freq["date"] = pd.to_datetime(
    freq["iso_year"].astype(str) + "-W" + freq["iso_week"].astype(str) + "-1",
    format="%G-W%V-%u"
)

freq = freq[["date", "match"]]
freq.columns = ["date", "count"]


# st.line_chart(freq.set_index("date"))
st.bar_chart(freq.set_index("date"))

# -----------------------
# Таблица новостей
# -----------------------
st.dataframe(
    df.sort_values("date", ascending=False)[["date", "title", "url"]],
    use_container_width=True
)