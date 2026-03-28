
import pandas as pd
from datetime import datetime, timedelta, date
import time
import os

from parsers.lenta import get_day_news
from parsers.gazeta import get_day_news as get_gazeta_news


import hashlib

BASE_DOMAIN = "https://www.gazeta.ru"
STATS_PATH = "storage/meta/stats.parquet"
PAUSE_BETWEEN_PAGES = 2

from datetime import date, timedelta

# run parser for a source and a range of dates
def run_source(source, raw_func, start_date):
    stats = load_stats()

    dates = get_dates_to_load(stats, source, start_date)

    print(f"[{source}] К загрузке дат: {len(dates)}")

    for d in dates:
        load_for_date(source, raw_func, d)

# Deterine which dates to load
def get_dates_to_load(stats, source, start_date):
    
    if isinstance(start_date, datetime):
        start_date = start_date.date()

    today = date.today()

    # даты, которые уже есть
    existing = stats[stats["source"] == source]

    success_dates = set(existing[existing["status"] == "success"]["date"])
    failed_dates = set(existing[existing["status"] == "failed"]["date"])

    # полный диапазон
    dates = []
    current = start_date

    while current <= today:
        d_str = current.strftime("%Y-%m-%d")

        if d_str not in success_dates:
            dates.append(current)

        current += timedelta(days=1)

    # добавляем failed (на всякий случай)
    #failed_dt = [datetime.strptime(d, "%Y-%m-%d") for d in failed_dates]
    failed_dt = [datetime.strptime(d, "%Y-%m-%d").date() for d in failed_dates]

    return sorted(set(dates + failed_dt))

# Load statistics about collected data
def load_stats():
    if not os.path.exists(STATS_PATH):
        return pd.DataFrame(columns=["source", "date", "status", "count", "updated_at"])

    return pd.read_parquet(STATS_PATH)

# Save statistics about collected data
def save_stats(df):
    df.to_parquet(STATS_PATH, index=False)

# Update statistics about collected data
def update_stats(stats_df, source, date, status, count):
    date_str = date.strftime("%Y-%m-%d")

    # удаляем старую запись (если была)
    stats_df = stats_df[
        ~((stats_df["source"] == source) & (stats_df["date"] == date_str))
    ]

    # добавляем новую
    new_row = pd.DataFrame([{
        "source": source,
        "date": date_str,
        "status": status,
        "count": count,
        "updated_at": datetime.now()
    }])

    stats_df = pd.concat([stats_df, new_row], ignore_index=True)

    return stats_df


from core.transform import build_dataframe
from parsers.lenta import get_day_news

def load_for_date(source, raw_func, date):
    stats = load_stats()

    print(f"[{source}] Загрузка {date.strftime('%Y-%m-%d')}")

    try:
        news = raw_func(date)

        # print(f"[DEBUG] raw news count: {len(news)}")           # debug
        # if len(news) > 0:                                       # debug
        #     print("[DEBUG] raw sample:", news[:2])              # debug

        df = build_dataframe(news, source)                      

        # print(f"[DEBUG] df shape: {df.shape}")                  # debug

        if df.empty:
            print("Нет данных")
            stats = update_stats(stats, source, date, "failed", 0)
        else:
            write_partitioned(df, source=source)
            stats = update_stats(stats, source, date, "success", len(df))

            print(f"Сохранено строк: {len(df)}")

    except Exception as e:
        print(f"[{source}] Ошибка: {e}")
        stats = update_stats(stats, source, date, "failed", 0)

    save_stats(stats)

def collect_period(start_date, end_date):
    """
    Обходит диапазон дат
    """
    all_news = []

    current = start_date

    while current <= end_date:
        print(f"Собираем {current.strftime('%Y-%m-%d')}")

        try:
            day_news = get_day_news(current)
            all_news.extend(day_news)
        except Exception as e:
            print(f"Ошибка на {current}: {e}")

        current += timedelta(days=1)
        time.sleep(PAUSE_BETWEEN_PAGES)

    return pd.DataFrame(all_news)

def write_partitioned(df, source="lenta"):
    df = df.copy()
    df["source"] = source

    df.to_parquet(
        "storage/data",
        engine="pyarrow",
        partition_cols=["source", "date"],
        index=False
    )


SOURCES = [
    ("lenta", get_day_news),
    ("gazeta", get_gazeta_news),
]

def run_all(start_date):
    for source, raw_func in SOURCES:
        print(f"=== Запуск {source} ===")

        try:
            run_source(source, raw_func, start_date)
        except Exception as e:
            print(f"Критическая ошибка в {source}: {e}")



if __name__ == "__main__":
    start_date = datetime(2025, 1, 1)
    run_all(start_date)