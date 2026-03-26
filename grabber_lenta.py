
import pandas as pd
from datetime import datetime, timedelta, date
import time
import os

from parsers.lenta import get_day_news

import hashlib

STATS_PATH = "storage/meta/stats.parquet"

from datetime import date, timedelta

from datetime import date, timedelta

# run parser for a source and a range of dates
def run_lenta(start_date):
    stats = load_stats()

    dates = get_dates_to_load(stats, "lenta", start_date)

    print(f"К загрузке дат: {len(dates)}")

    for d in dates:
        load_lenta_for_date(d)

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

def make_id(row):
    raw = f"{row['title'].strip().lower()}_{row['date']}_lenta"
    return hashlib.sha1(raw.encode()).hexdigest()


def fetch_lenta_for_date(date):
    news = get_day_news(date)

    df = pd.DataFrame(news)

    if df.empty:
        return df

    df = df[["date", "title", "url"]]

    # 👉 добавляем id
    df["id"] = df.apply(make_id, axis=1)

    # 👉 убираем дубли
    df = df.drop_duplicates(subset="id")

    return df

def load_lenta_for_date(date):
    stats = load_stats()

    print(f"Загрузка {date.strftime('%Y-%m-%d')}")

    try:
        df = fetch_lenta_for_date(date)

        if df.empty:
            print("Нет данных")
            stats = update_stats(stats, "lenta", date, "failed", 0)
        else:
            write_partitioned(df, source="lenta")
            stats = update_stats(stats, "lenta", date, "success", len(df))

            print(f"Сохранено строк: {len(df)}")

    except Exception as e:
        print(f"Ошибка: {e}")
        stats = update_stats(stats, "lenta", date, "failed", 0)

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

# if __name__ == "__main__":
#     start = datetime(2026, 3, 1)
#     end = datetime(2026, 3, 3)

#     df = collect_period(start, end)

#     df.drop_duplicates(subset="url", inplace=True)

#     df.to_csv("newstitles/lenta_news.csv", index=False, encoding="utf-8-sig")
#     # df.to_parquet("newstitles/lenta.parquet", index=False)
#     write_partitioned(df)

#     print("Готово:", len(df))

# if __name__ == "__main__":
#     test_date = datetime(2026, 3, 1)

#     df = fetch_lenta_for_date(test_date)

#     print(df.head())
#     print("rows:", len(df))
#     print(df.columns)

if __name__ == "__main__":
    start_date = datetime(2025, 1, 1)

    run_lenta(start_date)