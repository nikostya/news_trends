import hashlib

def make_id(row, source):
    raw = f"{row['title'].strip().lower()}_{row['date']}_{source}"
    return hashlib.sha1(raw.encode()).hexdigest()

import pandas as pd

def build_dataframe(news, source):
    df = pd.DataFrame(news)

    if df.empty:
        return df

    df = df[["date", "title", "url"]]

    df["id"] = df.apply(lambda row: make_id(row, source), axis=1)

    df = df.drop_duplicates(subset="id")

    return df