# DEBUG = True # debug
import requests
import time
import re
from bs4 import BeautifulSoup


BASE_URL = "https://www.gazeta.ru/search.shtml"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

NEWS_URL_PATTERN = re.compile(
    r"^https://www\.gazeta\.ru/[a-z]+/news/"
    r"(?P<date>\d{4}/\d{2}/\d{2})/"
    r"\d+\.shtml$"
)


def build_url(date_str: str, page: int) -> str:
    return (
        f"{BASE_URL}"
        f"?p=main"
        f"&page={page}"
        f"&text=%D0%BD%D0%B0"
        f"&input=utf8"
        f"&from={date_str}"
        f"&to={date_str}"
        f"&sort_order=published_desc"
    )


def parse_page(html: str, target_date: str):
    # if DEBUG:                                                   # debug
    #     print("[gazeta][DEBUG] parse_page called")              # debug

    soup = BeautifulSoup(html, "html.parser")

    #results = []

    items = soup.find_all("div", class_="b_ear-title")

    results = []

    for item in items:
        a = item.find("a", href=True)
        if not a:
            continue

        url = a["href"]
        if url.startswith("/"):
            url = "https://www.gazeta.ru" + url

        match = NEWS_URL_PATTERN.match(url)
        if not match:
            continue

        if match.group("date") != target_date:
            continue

        title = a.get_text(strip=True)

        if not title:
            continue

        results.append({
            "title": title,
            "url": url,
            "date": target_date.replace("/", "-")
        })
        
        # if DEBUG:                                                                   # debug
        #     print(f"[gazeta][DEBUG] final results count: {len(results)}")           # debug

    return results


def get_day_news(date):
    """
    date: datetime.date
    """
    date_str = date.strftime("%Y-%m-%d")
    target_date = date.strftime("%Y/%m/%d")

    all_news = []
    page = 1

    while True:

        url = build_url(date_str, page)

        # if DEBUG:                                           # debug
        #     print(f"[gazeta][DEBUG] page={page} url={url}") # debug

        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
        except Exception as e:
            print(f"[gazeta] error page {page}: {e}")
            break

        news = parse_page(resp.text, target_date)

        # if DEBUG:                                                               # debug
        #     print(f"[gazeta][DEBUG] page={page} parsed_items={len(news)}")      # debug
        #     if len(news) > 0:                                                   # debug
        #         print("[gazeta][DEBUG] sample:", news[:2])                      # debug

        if not news:
            break

        all_news.extend(news)

        print(f"[gazeta] {date_str} page {page}: {len(news)}")

        page += 1
        time.sleep(1.1)

        # if page > 10:                                               # debug
        #     print("[gazeta][DEBUG] forced stop at page 10")         # debug
        #     break                                                   # debug

    return all_news