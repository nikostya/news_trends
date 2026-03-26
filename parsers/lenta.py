import requests
from bs4 import BeautifulSoup
import time

BASE_URL = "https://lenta.ru"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

MAX_PAGES = 20
PAUSE_BETWEEN_PAGES = 2

import requests
from bs4 import BeautifulSoup
import time

BASE_URL = "https://lenta.ru"
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def get_day_news(date):
    news = []
    page = 1

    while True:
        if page > MAX_PAGES:
            print("Достигнут лимит страниц")
            break

        if page == 1:
            url = f"{BASE_URL}/{date.strftime('%Y/%m/%d/')}"
        else:
            url = f"{BASE_URL}/{date.strftime('%Y/%m/%d/')}" + f"page/{page}/"

        response = requests.get(url, headers=HEADERS)

        if response.status_code != 200:
            print(f"Ошибка {response.status_code} на {url}")
            break

        soup = BeautifulSoup(response.text, "html.parser")

        items = soup.find_all("a", class_="card-full-news")

        # 🔴 КЛЮЧЕВАЯ ПРОВЕРКА
        if not items:
            break

        for item in items:
            # title = item.text.strip()
            title_tag = item.find("h3", class_="card-full-news__title")
            title = title_tag.get_text(strip=True) if title_tag else None

            if not title:
                print("Пустой title:", item)

            link = item.get("href")

            if link and link.startswith("/"):
                link = BASE_URL + link

            news.append({
                "date": date.strftime("%Y-%m-%d"),
                "title": title,
                "url": link
            })

        print(f"  страница {page}: {len(items)} новостей")

        page += 1
        time.sleep(2)

    return news