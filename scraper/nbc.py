import requests
from bs4 import BeautifulSoup

BASE = "https://www.nbcnews.com"

def _extract_links(soup, selector, base=BASE, max_count=10):
    stories = []
    for item in soup.select(selector):
        title = item.get_text(strip=True)
        if not title:
            continue
        parent = item if item.name == "a" else item.find_parent("a")
        if parent:
            href = parent.get("href", "")
            link = base + href if href.startswith("/") else href
            if link not in [s[2] for s in stories]:
                stories.append(("NBC", title, link))
        if len(stories) >= max_count:
            break
    return stories

def get_nbc_world():
    url = f"{BASE}/world"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    return _extract_links(soup, "h2 a, h3 a, h5, a.card__link", max_count=10)

def get_nbc_us():
    url = f"{BASE}/us-news"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    return _extract_links(soup, "h2 a, h3 a, h5, a.card__link", max_count=10)

def get_nbc_politics():
    url = f"{BASE}/politics"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    return _extract_links(soup, "h2 a, h3 a, h5, a.card__link", max_count=10)

def get_nbc_business():
    url = f"{BASE}/business"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    return _extract_links(soup, "h2 a, h3 a, h5, a.card__link", max_count=10)

def get_nbc_sports():
    url = f"{BASE}/sports"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    return _extract_links(soup, "h2 a, h3 a, h5, a.card__link", max_count=10)

def get_nbc_news():
    return (
        get_nbc_world() +
        get_nbc_us() +
        get_nbc_politics() +
        get_nbc_business() +
        get_nbc_sports()
    )
