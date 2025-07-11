import requests
from bs4 import BeautifulSoup

BASE = "https://www.npr.org"

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
                stories.append(("NPR", title, link))
        if len(stories) >= max_count:
            break
    return stories

def get_npr_world():
    url = f"{BASE}/sections/world/"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    return _extract_links(soup, "article h2 a", max_count=10)

def get_npr_us():
    url = f"{BASE}/sections/national/"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    return _extract_links(soup, "article h2 a", max_count=10)

def get_npr_politics():
    url = f"{BASE}/sections/politics/"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    return _extract_links(soup, "article h2 a", max_count=10)

def get_npr_business():
    url = f"{BASE}/sections/business/"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    return _extract_links(soup, "article h2 a", max_count=10)

def get_npr_news():
    return (
        get_npr_world() +
        get_npr_us() +
        get_npr_politics() +
        get_npr_business()
    )
