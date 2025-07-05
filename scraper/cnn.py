import requests
from bs4 import BeautifulSoup

BASE = "https://www.cnn.com"

def _extract_links(soup, selector, base=BASE, max_count=5):
    stories = []
    for item in soup.select(selector):
        title = item.get_text(strip=True)
        if not title:
            continue
        parent = item.find_parent("a")
        if parent:
            href = parent.get("href", "")
            link = base + href if href.startswith("/") else href
            if link not in [s[2] for s in stories]:
                stories.append(("CNN", title, link))
        if len(stories) >= max_count:
            break
    return stories

def get_cnn_world():
    url = f"{BASE}/world"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    return _extract_links(soup, "h2 span, h3 span, .container__headline span", max_count=10)

def get_cnn_us():
    url = BASE
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    return _extract_links(soup, "h2 span, h3 span, .container__headline span", max_count=10)

def get_cnn_politics():
    url = f"{BASE}/politics"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    return _extract_links(soup, "h2 span, h3 span, .container__headline span", max_count=10)

def get_cnn_business():
    url = f"{BASE}/business"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    return _extract_links(soup, "h2 span, h3 span, .container__headline span", max_count=10)

def get_cnn_sports():
    url = f"{BASE}/sport"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    return _extract_links(soup, "h2 span, h3 span, .container__headline span", max_count=10)

def get_cnn_news():
    return (
        get_cnn_world() +
        get_cnn_us() +
        get_cnn_politics() +
        get_cnn_business() +
        get_cnn_sports()
    )
