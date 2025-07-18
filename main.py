import sys
from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import QTimer
from gui import NewsApp
from scraper.cnn import get_cnn_world, get_cnn_us, get_cnn_politics, get_cnn_business, get_cnn_sports
from scraper.nbc import get_nbc_world, get_nbc_us, get_nbc_politics, get_nbc_business, get_nbc_sports
from scraper.npr import get_npr_world, get_npr_us, get_npr_politics, get_npr_business
from scraper.tldr_tech import get_tldr_tech_articles
from scraper.tldr_infosec import get_tldr_infosec_articles
from scraper.tldr_webdev import get_tldr_webdev_articles
from scraper.tldr_devops import get_tldr_devops_articles
from scraper.tldr_ai import get_tldr_ai_articles
from scraper.tldr_data import get_tldr_data_articles
import json
from datetime import datetime, timezone
from database_connect import init_db, upsert_articles
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

def _find_url_and_title(item, platform, topic):
    title = None
    link = None
    if isinstance(item, dict):
        title = item.get("title") or item.get("text") or item.get("heading")
        link  = item.get("url") or item.get("link") or item.get("href")
    elif isinstance(item, (tuple, list)):
        flat = []
        for e in item:
            if isinstance(e, (tuple, list)):
                flat.extend(e)
            else:
                flat.append(e)
        for e in flat:
            if isinstance(e, str) and re.match(r"^https?://", e):
                link = e
                break
        cand = [
            e for e in flat
            if isinstance(e, str)
               and not re.match(r"^https?://", e)
               and e != platform
               and e != topic
        ]
        if cand:
            title = max(cand, key=len)
    if not (title and link):
        return None, None
    # link = urljoin("<BASE_URL_FOR_THIS_PLATFORM>", link)
    return title, link

def fetch_news(platforms=None, topics=None):
    platform_topic_map = {
        "CNN": {
            "World": get_cnn_world,
            "US News": get_cnn_us,
            "Politics": get_cnn_politics,
            "Business": get_cnn_business,
            "Sports": get_cnn_sports,
        },
        "NBC": {
            "World": get_nbc_world,
            "US News": get_nbc_us,
            "Politics": get_nbc_politics,
            "Business": get_nbc_business,
            "Sports": get_nbc_sports,
        },
        "NPR": {
            "World": get_npr_world,
            "US News": get_npr_us,
            "Politics": get_npr_politics,
            "Business": get_npr_business,
        },
        "TLDR": {
            "Tech": get_tldr_tech_articles,
            "Infosec": get_tldr_infosec_articles,
            "WebDev": get_tldr_webdev_articles,
            "DevOps": get_tldr_devops_articles,
            "AI": get_tldr_ai_articles,
            "Data": get_tldr_data_articles
        }
    }

    all_platforms = list(platform_topic_map.keys())
    all_topics = sorted({topic for pt_map in platform_topic_map.values() for topic in pt_map})

    if not platforms:
        platforms = all_platforms
    if not topics:
        topics = all_topics
    
    stories = []
    for platform in platforms:
        pt_map = platform_topic_map.get(platform, {})
        for topic in topics:
            fn = pt_map.get(topic)
            if not fn:
                continue
            for item in fn():
                if isinstance(item, tuple) and len(item) == 3:
                    source, title, link = item
                elif isinstance(item, tuple) and len(item) == 2:
                    source = platform
                    title, link = item
                elif isinstance(item, dict):
                    source = platform
                    title  = item.get("title")
                    link   = item.get("url") or item.get("link")
                else:
                    continue
                if not (title and link):
                    continue
                stories.append((source, title, link))

    return stories

def scrape_and_store():
    now = datetime.now(timezone.utc)
    print(f"[{now}] Scrape Start")
    raw = fetch_news() 
    batch = []
    for source, title, link in raw:
        batch.append({
            "url":        link,
            "payload": {
                "source": source,
                "title":  title,
                "url":    link
            },
            "fetched_at": now
        })
    upsert_articles(batch)
    print(f"[{datetime.now(timezone.utc)}] Upserted {len(batch)} articles.")

if __name__ == "__main__":
    init_db()
    scrape_and_store()
    print(fetch_news())
    app = QApplication(sys.argv)
    window = NewsApp(fetch_news)
    timer = QTimer()
    timer.timeout.connect(scrape_and_store)
    timer.start(6 * 60 * 60 * 1000)
    window.show()
    sys.exit(app.exec_())
