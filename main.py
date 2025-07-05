import sys
from PyQt5.QtWidgets import QApplication
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

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

def fetch_news(platforms, topics):
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
        for topic in topics:
            func = platform_topic_map.get(platform, {}).get(topic)
            if func:
                stories += func()
    return stories


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = NewsApp(fetch_news)
    window.show()
    sys.exit(app.exec_())
