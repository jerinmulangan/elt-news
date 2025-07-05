#tldr-devops.py
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from scraper.tldr_utils import fix_tldr_link

BASE_URL = "https://tldr.tech/devops"

def get_tldr_devops_articles():
    headers = {"User-Agent": "Mozilla/5.0"}
    
    for offset in range(2): 
        date = (datetime.now() - timedelta(days=offset)).strftime("%Y-%m-%d")
        url = f"{BASE_URL}/{date}"
        try:
            res = requests.get(url, headers=headers, timeout=10)

            if res.url.rstrip("/") != url.rstrip("/"):
                continue

            soup = BeautifulSoup(res.text, "html.parser")
            articles = []
            
            for section in soup.select("section")[1:]:
                for a_tag in section.select("article a"):
                    title = a_tag.get_text(strip=True)
                    href = a_tag.get("href", "")
                    if title and href:
                        link = fix_tldr_link(href)
                        articles.append(("TLDR DevOps", title, link))
            
            if articles:
                return articles

        except Exception as e:
            print(f"Error fetching {url}: {e}")
            continue

    return [] 

if __name__ == "__main__":
    for src, title, link in get_tldr_devops_articles():
        print(f"[{src}] {title}\n{link}\n")
