#scraper/tldr_utils.py

def fix_tldr_link(href: str) -> str:
   
    if href.startswith("http"):
        return href
    return "https://tldr.tech" + href
