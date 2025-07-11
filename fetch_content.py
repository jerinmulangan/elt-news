import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import trafilatura
import requests
from readability import Document
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logging.error("DATABASE_URL not set in .env")
    exit(1)

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def fetch_missing_articles():
    query = """
        SELECT id, payload->>'url' AS url
        FROM articles
        WHERE NOT (payload ? 'content');
        """
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
        logging.info(f"Fetched {len(rows)} articles missing content from DB")
        return rows

def extract_content_trafilatura(url):
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            text = trafilatura.extract(downloaded, include_comments=False, include_tables=False)
            if text:
                logging.info(f"Trafilatura extracted content of length {len(text)} for {url}")
                return text
    except Exception as e:
        logging.warning(f"Trafilatura failed for {url}: {e}")
    return None

def extract_content_readability(url):
    try:
        resp = requests.get(url, timeout=10)
        doc = Document(resp.text)
        html = doc.summary()
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text(separator=' ', strip=True)
        if text:
            logging.info(f"Readability extracted content of length {len(text)} for {url}")
            return text
    except Exception as e:
        logging.error(f"Readability fallback failed for {url}: {e}")
    return None

def extract_content(url):
    logging.info(f"Attempting Trafilatura for {url}")
    content = extract_content_trafilatura(url)
    if content and len(content) > 200:
        return content
    logging.info(f"Trafilatura result insufficient, falling back for {url}")
    return extract_content_readability(url)

def update_article_content(article_id, content):
    query = """
        UPDATE articles
        SET payload = jsonb_set(payload, '{content}', to_jsonb(%s::text))
        WHERE id = %s;
        """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(query, (content, article_id))
        conn.commit()
        logging.info(f"Updated article id {article_id} in DB")

def main():
    missing = fetch_missing_articles()
    if not missing:
        logging.info("No articles to update.")
        return
    for art in missing:
        aid = art['id']
        url = art['url']
        logging.info(f"Processing article id {aid}, url: {url}")
        content = extract_content(url)
        if content:
            update_article_content(aid, content)
        else:
            logging.warning(f"Content extraction returned None for article id {aid}")

if __name__ == '__main__':
    logging.info("Start Script")
    main()
    logging.info("Script Complete")
