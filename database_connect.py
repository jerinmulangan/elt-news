import os
import json
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values


load_dotenv()  

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found in environment")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        with open("schema.sql", "r") as f:
            cur.execute(f.read())
        conn.commit()

def upsert_articles(articles):

    sql = """
    INSERT INTO articles (url, payload, fetched_at)
    VALUES %s
    ON CONFLICT (url) DO UPDATE
      SET payload = EXCLUDED.payload,
          fetched_at = EXCLUDED.fetched_at;
    """
    records = [
        (art["url"], json.dumps(art["payload"]), art.get("fetched_at"))
        for art in articles
    ]
    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, records)
        conn.commit()
