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
    drop_sql = """
    DROP TRIGGER IF EXISTS tsvectorupdate ON articles;
    DROP FUNCTION IF EXISTS articles_tsv_trigger();

    -- drop index and table
    DROP INDEX IF EXISTS articles_tsv_idx;
    DROP TABLE IF EXISTS articles;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(drop_sql)
        conn.commit()

    with get_conn() as conn, conn.cursor() as cur:
        with open("news_schema.sql", "r") as f:
            cur.execute(f.read())
        conn.commit()

def upsert_articles(articles):

    unique = {}
    for art in articles:
        unique[art["url"]] = art
    deduped = list(unique.values())

    sql = """
    INSERT INTO articles (url, payload, fetched_at)
    VALUES %s
    ON CONFLICT (url) DO UPDATE
      SET payload    = EXCLUDED.payload,
          fetched_at = EXCLUDED.fetched_at;
    """
    records = [
        (art["url"], json.dumps(art["payload"]), art["fetched_at"])
        for art in deduped
    ]
    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, records)
        conn.commit()
