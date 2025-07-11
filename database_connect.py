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
    schema_sql = """
        CREATE TABLE IF NOT EXISTS articles (
        id          SERIAL PRIMARY KEY,
        url         TEXT        UNIQUE,
        fetched_at  TIMESTAMP   NOT NULL,
        payload     JSONB       NOT NULL,
        tsv         TSVECTOR
        );

        CREATE OR REPLACE FUNCTION articles_tsv_trigger() RETURNS trigger AS $$
        BEGIN
        NEW.tsv :=
            to_tsvector('english',
            coalesce(NEW.payload->>'title','') || ' ' ||
            coalesce(NEW.payload->>'body','')
            );
        RETURN NEW;
        END
        $$ LANGUAGE plpgsql;

        DO $$
        BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = 'tsvectorupdate'
            AND tgrelid = 'articles'::regclass
        ) THEN
            CREATE TRIGGER tsvectorupdate
            BEFORE INSERT OR UPDATE ON articles
            FOR EACH ROW EXECUTE FUNCTION articles_tsv_trigger();
        END IF;
        END
        $$;

        CREATE INDEX IF NOT EXISTS articles_tsv_idx
        ON articles USING GIN(tsv);
        """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(schema_sql)
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
