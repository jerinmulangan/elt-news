CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS articles (
  id         SERIAL PRIMARY KEY,
  url        TEXT        NOT NULL UNIQUE,
  fetched_at TIMESTAMP   NOT NULL DEFAULT NOW(),
  payload    JSONB       NOT NULL,
  tsv        TSVECTOR
);

CREATE FUNCTION articles_tsv_trigger() RETURNS trigger AS $$
begin
  new.tsv :=
    to_tsvector('english', coalesce(new.payload->>'title','') || ' ' ||
                         coalesce(new.payload->>'body',''));
  return new;
end
$$ LANGUAGE plpgsql;

CREATE TRIGGER tsvectorupdate
  BEFORE INSERT OR UPDATE ON articles
  FOR EACH ROW EXECUTE FUNCTION articles_tsv_trigger();

CREATE INDEX IF NOT EXISTS idx_articles_tsv ON articles USING GIN(tsv);