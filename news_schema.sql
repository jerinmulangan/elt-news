CREATE TABLE articles (
  id          SERIAL PRIMARY KEY,
  url         TEXT UNIQUE,
  fetched_at  TIMESTAMP NOT NULL,
  payload     JSONB,
  tsv         TSVECTOR
);

CREATE FUNCTION articles_tsv_trigger() RETURNS trigger AS $$
BEGIN
  NEW.tsv :=
    to_tsvector('english',
      coalesce(NEW.payload->>'title','') || ' ' ||
      coalesce(NEW.payload->>'body','')
    );
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER tsvectorupdate
  BEFORE INSERT OR UPDATE ON articles
  FOR EACH ROW EXECUTE FUNCTION articles_tsv_trigger();

CREATE INDEX articles_tsv_idx ON articles USING GIN(tsv);