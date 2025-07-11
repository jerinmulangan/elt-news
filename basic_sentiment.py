import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from transformers import pipeline, LongformerForSequenceClassification, LongformerTokenizer, AutoTokenizer
import nltk
from nltk import sent_tokenize

def ensure_nltk():
    try:
        nltk.download('punkt', quiet=True)
    except:
        pass
ensure_nltk()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logging.error("DATABASE_URL not set in .env")
    exit(1)

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def fetch_articles_with_content():
    query = """
    SELECT id, payload->>'content' AS content
    FROM articles
    WHERE payload ? 'content';
    """
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
        logging.info(f"Fetched {len(rows)} articles with content from DB")
        return rows

sent_pipe = pipeline(
    "sentiment-analysis",
    model="distilbert-base-uncased-finetuned-sst-2-english",
    tokenizer=AutoTokenizer.from_pretrained("distilbert-base-uncased-finetuned-sst-2-english")
)

doc_model_name = "allenai/longformer-base-4096"
doc_pipe = pipeline(
    "text-classification",
    model=LongformerForSequenceClassification.from_pretrained(doc_model_name, num_labels=3),
    tokenizer=LongformerTokenizer.from_pretrained(doc_model_name),
    top_k=None
)

def classify_hierarchical(article_text):
    sentences = sent_tokenize(article_text)
    sent_scores = []
    for s in sentences:
        try:
            res = sent_pipe(s)
            if isinstance(res, list):
                res = res[0]
            sent_scores.append(res)
        except Exception as e:
            logging.warning(f"Sentence classification failed: {e}")

    if sent_scores:
        numeric = [1 if sc.get('label') == 'POSITIVE' else -1 for sc in sent_scores]
        agg_score = sum(numeric) / len(numeric)
    else:
        agg_score = 0.0

    try:
        doc_scores = doc_pipe(article_text)
    except Exception as e:
        logging.error(f"Document-level classification failed: {e}")
        doc_scores = []

    return {
        'sentence_scores': sent_scores,
        'aggregate_sentence_score': agg_score,
        'doc_scores': doc_scores
    }

if __name__ == '__main__':
    articles = fetch_articles_with_content()
    for art in articles:
        aid = art['id']
        content = art['content']
        logging.info(f"Classifying article {aid}")
        results = classify_hierarchical(content)
        print(f"Article {aid}: agg sentence score = {results['aggregate_sentence_score']}")
        print(f"Document-level scores: {results['doc_scores']}\n")
