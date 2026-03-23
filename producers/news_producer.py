import os
import json
import time
import hashlib
import logging
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TICKERS = ["AAPL", "GOOGL", "MSFT", "TSLA", "NVDA"]
KAFKA_TOPIC = "news-articles"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POLL_INTERVAL_SECONDS = 3600  # 1 hour — free tier allows 25 calls/day
AV_BASE_URL = "https://www.alphavantage.co/query"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )


def fetch_news():
    params = {
        "function": "NEWS_SENTIMENT",
        "tickers": ",".join(TICKERS),
        "apikey": os.environ.get("ALPHA_VANTAGE_API_KEY"),
        "limit": 50,
        "sort": "LATEST",
    }
    response = requests.get(AV_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def produce_news(producer, seen_ids):
    data = fetch_news()

    if "Note" in data:
        logger.warning("Alpha Vantage rate limit hit. Skipping this cycle.")
        return

    articles = data.get("feed", [])
    new_count = 0

    for article in articles:
        article_id = hashlib.md5(article.get("url", "").encode()).hexdigest()
        if article_id in seen_ids:
            continue
        seen_ids.add(article_id)

        for ts in article.get("ticker_sentiment", []):
            ticker = ts.get("ticker")
            if ticker not in TICKERS:
                continue

            message = {
                "event_time": datetime.now(timezone.utc).isoformat(),
                "ticker": ticker,
                "article_id": article_id,
                "title": article.get("title", "")[:500],
                "body": article.get("summary", "")[:1000],
                "source": article.get("source", ""),
                "url": article.get("url", ""),
                "av_sentiment_score": float(ts.get("ticker_sentiment_score", 0)),
                "av_sentiment_label": ts.get("ticker_sentiment_label", "Neutral"),
            }
            producer.send(KAFKA_TOPIC, value=message)
            new_count += 1
            logger.info(f"Sent news for {ticker}: {article.get('title', '')[:60]}...")

    producer.flush()
    logger.info(f"Published {new_count} new articles. Total seen: {len(seen_ids)}")


def main():
    logger.info("Starting Alpha Vantage news producer...")
    producer = create_producer()
    seen_ids = set()
    try:
        while True:
            produce_news(producer, seen_ids)
            logger.info(f"Sleeping {POLL_INTERVAL_SECONDS}s...")
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Shutting down news producer.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
