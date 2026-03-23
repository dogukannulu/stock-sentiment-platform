"""
Integration tests: producer → Kafka → consumer message confirmed.
Requires Docker stack running: docker compose up -d
"""
import pytest
import json
import time
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

load_dotenv()

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _make_price_tick(ticker: str) -> dict:
    return {
        "event_time": datetime.now(timezone.utc).isoformat(),
        "ticker": ticker,
        "price": 175.50,
        "volume": 1000000,
        "source": "yahoo_finance",
    }


def _make_news_article(ticker: str) -> dict:
    return {
        "event_time": datetime.now(timezone.utc).isoformat(),
        "ticker": ticker,
        "article_id": f"test_{ticker}_{int(time.time())}",
        "title": f"{ticker} integration test article",
        "body": "This is a test article body.",
        "source": "test",
        "url": "https://example.com/test",
        "av_sentiment_score": 0.5,
        "av_sentiment_label": "Bullish",
    }


@pytest.fixture(scope="module")
def kafka_available():
    try:
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
        producer.close()
        return True
    except NoBrokersAvailable:
        pytest.skip("Kafka not available — start docker compose up -d")


def test_price_tick_reaches_kafka(kafka_available):
    """Produce one price tick and verify it arrives in the topic."""
    unique_ticker = f"TEST_{int(time.time())}"
    tick = _make_price_tick("AAPL")
    tick["source"] = unique_ticker  # use source as unique marker

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    producer.send("price-ticks", value=tick)
    producer.flush()
    producer.close()

    consumer = KafkaConsumer(
        "price-ticks",
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        group_id=f"integration-test-price-{int(time.time())}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    for msg in consumer:
        data = msg.value
        if data.get("source") == unique_ticker:
            assert data["price"] > 0
            assert "event_time" in data
            consumer.close()
            return

    consumer.close()
    pytest.fail("No price tick received within timeout")


def test_news_article_reaches_kafka(kafka_available):
    """Produce one news article and verify it arrives in the topic."""
    unique_id = f"integration_test_{int(time.time())}"
    article = _make_news_article("TSLA")
    article["article_id"] = unique_id

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    producer.send("news-articles", value=article)
    producer.flush()
    producer.close()

    consumer = KafkaConsumer(
        "news-articles",
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        group_id=f"integration-test-news-{int(time.time())}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    for msg in consumer:
        data = msg.value
        if data.get("article_id") == unique_id:
            assert len(data.get("title", "")) > 0
            assert data["ticker"] == "TSLA"
            consumer.close()
            return

    consumer.close()
    pytest.fail("No news article received within timeout")


def test_price_tick_message_schema(kafka_available):
    """Verify price tick messages conform to the Avro schema field names."""
    tick = _make_price_tick("NVDA")
    required_fields = {"event_time", "ticker", "price", "source"}
    assert required_fields.issubset(tick.keys())
    assert isinstance(tick["price"], float)
    assert tick["ticker"] == "NVDA"


def test_news_article_message_schema(kafka_available):
    """Verify news article messages conform to the Avro schema field names."""
    article = _make_news_article("MSFT")
    required_fields = {"event_time", "ticker", "article_id", "title", "body", "source", "url"}
    assert required_fields.issubset(article.keys())
    assert article["ticker"] == "MSFT"
