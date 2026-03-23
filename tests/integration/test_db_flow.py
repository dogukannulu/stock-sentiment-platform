"""
Integration tests: TimescaleDB inserts, hypertable verification, and queries.
Requires Docker stack running: docker compose up -d
"""
import pytest
import psycopg2
import json
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(scope="module")
def db_conn():
    try:
        conn = psycopg2.connect(
            host=os.environ.get("TIMESCALE_HOST", "localhost"),
            port=int(os.environ.get("TIMESCALE_PORT", 5432)),
            dbname=os.environ.get("TIMESCALE_DB", "sentiment_db"),
            user=os.environ.get("TIMESCALE_USER", "sentiment_user"),
            password=os.environ.get("TIMESCALE_PASSWORD"),
        )
        yield conn
        conn.close()
    except psycopg2.OperationalError:
        pytest.skip("TimescaleDB not available — start docker compose up -d")


def test_price_ticks_table_exists(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = 'price_ticks'
        """)
        assert cur.fetchone()[0] == 1


def test_news_articles_table_exists(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = 'news_articles'
        """)
        assert cur.fetchone()[0] == 1


def test_sentiment_events_table_exists(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = 'sentiment_events'
        """)
        assert cur.fetchone()[0] == 1


def test_all_tables_are_hypertables(db_conn):
    expected = {"price_ticks", "news_articles", "sentiment_events"}
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT hypertable_name FROM timescaledb_information.hypertables
            WHERE hypertable_schema = 'public'
        """)
        actual = {row[0] for row in cur.fetchall()}
    assert expected.issubset(actual), f"Missing hypertables: {expected - actual}"


def test_insert_and_query_price_tick(db_conn):
    now = datetime.now(timezone.utc)
    with db_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO price_ticks (event_time, ticker, price, volume, source)
            VALUES (%s, %s, %s, %s, %s)
        """, (now, "INTEGRATION_TEST", 99.99, 12345, "test"))
        db_conn.commit()

        cur.execute("""
            SELECT price FROM price_ticks
            WHERE ticker = 'INTEGRATION_TEST'
            ORDER BY event_time DESC LIMIT 1
        """)
        row = cur.fetchone()
        assert row is not None
        assert abs(float(row[0]) - 99.99) < 0.001

        cur.execute("DELETE FROM price_ticks WHERE ticker = 'INTEGRATION_TEST'")
        db_conn.commit()


def test_insert_and_query_sentiment_event(db_conn):
    now = datetime.now(timezone.utc)
    with db_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO sentiment_events
                (event_time, ticker, source, sentiment_score, sentiment_label, raw_text, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (now, "INTEGRATION_TEST", "test", 0.75, "positive", "test text", json.dumps({})))
        db_conn.commit()

        cur.execute("""
            SELECT sentiment_score, sentiment_label FROM sentiment_events
            WHERE ticker = 'INTEGRATION_TEST'
            ORDER BY event_time DESC LIMIT 1
        """)
        row = cur.fetchone()
        assert row is not None
        assert abs(float(row[0]) - 0.75) < 0.001
        assert row[1] == "positive"

        cur.execute("DELETE FROM sentiment_events WHERE ticker = 'INTEGRATION_TEST'")
        db_conn.commit()


def test_analytics_tables_exist(db_conn):
    expected = {"sentiment_by_symbol", "trending_tickers", "price_sentiment_correlation"}
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'analytics'
        """)
        actual = {row[0] for row in cur.fetchall()}
    assert expected.issubset(actual), f"Missing analytics tables: {expected - actual}"


def test_analytics_sentiment_by_symbol_has_data(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM analytics.sentiment_by_symbol")
        count = cur.fetchone()[0]
    assert count > 0, "analytics.sentiment_by_symbol is empty"
