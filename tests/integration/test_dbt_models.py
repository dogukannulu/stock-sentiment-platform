"""
Integration tests: dbt models build successfully and return data.
Requires Docker stack running and dbt installed.
"""
import pytest
import subprocess
import psycopg2
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


def test_dbt_run_succeeds():
    result = subprocess.run(
        ["dbt", "run", "--project-dir", "dbt"],
        capture_output=True, text=True,
        env={**os.environ},
    )
    assert result.returncode == 0, (
        f"dbt run failed:\n{result.stdout}\n{result.stderr}"
    )
    assert "PASS=" in result.stdout
    assert "ERROR=0" in result.stdout


def test_dbt_all_models_pass():
    result = subprocess.run(
        ["dbt", "run", "--project-dir", "dbt"],
        capture_output=True, text=True,
        env={**os.environ},
    )
    assert "Done. PASS=6" in result.stdout, (
        f"Expected 6 passing models.\n{result.stdout}"
    )


def test_staging_views_exist(db_conn):
    views = ["stg_price_ticks", "stg_sentiment_events"]
    with db_conn.cursor() as cur:
        for view in views:
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.views
                WHERE table_schema = 'public' AND table_name = %s
            """, (view,))
            assert cur.fetchone()[0] == 1, f"View '{view}' not found"


def test_sentiment_signals_mart_has_data(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM public.sentiment_signals")
        assert cur.fetchone()[0] > 0, "sentiment_signals mart is empty"


def test_analytics_sentiment_by_symbol_columns(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("SELECT * FROM analytics.sentiment_by_symbol LIMIT 1")
        col_names = [desc[0] for desc in cur.description]
    expected_cols = {"bucket", "ticker", "avg_sentiment_score", "mention_count", "sentiment_momentum"}
    assert expected_cols.issubset(set(col_names)), (
        f"Missing columns: {expected_cols - set(col_names)}"
    )


def test_analytics_price_sentiment_correlation_columns(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("SELECT * FROM analytics.price_sentiment_correlation LIMIT 1")
        col_names = [desc[0] for desc in cur.description]
    expected_cols = {"hour_bucket", "ticker", "open_price", "close_price", "price_change_pct", "avg_sentiment"}
    assert expected_cols.issubset(set(col_names)), (
        f"Missing columns: {expected_cols - set(col_names)}"
    )
