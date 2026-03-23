import json
import pytest
from unittest.mock import MagicMock, patch, call
from flink_jobs.sentiment_job import NewsEnricher, PriceWriter, SENTIMENT_PROMPT


def _make_news_record(**overrides):
    base = {
        "event_time": "2026-03-24T00:00:00+00:00",
        "ticker": "AAPL",
        "article_id": "abc123",
        "title": "Apple beats earnings",
        "body": "iPhone demand strong.",
        "source": "financial_times",
        "url": "https://example.com/1",
        "av_sentiment_score": 0.72,
        "av_sentiment_label": "Bullish",
    }
    base.update(overrides)
    return json.dumps(base)


def _make_price_record(**overrides):
    base = {
        "event_time": "2026-03-24T00:00:00+00:00",
        "ticker": "AAPL",
        "price": 175.5,
        "volume": 1000000,
        "source": "yahoo_finance",
    }
    base.update(overrides)
    return json.dumps(base)


def _mock_conn():
    """Return a psycopg2 connection mock that supports context manager cursors."""
    cur = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    conn._mock_cur = cur
    return conn


def _mock_claude(sentiment):
    client = MagicMock()
    msg = MagicMock()
    msg.content[0].text = json.dumps(sentiment)
    client.messages.create.return_value = msg
    return client


class TestNewsEnricher:

    def _enricher(self, sentiment=None):
        enricher = NewsEnricher()
        enricher.conn = _mock_conn()
        enricher.client = _mock_claude(
            sentiment or {"score": 0.8, "label": "positive", "reasoning": "Strong."}
        )
        return enricher

    def test_calls_claude_api(self):
        enricher = self._enricher()
        enricher.map(_make_news_record())
        enricher.client.messages.create.assert_called_once()

    def test_claude_receives_title_and_body(self):
        enricher = self._enricher()
        enricher.map(_make_news_record(title="Big news", body="Details here."))
        prompt_sent = enricher.client.messages.create.call_args[1]["messages"][0]["content"]
        assert "Big news" in prompt_sent
        assert "Details here." in prompt_sent

    def test_inserts_to_both_tables(self):
        enricher = self._enricher()
        enricher.map(_make_news_record())
        cur = enricher.conn._mock_cur
        assert cur.execute.call_count == 2
        sqls = [c[0][0] for c in cur.execute.call_args_list]
        assert any("news_articles" in s for s in sqls)
        assert any("sentiment_events" in s for s in sqls)

    def test_sentiment_label_written_correctly(self):
        enricher = self._enricher({"score": -0.7, "label": "negative", "reasoning": "Miss."})
        enricher.map(_make_news_record())
        cur = enricher.conn._mock_cur
        sentiment_args = cur.execute.call_args_list[1][0][1]
        assert "negative" in sentiment_args

    def test_returns_original_value(self):
        enricher = self._enricher()
        record = _make_news_record()
        assert enricher.map(record) == record

    def test_handles_claude_error_gracefully(self):
        enricher = self._enricher()
        enricher.client.messages.create.side_effect = Exception("API error")
        result = enricher.map(_make_news_record())
        assert result is not None


class TestPriceWriter:

    def _writer(self):
        writer = PriceWriter()
        writer.conn = _mock_conn()
        return writer

    def test_inserts_to_price_ticks(self):
        writer = self._writer()
        writer.map(_make_price_record())
        cur = writer.conn._mock_cur
        sql = cur.execute.call_args[0][0]
        assert "price_ticks" in sql

    def test_correct_values_inserted(self):
        writer = self._writer()
        writer.map(_make_price_record(ticker="TSLA", price=185.0))
        cur = writer.conn._mock_cur
        args = cur.execute.call_args[0][1]
        assert "TSLA" in args
        assert 185.0 in args

    def test_returns_original_value(self):
        writer = self._writer()
        record = _make_price_record()
        assert writer.map(record) == record

    def test_handles_db_error_gracefully(self):
        writer = self._writer()
        writer.conn.cursor.side_effect = Exception("DB down")
        result = writer.map(_make_price_record())
        assert result is not None


class TestSentimentPrompt:

    def test_prompt_contains_article_text(self):
        result = SENTIMENT_PROMPT.format(text="NVDA AI chip demand surges.")
        assert "NVDA AI chip demand surges." in result

    def test_prompt_requests_json(self):
        result = SENTIMENT_PROMPT.format(text="test")
        assert "JSON" in result

    def test_prompt_requests_score_and_label(self):
        result = SENTIMENT_PROMPT.format(text="test")
        assert "score" in result
        assert "label" in result

    def test_prompt_specifies_score_range(self):
        result = SENTIMENT_PROMPT.format(text="test")
        assert "-1.0" in result
        assert "1.0" in result
