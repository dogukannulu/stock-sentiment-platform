import pytest
from unittest.mock import patch, MagicMock
from producers.news_producer import produce_news, TICKERS


def _make_article(ticker, article_id="abc123"):
    return {
        "url": f"https://example.com/{article_id}",
        "title": f"{ticker} stock news",
        "summary": f"Analysis of {ticker} market performance.",
        "source": "test_source",
        "ticker_sentiment": [
            {
                "ticker": ticker,
                "ticker_sentiment_score": "0.65",
                "ticker_sentiment_label": "Bullish",
            }
        ],
    }


def test_message_structure():
    mock_producer = MagicMock()
    with patch("producers.news_producer.fetch_news") as mock_fetch:
        mock_fetch.return_value = {"feed": [_make_article("AAPL")]}
        produce_news(mock_producer, set())

    assert mock_producer.send.called
    message = mock_producer.send.call_args[1]["value"]
    for field in ("event_time", "ticker", "article_id", "title", "body",
                  "source", "url", "av_sentiment_score", "av_sentiment_label"):
        assert field in message, f"Missing field: {field}"


def test_deduplication():
    mock_producer = MagicMock()
    article = _make_article("AAPL", "dup123")
    seen = set()

    with patch("producers.news_producer.fetch_news") as mock_fetch:
        mock_fetch.return_value = {"feed": [article]}
        produce_news(mock_producer, seen)   # first call — sends
        produce_news(mock_producer, seen)   # second call — duplicate, skips

    assert mock_producer.send.call_count == 1


def test_only_watched_tickers_sent():
    mock_producer = MagicMock()
    article = _make_article("AAPL")
    article["ticker_sentiment"].append({
        "ticker": "AMZN",
        "ticker_sentiment_score": "0.5",
        "ticker_sentiment_label": "Neutral",
    })

    with patch("producers.news_producer.fetch_news") as mock_fetch:
        mock_fetch.return_value = {"feed": [article]}
        produce_news(mock_producer, set())

    tickers_sent = [c[1]["value"]["ticker"] for c in mock_producer.send.call_args_list]
    assert "AMZN" not in tickers_sent
    assert all(t in TICKERS for t in tickers_sent)


def test_rate_limit_response_skipped():
    mock_producer = MagicMock()
    with patch("producers.news_producer.fetch_news") as mock_fetch:
        mock_fetch.return_value = {"Note": "API rate limit reached."}
        produce_news(mock_producer, set())

    mock_producer.send.assert_not_called()


def test_sentiment_prompt_contains_text():
    from flink_jobs.sentiment_job import SENTIMENT_PROMPT
    result = SENTIMENT_PROMPT.format(text="NVDA AI chip demand surges.")
    assert "NVDA AI chip demand surges." in result
    assert "JSON" in result
    assert "score" in result
    assert "label" in result
