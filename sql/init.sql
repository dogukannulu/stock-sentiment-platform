CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw price ticks from Yahoo Finance
CREATE TABLE IF NOT EXISTS price_ticks (
    event_time  TIMESTAMPTZ NOT NULL,
    ticker      TEXT        NOT NULL,
    price       NUMERIC     NOT NULL,
    volume      BIGINT,
    source      TEXT        DEFAULT 'yahoo_finance'
);
SELECT create_hypertable('price_ticks', 'event_time', if_not_exists => TRUE);

-- Raw news articles from Alpha Vantage
CREATE TABLE IF NOT EXISTS news_articles (
    event_time          TIMESTAMPTZ NOT NULL,
    ticker              TEXT        NOT NULL,
    article_id          TEXT        NOT NULL,
    title               TEXT,
    body                TEXT,
    source              TEXT,
    url                 TEXT,
    av_sentiment_score  NUMERIC,
    av_sentiment_label  TEXT
);
SELECT create_hypertable('news_articles', 'event_time', if_not_exists => TRUE);

-- Enriched sentiment events (output of Flink job)
CREATE TABLE IF NOT EXISTS sentiment_events (
    event_time      TIMESTAMPTZ NOT NULL,
    ticker          TEXT        NOT NULL,
    source          TEXT        NOT NULL,
    sentiment_score NUMERIC,
    sentiment_label TEXT,
    raw_text        TEXT,
    price_at_event  NUMERIC,
    metadata        JSONB
);
SELECT create_hypertable('sentiment_events', 'event_time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_sentiment_ticker
    ON sentiment_events (ticker, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_price_ticker
    ON price_ticks (ticker, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_news_ticker
    ON news_articles (ticker, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_news_article_id
    ON news_articles (article_id, event_time DESC);
