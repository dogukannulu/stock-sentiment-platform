# Stock Market Sentiment Intelligence Platform

A production-grade real-time pipeline that ingests live stock prices and financial news, scores sentiment using the Anthropic Claude API, stores results in TimescaleDB, and surfaces insights through a live Grafana dashboard and an AI market analyst agent.

## Architecture

```
Yahoo Finance (yfinance)      Alpha Vantage News API
        |                              |
  price_producer.py           news_producer.py
        |                              |
  Kafka: price-ticks          Kafka: news-articles
  (Avro schema)               (Avro schema)
                    |
            Confluent Schema Registry
                    |
            Sentiment Job (Python threads)
            (Claude Haiku scores each article → TimescaleDB)
                    |
            TimescaleDB
            (price_ticks · news_articles · sentiment_events)
                    |
              dbt models
              (staging views → sentiment_signals mart → analytics schema)
                    |
        ┌───────────┴────────────┐
   Grafana Dashboard      AI Market Analyst Agent
   (4 live panels)        (LangGraph + FastAPI /brief)
```

## Stack

| Component | Technology |
|---|---|
| Message broker | Apache Kafka + Zookeeper |
| Schema contracts | Confluent Schema Registry (Avro) |
| Stream processing | Threaded Kafka Consumers (Python) |
| Sentiment scoring | Anthropic Claude API (claude-haiku-4-5) |
| Time-series storage | TimescaleDB (Postgres 15) |
| Transformations | dbt-postgres |
| Visualization | Grafana 10 |
| Market analyst agent | LangGraph + FastAPI |
| LLM evaluation | DeepEval GEval (Claude as judge) |
| Orchestration | Docker Compose |

## Prerequisites

- Docker Desktop
- Python 3.10+
- Anthropic API key — [console.anthropic.com](https://console.anthropic.com)
- Alpha Vantage API key — [alphavantage.co](https://www.alphavantage.co/support/#api-key) (free, instant)

## Setup

### 1. Clone and configure

```bash
git clone https://github.com/YOUR_USERNAME/stock-sentiment-platform
cd stock-sentiment-platform
cp .env.example .env
# Fill in ANTHROPIC_API_KEY, ALPHA_VANTAGE_API_KEY, and TIMESCALE_PASSWORD in .env
```

### 2. Start infrastructure

```bash
docker compose up -d
docker ps   # 6 containers: zookeeper, kafka, kafka-ui, schema-registry, timescaledb, grafana
```

| Service | URL |
|---|---|
| Grafana | http://localhost:3000 (admin / admin) |
| Kafka UI | http://localhost:8888 |
| Schema Registry | http://localhost:8081 |
| TimescaleDB | localhost:5432 |

### 3. Install Python dependencies

```bash
pip install kafka-python yfinance requests python-dotenv anthropic psycopg2-binary \
            dbt-postgres pandas \
            langchain langchain-anthropic langgraph fastapi uvicorn \
            deepeval pytest
```

### 4. Register Avro schemas and create Kafka topics

```bash
python producers/kafka_admin.py
```

### 5. Run the pipeline

Open three terminals:

```bash
# Terminal 1 — price producer (polls every 60s)
python producers/price_producer.py

# Terminal 2 — news producer (polls every 60min, Alpha Vantage free tier)
python producers/news_producer.py

# Terminal 3 — sentiment job (Claude scores each article in real time)
python flink_jobs/sentiment_job.py
```

### 6. Run dbt transformations

```bash
export $(grep -v '^#' .env | xargs)
cd dbt && dbt run
# Optionally cron this every 15 minutes
```

### 7. Open the dashboard

Navigate to **http://localhost:3000/d/stock-sentiment-v1**

### 8. Start the Market Analyst Agent API (optional)

```bash
uvicorn agent.api:app --reload --port 8000
# POST http://localhost:8000/brief  {"question": "What's the sentiment on NVDA today?"}
# GET  http://localhost:8000/docs   — Swagger UI
```

## Dashboard Panels

| Panel | Description |
|---|---|
| Sentiment Score Over Time | 15-min avg Claude sentiment score per ticker |
| Current Signal per Ticker | Bullish / Bearish / Neutral signal for current hour |
| Price vs Sentiment Overlay | 1-min price bars + 15-min sentiment on dual Y-axis |
| News Article Volume | Hourly article count per ticker |

## Market Analyst Agent

A LangGraph agent backed by `claude-haiku-4-5` that answers natural-language questions about the market by querying the `analytics.*` tables directly.

```bash
curl -s -X POST http://localhost:8000/brief \
  -H "Content-Type: application/json" \
  -d '{"question": "Which tickers have the most bullish sentiment in the last 6 hours?"}' \
  | jq .brief
```

Example output:
```
**Symbol:** NVDA
**Sentiment:** Strongly positive at 0.95, with positive momentum
**Price:** $175.64, mixed recent price action (-0.96% last hour)
**Signal:** Sentiment-price divergence detected — bullish sentiment not yet reflected in price
**Confidence:** Low-Medium (sparse mention volume)
```

## Tracked Tickers

`AAPL` `GOOGL` `MSFT` `TSLA` `NVDA`

## Running Tests

```bash
# Unit + integration tests (requires Docker stack running)
export $(grep -v '^#' .env | xargs)
pytest tests/ -v --ignore=tests/evaluation

# GEval sentiment quality evaluation (uses Claude as judge, ~$0.05)
pytest tests/evaluation/ -v -s
```

| Test suite | Count | Requires |
|---|---|---|
| Unit tests (producers + sentiment job) | 23 | Nothing (mocked) |
| Integration — Kafka flow | 4 | `docker compose up -d` |
| Integration — TimescaleDB | 8 | `docker compose up -d` |
| Integration — dbt models | 6 | `docker compose up -d` + dbt |
| GEval faithfulness (10 examples) | 1 | `ANTHROPIC_API_KEY` |
| Score direction accuracy (50 examples) | 1 | `ANTHROPIC_API_KEY` |

**Evaluation results on golden dataset:**
- Label faithfulness (GEval): **100%** on 10-example sample
- Score direction accuracy: **96%** (48/50 correct sign)

## Cost Estimate

| Service | Cost |
|---|---|
| Anthropic claude-haiku (~100 articles/day) | ~$0.001/article ≈ $0.70/week |
| Alpha Vantage | Free (25 calls/day) |
| Everything else | Free (open source, local Docker) |
| **Total** | **< $1/week** |

## Project Structure

```
stock-sentiment-platform/
├── producers/
│   ├── price_producer.py       # Yahoo Finance → Kafka price-ticks
│   ├── news_producer.py        # Alpha Vantage → Kafka news-articles
│   └── kafka_admin.py          # topic creation + Avro schema registration
├── schemas/
│   ├── price_tick.avsc         # Avro schema for price ticks
│   └── news_article.avsc       # Avro schema for news articles
├── flink_jobs/
│   └── sentiment_job.py        # Claude sentiment scoring → TimescaleDB
├── agent/
│   ├── market_analyst.py       # LangGraph agent with SQL tool
│   └── api.py                  # FastAPI wrapper (POST /brief)
├── dbt/
│   ├── macros/
│   │   └── generate_schema_name.sql  # clean schema naming
│   └── models/
│       ├── staging/            # stg_price_ticks, stg_sentiment_events
│       ├── marts/              # sentiment_signals (hourly aggregates + signal)
│       └── analytics/          # sentiment_by_symbol, trending_tickers,
│                               #   price_sentiment_correlation
├── grafana/
│   ├── provisioning/           # auto-configured datasource + dashboard loader
│   └── dashboards/             # sentiment_dashboard.json
├── sql/
│   └── init.sql                # TimescaleDB hypertables + indexes
├── tests/
│   ├── conftest.py             # anthropic/pyflink mocks for unit tests
│   ├── test_price_producer.py
│   ├── test_news_producer.py
│   ├── test_sentiment_job.py
│   ├── integration/
│   │   ├── test_kafka_flow.py
│   │   ├── test_db_flow.py
│   │   └── test_dbt_models.py
│   └── evaluation/
│       ├── conftest.py             # restores real anthropic for eval tests
│       ├── golden_dataset.json     # 50 hand-labeled financial news examples
│       └── test_sentiment_quality.py  # GEval faithfulness + score direction
├── docker-compose.yml
└── .env.example
```
