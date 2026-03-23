# CLAUDE.md — Week 1: Real-time Market Intelligence Engine with AI Analyst

## Project Goal

Build a production-grade real-time pipeline that ingests live stock data and
Reddit posts, scores sentiment using Claude Haiku, stores results in
TimescaleDB, transforms with dbt, visualizes on Grafana, and exposes an
AI market analyst agent that reads from the pipeline's own dbt marts to
generate natural language market briefs on demand.

The pipeline makes the AI trustworthy. The AI makes the pipeline compelling.

## Positioning

> "Anyone can call an LLM API. This project shows how to build the streaming
> infrastructure that makes LLM outputs reliable, grounded, and production-ready."

## Free Data Sources

- **Alpaca Markets** (free IEX feed — real-time US stock trades)
  Sign up at alpaca.markets → Dashboard → API Keys → generate key
  Select "Paper Trading" — same free data feed, no real money involved
- **Reddit API via PRAW** (free)
  reddit.com/prefs/apps → "create app" → type: script
  Subreddits: r/stocks, r/wallstreetbets, r/investing
- **Simulator fallback:** set USE_SIMULATOR=true in .env to skip both APIs
  and generate realistic fake data. Use this if APIs aren't ready yet.

## Full Stack

| Component | Tool | Why |
|-----------|------|-----|
| Message broker | Kafka + Zookeeper + Schema Registry | Decoupling, schema enforcement, replay |
| Stream processing | PyFlink 1.18 | Exactly-once semantics, stateful windowing |
| Time-series DB | TimescaleDB (Postgres 15) | Postgres-compatible, hypertables, dbt-native |
| Transformations | dbt-core | Version-controlled SQL, testable, documented |
| Visualization | Grafana 10 (provisioned) | Real-time dashboards, no manual setup |
| AI sentiment | Claude Haiku (claude-haiku-4-5-20251001) | Fast (<1s), cheap (~$0.001/post), accurate |
| AI analyst agent | LangChain + FastAPI | Reads dbt marts, generates grounded market briefs |
| LLM evaluation | DeepEval (GEval) | Quantitative sentiment quality score for article |
| Local orchestration | Docker Compose | One command to start everything |
| Language | Python 3.11 | |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                              │
│  Alpaca WebSocket          Reddit PRAW                           │
│  (price-ticks topic)       (social-posts topic)                  │
│         └──────── Schema Registry (Avro) ──────────┘            │
└─────────────────────────┬───────────────────────────────────────┘
                          │ Kafka
┌─────────────────────────▼───────────────────────────────────────┐
│                    PROCESSING LAYER                              │
│  PyFlink Job                                                     │
│  ├── Reads price-ticks → writes to TimescaleDB.price_ticks      │
│  ├── Reads social-posts → batches 10 posts                       │
│  │   → Claude Haiku (async sentiment scoring)                    │
│  └── Writes scored posts → TimescaleDB.sentiment_scores          │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                     STORAGE LAYER                                │
│  TimescaleDB                                                     │
│  ├── price_ticks (hypertable)                                    │
│  └── sentiment_scores (hypertable)                               │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                   ANALYTICS LAYER                                │
│  dbt models                                                      │
│  ├── staging: stg_price_ticks, stg_sentiment_scores             │
│  └── marts: sentiment_by_symbol, trending_tickers,              │
│             price_sentiment_correlation                          │
└──────────┬──────────────────────────────┬───────────────────────┘
           │                              │
┌──────────▼──────────┐     ┌────────────▼──────────────────────┐
│   Grafana Dashboard  │     │   AI Market Analyst Agent          │
│   (live panels)      │     │   LangChain + FastAPI              │
│                      │     │   Reads dbt marts via SQL tool     │
│                      │     │   → natural language market brief  │
└──────────────────────┘     └───────────────────────────────────┘
```

## Repo Structure

```
stock-sentiment-platform/
├── CLAUDE.md
├── README.md
├── Makefile
├── docker-compose.yml
├── docker-compose.test.yml
├── .env.example                  ← copy to .env, fill in keys
├── .gitignore                    ← includes .env, always
├── .pre-commit-config.yaml
├── requirements.txt
│
├── db/
│   └── init.sql                  ← TimescaleDB schema
│
├── schemas/
│   ├── price_tick.avsc            ← Avro schema for price events
│   └── social_post.avsc           ← Avro schema for Reddit posts
│
├── producers/
│   ├── __init__.py
│   ├── kafka_admin.py             ← create topics + register schemas
│   ├── alpaca_producer.py         ← Alpaca WebSocket → Kafka
│   ├── reddit_producer.py         ← Reddit PRAW → Kafka
│   └── simulator.py               ← fake data when USE_SIMULATOR=true
│
├── flink_jobs/
│   ├── __init__.py
│   └── sentiment_job.py           ← PyFlink: Kafka → Claude → TimescaleDB
│
├── agent/
│   ├── __init__.py
│   ├── market_analyst.py          ← LangChain agent (reads dbt marts)
│   └── api.py                     ← FastAPI wrapper for agent
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml               ← reads from env vars, never hardcoded
│   └── models/
│       ├── staging/
│       │   ├── stg_price_ticks.sql
│       │   └── stg_sentiment_scores.sql
│       ├── marts/
│       │   ├── sentiment_by_symbol.sql
│       │   ├── trending_tickers.sql
│       │   └── price_sentiment_correlation.sql
│       └── schema.yml             ← dbt tests + documentation
│
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       │   └── timescaledb.yaml
│       └── dashboards/
│           ├── dashboard.yaml
│           └── sentiment.json     ← exported after manual build
│
├── tests/
│   ├── unit/
│   │   ├── __init__.py
│   │   ├── test_producers.py
│   │   ├── test_sentiment_job.py
│   │   └── test_agent.py
│   ├── integration/
│   │   ├── __init__.py
│   │   ├── test_kafka_flow.py
│   │   ├── test_db_flow.py
│   │   └── test_dbt_models.py
│   └── evaluation/
│       ├── __init__.py
│       ├── golden_dataset.json    ← 50 hand-labeled financial posts
│       └── test_sentiment_quality.py
│
└── docs/
    └── architecture.drawio
```

---

## Security Rules — Read Before Every Step

1. NEVER hardcode any key, password, or token in any Python file.
2. NEVER commit the `.env` file — it is in `.gitignore` from Step 1.
3. Always use `os.environ.get("KEY")` to read secrets.
4. Always add `.env` check at startup: if a required key is missing,
   print a clear error and exit — do not silently continue.
5. Use `pre-commit` hooks from Step 1 to catch accidental secret commits.
6. Generated files (dbt target/, __pycache__) are in `.gitignore`.

---

## House Rules for Claude Code

- Read this entire CLAUDE.md before writing a single line of code.
- Implement one step at a time. Do not start Step 2 until Step 1's
  smoke test passes.
- Every Python file must have a `if __name__ == "__main__"` guard.
- Every function must have a docstring.
- Use `loguru` for all logging — never use `print()` in production code.
- Use type hints on all function signatures.
- After completing each step, explicitly state: "Step N complete.
  Run the smoke test, confirm it passes, then ask me to continue."

---

## Step 1 — Repo Scaffold & Docker Environment

### What we're doing
Create the complete folder structure, Docker Compose with all 8 services,
`.env.example`, `.gitignore`, `requirements.txt`, `Makefile`, and
`pre-commit` hooks to prevent accidental credential commits.

### Why this matters
A reproducible environment is the foundation of everything. Anyone should
be able to clone the repo, copy `.env.example` to `.env`, fill in their
keys, run `make up`, and have the full stack running in under 5 minutes.
Pre-commit hooks are the safety net that catches secrets before they
reach GitHub — non-negotiable for a production repo.

### How to implement

**1. Create the complete folder structure** (all directories from Repo
Structure above, with empty `__init__.py` files in Python packages).

**2. Create `docker-compose.yml`** with these exact services:

```yaml
version: "3.8"
services:

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"

  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    depends_on: [kafka]
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
      SCHEMA_REGISTRY_HOST_NAME: schema-registry

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on: [kafka, schema-registry]
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081

  timescaledb:
    image: timescale/timescaledb:latest-pg15
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: ${TIMESCALE_USER}
      POSTGRES_PASSWORD: ${TIMESCALE_PASSWORD}
      POSTGRES_DB: ${TIMESCALE_DB}
    volumes:
      - timescale_data:/var/lib/postgresql/data
      - ./db/init.sql:/docker-entrypoint-initdb.d/init.sql

  grafana:
    image: grafana/grafana:10.2.0
    depends_on: [timescaledb]
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD:-admin}
      GF_USERS_ALLOW_SIGN_UP: "false"
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning

  flink-jobmanager:
    image: flink:1.18-python
    ports:
      - "8082:8081"
    command: jobmanager
    environment:
      FLINK_PROPERTIES: "jobmanager.rpc.address: flink-jobmanager"
    volumes:
      - ./flink_jobs:/opt/flink/jobs
      - ./requirements.txt:/opt/flink/requirements.txt

  flink-taskmanager:
    image: flink:1.18-python
    depends_on: [flink-jobmanager]
    command: taskmanager
    environment:
      FLINK_PROPERTIES: "jobmanager.rpc.address: flink-jobmanager\ntaskmanager.numberOfTaskSlots: 2"
    volumes:
      - ./flink_jobs:/opt/flink/jobs

volumes:
  timescale_data:
  grafana_data:
```

**3. Create `.env.example`:**
```
# Anthropic
ANTHROPIC_API_KEY=your_key_here

# Alpaca (free at alpaca.markets → Paper Trading → API Keys)
ALPACA_API_KEY=your_key_here
ALPACA_API_SECRET=your_secret_here
ALPACA_WS_URL=wss://stream.data.alpaca.markets/v2/iex

# Reddit (reddit.com/prefs/apps → create script app)
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=sentiment-pipeline/1.0

# TimescaleDB
TIMESCALE_USER=sentiment_user
TIMESCALE_PASSWORD=choose_strong_password
TIMESCALE_DB=sentiment_db
TIMESCALE_HOST=localhost
TIMESCALE_PORT=5432

# Grafana
GRAFANA_PASSWORD=admin

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
SCHEMA_REGISTRY_URL=http://localhost:8081

# Agent API
AGENT_API_PORT=8000

# Simulator (set to true to skip real APIs)
USE_SIMULATOR=false

# Tickers to track
WATCHLIST=AAPL,TSLA,NVDA,MSFT,AMZN,GOOGL,META,SPY
```

**4. Create `.gitignore`:**
```
.env
*.pyc
__pycache__/
.pytest_cache/
dbt/target/
dbt/dbt_packages/
dbt/logs/
.dbt/
profiles.yml
*.egg-info/
dist/
.venv/
venv/
```

**5. Create `requirements.txt`:**
```
# Kafka
confluent-kafka==2.3.0
fastavro==1.9.4
requests==2.31.0

# Data sources
alpaca-py==0.20.0
praw==7.7.1
websocket-client==1.7.0

# Flink (install separately in Flink container)
apache-flink==1.18.0

# Database
psycopg2-binary==2.9.9
sqlalchemy==2.0.25

# dbt
dbt-postgres==1.7.4

# AI
anthropic==0.25.0
langchain==0.1.16
langchain-anthropic==0.1.6

# API
fastapi==0.110.0
uvicorn==0.27.1

# Evaluation
deepeval==0.21.0

# Utilities
python-dotenv==1.0.1
loguru==0.7.2
pydantic==2.6.3
```

**6. Create `.pre-commit-config.yaml`:**
```yaml
repos:
  - repo: https://github.com/Yelp/detect-secrets
    rev: v1.4.0
    hooks:
      - id: detect-secrets
        args: ['--baseline', '.secrets.baseline']
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-added-large-files
```

Run: `pip install pre-commit && pre-commit install`

**7. Create `Makefile`:**
```makefile
.PHONY: up down logs test test-unit test-integration test-eval \
        produce dbt-run dbt-test dbt-docs agent

up:
	docker-compose up -d

down:
	docker-compose down -v

logs:
	docker-compose logs -f

ps:
	docker-compose ps

test-unit:
	pytest tests/unit/ -v --tb=short

test-integration:
	pytest tests/integration/ -v --tb=short

test-eval:
	pytest tests/evaluation/ -v -s

test-all:
	make test-unit && make test-integration

produce:
	python producers/alpaca_producer.py & python producers/reddit_producer.py

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

dbt-docs:
	cd dbt && dbt docs generate && dbt docs serve

agent:
	uvicorn agent.api:app --reload --port 8000
```

### Smoke test
```bash
make up
sleep 15
make ps
# Every service should show "Up" — not "Exit" or "Restarting"
open http://localhost:8080   # Kafka UI — topics list (empty, that's fine)
open http://localhost:3000   # Grafana — login with admin / admin
open http://localhost:8082   # Flink Web UI — Overview page
```

**📸 Screenshot for article:** `docker-compose ps` output in terminal with
all 8 services showing "Up" status.

---

## Step 2 — TimescaleDB Schema

### What we're doing
Create two hypertables — TimescaleDB's core feature that automatically
partitions time-series data by time chunks for dramatically faster queries.
One for raw price ticks, one for sentiment scores.

### Why this matters
Schema-first design. Everything downstream (Flink sink, dbt models, Grafana
queries, agent SQL tool) depends on this contract being stable and correct.
Getting this right now prevents painful migrations later.

### How to implement

Create `db/init.sql`:

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw price ticks from Alpaca
CREATE TABLE IF NOT EXISTS price_ticks (
    time        TIMESTAMPTZ     NOT NULL,
    symbol      TEXT            NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    volume      BIGINT,
    trade_id    TEXT,
    source      TEXT            DEFAULT 'alpaca'
);
SELECT create_hypertable('price_ticks', 'time', if_not_exists => TRUE);
CREATE INDEX ON price_ticks (symbol, time DESC);

-- Sentiment scores from Claude (via Flink)
CREATE TABLE IF NOT EXISTS sentiment_scores (
    time        TIMESTAMPTZ     NOT NULL,
    symbol      TEXT            NOT NULL,
    source      TEXT            NOT NULL,  -- 'reddit'
    subreddit   TEXT,
    post_id     TEXT,
    raw_text    TEXT,
    sentiment   TEXT            NOT NULL,  -- 'positive','negative','neutral'
    score       DOUBLE PRECISION NOT NULL, -- -1.0 to 1.0
    model       TEXT            DEFAULT 'claude-haiku-4-5-20251001',
    batch_id    TEXT
);
SELECT create_hypertable('sentiment_scores', 'time', if_not_exists => TRUE);
CREATE INDEX ON sentiment_scores (symbol, time DESC);
CREATE INDEX ON sentiment_scores (sentiment, time DESC);
```

### Smoke test
```bash
docker exec -it $(docker-compose ps -q timescaledb) \
  psql -U $TIMESCALE_USER -d $TIMESCALE_DB -c "\dt"
# Should show: price_ticks, sentiment_scores
docker exec -it $(docker-compose ps -q timescaledb) \
  psql -U $TIMESCALE_USER -d $TIMESCALE_DB \
  -c "SELECT * FROM timescaledb_information.hypertables;"
# Both tables should appear as hypertables
```

**📸 Screenshot for article:** The `timescaledb_information.hypertables`
query output — shows both tables are proper time-series hypertables.

---

## Step 3 — Kafka Topics & Avro Schemas

### What we're doing
Create 2 Kafka topics with Avro schemas registered in Schema Registry.
Topics: `price-ticks` and `social-posts`. Avro enforces the data contract
— a producer sending a malformed message is rejected before it hits Kafka.

### Why this matters
Schema Registry is the difference between a hobby project and a production
pipeline. Without it, a schema change in a producer silently breaks all
consumers. With it, the contract is explicit, versioned, and enforced.

### How to implement

Create `schemas/price_tick.avsc`:
```json
{
  "type": "record",
  "name": "PriceTick",
  "namespace": "com.sentiment.pipeline",
  "fields": [
    {"name": "time",     "type": "string"},
    {"name": "symbol",   "type": "string"},
    {"name": "price",    "type": "double"},
    {"name": "volume",   "type": ["null", "long"], "default": null},
    {"name": "trade_id", "type": ["null", "string"], "default": null},
    {"name": "source",   "type": "string", "default": "alpaca"}
  ]
}
```

Create `schemas/social_post.avsc`:
```json
{
  "type": "record",
  "name": "SocialPost",
  "namespace": "com.sentiment.pipeline",
  "fields": [
    {"name": "time",      "type": "string"},
    {"name": "symbol",    "type": "string"},
    {"name": "text",      "type": "string"},
    {"name": "post_id",   "type": "string"},
    {"name": "subreddit", "type": "string"},
    {"name": "source",    "type": "string", "default": "reddit"}
  ]
}
```

Create `producers/kafka_admin.py` — script that:
1. Creates both topics (3 partitions, replication factor 1 for local)
2. Registers both Avro schemas with Schema Registry
3. Verifies registration by fetching the schema back
4. Prints a clear success/failure message for each step

Key logic in `kafka_admin.py`:
```python
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSchema
import json, os
from loguru import logger

def create_topics(bootstrap_servers: str) -> None:
    """Create Kafka topics if they don't already exist."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    topics = [
        NewTopic("price-ticks",   num_partitions=3, replication_factor=1),
        NewTopic("social-posts",  num_partitions=3, replication_factor=1),
    ]
    futures = admin.create_topics(topics)
    for topic, future in futures.items():
        try:
            future.result()
            logger.info(f"Topic '{topic}' created successfully")
        except Exception as e:
            if "already exists" in str(e):
                logger.info(f"Topic '{topic}' already exists — skipping")
            else:
                raise

def register_schemas(registry_url: str) -> None:
    """Register Avro schemas with Schema Registry."""
    client = SchemaRegistryClient({"url": registry_url})
    for name, path in [
        ("price-ticks-value",  "schemas/price_tick.avsc"),
        ("social-posts-value", "schemas/social_post.avsc"),
    ]:
        with open(path) as f:
            schema_str = f.read()
        schema_id = client.register_schema(name, AvroSchema(schema_str))
        logger.info(f"Schema '{name}' registered with ID {schema_id}")
```

### Smoke test
```bash
python producers/kafka_admin.py
# Open http://localhost:8080 → Topics
# price-ticks and social-posts should appear with 3 partitions each
curl http://localhost:8081/subjects
# Should return: ["price-ticks-value","social-posts-value"]
```

**📸 Screenshot for article:** Kafka UI showing both topics + Schema
Registry `/subjects` endpoint in browser — two clean visuals side by side.

---

## Step 4 — Data Producers

### What we're doing
Build 2 producers and a shared simulator. The Alpaca producer connects
to the Alpaca WebSocket for real-time trade data. The Reddit producer
streams new posts from 3 subreddits. Both fall back cleanly to the
simulator when `USE_SIMULATOR=true`.

### Why this matters
Real data makes the demo credible and the article honest. The simulator
fallback means the entire pipeline can be demonstrated even without API
keys — essential for readers trying to replicate it.

### How to implement

**`producers/simulator.py`** — shared simulator utilities:
```python
import random
import time
from datetime import datetime, timezone
from typing import Generator
from loguru import logger

WATCHLIST = ["AAPL", "TSLA", "NVDA", "MSFT", "AMZN", "GOOGL", "META", "SPY"]

BASE_PRICES = {
    "AAPL": 185.0, "TSLA": 250.0, "NVDA": 850.0, "MSFT": 415.0,
    "AMZN": 185.0, "GOOGL": 175.0, "META": 500.0, "SPY": 510.0
}

REDDIT_TEMPLATES = [
    ("{symbol} earnings beat expectations, revenue up 15% YoY", "stocks"),
    ("Just bought more {symbol}, this dip is a gift", "wallstreetbets"),
    ("{symbol} looking bearish, breaking below 200-day MA", "investing"),
    ("Analysts raise {symbol} price target to ${target}", "stocks"),
    ("Why I think {symbol} is overvalued at current levels", "investing"),
    ("{symbol} to the moon!! 🚀🚀", "wallstreetbets"),
    ("Selling my {symbol} position, taking profits here", "stocks"),
    ("{symbol} Q3 guidance disappoints, stock tanks after-hours", "investing"),
]

def generate_price_tick(symbol: str | None = None) -> dict:
    """Generate a realistic simulated price tick."""
    sym = symbol or random.choice(WATCHLIST)
    base = BASE_PRICES[sym]
    # Random walk with drift
    BASE_PRICES[sym] = base * (1 + random.gauss(0, 0.001))
    return {
        "time":     datetime.now(timezone.utc).isoformat(),
        "symbol":   sym,
        "price":    round(BASE_PRICES[sym], 2),
        "volume":   random.randint(100, 10000),
        "trade_id": f"sim_{int(time.time() * 1000)}",
        "source":   "simulator",
    }

def generate_reddit_post(symbol: str | None = None) -> dict:
    """Generate a realistic simulated Reddit post."""
    sym = symbol or random.choice(WATCHLIST)
    template, subreddit = random.choice(REDDIT_TEMPLATES)
    text = template.format(symbol=sym, target=round(BASE_PRICES[sym] * 1.15))
    return {
        "time":      datetime.now(timezone.utc).isoformat(),
        "symbol":    sym,
        "text":      text,
        "post_id":   f"sim_{int(time.time() * 1000)}",
        "subreddit": subreddit,
        "source":    "simulator",
    }

def price_tick_stream(interval_seconds: float = 1.0) -> Generator:
    """Yield simulated price ticks continuously."""
    while True:
        yield generate_price_tick()
        time.sleep(interval_seconds)

def reddit_post_stream(interval_seconds: float = 5.0) -> Generator:
    """Yield simulated Reddit posts continuously."""
    while True:
        yield generate_reddit_post()
        time.sleep(interval_seconds)
```

**`producers/alpaca_producer.py`** — key structure:
```python
import os, json
from loguru import logger
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

load_dotenv()

WATCHLIST = os.environ.get("WATCHLIST", "AAPL,TSLA,NVDA,MSFT").split(",")
USE_SIMULATOR = os.environ.get("USE_SIMULATOR", "false").lower() == "true"

def run() -> None:
    """Main entry point — runs real Alpaca stream or simulator."""
    producer = build_producer()
    if USE_SIMULATOR:
        logger.info("USE_SIMULATOR=true — running price tick simulator")
        run_simulator(producer)
    else:
        logger.info("Connecting to Alpaca WebSocket")
        run_alpaca_stream(producer)

def run_alpaca_stream(producer: Producer) -> None:
    """Connect to Alpaca IEX WebSocket and stream trade events to Kafka."""
    # Use alpaca-py AlpacaDataStream
    # On each trade event: serialize with Avro → produce to price-ticks topic
    # Handle reconnection on disconnect
    ...

def run_simulator(producer: Producer) -> None:
    """Generate fake price ticks and produce to Kafka."""
    from producers.simulator import price_tick_stream
    for tick in price_tick_stream(interval_seconds=1.0):
        produce_message(producer, "price-ticks", tick["symbol"], tick)
        logger.debug(f"Simulated tick: {tick['symbol']} @ {tick['price']}")

if __name__ == "__main__":
    run()
```

**`producers/reddit_producer.py`** — key structure:
```python
import os, re
from loguru import logger
from dotenv import load_dotenv
import praw

load_dotenv()

WATCHLIST = set(os.environ.get("WATCHLIST", "AAPL,TSLA,NVDA").split(","))
SUBREDDITS = ["stocks", "wallstreetbets", "investing"]
TICKER_PATTERN = re.compile(r'\b([A-Z]{2,5})\b')

def extract_symbols(text: str) -> list[str]:
    """Extract ticker symbols from post text that match our watchlist."""
    found = TICKER_PATTERN.findall(text)
    return [t for t in found if t in WATCHLIST]

def run() -> None:
    """Main entry point — runs real Reddit stream or simulator."""
    if os.environ.get("USE_SIMULATOR", "false").lower() == "true":
        logger.info("USE_SIMULATOR=true — running Reddit post simulator")
        run_simulator()
    else:
        run_reddit_stream()

def run_reddit_stream() -> None:
    """Stream Reddit posts and produce to Kafka when ticker found."""
    reddit = praw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        user_agent=os.environ["REDDIT_USER_AGENT"],
    )
    subreddit = reddit.subreddit("+".join(SUBREDDITS))
    for post in subreddit.stream.submissions(skip_existing=True):
        symbols = extract_symbols(f"{post.title} {post.selftext}")
        for symbol in symbols:
            message = {
                "time":      str(post.created_utc),
                "symbol":    symbol,
                "text":      f"{post.title} {post.selftext[:500]}",
                "post_id":   post.id,
                "subreddit": post.subreddit.display_name,
                "source":    "reddit",
            }
            produce_message(producer, "social-posts", symbol, message)
            logger.info(f"Post for {symbol} from r/{message['subreddit']}")

if __name__ == "__main__":
    run()
```

### Smoke test
```bash
# Terminal 1
USE_SIMULATOR=true python producers/alpaca_producer.py

# Terminal 2
USE_SIMULATOR=true python producers/reddit_producer.py

# After 30 seconds: open Kafka UI → Topics → price-ticks → Messages tab
# Verify messages are flowing with decoded Avro payloads
```

**📸 Screenshot for article:** Kafka UI Messages tab showing decoded Avro
messages for both topics — symbol, price, timestamp clearly visible.

---

## Step 5 — PyFlink Sentiment Job

### What we're doing
Build the PyFlink streaming job that reads from both Kafka topics, calls
Claude Haiku for sentiment scoring (batched async for efficiency), and
writes results to TimescaleDB.

### Why this matters
This is the heart of the pipeline. Flink provides exactly-once semantics —
no duplicate sentiment scores, no missed messages, even if the job restarts.
Batching Claude calls (10 posts per call) reduces API costs by 10x vs.
calling Claude once per post.

### How to implement

Create `flink_jobs/sentiment_job.py`:

```python
"""
PyFlink job: Kafka → Claude Haiku sentiment scoring → TimescaleDB

Flow:
  social-posts topic → batch 10 posts → Claude Haiku → sentiment_scores table
  price-ticks topic  → pass-through   →               → price_ticks table
"""
import os, json, asyncio
from datetime import datetime, timezone
from loguru import logger
from dotenv import load_dotenv
import anthropic
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
BATCH_SIZE = 10
MODEL = "claude-haiku-4-5-20251001"

SENTIMENT_PROMPT = """You are a financial sentiment analyzer.
Analyze the sentiment of these social media posts about stocks.
For each post, respond with ONLY a JSON array (no other text):
[
  {{"post_id": "<id>", "sentiment": "positive"|"negative"|"neutral", "score": <float -1.0 to 1.0>}},
  ...
]

Posts to analyze:
{posts}"""

def get_db_connection():
    """Create a PostgreSQL connection to TimescaleDB."""
    return psycopg2.connect(
        host=os.environ["TIMESCALE_HOST"],
        port=int(os.environ["TIMESCALE_PORT"]),
        dbname=os.environ["TIMESCALE_DB"],
        user=os.environ["TIMESCALE_USER"],
        password=os.environ["TIMESCALE_PASSWORD"],
    )

async def score_batch(posts: list[dict]) -> list[dict]:
    """Score a batch of posts using Claude Haiku. Returns posts with scores."""
    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
    posts_text = "\n".join(
        f"post_id: {p['post_id']} | symbol: {p['symbol']} | text: {p['text'][:300]}"
        for p in posts
    )
    message = await client.messages.create(
        model=MODEL,
        max_tokens=512,
        messages=[{"role": "user", "content": SENTIMENT_PROMPT.format(posts=posts_text)}]
    )
    raw = message.content[0].text.strip()
    results = json.loads(raw)
    # Map results back to original posts
    result_map = {r["post_id"]: r for r in results}
    scored = []
    for post in posts:
        result = result_map.get(post["post_id"], {})
        scored.append({
            **post,
            "sentiment": result.get("sentiment", "neutral"),
            "score":     result.get("score", 0.0),
            "model":     MODEL,
        })
    return scored

def write_sentiment_scores(conn, scored_posts: list[dict]) -> None:
    """Batch insert sentiment scores to TimescaleDB."""
    rows = [
        (
            datetime.now(timezone.utc),
            p["symbol"],
            p["source"],
            p.get("subreddit"),
            p["post_id"],
            p["text"][:1000],
            p["sentiment"],
            p["score"],
            p["model"],
        )
        for p in scored_posts
    ]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO sentiment_scores
              (time, symbol, source, subreddit, post_id, raw_text,
               sentiment, score, model)
            VALUES %s
            ON CONFLICT DO NOTHING
        """, rows)
    conn.commit()
    logger.info(f"Inserted {len(rows)} sentiment scores")

def write_price_ticks(conn, ticks: list[dict]) -> None:
    """Batch insert price ticks to TimescaleDB."""
    rows = [
        (t["time"], t["symbol"], t["price"], t.get("volume"), t.get("trade_id"), t["source"])
        for t in ticks
    ]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO price_ticks (time, symbol, price, volume, trade_id, source)
            VALUES %s ON CONFLICT DO NOTHING
        """, rows)
    conn.commit()

def consume_and_process() -> None:
    """Main loop: consume from Kafka, batch, score, write to TimescaleDB."""
    from confluent_kafka import Consumer, KafkaException

    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id":          "flink-sentiment-job",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe(["social-posts", "price-ticks"])
    conn = get_db_connection()

    post_batch: list[dict] = []
    tick_batch: list[dict] = []

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                # Flush batches on timeout
                if post_batch:
                    scored = asyncio.run(score_batch(post_batch))
                    write_sentiment_scores(conn, scored)
                    post_batch.clear()
                if tick_batch:
                    write_price_ticks(conn, tick_batch)
                    tick_batch.clear()
                continue

            if msg.error():
                raise KafkaException(msg.error())

            value = json.loads(msg.value().decode("utf-8"))

            if msg.topic() == "social-posts":
                post_batch.append(value)
                if len(post_batch) >= BATCH_SIZE:
                    scored = asyncio.run(score_batch(post_batch))
                    write_sentiment_scores(conn, scored)
                    post_batch.clear()

            elif msg.topic() == "price-ticks":
                tick_batch.append(value)
                if len(tick_batch) >= 50:
                    write_price_ticks(conn, tick_batch)
                    tick_batch.clear()

    except KeyboardInterrupt:
        logger.info("Shutting down sentiment job")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    consume_and_process()
```

### Smoke test
```bash
# Terminal 1 — producers running (simulator mode)
make produce

# Terminal 2 — Flink sentiment job
python flink_jobs/sentiment_job.py

# After 2 minutes — query TimescaleDB
docker exec -it $(docker-compose ps -q timescaledb) \
  psql -U $TIMESCALE_USER -d $TIMESCALE_DB \
  -c "SELECT symbol, sentiment, score, time FROM sentiment_scores ORDER BY time DESC LIMIT 10;"
```

**📸 Screenshot for article:** TimescaleDB query showing live sentiment
scores with symbol, label, and score — proves the pipeline is working
end-to-end. Strong article visual.

---

## Step 6 — dbt Transformation Layer

### What we're doing
Build dbt models on top of TimescaleDB: staging models clean the raw data,
mart models compute the aggregations that Grafana and the AI agent consume.

### Why this matters
The AI analyst agent reads from dbt mart models, not raw tables. This
separation is critical — if the agent reads raw data directly, it gets
noise. The mart models are the clean, trusted, business-logic layer.
dbt also gives you tests, documentation, and lineage for free.

### How to implement

**`dbt/profiles.yml`** (reads from env vars — never hardcode):
```yaml
sentiment_pipeline:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('TIMESCALE_HOST') }}"
      port: "{{ env_var('TIMESCALE_PORT') | int }}"
      user: "{{ env_var('TIMESCALE_USER') }}"
      password: "{{ env_var('TIMESCALE_PASSWORD') }}"
      dbname: "{{ env_var('TIMESCALE_DB') }}"
      schema: analytics
      threads: 4
```

**`dbt/models/staging/stg_price_ticks.sql`:**
```sql
select
    time::timestamptz                   as time,
    symbol,
    price::double precision             as price,
    coalesce(volume, 0)::bigint         as volume,
    source,
    date_trunc('minute', time)          as minute_bucket,
    date_trunc('hour', time)            as hour_bucket
from {{ source('raw', 'price_ticks') }}
where time >= now() - interval '7 days'
  and price > 0
  and symbol is not null
```

**`dbt/models/staging/stg_sentiment_scores.sql`:**
```sql
select
    time::timestamptz                   as time,
    symbol,
    source,
    subreddit,
    post_id,
    raw_text,
    sentiment,
    score::double precision             as score,
    model,
    date_trunc('5 minutes', time)       as five_min_bucket,
    date_trunc('hour', time)            as hour_bucket
from {{ source('raw', 'sentiment_scores') }}
where time >= now() - interval '7 days'
  and sentiment in ('positive', 'negative', 'neutral')
  and score between -1.0 and 1.0
```

**`dbt/models/marts/sentiment_by_symbol.sql`:**
```sql
-- 5-minute windowed sentiment aggregation per ticker
-- This is the primary table the AI analyst agent queries
select
    five_min_bucket                     as bucket,
    symbol,
    avg(score)                          as avg_sentiment_score,
    count(*)                            as mention_count,
    sum(case when sentiment = 'positive' then 1 else 0 end)
                                        as positive_count,
    sum(case when sentiment = 'negative' then 1 else 0 end)
                                        as negative_count,
    sum(case when sentiment = 'neutral'  then 1 else 0 end)
                                        as neutral_count,
    -- Sentiment momentum: current window vs prior window
    avg(score) - lag(avg(score), 1)
        over (partition by symbol order by five_min_bucket)
                                        as sentiment_momentum
from {{ ref('stg_sentiment_scores') }}
group by five_min_bucket, symbol
order by five_min_bucket desc, mention_count desc
```

**`dbt/models/marts/trending_tickers.sql`:**
```sql
-- Tickers with most mentions in the last hour vs prior hour
with current_hour as (
    select symbol, count(*) as current_mentions
    from {{ ref('stg_sentiment_scores') }}
    where time >= now() - interval '1 hour'
    group by symbol
),
prior_hour as (
    select symbol, count(*) as prior_mentions
    from {{ ref('stg_sentiment_scores') }}
    where time between now() - interval '2 hours'
                   and now() - interval '1 hour'
    group by symbol
)
select
    c.symbol,
    c.current_mentions,
    coalesce(p.prior_mentions, 0)       as prior_mentions,
    c.current_mentions - coalesce(p.prior_mentions, 0)
                                        as mention_delta,
    case when coalesce(p.prior_mentions, 0) = 0 then null
         else round(c.current_mentions::numeric /
                    p.prior_mentions * 100 - 100, 1)
    end                                 as pct_change
from current_hour c
left join prior_hour p using (symbol)
order by mention_delta desc
```

**`dbt/models/marts/price_sentiment_correlation.sql`:**
```sql
-- Hourly price change vs sentiment — what the AI agent uses for briefs
with prices as (
    select
        hour_bucket,
        symbol,
        first_value(price) over w        as open_price,
        last_value(price)  over w        as close_price,
        max(price)         over w        as high_price,
        min(price)         over w        as low_price
    from {{ ref('stg_price_ticks') }}
    window w as (partition by symbol, hour_bucket
                 order by time
                 rows between unbounded preceding and unbounded following)
),
sentiment as (
    select
        hour_bucket,
        symbol,
        avg(score)      as avg_sentiment,
        count(*)        as mention_count
    from {{ ref('stg_sentiment_scores') }}
    group by hour_bucket, symbol
)
select
    p.hour_bucket,
    p.symbol,
    p.open_price,
    p.close_price,
    round(((p.close_price - p.open_price) / p.open_price * 100)::numeric, 3)
                        as price_change_pct,
    s.avg_sentiment,
    s.mention_count
from prices p
left join sentiment s
       on p.hour_bucket = s.hour_bucket
      and p.symbol      = s.symbol
where p.open_price is not null
order by p.hour_bucket desc, p.symbol
```

Add `dbt/models/schema.yml` with tests:
```yaml
version: 2
models:
  - name: stg_sentiment_scores
    columns:
      - name: symbol
        tests: [not_null]
      - name: sentiment
        tests:
          - accepted_values:
              values: ['positive', 'negative', 'neutral']
      - name: score
        tests: [not_null]
  - name: sentiment_by_symbol
    columns:
      - name: symbol
        tests: [not_null]
      - name: avg_sentiment_score
        tests: [not_null]
```

### Smoke test
```bash
make dbt-run
# All 5 models should show green

make dbt-test
# All tests should pass

make dbt-docs
# Opens browser — verify lineage graph shows staging → marts correctly
```

**📸 Screenshot for article:** `dbt run` output (all green) + lineage graph
in `dbt docs serve`. The lineage graph is clean and visual.

---

## Step 7 — AI Market Analyst Agent

### What we're doing
Build a LangChain agent that reads from the dbt mart models using a SQL
tool and generates natural language market briefs on demand. The agent's
responses are grounded in real pipeline data — not hallucinated.

### Why this matters
This is what separates this project from every other sentiment pipeline
tutorial. The agent turns data into insight. A reader seeing
*"NVDA sentiment turned strongly positive 23 minutes ago. Price has not
yet reacted. This pattern preceded a >2% move in 4 of the last 5 similar
setups."* immediately understands the value. The DE stack made that
possible. The AI made it accessible.

### How to implement

**`agent/market_analyst.py`:**
```python
"""
Market Analyst Agent — LangChain agent that reads dbt mart models
and generates natural language market briefs grounded in real data.
"""
import os
from loguru import logger
from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic
from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain.tools import tool
from langchain_core.prompts import ChatPromptTemplate
import psycopg2
import pandas as pd

load_dotenv()

SYSTEM_PROMPT = """You are a quantitative market analyst with access to
real-time sentiment and price data from a streaming pipeline.

When asked about a stock or market trend, you:
1. Query the relevant data tables using your SQL tool
2. Analyze the numbers carefully
3. Generate a concise, data-backed market brief

Always cite specific numbers from the data. Never speculate beyond
what the data shows. If data is insufficient, say so clearly.

Format your brief as:
- **Symbol:** [TICKER]
- **Sentiment:** [current trend and score]
- **Price:** [current price and recent change]
- **Signal:** [what the data suggests]
- **Confidence:** [low/medium/high based on data volume]
"""

def get_db_connection():
    return psycopg2.connect(
        host=os.environ["TIMESCALE_HOST"],
        port=int(os.environ["TIMESCALE_PORT"]),
        dbname=os.environ["TIMESCALE_DB"],
        user=os.environ["TIMESCALE_USER"],
        password=os.environ["TIMESCALE_PASSWORD"],
    )

@tool
def query_sentiment_data(sql: str) -> str:
    """
    Execute a SQL query against the analytics schema (dbt mart models).
    Available tables: analytics.sentiment_by_symbol, analytics.trending_tickers,
    analytics.price_sentiment_correlation.
    Always use the analytics schema prefix.
    Returns results as a formatted string.
    """
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(sql, conn)
        conn.close()
        if df.empty:
            return "No data found for this query."
        return df.to_string(index=False, max_rows=20)
    except Exception as e:
        return f"Query error: {e}"

def build_agent() -> AgentExecutor:
    """Build and return the market analyst agent."""
    llm = ChatAnthropic(
        model="claude-haiku-4-5-20251001",
        api_key=os.environ["ANTHROPIC_API_KEY"],
        temperature=0,
    )
    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT),
        ("human",  "{input}"),
        ("placeholder", "{agent_scratchpad}"),
    ])
    agent = create_tool_calling_agent(llm, [query_sentiment_data], prompt)
    return AgentExecutor(agent=agent, tools=[query_sentiment_data], verbose=True)

def get_market_brief(question: str) -> str:
    """Get a market brief for a given question."""
    executor = build_agent()
    result = executor.invoke({"input": question})
    return result["output"]

if __name__ == "__main__":
    brief = get_market_brief(
        "What is the current sentiment for NVDA and has price moved in the same direction?"
    )
    print(brief)
```

**`agent/api.py`** — FastAPI wrapper:
```python
"""FastAPI wrapper for the market analyst agent."""
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from loguru import logger
from agent.market_analyst import get_market_brief

app = FastAPI(title="Market Analyst Agent API", version="1.0.0")

class BriefRequest(BaseModel):
    question: str

class BriefResponse(BaseModel):
    question: str
    brief: str

@app.post("/brief", response_model=BriefResponse)
async def get_brief(request: BriefRequest) -> BriefResponse:
    """Generate a market brief for the given question."""
    try:
        brief = get_market_brief(request.question)
        return BriefResponse(question=request.question, brief=brief)
    except Exception as e:
        logger.error(f"Agent error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
```

### Smoke test
```bash
# Start the agent API
make agent

# In another terminal — ask the agent a question
curl -X POST http://localhost:8000/brief \
  -H "Content-Type: application/json" \
  -d '{"question": "What are the most mentioned tickers in the last hour and what is their sentiment?"}'

# Should return a structured market brief backed by real data
```

**📸 Screenshot for article:** Terminal showing the agent's natural language
market brief output. This is your **article hero image** — show the raw
pipeline data on one side and the AI brief on the other. This visual tells
the whole story in one screenshot.

---

## Step 8 — Grafana Dashboard

### What we're doing
Build a 4-panel Grafana dashboard provisioned as code — no manual clicking.
It comes up automatically with `make up`.

### Why this matters
Provisioned dashboards are reproducible. They demonstrate infrastructure-as-code
thinking. A reader who clones the repo gets the full dashboard in seconds.

### How to implement

**`grafana/provisioning/datasources/timescaledb.yaml`:**
```yaml
apiVersion: 1
datasources:
  - name: TimescaleDB
    type: postgres
    url: timescaledb:5432
    database: sentiment_db
    user: ${TIMESCALE_USER}
    secureJsonData:
      password: ${TIMESCALE_PASSWORD}
    jsonData:
      sslmode: disable
      postgresVersion: 1500
      timescaledb: true
    isDefault: true
```

**`grafana/provisioning/dashboards/dashboard.yaml`:**
```yaml
apiVersion: 1
providers:
  - name: default
    folder: ''
    type: file
    options:
      path: /etc/grafana/provisioning/dashboards
```

Build 4 panels manually in Grafana UI (then export JSON):

**Panel 1 — Live Sentiment Score by Ticker (Time series)**
```sql
SELECT bucket as time, symbol, avg_sentiment_score
FROM analytics.sentiment_by_symbol
WHERE $__timeFilter(bucket)
ORDER BY bucket
```

**Panel 2 — Trending Tickers (Table)**
```sql
SELECT symbol, current_mentions, prior_mentions, mention_delta, pct_change
FROM analytics.trending_tickers
ORDER BY mention_delta DESC LIMIT 10
```

**Panel 3 — Sentiment Distribution (Bar chart)**
```sql
SELECT symbol,
  positive_count, negative_count, neutral_count
FROM analytics.sentiment_by_symbol
WHERE bucket >= now() - interval '1 hour'
ORDER BY mention_count DESC LIMIT 8
```

**Panel 4 — Price vs Sentiment (Dual axis)**
```sql
SELECT hour_bucket as time, symbol, price_change_pct, avg_sentiment
FROM analytics.price_sentiment_correlation
WHERE $__timeFilter(hour_bucket) AND symbol = 'NVDA'
ORDER BY hour_bucket
```

After building: Dashboard → Share → Export → Save JSON to
`grafana/provisioning/dashboards/sentiment.json`

### Smoke test
```bash
open http://localhost:3000
# Dashboard loads automatically — all 4 panels show data
# No manual setup required after docker-compose up
```

**📸 Screenshot for article:** Full Grafana dashboard with live data in
all 4 panels, multiple tickers visible. Wait 30+ minutes for data to
accumulate before screenshotting.

---

## Step 9 — Unit Tests

### What we're doing
Fast, isolated tests for all components. No external dependencies.
Mock everything. Must run with zero infrastructure in under 10 seconds.

### How to implement

**`tests/unit/test_producers.py`:**
```python
"""Unit tests for Kafka producers and simulator."""
import pytest
from unittest.mock import patch, MagicMock
from producers.simulator import (
    generate_price_tick, generate_reddit_post, WATCHLIST
)
from producers.reddit_producer import extract_symbols

class TestSimulator:
    def test_price_tick_schema(self):
        tick = generate_price_tick()
        assert all(k in tick for k in ["time", "symbol", "price", "volume", "trade_id", "source"])
        assert tick["symbol"] in WATCHLIST
        assert isinstance(tick["price"], float)
        assert tick["price"] > 0

    def test_price_tick_specific_symbol(self):
        tick = generate_price_tick("AAPL")
        assert tick["symbol"] == "AAPL"

    def test_reddit_post_schema(self):
        post = generate_reddit_post()
        assert all(k in post for k in ["time", "symbol", "text", "post_id", "subreddit", "source"])
        assert post["symbol"] in WATCHLIST
        assert len(post["text"]) > 0

    def test_reddit_post_source(self):
        post = generate_reddit_post()
        assert post["source"] == "simulator"

class TestTickerExtraction:
    def test_dollar_sign_ticker(self):
        assert "TSLA" in extract_symbols("I love $TSLA stock")

    def test_plain_ticker(self):
        assert "AAPL" in extract_symbols("AAPL earnings beat expectations")

    def test_multiple_tickers(self):
        symbols = extract_symbols("NVDA and MSFT both up today")
        assert "NVDA" in symbols
        assert "MSFT" in symbols

    def test_no_ticker(self):
        assert extract_symbols("no tickers in this post") == []

    def test_non_watchlist_ticker_excluded(self):
        assert "ZZZZ" not in extract_symbols("ZZZZ is not a real ticker")
```

**`tests/unit/test_sentiment_job.py`:**
```python
"""Unit tests for sentiment job logic."""
import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock
from flink_jobs.sentiment_job import SENTIMENT_PROMPT, score_batch

class TestSentimentPrompt:
    def test_prompt_includes_post_id(self):
        formatted = SENTIMENT_PROMPT.format(posts="post_id: abc123 | text: test")
        assert "abc123" in formatted

    def test_prompt_includes_json_instruction(self):
        formatted = SENTIMENT_PROMPT.format(posts="test post")
        assert "JSON" in formatted

class TestScoreBatch:
    @pytest.mark.asyncio
    async def test_score_batch_returns_all_posts(self):
        posts = [
            {"post_id": "1", "symbol": "AAPL", "text": "AAPL is great", "source": "reddit"},
            {"post_id": "2", "symbol": "TSLA", "text": "TSLA is crashing", "source": "reddit"},
        ]
        mock_response = MagicMock()
        mock_response.content[0].text = json.dumps([
            {"post_id": "1", "sentiment": "positive", "score": 0.8},
            {"post_id": "2", "sentiment": "negative", "score": -0.7},
        ])
        with patch("anthropic.AsyncAnthropic") as mock_client:
            mock_client.return_value.messages.create = AsyncMock(return_value=mock_response)
            results = await score_batch(posts)
        assert len(results) == 2
        assert results[0]["sentiment"] == "positive"
        assert results[1]["sentiment"] == "negative"

    @pytest.mark.asyncio
    async def test_malformed_response_defaults_to_neutral(self):
        posts = [{"post_id": "1", "symbol": "AAPL", "text": "test", "source": "reddit"}]
        mock_response = MagicMock()
        mock_response.content[0].text = "not valid json"
        with patch("anthropic.AsyncAnthropic") as mock_client:
            mock_client.return_value.messages.create = AsyncMock(return_value=mock_response)
            with pytest.raises(json.JSONDecodeError):
                await score_batch(posts)
```

**`tests/unit/test_agent.py`:**
```python
"""Unit tests for the market analyst agent."""
import pytest
from unittest.mock import patch, MagicMock

class TestQuerySentimentDataTool:
    def test_tool_returns_dataframe_string(self):
        mock_df = MagicMock()
        mock_df.empty = False
        mock_df.to_string.return_value = "symbol  avg_sentiment_score\n  AAPL              0.42"
        with patch("pandas.read_sql_query", return_value=mock_df):
            with patch("agent.market_analyst.get_db_connection"):
                from agent.market_analyst import query_sentiment_data
                result = query_sentiment_data.invoke(
                    "SELECT * FROM analytics.sentiment_by_symbol LIMIT 1"
                )
        assert "AAPL" in result

    def test_tool_handles_empty_result(self):
        mock_df = MagicMock()
        mock_df.empty = True
        with patch("pandas.read_sql_query", return_value=mock_df):
            with patch("agent.market_analyst.get_db_connection"):
                from agent.market_analyst import query_sentiment_data
                result = query_sentiment_data.invoke("SELECT 1")
        assert "No data found" in result

    def test_tool_handles_db_error(self):
        with patch("agent.market_analyst.get_db_connection", side_effect=Exception("conn refused")):
            from agent.market_analyst import query_sentiment_data
            result = query_sentiment_data.invoke("SELECT 1")
        assert "Query error" in result
```

### Smoke test
```bash
make test-unit
# Should complete in under 10 seconds, zero infrastructure needed
# All tests green
```

**📸 Screenshot for article:** `pytest tests/unit/ -v` output — clean
green list of passing tests.

---

## Step 10 — Integration Tests

### What we're doing
Tests that exercise real infrastructure: Docker stack running, real Kafka
messages, real TimescaleDB rows, real dbt models. These catch wiring bugs
that unit tests can never catch.

### How to implement

**`tests/integration/test_kafka_flow.py`:**
```python
"""Integration test: producer → Kafka → message confirmed."""
import pytest
import json
import time
from confluent_kafka import Consumer, Producer
from producers.simulator import generate_price_tick, generate_reddit_post
import os
from dotenv import load_dotenv

load_dotenv()

@pytest.fixture(scope="module")
def consumer():
    c = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id":          "integration-test-group",
        "auto.offset.reset": "earliest",
    })
    yield c
    c.close()

def test_price_tick_reaches_kafka(consumer):
    """Produce one price tick and verify it arrives in the topic."""
    producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
    tick = generate_price_tick("AAPL")
    producer.produce("price-ticks", key="AAPL", value=json.dumps(tick).encode())
    producer.flush()

    consumer.subscribe(["price-ticks"])
    start = time.time()
    while time.time() - start < 10:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            data = json.loads(msg.value().decode())
            if data.get("symbol") == "AAPL":
                assert data["price"] > 0
                return
    pytest.fail("No price tick received within 10 seconds")

def test_reddit_post_reaches_kafka(consumer):
    """Produce one Reddit post and verify it arrives in the topic."""
    producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
    post = generate_reddit_post("TSLA")
    producer.produce("social-posts", key="TSLA", value=json.dumps(post).encode())
    producer.flush()

    consumer.subscribe(["social-posts"])
    start = time.time()
    while time.time() - start < 10:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            data = json.loads(msg.value().decode())
            if data.get("symbol") == "TSLA":
                assert len(data["text"]) > 0
                return
    pytest.fail("No Reddit post received within 10 seconds")
```

**`tests/integration/test_db_flow.py`:**
```python
"""Integration test: TimescaleDB inserts and hypertable verification."""
import pytest
import psycopg2
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

load_dotenv()

@pytest.fixture(scope="module")
def db_conn():
    conn = psycopg2.connect(
        host=os.environ["TIMESCALE_HOST"],
        port=int(os.environ["TIMESCALE_PORT"]),
        dbname=os.environ["TIMESCALE_DB"],
        user=os.environ["TIMESCALE_USER"],
        password=os.environ["TIMESCALE_PASSWORD"],
    )
    yield conn
    conn.close()

def test_price_ticks_table_exists(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'price_ticks'")
        assert cur.fetchone()[0] == 1

def test_sentiment_scores_is_hypertable(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM timescaledb_information.hypertables
            WHERE hypertable_name = 'sentiment_scores'
        """)
        assert cur.fetchone()[0] == 1

def test_insert_and_query_sentiment_score(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO sentiment_scores
              (time, symbol, source, post_id, raw_text, sentiment, score, model)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            datetime.now(timezone.utc),
            "INTEGRATION_TEST",
            "test",
            "test_post_001",
            "This is a test post",
            "positive",
            0.75,
            "test_model",
        ))
        db_conn.commit()

        cur.execute("""
            SELECT sentiment, score FROM sentiment_scores
            WHERE symbol = 'INTEGRATION_TEST' AND post_id = 'test_post_001'
        """)
        row = cur.fetchone()
        assert row is not None
        assert row[0] == "positive"
        assert abs(row[1] - 0.75) < 0.001

        # Clean up
        cur.execute("DELETE FROM sentiment_scores WHERE symbol = 'INTEGRATION_TEST'")
        db_conn.commit()
```

**`tests/integration/test_dbt_models.py`:**
```python
"""Integration test: dbt models build and return data."""
import pytest
import subprocess
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def test_dbt_run_succeeds():
    result = subprocess.run(
        ["dbt", "run", "--project-dir", "dbt", "--profiles-dir", "dbt"],
        capture_output=True, text=True
    )
    assert result.returncode == 0, f"dbt run failed:\n{result.stdout}\n{result.stderr}"

def test_dbt_tests_pass():
    result = subprocess.run(
        ["dbt", "test", "--project-dir", "dbt", "--profiles-dir", "dbt"],
        capture_output=True, text=True
    )
    assert result.returncode == 0, f"dbt tests failed:\n{result.stdout}"

def test_mart_tables_exist():
    conn = psycopg2.connect(
        host=os.environ["TIMESCALE_HOST"],
        port=int(os.environ["TIMESCALE_PORT"]),
        dbname=os.environ["TIMESCALE_DB"],
        user=os.environ["TIMESCALE_USER"],
        password=os.environ["TIMESCALE_PASSWORD"],
    )
    with conn.cursor() as cur:
        for table in ["sentiment_by_symbol", "trending_tickers", "price_sentiment_correlation"]:
            cur.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = 'analytics' AND table_name = '{table}'
            """)
            assert cur.fetchone()[0] == 1, f"Table analytics.{table} not found"
    conn.close()
```

### Smoke test
```bash
# Requires docker stack running
make up && sleep 20
make test-integration
```

**📸 Screenshot for article:** `pytest tests/integration/ -v` output.

---

## Step 11 — GEval Sentiment Quality Evaluation

### What we're doing
Use DeepEval's GEval metric to evaluate Claude Haiku's sentiment scoring
quality on a 50-post golden dataset of real financial posts with known labels.
This produces a quantitative score we can report in the article.

### Why this matters
Without evaluation, you have no idea if the sentiment model is good.
With GEval, you can write: *"Claude Haiku achieved 86% label accuracy on
our financial sentiment benchmark — higher than TextBlob (71%) and
comparable to FinBERT (89%) at 100x lower infrastructure cost."*
That sentence makes the article credible and shareable.

### How to implement

**`tests/evaluation/golden_dataset.json`** — create 50 hand-labeled examples
covering clear positive, clear negative, neutral, ambiguous, and sarcastic:
```json
[
  {
    "text": "NVDA smashed earnings, revenue up 122% YoY, guidance raised significantly",
    "symbol": "NVDA",
    "expected_sentiment": "positive",
    "expected_score_direction": 1
  },
  {
    "text": "Fed signals more rate hikes ahead, markets selloff across the board",
    "symbol": "SPY",
    "expected_sentiment": "negative",
    "expected_score_direction": -1
  },
  {
    "text": "AAPL trading flat ahead of next week's product event",
    "symbol": "AAPL",
    "expected_sentiment": "neutral",
    "expected_score_direction": 0
  },
  {
    "text": "Yeah sure TSLA is definitely going to $1000 lmao",
    "symbol": "TSLA",
    "expected_sentiment": "negative",
    "expected_score_direction": -1
  }
]
```
Build the full 50-example dataset covering all 8 tickers in the watchlist,
across all 3 sentiment classes, including edge cases.

**`tests/evaluation/test_sentiment_quality.py`:**
```python
"""
GEval evaluation of Claude Haiku sentiment scoring quality.
Runs against the golden dataset and reports faithfulness score.
Costs approximately $0.05 for 50 examples with Claude Haiku.
"""
import pytest
import json
import asyncio
from deepeval import evaluate
from deepeval.metrics import GEval
from deepeval.test_case import LLMTestCase, LLMTestCaseParams
from flink_jobs.sentiment_job import score_batch

@pytest.fixture(scope="module")
def golden_dataset():
    with open("tests/evaluation/golden_dataset.json") as f:
        return json.load(f)

faithfulness_metric = GEval(
    name="Sentiment Label Faithfulness",
    criteria=(
        "The predicted sentiment label (positive/negative/neutral) correctly "
        "reflects the financial sentiment expressed in the input text. "
        "Consider: sarcasm, context, financial domain conventions."
    ),
    evaluation_params=[
        LLMTestCaseParams.INPUT,
        LLMTestCaseParams.ACTUAL_OUTPUT,
        LLMTestCaseParams.EXPECTED_OUTPUT,
    ],
    threshold=0.75,
)

score_calibration_metric = GEval(
    name="Score Direction Calibration",
    criteria=(
        "The sentiment score's sign (+/-/0) matches the expected direction. "
        "Positive sentiment should have score > 0, negative < 0, neutral ≈ 0."
    ),
    evaluation_params=[
        LLMTestCaseParams.INPUT,
        LLMTestCaseParams.ACTUAL_OUTPUT,
        LLMTestCaseParams.EXPECTED_OUTPUT,
    ],
    threshold=0.80,
)

def test_sentiment_faithfulness(golden_dataset):
    """Test that Claude's sentiment labels match ground truth."""
    test_cases = []

    # Score all posts in batches of 10
    for i in range(0, len(golden_dataset), 10):
        batch = golden_dataset[i:i+10]
        posts = [
            {
                "post_id": str(j),
                "symbol":  item["symbol"],
                "text":    item["text"],
                "source":  "evaluation",
            }
            for j, item in enumerate(batch, start=i)
        ]
        results = asyncio.run(score_batch(posts))

        for item, result in zip(batch, results):
            test_cases.append(LLMTestCase(
                input=item["text"],
                actual_output=f"{result['sentiment']} (score: {result['score']:.2f})",
                expected_output=item["expected_sentiment"],
            ))

    evaluate(test_cases, [faithfulness_metric, score_calibration_metric])

    # Assert metrics passed their thresholds
    for tc in test_cases:
        for metric in tc.metrics_data or []:
            assert metric.success, (
                f"Metric '{metric.name}' failed with score {metric.score:.2f} "
                f"on input: {tc.input[:80]}..."
            )
```

### Smoke test
```bash
make test-eval
# GEval will print a detailed score report
# Target: faithfulness > 0.75, calibration > 0.80
# This call costs ~$0.05 total with Claude Haiku
```

**📸 Screenshot for article:** GEval evaluation report output showing
both metric scores. This is a unique visual — very few DE/AI articles
include quantitative LLM quality evaluation with actual scores.

---

## Full Startup Sequence

```bash
# 1. Copy and fill in credentials
cp .env.example .env
# Fill in ANTHROPIC_API_KEY, ALPACA_API_KEY, etc.
# Or set USE_SIMULATOR=true to skip real APIs

# 2. Install pre-commit hooks
pip install pre-commit
pre-commit install

# 3. Start the full stack
make up
sleep 20

# 4. Create Kafka topics + register schemas
python producers/kafka_admin.py

# 5. Run dbt
make dbt-run

# 6. Start producers (two terminals)
python producers/alpaca_producer.py
python producers/reddit_producer.py

# 7. Start Flink sentiment job (new terminal)
python flink_jobs/sentiment_job.py

# 8. Start agent API (new terminal)
make agent

# 9. Open dashboards
open http://localhost:3000   # Grafana
open http://localhost:8080   # Kafka UI
open http://localhost:8082   # Flink Web UI
open http://localhost:8000/docs  # Agent API (Swagger)

# 10. Run tests
make test-all
```

---

## Medium Article Outline
*(Write this after the project is complete and tested)*

1. **Hook:** "My data pipeline now writes its own market reports — here's how"
2. Architecture diagram (describe to Claude Code → generate as drawio)
3. Free real-time data: Alpaca + Reddit setup (< 10 minutes, no credit card)
4. Kafka + Schema Registry: enforcing data contracts in streaming
5. PyFlink sentiment job: batched async Claude calls — architecture and cost analysis
6. TimescaleDB + dbt: why the analytics layer matters for AI grounding
7. The AI analyst agent: how LangChain reads from dbt marts
8. Sample agent output — show a real market brief (screenshot)
9. GEval results: measuring Claude's sentiment accuracy (show the score)
10. Grafana dashboard walkthrough (hero image)
11. What I'd do differently + what's coming in Week 2
12. GitHub link + newsletter CTA + GitHub Sponsors link

---

## Cost Estimate

| Resource | Cost |
|----------|------|
| Alpaca Markets (IEX feed) | $0 |
| Reddit API | $0 |
| Anthropic Claude Haiku (sentiment, ~100 posts/day) | ~$0.10/day |
| Anthropic Claude Haiku (agent queries, ~20/day) | ~$0.02/day |
| GEval evaluation run (50 posts, one time) | ~$0.05 total |
| Infrastructure (all local Docker) | $0 |
| **Total for a full week of running** | **< $1** |
