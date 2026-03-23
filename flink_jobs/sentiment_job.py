import os
import json
import asyncio
import logging
import psycopg2
from dotenv import load_dotenv
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream.functions import MapFunction
import anthropic

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DB_CONFIG = {
    "host":     os.environ.get("TIMESCALE_HOST", "localhost"),
    "port":     int(os.environ.get("TIMESCALE_PORT", 5432)),
    "dbname":   os.environ.get("TIMESCALE_DB", "sentiment_db"),
    "user":     os.environ.get("TIMESCALE_USER", "sentiment_user"),
    "password": os.environ.get("TIMESCALE_PASSWORD"),
}

SENTIMENT_PROMPT = """Analyze the sentiment of this financial news article toward the stock ticker mentioned.
Respond ONLY with a JSON object, no other text:
{{"score": <float between -1.0 and 1.0>, "label": "<positive|negative|neutral>", "reasoning": "<one sentence>"}}

Article: {text}"""


class NewsEnricher(MapFunction):

    def open(self, runtime_context):
        self.client = anthropic.Anthropic(
            api_key=os.environ.get("ANTHROPIC_API_KEY")
        )
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = True

    def map(self, value):
        try:
            record = json.loads(value)
            text = f"{record.get('title', '')} {record.get('body', '')}".strip()[:1500]

            # Write raw article to news_articles table
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO news_articles
                        (event_time, ticker, article_id, title, body,
                         source, url, av_sentiment_score, av_sentiment_label)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    record.get("event_time"),
                    record.get("ticker"),
                    record.get("article_id"),
                    record.get("title", "")[:500],
                    record.get("body", "")[:1000],
                    record.get("source", ""),
                    record.get("url", ""),
                    record.get("av_sentiment_score", 0.0),
                    record.get("av_sentiment_label", "Neutral"),
                ))

            # Call Claude for higher-quality sentiment scoring
            message = self.client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=200,
                messages=[{
                    "role": "user",
                    "content": SENTIMENT_PROMPT.format(text=text)
                }],
            )
            sentiment = json.loads(message.content[0].text.strip())

            # Write enriched result to sentiment_events table
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO sentiment_events
                        (event_time, ticker, source, sentiment_score,
                         sentiment_label, raw_text, price_at_event, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    record.get("event_time"),
                    record.get("ticker"),
                    "alpha_vantage",
                    sentiment.get("score", 0.0),
                    sentiment.get("label", "neutral"),
                    text[:500],
                    None,
                    json.dumps({
                        "article_id": record.get("article_id"),
                        "source":     record.get("source"),
                        "url":        record.get("url"),
                        "reasoning":  sentiment.get("reasoning", ""),
                        "av_score":   record.get("av_sentiment_score"),
                        "av_label":   record.get("av_sentiment_label"),
                    }),
                ))

            logger.info(
                f"Scored {record['ticker']}: "
                f"{sentiment['label']} ({sentiment['score']}) — {record.get('title', '')[:60]}"
            )

        except Exception as e:
            logger.error(f"Error processing news record: {e}")

        return value


class PriceWriter(MapFunction):

    def open(self, runtime_context):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = True

    def map(self, value):
        try:
            record = json.loads(value)
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO price_ticks
                        (event_time, ticker, price, volume, source)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    record["event_time"],
                    record["ticker"],
                    record["price"],
                    record.get("volume"),
                    record.get("source", "yahoo_finance"),
                ))
            logger.info(f"Wrote price: {record['ticker']} @ {record['price']}")
        except Exception as e:
            logger.error(f"Error writing price tick: {e}")
        return value


def build_kafka_source(topic, group_id):
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    wm = WatermarkStrategy.for_monotonous_timestamps()

    news_stream = env.from_source(
        build_kafka_source("news-articles", "sentiment-job-news"), wm, "News"
    )
    price_stream = env.from_source(
        build_kafka_source("price-ticks", "sentiment-job-price"), wm, "Price"
    )

    news_stream.map(NewsEnricher())
    price_stream.map(PriceWriter())

    logger.info("Starting Flink sentiment job...")
    env.execute("Stock Sentiment Job")


async def score_batch(posts: list) -> list:
    """
    Score a batch of posts using Claude API.
    Used by GEval evaluation tests.

    Args:
        posts: list of dicts with keys: post_id, symbol, text, source
    Returns:
        list of dicts with keys: post_id, symbol, sentiment, score, reasoning
    """
    client = anthropic.AsyncAnthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY")
    )
    results = []
    for post in posts:
        text = post.get("text", "")[:1500]
        message = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{
                "role": "user",
                "content": SENTIMENT_PROMPT.format(text=text),
            }],
        )
        result = json.loads(message.content[0].text.strip())
        results.append({
            "post_id":   post.get("post_id"),
            "symbol":    post.get("symbol"),
            "sentiment": result.get("label", "neutral"),
            "score":     float(result.get("score", 0.0)),
            "reasoning": result.get("reasoning", ""),
        })
    return results


if __name__ == "__main__":
    main()
