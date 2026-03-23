import os
import json
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import yfinance as yf
from kafka import KafkaProducer

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TICKERS = ["AAPL", "GOOGL", "MSFT", "TSLA", "NVDA"]
POLL_INTERVAL_SECONDS = 60
KAFKA_TOPIC = "price-ticks"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )


def fetch_and_produce(producer):
    for ticker in TICKERS:
        try:
            data = yf.Ticker(ticker)
            info = data.fast_info
            price = info.last_price
            volume = info.three_month_average_volume

            if price is None:
                logger.warning(f"No price data for {ticker}, skipping.")
                continue

            message = {
                "event_time": datetime.now(timezone.utc).isoformat(),
                "ticker": ticker,
                "price": round(float(price), 4),
                "volume": int(volume) if volume else None,
                "source": "yahoo_finance",
            }
            producer.send(KAFKA_TOPIC, value=message)
            logger.info(f"Sent price tick: {ticker} @ {price}")

        except Exception as e:
            logger.error(f"Error fetching {ticker}: {e}")

    producer.flush()


def main():
    logger.info("Starting price producer...")
    producer = create_producer()
    try:
        while True:
            fetch_and_produce(producer)
            logger.info(f"Sleeping {POLL_INTERVAL_SECONDS}s...")
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Shutting down price producer.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
