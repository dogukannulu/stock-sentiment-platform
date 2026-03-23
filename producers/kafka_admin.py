"""
Kafka topic creation and Avro schema registration.
Run once before starting producers:
    python producers/kafka_admin.py
"""
import os
import json
import logging
from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import requests

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")

TOPICS = [
    NewTopic(name="price-ticks",    num_partitions=3, replication_factor=1),
    NewTopic(name="news-articles",  num_partitions=3, replication_factor=1),
]

SCHEMAS = [
    ("price-ticks-value",   "schemas/price_tick.avsc"),
    ("news-articles-value", "schemas/news_article.avsc"),
]


def create_topics() -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    for topic in TOPICS:
        try:
            admin.create_topics([topic])
            logger.info(f"Topic '{topic.name}' created (partitions={topic.num_partitions})")
        except TopicAlreadyExistsError:
            logger.info(f"Topic '{topic.name}' already exists — skipping")
        except Exception as e:
            logger.error(f"Failed to create topic '{topic.name}': {e}")
    admin.close()


def register_schemas() -> None:
    for subject, path in SCHEMAS:
        try:
            with open(path) as f:
                schema_str = f.read()

            url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
            resp = requests.post(
                url,
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                json={"schema": schema_str},
                timeout=10,
            )
            if resp.status_code in (200, 409):
                schema_id = resp.json().get("id", "already registered")
                logger.info(f"Schema '{subject}' registered (id={schema_id})")
            else:
                logger.error(f"Failed to register '{subject}': {resp.status_code} {resp.text}")
        except Exception as e:
            logger.error(f"Error registering schema '{subject}': {e}")


def verify_schemas() -> None:
    for subject, _ in SCHEMAS:
        try:
            resp = requests.get(
                f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest",
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                logger.info(f"Verified '{subject}' — version {data['version']}, id {data['id']}")
            else:
                logger.warning(f"Could not verify '{subject}': {resp.status_code}")
        except Exception as e:
            logger.error(f"Error verifying '{subject}': {e}")


if __name__ == "__main__":
    logger.info("=== Creating Kafka topics ===")
    create_topics()

    logger.info("=== Registering Avro schemas ===")
    register_schemas()

    logger.info("=== Verifying schema registration ===")
    verify_schemas()

    logger.info("=== Done ===")
