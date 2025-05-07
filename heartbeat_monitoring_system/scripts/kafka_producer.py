import os
import json
import time
import logging
from confluent_kafka import Producer
from data_generator import generate_heart_rate_data

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def delivery_report(err, msg):
    """Callback for Kafka message delivery."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def create_producer(bootstrap_servers, max_retries=5, retry_delay=5):
    """Create a Kafka producer with retry logic."""
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "heartbeat-producer"
    }

    logger.info("Producer attempting to connect to Kafka...")
    for attempt in range(max_retries):
        try:
            producer = Producer(conf)
            logger.info("Successfully connected to Kafka")
            return producer
        except Exception as e:
            logger.warning(f"Failed to connect to Kafka (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)

    raise Exception("Failed to connect to Kafka after retries")

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = "heartbeats"

    producer = create_producer(bootstrap_servers)

    logger.info("Producer started, generating and sending heart rate data...")
    try:
        for data in generate_heart_rate_data():
            producer.produce(
                topic,
                key=data["customer_id"].encode("utf-8"),
                value=json.dumps(data).encode("utf-8"),
                callback=delivery_report
            )
            producer.poll(0)
            producer.flush()
    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    finally:
        producer.flush()
        logger.info("Producer flushed and closed.")

if __name__ == "__main__":
    main()
