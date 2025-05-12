import os
import json
import time
import logging
import psycopg2
from confluent_kafka import Consumer, KafkaError

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# --- Database Connection ---
def get_db_connection(max_retries=5, retry_delay=5):
    db_params = {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", 5432),
        "user": os.getenv("POSTGRES_USER", "admin"),
        "password": os.getenv("POSTGRES_PASSWORD", "password"),
        "database": os.getenv("POSTGRES_DB", "heartbeat_db")
    }
    logger.info(f"Attempting to connect to database: {db_params['database']}")

    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**db_params)
            logger.info("Successfully connected to PostgreSQL")
            return conn
        except Exception as e:
            logger.warning(f"Failed to connect to PostgreSQL (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)

    raise Exception("Failed to connect to PostgreSQL after retries")

# --- Kafka Consumer Setup ---
def create_consumer(bootstrap_servers, topic, max_retries=5, retry_delay=5):
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": "heartbeat-consumer-group",
        "auto.offset.reset": "earliest"
    }

    for attempt in range(max_retries):
        try:
            consumer = Consumer(conf)
            logger.info("Successfully connected to Kafka")
            consumer.subscribe([topic])
            logger.info(f"Successfully subscribed to topic: {topic}")
            return consumer
        except Exception as e:
            logger.warning(f"Failed to connect or subscribe to Kafka (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)

    raise Exception("Failed to connect or subscribe to Kafka after retries")

# --- Insert Data into DB ---
def insert_heartbeat(conn, data):
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO heartbeats (customer_id, timestamp, heart_rate)
                VALUES (%s, %s, %s)
                """,
                (data["customer_id"], data["timestamp"], data["heart_rate"])
            )
        conn.commit()
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        conn.rollback()

# --- Main Consumer Loop ---
def main():
    logger.info("Consumer starting, waiting 10 seconds to allow producer to initialize...")
    time.sleep(10)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = "heartbeats"

    consumer = create_consumer(bootstrap_servers, topic)
    conn = get_db_connection()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.warning(f"Topic {topic} not available, retrying subscription in 5 seconds...")
                    time.sleep(5)
                    try:
                        consumer.subscribe([topic])
                        logger.info(f"Successfully resubscribed to topic: {topic}")
                    except Exception as e:
                        logger.error(f"Failed to resubscribe: {e}")
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    break

            try:
                data = json.loads(msg.value().decode("utf-8"))
                if 30 <= data["heart_rate"] <= 150:
                    insert_heartbeat(conn, data)
                    logger.info(f"Inserted: {data}")
                else:
                    logger.warning(f"Invalid heart rate: {data}")
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
            except Exception as e:
                logger.error(f"Unhandled error: {e}")

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    finally:
        consumer.close()
        conn.close()
        logger.info("Connections closed.")

if __name__ == "__main__":
    main()