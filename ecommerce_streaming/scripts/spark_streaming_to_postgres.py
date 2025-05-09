import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, to_date, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Configure basic logging
logging.basicConfig(filename="/logs/spark_streaming.log", level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EcommerceStreaming") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.4.jar") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

# Define schema for input CSVs
schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("brand", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("timestamp", TimestampType(), False)
])

# Read streaming CSVs
input_path = "/data/input/"
checkpoint_dir = "/data/checkpoints/raw_events"
os.makedirs(checkpoint_dir, exist_ok=True)

streaming_df = spark.readStream \
    .schema(schema) \
    .option("header", "true") \
    .option("enforceSchema", "true") \
    .option("maxFilesPerTrigger", 3) \
    .option("cleanSource", "archive") \
    .option("sourceArchiveDir", "/data/archive") \
    .csv(input_path)

# Validate and transform data
transformed_df = streaming_df.dropna(how="any") \
    .filter(col("event_id").isNotNull() & (col("event_id") != "")) \
    .select(
        col("event_id").cast(StringType()).alias("event_id"),
        col("session_id").cast(StringType()).alias("session_id"),
        col("user_id").cast(StringType()).alias("user_id"),
        lower(col("event_type")).cast(StringType()).alias("event_type"),
        col("product_id").cast(StringType()).alias("product_id"),
        col("product_name").cast(StringType()).alias("product_name"),
        col("brand").cast(StringType()).alias("brand"),
        col("category").cast(StringType()).alias("category"),
        round(col("price").cast(DoubleType()), 2).alias("price"),
        col("quantity").cast(IntegerType()).alias("quantity"),
        round((col("price") * col("quantity")).cast(DoubleType()), 2).alias("total_price"),
        to_date(col("timestamp")).alias("event_date"),
        col("timestamp").cast(TimestampType()).alias("timestamp")
    ).filter(col("event_type").isin("view", "click", "buy"))

# PostgreSQL connection properties
pg_properties = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://postgres:5432/ecommerce",
    "dbtable": "raw_events"
}

if not pg_properties["user"] or not pg_properties["password"]:
    logger.error("POSTGRES_USER or POSTGRES_PASSWORD not set")
    raise ValueError("POSTGRES_USER or POSTGRES_PASSWORD not set")

# Write stream to PostgreSQL
def write_to_postgres(df, batch_id):
    try:
        df.write.format("jdbc") \
            .options(**pg_properties) \
            .option("truncate", "false") \
            .mode("append") \
            .save()
        logger.info("Batch %d written to PostgreSQL", batch_id)
    except Exception as e:
        logger.error("Failed to write batch %d to PostgreSQL: %s", batch_id, str(e))

logger.info("Starting raw events query")
query = transformed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", checkpoint_dir) \
    .trigger(processingTime="10 seconds") \
    .start()

# Await termination
try:
    query.awaitTermination()
except Exception as e:
    logger.error("Streaming terminated with error: %s", str(e))
finally:
    spark.stop()
    logger.info("Spark session stopped")